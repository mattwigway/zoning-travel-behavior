# run the Frank-Wolfe static traffic assignment

using ArgParse
using DataFrames
using CSV
using Tables
using MetaGraphs
using Serialization
using LibSpatialIndex
using Geodesy
using DataFrames
using HDF5

include("constants.jl")
include("fw_weights.jl")
include("scag_cost_funcs.jl")
include("assignment.jl")

function parse_args()
    s = ArgParseSettings()

    @add_arg_table! s begin
        "--graph"
            help = "Frank-Wolfe graph from build_fw_graph.jl"
            required = true
            arg_type = String
            action = :store_arg
        "--od"
            help = "Origin-destination file with lat/lon columns"
            required = true
            arg_type = String
            action = :store_arg
        "--demand-file"
            help = "OpenMatrix file containing o-d demand from a travel model. WARN: mappings not yet supported."
            required = true
            arg_type = String
            action = :store_arg
        "--demand-matrix"
            help = """
            Name of matrix in OMX file
            If more than one is specified, they will be summed together
            e.g. --demand-matrix DRIVE_ALONE CARPOOL
            """
            required = true
            nargs = '+'
            arg_type = String
            action = :store_arg
        "--demand-scale"
            help = "Scale demand by factor (e.g. when partial simulation has occurred, or when using a multi-hour matrix that needs to be scaled down, or to account for vehicle occupancy)"
            arg_type = Float64
            default = 1.0
            action = :store_arg
        "--centroid-connector-speed-kph"
            help = "Centroid connector speed in km/hr"
            arg_type = Float64
            default = 30.0
            action = :store_arg
        "--cost-function"
            help = """Cost function for computing congested travel times.
Available cost functions:
SCAG         Simplified cost function from the Southern California Association of Governments four-step model
    """
            arg_type = String
            default = "scag"
            action = :store_arg
        "--output"
            help = """CSV file for output - with columns segment_id, target_segment_id, turn_flow, total_flow, free_flow_travel_time_secs, congested_travel_time_secs.
            Note that total flow is for all targets for a single segment, not for (say) just the left turns."""
            required = true
            arg_type = String
            action = :store_arg
        "--limit-ods"
            help = "Sample the first N O-D pairs for assignment (for speed when testing)"
            arg_type = Int64
        "--single-strong-component"
            help = """
            Only allow routing on the single largest strong component of the graph (removes disconnected areas e.g. due to OSM errors).
            Will also remove actual islands, which may not be desirable, if the OD matrix contains no trips between them"""
            action = :store_true
        "--relative-gap-tolerance"
            help = """
            Tolerance for convergence of relative gap.
            Default 0.0001, based on Boyce, D., Ralevic-Dekic, B., & Bar-Gera, H. (2004).
            Convergence of Traffic Assignments: How Much is Enough? Journal of Transportation Engineering,
            130(1), 49–55. https://doi.org/10.1061/(ASCE)0733-947X(2004)130:1(49)
            """
            arg_type = Float64
            default = 0.0001
    end

    return ArgParse.parse_args(s)
end

function main()
    args = parse_args()

    @info "reading graph"
    G = deserialize(args["graph"])

    @info "reading ods"
    odraw = CSV.File(args["od"]) |> DataFrame

    @info "reading od matrices"
    first, rest = Iterators.peel(args["demand-matrix"])

    h5open(args["demand-file"]) do file
        @info "..$first"
        odmat = read(file, "/data/$first")

        for mtxn in rest
            @info "..$mtxn"
            odmat += read(file, "/data/$mtxn")
        end
    end
    
    @assert ndims(odmat) == 2 "OD matrix must have two dimensions"
    @assert size(odmat, 1) == size(odmat, 2) "OD matrix must be square"
    # could do this more efficiently with a loop
    @assert all(odmat .>= 0) "OD matrix has negative flows"
    n_ods = size(odmat, 1)
    @info "read $n_ods x $n_ods OD matrix"

    scale_factor = args["demand-scale"]
    @info "scaling travel demand by factor $scale_factor"
    odmat *= scale_factor

    @assert nrow(odraw) == n_ods "OD file and OD matrix must have same number of ODs"

    sample_ods = args["limit-ods"]
    if !isnothing(sample_ods)
        @warn "SAMPLING ONLY FIRST $sample_ods ODs for testing speed - NOT INTENDED FOR PRODUCTION"
        odraw = odraw[1:sample_ods,:]
        odmat = odmat[1:sample_ods,1:sample_ods]
        n_ods = sample_ods
    end

    tottrp = sum(odmat)
    @assert tottrp > 0 "no trips in O-D matrix!"

    @info "$tottrp trips in O-D matrix"

    lon_scale::Float64 = -999.0

    if args["single-strong-component"]
        @info "finding strongly-connected components"
        components = strongly_connected_components(G)

        largest_component_size = 0
        next_largest_component_size = 0
        largest_component_idx = -1

        for (i, component) in enumerate(components)
            component_size = length(component)
            if component_size > largest_component_size
                next_largest_component_size = largest_component_size
                largest_component_size = component_size
                largest_component_idx = i
            elseif component_size > next_largest_component_size
                next_largest_component_size = component_size
            end
        end

        @info "largest strong component has $largest_component_size vertices"
        @info "$(length(components) - 1) strong components with ≤ $next_largest_component_size vertices will not be used in assignment"

        # we don't actually remove the other components from the graph, just don't add them to the spatial index so the ODs
        # can't be connected to them. This avoids lots of consternation and keeping track of changing vertex indices.
        vertices_to_index = components[largest_component_idx]
    else
        vertices_to_index = 1:nv(G)

        if !is_strongly_connected(G)
            @warn "graph is not strongly connected. Consider running with --single-strong-component"
        end
    end

    @info "building spatial index"
    sidx = LibSpatialIndex.RTree(2)
    lon_scale = cosd((get_prop(G, vertices_to_index[1], :geom)::LLA).lat)
    for v in vertices_to_index
        geom = get_prop(G, v, :geom)::LLA
        LibSpatialIndex.insert!(sidx, v, [geom.lat, geom.lon * lon_scale], [geom.lat, geom.lon * lon_scale])
    end

    @info "linking ods to graph"
    centroid_connector_speed_kph = args["centroid-connector-speed-kph"]

    # ODs become vertices at high end of graph
    # note that this is actually the index of the last true vertex - but since vertex indices are
    # 1-based, the first vertex will be at offset + 1
    od_vertex_offset = nv(G)
    set_prop!(G, :od_vertex_offset, od_vertex_offset)

    # add two vertices per OD - one for origin, one for destinations
    # we have to use two vertices to prevent the router from taking shortcuts through centroid
    # connectors
    @assert add_vertices!(G, n_ods * 2) == n_ods * 2

    centroid_connectors = map(enumerate(Tables.rows(odraw))) do t
        i, r = t

        # query the spatial index for 15 nearest neighbors
        # if there is a four-way intersection there will be 12 nearest neighbors at exactly the same distance
        # for each departing way and its three turn possibilities
        candidates = LibSpatialIndex.knn(sidx, [r.lat, r.lon * lon_scale], 15)
        distances = map(candidates) do c
            return euclidean_distance(LLA(r.lat, r.lon, 0.0), get_prop(G, c, :geom))
        end

        thres_dist = minimum(distances) * 1.1

        linked = false
        for (candidate, distance) in zip(candidates, distances)
            if distance < thres_dist
                linked = true
                # link destructively into graph - harmless as graph is re-read each time this script is run
                vertex_id = od_vertex_offset + i
                @assert add_edge!(G, vertex_id, candidate)
                set_prop!(G, vertex_id, candidate, :length_m, distance)
                set_prop!(G, vertex_id, candidate, :this_class, centroid_connector)

                # and the back edge
                # back edges (to destinations) go to a different set of vertices, to prevent the router from taking "shortcuts"
                # through centroid connectors
                back_vertex_id = od_vertex_offset + n_ods + i
                @assert add_edge!(G, candidate, back_vertex_id)
                set_prop!(G, candidate, back_vertex_id, :length_m, distance)
                set_prop!(G, candidate, back_vertex_id, :this_class, centroid_connector)
            end
        end

        @assert linked
    end

    @info "initializing edge weights"
    compute_inital_weights!(G, args["centroid-connector-speed-kph"])

    cost_func_name = lowercase(args["cost-function"])
    if cost_func_name == "scag"
        cost_func = scag_bpr_traversal
    else
        error("Unknown cost function $cost_func. Available cost functions: scag")
    end

    @info "performing assignment"
    raw_segment_flows, raw_turn_flows = assign_frankwolfe!(G, odmat, od_vertex_offset, cost_func, rel_gap_tol=args["relative-gap-tolerance"])

    # compile results
    @info "writing results"
    segment_id = Vector{Int64}()
    target_segment_id = Vector{Int64}()
    segment_flow = Vector{Float64}()
    turn_flow = Vector{Float64}()
    freeflow_traversal_time_secs = Vector{Float64}()
    turn_costs = Vector{Float64}()
    congested_travel_time_secs = Vector{Float64}()

    sizehint!(segment_id, ne(G))
    sizehint!(target_segment_id, ne(G))
    sizehint!(segment_flow, ne(G))
    sizehint!(turn_flow, ne(G))
    sizehint!(freeflow_traversal_time_secs, ne(G))
    sizehint!(congested_travel_time_secs, ne(G))
    sizehint!(turn_costs, ne(G))

    for edge in edges(G)
        eidx = get_prop(G, edge, :idx)::Int64
        push!(segment_id, edge.src)
        push!(target_segment_id, edge.dst)
        push!(segment_flow, raw_segment_flows[edge.src])
        push!(turn_flow, raw_turn_flows[eidx])
        push!(freeflow_traversal_time_secs, get_prop(G, edge, :ff_traversal_time)::Float64)
        push!(turn_costs, get_prop(G, edge, :turn_cost)::Float64)
        push!(congested_travel_time_secs, get_prop(G, edge, :weight)::Float64)  # this will have been updated in the assignment process
    end

    out = DataFrame(
        segment_id = segment_id,
        target_segment_id = target_segment_id,
        segment_flow = segment_flow,
        turn_flow = turn_flow,
        freeflow_traversal_time_secs = freeflow_traversal_time_secs,
        turn_costs = turn_costs,
        congested_travel_time_secs = congested_travel_time_secs
    )

    CSV.write(args["output"], out)
end

main()