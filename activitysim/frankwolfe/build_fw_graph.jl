# Build a Franke-Wolfe graph from OSM data

using MetaGraphs
using LightGraphs
using OSMPBF
using ArgParse
using DataStructures
using Geodesy
using Serialization
using CSV
using DataFrames
using CodecZlib

const MILES_TO_KILOMETERS = 1.609344
const KNOTS_TO_KMH = 1.852

include("compute_heading.jl")
include("fw_types.jl")

const hwytags = Set(["motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link", "secondary", "secondary_link", "tertiary_link", "unclassified"])

function parse_args()
    s = ArgParseSettings()
    @add_arg_table s begin
        "osm_pbf"
            help = "An OpenStreetMap .pbf file to process"
        "network"
            help = "An output file to write the network to (extension .fwgr)"
        "--save-names"
            help = "Save a sidecar file with street names"
            action = :store_true
        "--save-geometries"
            help = "Save a sidecar file with way geometries"
            action = :store_true
    end
    return ArgParse.parse_args(s)
end

function save_geoms(way_segments, node_geoms, filename)
    # store the geometries for later use in visualization
    geoms::Vector{Vector{NodeAndCoord}} = map(way_segments) do ws
        map(ws.nodes) do nid
            geom = node_geoms[nid]::LLA
            return NodeAndCoord(nid, geom.lat, geom.lon)
        end
    end

    serialize(filename, geoms)
end

function parse_max_speed(speed_text)::Union{Float64, Missing}
    try
        return parse(Float64, speed_text)
    catch
        # not a raw km/hr number
        mtch = match(r"([0-9]+)(?: ?)([a-zA-Z/]+)", speed_text)
        if isnothing(mtch)
            @warn "unable to parse speed limit $speed_text"
            return missing
        else
            speed_scalar = parse(Float64, mtch.captures[1])
            units = lowercase(mtch.captures[2])

            if (units == "kph" || units == "km/h" || units == "kmph")
                return speed_scalar
            elseif units == "mph"
                return speed_scalar * MILES_TO_KILOMETERS
            elseif units == "knots"
                return speed_scalar * KNOTS_TO_KMH
            else
                @warn "unknown speed unit $units"
                return missing
            end
        end
    end
end

function main()
    args = parse_args()
    pbf = args["osm_pbf"]::String
    outf = args["network"]::String
    save_names = args["save-names"]::Bool
    save_geom = args["save-geometries"]::Bool

    # find all nodes that occur in more than one way
    node_count = counter(Int64)

    @info "Pass 1: find intersection nodes"
    n_ways::UInt32 = 0
    scan_pbf(
        pbf, 
        ways=w -> begin
            if haskey(w.tags, "highway") && in(w.tags["highway"], hwytags)
                n_ways += 1
                for node in w.nodes
                    inc!(node_count, node)
                end
            end
        end
    )

    @info "..parsed $n_ways ways"

    # now retain all intersection nodes
    intersection_nodes = Set{Int64}()
    # just a guess but preallocate a bunch of space
    sizehint!(intersection_nodes, n_ways * 3)
    for (nidx, n_refs) in node_count
        if n_refs >= 2
            push!(intersection_nodes, nidx)
        end
    end

    @info "..found $(length(intersection_nodes)) intersection nodes"

    @info "Pass 2: read intersection and other highway nodes"
    node_geom = Dict{Int64, LLA}()
    traffic_signal_nodes = Set{Int64}()
    scan_pbf(
        pbf,
        nodes=n -> begin
            if haskey(node_count, n.id)
                # lat lon, not lon lat, and we're not using altitude
                # LA is not that far above sea level anyways...
                node_geom[n.id] = LLA(n.lat, n.lon, 0)

                if haskey(n.tags, "highway") && n.tags["highway"] == "traffic_signals"
                    push!(traffic_signal_nodes, n.id)
                end
            end
        end
    )

    # save memory
    # TODO will this create type instability?
    node_count = nothing

    @info "Pass 3: re-read and catalog ways"
    way_segments = Vector{WaySegment}()
    way_segments_by_start_node = DefaultDict{Int64, Vector{Int64}}(Vector{Int64})
    way_segments_by_end_node = DefaultDict{Int64, Vector{Int64}}(Vector{Int64})

    sizehint!(way_segments, n_ways * 2)

    if save_names
        way_segment_names = Vector{String}()
        sizehint!(way_segment_names, n_ways * 2)
    end

    scan_pbf(
        pbf,
        ways=w -> begin
            if haskey(w.tags, "highway") && in(w.tags["highway"], hwytags)
                seg_length::Float64 = 0
                origin_node::Int64 = w.nodes[1]
                heading_start::Float32 = NaN32
                heading_end::Float32 = NaN32
                traffic_signal::Int32 = 0
                back_traffic_signal::Int32 = 0

                rclass = get_road_class(w.tags["highway"])

                # figure out one-way
                oneway = false
                if haskey(w.tags, "oneway")
                    owt = w.tags["oneway"]
                    if (owt == "yes" || owt == "1" || owt == "true")
                        oneway = true
                    elseif (owt == "-1" || owt == "reverse")
                        oneway = true
                        # don't try to have a separate codepath for reversed, just reverse the nodes
                        # it's okay to be destructive, we aren't using this way object again
                        # this might not be the most efficient but reverse oneways are rare
                        reverse!(w.nodes)
                    end
                end

                if haskey(w.tags, "name")
                    name = w.tags["name"]
                else
                    hwy = w.tags["highway"]
                    name = "unnamed $(hwy)"
                end

                # store number of lanes, if present
                lanes_per_direction::Union{Int64, Missing} = missing
                if haskey(w.tags, "lanes")
                    try
                        lanes_per_direction = parse(Int64, w.tags["lanes"])
                        if !oneway
                            lanes_per_direction ÷= 2
                        end
                    catch
                        lanes_str = w.tags["lanes"]
                        @warn "could not parse lanes values $lanes_str"
                        lanes_per_direction = missing
                    end
                end

                # store max speed, if present
                maxspeed::Union{Float64, Missing} = missing
                if haskey(w.tags, "maxspeed")
                    maxspeed = parse_max_speed(w.tags["maxspeed"])
                end

                accumulated_nodes = Vector{Int64}()
                push!(accumulated_nodes, w.nodes[1]) # initialize with first node

                for idx in 2:length(w.nodes)
                    this_node = w.nodes[idx]
                    this_node_geom = node_geom[this_node]
                    prev_node = w.nodes[idx - 1]
                    prev_node_geom = node_geom[prev_node]

                    push!(accumulated_nodes, this_node)

                    if in(this_node, traffic_signal_nodes)
                        traffic_signal += 1
                    end

                    # gotta keep track of it the other way as well, b/c the traffic signals are on different nodes going the other way
                    if in(prev_node, traffic_signal_nodes)
                        back_traffic_signal += 1
                    end

                    seg_length += euclidean_distance(prev_node_geom, this_node_geom)

                    if idx == 2
                        # special case at start of way: compute heading
                        heading_start = compute_heading(prev_node_geom, this_node_geom)
                    end

                    if (idx == length(w.nodes) || in(this_node, intersection_nodes))
                        # save this way segment and start a new one
                        heading_end = compute_heading(prev_node_geom, this_node_geom)
                        # TODO figure out one-way
                        ws = WaySegment(
                            origin_node,
                            this_node,
                            w.id,
                            heading_start,
                            heading_end,
                            convert(Float32, seg_length),
                            rclass,
                            oneway,
                            traffic_signal,
                            back_traffic_signal,
                            lanes_per_direction,
                            maxspeed,
                            accumulated_nodes
                        )
                        push!(way_segments, ws)

                        # index it by node
                        wsidx = length(way_segments)
                        push!(way_segments_by_start_node[origin_node], wsidx)
                        push!(way_segments_by_end_node[this_node], wsidx)

                        if save_names
                            # note that there will be many adjacent identical values, which doesn't matter b/c the
                            # names file gets gzipped
                            push!(way_segment_names, name)
                        end

                        # prepare for next iteration
                        if idx < length(w.nodes)
                            origin_node = this_node
                            heading_start = compute_heading(this_node_geom, node_geom[w.nodes[idx + 1]])
                            seg_length = 0
                            traffic_signal = 0
                            back_traffic_signal = 0
                            accumulated_nodes = Vector{Int64}()
                        end
                    end
                end
            end
        end
    )

    @info "merging ways with no intersections"
    removed_way_segments = Set{Int64}()

    function merge_way_segment!(ws)
        # check if this is a straight-through node, i.e. indegree = 1, outdegree = 1
        # note that this still will not merge a situation where you have ways like ---->*<---- which could be merged if not one-way
        # but this is a good start anyhow
        if length(way_segments_by_start_node[ws.destination_node]) == 1 && length(way_segments_by_end_node[ws.destination_node]) == 1
            # if this is the only way to access this way, there shouldn't be other ways that it has already been merged with
            ws2ix = way_segments_by_start_node[ws.destination_node][1]
            @assert !in(ws2ix, removed_way_segments)

            ws2 = way_segments[ws2ix]
            # don't create loops
            if (ws2.destination_node != ws.origin_node) && (ws.oneway == ws2.oneway)
                @assert ws.destination_node == ws2.origin_node "Indexing error, way segments indexed as connected are in fact not"
                # extend this way segment
                ws.destination_node = ws2.destination_node
                ws.heading_end = ws2.heading_end
                ws.length_m += ws2.length_m
                ws.traffic_signal += ws2.traffic_signal
                ws.back_traffic_signal += ws2.back_traffic_signal
                # minimum number of lanes of either segment, unless one is missing
                ws.lanes = ismissing(ws.lanes) ? ws2.lanes : (ismissing(ws2.lanes) ? ws.lanes : min(ws.lanes, ws2.lanes))
                # same logic for speed
                ws.speed_kmh = ismissing(ws.speed_kmh) ? ws2.speed_kmh : (ismissing(ws2.speed_kmh) ? ws.speed_kmh : min(ws.speed_kmh, ws2.speed_kmh))

                # stick the geometries together, but not the duplicated node in the middle
                append!(ws.nodes, @view ws2.nodes[2:length(ws2.nodes)])

                # mark ws2 as removed
                push!(removed_way_segments, ws2ix)

                # and try to keep extending
                merge_way_segment!(ws)
            end
        end
    end

    for (wsidx, ws) in enumerate(way_segments)
        # don't call merge_way_segment on way segment that already been merged with a previous
        # segment
        if !in(wsidx, removed_way_segments)
            merge_way_segment!(ws)
        end
    end

    @info "merged $(length(removed_way_segments)) segments"

    @info "expanding bidirectional edges to unidirectional edges"
    new_way_segments = Vector{WaySegment}()
    sizehint!(new_way_segments, length(way_segments))

    if save_names
        new_way_segment_names = Vector{String}()
        sizehint!(new_way_segment_names, length(way_segments))
    end

    for (i, ws) in enumerate(way_segments)
        if !in(i, removed_way_segments)
            push!(new_way_segments, ws)
            if save_names
                name = way_segment_names[i]
                push!(new_way_segment_names, name)
            end

            if !ws.oneway
                # two-way street, add a back edge
                back = WaySegment(
                    ws.destination_node,
                    ws.origin_node,
                    ws.way_id,
                    circular_add(ws.heading_end, 180),
                    circular_add(ws.heading_start, 180),
                    ws.length_m,
                    ws.class,
                    ws.oneway,
                    ws.back_traffic_signal,
                    ws.traffic_signal,
                    ws.lanes,
                    ws.speed_kmh,
                    reverse(ws.nodes)
                )

                push!(new_way_segments, back)
                if save_names
                    push!(new_way_segment_names, name)
                end
            end
        end
    end

    way_segments = new_way_segments
    way_segment_names = new_way_segment_names

    @info "non-edge-based graph has $(length(way_segments)) edges"

    if save_names
        @assert length(way_segments) == length(way_segment_names) "way segments and names do not have same length!"
    end

    @info "reindexing segments for edge-based graph construction"
    way_segments_by_end_node = nothing  # should not be used anymore
    empty!(way_segments_by_start_node)
    for (wsidx, ws) in enumerate(way_segments)
        push!(way_segments_by_start_node[ws.origin_node], wsidx)
    end

    @info "creating edge-based graph"
    # confusing, but this is an edge-based graph - one vertex per _way segment_, and the numbers
    # are parallel to the vector way_segments
    G = MetaDiGraph(length(way_segments))

    for (srcidx, way_segment) in enumerate(way_segments)
        # set the location of this way segment vertex to be the start of the way - used for snapping
        # in snapping, we will still be able to snap to the end of a cul-de-sac because of the back edge,
        # unless it is a one-way cul-de-sac... cf. https://github.com/conveyal/r5/blob/dev/src/main/java/com/conveyal/r5/streets/TarjanIslandPruner.java
        set_prop!(G, srcidx, :geom, node_geom[way_segment.origin_node])

        # find all of the way segments this way segment is connected to
        for tgtidx in way_segments_by_start_node[way_segment.destination_node]
            # figure out if this is a straight-on or turn action
            tgtseg = way_segments[tgtidx]

            Δhdg = heading_between(way_segment.heading_end, tgtseg.heading_start)

            # Δhdg should now be the angle from the entry heading to the exit heading
            # if |Δhdg| < 45, we call it straight-on
            # if Δhdg > 45, right turn
            # if Δhdg < 45, left turn
            # if Δhdg > 45
            #     turn_dir = right
            # elseif Δhdg < -45
            #     turn_dir = left
            # else
            #     turn_dir = straight
            # end

            # error if adding edge fails
            @assert add_edge!(G, srcidx, tgtidx)
            
            # set the edge metadata
            set_prop!(G, srcidx, tgtidx, :length_m, way_segment.length_m)
            set_prop!(G, srcidx, tgtidx, :this_class, way_segment.class)
            set_prop!(G, srcidx, tgtidx, :next_class, tgtseg.class)
            set_prop!(G, srcidx, tgtidx, :turn_angle, Δhdg)
            set_prop!(G, srcidx, tgtidx, :traffic_signal, way_segment.traffic_signal)
            set_prop!(G, srcidx, tgtidx, :speed_kmh, way_segment.speed_kmh)
            set_prop!(G, srcidx, tgtidx, :lanes, way_segment.lanes)
            set_prop!(G, srcidx, tgtidx, :oneway, way_segment.oneway)
        end
    end

    @info "writing graph"
    serialize(outf, G)

    if save_names
        @info "writing names"
        name_file = "$outf.names.gz"

        # use CSV module to save a vector of names (1 column, n rows) so that we don't have to handle quoting ourselves
        # https://stackoverflow.com/questions/52900232/export-an-array-to-a-csv-file-in-julia
        open(name_file, "w") do raw_stream
            gzstream = GzipCompressorStream(raw_stream)
            CSV.write(gzstream, DataFrame(nm=way_segment_names), writeheader=false)
            close(gzstream)
        end
    end

    if save_geom
        save_geoms(new_way_segments, node_geom, "$outf.geoms")
    end
end

main()