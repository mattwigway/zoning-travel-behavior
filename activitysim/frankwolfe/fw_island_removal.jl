# Remove all but the largest strongly connected componen

using ArgParse
using Serialization
using LightGraphs
using MetaGraphs
using CodecZlib
using CSV
using DataFrames
using Geodesy

include("fw_types.jl")

function main()
    s = ArgParseSettings()

    @add_arg_table! s begin
        "in_graph"
            help = "Graph produced by build_fw_graph.jl"
        "out_graph"
            help = "Output graph with all but one strongly connected component removed"
    end

    args = parse_args(s)

    inf = args["in_graph"]
    out = args["out_graph"]

    G = deserialize(inf)

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

    # before removing vertices, mark the ones we're keeping with their old indices
    for (i, v) in enumerate(vertices(G))
        set_prop!(G, v, :orig_idx, i)
    end

    @info "Retaining largest strong component, with $largest_component_size vertices"
    @info "Removing all other strong components, all with $next_largest_component_size or fewer vertices"

    for (i, component) in enumerate(components)
        if i != largest_component_idx
            for v in component
                rem_vertex!(G, v)
            end
        end
    end

    # this vector maps the new vertex IDs to the old ones
    orig_vertex_ids = Vector{Int64}()
    sizehint!(orig_vertex_ids, largest_component_size)
    
    for v in vertices
        push!(orig_vertex_ids, get_prop(G, v, :orig_idx))
        rem_prop!(G, v, :orig_idx)
    end

    @info "saving output"
    serialize(out, G)

    if isfile("$inf.names.gz")
        @info "writing names"
        open("$inf.names.gz") do comp_stream
            gzin = GzipDecompressorStream(comp_stream)
            names = CSV.File(gzin, header=false) |> DataFrame
            close(gzin)
        end

        names = names[orig_vertex_ids, :]

        open("$out.names.gz") do raw_stream
            gzout = GzipCompressorStream(raw_stream)
            write_csv(gzout, names, writeheader=false)
            close(gzout)
        end
    end

    if isfile("$inf.geoms")
        @info "writing geoms"
        geoms = deserialize("$inf.geoms")::Vector{Vector{NodeAndCoord}}
        serialize("$out.geoms", geoms[orig_vertex_ids])
    end
end

main()