using LightGraphs
using MetaGraphs
using Geodesy
using Serialization
using DataFrames
using CSV

include("fw_types.jl")

function main()
    G = deserialize(ARGS[1])
    geoms = deserialize("$(ARGS[1]).geoms")

    source_id = Vector{Int64}()
    target_id = Vector{Int64}()
    length_m = Vector{Float32}()
    rclass = Vector{RoadClass}()
    geom = Vector{String}()

    for (i, e) in enumerate(edges(G))
        push!(source_id, e.src)
        push!(target_id, e.dst)
        push!(length_m, get_prop(G, e, :length_m))
        push!(rclass, get_prop(G, e, :this_class))
        coords = map(c -> "$(c.lon) $(c.lat)", geoms[e.src])
        # some edges missing first coord due to graph build bug
        firstcoord = get_prop(G, e.src, :geom)::LLA
        pushfirst!(coords, "$(firstcoord.lon) $(firstcoord.lat)")
        seg_geom = "LINESTRING(" * join(coords, ", ") * ")"
        push!(geom, seg_geom)
    end

    df = DataFrame(
        source_id  = source_id,
        target_id = target_id,
        length_m = length_m,
        rclass = rclass,
        geom = geom
    )

    CSV.write("$(ARGS[1]).meta.csv", df)
end

main()