@enum RoadClass motorway motorway_link trunk trunk_link primary primary_link secondary secondary_link tertiary tertiary_link unclassified centroid_connector

function get_road_class(cln::String)
    if cln == "motorway"
        return motorway
    elseif cln == "motorway_link"
        return motorway_link
    elseif cln == "trunk"
        return trunk
    elseif cln == "trunk_link"
        return trunk_link
    elseif cln == "primary"
        return primary
    elseif cln == "primary_link"
        return primary_link
    elseif cln == "secondary"
        return secondary
    elseif cln == "secondary_link"
        return secondary_link
    elseif cln == "tertiary"
        return tertiary
    elseif cln == "tertiary_link"
        return tertiary_link
    elseif cln == "unclassified"
        return unclassified
    elseif cln == "centroid_connector"
        return centroid_connector
    else
        error("unknown highway class $cln")
    end
end

@enum TurnType left right straight

mutable struct WaySegment
    origin_node::Int64
    destination_node::Int64
    way_id::Int64
    heading_start::Float32
    heading_end::Float32
    length_m::Float32
    class::RoadClass
    oneway::Bool
    traffic_signal::Int32  # number of traffic signals on this way, _not including at first node_
    back_traffic_signal::Int32 # number of traffic signals on this way, _not including at last node_
    lanes::Union{Int64, Missing}
    speed_kmh::Union{Float64, Missing}
    # can't be packed, oh well - we're not serializing anyhow
    nodes::Vector{Int64}
end

struct NodeAndCoord
    id::Int64
    lat::Float64
    lon::Float64
end