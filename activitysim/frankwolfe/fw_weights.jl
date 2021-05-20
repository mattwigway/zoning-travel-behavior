include("fw_types.jl")

const KMH_TO_MS = 1000 / 3600
# car.lua says the traffic light penalty is 2 deciseconds or 0.2 seconds, but that seems unreasonably
# short. Based on reading the code it seems likely that these are intended to be _decaseconds_ or
# tens of seconds, and the docs are simply wrong
# https://github.com/Project-OSRM/osrm-backend/issues/5989
const TRAFFIC_LIGHT_PENALTY_SECS = 20.0
const BASE_TURN_PENALTY = 7.5
# makes left turns more costly than right - for drive-on-left countries, set to
# 1/1.075
const TURN_BIAS = 1.075
const TURN_PENALTY = 7.5

# multiply all osm-provided maxspeeds by this
const FREEFLOW_SPEED_REDUCTION = 0.8

# copied from OSRM 5.24.0 car.lua
const freeflow_speeds_kmh = Dict(
    motorway        => 90,
    motorway_link   => 45,
    trunk           => 85,
    trunk_link      => 40,
    primary         => 65,
    primary_link    => 30,
    secondary       => 55,
    secondary_link  => 25,
    tertiary        => 40,
    tertiary_link   => 20,
    unclassified    => 25
)

function get_freeflow_speed_kmh(G::MetaDiGraph, edge)
    # read the speed limit if it was in OSM, otherwise use default
    speed_kmh = get_prop(G, edge, :speed_kmh)::Union{Float64, Missing}

    if ismissing(speed_kmh)
        rclass = get_prop(G, edge, :this_class)::RoadClass
        speed_kmh = freeflow_speeds_kmh[rclass]
    else
        speed_kmh *= FREEFLOW_SPEED_REDUCTION
    end

    return speed_kmh
end

# get the free flow weight (i.e. travel time, in seconds) for an edge
# this is heavily based on the car.lua profile from OSRM v5.24.0
function compute_freeflow_weight(G, edge, centroid_connector_speed_kmh)
    traversal_time_secs::Float64 = 0.0

    # first, compute traversal time for the segment
    rclass = get_prop(G, edge, :this_class)::RoadClass
    length_m = convert(Float64, get_prop(G, edge, :length_m))::Float64

    if rclass == centroid_connector
        # short-circuit evaluation
        return length_m / (centroid_connector_speed_kmh * KMH_TO_MS), 0
    end

    next_rclass = get_prop(G, edge, :next_class)

    # read the speed limit if it was in OSM, otherwise use default
    speed_kmh = get_freeflow_speed_kmh(G, edge)

    traffic_signal = get_prop(G, edge, :traffic_signal)::Int32

    traversal_time_secs += length_m / (speed_kmh * KMH_TO_MS)

    traversal_time_secs += TRAFFIC_LIGHT_PENALTY_SECS * traffic_signal

    # now the turn time at the _end_ of the segment (turn time at start of segment already accounted for)
    # TODO I need to add the concept of a "free flowing intersection" here where ways are split but there
    # is no physical intersection

    # first, determine if we even need a turn cost
    if ((rclass == motorway || rclass == motorway_link) &&
        (next_rclass == motorway || next_rclass == motorway_link) && (traffic_signal == 0))
        # no turn costs needed in the motorway system
        return traversal_time_secs, 0.0
    else
        # need to compute turn costs
        turn_cost = 0.0      

        turn_angle = get_prop(G, edge, :turn_angle)

        if turn_angle >= 0
            # copied directly from lua code, I don't understand the math, it's a sigmoid of some sort
            # 10 converts decaseconds to seconds
            turn_cost += 10 * TURN_PENALTY / (1 + exp( -((13 / TURN_BIAS) *  turn_angle/180 - 6.5*TURN_BIAS)))
        else
            turn_cost += 10 * TURN_PENALTY / (1 + exp( -((13 * TURN_BIAS) * -turn_angle/180 - 6.5/TURN_BIAS)))
        end
    end

    return traversal_time_secs, turn_cost
end

function compute_inital_weights!(G, centroid_connector_speed_kmh)
    for edge in edges(G)
        traversal_time, turn_cost = compute_freeflow_weight(G, edge, centroid_connector_speed_kmh)
        set_prop!(G, edge, :ff_traversal_time, convert(Float64, traversal_time))
        set_prop!(G, edge, :turn_cost, convert(Float64, turn_cost))
        set_prop!(G, edge, :weight, traversal_time + turn_cost)
    end
end