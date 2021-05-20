# cost functions for network loading
using MetaGraphs

# https://scag.ca.gov/sites/main/files/file-attachments/scag_rtdm_2012modelvalidation.pdf?1605571641 page 9-1
# note for ones differentiated by area type I'm just using areas 1-5. 6 and 7 are rural and mountain areas
const SCAG_BPR_ALPHA = 0.6
const SCAG_BPR_BETA_FWY = 8.0
const SCAG_BPR_BETA_EXWY_45MPH_OR_LESS = 5.0
const SCAG_BPR_BETA_EXWY_OVER_45MPH = 8.0
const SCAG_BPR_BETA_OTHER = 5.0

const DEFAULT_LANES_PER_DIRECTION_BY_FACILITY_TYPE = Dict(
    motorway => 4,
    motorway_link => 1,
    trunk => 3,
    trunk_link => 1,
    primary => 2,
    primary_link => 1,
    secondary => 2,
    secondary_link => 1,
    tertiary => 1,
    tertiary_link => 1,
    unclassified => 1
)

function get_number_of_lanes_per_direction(G, edge, rclass)
    # Note that even though this function takes an edge because that is how the data are indexed, the number of lanes
    # per direction are for all edges departing from a node (i.e. the left turn, right turn, and straight actions might
    # all say 3 lanes when one is a left-turn lane, and one is a right-turn lane.
    lanes = get_prop(G, edge, :lanes)::Union{Int64, Missing}
    if ismissing(lanes) || lanes <= 0
        lanes = DEFAULT_LANES_PER_DIRECTION_BY_FACILITY_TYPE[rclass]
    end
    return lanes
end

function get_capacity(rclass, lanes::Integer, oneway::Bool, speed_mph)
    # get the capacity of a link (not per-lane) based on its characteristics
    # using the tables from Table 4-3 in the SCAG model document, but heavily simplified - just the per-lane capacity
    # of an arterial crossing another arterial of the same size in AT5_Suburban with signals less than 2 miles apart
    # and for ramps I assume suburban as well
    @assert lanes >= 1
    if speed_mph <= 0
        @warn "unexpected speed $speed_mph mph"
    end
    cap = -1
    if rclass === motorway
        if speed_mph <= 55 cap = 1900 * lanes
        elseif speed_mph >= 70 cap = 2100 * lanes
        else cap = 2000 * lanes end
    elseif (rclass == trunk || rclass === primary || rclass === secondary || rclass === tertiary || rclass === unclassified)
        # we have lanes per direction, I think SCAG uses lanes total
        if lanes == 1 cap = 675 * lanes
        elseif lanes >= 2 cap = 750 * lanes
        else error("Invalid lane spec") end

        if oneway
            cap *= 1.2
        end
    elseif (rclass == motorway_link || rclass == trunk_link || rclass == primary_link || rclass == secondary_link || rclass == tertiary_link)
        cap = 1400 + 600 * (lanes - 1)
    else
        error("Unrecognized road class $rclass")
    end

    return cap
end

# basically copied from site listed above
# note that flow is _hourly_
function scag_bpr_traversal(G::MetaDiGraph, edge, flow)
    rclass = get_prop(G, edge, :this_class)::RoadClass
    if rclass == centroid_connector
        # no congestion or turn costs on centroid connectors
        return get_prop(G, edge, :ff_traversal_time)::Float64
    end

    next_rclass = get_prop(G, edge, :next_class)::RoadClass

    if rclass == motorway_link && next_rclass == motorway
        # this is a freeway on ramp (specifically not an off ramp)
        return scag_motorway_entrance_traversal(G, edge, flow)
    else
        return scag_general_traversal(G, edge, rclass, flow)
    end
end

function scag_general_traversal(G::MetaDiGraph, edge, rclass::RoadClass, flow)
    link_traversal_secs = get_prop(G, edge, :ff_traversal_time)::Float64
    turn_cost = get_prop(G, edge, :turn_cost)::Float64
    maxspeed_mph = get_freeflow_speed_kmh(G, edge) / MILES_TO_KILOMETERS

    if rclass === motorway
        β = SCAG_BPR_BETA_FWY
    elseif (rclass === trunk || rclass === primary)
        if maxspeed_mph > 45
            β = SCAG_BPR_BETA_EXWY_OVER_45MPH
        else
            β = SCAG_BPR_BETA_EXWY_45MPH_OR_LESS
        end
    else
        β = SCAG_BPR_BETA_OTHER
    end

    oneway = get_prop(G, edge, :oneway)::Bool
    lanes_per_direction = get_number_of_lanes_per_direction(G, edge, rclass)
    capacity = get_capacity(rclass, lanes_per_direction, oneway, maxspeed_mph)

    # if flow > 0
    #     @info "V/C ratio: $(flow / capacity)"
    # end

    # Only apply the traffic to the common part of the segment (common to all turns) so that the edge
    # costs remain independent. If we scale the turn costs based on the segment flows, the costs in
    # the network are no longer independent.
    # an alternate way to think about the graph we have here is that it is really just a
    # simplified representation of an edge+turn based graph, where every street segment has an edge, but the node at
    # the end is connected to other street networks by turn edges. The link traversal seconds is the between intersections
    # component of costs, and the turn cost is the component that would be on the turns. However, if we scale the turn cost portion
    # based on the flow on the total segment, the edges in our hyopthetical graph no longer have costs that are only a function of flow
    # on that edge. But if we don't scale the turn costs, it should be okay.
    return link_traversal_secs * (1 + SCAG_BPR_ALPHA * (flow / capacity)) + turn_cost
end

function scag_motorway_entrance_traversal(G::MetaDiGraph, edge, flow)
    length_mi = get_prop(G, edge, :length_m)::Float32 / 1000 / MILES_TO_KILOMETERS
    speed_mph = get_freeflow_speed_kmh(G, edge) / MILES_TO_KILOMETERS
    lanes = get_number_of_lanes_per_direction(G, edge, motorway_link)
    capacity = get_capacity(motorway_link, lanes, true, speed_mph)

    per_lane_flow = flow / lanes

    return length_mi / speed_mph + (per_lane_flow / 120 * 5.0 * (1.0 + flow / capacity)^8) / 60
end

