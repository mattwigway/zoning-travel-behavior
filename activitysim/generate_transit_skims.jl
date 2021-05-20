# TODO absolute path is ugly - figure out how to install TransitRouter.jl in path
include("/home/ubuntu/git/TransitRouter.jl/src/TransitRouter.jl")

using CSV
using DataFrames
using .TransitRouter
using .TransitRouter.OSRM
using Tables
using ProgressBars
using Parquet
using Dates
using Suppressor
using ArgParse

const FERRY_ROUTE_TYPE = 4
const WALK_ACCEGR_DIST = 2000 # meters
const DRIVE_ACCEGR_DIST = 20000 # meters

function get_cmd_args() 
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--network"
            help = ".trjl network file to use"
            arg_type = String
            required = true
        "--walk-osrm"
            help = "OSRM path to contraction-hierachies-enabled walking network"
            arg_type = String
            required = true
        "--drive-osrm"
            help = "OSRM path to contraction-hierachies-enabled walking network"
            arg_type = String
            required = true
        "--mode-id"     
            help = "ActivitySim/TM1 mode ID (e.g. TRN, LOC, LRF...)"
            arg_type = String
            required = true
        "--destinations"
            help = "CSV file with destinations (columns geoid, lat, lon)"
            arg_type = String
            required = true
    end

    return parse_args(s)
end

function main()
    args = get_cmd_args()

    @info "Reading destinations"
    # read destinations
    dest_df = DataFrame(CSV.File(args["destinations"], types=Dict(:geoid=>String)))

    # convert to trjl format
    destinations = map(d -> Coordinate(d.lat, d.lon), Tables.rows(dest_df))


    @info "Starting OSRM"
    walkosrm = start_osrm(args["walk-osrm"], "ch")
    driveosrm = start_osrm(args["drive-osrm"], "ch")

    time_periods = (
        ("EA", 4 * 3600),
        ("AM", 7 * 3600),
        ("MD", convert(Int32, 11.5 * 3600)),
        ("PM", 16 * 3600),
        ("EV", 21 * 2600)
    )

    modes = Dict(
        "LOC" => 3,
        "HVY" => 1,
        "LRF" => 0,
        "EXP" => 42,  # hacked these to have a different route type
        "COM" => 2,
        "TRN" => -1 # no key mode here
    )

    acc_egr = (
#        ("WLK", "WLK"),  # temporarily commented out, just recalc'ing DRV skims
        ("WLK", "DRV"),
        ("DRV", "WLK")
    )

    net = load_network(args["network"])

    @info "Linking destinations to network for egress mode drive (radius $(DRIVE_ACCEGR_DIST/1000)km)"
    drive_egress = find_egress_times(net, driveosrm, destinations, DRIVE_ACCEGR_DIST)

    @info "Linking destinations to network for egress mode walk (radius $(WALK_ACCEGR_DIST/1000)km)"
    walk_egress = find_egress_times(net, walkosrm, destinations, WALK_ACCEGR_DIST)

    for (acc, egr) in acc_egr
        @info "Access mode $acc, egress mode $egr"

        mode_egress = egr == "DRV" ? drive_egress : walk_egress
        access_osrm = acc == "DRV" ? driveosrm : walkosrm
        access_dist = acc == "DRV" ? DRIVE_ACCEGR_DIST : WALK_ACCEGR_DIST

        results = Vector{DataFrame}(undef, length(time_periods) * length(destinations))

        modeid::String = args["mode-id"]
        key_route_type::Int64 = modes[modeid]

        @info "Performing $(length(time_periods) * length(destinations)) searches"

        for (tpidx, tp) in enumerate(time_periods)
            time_period, departure_time = tp
            for (idx, origin) in collect(enumerate(Tables.rows(dest_df)))
                if idx % 100 == 0
                    @info "$acc $modeid $egr $tp: $idx / $(nrow(dest_df))"
                end

                request = StreetRaptorRequest(
                    Coordinate(origin.lat, origin.lon),
                    departure_time,
                    Date(2018, 1, 8),
                    access_dist,
                    5 * 1000 / 3600,  # 5 km/h
                    4
                )

                result = @suppress begin
                    street_raptor(net, access_osrm, request, mode_egress) 
                end

                if ismissing(result)
                    # no transit near origin
                    results[(tpidx - 1) * length(destinations) + idx] = DataFrame()
                else
                    total_ivt = Vector{Union{Int32, Missing}}()
                    key_ivt = Vector{Union{Int32, Missing}}()  # seconds on the mode matching this mode, e.g. commuter rail - which we allow access to by bus
                    walk_time_xfers = Vector{Union{Int32, Missing}}()
                    wait_time_xfers = Vector{Union{Int32, Missing}}()
                    initial_wait = Vector{Union{Int32, Missing}}()
                    walk_egress_time = Vector{Union{Int32, Missing}}()
                    walk_access_time = Vector{Union{Int32, Missing}}()
                    ferry_ivt = Vector{Union{Int32, Missing}}()
                    n_boardings = Vector{Union{Int32, Missing}}()
                    drive_time = Vector{Union{Int32, Missing}}()
                    drive_dist = Vector{Union{Int32, Missing}}()

                    sizehint!(total_ivt, length(destinations))
                    sizehint!(key_ivt, length(destinations))  # seconds on the mode matching this mode, e.g. commuter rail - which we allow access to by bus
                    sizehint!(walk_time_xfers, length(destinations))
                    sizehint!(wait_time_xfers, length(destinations))
                    sizehint!(initial_wait, length(destinations))
                    sizehint!(walk_egress_time, length(destinations))
                    sizehint!(walk_access_time, length(destinations))
                    sizehint!(n_boardings, length(destinations))
                    sizehint!(drive_dist, length(destinations))
                    sizehint!(drive_time, length(destinations))

                    for i in 1:length(destinations)
                        legs = trace_path(result, i)

                        if ismissing(legs)
                            push!(total_ivt, missing)
                            push!(key_ivt, missing)
                            push!(walk_time_xfers, missing)
                            push!(wait_time_xfers, missing)
                            push!(initial_wait, missing)
                            push!(walk_egress_time, missing)
                            push!(walk_access_time, missing)
                            push!(ferry_ivt, missing)
                            push!(n_boardings, missing)
                            push!(drive_dist, missing)
                            push!(drive_time, missing)
                        else
                            d_total_ivt::Int32 = 0
                            d_key_ivt::Int32 = 0
                            d_walk_time_xfers::Int32 = 0
                            d_wait_time_xfers::Int32 = 0
                            d_initial_wait::Int32 = 0
                            d_walk_egress_time::Int32 = 0
                            d_walk_access_time::Int32 = 0
                            d_ferry_ivt::Int32 = 0
                            d_n_boardings::Int32 = 0
                            d_drive_dist::Int32 = 0
                            d_drive_time::Int32 = 0

                            for (i, leg) in enumerate(legs)
                                if leg.type == TransitRouter.access
                                    duration = leg.end_time - leg.start_time

                                    if acc == "WLK"
                                        d_walk_access_time = duration
                                    elseif acc == "DRV"
                                        d_drive_time += duration
                                        d_drive_dist += leg.distance_meters
                                    else error("unrecognized access mode $acc") end
                                elseif leg.type == TransitRouter.transit
                                    leg_ivt = leg.end_time - leg.start_time
                                    d_total_ivt += leg_ivt

                                    rttyp = net.routes[leg.route].route_type

                                    if rttyp == key_route_type
                                        # todo have to separate express buses somehow...
                                        d_key_ivt += leg_ivt
                                    end

                                    if rttyp == FERRY_ROUTE_TYPE
                                        d_ferry_ivt += leg_ivt
                                    end

                                    prev_leg = legs[i - 1]

                                    leg_wait = leg.start_time - prev_leg.end_time
                                    if prev_leg.type == TransitRouter.access
                                        d_initial_wait = leg_wait
                                    else
                                        d_wait_time_xfers += leg_wait
                                    end

                                    d_n_boardings += 1
                                elseif leg.type == TransitRouter.transfer
                                    d_walk_time_xfers = leg.end_time - leg.start_time  # wait is handled in transit
                                elseif leg.type == TransitRouter.egress
                                    duration = leg.end_time - leg.start_time
                                    if egr == "WLK"
                                        d_walk_egress_time = duration
                                    elseif egr == "DRV"
                                        d_drive_time += duration
                                        d_drive_dist += leg.distance_meters
                                    end
                                else
                                    error("unhandled leg type")
                                end
                            end

                            push!(total_ivt, d_total_ivt)
                            push!(key_ivt, d_key_ivt)
                            push!(walk_time_xfers, d_walk_time_xfers)
                            push!(wait_time_xfers, d_wait_time_xfers)
                            push!(initial_wait, d_initial_wait)
                            push!(walk_egress_time, d_walk_egress_time)
                            push!(walk_access_time, d_walk_access_time)
                            push!(ferry_ivt, d_ferry_ivt)
                            push!(n_boardings, d_n_boardings)
                            push!(drive_time, d_drive_time)
                            push!(drive_dist, d_drive_dist)
                        end
                    end

                    # create the travel times from this origin to all others
                    res = DataFrame(
                        to_geoid = dest_df.geoid,
                        total_ivt = total_ivt,
                        key_ivt = key_ivt,
                        walk_time_xfers = walk_time_xfers,
                        wait_time_xfers = wait_time_xfers,
                        initial_wait = initial_wait,
                        walk_egress_time = walk_egress_time,
                        walk_access_time = walk_access_time,
                        n_boardings = n_boardings,
                        drive_time = drive_time,
                        drive_dist = drive_dist,
                        wait = initial_wait + wait_time_xfers
                    )

                    res[!, :from_geoid] .= origin.geoid
                    res[!, :time_period] .= time_period

                    results[(tpidx - 1) * length(destinations) + idx] = res
                end
            end
        end
        output = vcat(results...)

        write_parquet("transit_skims_$(acc)_$(modeid)_$(egr).parquet", output)
    end
end

main()