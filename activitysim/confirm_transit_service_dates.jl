# confirm that all transit services/feeds have service on January 8, 2018

include("/Users/matthewc/git/TransitRouter.jl/src/TransitRouter.jl")

using .TransitRouter
using Dates

function main()
    @info "Loading network"
    net = load_network("../transit/all_transit.trjl")

    date = Date(2018, 1, 8)

    @info "Extracting services running"
    services_running = map(net.services) do s
        TransitRouter.is_service_running(s, date)
    end

    @info "$(sum(services_running)) services running"

    @info "Extracting agency IDs"
    service_id_for_service = Vector{Union{Missing, String}}(missing, length(net.services))

    for (svcid, svcidx) in net.serviceidx_for_id
        @assert ismissing(service_id_for_service[svcidx])
        service_id_for_service[svcidx] = svcid
    end

    @assert !any(ismissing.(service_id_for_service))

    # extract feed IDs
    feed_id_for_service = map(service_id_for_service) do svcid
        split(svcid, ':', limit=2)[1]
    end

    all_unique_ids = Set(feed_id_for_service)
    running_unique_ids = Set(feed_id_for_service[services_running])
    missing_unique_ids = setdiff(all_unique_ids, running_unique_ids)

    if length(missing_unique_ids) > 0
        @warn "The following feeds do not have service on chosen date\n" * join(missing_unique_ids, "\n")
    else
        @info "all feeds have service on chosen date"
    end
end

main()