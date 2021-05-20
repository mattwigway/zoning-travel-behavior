# load trips onto the network
using LightGraphs
using MetaGraphs
using ProgressBars
using ForwardDiff

MIN_λ = 1e-5

# NB b/c we are using a turn-based graph, "segment" refers to vertices herein

struct AssignmentState
    current_segment_flows::Vector{Float64}
    current_turn_flows::Vector{Float64}
    all_or_nothing_segment_flows::Vector{Float64}
    all_or_nothing_turn_flows::Vector{Float64}
end

AssignmentState(G::MetaDiGraph) = AssignmentState(
    zeros(Float64, nv(G)),
    zeros(Float64, ne(G)),
    zeros(Float64, nv(G)),
    zeros(Float64, ne(G))
)

function copy_state!(dst::AssignmentState, src::AssignmentState)::AssignmentState
    copy!(dst.current_segment_flows, src.current_segment_flows)
    copy!(dst.current_turn_flows, src.current_turn_flows)
    copy!(dst.all_or_nothing_segment_flows, src.all_or_nothing_segment_flows)
    copy!(dst.all_or_nothing_turn_flows, src.all_or_nothing_turn_flows)
    return dst
end

# G is graph, odmat is o-d matrix, dest_offset is the index of the first origin/destination, costfunc is a cost function
# this is a bit of an odd function, as it will mutate the graph costs, but also returns the vertex (i.e. segment, remember, edge
# /turn based graph) level flows
function assign_frankwolfe!(G, odmat, dest_offset, costfunc; maxiter::Int64 = 100000, rel_gap_tol::Float64 = 0.0001)
    @info "indexing graph edges"
    for (i, edge) in enumerate(edges(G))
        set_prop!(G, edge, :idx, i)
    end

    state = AssignmentState(G)
    old_state = AssignmentState(G)

    @info "initial all-or-nothing assignment"
    # in Ortuzar and Willumsen they don't have an initial assignment, but they also have to do the all
    # or nothing assignment twice, implicitly, once explicitly and once to compute relgap
    all_or_nothing!(G, odmat, state)

    # And in order to make sure that all flows are preserved, I believe we need to set current flows to the
    # all-or-nothing assignments (rather than zero), because otherwise every successive step will still have a
    # little of the initial zero mixed in, and the total flows won't sum up.
    copy!(state.current_segment_flows, state.all_or_nothing_segment_flows)
    copy!(state.current_turn_flows, state.all_or_nothing_turn_flows)

    λ = 0.5

    for iter in 1:maxiter
        @info "assignment: begin iteration $iter"   

        λ = find_optimal_λ(G, state, λ, costfunc)

        @info "optimal λ = $λ"

        @info "updating flows and costs"
        update_flows_and_costs!(G, λ, state, costfunc)

        @info "..all-or-nothing assignment"
        # In Ortuzar and Willumsen, they say to just compute the relative gap after updating the flows and
        # costs, but gloss over that you need all or nothing flows given current costs to be the lower bound
        # so do the all or nothing assignment here, and save it for the next iteration
        all_or_nothing!(G, odmat, state)

        rel_gap = compute_rel_gap(G, state)

        if rel_gap <= rel_gap_tol
            @info "assignment CONVERGED after $(iter + 1) iterations! relative gap $rel_gap <= $rel_gap_tol"
            break
        elseif iter == maxiter
            @error "assignment FAILED TO CONVERGE in maximum $maxiter iterations! final relative gap: $rel_gap"
            break
        else
            @info "relative gap: $rel_gap > $rel_gap_tol"
            continue
        end
    end

    return state.current_segment_flows, state.current_turn_flows
end

function all_or_nothing!(G::MetaDiGraph, odmat::Array{Float64, 2}, state::AssignmentState)
    # perform all-or-nothing assignment with current costs
    # split-apply-combine - allocate an array per thread and sum them up at the end
    # todo could allocate these per-thread flows once, but let's not optimize prematurely
    # threads is the second dimension for ease of summation below
    # this is a little more complicated than a stardard Frank-Wolfe algorithm b/c we have to track segment/vertex
    # flows which are used to calculate costs for each turn, as well as turn/edge flows because they are
    # used in the relative gap metric
    dest_offset = get_prop(G, :od_vertex_offset)::Int64

    per_thread_segment_flows = zeros(Float64, (length(state.all_or_nothing_segment_flows)), Threads.nthreads())
    per_thread_turn_flows = zeros(Float64, (length(state.all_or_nothing_turn_flows), Threads.nthreads()))
    dest_vertices = (dest_offset + size(odmat, 1) + 1):(dest_offset + size(odmat, 1) + size(odmat, 1))
    @info "....using $(Threads.nthreads()) threads for all-or-nothing assignment"
    Threads.@threads for (originix, origin) in ProgressBar(collect(enumerate((dest_offset + 1):(dest_offset + size(odmat, 1)))))
        thread = Threads.threadid()
        
        # run a Dijkstra search on the graph
        # nb this or the enumeration is creating excessive GC. rewrite to save less data (e.g. record
        # flow during path tracing, don't trave all paths and then record flow
        djstate = dijkstra_shortest_paths(G, [origin], allpaths=true)
        # indexed not by vertex index, but by vertex position in dest_vertices
        # this is extremely similar to enumerate paths in
        # https://github.com/JuliaGraphs/LightGraphs.jl/blob/master/src/shortestpaths/bellman-ford.jl#L113
        # but with fewer allocations
        for (destix, dest) in enumerate(dest_vertices)
            n_trips = odmat[originix, destix]
            if n_trips > 0
                current_vertex = dest

                @assert  djstate.parents[current_vertex] != 0 && djstate.parents[current_vertex] != current_vertex """
                No path from origin $originix to destination $dest
                $n_trips trips cannot be completed
                consider using --single-strong-component
                """

                while (djstate.parents[current_vertex] != 0 && djstate.parents[current_vertex] != current_vertex)
                    parent = djstate.parents[current_vertex]
                    eidx = get_prop(G, parent, current_vertex, :idx)::Int64
                    per_thread_turn_flows[eidx, thread] += n_trips
                    per_thread_segment_flows[parent, thread] += n_trips
                    current_vertex = parent
                end
            end
        end
    end

    # all or nothing flows is column vector, sum! does rowwise sum
    sum!(state.all_or_nothing_segment_flows, per_thread_segment_flows)
    sum!(state.all_or_nothing_turn_flows, per_thread_turn_flows)
end

# https://sboyles.github.io/teaching/ce392c/5-beckmannmsafw.pdf is a very useful reference for this
function find_optimal_λ(G, state, λ, costfunc)
    # newton's method
    prev_λ = -1
    while true
        objval = aggregate_cost(G, state, λ, costfunc)
        
        if abs(objval) < 1e-5 || abs(λ - prev_λ) < 1e-5
            return λ
        end
        
        drv_objval = ForwardDiff.derivative(x -> aggregate_cost(G, state, x, costfunc), λ)

        prev_λ = λ
        λ = clamp(λ - objval / drv_objval, MIN_λ, 1)
    end
end

# return the FW objective function value for the current and all-or-nothing flows in state, and the given λ
function aggregate_cost(G, state, λ, costfunc)
    obj = 0.0
    for edge in edges(G)
        # x̂ in presentation
        current_segment_flow = λ * state.all_or_nothing_segment_flows[edge.src] + (1 - λ) * state.current_segment_flows[edge.src]
        turn_cost = costfunc(G, edge, current_segment_flow)  # costs based on _segment_ flow

        eidx = get_prop(G, edge, :idx)::Int64
        last_turn_flow = state.current_turn_flows[eidx]
        aon_turn_flow = state.all_or_nothing_turn_flows[eidx]
        obj += turn_cost * (aon_turn_flow - last_turn_flow)
    end

    return obj
end

function update_flows_and_costs!(G::MetaDiGraph, λ::Float64, state::AssignmentState, costfunc)

    # first update the flows
    # need to do this before costs since costs depend on flows not only on the same link
    # okay to update segment and turn flows separately. as long as they sum at the start, they will still
    # sum after a linear transformation
    for i in 1:length(state.current_segment_flows)
        state.current_segment_flows[i] = λ * state.all_or_nothing_segment_flows[i] + (1 - λ) * state.current_segment_flows[i]
    end

    for i in 1:length(state.current_turn_flows)
        state.current_turn_flows[i] = λ * state.all_or_nothing_turn_flows[i] + (1 - λ) * state.current_turn_flows[i]
    end

    for edge in edges(G)
        flow = state.current_segment_flows[edge.src]

        # costs are computed based on flow on the _segment_
        edge_cost = costfunc(G, edge, flow)::Float64

        # 0 cost could be okay, if the segment has no length (data issue), but negative costs are definitely a no-no
        @assert edge_cost >= 0.0

        # update the edge costs and flows
        set_prop!(G, edge, :weight, edge_cost)
    end
end

function compute_rel_gap(G, state)
    agg_current_flows::Float64 = 0.0
    agg_aon_flows::Float64 = 0.0

    for e in edges(G)
        eidx = get_prop(G, e, :idx)::Int64
        current_turn_flow = state.current_turn_flows[eidx]
        aon_turn_flow = state.all_or_nothing_turn_flows[eidx]
        turn_cost = get_prop(G, e, :weight)::Float64

        agg_current_flows += current_turn_flow * turn_cost
        agg_aon_flows += aon_turn_flow * turn_cost
    end

    return (agg_current_flows - agg_aon_flows) / agg_current_flows
end