# Usage:
# julia -t N_THREADS --project popsyn.jl probabilities.parquet hh.parquet proportions.parquet output.parquet

using Parquet
using SQLite
using DataFrames
using Printf
using Query
using StatsBase
using Random
using Profile
using Tables
using CategoricalArrays

function main()
    probs_file = ARGS[1]
    hh_file = ARGS[2]
    proportions_file = ARGS[3]
    output_file = ARGS[4]


    @printf("""
Starting population synthesis
Probabilities file: %s
    Household file: %s
  Proportions file: %s
       Output file: %s
           Threads: %d
""", probs_file, hh_file, proportions_file, output_file, Threads.nthreads())

    hh = DataFrame(read_parquet(hh_file))
    pqt = Parquet.File(probs_file)
    proportions = read_parquet(proportions_file) |> DataFrame
    hhwt = hh[:,[:serial,:hhwt]]

    dbpath, openf = mktemp()
    close(openf)
    @printf("Initializing temporary SQLite database at %s\n", dbpath)
    db = SQLite.DB(dbpath)

    load_to_sqlite(db, pqt, hhwt)

    pqt = nothing  # get rid of any caches, etc. on next gc

    # multithreading - this is tricky to make sure that the sampling is reproducible
    # Even if we seed a single RNG before calling draw_households, with threads there is no
    # guarantee of execution order so the results will be different between runs.
    # Thus, we first calculate seeds for every housing type in a single thread, and use those
    # seeds to seed private random number generators for each housing type. Thus housing
    # types can be processed in any order and still give the same answers
    phys_housing_types = unique(proportions.phys_housing)
    sort!(phys_housing_types) # ensure stable iteration order, since seeds are determined by iteration order
    rng = Random.MersenneTwister(387625)
    seeds = Random.rand(rng, UInt32, length(phys_housing_types))
    total = length(phys_housing_types)

    dbs = Vector{SQLite.DB}(undef, Threads.nthreads())

    # separate SQLite DBs for each thread as not doing this causes segfault,
    # I guess the DB objects are not threadsafe, but since we're only reading,
    # we can read from multiple threads with separate objects (or even multiple processes)
    # Amazing, theoretically we could even write to the file with these and somehow SQLite
    # handles concurrency, 
    for i in range(1, Threads.nthreads(), step=1)
        dbs[i] = SQLite.DB(dbpath)
    end

    results = Vector{DataFrame}(undef, length(phys_housing_types))

    # barrier function for optimization
    @time compute_results!(convert(Vector{String}, phys_housing_types)::Vector{String}, dbs, seeds, proportions, results)

    simulated_pop = vcat(results...)

    print("Synthesis completed, writing output\n")
    @printf("Drew %d households into synthetic population\n", nrow(simulated_pop))

    # Julia Parquet can't handle int8
    simulated_pop.uneq_choice = convert(Vector{Int64}, simulated_pop.uneq_choice)

    write_parquet(output_file, simulated_pop)

end

function load_to_sqlite(db::SQLite.DB, pqt::Parquet.File, hhwt::DataFrame)
    # batch read the file and add to SQLite
    bcc = BatchedColumnsCursor(pqt, reusebuffer=true)

    total = nrows(pqt)
    cumload = 0

    groups = nothing

    # this is disk-bound, so not much point in multithreading, I think
    @time for batch in bcc
        df = DataFrame(batch)
        # go ahead and add the weights now
        df = leftjoin(df, hhwt, on=:hh => :serial)
        
        df.phys_housing_type = map(h -> h[1:12], df.housing)
        
        # Split each phys housing type into separate tables
        for group in groupby(df, :phys_housing_type)
            SQLite.load!(group, db, group.phys_housing_type[1])
        end
        
        size = nrow(df)
        cumload += size
        pctload = cumload / total * 100
        
        @printf("Loaded %d rows, %.2f%% complete\n", cumload, pctload)
    end
end

# This function leaks memory. For now I can work around it by only running with 4 threads instead of 8
# to reduce memory demand so that the synthesis finishes before I run out of memory, but I have no idea why it's leaking.
# it's not due to multithreading, removing Threads.@threads does not solve the problem (i.e. it still leaks memory - although
# with a single thread it will complete before it runs out)
function compute_results!(phys_housing_types::Vector{String}, dbs::Vector{SQLite.DB}, seeds::Vector{UInt32}, proportions::DataFrame, results::Vector{DataFrame})
    complete_count = Threads.Atomic{Int64}(0)
    Threads.@threads for (i, phys_housing_type) in collect(enumerate(phys_housing_types))
        db = dbs[Threads.threadid()]
        results[i] = draw_households(phys_housing_type, seeds[i], proportions, db)

        # atomic add returns old value, add one to get current value
        complete = Threads.atomic_add!(complete_count, 1) + 1

        if (complete % 10 == 0)
            @printf("completed %d / %d housing types\n", complete, length(phys_housing_types))
            # REMOVE POSSIBLE DATA RACE, BUT OKAY FOR DEBUGGING
            #@printf("current data size: %.3f mb\n", get_data_size(results) / 1_000_000)
        end
    end
end

function get_data_size(results::Vector{DataFrame})
    total::Int64 = 0
    for i in 1:length(results)
        if isassigned(results, i)
            total += Base.summarysize(results[i].hh)
            total += Base.summarysize(results[i].uneq_choice)
            total += Base.summarysize(results[i].tract_geoid)
            total += Base.summarysize(results[i].rent)
        end
    end
    return total
end

function draw_households(puma_phys_housing::String, seed::UInt32, proportions::DataFrame, db::SQLite.DB)::DataFrame
    # yes ideally you don't build sql queries this way, but since it is a table name it can't be
    # parameterized, and this is a controlled environment
    
    candidate_hhs::DataFrame = DBInterface.execute(db, "SELECT hh, housing, uneq_choice, probability, hhwt FROM \"" * puma_phys_housing * "\"") |> DataFrame
    
    # extract tenure choice from joint choice
    candidate_hhs.rent = convert(Vector{Bool}, map(s -> s[14:length(s)] == "rent", candidate_hhs.housing))

    # save memory, make uneq_choice an int8
    candidate_hhs.uneq_choice[candidate_hhs.uneq_choice .== "3+"] .= "3"
    candidate_hhs.uneq_choice = map(u -> parse(Int8, u), candidate_hhs.uneq_choice)

    # weight the probability to be expected number of hhs in this puma of this type
    candidate_hhs.weighted_probs = candidate_hhs.probability .* candidate_hhs.hhwt

    # extract relevant tract probabilities
    rel_counts = proportions |> @filter(_.phys_housing == puma_phys_housing) |> DataFrame
    
    # see comment in main() about threads and random numbers
    rng = MersenneTwister(seed)

    # get the results, perf optimizations from https://github.com/JuliaData/DataFrames.jl/issues/1827
    tract_results = Vector{DataFrame}()
    sizehint!(tract_results, 50)
    # note that it is not actually necessary to multiply by the proportion_of_puma value, because
    # this is constant within a tract and we control the total number of draws from the tract using
    # n_hhs which is equivalent.
    w = StatsBase.ProbabilityWeights(candidate_hhs.weighted_probs)
    do_hh_draws!(Tables.columntable(rel_counts), candidate_hhs, w, rng, tract_results)

    return vcat(tract_results...)
end

function do_hh_draws!(rel_counts, candidate_hhs::DataFrame, w::StatsBase.ProbabilityWeights, rng::MersenneTwister, tract_results::Vector{DataFrame})
    for tract in Tables.rows(rel_counts)
        n_hhs = tract.n_hhs::Int64

        sample = StatsBase.sample(rng, 1:nrow(candidate_hhs), w, n_hhs)
        
        # this will create a copy, okay to destructively modify
        # https://dataframes.juliadata.org/stable/lib/indexing/#getindex-and-view
        # that document suggests that it will be a rowwise view into a copy of the df (so will)
        # be as large as candidate_hhs), but this is empirically not the case
        res = candidate_hhs[sample, [:hh, :rent, :uneq_choice]]
        res[!, :tract_geoid] .= tract.tract_geoid::String

        # # save memory
        # # not compressing this one, because it ultimately ends up concatenated with more unique values
        #res.tract_geoid = CategoricalArray(res.tract_geoid)

        push!(tract_results, res)
    end
end

# start this whole Rube Goldberg machine
main()