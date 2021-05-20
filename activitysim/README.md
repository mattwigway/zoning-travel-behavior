# Activity-based travel demand model code

- Driving and Walking Skims.ipynb - Create the base skims for uncongested driving, walking, and biking
- Prepare congestion model data.ipynb - computes origin, destination features for the congestion model
- congested_routes_mp.py - computes the along-route features for all tract pairs for the baseline congestion model (random forest based on Uber Movement data)
- Congestion model.ipynb - build the random forest model for congestion based on Uber Movement data
- congestion_prediction_mp.py - predict congestion levels and travel times for all O-D pairs at all times of day
- download_dem_data.py - download elevation data for creating land use topography data
- Split GTFS.ipynb - splits the Los Angeles area GTFS files into express bus, local bus, light rail, and heavy 
- confirm_transit_service_dates.jl - after building a TransitRouter.jl network, make sure all feeds have service on the chosen analysis date
- generate_transit_skims.jl - generates transit skims using a TransitRouter.jl network for a particular subset of service defined by ActivitySim (e.g. heavy rail). (network built using network builder included with TransitRouter.jl)
- Assemble Skims.ipynb - put together driving, transit, biking, and walking skims from different sources
- Rename skims.ipynb - put all skim names into ActivitySim format
- Fix walking and biking skims.ipynb - Handle two tracts that are (correctly) disconnected from the walking and biking network in SoCal
- Create FW OD file.ipynb - reformat tract data into a CSV for the Frank-Wolfe simulation
- Reverse engineer urban area types.ipynb - the ActivitySim urban area type variable is not documented. This reverse-engineers it based on density.
- Reverse-engineed topography.ipynb - the ActivitySim "topology" (topography) variable is not documented. This reverse-engineers it based on slopes.
- Land use.ipynb - create land use input data for ActivitySim (run once for each scenario simulated)
- nhts.py - utility functions for working with NHTS data
- Scenario vs fitted.ipynb - Compare the scenario and baseline travel demand model results
- Compare base model results to observed.ipynb - Compares the predictions of the activity-based model under the baseline scenario with observed travel from the NHTS
- Congestion visualization.ipynb - visualizes output of Frank-Wolfe traffic assignment algorithm
- activitysim_configs - configuration files for ActivitySim. Copied with slight modifications from [the ActivitySim/MTC TM1 example](https://activitysim.github.io/activitysim/examples.html#example-mtc).
- frankwolfe/Edge-based graph.ipynb - generates Figure 31
- frankwolfe/build_fw_graph.jl - Builds a graph from OpenStreetMap data for the Frank-Wolfe algorithm
- frankwolfe/run_fw.jl - Runs the Frank-Wolfe static traffic assignment algorithm
- frankwolfe/extract_fw_graph_data.jl - Extract data from the Frank-Wolfe graph for visualization
- Other files in the frankwolfe directory are included by the files listed above