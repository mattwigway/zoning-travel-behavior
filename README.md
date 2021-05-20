# If you build it, who will come, and how will they travel?

This repository contains the code used to produce the results in my dissertation, [_If you build it, who will come, and how will they travel? The effects of relaxed zoning regulations on travel behavior_](https://files.indicatrix.org/conway_dissertation_proquest.pdf), defended April 2021 at Arizona State University. It is divided into subfolders by chapter, and in each subfolder there is a readme detailing the code for that specific chapter.

1. Introduction - this chapter did not use code
2. Profitability - code to estimate the profitability of redevelopment
3. Sales - code to estimate whether profitable properties sell to developers
4. Residential location choice - the sorting model of residential location choice
5. Population synthesis - the custom population synthesizer to synthesize populations in accordance with the sorting model results
6. Activity-based travel demand model - code for the ActivitySim-based travel demand model
7. Discussion and conclusion - this chapter did not use code

In addition to the code used herein, several of the components I developed for this dissertation are released in separate repositories:

- [ztraxdb](https://github.com/mattwigway/ztraxdb): Python code used to load ZTrax data into a database
- [sqmake](https://github.com/mattwigway/sqmake): used to orchestrate ETL processes for getting data in place in an SQL database (Postgres, in the case of this dissertation)
- [eqsormo](https://github.com/mattwigway/eqsormo): used to run equilibrium sorting models in Python
- [TransitRouter.jl](https://github.com/mattwigway/TransitRouter.jl): used to generate transit skims for the travel demand model
- [OSMPBF.jl](https://github.com/mattwigway/OSMPBF.jl): used to read OpenStreetMap data into Julia for use in traffic assignment

This code was run in a fairly complicated environment, involving database servers and hosted on a local machine, bare-metal Linux machines, Amazon Web Services, and the Arizona State University High-Performance Computing Cluster. Running it again will require getting these components in place in your environment. Furthermore, it requires proprietary data from Zillow and RSMeans, so the code in this repository will likely be more useful for understanding the methods rather than replicating the outputs.

For any questions, feel free to contact me at [mwconway@asu.edu](mailto:mwconway@asu.edu).