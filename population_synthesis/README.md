# Population synthesis

The code here synthesizes the population based on the residential location choice model results.

- Assemble population scenarios.ipynb - creates the marginal distributions based on redevelopment scenarios for population synthesis
- popsyn.jl - core of the population synthesis algorithm - draws households into synthetic population and assigns to a Census tract
- materialize_simulated_population.py - adds important attributes to the synthetic population and formats it for ActivitySim
- Compare simulated population to actual.ipynb - compares the base synthetic population to the actual population 