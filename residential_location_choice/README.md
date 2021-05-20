# Residential location choice model

This folder contains code to run the sorting model.

- Full sorting data.ipynb - generates the input files to run the model from the PostGIS database
- Final Visualizations.ipynb - generates the figures used in chapter 3 of the dissertation
- RHNA final projection.ipynb - generates development projections for the RHNA scenario
- run_model/run_model.py - main code to estimate and simulate with the sorting model. Requires significant RAM.
- summarize.py - summarizes model simulation outputs (large list of probabilities for each household x joint choice of all options) to marginal distributions of vehicle ownership, PUMA of residence, etc. Requires significant RAM, like the model itself.
- PUMA growth graphic.ipynb - generates figure showing growth under the RHNA scenario