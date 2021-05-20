# Profitability

This folder contains code to compute the feasibility and profitability of redevelopment of all single-family lots in Los Angeles county.

## Components

- SQMake files
    In the `sql` folder, there are [sqmake](https://github.com/mattwigway/sqmake) files that load necessary data into a Postgres database (schema `diss`) and perform some initial manipulations of it. The SQMake files will download most of the needed data, but a few other datasets that are needed:
        - [SCAG zoning](https://gisdata-scag.opendata.arcgis.com/datasets/landuse-combined-los-angeles) - need to combine SCAG zoning for all counties and load to a table `gp16` (general plan 2016) before running `sqmake`.
        - [RSMeans cost estimates](https://rsmeans.com) - cost estimates for constructing the prototype buildings in Los Angeles; these need to be put in an Excel sheet that can be loaded to the database, and also entered into the `construction-costs.sql` file. These cannot be redistributed due to licensing restrictions.
        - [ZTrax data](https://zillow.com/research/ztrax) - Assessment and transaction data from Zillow, loaded to SQLite using [ztraxdb](https://github.com/mattwigway/ztraxdb). Cannot be redistributed due to licensing restrictions.

- Polygon-polygon tests
    This script determines whether particular prototype buildings will fit on particular lots.

- Cap rates.ipynb - Computes a capitalization rate based on recent Los Angeles-area property sales
- Hedonic model.ipynb - Estimates rents for new and existing buildings
- Vacant property hedonic.ipynb - Estimates values for vacant properties
- Inflation.ipynb - Estimates property inflation in Los Angeles over the last several years
- Profitability.ipynb - Estimates property redevelopment profitability for each prototype building (make sure to run `Hedonic model` and `Vacant property hedonic` first)
- Compare fit to sq foot calculation.ipynb - Compares building-fit metrics based on polygon-in-polygon tests with simpler, square-foot-based metrics
- Construction vacancy.ipynb - Evaluates how much of the profitable redevelopment is on vacant lots rather than lots with existing homes
- Raster polygon containment image.ipynb - Creates Figure 6
- Rent adjustments.ipynb - Adjust IPUMS rents to be in line with Zillow rents
- Unmatched parcels.ipynb - Identify parcels that did not match between Zillow and SCAG datasets (Table 3)