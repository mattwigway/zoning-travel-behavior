BUCKET = "rhna-sorting"
MODEL = "defense_estimate"
PROJECTIONS = "puma_growth.csv"
NPV_PROJECTIONS = "npv_scenarios.parquet"


import pandas as pd
import numpy as np
import eqsormo
import logging
import time
import zipfile
import datetime
from eqsormo.common.util import human_time
import os

LOG = logging.getLogger(__name__)


def configure_parser(parser):
    parser.add_argument("option", action="store")


def sim(args, model_name):
    sim_run = model_name

    try:
        LOG.info("Initializing sorting model for simulation")

        LOG.info("Loading model")
        model = eqsormo.tra.TraSortingModel.loadnew(MODEL)
        # not set in pre-linesearch eqsormo versions used to fit model
        model.fixed_price = "03744_MF_old_rent"

        # put outside conditional so they are available even in base case
        ex_supply = model.weighted_supply.copy()
        new_supply = ex_supply.copy()
        if args.option == "base":
            LOG.info("Base (fitted) probabilities requested, not performing sorting!")
        elif args.option.startswith("npv"):
            LOG.info("Loading NPV-based projections")
            npv_delta = pd.read_parquet(NPV_PROJECTIONS)

            deltas = npv_delta[args.option].reindex(new_supply.index)
            nna = deltas.isnull().sum()
            if nna > 0:
                LOG.warn(f"{nna} deltas are NaN")
            deltas = deltas.fillna(0)
            new_supply += deltas
            new_supply = new_supply.reindex(model.housing_xwalk.index)
            new_supply *= ex_supply.sum() / new_supply.sum()

            assert not (new_supply <= 0).any(), "some supplies fall to zero or below!"

            LOG.info(f"Unit change:\n{(new_supply / ex_supply).describe()}")

            # TODO duplicate code
            LOG.info("performing sorting")
            start_time = time.perf_counter()
            model.weighted_supply = new_supply
            model.sort()
            end_time = time.perf_counter()

            total_time = end_time - start_time
            LOG.info(f"Simulation finished after {human_time(total_time)}")
        elif args.option == "rhna":
            LOG.info("Loading projections")

            rhna = pd.read_csv(PROJECTIONS).rename(columns={"Unnamed: 0": "pumamf"})
            projected = pd.DataFrame(
                model.weighted_supply.copy().rename("units")
            ).reset_index()
            projected[["puma", "mf", "age", "tenure"]] = projected.choice.str.split(
                "_", expand=True
            )

            currentTotals = pd.DataFrame(
                projected.groupby(["puma", "mf", "tenure"])
                .units.sum()
                .rename("current_units")
            ).reset_index()
            currentTotals["pumamf"] = currentTotals.puma.str.cat(
                currentTotals.mf.str.lower(), sep="_"
            )

            rhna = currentTotals.merge(rhna, how="left", on="pumamf", validate="m:1")

            rhna[f"total_new_units"] = (
                    rhna[f"total_growth"] * rhna.current_units
                )

            # All RHNA-provided units will be new
            rhna["choice"] = (
                rhna.puma.str.cat(rhna.mf, sep="_") + "_" + "new"
            ).str.cat(rhna.tenure, sep="_")
            rhna.set_index("choice", inplace=True)

            new_units = rhna[f"total_new_units"].reindex(
                new_supply.index, fill_value=0
            )

            if (nna := new_units.isnull().sum()) > 0:
                LOG.warn(f"{nna} NAs found in RHNA projections, setting to zero")
                new_units = new_units.fillna(0)

            new_supply += new_units
            new_supply = new_supply.reindex(model.housing_xwalk.index)
            new_supply *= ex_supply.sum() / new_supply.sum()

            assert np.all(np.isfinite(new_supply)), "some elements of new supply are not finite"
            
            LOG.info(f"Unit change:\n{(new_supply / ex_supply).describe()}")

            # if new_supply.sum() != model.weights.sum():
            #     diff = new_supply.sum() - model.weights.sum()
            #     # rounding
            #     assert diff < 1e-8
            #     new_supply.iloc[-1] -= diff
            #     assert new_supply.sum() == model.weights.sum()

            LOG.info("performing sorting")
            start_time = time.perf_counter()
            model.weighted_supply = new_supply
            model.sort()
            end_time = time.perf_counter()

            total_time = end_time - start_time
            LOG.info(f"Simulation finished after {human_time(total_time)}")

        LOG.info("computing probabilities")
        probs = model.probabilities()

        LOG.info("writing results")

        # don't compress the zipfile, individual parquet files within are compressed
        with zipfile.ZipFile(
            f"simulation_{model_name}.parquet.zip", "w", compression=zipfile.ZIP_STORED
        ) as zf:
            with zf.open("housing.parquet.gz", "w") as of:
                pd.DataFrame(
                    {
                        "existing_supply": ex_supply,
                        "new_supply": new_supply,
                        "orig_price": model.orig_price,
                        "new_price": model.price,
                    }
                ).to_parquet(of, compression="gzip")
            with zf.open("probabilities.parquet.gz", "w", force_zip64=True) as of:
                probs.to_parquet(of, compression="gzip")

    except:
        LOG.exception("exception fitting model!")
