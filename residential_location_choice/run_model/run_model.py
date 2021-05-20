# Run the model in eqsormo

import argparse
import fit
import sim
import eqsormo
import boto3
import datetime
import logging
import numpy as np  # unused but needed for unpickling to work right
import pandas as pd  # more dill issues
import dill
import multiprocessing

if __name__ == "__main__":

    # hack around dill issue where top-level functions are not serialized inside lambdas
    def weighted_percentile(vals, percentiles, weights):
        if len(vals) != len(weights):
            raise ArgumentError("values and weights arrays are not same length!")

        nas = pd.isnull(vals) | pd.isnull(weights)

        nnas = np.sum(nas)
        if nnas > 0:
            warn(f"found {nnas} NAs in data, dropping them")

        vals = vals[~nas]
        weights = weights[~nas]

        weights = weights / np.sum(weights)
        sortIdx = np.argsort(vals)
        vals = vals.iloc[sortIdx]
        weights = weights.iloc[sortIdx]

        cumWeights = np.cumsum(weights)
        if not isinstance(percentiles, np.ndarray):
            percentiles = np.array(percentiles)
        percentiles = percentiles / 100

        # center weights, i.e. put the point value halfway through the weight
        # https://github.com/nudomarinero/wquantiles/blob/master/wquantiles.py
        centeredCumWeights = cumWeights - 0.5 * weights
        return np.interp(percentiles, centeredCumWeights, vals)

    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", default="FULL")
    subcommands = parser.add_subparsers()

    estimate = subcommands.add_parser("estimate")
    fit.configure_parser(estimate)
    estimate.set_defaults(func=fit.fit, op="estimate")

    simulate = subcommands.add_parser("simulate")
    sim.configure_parser(simulate)
    simulate.set_defaults(func=sim.sim, op="sim")

    args = parser.parse_args()

    model_name = (
        args.model_name
        + "_"
        + args.op
        + "_"
        + datetime.datetime.today().strftime("%Y-%m-%d_%H_%M_%S")
    )

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOG = logging.getLogger()
    file_handler = logging.FileHandler(f"{model_name}.log", mode="w", encoding="UTF-8")
    file_handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    LOG.addHandler(file_handler)

    if args.op == "sim":
        model_name += f"_{args.option}"

    if args.op == "estimate":
        if args.fast_fit:
            model_name += "_FAST_FIT"

    try:
        args.func(args, model_name)
    finally:
        file_handler.close()
