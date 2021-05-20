# Fit the full equilibrium sorting model, with renter and owner considered separately

HOUSING_DATA = "full_alts.parquet"
HH_DATA = "full_hh.parquet"
SEED = 85927752

import pandas as pd
import numpy as np
import eqsormo
import logging
import argparse
import time
import datetime
from eqsormo.common.util import human_time

LOG = logging.getLogger(__name__)


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


def configure_parser(parser):
    parser.add_argument("--fast-fit", action="store_true")


def fit(args, model_name):
    failed = False

    try:
        LOG.info("Initializing sorting model")

        # LOG.info("Downloading household data")
        # s3.download_file(*HH_DATA, "hh.parquet")
        # s3.download_file(*HOUSING_DATA, "housing.parquet")
        # # s3.download_file(*HH_HOUSING_DATA, 'hh_housing.pickle')

        LOG.info("Reading data")
        alts = pd.read_parquet(HOUSING_DATA)
        hh = pd.read_parquet(HH_DATA)
        # hh_household = pd.read_pickle('hh_housing.pickle')

        if args.fast_fit:
            # hack for speed during testing
            hh = (
                hh.reset_index()
                .groupby("choice", as_index=False)
                .first()
                .set_index("serial")
            )

        np.seterr(all="raise")

        # make 0 vehicles ref category
        # hh = hh.sort_values('vehchoice')

        LOG.info("Initializing sorting model")
        start_time = time.perf_counter()
        logmodel = eqsormo.TraSortingModel(
            # some choices lost in income requirements above
            housing_attributes=alts.loc[
                hh.choice.unique(),
                [
                    "fifthGradeMathMedianProficient:rent",
                    "jobAccessAuto:rent",
                    "sfh:rent",
                    "intersectionDens:rent",
                    "retailJobDensSqKm:rent",
                    "locacc:rent",
                    "regacc:rent",
                    "hilocacc:rent",
                    "hiregacc:rent",
                    "fifthGradeMathMedianProficient:own",
                    "jobAccessAuto:own",
                    "sfh:own",
                    "intersectionDens:own",
                    "retailJobDensSqKm:own",
                    "locacc:own",
                    "regacc:own",
                    "hilocacc:own",
                    "hiregacc:own",
                    "rent",
                    "locacc",
                    "regacc",
                    "hilocacc",
                    "hiregacc"
                ],
            ].astype("float64"),
            household_attributes=hh[
                [
                    "university",
                    "worker",
                    "immigrant",
                    "child",
                    "senior",
                    "numprec",
                    "inc_50_100k",
                    "inc_100k_plus",
                    "inc_under_50k:immigrant",
                    "inc_50_100k:immigrant",
                    "inc_100k_plus:immigrant",
                    "inc_under_50k:child:university",
                    "inc_50_100k:child:university",
                    "inc_100k_plus:child:university",
                    "inc_under_50k:child:not_university",
                    "inc_50_100k:child:not_university",
                    "inc_100k_plus:child:not_university"
                ]
            ].astype("float64"),
            # household_housing_attributes=hh_household[['liveworksame']],
            interactions=[
                # Tenure choice model
                ("university", "rent"),
                ("worker", "rent"),
                ("child", "rent"),
                ("senior", "rent"),
                ("numprec", "rent"),
                ("inc_50_100k", "rent"),
                ("inc_100k_plus", "rent"),
                # separate immigrant coefs by income
                ("inc_under_50k:immigrant", "rent"),
                ("inc_50_100k:immigrant", "rent"),
                ("inc_100k_plus:immigrant", "rent"),

                # RL choice model, renters
                ("university", "fifthGradeMathMedianProficient:rent"),
                ("university", "locacc:rent"),
                ("university", "regacc:rent"),
                ("university", "hilocacc:rent"),
                ("university", "hiregacc:rent"),
                ("university", "sfh:rent"),
                ("worker", "locacc:rent"),
                ("worker", "regacc:rent"),
                ("worker", "hilocacc:rent"),
                ("worker", "hiregacc:rent"),
                ("immigrant", "sfh:rent"),
                ("immigrant", "locacc:rent"),
                ("immigrant", "regacc:rent"),
                ("immigrant", "hilocacc:rent"),
                ("immigrant", "hiregacc:rent"),
                ("child", "sfh:rent"),
                ("child", "locacc:rent"),
                ("child", "regacc:rent"),
                ("child", "hilocacc:rent"),
                ("child", "hiregacc:rent"),
                ("senior", "fifthGradeMathMedianProficient:rent"),  # resale value
                ("senior", "sfh:rent"),
                ("senior", "locacc:rent"),
                ("senior", "regacc:rent"),
                ("senior", "hilocacc:rent"),
                ("senior", "hiregacc:rent"),
                ("numprec", "sfh:rent"),
                ("inc_50_100k", "fifthGradeMathMedianProficient:rent"),
                ("inc_50_100k", "sfh:rent"),
                ("inc_50_100k", "locacc:rent"),
                ("inc_50_100k", "regacc:rent"),
                ("inc_50_100k", "hilocacc:rent"),
                ("inc_50_100k", "hiregacc:rent"),
                ("inc_100k_plus", "fifthGradeMathMedianProficient:rent"),
                ("inc_100k_plus", "sfh:rent"),
                ("inc_100k_plus", "locacc:rent"),
                ("inc_100k_plus", "regacc:rent"),
                ("inc_100k_plus", "hilocacc:rent"),
                ("inc_100k_plus", "hiregacc:rent"),

                ("inc_under_50k:child:not_university", "fifthGradeMathMedianProficient:rent"),
                ("inc_50_100k:child:not_university", "fifthGradeMathMedianProficient:rent"),
                ("inc_100k_plus:child:not_university", "fifthGradeMathMedianProficient:rent"),

                ("inc_under_50k:child:university", "fifthGradeMathMedianProficient:rent"),
                ("inc_50_100k:child:university", "fifthGradeMathMedianProficient:rent"),
                ("inc_100k_plus:child:university", "fifthGradeMathMedianProficient:rent"),


                # RL choice model, owners
                ("university", "fifthGradeMathMedianProficient:own"),
                ("university", "locacc:own"),
                ("university", "regacc:own"),
                ("university", "hilocacc:own"),
                ("university", "hiregacc:own"),
                ("university", "sfh:own"),
                ("worker", "locacc:own"),
                ("worker", "regacc:own"),
                ("worker", "hilocacc:own"),
                ("worker", "hiregacc:own"),
                ("immigrant", "sfh:own"),
                ("immigrant", "locacc:own"),
                ("immigrant", "regacc:own"),
                ("immigrant", "hilocacc:own"),
                ("immigrant", "hiregacc:own"),
                ("child", "sfh:own"),
                ("child", "locacc:own"),
                ("child", "regacc:own"),
                ("child", "hilocacc:own"),
                ("child", "hiregacc:own"),
                ("senior", "fifthGradeMathMedianProficient:own"),  # resale value
                ("senior", "sfh:own"),
                ("senior", "locacc:own"),
                ("senior", "regacc:own"),
                ("senior", "hilocacc:own"),
                ("senior", "hiregacc:own"),
                ("numprec", "sfh:own"),
                ("inc_50_100k", "fifthGradeMathMedianProficient:own"),
                ("inc_50_100k", "sfh:own"),
                ("inc_50_100k", "locacc:own"),
                ("inc_50_100k", "regacc:own"),
                ("inc_50_100k", "hilocacc:own"),
                ("inc_50_100k", "hiregacc:own"),
                ("inc_100k_plus", "fifthGradeMathMedianProficient:own"),
                ("inc_100k_plus", "sfh:own"),
                ("inc_100k_plus", "locacc:own"),
                ("inc_100k_plus", "regacc:own"),
                ("inc_100k_plus", "hilocacc:own"),
                ("inc_100k_plus", "hiregacc:own"),

                ("inc_under_50k:child:not_university", "fifthGradeMathMedianProficient:own"),
                ("inc_50_100k:child:not_university", "fifthGradeMathMedianProficient:own"),
                ("inc_100k_plus:child:not_university", "fifthGradeMathMedianProficient:own"),

                ("inc_under_50k:child:university", "fifthGradeMathMedianProficient:own"),
                ("inc_50_100k:child:university", "fifthGradeMathMedianProficient:own"),
                ("inc_100k_plus:child:university", "fifthGradeMathMedianProficient:own"),


                # RL choice model, common (due to technical limitations)
                # ("nonhispwhite_all", "propnonhispwhite"),
                # ("hhincome", "medinc"),
            ],
            second_stage_params=[
                "rent",
                "sfh:rent",
                "locacc:rent",
                "regacc:rent",
                "hilocacc:rent",
                "hiregacc:rent",
                "fifthGradeMathMedianProficient:rent",
                "sfh:own",
                "locacc:own",
                "regacc:own",
                "hilocacc:own",
                "hiregacc:own",
                "fifthGradeMathMedianProficient:own",
            ],
            choice=hh.choice,
            unequilibrated_choice=hh.vehchoice,
            unequilibrated_hh_params=[
                "worker",
                "university",
                "child",
                "numprec",
                "inc_50_100k",
                "inc_100k_plus"
            ],
            unequilibrated_hsg_params=["locacc", "regacc", "hilocacc", "hiregacc"],
            # endogenous_variable_defs={
            #     "propnonhispwhite": lambda hhs, income, weights: (
            #         np.sum(hhs.nonhispwhite_count * weights)
            #         / np.sum(hhs.numprec * weights)
            #     ),
            #     "medinc": lambda hhs, income, weights: weighted_percentile(
            #         income, 50, pd.Series(weights, index=hhs.index)
            #     ),
            # },
            neighborhoods=alts.puma,
            price=alts.annvalue,
            income=hh.hhincome,
            sample_alternatives=10,
            price_income_transformation=eqsormo.tra.price_income.logdiff,
            # the transform is not defined for rent > income, but this is cts - b/c log(0) = -inf which means utility is
            # -infinity when price=income, which is effectively the same as not being in choice set
            max_rent_to_income=1,
            weights=hh.hhwt,
            est_first_stage_ses=True,
            method="L-BFGS-B",
            minimize_options={"maxfun": 1_000_000, "maxiter": 1_000},
            seed=SEED,
            fixed_price="03744_MF_old_rent"
        )

        np.random.seed(SEED)  # may not affect default_rng instance...
        logmodel.create_alternatives()
        logmodel.fit()

        LOG.info(logmodel.to_text())

        logmodel.to_text(f"{model_name}.txt")
        logmodel.savenew(model_name)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        LOG.info(f"Model fitting finished after {human_time(total_time)}")

        # LOG.info("Uploading model results to S3")

        # s3.upload_file(
        #     "log.pickle", "rhna-sorting", f"results/{model_name}/{model_name}.pickle"
        # )
        # s3.upload_file(
        #     "log.npz", "rhna-sorting", f"results/{model_name}/{model_name}.npz"
        # )
        # s3.upload_file(
        #     "log.txt", "rhna-sorting", f"results/{model_name}/{model_name}.txt"
        # )
    except:
        LOG.exception("exception fitting model!")
        failed = True
    finally:
        # make sure this always happens
        if failed:
            model_name += "_FAILED"
