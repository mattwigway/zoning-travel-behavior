# NHTS replicate weight estimation utility functions
import numpy as np


def multiReplicateSE(data, colPrefix, year):
    """
    Estimate SEs for a data frame containing estimates using each replicate weight.
    Such a data frame is frequently produced by an agg() call, summing up the weights in a number of categories.
    Pass in the data frame, the column prefix (there is expected to be a column with this name which represents the point
    estimate, and columns with this prefix and the suffix 1-n for the n replicate weights), and the NHTS year (which determines
    the exact functional form of the estimater.
    """

    if year == 2017:
        scaleFactor = 6 / 7
        n = 98
    elif year == 2009:
        scaleFactor = 99 / 100
        n = 100
    elif year == 2001:
        scaleFactor = 98 / 99
        n = 99
    else:
        raise ArgumentError("Invalid year")

    se = np.zeros(len(data))
    for i in range(1, n + 1):
        se += (data[f"{colPrefix}{i}"] - data[colPrefix]) ** 2
    return np.sqrt(scaleFactor * se)


def estReplicateSE(estimater, ptEst, year=None, n=None, scaleFactor=None):
    """
    Estimate a replicate standard error for a single estimate. Pass in a function (probably a lambda) that
    takes a number n and returns the estimate for the nth replicate weight, the point estimate, and the year of the NHTS.
    Alternately, the number of replicate weights and the scale factor can be set individually (useful for replicate weight
    error estimation with other datasets).
    """
    if year == 2017:
        n = 98
        scaleFactor = 6 / 7
    elif year == 2009:
        n = 100
        scaleFactor = 99 / 100
    elif year == 2001:
        n = 99
        scaleFactor = 98 / 99

    replicateEsts = np.vectorize(estimater)(np.arange(1, n + 1))

    return np.sqrt(scaleFactor * np.sum((replicateEsts - ptEst) ** 2))
