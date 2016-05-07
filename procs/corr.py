from timeseries import TimeSeries
import numpy as np

from ._corr import stand, kernel_corr

import asyncio


def proc_main(pk, row, arg):
    '''
    Calculates the distance between two time series, using the normalized
    kernelized cross-correlation.
    Note: used directly for augmented selects.

    Parameters
    ----------
    pk : any hashable type
        The primary key of the database entry
    row : dictionary
        The database entry

    Returns
    -------
    [damean, dastd] : list of floats
        Mean and standard deviation of the time series data
    '''

    # recast the argument as a time series (type is lost due to serialization)
    if isinstance(arg, TimeSeries):
        argts = arg  # for server-side testing
    else:
        argts = TimeSeries(*arg)  # for live client-side operations

    # standardize the time series
    stand_argts = stand(argts, argts.mean(), argts.std())

    # standardize each row of the data that has tripped the trigger
    stand_rowts = stand(row['ts'], row['ts'].mean(), row['ts'].std())

    # compute the normalized kernelized cross-correlation between the
    # time series being upserted/selected and the time series argument
    kerncorr = kernel_corr(stand_rowts, stand_argts, 5)

    # use the normalized kernelized cross-correlation to compute the distance
    # between the time series and return
    return [np.sqrt(2*(1-kerncorr))]


async def main(pk, row, arg):
    '''
    Calls the proc function.
    Note: used for triggers, not augmented selects.

    Parameters
    ----------
    pk : any hashable type
        The primary key of the database entry
    row : dictionary
        The database entry

    Returns
    -------
    Result of the proc function.
    '''
    return proc_main(pk, row, arg)
