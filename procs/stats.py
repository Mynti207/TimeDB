import asyncio


def proc_main(pk, row, arg):
    '''
    Defines the 'stats' trigger coroutine - used to calculate the mean and
    standard deviation of time series data.
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
    # calculate mean
    damean = row['ts'].mean()

    # calculate standard deviation
    dastd = row['ts'].std()

    # return mean, standard deviation tuple
    return [damean, dastd]

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
