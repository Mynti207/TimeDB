import asyncio


def proc_main(pk, row, arg):
    '''
    Defines the 'junk' trigger coroutine - does nothing.
    Note: used directly for augmented selects.

    Parameters
    ----------
    pk : any hashable type
        The primary key of the database entry
    row : dictionary
        The database entry

    Returns
    -------
    [None]
    '''
    return [None]

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
