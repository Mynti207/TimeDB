#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np
import asyncio
import matplotlib.pyplot as plt
import sys

from scipy.stats import norm

########################################
#
# This file can be used to test the basic client functionality.
# For it to work, you will need to first run go_server.py and go_webserver.py.
# 
# NOTE: This file does not incorporate the persistent database behavior.
#
########################################


def tsmaker(m, s, j):
    '''
    Helper function: randomly generates a time series for testing.

    Parameters
    ----------
    m : float
        Mean value for generating time series data
    s : float
        Standard deviation value for generating time series data
    j : float
        Quantifies the "jitter" to add to the time series data

    Returns
    -------
    A time series and associated meta data.
    '''

    # generate metadata
    meta = {}
    meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0,
                                          1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    meta['vp'] = False  # initialize vantage point indicator as negative

    # generate time series data
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j * np.random.randn(100)

    # return time series and metadata
    return meta, ts.TimeSeries(t, v)


async def main():

    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')

    # initialize database client
    client = TSDBClient()

    ########################################
    #
    # create dummy data
    #
    ########################################

    # parameters for testing
    num_ts = 50
    num_vps = 5

    # time series parameters
    mus = np.random.uniform(low=0.0, high=1.0, size=num_ts)
    sigs = np.random.uniform(low=0.05, high=0.4, size=num_ts)
    jits = np.random.uniform(low=0.05, high=0.2, size=num_ts)

    # initialize dictionaries for time series and their metadata
    tsdict = {}
    metadict = {}

    # fill dictionaries with randomly generated entries for database
    for i, m, s, j in zip(range(50), mus, sigs, jits):
        meta, tsrs = tsmaker(m, s, j)  # generate data
        pk = "ts-{}".format(i)  # generate primary key
        tsdict[pk] = tsrs  # store time series data
        metadict[pk] = meta  # store metadata

    ########################################
    #
    # trigger operations
    #
    ########################################

    # add a trigger - note: does not do anything
    await client.add_trigger('junk', 'insert_ts', None, 'db:one:ts')

    # add stats trigger to calculate mean and standard deviation of
    # every time series that is added
    await client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)

    ########################################
    #
    # time series insertion & metadata upsertion
    #
    ########################################

    print("\nSTARTING UPSERTS")
    print('\n---------------------')

    # insert the time series
    for k in tsdict:
        await client.insert_ts(k, tsdict[k])

    # upsert the metadata
    for k in tsdict:
        await client.upsert_meta(k, metadict[k])

    print("\nUPSERTS FINISHED")
    print('\n---------------------')

    ########################################
    #
    # selects
    #
    ########################################

    print("\nSTARTING SELECTS")
    print('\n---------------------')

    # select all database entries; no metadata fields
    print('\n---------DEFAULT------------')
    _, results = await client.select()
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all database entries; sort by 'order'; no metadata fields
    print('\n---------ADDITIONAL------------')
    _, results = await client.select(additional={'sort_by': '-order'})
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all database entries; return 'order' metadata
    print('\n----------ORDER FIELD-----------')
    _, results = await client.select(fields=['order'])
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all database entries; return all metadata fields
    print('\n---------ALL FIELDS------------')
    _, results = await client.select(fields=[])
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all entries with order = 1; return 'ts' metadata
    print('\n------------TS with order 1---------')
    _, results = await client.select({'order': 1}, fields=['ts'])
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all entries with blarg = 1; return all metadata fields
    print('\n------------All fields, blarg 1 ---------')
    _, results = await client.select({'blarg': 1}, fields=[])
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all entries with order = 1 AND blarg = 2; no metadata fields
    print('\n------------order 1 blarg 2 no fields---------')
    _, results = await client.select({'order': 1, 'blarg': 2})
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all entries with order >= 4; return 'order', 'blarg', 'mean'
    # metadata fields
    print('\n------------order >= 4  order, blarg and mean sent back, '
          'also sorted---------')
    _, results = await client.select({'order': {'>=': 4}},
                                     fields=['order', 'blarg', 'mean'],
                                     additional={'sort_by': '-order'})
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    # select all entries with blarg >= 1 AND order = 1; return 'blarg' and
    # 'std' metadata fields
    print('\n------------order 1 blarg >= 1 fields blarg and std---------')
    _, results = await client.select({'blarg': {'>=': 1}, 'order': 1},
                                     fields=['blarg', 'std'])
    if len(results) > 0:
        print('C> metadata fields:',
              list(results[list(results.keys())[0]].keys()))

    print("\nSELECTS FINISHED")
    print('\n---------------------')

    ########################################
    #
    # time series similarity search
    #
    ########################################

    print("\nSTARTING TIME SERIES SIMILARITY SEARCH")
    print('\n---------------------')

    # randomly choose time series as vantage points
    random_vps = np.random.choice(
        range(num_ts), size=num_vps, replace=False)
    vpkeys = ['ts-{}'.format(i) for i in random_vps]
    distkeys = ['d_vp-{}'.format(i) for i in range(num_vps)]

    # add the time series as vantage points
    for i in range(num_vps):
        status, payload = await client.insert_vp(vpkeys[i])

    # primary keys of vantage points
    print("VPS", vpkeys)

    # first create a query time series
    _, query = tsmaker(np.random.uniform(low=0.0, high=1.0),
                       np.random.uniform(low=0.05, high=0.4),
                       np.random.uniform(low=0.05, high=0.2))

    # get distances from query time series to all the vantage points
    _, result_distance = await client.augmented_select(
        'corr', ['vpdist'], query, {'vp': {'==': True}})
    vpdist = {v: result_distance[v]['vpdist'] for v in vpkeys}
    print(vpdist)

    # pick the closest vantage point
    nearest_vp_to_query = min(vpkeys, key=lambda v: vpdist[v])

    # define circle radius as 2 x distance to closest vantage point
    radius = 2 * vpdist[nearest_vp_to_query]
    print('Radius: {:.2f}'.format(radius))

    # find relative index of nearest vantage point
    relative_index_vp = vpkeys.index(nearest_vp_to_query)

    # calculate distance to all time series within the circle radius
    _, results = await client.augmented_select(
        'corr', ['towantedvp'], query, {'d_vp-{}'.format(relative_index_vp):
                                        {'<=': radius}})

    # find the closest time series
    nearestwanted1 = min(results.keys(),
                         key=lambda k: results[k]['towantedvp'])
    print('Nearest time series: {}; distance: {:.2f}'.
          format(nearestwanted1, results[nearestwanted1]['towantedvp']))

    status, payload = await client.vp_similarity_search(query, 1)
    nearestwanted2 = list(payload.keys())[0]

    # visualize results
    plt.plot(query, label='Input TS')
    plt.plot(tsdict[nearestwanted1], label='Closest TS (manual)')
    plt.plot(tsdict[nearestwanted2],
             label='Closest TS (DB operation)')
    plt.legend(loc='best')
    plt.show()

    print("\nTIME SERIES SIMILARITY SEARCH FINISHED")
    print('\n---------------------')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    coro = asyncio.ensure_future(main())
    loop.run_until_complete(coro)
