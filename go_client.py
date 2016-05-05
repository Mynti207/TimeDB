#!/usr/bin/env python3
from tsdb import TSDBClient
import timeseries as ts
import numpy as np
import asyncio
import matplotlib.pyplot as plt

from scipy.stats import norm

# number of vantage points
NUMVPS = 5


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


async def main(plot_graph):

    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')

    # initialize database client
    client = TSDBClient()

    # add a trigger - note: does not do anything
    await client.add_trigger('junk', 'insert_ts', None, 'db:one:ts')

    # add stats trigger to calculate mean and standard deviation of
    # every time series that is added
    await client.add_trigger('stats', 'insert_ts', ['mean', 'std'], None)

    # randomly generate mean, standard deviation and jitter parameters for
    # 50 time series
    mus = np.random.uniform(low=0.0, high=1.0, size=50)
    sigs = np.random.uniform(low=0.05, high=0.4, size=50)
    jits = np.random.uniform(low=0.05, high=0.2, size=50)

    # initialize dictionaries for time series and their metadata
    tsdict = {}
    metadict = {}

    # fill dictionaries with randomly generated entries for database
    for i, m, s, j in zip(range(50), mus, sigs, jits):
        meta, tsrs = tsmaker(m, s, j)  # generate data
        pk = "ts-{}".format(i)  # generate primary key
        tsdict[pk] = tsrs  # store time series data
        metadict[pk] = meta  # store metadata

    # randomly choose five time series as vantage points
    random_vps = np.random.choice(range(50), size=NUMVPS, replace=False)
    vpkeys = ["ts-{}".format(i) for i in random_vps]

    # add triggers to calculate the distances to the five vantage points
    for i in range(NUMVPS):
        # add 5 triggers to upsert distances to these vantage points
        await client.add_trigger('corr', 'insert_ts',
                                 ["d_vp-{}".format(i)], tsdict[vpkeys[i]])
        # change the metadata for the vantage points to have meta['vp']=True
        metadict[vpkeys[i]]['vp'] = True

    # insert the time series and upsert the metadata
    for k in tsdict:
        await client.insert_ts(k, tsdict[k])
        await client.upsert_meta(k, metadict[k])

    print("\nUPSERTS FINISHED")
    print('\n---------------------')
    print("\nSTARTING SELECTS")

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

    # TODO: what does this do??
    print('\n------now computing vantage point stuff---------------------')
    print("VPS", vpkeys)

    # we first create a query time series.
    _, query = tsmaker(0.5, 0.2, 0.1)

    # your code here begins

    # Step 1: in the vpdist key, get  distances from query to vantage points
    # this is an augmented select
    vpdist = {}
    _, result_distance = await client.augmented_select('corr',
                                                       ['vpdist'],
                                                       query,
                                                       {'vp': {'==': True}})
    vpdist = {v: result_distance[v]['vpdist'] for v in vpkeys}
    print(vpdist)
    # 1b: choose the lowest distance vantage point
    # you can do this in local code
    nearest_vp_to_query = min(vpkeys, key=lambda v: vpdist[v])

    # Step 2: find all time series within 2*d(query, nearest_vp_to_query)
    # this is an augmented select to the same proc in correlation
    radius = 2 * vpdist[nearest_vp_to_query]
    print('Radius: {:.2f}'.format(radius))
    # Find the reative index of the nearest_vp_to_query
    relative_index_vp = vpkeys.index(nearest_vp_to_query)
    _, results = await client.augmented_select('corr',
                                               ['towantedvp'],
                                               query,
                                               {'d_vp-{}'.format(
                                                relative_index_vp):
                                                {'<=': radius}})

    # 2b: find the smallest distance amongst this ( or k smallest)
    # you can do this in local code
    nearestwanted = min(results.keys(), key=lambda k: results[k]['towantedvp'])
    print('Nearest time series: {}; distance: {:.2f}'.
          format(nearestwanted, results[nearestwanted]['towantedvp']))

    # plot the timeseries to see visually how we did.
    if plot_graph:
        plt.plot(query)
        plt.plot(tsdict[nearestwanted])
        plt.show()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    coro = asyncio.ensure_future(main(plot_graph=False))
    loop.run_until_complete(coro)
