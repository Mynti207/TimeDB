#!/usr/bin/env python3
from tsdb import *
import timeseries as ts
import numpy as np
import asyncio
import requests
import json

from scipy.stats import norm


# Helper to build the HTTP request for the REST API
def request_insert(pk, ts):
    msg = TSDBOp_InsertTS(pk, ts).to_json()
    r = requests.post("http://127.0.0.1:8080/tsdb/insert_ts", data=json.dumps(msg))

def request_upsert_meta(pk, md):
    msg = TSDBOp_UpsertMeta(pk, md).to_json()
    r = requests.post("http://127.0.0.1:8080/tsdb/upsert_meta", data=json.dumps(msg))


def request_select(md={}, fields=None, additional=None):
    msg = TSDBOp_Select(md, fields, additional).to_json()
    r = requests.get("http://127.0.0.1:8080/tsdb/select", data=json.dumps(msg))
    print('r.text: ', r.text)
    return json.loads(r.text, object_pairs_hook=OrderedDict)
    
def request_augmented_select(proc, target, arg=None, md={}, additional=None):
    msg = TSDBOp_AugmentedSelect(proc, target, arg, md, additional).to_json()
    r = requests.get("http://127.0.0.1:8080/tsdb/augmented_select", data=json.dumps(msg))
    print('r.text: ', r.text)
    return json.loads(r.text, object_pairs_hook=OrderedDict)
    
def request_add_trigger(proc, onwhat, target, arg=None):
    msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()
    r = requests.post("http://127.0.0.1:8080/tsdb/add_trigger", data=json.dumps(msg))
    
def request_remove_trigger(proc, onwhat):
    msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()
    r = requests.post("http://127.0.0.1:8080/tsdb/remove_trigger", data=json.dumps(msg))

def request_similarity_search(query, top=1):
    msg = {'query': query.to_json(), 'top': top}
    r = requests.get("http://127.0.0.1:8080/tsdb/similarity_search", data=json.dumps(msg))
    print('r.text' is r.text)
    return json.loads(r.text, object_pairs_hook=OrderedDict)
    
# m is the mean, s is the standard deviation, and j is the jitter
# the meta just fills in values for order and blarg from the schema
def tsmaker(m, s, j):
    "returns metadata and a time series in the shape of a jittered normal"
    meta={}
    meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return meta, ts.TimeSeries(t, v)


def main():
    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')

    # add a trigger. notice the argument. It does not do anything here but
    # could be used to save a shlep of data from client to server.
    request_add_trigger('junk', 'insert_ts', None, 'db:one:ts')
    # our stats trigger
    request_add_trigger('stats', 'insert_ts', ['mean', 'std'], None)
    #Set up 50 time series
    mus = np.random.uniform(low=0.0, high=1.0, size=50)
    sigs = np.random.uniform(low=0.05, high=0.4, size=50)
    jits = np.random.uniform(low=0.05, high=0.2, size=50)

    # dictionaries for time series and their metadata
    tsdict={}
    metadict={}
    for i, m, s, j in zip(range(50), mus, sigs, jits):
        meta, tsrs = tsmaker(m, s, j)
        # the primary key format is ts-1, ts-2, etc
        pk = "ts-{}".format(i)
        tsdict[pk] = tsrs
        meta['vp'] = False # augment metadata with a boolean asking if this is a  VP.
        metadict[pk] = meta

    # choose 5 distinct vantage point time series
    vpkeys = ["ts-{}".format(i) for i in np.random.choice(range(50), size=5, replace=False)]
    for i in range(5):
        # add 5 triggers to upsert distances to these vantage points
        request_add_trigger('corr', 'insert_ts', ["d_vp-{}".format(i)], tsdict[vpkeys[i]])
        # change the metadata for the vantage points to have meta['vp']=True
        metadict[vpkeys[i]]['vp']=True
    # Having set up the triggers, now inser the time series, and upsert the metadata
    for k in tsdict:
        request_insert(k, tsdict[k])
        request_upsert_meta(k, metadict[k])

    print("UPSERTS FINISHED")
    print('---------------------')
    print("STARTING SELECTS")

    print('---------DEFAULT------------')
    request_select()

    #in this version, select has sprouted an additional keyword argument
    # to allow for sorting. Limits could also be enforced through this.
    print('---------ADDITIONAL------------')
    request_select(additional={'sort_by': '-order'})

    print('----------ORDER FIELD-----------')
    results = request_select(fields=['order'])
    for k in results:
        print(k, results[k])

    print('---------ALL FILEDS------------')
    request_select(fields=[])

    print('------------TS with order 1---------')
    request_select({'order': 1}, fields=['ts'])

    print('------------All fields, blarg 1 ---------')
    request_select({'blarg': 1}, fields=[])

    print('------------order 1 blarg 2 no fields---------')
    bla = request_select({'order': 1, 'blarg': 2})
    print(bla)

    print('------------order >= 4  order, blarg and mean sent back, also sorted---------')
    results = request_select({'order': {'>=': 4}}, fields=['order', 'blarg', 'mean'], additional={'sort_by': '-order'})
    for k in results:
        print(k, results[k])

    print('------------order 1 blarg >= 1 fields blarg and std---------')
    results = request_select({'blarg': {'>=': 1}, 'order': 1}, fields=['blarg', 'std'])
    for k in results:
        print(k, results[k])

    print('------now computing vantage point stuff---------------------')
    print("VPS", vpkeys)

    #we first create a query time series.
    _, query = tsmaker(0.5, 0.2, 0.1)

    # your code here begins

    # Step 1: in the vpdist key, get  distances from query to vantage points
    # this is an augmented select
    vpdist = {}
    result_distance = request_augmented_select('corr', ['vpdist'], query, {'vp':{'==':True}})
    vpdist = {v:result_distance[v]['vpdist'] for v in vpkeys}
    print('vpdist is: ', vpdist)
    #1b: choose the lowest distance vantage point
    # you can do this in local code
    nearest_vp_to_query = min(vpkeys, key=lambda v:vpdist[v])

    # Step 2: find all time series within 2*d(query, nearest_vp_to_query)
    #this is an augmented select to the same proc in correlation
    radius = 2*vpdist[nearest_vp_to_query]
    print('Radius is {}'.format(radius))
    # Find the reative index of the nearest_vp_to_query
    realtive_index_vp = vpkeys.index(nearest_vp_to_query)
    results = request_augmented_select('corr', ['towantedvp'], query,
                                         {'d_vp-{}'.format(realtive_index_vp):{'<=': radius}})

    #2b: find the smallest distance amongst this ( or k smallest)
    #you can do this in local code
    nearestwanted = min(results.keys(), key=lambda k:results[k]['towantedvp'])
    print('nearest is {} distance is {}'.format(nearestwanted, results[nearestwanted]['towantedvp']))
    #your code here ends
    # plot the timeseries to see visually how we did.
    import matplotlib.pyplot as plt
    plt.plot(query)
    plt.plot(tsdict[nearestwanted])
    plt.show()

    # Similarity search
    nearestwanted = request_similarity_search(query, 3)
    print('Nearest wanted ', nearestwanted)

if __name__=='__main__':
    main()
