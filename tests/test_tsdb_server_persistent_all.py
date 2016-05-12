from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
import time
import pytest

identity = lambda x: x

# index: 1 is binary tree index, 2 is bitmap index
schema = {
  'pk': {'type': 'str', 'convert': identity, 'index': None, 'values': None},
  'ts': {'type': 'int', 'convert': identity, 'index': None, 'values': None},
  'order': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
  'blarg': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
  'useless': {'type': 'int', 'convert': identity, 'index': 1, 'values': None},
  'mean': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
  'std': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
  'vp': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]},
  'deleted': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]}
}


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
    meta['order'] = int(np.random.choice(
        [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))

    # generate time series data
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j * np.random.randn(100)

    # return time series and metadata
    return meta, TimeSeries(t, v)


def test_server():

    ########################################
    #
    # set up
    #
    ########################################

    # initialize file system
    data_dir = 'db_files/default/'
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    filelist = [data_dir + f for f in os.listdir(data_dir)]
    for f in filelist:
        os.remove(f)

    # initialize database
    db = PersistentDB(schema, 'pk', 100)

    # initialize server
    server = TSDBServer(db)
    assert server.db == db
    assert server.port == 9999

    # initialize protocol
    protocol = TSDBProtocol(server)
    assert protocol.server == server

    # parameters for testing
    num_ts = 25
    num_vps = 5

    ########################################
    #
    # create dummy data for testing
    #
    ########################################

    # a manageable number of test time series
    mus = np.random.uniform(low=0.0, high=1.0, size=num_ts)
    sigs = np.random.uniform(low=0.05, high=0.4, size=num_ts)
    jits = np.random.uniform(low=0.05, high=0.2, size=num_ts)

    # initialize dictionaries for time series and their metadata
    tsdict = {}
    metadict = {}

    # fill dictionaries with randomly generated entries for database
    for i, m, s, j in zip(range(num_ts), mus, sigs, jits):
        meta, tsrs = tsmaker(m, s, j)  # generate data
        pk = "ts-{}".format(i)  # generate primary key
        tsdict[pk] = tsrs  # store time series data
        metadict[pk] = meta  # store metadata

    # for testing later on
    ts_keys = sorted(tsdict.keys())

    # randomly choose time series as vantage points
    vpkeys = sorted(list(np.random.choice(ts_keys, size=num_vps, replace=False)))
    vpdist = ['d_vp_{}'.format(i) for i in vpkeys]

    ########################################
    #
    # for all tests below:
    # - package the operation
    # - test that this is packaged as expected
    # - run the operation
    # - unpack the results of running the operation
    # - test that the return values are as expected
    #
    ########################################

    ########################################
    #
    # add stats trigger
    #
    ########################################

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'stats', 'onwhat': 'insert_ts',
          'target': ['mean', 'std'], 'arg': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger('stats', 'insert_ts', ['mean', 'std'], None)
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    ########################################
    #
    # insert time series
    #
    ########################################

    for k in tsdict:
        # package the operation
        op = {'op': 'insert_ts', 'pk': k, 'ts': tsdict[k]}
        # test that this is packaged as expected
        assert op == TSDBOp_InsertTS(k, tsdict[k])
        # run operation
        result = protocol._insert_ts(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        assert status == TSDBStatus.OK
        assert payload is None

    ########################################
    #
    # upsert metadata
    #
    ########################################

    for k in metadict:
        # package the operation
        op = {'op': 'upsert_meta', 'pk': k, 'md': metadict[k]}
        # test that this is packaged as expected
        assert op == TSDBOp_UpsertMeta(k, metadict[k])
        # run operation
        result = protocol._upsert_meta(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        assert status == TSDBStatus.OK
        assert payload is None

    ########################################
    #
    # add vantage points
    #
    ########################################

    # add the time series as vantage points

    for i in range(num_vps):

        # package the operation
        op = {'op': 'insert_vp', 'pk': vpkeys[i]}
        # test that this is packaged as expected
        assert op == TSDBOp_InsertVP(vpkeys[i])
        # run operation
        result = protocol._insert_vp(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        assert status == TSDBStatus.OK
        assert payload is None

    ########################################
    #
    # test that all data matches
    #
    ########################################

    for k in tsdict:

        # time series data

        # package the operation
        op = {'op': 'select', 'md': {'pk': k}, 'fields': ['ts'],
              'additional': None}
        # test that this is packaged as expected
        assert op == TSDBOp_Select({'pk': k}, ['ts'], None)
        # run operation
        result = protocol._select(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        assert status == TSDBStatus.OK
        assert payload[k]['ts'] == tsdict[k]

        # all other metadata

        op = {'op': 'select', 'md': {'pk': k}, 'fields': [],
              'additional': None}
        # test that this is packaged as expected
        assert op == TSDBOp_Select({'pk': k}, [], None)
        # run operation
        result = protocol._select(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        for field in metadict[k]:
            assert metadict[k][field] == payload[k][field]

    ########################################
    #
    # test that vantage points match
    #
    ########################################

    # package the operation
    op = {'op': 'select', 'md': {'vp': True}, 'fields': None,
          'additional': {'sort_by': '+pk'}}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'vp': True}, None, {'sort_by': '+pk'})
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert list(payload.keys()) == vpkeys

    ########################################
    #
    # test that vantage point distance fields have been created
    #
    ########################################

    # package the operation
    op = {'op': 'select', 'md': {'vp': True}, 'fields': vpdist,
          'additional': {'sort_by': '+pk', 'limit': 1}}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'vp': True}, vpdist,
                               {'sort_by': '+pk', 'limit': 1})
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert sorted(list(list(payload.values())[0].keys())) == vpdist

    ########################################
    #
    # store similarity search results
    #
    ########################################

    # randomly generate query time series
    _, query = tsmaker(np.random.uniform(low=0.0, high=1.0),
                       np.random.uniform(low=0.05, high=0.4),
                       np.random.uniform(low=0.05, high=0.2))

    # vantage point similarity

    # package the operation
    op = {'op': 'vp_similarity_search', 'query': query, 'top': 1}
    # test that this is packaged as expected
    assert op == TSDBOp_VPSimilaritySearch(query, 1)
    # run operation
    result = protocol._vp_similarity_search(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 1
    similarity_vp = payload.copy()

    # isax similarity

    # package the operation
    op = {'op': 'isax_similarity_search', 'query': query}
    # test that this is packaged as expected
    assert op == TSDBOp_iSAXSimilaritySearch(query)
    # run operation
    result = protocol._isax_similarity_search(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert ((status == TSDBStatus.OK and len(payload) == 1) or
            (status == TSDBStatus.NO_MATCH and payload is None))
    if payload is None:
        similarity_isax = None
    else:
        similarity_isax = payload.copy()

    ########################################
    #
    # close database/server
    #
    ########################################

    db.close()
    server = None
    protocol = None

    ########################################
    #
    # restart database/server
    #
    ########################################

    # try to initialize with the wrong time series length
    with pytest.raises(ValueError):
        db = PersistentDB(schema, 'pk', 101)

    # initialize database
    db = PersistentDB(schema, 'pk', 100)

    # initialize server
    server = TSDBServer(db)
    assert server.db == db
    assert server.port == 9999

    # initialize protocol
    protocol = TSDBProtocol(server)
    assert protocol.server == server

    ########################################
    #
    # test that all the primary keys are present
    #
    ########################################

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': None, 'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == num_ts

    ########################################
    #
    # test that all data matches
    #
    ########################################

    for k in tsdict:

        # time series data

        # package the operation
        op = {'op': 'select', 'md': {'pk': k}, 'fields': ['ts'],
              'additional': None}
        # test that this is packaged as expected
        assert op == TSDBOp_Select({'pk': k}, ['ts'], None)
        # run operation
        result = protocol._select(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        assert status == TSDBStatus.OK
        assert payload[k]['ts'] == tsdict[k]

        # all other metadata

        op = {'op': 'select', 'md': {'pk': k}, 'fields': [],
              'additional': None}
        # test that this is packaged as expected
        assert op == TSDBOp_Select({'pk': k}, [], None)
        # run operation
        result = protocol._select(op)
        # unpack results
        status, payload = result['status'], result['payload']
        # test that return values are as expected
        for field in metadict[k]:
            assert metadict[k][field] == payload[k][field]

    ########################################
    #
    # test that vantage points match
    #
    ########################################

    # package the operation
    op = {'op': 'select', 'md': {'vp': True}, 'fields': None,
          'additional': {'sort_by': '+pk'}}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'vp': True}, None, {'sort_by': '+pk'})
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert list(payload.keys()) == vpkeys

    ########################################
    #
    # test that vantage point distance fields have been created
    #
    ########################################

    # package the operation
    op = {'op': 'select', 'md': {'vp': True}, 'fields': vpdist,
          'additional': {'sort_by': '+pk', 'limit': 1}}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'vp': True}, vpdist,
                               {'sort_by': '+pk', 'limit': 1})
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert sorted(list(list(payload.values())[0].keys())) == vpdist

    ########################################
    #
    # store similarity search results
    #
    ########################################

    # vantage point similarity

    # package the operation
    op = {'op': 'vp_similarity_search', 'query': query, 'top': 1}
    # test that this is packaged as expected
    assert op == TSDBOp_VPSimilaritySearch(query, 1)
    # run operation
    result = protocol._vp_similarity_search(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 1
    assert list(payload)[0] == list(similarity_vp)[0]

    # isax similarity

    # package the operation
    op = {'op': 'isax_similarity_search', 'query': query}
    # test that this is packaged as expected
    assert op == TSDBOp_iSAXSimilaritySearch(query)
    # run operation
    result = protocol._isax_similarity_search(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert ((status == TSDBStatus.OK and len(payload) == 1) or
            (status == TSDBStatus.NO_MATCH and payload is None))
    if status == TSDBStatus.NO_MATCH:
        assert payload == similarity_isax
    else:
        assert list(payload)[0] == list(similarity_isax)[0]

    ########################################
    #
    # isax tree
    #
    ########################################

    # package the operation
    op = {'op': 'isax_tree'}
    # test that this is packaged as expected
    assert op == TSDBOp_iSAXTree()
    # run operation
    result = protocol._isax_tree(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert isinstance(payload, str)
    assert len(payload) > 0
    assert payload[:6] != 'ERROR'

    ########################################
    #
    # tear down
    #
    ########################################

    db.close()
    server = None
    protocol = None
