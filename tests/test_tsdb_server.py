from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
import time
identity = lambda x: x

schema = {
  'pk':         {'convert': identity,   'index': None},
  'ts':         {'convert': identity,   'index': None},
  'order':      {'convert': int,        'index': 1},
  'blarg':      {'convert': int,        'index': 1},
  'useless':    {'convert': identity,   'index': None},
  'mean':       {'convert': float,      'index': 1},
  'std':        {'convert': float,      'index': 1},
  'vp':         {'convert': bool,       'index': 1}
}

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
    meta['order'] = int(np.random.choice(
        [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    meta['vp'] = False  # initialize vantage point indicator as negative

    # generate time series data
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j * np.random.randn(100)

    # return time series and metadata
    return meta, TimeSeries(t, v)


def test_server():

    ########################################
    #
    # database initializations
    #
    ########################################

    # augment the schema by adding column for one vantage point
    schema['d_vp-1'] = {'convert': float, 'index': 1}

    # initialize database
    db = DictDB(schema, 'pk')

    # initialize server
    server = TSDBServer(db)
    assert server.db == db
    assert server.port == 9999

    # initialize protocol
    protocol = TSDBProtocol(server)
    assert protocol.server == server

    ########################################
    #
    # create dummy data for testing
    #
    ########################################

    # a manageable number of test time series
    num_ts = 25
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

    # randomly choose one time series as the vantage point
    random_vp = np.random.choice(range(num_ts))
    vpkey = "ts-{}".format(random_vp)
    metadict["ts-{}".format(random_vp)]['vp'] = True

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
    # test time series insert
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
    # test metadata upsert
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
    # test select operations
    #
    ########################################

    # select all database entries; no metadata fields

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
    if len(payload) > 0:
        assert list(payload[list(payload.keys())[0]].keys()) == []
        assert sorted(payload.keys()) == ts_keys

    # select all database entries; all metadata fields

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': [], 'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, [], None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    if len(payload) > 0:
        assert (sorted(list(payload[list(payload.keys())[0]].keys())) ==
                ['blarg', 'order', 'pk', 'vp'])
        assert sorted(payload.keys()) == ts_keys

    # select all database entries; all invalid metadata fields

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': ['wrong', 'oops'],
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, ['wrong', 'oops'], None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    if len(payload) > 0:
        assert sorted(list(payload[list(payload.keys())[0]].keys())) == []
        assert sorted(payload.keys()) == ts_keys

    # select all database entries; some invalid metadata fields

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': ['not_there', 'blarg'],
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, ['not_there', 'blarg'], None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    if len(payload) > 0:
        assert list(payload[list(payload.keys())[0]].keys()) == ['blarg']
        assert sorted(payload.keys()) == ts_keys

    # select all database entries; specific metadata fields

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': ['blarg', 'order'],
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, ['blarg', 'order'], None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    if len(payload) > 0:
        assert (sorted(list(payload[list(payload.keys())[0]].keys())) ==
                ['blarg', 'order'])
        assert sorted(payload.keys()) == ts_keys

    # not present based on how time series were generated

    # package the operation
    op = {'op': 'select', 'md': {'order': 10}, 'fields': None,
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'order': 10}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 0

    # not present based on how time series were generated

    # package the operation
    op = {'op': 'select', 'md': {'blarg': 0}, 'fields': None,
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'blarg': 0}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 0

    # multiple select criteria
    # not present based on how time series were generated

    # package the operation
    op = {'op': 'select', 'md': {'order': 10, 'blarg': 0}, 'fields': None,
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'order': 10, 'blarg': 0}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 0

    # operator select criteria
    # not present based on how time series were generated

    # package the operation
    op = {'op': 'select', 'md': {'order': {'>=': 10}}, 'fields': None,
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'order': {'>=': 10}}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 0

    # operator select criteria
    # present based on how time series were generated

    # package the operation
    op = {'op': 'select', 'md': {'order': {'<': 10}}, 'fields': None,
          'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({'order': {'<': 10}}, None, None)
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) > 0

    ########################################
    #
    # test trigger operations
    #
    ########################################

    # add trigger to calculate the distances to the vantage point

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'corr', 'onwhat': 'insert_ts',
          'target': ['d_vp-1'], 'arg': tsdict["ts-{}".format(random_vp)]}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger('corr', 'insert_ts', ['d_vp-1'],
                                   tsdict["ts-{}".format(random_vp)])
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # add dummy trigger

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'junk', 'onwhat': 'insert_ts',
          'target': None, 'arg': 'db:one:ts'}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger('junk', 'insert_ts', None, 'db:one:ts')
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # add stats trigger

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

    # try to add a trigger on an invalid event

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'junk', 'onwhat': 'stuff_happening',
          'target': None, 'arg': 'db:one:ts'}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger(
        'junk', 'stuff_happening', None, 'db:one:ts')
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # try to add a trigger to an invalid field

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'stats', 'onwhat': 'insert_ts',
          'target': ['mean', 'wrong_one'], 'arg': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger(
        'stats', 'insert_ts', ['mean', 'wrong_one'], None)
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # try to remove a trigger that doesn't exist

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'not_here', 'onwhat': 'insert_ts',
          'target': None, 'arg': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger('not_here', 'insert_ts', None, None)
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # try to remove a trigger on an invalid event

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'stats', 'onwhat': 'stuff_happening',
          'target': None, 'arg': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger('stats', 'stuff_happening', None, None)
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # check all triggers
    triggers = [t for t in server.triggers if len(server.triggers[t]) > 0]
    assert triggers == ['insert_ts']
    assert (sorted([t[0] for t in server.triggers['insert_ts']]) ==
            ['corr', 'junk', 'stats'])

    ########################################
    #
    # test augmented select operations
    #
    ########################################

    # remove trigger

    op = {'op': 'remove_trigger', 'proc': 'stats', 'onwhat': 'insert_ts'}
    # test that this is packaged as expected
    assert op == TSDBOp_RemoveTrigger('stats', 'insert_ts')
    # run operation
    result = protocol._remove_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # add a new time series

    # package the operation
    op = {'op': 'insert_ts', 'pk': 'test', 'ts': tsdict['ts-1']}
    # test that this is packaged as expected
    assert op == TSDBOp_InsertTS('test', tsdict['ts-1'])
    # run operation
    result = protocol._insert_ts(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # augmented select

    # package the operation
    op = {'op': 'augmented_select', 'proc': 'stats', 'target': ['mean', 'std'],
          'arg': None, 'md': {'pk': 'test'}, 'additional': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AugmentedSelect(
        'stats', ['mean', 'std'], None, {'pk': 'test'}, None)
    # run operation
    result = protocol._augmented_select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert len(payload) == 1
    payload_fields = list(payload[list(payload.keys())[0]].keys())
    assert 'mean' in payload_fields
    assert 'std' in payload_fields

    db = None
    server = None
    protocol = None
