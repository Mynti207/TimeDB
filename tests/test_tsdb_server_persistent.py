from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
import time

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
    meta['vp'] = False  # initialize vantage point indicator as negative

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

    idx = np.random.choice(list(tsdict.keys()))

    # try to insert a duplicate primary key
    op = {'op': 'insert_ts', 'pk': idx, 'ts': tsdict[idx]}
    # test that this is packaged as expected
    assert op == TSDBOp_InsertTS(idx, tsdict[idx])
    # run operation
    result = protocol._insert_ts(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_KEY
    assert payload is None

    # ########################################
    # #
    # # test time series deletion
    # #
    # ########################################
    #
    # idx = np.random.choice(list(tsdict.keys()))
    #
    # # delete a valid time series
    #
    # # package the operation
    # op = {'op': 'delete_ts', 'pk': idx}
    # # test that this is packaged as expected
    # assert op == TSDBOp_DeleteTS(idx)
    # # run operation
    # result = protocol._delete_ts(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert payload is None
    #
    # # check that it isn't present any more
    #
    # # package the operation
    # op = {'op': 'select', 'md': {'pk': idx}, 'fields': None,
    #       'additional': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_Select({'pk': idx}, None, None)
    # # run operation
    # result = protocol._select(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) == 0
    #
    # # add it back in
    #
    # # package the operation
    # op = {'op': 'insert_ts', 'pk': idx, 'ts': tsdict[idx]}
    # # test that this is packaged as expected
    # assert op == TSDBOp_InsertTS(idx, tsdict[idx])
    # # run operation
    # result = protocol._insert_ts(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert payload is None
    #
    # # check that it's present now
    #
    # # package the operation
    # op = {'op': 'select', 'md': {'pk': idx}, 'fields': None,
    #       'additional': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_Select({'pk': idx}, None, None)
    # # run operation
    # result = protocol._select(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) == 1
    #
    # # delete an invalid time series
    #
    # # package the operation
    # op = {'op': 'delete_ts', 'pk': 'mistake'}
    # # test that this is packaged as expected
    # assert op == TSDBOp_DeleteTS('mistake')
    # # run operation
    # result = protocol._delete_ts(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.INVALID_KEY
    # assert payload is None

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

    # select all database entries; no metadata fields; sort by primary key

    # package the operation
    op = {'op': 'select', 'md': {}, 'fields': None,
          'additional': {'sort_by': '+pk'}}
    # test that this is packaged as expected
    assert op == TSDBOp_Select({}, None, {'sort_by': '+pk'})
    # run operation
    result = protocol._select(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    if len(payload) > 0:
        assert list(payload[list(payload.keys())[0]].keys()) == []
        assert list(payload.keys()) == ts_keys

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
                ['blarg', 'mean', 'order', 'pk', 'std', 'useless', 'vp'])
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
    op = {'op': 'remove_trigger', 'proc': 'not_here', 'onwhat': 'insert_ts',
          'target': None}
    # test that this is packaged as expected
    assert op == TSDBOp_RemoveTrigger('not_here', 'insert_ts', None)
    # run operation
    result = protocol._remove_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # try to remove a trigger on an invalid event

    # package the operation
    op = {'op': 'remove_trigger', 'proc': 'stats', 'onwhat': 'stuff_happening',
          'target': None}
    # test that this is packaged as expected
    assert op == TSDBOp_RemoveTrigger('stats', 'stuff_happening', None)
    # run operation
    result = protocol._remove_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.INVALID_OPERATION
    assert payload is None

    # try to remove a trigger associated with a particular target
    # (used to delete vantage point representation)

    # package the operation
    op = {'op': 'remove_trigger', 'proc': 'stats', 'onwhat': 'insert_ts',
          'target': ['mean', 'std']}
    # test that this is packaged as expected
    assert op == TSDBOp_RemoveTrigger('stats', 'insert_ts', ['mean', 'std'])
    # run operation
    result = protocol._remove_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # add trigger back in

    # package the operation
    op = {'op': 'add_trigger', 'proc': 'stats', 'onwhat': 'insert_ts',
          'target': ['mean', 'std'], 'arg': None}
    # test that this is packaged as expected
    assert op == TSDBOp_AddTrigger(
        'stats', 'insert_ts', ['mean', 'std'], None)
    # run operation
    result = protocol._add_trigger(op)
    # unpack results
    status, payload = result['status'], result['payload']
    # test that return values are as expected
    assert status == TSDBStatus.OK
    assert payload is None

    # check all triggers
    triggers = [k for k, v in server.db.triggers.items() if len(v) > 0]
    assert triggers == ['insert_ts']
    assert (sorted([t[0] for t in server.db.triggers['insert_ts']]) ==
            ['junk', 'stats'])

    # ########################################
    # #
    # # test augmented select operations
    # #
    # ########################################
    #
    # # remove trigger
    #
    # op = {'op': 'remove_trigger', 'proc': 'stats', 'onwhat': 'insert_ts',
    #       'target': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_RemoveTrigger('stats', 'insert_ts', None)
    # # run operation
    # result = protocol._remove_trigger(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert payload is None
    #
    # # add a new time series
    #
    # # package the operation
    # op = {'op': 'insert_ts', 'pk': 'test', 'ts': tsdict['ts-1']}
    # # test that this is packaged as expected
    # assert op == TSDBOp_InsertTS('test', tsdict['ts-1'])
    # # run operation
    # result = protocol._insert_ts(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert payload is None
    #
    # ########################################
    # #
    # # test augmented select
    # #
    # ########################################
    #
    # # package the operation
    # op = {'op': 'augmented_select', 'proc': 'stats', 'target': ['mean', 'std'],
    #       'arg': None, 'md': {'pk': 'test'}, 'additional': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_AugmentedSelect(
    #     'stats', ['mean', 'std'], None, {'pk': 'test'}, None)
    # # run operation
    # result = protocol._augmented_select(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) == 1
    # payload_fields = list(payload[list(payload.keys())[0]].keys())
    # assert 'mean' in payload_fields
    # assert 'std' in payload_fields

    ########################################
    #
    # test vantage point representation
    #
    ########################################

    # # pick a new time series to add as a vantage point
    #
    # # randomly choose time series as vantage points
    # vpkeys = list(np.random.choice(ts_keys, size=num_vps, replace=False))
    # distkeys = sorted(['d_vp_' + i for i in vpkeys])
    #
    # # add the time series as vantage points
    #
    # for i in range(num_vps):
    #
    #     # package the operation
    #     op = {'op': 'insert_vp', 'pk': vpkeys[i]}
    #     # test that this is packaged as expected
    #     assert op == TSDBOp_InsertVP(vpkeys[i])
    #     # run operation
    #     result = protocol._insert_vp(op)
    #     # unpack results
    #     status, payload = result['status'], result['payload']
    #     # test that return values are as expected
    #     assert status == TSDBStatus.OK
    #     assert payload is None

    # # check that the distance fields are now in the database
    #
    # # package the operation
    # op = {'op': 'select', 'md': {}, 'fields': distkeys, 'additional': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_Select({}, distkeys, None)
    # # run operation
    # result = protocol._select(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # if len(payload) > 0:
    #     assert (sorted(list(payload[list(payload.keys())[0]].keys())) ==
    #             distkeys)
    #
    # # try to add a time series that doesn't exist as a vantage point
    #
    # # package the operation
    # op = {'op': 'insert_vp', 'pk': 'mistake'}
    # # test that this is packaged as expected
    # assert op == TSDBOp_InsertVP('mistake')
    # # run operation
    # result = protocol._insert_vp(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.INVALID_KEY
    # assert payload is None
    #
    # # remove them all
    #
    # for i in range(num_vps):
    #
    #     # package the operation
    #     op = {'op': 'delete_vp', 'pk': vpkeys[i]}
    #     # test that this is packaged as expected
    #     assert op == TSDBOp_DeleteVP(vpkeys[i])
    #     # run operation
    #     result = protocol._delete_vp(op)
    #     # # unpack results
    #     status, payload = result['status'], result['payload']
    #     # test that return values are as expected
    #     assert status == TSDBStatus.OK
    #     assert payload is None
    #
    # # check that the distance fields are now not in the database
    #
    # # package the operation
    # op = {'op': 'select', 'md': {}, 'fields': distkeys, 'additional': None}
    # # test that this is packaged as expected
    # assert op == TSDBOp_Select({}, distkeys, None)
    # # run operation
    # result = protocol._select(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # if len(payload) > 0:
    #     assert (list(payload[list(payload.keys())[0]].keys()) == [])
    #
    # # try to delete a vantage point that doesn't exist
    #
    # # package the operation
    # op = {'op': 'delete_vp', 'pk': 'mistake'}
    # # test that this is packaged as expected
    # assert op == TSDBOp_DeleteVP('mistake')
    # # run operation
    # result = protocol._delete_vp(op)
    # # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.INVALID_KEY
    # assert payload is None
    #
    # # add them back in
    #
    # for i in range(num_vps):
    #
    #     # package the operation
    #     op = {'op': 'insert_vp', 'pk': vpkeys[i]}
    #     # test that this is packaged as expected
    #     assert op == TSDBOp_InsertVP(vpkeys[i])
    #     # run operation
    #     result = protocol._insert_vp(op)
    #     # unpack results
    #     status, payload = result['status'], result['payload']
    #     # test that return values are as expected
    #     assert status == TSDBStatus.OK
    #     assert payload is None
    #
    # ########################################
    # #
    # # test vantage point similarity search
    # #
    # ########################################
    #
    # # first create a query time series
    # _, query = tsmaker(np.random.uniform(low=0.0, high=1.0),
    #                    np.random.uniform(low=0.05, high=0.4),
    #                    np.random.uniform(low=0.05, high=0.2))
    #
    # # single closest time series
    #
    # # package the operation
    # op = {'op': 'vp_similarity_search', 'query': query, 'top': 1}
    # # test that this is packaged as expected
    # assert op == TSDBOp_VPSimilaritySearch(query, 1)
    # # run operation
    # result = protocol._vp_similarity_search(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) == 1
    #
    # # 5 closest time series
    #
    # # package the operation
    # op = {'op': 'vp_similarity_search', 'query': query, 'top': 5}
    # # test that this is packaged as expected
    # assert op == TSDBOp_VPSimilaritySearch(query, 5)
    # # run operation
    # result = protocol._vp_similarity_search(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) <= 5
    #
    # # run similarity search on an existing time series - should return itself
    #
    # # pick a random time series
    # idx = np.random.choice(list(tsdict.keys()))
    # # package the operation
    # op = {'op': 'vp_similarity_search', 'query': tsdict[idx], 'top': 1}
    # # test that this is packaged as expected
    # assert op == TSDBOp_VPSimilaritySearch(tsdict[idx], 1)
    # # run operation
    # result = protocol._vp_similarity_search(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # assert len(payload) == 1
    # assert list(payload)[0] == idx
    #
    # ########################################
    # #
    # # test isax functions
    # #
    # ########################################
    #
    # # run similarity search on an existing time series - should return itself
    #
    # # pick a random time series
    # idx = np.random.choice(list(tsdict.keys()))
    # # package the operation
    # op = {'op': 'isax_similarity_search', 'query': tsdict[idx]}
    # # test that this is packaged as expected
    # assert op == TSDBOp_iSAXSimilaritySearch(tsdict[idx])
    # # run operation
    # result = protocol._isax_similarity_search(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert status == TSDBStatus.OK
    # print('COMPARING', idx, 'WITH', payload)
    # assert len(payload) == 1
    # assert list(payload)[0] == idx
    #
    # # visualize tree representation
    #
    # # package the operation
    # op = {'op': 'isax_tree'}
    # # test that this is packaged as expected
    # assert op == TSDBOp_iSAXTree()
    # # run operation
    # result = protocol._isax_tree(op)
    # # unpack results
    # status, payload = result['status'], result['payload']
    # # test that return values are as expected
    # assert isinstance(payload, str)
    #
    # ########################################
    # #
    # # tear down
    # #
    # ########################################

    db = None
    server = None
    protocol = None
