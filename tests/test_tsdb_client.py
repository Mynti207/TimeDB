import unittest
import asynctest
import asyncio
from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
import time
import subprocess


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

########################################
#
# we use unit tests instead of pytests, because they facilitate the build-up
# and tear-down of the server (and avoid the tests hanging)
#
# adapted from go_server.py and go_client.py
# subprocess reference: https://docs.python.org/2/library/subprocess.html
#
########################################

class test_client(asynctest.TestCase):

    # database initializations
    def setUp(self):

        # augment the schema by adding column for one vantage point
        schema['d_vp-1'] = {'convert': float, 'index': 1}

        # initialize the database
        self.db = DictDB(schema, 'pk')

        # initialize & run the server
        self.server = subprocess.Popen(['python', 'go_server.py'])
        time.sleep(5)

        # initialize database client
        self.client = TSDBClient()

    # avoids the server hanging
    def tearDown(self):
        self.server.terminate()
        time.sleep(5)

    # run client tests
    async def test_client_ops(self):

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
        # test trigger operations
        #
        ########################################

        # add trigger to calculate the distances to the vantage point
        status, payload = await self.client.add_trigger(
            'corr', 'insert_ts', ['d_vp-1'], tsdict["ts-{}".format(random_vp)])
        assert status == TSDBStatus.OK
        assert payload is None

        # add dummy trigger
        status, payload = await self.client.add_trigger(
            'junk', 'insert_ts', None, 'db:one:ts')
        assert status == TSDBStatus.OK
        assert payload is None

        # add stats trigger
        status, payload = await self.client.add_trigger(
            'stats', 'insert_ts', ['mean', 'std'], None)
        assert status == TSDBStatus.OK
        assert payload is None

        # try to add a trigger on an invalid event
        status, payload = await self.client.add_trigger(
            'junk', 'stuff_happening', None, 'db:one:ts')
        assert status == TSDBStatus.INVALID_OPERATION
        assert payload is None

        # try to add a trigger to an invalid field
        status, payload = await self.client.add_trigger(
            'stats', 'insert_ts', ['mean', 'wrong_one'], None)
        assert status == TSDBStatus.INVALID_OPERATION
        assert payload is None

        # try to remove a trigger that doesn't exist
        status, payload = await self.client.remove_trigger(
            'not_here', 'insert_ts')
        assert status == TSDBStatus.INVALID_OPERATION
        assert payload is None

        # try to remove a trigger on an invalid event
        status, payload = await self.client.remove_trigger(
            'stats', 'stuff_happening')
        assert status == TSDBStatus.INVALID_OPERATION
        assert payload is None

        ########################################
        #
        # test time series insertion
        #
        ########################################

        # insert the time series and upsert the metadata
        for k in tsdict:
            status, payload = await self.client.insert_ts(k, tsdict[k])
            assert status == TSDBStatus.OK
            assert payload is None

        # try to add duplicate primary key
        status, payload = await self.client.insert_ts(vpkey, tsdict[vpkey])
        assert status == TSDBStatus.INVALID_KEY
        assert payload is None

        ########################################
        #
        # test metadata upsertion
        #
        ########################################

        # insert the time series and upsert the metadata
        for k in tsdict:
            status, payload = await self.client.upsert_meta(k, metadict[k])
            assert status == TSDBStatus.OK
            assert payload is None

        ########################################
        #
        # test select operations
        #
        ########################################

        # select all database entries; no metadata fields
        status, payload = await self.client.select()
        assert status == TSDBStatus.OK
        if len(payload) > 0:
            assert list(payload[list(payload.keys())[0]].keys()) == []
            assert sorted(payload.keys()) == ts_keys

        # select all database entries; all metadata fields
        status, payload = await self.client.select(fields=[])
        assert status == TSDBStatus.OK
        if len(payload) > 0:
            assert (sorted(list(payload[list(payload.keys())[0]].keys())) ==
                    ['blarg', 'd_vp-1', 'mean', 'order', 'pk', 'std', 'vp'])
            assert sorted(payload.keys()) == ts_keys

        # select all database entries; all invalid metadata fields
        status, payload = await self.client.select(
            fields=['wrong', 'oops'])
        assert status == TSDBStatus.OK
        if len(payload) > 0:
            assert sorted(list(payload[list(payload.keys())[0]].keys())) == []
            assert sorted(payload.keys()) == ts_keys

        # select all database entries; some invalid metadata fields
        status, payload = await self.client.select(fields=['not_there', 'std'])
        assert status == TSDBStatus.OK
        if len(payload) > 0:
            assert list(payload[list(payload.keys())[0]].keys()) == ['std']
            assert sorted(payload.keys()) == ts_keys

        # select all database entries; specific metadata fields
        status, payload = await self.client.select(fields=['blarg', 'mean'])
        assert status == TSDBStatus.OK
        if len(payload) > 0:
            assert (sorted(list(payload[list(payload.keys())[0]].keys())) ==
                    ['blarg', 'mean'])
            assert sorted(payload.keys()) == ts_keys

        # not present based on how time series were generated
        status, payload = await self.client.select({'order': 10})
        assert status == TSDBStatus.OK
        assert len(payload) == 0

        # not present based on how time series were generated
        status, payload = await self.client.select({'blarg': 0})
        assert status == TSDBStatus.OK
        assert len(payload) == 0

        # multiple select criteria
        # not present based on how time series were generated
        status, payload = await self.client.select({'order': 10, 'blarg': 0})
        assert status == TSDBStatus.OK
        assert len(payload) == 0

        # operator select criteria
        # not present based on how time series were generated
        status, payload = await self.client.select({'order': {'>=': 10}})
        assert status == TSDBStatus.OK
        assert len(payload) == 0

        # operator select criteria
        # present based on how time series were generated
        status, payload = await self.client.select({'order': {'<': 10}})
        assert status == TSDBStatus.OK
        assert len(payload) > 0

        ########################################
        #
        # test augmented select operations
        #
        ########################################

        # remove trigger
        status, payload = await self.client.remove_trigger(
            'stats', 'insert_ts')
        assert status == TSDBStatus.OK
        assert payload is None

        # add a new time series (without trigger)
        status, payload = await self.client.insert_ts(
            'test', tsdict['ts-1'])
        assert status == TSDBStatus.OK
        assert payload is None

        # check that mean and std haven't been added
        status, payload = await self.client.select(
            {'pk': 'test'}, fields=[])
        assert status == TSDBStatus.OK
        assert len(payload) == 1
        payload_fields = list(payload[list(payload.keys())[0]].keys())
        assert 'mean' not in payload_fields
        assert 'std' not in payload_fields

        # now let's add mean and std back
        status, payload = await self.client.augmented_select(
            'stats', ['mean', 'std'], metadata_dict={'pk': 'test'})
        assert status == TSDBStatus.OK
        assert len(payload) == 1
        payload_fields = list(payload[list(payload.keys())[0]].keys())
        assert 'mean' in payload_fields
        assert 'std' in payload_fields
