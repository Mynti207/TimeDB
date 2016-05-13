import unittest
import asynctest
import asyncio
from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
import time
import subprocess
import signal

########################################
#
# we use unit tests instead of pytests, because they facilitate the build-up
# and tear-down of the server (and avoid the tests hanging)
#
# adapted from go_server.py and go_client.py
# subprocess reference: https://docs.python.org/2/library/subprocess.html
#
# note: server code run through the subprocess is not reflected in coverage
#
########################################


class test_client(asynctest.TestCase):

    # database initializations
    def setUp(self):

        # persistent database parameters
        self.data_dir = 'db_files'
        self.db_name = 'default'
        self.ts_length = 100

        # clear file system for testing
        dir_clean = self.data_dir + '/' + self.db_name + '/'
        if not os.path.exists(dir_clean):
            os.makedirs(dir_clean)
        filelist = [dir_clean + f for f in os.listdir(dir_clean)]
        for f in filelist:
            os.remove(f)

        # initialize & run the server
        self.server = subprocess.Popen(
            ['python', 'go_server_persistent.py',
                '--ts_length', str(self.ts_length),
                '--data_dir', self.data_dir, '--db_name', self.db_name])
        time.sleep(5)

        # initialize database client
        self.client = TSDBClient()

        # parameters for testing
        self.num_ts = 25
        self.num_vps = 5

    # avoids the server hanging
    def tearDown(self):
        os.kill(self.server.pid, signal.SIGINT)
        time.sleep(5)
        self.client = None

    def tsmaker(self, m, s, j):
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
        v = norm.pdf(t, m, s) + j * np.random.randn(self.ts_length)

        # return time series and metadata
        return meta, TimeSeries(t, v)

    # run client tests
    async def test_client_ops(self):

        ########################################
        #
        # create dummy data for testing
        #
        ########################################

        # a manageable number of test time series
        mus = np.random.uniform(low=0.0, high=1.0, size=self.num_ts)
        sigs = np.random.uniform(low=0.05, high=0.4, size=self.num_ts)
        jits = np.random.uniform(low=0.05, high=0.2, size=self.num_ts)

        # initialize dictionaries for time series and their metadata
        tsdict = {}
        metadict = {}

        # fill dictionaries with randomly generated entries for database
        for i, m, s, j in zip(range(self.num_ts), mus, sigs, jits):
            meta, tsrs = self.tsmaker(m, s, j)  # generate data
            pk = "ts-{}".format(i)  # generate primary key
            tsdict[pk] = tsrs  # store time series data
            metadict[pk] = meta  # store metadata

        # for testing later on
        ts_keys = sorted(tsdict.keys())

        # randomly choose time series as vantage points
        vpkeys = sorted(list(np.random.choice(
            ts_keys, size=self.num_vps, replace=False)))
        vpdist = ['d_vp_' + i for i in vpkeys]

        #######################################
        #
        # add stats trigger
        #
        #######################################

        status, payload = await self.client.add_trigger(
            'stats', 'insert_ts', ['mean', 'std'], None)
        assert status == TSDBStatus.OK
        assert payload is None

        ########################################
        #
        # insert time series
        #
        ########################################

        # insert the time series and upsert the metadata
        for k in tsdict:
            status, payload = await self.client.insert_ts(k, tsdict[k])
            assert status == TSDBStatus.OK
            assert payload is None

        ########################################
        #
        # upsert metadata
        #
        ########################################

        # insert the time series and upsert the metadata
        for k in tsdict:
            status, payload = await self.client.upsert_meta(k, metadict[k])
            assert status == TSDBStatus.OK
            assert payload is None

        ########################################
        #
        # add vantage points
        #
        ########################################

        # add the time series as vantage points
        for i in range(self.num_vps):
            status, payload = await self.client.insert_vp(vpkeys[i])
            assert status == TSDBStatus.OK
            assert payload is None

        ########################################
        #
        # test that all data matches
        #
        ########################################

        # note: recast time series objects as shouldn't really be
        # communicating directly with client!
        for k in tsdict:

            # time series data
            status, payload = await self.client.select({'pk': k}, ['ts'], None)
            assert status == TSDBStatus.OK
            assert TimeSeries(*payload[k]['ts']) == tsdict[k]

            # all other metadata
            status, payload = await self.client.select({'pk': k}, [])
            for field in metadict[k]:
                assert metadict[k][field] == payload[k][field]

        ########################################
        #
        # test that vantage points match
        #
        ########################################

        status, payload = await self.client.select(
            {'vp': True}, None, {'sort_by': '+pk'})
        assert status == TSDBStatus.OK
        assert list(payload.keys()) == vpkeys

        ########################################
        #
        # test that vantage point distance fields have been created
        #
        ########################################

        use_keys = vpdist[:]  # avoid namespace issues
        status, payload = await self.client.select(
            {'vp': True}, use_keys, {'sort_by': '+pk', 'limit': 1})
        assert status == TSDBStatus.OK
        assert sorted(list(list(payload.values())[0].keys())) == vpdist

        ########################################
        #
        # store similarity search results
        #
        ########################################

        # randomly generate query time series
        _, query = self.tsmaker(np.random.uniform(low=0.0, high=1.0),
                                np.random.uniform(low=0.05, high=0.4),
                                np.random.uniform(low=0.05, high=0.2))

        # vantage point similarity
        status, payload = await self.client.vp_similarity_search(query, 1)
        assert status == TSDBStatus.OK
        assert len(payload) == 1
        similarity_vp = payload.copy()

        # isax similarity
        status, payload = await self.client.isax_similarity_search(query)
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

        os.kill(self.server.pid, signal.SIGINT)
        time.sleep(5)
        self.client = None
        time.sleep(5)

        ########################################
        #
        # reload database/server
        #
        ########################################

        # initialize & run the server
        self.server = subprocess.Popen(
            ['python', 'go_server_persistent.py',
                '--ts_length', str(self.ts_length),
                '--data_dir', self.data_dir, '--db_name', self.db_name])
        time.sleep(5)

        # initialize database client
        self.client = TSDBClient()

        ########################################
        #
        # test that all data matches
        #
        ########################################

        # note: recast time series objects as shouldn't really be
        # communicating directly with client!
        for k in tsdict:

            # time series
            status, payload = await self.client.select({'pk': k}, ['ts'], None)
            assert status == TSDBStatus.OK
            assert TimeSeries(*payload[k]['ts']) == tsdict[k]

            # all other metadata
            status, payload = await self.client.select({'pk': k}, [])
            for field in metadict[k]:
                assert metadict[k][field] == payload[k][field]

        ########################################
        #
        # test that vantage points match
        #
        ########################################

        status, payload = await self.client.select(
            {'vp': True}, None, {'sort_by': '+pk'})
        assert status == TSDBStatus.OK
        assert list(payload.keys()) == vpkeys

        ########################################
        #
        # test that vantage point distance fields have been created
        #
        ########################################

        use_keys = vpdist[:]  # avoid namespace issues
        status, payload = await self.client.select(
            {'vp': True}, use_keys, {'sort_by': '+pk', 'limit': 1})
        assert status == TSDBStatus.OK
        assert sorted(list(list(payload.values())[0].keys())) == vpdist

        ########################################
        #
        # store similarity search results
        #
        ########################################

        # vantage point similarity
        status, payload = await self.client.vp_similarity_search(query, 1)
        assert status == TSDBStatus.OK
        assert len(payload) == 1
        assert list(payload)[0] == list(similarity_vp)[0]

        # isax similarity
        status, payload = await self.client.isax_similarity_search(query)
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

        status, payload = await self.client.isax_tree()
        assert isinstance(payload, str)
        assert len(payload) > 0
        assert payload[:6] != 'ERROR'

if __name__ == '__main__':
    unittest.main()
