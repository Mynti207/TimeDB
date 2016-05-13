import unittest
import asynctest
import asyncio
from timeseries import TimeSeries
import numpy as np
from scipy.stats import norm
from tsdb import *
from webserver import *
import time
import subprocess

########################################
#
# we use unit tests instead of pytests, because they facilitate the build-up
# and tear-down of the server (and avoid the tests hanging)
#
# adapted from go_server.py and go_client.py
# subprocess reference: https://docs.python.org/2/library/subprocess.html
#
# note: server and webserver code run through the subprocess is not reflected
# in coverage
#
########################################


class test_webinterface(asynctest.TestCase):

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

        # intialize webserver
        self.webserver = subprocess.Popen(['python', 'go_webserver.py'])
        time.sleep(5)

        # initialize web interface
        self.web_interface = WebInterface()

        # parameters for testing
        self.num_ts = 25
        self.num_vps = 5

    # avoids the server hanging
    def tearDown(self):
        self.server.terminate()
        time.sleep(5)
        self.webserver.terminate()
        time.sleep(5)

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
        meta['vp'] = False  # initialize vantage point indicator as negative

        # generate time series data
        t = np.arange(0.0, 1.0, 0.01)
        v = norm.pdf(t, m, s) + j * np.random.randn(self.ts_length)

        # return time series and metadata
        return meta, TimeSeries(t, v)

    # run client tests
    async def test_webinterface_ops(self):

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

        ########################################
        #
        # test trigger operations
        #
        ########################################

        # add dummy trigger
        results = self.web_interface.add_trigger(
            'junk', 'insert_ts', None, None)
        assert results == 'OK'

        # add stats trigger
        results = self.web_interface.add_trigger(
            'stats', 'insert_ts', ['mean', 'std'], None)
        assert results == 'OK'

        # try to add a trigger on an invalid event
        results = self.web_interface.add_trigger(
            'junk', 'stuff_happening', None, None)
        assert results == 'ERROR: INVALID_OPERATION'

        # try to add a trigger to an invalid field
        results = self.web_interface.add_trigger(
            'stats', 'insert_ts', ['mean', 'wrong_one'], None)
        assert results == 'ERROR: INVALID_OPERATION'

        # try to remove a trigger that doesn't exist
        results = self.web_interface.remove_trigger('not_here', 'insert_ts')
        assert results == 'ERROR: INVALID_OPERATION'

        # try to remove a trigger on an invalid event
        results = self.web_interface.remove_trigger('stats', 'stuff_happening')
        assert results == 'ERROR: INVALID_OPERATION'

        ########################################
        #
        # test time series insertion
        #
        ########################################

        # insert the time series
        for k in tsdict:
            results = self.web_interface.insert_ts(k, tsdict[k])
            assert results == 'OK'

        # pick a random time series
        idx = np.random.choice(list(tsdict.keys()))

        # try to add duplicate primary key
        results = self.web_interface.insert_ts(idx, tsdict[idx])
        assert results != 'ERROR: INVALID KEY'

        ########################################
        #
        # test time series deletion
        #
        ########################################

        # pick a random time series
        idx = np.random.choice(list(tsdict.keys()))

        # check that the time series is there now
        results = self.web_interface.select({'pk': idx})
        assert len(results) == 1

        # delete an existing time series
        results = self.web_interface.delete_ts(idx)
        assert results == 'OK'

        # check that the time series is no longer there
        results = self.web_interface.select({'pk': idx})
        assert len(results) == 0

        # add the time series back in
        results = self.web_interface.insert_ts(idx, tsdict[idx])
        assert results == 'OK'

        # check that the time series is there now
        results = self.web_interface.select({'pk': idx})
        assert len(results) == 1

        # delete an invalid time series
        results = self.web_interface.delete_ts('mistake')
        assert results == 'ERROR: INVALID_KEY'

        ########################################
        #
        # test metadata upsertion
        #
        ########################################

        # upsert the metadata
        for k in tsdict:
            results = self.web_interface.upsert_meta(k, metadict[k])
            assert results == 'OK'

        # upsert metadata for a primary key that doesn't exist
        results = self.web_interface.upsert_meta('mistake', metadict[k])
        assert results == 'ERROR: INVALID_KEY'

        ########################################
        #
        # test select operations
        #
        ########################################

        # select all database entries; no metadata fields
        results = self.web_interface.select()
        if len(results) > 0:
            assert list(results[list(results.keys())[0]].keys()) == []
            assert sorted(results.keys()) == ts_keys

        # select all database entries; all metadata fields
        results = self.web_interface.select(fields=[])
        if len(results) > 0:
            assert (sorted(list(results[list(results.keys())[0]].keys())) ==
                    ['blarg', 'mean', 'order', 'pk', 'std', 'useless', 'vp'])
            assert sorted(results.keys()) == ts_keys

        # select all database entries; all invalid metadata fields
        results = self.web_interface.select(fields=['wrong', 'oops'])
        if len(results) > 0:
            assert sorted(list(results[list(results.keys())[0]].keys())) == []
            assert sorted(results.keys()) == ts_keys

        # select all database entries; some invalid metadata fields
        results = self.web_interface.select(fields=['not_there', 'std'])
        if len(results) > 0:
            assert list(results[list(results.keys())[0]].keys()) == ['std']
            assert sorted(results.keys()) == ts_keys

        # select all database entries; specific metadata fields
        results = self.web_interface.select(fields=['blarg', 'mean'])
        if len(results) > 0:
            assert (sorted(list(results[list(results.keys())[0]].keys())) ==
                    ['blarg', 'mean'])
            assert sorted(results.keys()) == ts_keys

        # not present based on how time series were generated
        results = self.web_interface.select({'order': 10})
        assert len(results) == 0

        # not present based on how time series were generated
        results = self.web_interface.select({'blarg': 0})
        assert len(results) == 0

        # multiple select criteria
        # not present based on how time series were generated
        results = self.web_interface.select({'order': 10, 'blarg': 0})
        assert len(results) == 0

        # operator select criteria
        # not present based on how time series were generated
        results = self.web_interface.select({'order': {'>=': 10}})
        assert len(results) == 0

        # operator select criteria
        # present based on how time series were generated
        results = self.web_interface.select({'order': {'<': 10}})
        assert len(results) > 0

        # check time series select
        results = self.web_interface.select({'pk': idx}, ['ts'])
        assert len(results) == 1
        assert results[idx]['ts'] == tsdict[idx]

        ########################################
        #
        # test augmented select
        #
        ########################################

        results = self.web_interface.augmented_select(
            proc='stats', target=['mean', 'std'], md={'pk': 'ts-0'})
        assert len(results) == 1
        assert 'mean' in results['ts-0']
        assert 'std' in results['ts-0']
        assert (np.round(results['ts-0']['mean'], 4) ==
                np.round(tsdict['ts-0'].mean(), 4))
        assert (np.round(results['ts-0']['std'], 4) ==
                np.round(tsdict['ts-0'].std(), 4))

        ########################################
        #
        # test vantage point representation
        #
        ########################################

        # randomly choose time series as vantage points
        vpkeys = list(np.random.choice(ts_keys, size=self.num_vps,
                                       replace=False))
        distkeys = sorted(['d_vp_' + i for i in vpkeys])

        # add the time series as vantage points
        for i in range(self.num_vps):
            self.web_interface.insert_vp(vpkeys[i])

        # check that the distance fields are now in the database
        results = self.web_interface.select(md={}, fields=distkeys)
        if len(results) > 0:
            assert (sorted(list(results[list(results.keys())[0]].keys())) ==
                    distkeys)

        # try to add a time series that doesn't exist as a vantage point
        self.web_interface.insert_vp('mistake')

        # remove them all
        for i in range(self.num_vps):
            self.web_interface.delete_vp(vpkeys[i])

        # check that the distance fields are now not in the database
        results = self.web_interface.select(md={}, fields=distkeys)
        if len(results) > 0:
            assert (list(results[list(results.keys())[0]].keys()) == [])

        # try to delete a vantage point that doesn't exist
        self.web_interface.delete_vp('mistake')

        # add them back in
        for i in range(self.num_vps):
            self.web_interface.insert_vp(vpkeys[i])

        ########################################
        #
        # test time series similarity search
        #
        ########################################

        # first create a query time series
        _, query = self.tsmaker(np.random.uniform(low=0.0, high=1.0),
                                np.random.uniform(low=0.05, high=0.4),
                                np.random.uniform(low=0.05, high=0.2))

        # get distance from query time series to the vantage point
        result_distance = self.web_interface.augmented_select(
            proc='corr', target=['vpdist'], arg=query, md={'vp': {'==': True}})
        vpdist = {v: result_distance[v]['vpdist'] for v in vpkeys}
        assert len(vpdist) == self.num_vps

        # pick the closest vantage point
        nearest_vp_to_query = min(vpkeys, key=lambda v: vpdist[v])

        # define circle radius as 2 x distance to closest vantage point
        radius = 2 * vpdist[nearest_vp_to_query]

        # find relative index of nearest vantage point
        relative_index_vp = vpkeys.index(nearest_vp_to_query)

        # calculate distance to all time series within the circle radius
        results = self.web_interface.augmented_select(
            'corr', ['towantedvp'], query,
            {'d_vp-{}'.format(relative_index_vp): {'<=': radius}})

        # find the closest time series
        nearestwanted1 = min(results.keys(),
                             key=lambda k: results[k]['towantedvp'])

        # compare to database similarity search
        nearestwanted2 = self.web_interface.vp_similarity_search(query, 1)
        # compare primary keys
        assert nearestwanted1 == list(nearestwanted2.keys())[0]

        # run similarity search on an existing time series
        # -> should return itself

        idx = np.random.choice(list(tsdict.keys()))
        results = self.web_interface.vp_similarity_search(tsdict[idx], 1)

        # recover the time series for comparison
        closest_ts = list(results)[0]
        results = self.web_interface.select(
            md={'pk': closest_ts}, fields=['ts'])
        assert results[closest_ts]['ts'] == tsdict[idx]

        ########################################
        #
        # test isax functions
        #
        ########################################

        # run similarity search on an existing time series
        # -> should return itself
        idx = np.random.choice(list(tsdict.keys()))
        results = self.web_interface.isax_similarity_search(tsdict[idx])

        # recover the time series for comparison
        closest_ts = list(results)[0]
        results = self.web_interface.select(
            md={'pk': closest_ts}, fields=['ts'])
        assert results[closest_ts]['ts'] == tsdict[idx]

        # visualize tree representation
        results = self.web_interface.isax_tree()
        assert isinstance(results, str)


if __name__ == '__main__':
    unittest.main()
