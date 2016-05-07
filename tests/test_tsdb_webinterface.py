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
# note: server and webserver code run through the subprocess is not reflected
# in coverage
#
########################################


class test_webserver(asynctest.TestCase):

    # database initializations
    def setUp(self):

        # initialize & run the server
        self.server = subprocess.Popen(['python', 'go_server.py'])
        time.sleep(5)

        # intialize webserver
        self.webserver = subprocess.Popen(['python', 'go_webserver.py'])
        time.sleep(5)

        # initialize web interface
        self.web_interface = WebInterface()

    # avoids the server hanging
    def tearDown(self):
        self.server.terminate()
        time.sleep(5)
        self.webserver.terminate()
        time.sleep(5)

    # run client tests
    async def test_webserver_ops(self):

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

        # randomly choose two time series as the vantage points
        num_vps = 2
        random_vps = np.random.choice(range(num_ts), size=num_vps,
                                      replace=False)
        vpkeys = ["ts-{}".format(i) for i in random_vps]

        # change the metadata for the vantage points to have meta['vp']=True
        for i in range(num_vps):
            metadict[vpkeys[i]]['vp'] = True

        ########################################
        #
        # test trigger operations
        #
        ########################################

        # add trigger to calculate the distances to the vantage point
        for i in range(num_vps):
            self.web_interface.add_trigger(
                'corr', 'insert_ts', ["d_vp-{}".format(i)], tsdict[vpkeys[i]])

        # add dummy trigger
        self.web_interface.add_trigger('junk', 'insert_ts', None, None)

        # add stats trigger
        self.web_interface.add_trigger(
            'stats', 'insert_ts', ['mean', 'std'], None)

        # try to add a trigger on an invalid event
        self.web_interface.add_trigger(
            'junk', 'stuff_happening', None, None)

        # try to add a trigger to an invalid field
        self.web_interface.add_trigger(
            'stats', 'insert_ts', ['mean', 'wrong_one'], None)

        # try to remove a trigger that doesn't exist
        self.web_interface.remove_trigger('not_here', 'insert_ts')

        # try to remove a trigger on an invalid event
        self.web_interface.remove_trigger('stats', 'stuff_happening')

        ########################################
        #
        # test time series insertion
        #
        ########################################

        # insert the time series
        for k in tsdict:
            self.web_interface.insert_ts(k, tsdict[k])

        # try to add duplicate primary key
        self.web_interface.insert_ts(vpkeys[0], tsdict[vpkeys[0]])

        ########################################
        #
        # test metadata upsertion
        #
        ########################################

        # upsert the metadata
        for k in tsdict:
            self.web_interface.upsert_meta(k, metadict[k])

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
                    ['blarg', 'd_vp-0', 'd_vp-1', 'mean', 'order', 'pk', 'std',
                     'vp'])
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
        # # assert len(results) == 0

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

        ########################################
        #
        # test time series similarity search
        #
        ########################################

        # first create a query time series
        _, query = tsmaker(np.random.uniform(low=0.0, high=1.0),
                           np.random.uniform(low=0.05, high=0.4),
                           np.random.uniform(low=0.05, high=0.2))

        # get distance from query time series to the vantage point
        result_distance = self.web_interface.augmented_select(
            'corr', ['vpdist'], query, {'vp': {'==': True}})
        vpdist = {v: result_distance[v]['vpdist'] for v in vpkeys}
        assert len(vpdist) == num_vps
        #
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
        nearestwanted2 = self.web_interface.similarity_search(query, 1)
        assert nearestwanted1 == nearestwanted2[0][0]  # compare primary keys
