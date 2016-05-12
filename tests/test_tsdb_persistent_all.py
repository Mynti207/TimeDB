# import unittest
# import asynctest
# import asyncio
# from timeseries import TimeSeries
# import numpy as np
# from scipy.stats import norm
# from tsdb import *
# from webserver import *
# import time
# import subprocess
#
# ########################################
# #
# # we use unit tests instead of pytests, because they facilitate the build-up
# # and tear-down of the server (and avoid the tests hanging)
# #
# # adapted from go_server.py and go_client.py
# # subprocess reference: https://docs.python.org/2/library/subprocess.html
# #
# # note: server and webserver code run through the subprocess is not reflected
# # in coverage
# #
# ########################################
#
#
# class test_webinterface(asynctest.TestCase):
#
#     # database initializations
#     def setUp(self):
#
#         # persistent database parameters
#         self.data_dir = 'db_files'
#         self.db_name = 'default'
#         self.ts_length = 100
#
#         # clear file system for testing
#         dir_clean = self.data_dir + '/' + self.db_name + '/'
#         if not os.path.exists(dir_clean):
#             os.makedirs(dir_clean)
#         filelist = [dir_clean + f for f in os.listdir(dir_clean)]
#         for f in filelist:
#             os.remove(f)
#
#         # initialize & run the server
#         self.server = subprocess.Popen(
#             ['python', 'go_server_persistent.py',
#                 '--ts_length', str(self.ts_length),
#                 '--data_dir', self.data_dir, '--db_name', self.db_name])
#         time.sleep(5)
#
#         # intialize webserver
#         self.webserver = subprocess.Popen(['python', 'go_webserver.py'])
#         time.sleep(5)
#
#         # initialize web interface
#         self.web_interface = WebInterface()
#
#         # parameters for testing
#         self.num_ts = 25
#         self.num_vps = 5
#
#     # avoids the server hanging
#     def tearDown(self):
#         self.server.terminate()
#         time.sleep(5)
#         self.webserver.terminate()
#         time.sleep(5)
#
#     def tsmaker(self, m, s, j):
#         '''
#         Helper function: randomly generates a time series for testing.
#
#         Parameters
#         ----------
#         m : float
#             Mean value for generating time series data
#         s : float
#             Standard deviation value for generating time series data
#         j : float
#             Quantifies the "jitter" to add to the time series data
#
#         Returns
#         -------
#         A time series and associated meta data.
#         '''
#
#         # generate metadata
#         meta = {}
#         meta['order'] = int(np.random.choice(
#             [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5]))
#         meta['blarg'] = int(np.random.choice([1, 2]))
#         meta['vp'] = False  # initialize vantage point indicator as negative
#
#         # generate time series data
#         t = np.arange(0.0, 1.0, 0.01)
#         v = norm.pdf(t, m, s) + j * np.random.randn(self.ts_length)
#
#         # return time series and metadata
#         return meta, TimeSeries(t, v)
#
#     # run client tests
#     async def test_webinterface_ops(self):
#
#         ########################################
#         #
#         # create dummy data for testing
#         #
#         ########################################
#
#         # a manageable number of test time series
#         mus = np.random.uniform(low=0.0, high=1.0, size=self.num_ts)
#         sigs = np.random.uniform(low=0.05, high=0.4, size=self.num_ts)
#         jits = np.random.uniform(low=0.05, high=0.2, size=self.num_ts)
#
#         # initialize dictionaries for time series and their metadata
#         tsdict = {}
#         metadict = {}
#
#         # fill dictionaries with randomly generated entries for database
#         for i, m, s, j in zip(range(self.num_ts), mus, sigs, jits):
#             meta, tsrs = self.tsmaker(m, s, j)  # generate data
#             pk = "ts-{}".format(i)  # generate primary key
#             tsdict[pk] = tsrs  # store time series data
#             metadict[pk] = meta  # store metadata
#
#         # for testing later on
#         ts_keys = sorted(tsdict.keys())
#
#         # randomly choose time series as vantage points
#         vpkeys = list(np.random.choice(ts_keys, size=self.num_vps,
#                                        replace=False))
#         distkeys = sorted(['d_vp_' + i for i in vpkeys])
#
#         ########################################
#         #
#         # load data into 'empty' database
#         #
#         ########################################
#
#         # add stats trigger
#         results = self.web_interface.add_trigger(
#             'stats', 'insert_ts', ['mean', 'std'], None)
#         assert results == 'OK'
#
#         # insert the time series
#         for k in tsdict:
#             results = self.web_interface.insert_ts(k, tsdict[k])
#             assert results == 'OK'
#
#         # upsert the metadata
#         for k in tsdict:
#             results = self.web_interface.upsert_meta(k, metadict[k])
#             assert results == 'OK'
#
#         # add the time series as vantage points
#         for i in range(self.num_vps):
#             self.web_interface.insert_vp(vpkeys[i])
#
#         ########################################
#         #
#         # check that everything has loaded
#         #
#         ########################################
#
#         # select all database entries; all metadata fields
#         results = self.web_interface.select(fields=[])
#
#         # we have the right number of database entries
#         assert len(results) == self.num_ts
#
#         # we have all the right primary keys
#         assert sorted(results.keys()) == ts_keys
#
#         # check that the distance fields are now in the database
#         results = self.web_interface.select(md={}, fields=distkeys)
#         if len(results) > 0:
#             assert (sorted(list(results[list(results.keys())[0]].keys())) ==
#                     distkeys)
#
#         ########################################
#         #
#         # store similarity results
#         #
#         ########################################
#
#         # create a query time series
#         _, query = self.tsmaker(np.random.uniform(low=0.0, high=1.0),
#                                 np.random.uniform(low=0.05, high=0.4),
#                                 np.random.uniform(low=0.05, high=0.2))
#
#         # run the vantage point similarity search
#         results_vp = self.web_interface.vp_similarity_search(query, 1)
#         assert len(results_vp) == 1
#         assert list(results_vp)[0] in ts_keys
#
#         # run the isax similarity search
#         results_isax = self.web_interface.isax_similarity_search(query)
#         assert len(results_isax) == 1
#         assert list(results_isax)[0] in ts_keys
#
#         # visualize tree representation
#         results_tree = self.web_interface.isax_tree()
#         assert isinstance(results_tree, str)
#
#
# if __name__ == '__main__':
#     unittest.main()
