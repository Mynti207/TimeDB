# from tsdb import *
import requests
import json
from collections import OrderedDict


class WebInterface():
    '''
    Used to communicate with the REST API webserver.
    Note: requires that the server and webserver are both already running.
    '''

    def __init__(self, server='http://127.0.0.1:8080/tsdb/'):
        '''
        Initializes the WebInterface class.

        Parameters
        ----------
        server : string
            Specifies the location of the webserver

        Returns
        -------
        An initialized WebInterface object
        '''
        self.server = server

    def insert_ts(self, pk, ts):
        '''
        Inserts a time series into the database..

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry
        ts : TimeSeries
            Time series to be inserted into the database

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        if hasattr(ts, 'to_json'):
            ts = ts.to_json()
        msg = {'pk': pk, 'ts': ts}

        # post to webserver
        r = requests.post(self.server + 'insert_ts', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def delete_ts(self, pk):
        '''
        Deletes a time series from the database.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the database entry to be deleted

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'pk': pk}

        # post to webserver
        r = requests.post(self.server + 'delete_ts', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def insert_vp(self, pk):
        '''
        Marks a time series as a vantage point.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the time series to be marked as a vantage point

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'pk': pk}

        # post to webserver
        r = requests.post(self.server + 'insert_vp', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def delete_vp(self, pk):
        '''
        Unmarks a time series as a vantage point.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the time series to be unmarked as a vantage point

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'pk': pk}

        # post to webserver
        r = requests.post(self.server + 'delete_vp', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def upsert_meta(self, pk, md):
        '''
        Upserts metadata for a database entry.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the  database entry
        md : dictionary
            Metadata to be upserted into the database.

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'pk': pk, 'md': md}

        # post to webserver
        r = requests.post(self.server + 'upsert_meta', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def select(self, md={}, fields=None, additional=None):
        '''
        Selects (queries) database entries based on specified criteria.

        Parameters
        ----------
        md : dictionary
            Criteria to apply to metadata
        fields : list
            List of fields to return
        additional : dictionary
            Additional criteria ('sort_by' and 'order')

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'md': md, 'fields': fields, 'additional': additional}

        # post to webserver
        r = requests.get(self.server + 'select', data=json.dumps(msg))

        # return result of request operation
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def augmented_select(self, proc, target, arg=None, md={}, additional=None):
        '''
        Selects (queries) database entries based on specified criteria, then
        runs a coroutine.
        Note: result of coroutine is returned to user and is not upserted.

        Parameters
        ----------
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        target : string
            Array of field names to which to apply the results of the
            coroutine, and to return.
        arg : string
            Possible additional arguments (e.g. time series for similarity
            search)
        md : dictionary
            Criteria to apply to metadata
        additional : dictionary
            Additional criteria ('sort_by' and 'order')

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        if hasattr(arg, 'to_json'):
            arg = arg.to_json()
        msg = {'proc': proc, 'target': target, 'arg': arg, 'md': md,
               'additional': additional}

        # post to webserver
        r = requests.get(
            self.server + 'augmented_select', data=json.dumps(msg))

        # return result of request operation
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def vp_similarity_search(self, query, top=1):
        '''
        Finds the time series in the database that are closest to the query
        time series. (Based on vantage points.)

        Parameters
        ----------
        query : TimeSeries
            The time series to compare distances
        top : int
            The number of closest time series to return

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        if hasattr(query, 'to_json'):
            query = query.to_json()
        msg = {'query': query, 'top': top}

        # post to webserver
        r = requests.get(
            self.server + 'vp_similarity_search', data=json.dumps(msg))

        # return result of request operation
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def isax_similarity_search(self, query):
        '''
        Finds the time series in the database that are closest to the query
        time series. (Based on iSAX tree.)

        Parameters
        ----------
        query : TimeSeries
            The time series to compare distances

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        if hasattr(query, 'to_json'):
            query = query.to_json()
        msg = {'query': query}

        # post to webserver
        r = requests.get(
            self.server + 'isax_similarity_search', data=json.dumps(msg))

        # return result of request operation
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def isax_tree(self):
        '''
        Returns a visual representation of the iSAX tree.

        Parameters
        ----------
        None.

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {}

        # post to webserver
        r = requests.get(self.server + 'isax_tree', data=json.dumps(msg))

        # return result of request operation
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def add_trigger(self, proc, onwhat, target, arg=None):
        '''
        Adds a trigger (similar to an event loop in asynchronous programming,
        i.e. will take some action when a certain event occurs.)

        Parameters
        ----------
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        target : string
            Array of field names to which to apply the results of the coroutine
        arg : string
            Possible additional arguments (e.g. 'sort_by', 'order')

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'proc': proc, 'onwhat': onwhat, 'target': target, 'arg': arg}

        # post to webserver
        r = requests.post(self.server + 'add_trigger', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def remove_trigger(self, proc, onwhat, target=None):
        '''
        Removes a previously-set trigger.

        Parameters
        ----------
        proc : string
            Name of the module in procs that defines the trigger action
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        target : string
            Array of field names to which the results of the coroutine
            are applied

        Returns
        -------
        Result of the database operation (or error message).
        '''

        # package message as dictionary
        msg = {'proc': proc, 'onwhat': onwhat, 'target': target}

        # post to webserver
        r = requests.post(self.server + 'remove_trigger', data=json.dumps(msg))

        # return result of request operation (either None or error message)
        return json.loads(r.text, object_pairs_hook=OrderedDict)
