import asyncio
import json
from aiohttp import web
from timeseries import TimeSeries

from tsdb import TSDBClient, TSDBStatus


def check_arguments(request_type, request_json, *required_args):
    '''
    Helper function: processes requests to the REST API. Checks whether
    requests' json messages contains all required arguments.

    Parameters
    ----------
    request_type : string
        Name of request type
    request_json : json
        Full request, encoded in json format
    required_args : list
        Required arguments for the request type

    Returns
    -------
    None if the request is valid; error message otherwise.
    '''
    # loop through all the required arguments
    for arg in required_args:

        # return an error message if any of them are missing
        if arg not in request_json:
            return bytes('Bad format of data with request {}. '
                         'Parameters expected are: {}'.
                         format(request_type, ', '.join(required_args)),
                         'utf-8')

    # otherwise return None
    return None


def get_body(status, payload):
    '''
    Helper function: reads the status message and uses it to build the
    body message.

    Parameters
    ----------
    status : int
        TSDB status code from running a tsdb operation (e.g. OK, error, etc.)
    payload : dict
        Result of running a tsdb operation

    Returns
    ----------
    UTF-8 encoded payload if no errors; error message otherwise.
    '''
    if status == TSDBStatus.OK:
        if payload is None:
            msg = str(TSDBStatus(status))
            msg = msg[(msg.index('.') + 1):]
            body = json.dumps(msg).encode('utf-8')
        else:
            body = json.dumps(payload).encode('utf-8')
    else:
        msg = str(TSDBStatus(status))
        msg = msg[(msg.index('.') + 1):]
        body = json.dumps('ERROR: ' + msg).encode('utf-8')
    return body


class Handler(object):
    '''
    Asynchronous REST API handler.
    Reference: http://aiohttp.readthedocs.io/en/stable/web.html
    '''

    def __init__(self):
        '''
        Initializes the class.

        Parameters
        ----------
        None

        Returns
        -------
        An initialized Handler object
        '''
        self.client = TSDBClient()

    async def handle_intro(self, request):
        '''
        Handler for tsdb landing page.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''
        return web.Response(
            body='REST API for TimeSeries Implementation')

    async def handle_insert_ts(self, request):
        '''
        Handler for inserting a new time series into the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['pk', 'ts']
        check = check_arguments('insert_ts', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        pk = request_json['pk']
        ts_ = TimeSeries(*request_json['ts'])

        # send the operation to the client
        status, payload = await self.client.insert_ts(pk, ts_)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_delete_ts(self, request):
        '''
        Handler for deleting a time series from the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['pk']
        check = check_arguments('delete_ts', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        pk = request_json['pk']

        # send the operation to the client
        status, payload = await self.client.delete_ts(pk)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_insert_vp(self, request):
        '''
        Handler for marking a time series as a vantage point.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['pk']
        check = check_arguments('insert_vp', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        pk = request_json['pk']

        # send the operation to the client
        status, payload = await self.client.insert_vp(pk)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_delete_vp(self, request):
        '''
        Handler for unmarking a time series as a vantage point.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['pk']
        check = check_arguments('delete_vp', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        pk = request_json['pk']

        # send the operation to the client
        status, payload = await self.client.delete_vp(pk)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_upsert_meta(self, request):
        '''
        Handler for upserting metadata to the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['pk', 'md']
        check = check_arguments('upsert_meta', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        pk = request_json['pk']
        md = request_json['md']

        # send the operation to the client
        status, payload = await self.client.upsert_meta(pk, md)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_select(self, request):
        '''
        Handler for selecting (querying) data from the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # note: no specific arguments required, so no need to check that
        # request format is in line with specifications

        # unpack the database operation parameters
        md = request_json['md'] if 'md' in request_json else {}
        fields = request_json['fields'] if 'fields' in request_json else None
        additional = (request_json['additional']
                      if 'additional' in request_json else None)

        # send the operation to the client
        status, payload = await self.client.select(
            metadata_dict=md, fields=fields, additional=additional)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_augmented_select(self, request):
        '''
        Handler for augmented select, i.e. selecting (querying) data from the
        database, then running a pre-defined function on the data that was
        returned.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['proc', 'target']
        check = check_arguments(
            'augmented_select', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        # unpack the database operation parameters
        proc = request_json['proc']
        target = request_json['target']
        if 'arg' in request_json:
            if request_json['arg'] is None:
                arg = None
            else:
                arg = TimeSeries(*request_json['arg'])
        md = request_json['md'] if 'md' in request_json else {}
        additional = (request_json['additional']
                      if 'additional' in request_json else None)

        # send the operation to the client
        status, payload = await self.client.augmented_select(
            proc=proc, target=target, arg=arg, metadata_dict=md,
            additional=additional)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_similarity_search(self, request):
        '''
        Handler for carrying out a similarity search, i.e. finding the
        'closest' time series in the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['query', 'top']
        check = check_arguments(
            'similarity_search', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        # unpack the database operation parameters
        query = TimeSeries(*request_json['query'])
        top = int(request_json['top']) if 'top' in request_json else 1

        # send the operation to the client
        status, payload = await self.client.similarity_search(query, top)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_add_trigger(self, request):
        '''
        Handler for adding a trigger to the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['proc', 'onwhat', 'target']
        check = check_arguments('add_trigger', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        proc = request_json['proc']
        onwhat = request_json['onwhat']
        target = request_json['target']
        try:
            arg = (TimeSeries(*request_json['arg']))
        except:
            arg = None

        # send the operation to the client
        status, payload = await self.client.add_trigger(
            proc, onwhat, target, arg)

        # unpack and return response
        return web.Response(body=get_body(status, payload))

    async def handle_remove_trigger(self, request):
        '''
        Handler for removing a trigger from the database.

        Parameters
        ----------
        request : request instance
            Request instance that packages all database operation parameters

        Returns
        -------
        StreamResponse
        '''

        # convert request to json format
        request_json = await request.json()

        # check that the request format is in line with specifications
        required_args = ['proc', 'onwhat']
        check = check_arguments('remove_trigger', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        # unpack the database operation parameters
        proc = request_json['proc']
        onwhat = request_json['onwhat']
        target = request_json['target']

        # send the operation to the client
        status, payload = await self.client.remove_trigger(proc, onwhat, target)

        # unpack and return response
        return web.Response(body=get_body(status, payload))


class WebServer(object):
    '''
    Asynchronous REST API webserver.
    '''

    def __init__(self):
        '''
        Initializes the class.

        Parameters
        ----------
        None

        Returns
        -------
        An initialized WebServer object
        '''

        # initialize web application
        self.app = web.Application()

        # initialize handler
        self.handler = Handler()

        # add routes for supported operations
        self.app.router.add_route('GET', '/tsdb',
                                  self.handler.handle_intro)
        self.app.router.add_route('POST', '/tsdb/insert_ts',
                                  self.handler.handle_insert_ts)
        self.app.router.add_route('POST', '/tsdb/delete_ts',
                                  self.handler.handle_delete_ts)
        self.app.router.add_route('POST', '/tsdb/upsert_meta',
                                  self.handler.handle_upsert_meta)
        self.app.router.add_route('GET', '/tsdb/select',
                                  self.handler.handle_select)
        self.app.router.add_route('GET', '/tsdb/augmented_select',
                                  self.handler.handle_augmented_select)
        self.app.router.add_route('GET', '/tsdb/similarity_search',
                                  self.handler.handle_similarity_search)
        self.app.router.add_route('POST', '/tsdb/add_trigger',
                                  self.handler.handle_add_trigger)
        self.app.router.add_route('POST', '/tsdb/remove_trigger',
                                  self.handler.handle_remove_trigger)
        self.app.router.add_route('POST', '/tsdb/insert_vp',
                                  self.handler.handle_insert_vp)
        self.app.router.add_route('POST', '/tsdb/delete_vp',
                                  self.handler.handle_delete_vp)

    def run(self):
        '''
        Runs the REST API webserver.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''
        web.run_app(self.app)
