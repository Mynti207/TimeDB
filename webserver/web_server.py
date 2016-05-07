import asyncio
import json
from aiohttp import web
from timeseries import TimeSeries

from tsdb import TSDBClient


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
    UTF-8 encoded payload if no errors; 'Error with status' otherwise.
    '''
    if status == 0:
        body = json.dumps(payload).encode('utf-8')
    else:
        body = bytes('Error with status {}'.format(status), 'utf-8')
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
        Input: Request instances
        Returns: StreamResponse
        '''
        return web.Response(
            body='REST API for TimeSeries Implementation')

    async def handle_insert_ts(self, request):

        request_json = await request.json()

        # Checking format
        required_args = ['pk', 'ts']
        check = check_arguments('insert_ts', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        pk = request_json['pk']
        ts_ = TimeSeries(*request_json['ts'])

        status, payload = await self.client.insert_ts(pk, ts_)
        body = get_body(status, payload)

        return web.Response(body=body)

    async def handle_upsert_meta(self, request):
        request_json = await request.json()
        # Checking format
        required_args = ['pk', 'md']
        check = check_arguments('upsert_meta', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)
        pk = request_json['pk']
        md = request_json['md']

        status, payload = await self.client.upsert_meta(pk, md)
        body = get_body(status, payload)

        return web.Response(body=get_body(status, payload))

    async def handle_select(self, request):
        request_json = await request.json()

        # No required argument
        md = request_json['md'] if 'md' in request_json else {}
        fields = request_json['fields'] if 'fields' in request_json else None
        additional = (request_json['additional']
                      if 'additional' in request_json else None)

        status, payload = await self.client.select(
            metadata_dict=md, fields=fields, additional=additional)

        return web.Response(body=get_body(status, payload))

    async def handle_augmented_select(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'target']
        check = check_arguments('augmented_select',
                                request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        proc = request_json['proc']
        target = request_json['target']
        md = request_json['md'] if 'md' in request_json else {}
        arg = (TimeSeries(*request_json['arg'])
               if 'arg' in request_json else None)
        additional = (request_json['additional']
                      if 'additional' in request_json else None)
        status, payload = await self.client.augmented_select(
            proc, target, arg=arg, metadata_dict=md, additional=additional)

        return web.Response(body=get_body(status, payload))

    async def handle_add_trigger(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'onwhat', 'target']
        check = check_arguments('add_trigger', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        proc = request_json['proc']
        onwhat = request_json['onwhat']
        target = request_json['target']
        try:
            arg = (TimeSeries(*request_json['arg']))
        except:
            arg = None

        status, payload = await self.client.add_trigger(
            proc, onwhat, target, arg)

        return web.Response(body=get_body(status, payload))

    async def handle_remove_trigger(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'onwhat']
        check = check_arguments('remove_trigger', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        proc = request_json['proc']
        onwhat = request_json['onwhat']

        status, payload = await self.client.remove_trigger(proc, onwhat)

        return web.Response(body=get_body(status, payload))

    async def handle_similarity_search(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['query']
        check = check_arguments('remove_trigger', request_json, *required_args)
        if check is not None:
            return aiohttp.web.Response(body=check)

        query = TimeSeries(*request_json['query'])
        top = int(request_json['top']) if 'top' in request_json else 1

        # Computing the distance to the query timeseries
        status, results = await self.client.augmented_select(
            'corr', ['d'], query)
        # Retrieving the closest ts with associated distance
        nearestwanted = [(k, results[k]['d']) for k in results.keys()]
        nearestwanted.sort(key=lambda x: x[1])

        return web.Response(body=get_body(status, nearestwanted[:top]))


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
        self.app.router.add_route(
            'GET', '/tsdb', self.handler.handle_intro)
        self.app.router.add_route(
            'POST', '/tsdb/insert_ts', self.handler.handle_insert_ts)
        self.app.router.add_route(
            'POST', '/tsdb/upsert_meta', self.handler.handle_upsert_meta)
        self.app.router.add_route(
            'GET', '/tsdb/select', self.handler.handle_select)
        self.app.router.add_route(
            'GET', '/tsdb/augmented_select',
            self.handler.handle_augmented_select)
        self.app.router.add_route(
            'POST', '/tsdb/add_trigger',
            self.handler.handle_add_trigger)
        self.app.router.add_route(
            'POST', '/tsdb/remove_trigger',
            self.handler.handle_remove_trigger)
        self.app.router.add_route(
            'GET', '/tsdb/similarity_search',
            self.handler.handle_similarity_search)

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
