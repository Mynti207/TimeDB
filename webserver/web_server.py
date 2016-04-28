import asyncio
import json
import timeseries as ts

from aiohttp import web
from tsdb import TSDBClient


def check_arguments(request_type, request_json, *required_args):
    '''
    Check that the json message of the request contain the
    required arguments.
    Ouput: None if valid, error message else
    '''
    for arg in required_args:
        if arg not in request_json:
            return bytes('Bad format of data with request {}. Parameters expected are: {}'.format(request_type, ', '.join(required_args)), 'utf-8')
    return None


def get_body(status, payload):
    '''
    Read the status message and build the body message according to it.
    Output:
        if status = 0, return body with payload
        else, return body with 'Error with status '
    '''
    if status == 0:
        body = json.dumps(payload).encode('utf-8')
    else:
        body = bytes('Error with status {}'.format(status), 'utf-8')
    return body


class Handler(object):

    def __init__(self):
        self.client = TSDBClient()

    async def handle_intro(self, request):
        return web.Response(body=b"REST API for TimeSeries Implementation")

    async def handle_insert_ts(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['pk', 'ts']
        check = check_arguments('insert_ts', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        pk = request_json['pk']
        ts_ = ts.TimeSeries(*request_json['ts'])

        status, payload = await self.client.insert_ts(pk, ts_)
        body = get_body(status, payload)

        return web.Response(body=body)

    async def handle_upsert_meta(self, request):
        request_json = await request.json()
        # Checking format
        required_args = ['pk', 'md']
        check = check_arguments('upsert_meta', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)
        pk = request_json['pk']
        md = request_json['md']

        status, payloa = await self.client.upsert_meta(pk, md)
        body = get_body(status, payload)

        return web.Response(body=get_body(status, payload))

    async def handle_select(self, request):
        request_json = await request.json()

        # No required argument
        md = request_json['md'] if 'md' in request_json else {}
        fields = request_json['fields'] if 'fields' in request_json else None
        additional = request_json['additional'] if 'additional' in request_json else None

        status, payload = await self.client.select(metadata_dict=md,
                                                   fields=fields,
                                                   additional=additional)

        return web.Response(body=get_body(status, payload))

    async def handle_augmented_select(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'target']
        check = check_arguments('augmented_select', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        proc = request_json['proc']
        target = request_json['target']
        md = request_json['md'] if 'md' in request_json else {}
        arg = ts.TimeSeries(*request_json['arg']) if 'arg' in request_json else None
        additional = request_json['additional'] if 'additional' in request_json else None
        status, payload = await self.client.augmented_select(proc, target,
                                                             arg=arg,
                                                             metadata_dict=md,
                                                             additional=additional)

        return web.Response(body=get_body(status, payload))

    async def handle_add_trigger(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'onwhat', 'target']
        check = check_arguments('add_trigger', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        proc = request_json['proc']
        onwhat = request_json['onwhat']
        target = request_json['target']
        arg = ts.TimeSeries(*request_json['arg']) if 'arg' in request_json else None

        status, payload = await self.client.add_trigger(proc, onwhat, target,
                                                        arg)

        return web.Response(body=get_body(status, payload))

    async def handle_remove_trigger(self, request):
        request_json = await request.json()

        # Checking format
        required_args = ['proc', 'onwhat']
        check = check_arguments('remove_trigger', request_json, *required_args)
        if check is not None:
            return web.Response(body=check)

        proc = request_json['proc']
        onwhat = request_json['onwhat']

        status, payload = await self.client.add_trigger(proc, onwhat)

        return web.Response(body=get_body(status, payload))


class WebServer(object):

    def __init__(self):
        # Create the web application
        self.app = web.Application()
        self.handler = Handler()
        # Adding the routes
        self.app.router.add_route('GET', '/tsdb', self.handler.handle_intro)
        self.app.router.add_route('POST', '/tsdb/insert_ts',
                                  self.handler.handle_insert_ts)
        self.app.router.add_route('POST', '/tsdb/upsert_meta',
                                  self.handler.handle_upsert_meta)
        self.app.router.add_route('GET', '/tsdb/select',
                                  self.handler.handle_select)
        self.app.router.add_route('GET', '/tsdb/augmented_select',
                                  self.handler.handle_augmented_select)
        self.app.router.add_route('POST', '/tsdb/add_trigger',
                                  self.handler.handle_add_trigger)
        self.app.router.add_route('POST', '/tsdb/remove_trigger',
                                  self.handler.handle_remove_trigger)

    def run(self):
        # Run the app
        web.run_app(self.app)

if __name__ == '__main__':
    wb = WebServer()
    wb.run()
