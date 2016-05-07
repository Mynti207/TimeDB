from tsdb import *
import requests
import asyncio


class WebInterface():
    '''
    Used to communicate with the REST API webserver.
    Note: requires that the server and webserver are both already running.
    '''

    def __init__(self):
        self.server = 'http://127.0.0.1:8080/tsdb/'

    def insert_ts(self, pk, ts):
        msg = TSDBOp_InsertTS(pk, ts).to_json()
        r = requests.post(self.server + 'insert_ts', data=json.dumps(msg))

    def delete_ts(self, pk):
        msg = TSDBOp_DeleteTS(pk).to_json()
        r = requests.post(self.server + 'delete_ts', data=json.dumps(msg))

    def upsert_meta(self, pk, md):
        msg = TSDBOp_UpsertMeta(pk, md).to_json()
        requests.post(self.server + 'upsert_meta', data=json.dumps(msg))

    def select(self, md={}, fields=None, additional=None):
        msg = TSDBOp_Select(md, fields, additional).to_json()
        r = requests.get(self.server + 'select', data=json.dumps(msg))
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def augmented_select(self, proc, target, arg=None, md={}, additional=None):
        msg = TSDBOp_AugmentedSelect(proc, target, arg, md, additional).to_json()
        r = requests.get(self.server + 'augmented_select', data=json.dumps(msg))
        return json.loads(r.text, object_pairs_hook=OrderedDict)

    def add_trigger(self, proc, onwhat, target, arg=None):
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()
        requests.post(self.server + 'add_trigger', data=json.dumps(msg))

    def remove_trigger(self, proc, onwhat):
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()
        requests.post(self.server + 'remove_trigger', data=json.dumps(msg))

    def similarity_search(self, query, top=1):
        msg = {'query': query.to_json(), 'top': top}
        r = requests.get(self.server + 'similarity_search', data=json.dumps(msg))
        return json.loads(r.text, object_pairs_hook=OrderedDict)
