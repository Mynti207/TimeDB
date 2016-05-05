import asyncio
from .tsdb_serialization import serialize, LENGTH_FIELD_LENGTH, Deserializer
from .tsdb_ops import *
from .tsdb_error import *


class TSDBClient(object):
    """
    The client. This could be used in a python program, web server, or REPL!
    """
    def __init__(self, port=9999):
        self.port = port

    async def insert_ts(self, primary_key, ts):
        msg = TSDBOp_InsertTS(primary_key, ts).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    async def upsert_meta(self, primary_key, metadata_dict):
        msg = TSDBOp_UpsertMeta(primary_key, metadata_dict).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    async def select(self, metadata_dict={}, fields=None, additional=None):
        msg = TSDBOp_Select(metadata_dict, fields, additional).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    async def augmented_select(self, proc, target, arg=None, metadata_dict={},
                               additional=None):
        msg = TSDBOp_AugmentedSelect(proc, target, arg, metadata_dict,
                                     additional).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    async def add_trigger(self, proc, onwhat, target, arg):
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    async def remove_trigger(self, proc, onwhat):
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()
        # print("C> msg", msg)
        status, payload = await self._send(msg)
        return status, payload

    # Feel free to change this to be completely synchronous
    # from here onwards. Return the status and the payload
    async def _send_coro(self, msg, loop):
        # serialize the message
        msg_serialized = serialize(msg)

        # Open connection with the server
        reader, writer = await asyncio.open_connection(host='127.0.0.1',
                                                       port=self.port,
                                                       loop=loop)

        # Write message
        writer.write(msg_serialized)
        print("C> writing")

        # Read message
        data = await reader.read()
        deserializer = Deserializer()
        deserializer.append(data)
        msg_recieved = deserializer.deserialize()
        status = msg_recieved['status']
        payload = msg_recieved['payload']

        # Verbose
        print("C> status: ", status)
        print("C> payload: ", payload)

        return status, payload

    # call `_send` with a well formed message to send.
    # once again replace this function if appropriate
    async def _send(self, msg):
        # coro = asyncio.ensure_future(self._send_coro(msg, loop))
        # loop.run_until_complete(coro)
        # return coro.result()
        loop = asyncio.get_event_loop()
        status, payload = await self._send_coro(msg, loop)
        return status, payload
