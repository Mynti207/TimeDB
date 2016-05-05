import asyncio
from .tsdb_serialization import serialize, LENGTH_FIELD_LENGTH, Deserializer
from .tsdb_ops import *
from .tsdb_error import TSDBStatus


class TSDBClient(object):
    '''
    Implements a client for the database that packs operations in a json
    format and sends it out. All operations are equivalent to their
    specification in tsdb_ops.
    Note: can be used in a python program, web server, or repl.
    '''

    def __init__(self, port=9999):
        '''
        Initializes the TSDBClient class.

        Parameters
        ----------
        port : int
            Specifies the port the database client uses.

        Returns
        -------
        An initialized TSDB client object
        '''
        self.port = port

    async def insert_ts(self, primary_key, ts):
        '''
        Inserts a time series into the database.

        Parameters
        ----------
        primary_key : any hashable type
            Primary key for the new database entry
        ts : TimeSeries
            Time series to be inserted into the database.

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # convert operation into message in json form
        msg = TSDBOp_InsertTS(primary_key, ts).to_json()

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def upsert_meta(self, primary_key, metadata_dict):
        '''
        Upserts (inserts/updates) metadata for a database entry. Requires
        that the metadata fields are in the schema.

        Parameters
        ----------
        primary_key : any hashable type
            Primary key for the  database entry
        metadata_dict : dictionary
            Metadata to be upserted into the database.

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # convert operation into message in json form
        msg = TSDBOp_UpsertMeta(primary_key, metadata_dict).to_json()

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def select(self, metadata_dict={}, fields=None, additional=None):
        '''
        Select database entries based on specified criteria.

        Parameters
        ----------
        metadata_dict : dictionary
            Criteria to apply to metadata
        fields : list
            List of fields to return (default=None)
        additional : dictionary
            Additional criteria, e.g. apply sorting (default=None)

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # convert operation into message in json form
        msg = TSDBOp_Select(metadata_dict, fields, additional).to_json()

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def augmented_select(self, proc, target, arg=None, metadata_dict={},
                               additional=None):
        # TODO: document

        # convert operation into message in json form
        msg = TSDBOp_AugmentedSelect(proc, target, arg, metadata_dict,
                                     additional).to_json()
        # send message
        status, payload = await self._send(msg)
        # return the result of sending the message
        return status, payload

    async def add_trigger(self, proc, onwhat, target, arg):
        # TODO: document

        # convert operation into message in json form
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def remove_trigger(self, proc, onwhat):
        # TODO: document

        # convert operation into message in json form
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def _send_coro(self, msg, loop):
        '''
        Asynchronous co-routing for sending well-formed "messages"
        (i.e. TSDB database operations).

        Parameters
        ----------
        msg : json
            json-encoded message (i.e. tsdb database operation)

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # serialize the message
        msg_serialized = serialize(msg)

        # open connection with the server
        reader, writer = await asyncio.open_connection(host='127.0.0.1',
                                                       port=self.port,
                                                       loop=loop)

        # write the message
        writer.write(msg_serialized)

        # read the message
        data = await reader.read()

        # deserialize the message
        deserializer = Deserializer()
        deserializer.append(data)
        msg_received = deserializer.deserialize()

        # unpack the message and return
        status = msg_received['status']
        payload = msg_received['payload']
        return status, payload

    async def _send(self, msg):
        '''
        Sends a well-formed "message" (i.e. a TSDB database operation).

        Parameters
        ----------
        msg : json
            json-encoded message (i.e. tsdb database operation)

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # create asyncio event loop
        loop = asyncio.get_event_loop()

        # await the result of sending the message
        status, payload = await self._send_coro(msg, loop)

        # return the result of sending the message
        return status, payload
