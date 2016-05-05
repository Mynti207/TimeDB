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

    def __init__(self, port=9999, verbose=False):
        '''
        Initializes the TSDBClient class.

        Parameters
        ----------
        port : int
            Specifies the port the database client uses (default=9999)
        verbose : boolean
            Determines whether status updates are printed

        Returns
        -------
        An initialized TSDB client object
        '''
        self.port = port
        self.verbose = False

    async def insert_ts(self, primary_key, ts):
        '''
        Inserts a time series into the database.

        Parameters
        ----------
        primary_key : any hashable type
            Primary key for the new database entry
        ts : TimeSeries
            Time series to be inserted into the database

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # convert operation into message in json form
        msg = TSDBOp_InsertTS(primary_key, ts).to_json()

        # status update
        if self.verbose: print('C> msg', msg)

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
            Metadata to be upserted into the database
        verbose : boolean
            Determines whether status updates are displayed

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''
        # convert operation into message in json form
        msg = TSDBOp_UpsertMeta(primary_key, metadata_dict).to_json()

        # status update
        if self.verbose: print('C> msg', msg)

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

        # status update
        if self.verbose: print('C> msg', msg)

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def add_trigger(self, proc, onwhat, target, arg):
        '''
        Adds a trigger - similar to an event in asynchronous programming,
        i.e. will take some action when a certain event happens.

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
            Possible additional arguments ('sort_by' and 'order')

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''

        # convert operation into message in json form
        msg = TSDBOp_AddTrigger(proc, onwhat, target, arg).to_json()

        # status update
        if self.verbose: print('C> msg', msg)

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def remove_trigger(self, proc, onwhat):
        '''
        Removes a previously-set trigger.

        Parameters
        ----------
        proc : string
            Name of the module in procs that defines the trigger action
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''

        # convert operation into message in json form
        msg = TSDBOp_RemoveTrigger(proc, onwhat).to_json()

        # status update
        if self.verbose: print('C> msg', msg)

        # send message
        status, payload = await self._send(msg)

        # return the result of sending the message
        return status, payload

    async def augmented_select(self, proc, target, arg=None, metadata_dict={},
                               additional=None):
        '''
        Select database entries based on specified criteria, then run a
        coroutine.
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
            Possible additional arguments ('sort_by' and 'order')
        metadata_dict : dictionary
            Criteria to apply to metadata
        additional : dictionary
            Additional criteria, e.g. apply sorting (default=None)

        Returns
        -------
        Result of sending the message with the TSDB operation.
        '''

        # convert operation into message in json form
        msg = TSDBOp_AugmentedSelect(proc, target, arg, metadata_dict,
                                     additional).to_json()
        # status update
        if self.verbose: print('C> msg', msg)

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

        # unpack the message
        status = msg_received['status']
        payload = msg_received['payload']

        # status update
        if self.verbose:
            print('C> status:', TSDBStatus(status))
            try:
                print('C> payload:', list(payload.keys()))
            except:
                print('C> payload:', payload)

        # return the message
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
