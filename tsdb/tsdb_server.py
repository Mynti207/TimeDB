import asyncio
from .dictdb import DictDB
from importlib import import_module
from collections import defaultdict, OrderedDict
from .tsdb_serialization import Deserializer, serialize
from .tsdb_error import TSDBStatus
from .tsdb_ops import *
import procs


def trigger_callback_maker(pk, target, calltomake):
    '''
    Calculates trigger coroutines

    Parameters
    ----------
    pk : any hashable type
        Primary key of database entry on which to run trigger coroutine
    target: list
        Metadata field(s) to which to apply the result(s) of the trigger
        coroutine
    calltomake : TSDBOp
        Database network operation to make with the result of the trigger
        coroutine

    Returns
    -------
    Nothing, modifies in place.
    '''
    def callback_(future):
        result = future.result()
        if target is not None:
            calltomake(pk, dict(zip(target, result)))
        return result
    return callback_


class TSDBProtocol(asyncio.Protocol):
    '''
    Protocols for the time series database. Unpack the json-encoded database
    operations and run them.
    '''
    def __init__(self, server, verbose=False):
        '''
        Initializes the TSDBProtocol class.

        Parameters
        ----------
        server : TSDBServer
            The server on which tsdb network operations are run
        verbose : boolean
            Determines whether status updates are printed

        Returns
        -------
        An initialized TSDB protocol object
        '''
        self.server = server
        self.deserializer = Deserializer()
        self.futures = []
        self.verbose = verbose

    def _insert_ts(self, op):
        '''
        Protocol for inserting a time series into the database.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for inserting a time series

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # try to insert the time series, raise value error if the
        # primary key is invalid
        try:
            self.server.db.insert_ts(op['pk'], op['ts'])
        except ValueError:
            return TSDBOp_Return(TSDBStatus.INVALID_KEY, op['op'])

        # run any triggers that result from inserting time series data
        self._run_trigger('insert_ts', [op['pk']])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _upsert_meta(self, op):
        '''
        Protocol for upsertinh (insertinh/updatinh) metadata for a database
        entry. Requires that the metadata fields are in the schema.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for upserting metadata

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''
        # upsert the metadata
        self.server.db.upsert_meta(op['pk'], op['md'])

        # run any triggers that result from upserting metadata
        self._run_trigger('upsert_meta', [op['pk']])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _select(self, op):
        '''
        Protocol for running a select command on the database.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for select

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # run select command
        loids, fields = self.server.db.select(
            op['md'], op['fields'], op['additional'])

        # run any triggers that result from selecting data
        # differs from augmented select, because this will also
        # upsert the results of running the trigger(s)
        self._run_trigger('select', loids)

        # create ordered dictionary with primary keys and data returned
        if fields is not None:
            d = OrderedDict(zip(loids, fields))
        else:
            d = OrderedDict((k, {}) for k in loids)

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'], d)

    def _augmented_select(self, op):
        '''
        Protocol for running an augmented select command on the database,
        i.e. select database entries based on specified criteria, then run
        a coroutine (trigger).
        Note: result of coroutine is returned to user and is not upserted.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for augmented select

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # run 'normal' select
        loids, fields = self.server.db.select(op['md'], None, op['additional'])

        # name of procs module to apply
        proc = op['proc']

        # possible additional arguments ('sort_by' and 'order')
        arg = op['arg']

        # array of field names to which to apply the results
        # note: result is only returned to user and is not upserted
        target = op['target']

        # wrap as list if necessary
        if not isinstance(target, list):
            target = [target]

        # run procs module on all returned database entries
        mod = import_module('procs.'+proc)
        storedproc = getattr(mod, 'proc_main')
        results = []
        for pk in loids:
            row = self.server.db.rows[pk]
            result = storedproc(pk, row, arg)
            results.append(dict(zip(target, result)))

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'],
                             dict(zip(loids, results)))

    def _add_trigger(self, op):
        '''
        Protocol for adding a trigger - similar to an event in asynchronous
        programming, i.e. will take some action when a certain event happens.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for adding a trigger

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''
        # the module in procs with a coroutine that defines
        # the action to take when the trigger is met
        trigger_proc = op['proc']

        # load the coroutine associated with the trigger
        # return an error if this is not possible/well defined
        try:
            mod = import_module('procs.' + trigger_proc)
            storedproc = getattr(mod, 'main')
        except:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # the operation that triggers the coroutine (e.g. 'insert_ts')
        trigger_onwhat = op['onwhat']

        # check that this is a valid operation
        if trigger_onwhat not in typemap:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # array of field names to which to apply the results of the coroutine
        trigger_target = op['target']

        # check that these are valid field names
        if trigger_target is not None:
            for t in trigger_target:
                if t not in self.server.db.schema:
                    return TSDBOp_Return(
                        TSDBStatus.INVALID_OPERATION, op['op'])

        # possible additional arguments ('sort_by' and 'order')
        trigger_arg = op['arg']

        # update trigger list
        self.server.triggers[trigger_onwhat].append(
            (trigger_proc, storedproc, trigger_arg, trigger_target))

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _remove_trigger(self, op):
        '''
        Protocol for removing a previously-set trigger.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for removing a trigger

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # the module in procs with a coroutine that defines
        # the action to take when the trigger is met
        trigger_proc = op['proc']

        # the operation that triggers the coroutine (e.g. 'insert_ts')
        trigger_onwhat = op['onwhat']

        # check that this is a valid operation
        if trigger_onwhat not in typemap:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # look up all triggers associated with that operation
        trigs = self.server.triggers[trigger_onwhat]

        # keep track of number of triggers removed
        removed = 0

        # remove all instances of the particular coroutine associated
        # with that operation
        for t in trigs:
            if t[0] == trigger_proc:
                trigs.remove(t)
                removed += 1

        # confirm that at least one trigger has been removed
        if removed == 0:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _run_trigger(self, opname, rowmatch):
        '''
        Protocol for running a trigger.

        Parameters
        ----------
        opname : TSDBOp
            TSDB network operation that caused the trigger
        rowmatch : list
            List of primary keys, identifies database entries on which
            to run the trigger coroutine.

        Returns
        -------
        None, modifies in place.
        '''

        # look up the triggers associated with the network operation
        lot = self.server.triggers[opname]

        # status update
        if self.verbose: print("S> list of triggers to run", lot)

        # loop through all relevant trigger coroutines
        for tname, t, arg, target in lot:

            # loop through all applicable primary keys
            # and apply the trigger coroutine
            for pk in rowmatch:
                row = self.server.db.rows[pk]
                task = asyncio.ensure_future(t(pk, row, arg))
                task.add_done_callback(
                    trigger_callback_maker(pk, target,
                                           self.server.db.upsert_meta))

    def connection_made(self, conn):
        '''
        Protocol for a made connection.
        '''
        if self.verbose: print('S> connection made')
        self.conn = conn

    def data_received(self, data):
        '''
        Protocol for receiving data.

        Parameters
        ----------
        data : bytes
            Binary data to be deserialized

        Returns
        -------
        Nothing, modifies in place.
        '''

        # add the newly received data to the deserializer queue
        self.deserializer.append(data)

        # wait until the deserializer is ready (i.e. have enough data)
        if self.deserializer.ready():

            # deserialize
            msg = self.deserializer.deserialize()

            # initialize status and response
            status = TSDBStatus.OK  # until proven otherwise.
            response = TSDBOp_Return(status, None)  # until proven otherwise.

            # try to convert to TSDBOp class
            try:
                op = TSDBOp.from_json(msg)
            except TypeError:
                status = TSDBStatus.INVALID_OPERATION
                response = TSDBOp_Return(status, None)

            # if we converted successfully, carry out the relevant operation
            if status is TSDBStatus.OK:
                if isinstance(op, TSDBOp_InsertTS):
                    response = self._insert_ts(op)
                elif isinstance(op, TSDBOp_UpsertMeta):
                    response = self._upsert_meta(op)
                elif isinstance(op, TSDBOp_Select):
                    response = self._select(op)
                elif isinstance(op, TSDBOp_AugmentedSelect):
                    response = self._augmented_select(op)
                elif isinstance(op, TSDBOp_AddTrigger):
                    response = self._add_trigger(op)
                elif isinstance(op, TSDBOp_RemoveTrigger):
                    response = self._remove_trigger(op)
                else:
                    response = TSDBOp_Return(
                        TSDBStatus.UNKNOWN_ERROR, op['op'])

            # serialize the operation response
            self.conn.write(serialize(response.to_json()))

            # close the connection
            self.conn.close()

    def connection_lost(self, transport):
        '''
        Protocol for a closed/lost connection.
        '''
        if self.verbose: print('S> connection lost')


class TSDBServer(object):
    '''
    Callback-based asynchronous socket server.
    '''

    def __init__(self, db, port=9999, verbose=False):
        '''
        Initializes the class.

        Parameters
        ----------
        db : DictDB object
            The underlying dictionary-based database.
        port : int
            Specifies the port the database client uses (default=9999)
        verbose : boolean
            Determines whether status updates are printed

        Returns
        -------
        An initialized TSDB server object
        '''
        self.port = port
        self.db = db
        self.triggers = defaultdict(list)
        self.trigger_arg_cache = defaultdict(dict)
        self.autokeys = {}
        self.verbose = verbose

    def exception_handler(self, loop, context):
        '''
        Stops the ayncio event loop if an exception occurs, i.e. stop
        listening for new database network operations.

        Parameters
        ----------
        loop : asyncio event loop
            The active 'listener'
        context : str
            Context in which the exception occurred

        Returns
        -------
        Nothing, modifies in place.
        '''
        # status update
        if self.verbose: print('S> EXCEPTION:', str(context))

        # stop ayncio event loop (listener)
        loop.stop()

    def run(self):
        '''
        Runs the TSDB Server; listens for database network operations.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # status update
        if self.verbose: print('S> Starting TSDB server on port', self.port)

        # initialize ayncio event loop
        loop = asyncio.get_event_loop()

        # enable to stop on an error
        # loop.set_exception_handler(self.exception_handler)

        # listen for tsdb network operations
        self.listener = loop.create_server(
            lambda: TSDBProtocol(self), '127.0.0.1', self.port)
        listener = loop.run_until_complete(self.listener)

        # keep listening until complete/error/interrupted
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            if self.verbose: print('S> Exiting.')
        except Exception as e:
            if self.verbose: print('S> Exception:', e)
        finally:
            listener.close()
            loop.close()
