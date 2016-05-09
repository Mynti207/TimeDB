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
        self.deserializer = Deserializer(verbose=verbose)
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

    def _delete_ts(self, op):
        '''
        Protocol for deleting a time series from the database.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for deleting a time series

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # first try to delete the time series as a vantage point
        # don't raise errors, as the time series may not be a vantage point
        self.server.db.delete_vp(op['pk'], raise_error=False)

        # try to delete the time series, raise value error if the
        # primary key is invalid
        try:
            self.server.db.delete_ts(op['pk'])
        except ValueError:
            return TSDBOp_Return(TSDBStatus.INVALID_KEY, op['op'])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _insert_vp(self, op):
        '''
        Protocol for marking a time series as a vantage point.

        Steps:
        1. Sets the vantage point indicator in the time series' metadata
        to True.
        2. Adds a field to stores the distance from other time series
        from the database schema and index lookup.
        3. Adds a trigger that calculates the distance upon the insertion
        of new time series.
        4. Calculates the distance to all time series currently in the
        database.
        5. Adds the time series to the list of vantage points.

        Steps 1, 2, and 5 are carried out at the DictDB level. Steps 3 and 4
        are carried out server-side.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for removing a vantage point

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # try to add vantage point, raise value error if the primary
        # key is invalid
        try:
            idx, pk, ts = self.server.db.insert_vp(op['pk'])
        except ValueError:
            return TSDBOp_Return(TSDBStatus.INVALID_KEY, op['op'])

        # additional server-side operations:

        # add trigger to calculate distance when a new time series is added
        run_op = TSDBOp_AddTrigger(proc='corr', onwhat='insert_ts',
                                   target=[idx], arg=ts)
        result = self._add_trigger(run_op)

        # check that the operation was successful
        status, payload = result['status'], result['payload']
        if status != TSDBStatus.OK:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # calculate distance for all existing time series
        run_op = TSDBOp_AugmentedSelect(proc='corr', target=[idx], arg=ts,
                                        md={}, additional=None)
        result = self._augmented_select(run_op)

        # check that the operation was successful
        status, payload = result['status'], result['payload']
        if status != TSDBStatus.OK:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # upsert distance for each time series
        for pk, md in payload.items():

            # pack and run operation
            run_op = TSDBOp_UpsertMeta(pk=pk, md=md)
            result = self._upsert_meta(run_op)

            # check that the operation was successful
            status, payload = result['status'], result['payload']
            if status != TSDBStatus.OK:
                return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _delete_vp(self, op):
        '''
        Protocol for unmarking a time series as a vantage point.

        Steps:
        1. Sets the vantage point indicator in the time series' metadata
        to False.
        2. Removes the field that stores the distance from other time series
        from the database schema and index lookup.
        3. Removes the trigger that calculates the distance upon time
        series insertion.
        4. Clears the previously-calculated distances from the metadata.
        5. Removes the time series from the list of vantage points.

        Steps 1, 2, 4, and 5 are carried out at the DictDB level. Step 4
        is carried out server-side.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for removing a vantage point

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # try to delete vantage point, raise value error if the primary
        # key is invalid
        try:
            didx = self.server.db.delete_vp(op['pk'])
        except ValueError:
            return TSDBOp_Return(TSDBStatus.INVALID_KEY, op['op'])

        # additional server-side operation:
        # remove trigger to calculate distance when a new time series is added
        run_op = TSDBOp_RemoveTrigger(proc='corr', onwhat='insert_ts',
                                      target=[didx])
        result = self._remove_trigger(run_op)

        # check that the operation was successful
        status, payload = result['status'], result['payload']
        if status != TSDBStatus.OK:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # return status and payload
        return TSDBOp_Return(TSDBStatus.OK, op['op'])

    def _upsert_meta(self, op):
        '''
        Protocol for upserting (inserting/updating) metadata for a database
        entry. Requires that the metadata fields are in the schema.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for upserting metadata

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # upsert the metadata - raise ValueError if the primary key is invalid
        try:
            self.server.db.upsert_meta(op['pk'], op['md'])
        except ValueError:
            return TSDBOp_Return(TSDBStatus.INVALID_KEY, op['op'])

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

        # possible additional arguments for coroutine (e.g. time series)
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

    def _vp_similarity_search(self, op):
        '''
        Protocol for running a vantage point similarity search on the database,
        i.e. finding the 'closest' time series in the database.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for similarity search

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # check whether there are any vantage points - won't work otherwise!
        if len(self.server.db.vantage_points) == 0:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # compute distance to the query time series -->

        # check that a query time series is present
        if 'query' not in op:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # check that the query can be cast as a time series
        # no need to store here - will be carried out in coroutine
        if not isinstance(op['query'], TimeSeries):
            try:
                TimeSeries(*op['query'])
            except:
                return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # unpack operation parameters
        arg = op['query']  # query time series
        top = int(op['top']) if 'top' in op else 1  # number of TS to return

        # step 1: get distances from query time series to all vantage points

        run_op = TSDBOp_AugmentedSelect(
            md={'vp': {'==': True}}, proc='corr', arg=arg,
            target=['vpdist'], additional=None)
        result = self._augmented_select(run_op)
        status, payload = result['status'], result['payload']
        if status != TSDBStatus.OK:
            return TSDBOp_Return(TSDBStatus.UNKNOWN_ERROR, op['op'])

        # step 2: pick closest vantage point
        vpkeys = list(self.server.db.vantage_points.values())
        vpdist = {v: payload[v]['vpdist'] for v in vpkeys}
        nearest_vp = min(vpkeys, key=lambda v: vpdist[v])

        # step 3: define circle radius as 2 x distance to closest vantage point
        radius = 2 * vpdist[nearest_vp]

        # step 4: find relative index of nearest vantage point
        relative_idx = vpkeys.index(nearest_vp)

        # step 5: calculate distance to all time series within the radius
        run_op = TSDBOp_AugmentedSelect(
            md={'d_vp-{}'.format(relative_idx): {'<=': radius}},
            proc='corr', arg=op['query'], target=['towantedvp'],
            additional=None)
        result = self._augmented_select(run_op)
        status, payload = result['status'], result['payload']
        if status != TSDBStatus.OK:
            return TSDBOp_Return(TSDBStatus.UNKNOWN_ERROR, op['op'])

        # step 6: find the closest time series and return
        nearestwanted = [(k, payload[k]['towantedvp']) for k in payload.keys()]
        nearestwanted.sort(key=lambda x: x[1])
        nearestresult = {n[0]: n[1] for n in nearestwanted[:top]}

        # step 7: return status and payload
        if len(nearestresult) == 0:
            return TSDBOp_Return(TSDBStatus.NO_MATCH, op['op'], nearestresult)
        else:
            return TSDBOp_Return(TSDBStatus.OK, op['op'], nearestresult)

    def _isax_similarity_search(self, op):
        '''
        Protocol for running an iSAX similarity search on the database,
        i.e. finding the 'closest' time series in the database.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for similarity search

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # check that a query time series is present
        if 'query' not in op:
            return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # check query format, case as time series if necessary
        if isinstance(op['query'], TimeSeries):
            query = op['query']
        else:
            try:
                query = TimeSeries(*op['query'])
            except:
                return TSDBOp_Return(TSDBStatus.INVALID_OPERATION, op['op'])

        # unpack operation parameters
        query = query.values()  # query time series values

        # run similarity search
        result = self.server.db.tree.find_nbr(query)

        # return status and payload
        if result is None:
            # couldn't find a match
            return TSDBOp_Return(TSDBStatus.NO_MATCH, op['op'], result)
        else:
            # return id of nearest time series (string)
            return TSDBOp_Return(TSDBStatus.OK, op['op'], result)

    def _isax_tree(self, op):
        '''
        Protocol for generating a visual representation of the iSAX tree.

        Parameters
        ----------
        op : TSDBOp
            TSDB network operation for tree representation

        Returns
        -------
        TSDBOp_Return: status and payload; result of running the TSDB operation
        '''

        # generate representation
        result = self.server.db.tree.display_tree()

        # return status and payload
        if len(result) == 0:
            # nothing returned
            return TSDBOp_Return(TSDBStatus.UNKNOWN_ERROR, op['op'], result)
        else:
            # return id of nearest time series (string)
            return TSDBOp_Return(TSDBStatus.OK, op['op'], result)

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

        # possible additional arguments ('sort_by' and 'limit')
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

        # the field(s) to which the result of the coroutine is applied
        trigger_target = op['target']

        # delete all triggers associated with the action and coroutine
        if trigger_target is None:

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

        # only remove a particular trigger
        # (used to delete vantage point representation)
        else:

            # look up all triggers associated with that operation
            trigs = self.server.triggers[trigger_onwhat]

            # delete the relevant trigger
            for t in trigs:
                if t[0] == trigger_proc:  # matches coroutine
                    if t[3] == trigger_target:  # matches target
                        trigs.remove(t)

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
                elif isinstance(op, TSDBOp_DeleteTS):
                    response = self._delete_ts(op)
                elif isinstance(op, TSDBOp_UpsertMeta):
                    response = self._upsert_meta(op)
                elif isinstance(op, TSDBOp_Select):
                    response = self._select(op)
                elif isinstance(op, TSDBOp_AugmentedSelect):
                    response = self._augmented_select(op)
                elif isinstance(op, TSDBOp_VPSimilaritySearch):
                    response = self._vp_similarity_search(op)
                elif isinstance(op, TSDBOp_iSAXSimilaritySearch):
                    response = self._isax_similarity_search(op)
                elif isinstance(op, TSDBOp_iSAXTree):
                    response = self._isax_tree(op)
                elif isinstance(op, TSDBOp_AddTrigger):
                    response = self._add_trigger(op)
                elif isinstance(op, TSDBOp_RemoveTrigger):
                    response = self._remove_trigger(op)
                elif isinstance(op, TSDBOp_InsertVP):
                    response = self._insert_vp(op)
                elif isinstance(op, TSDBOp_DeleteVP):
                    response = self._delete_vp(op)
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
            The underlying dictionary-based database
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
