from .indexes import PrimaryIndex, BinTreeIndex, BitMapIndex, TriggerIndex
from .heaps import TSHeap, MetaHeap
import operator
import os
from .isax import *
import pickle

# dictionary that maps operator functions, useful for select operations
OPMAP = {
    '<': operator.lt,
    '>': operator.gt,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}

identity = lambda x: x


class PersistentDB:

    '''
    Database implementation with persistency.

    Structure:
        - TSHeap to store the raw timeseries in a binary file
            We choose to use the same length for all the timeseries in the db,
            making it easier to encode the binary file.
        - MetaHeap to store the metadata for each timeserie present in the db.
            We choose to initialize all the fields for each new insertion, then
            the user can call upsert_meta to update the fields
        - PrimaryIndex to store the mapping {'pk': ('offset_in_TSHeap',
            'offset_in_MetaHeap')}
        - BinaryTreeIndex/BitMap index for other fields in the schema labelled
            with an index. Store as key the field name and as value
            pk set or pk bitmap.

    Files on disk: [All the files are saved in the directory 'data_dir',
        under the sub-directory 'db_name']
        - TSHeap:
            heap_ts stores in a binary file the raw timeseries
            sequentially with ts_length stored at the beginning of the file.
            offset_in_TSHeap for each timeseries is stored in PrimaryIndex
        - MetaHeap:
            heap_meta stores in a binary file the all the fields in
            meta.
            offset_in_MetaHeap for each timeseries is stored in PrimaryIndex
        - PrimaryIndex:
            pk.idx
        - BinaryTreeIndex index:
            index_{'field'}.idx
        - BitMap index:
            index_{'field'}.idx (bitmap encoding)
            index_{'field'}_pks.idx (for conversion to/from bitmap)
        - TriggerIndex for server-side trigger operations. Stored as dictionary
            of lists.
            triggers.idx
        - schema (dictionary): to allow the user to make changes to the schema
            schema.idx
    NB:
        - For the deletion, we update the meta field 'deleted' in the meta heap
            but we then remove the pk from the indexes, both primary index and
            other. As a result, the field deleted will actually never be used
            when set to true as it's not indexed in that case.

    TODO:
        - modify the way to store on disk to use a log and commit by batch
            instead of element by element. Changes to do in indexes.py, could
            use a temporary log on memory keeping track of the last uncommited
            changes.
        - implement atomic transactions, with appropriate exception handling
            and rollback on fail

    '''

    def __init__(self, schema, pkfield, ts_length, db_name="default",
                 data_dir="db_files", commit_step=10):
        '''
        Initializes the PersistentDB class.

        Parameters
        ----------
        schema : dictionary
            Specifies the fields to be included in the database and their
            formats
        pkfield : string
            Specifies the name of the primary key field
        ts_length : int
            Length of the time series we will insert (immutable attribute)

        Returns
        -------
        An initialized PersistentDB object
        '''
        self.db_name = db_name

        # directory to save files
        self.data_dir = data_dir + '/' + db_name

        # check if a schema already exists
        try:
            self.load_schema()

        # otherwise use provided schema
        except:
            self.schema = schema

        # set up directory for db data
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self.ts_length = ts_length
        self.pkfield = pkfield

        # intialize primary key index
        self.pks = PrimaryIndex(pkfield, self.data_dir + '/')

        # initialize indexes defined in schema
        self.indexes = {}
        for field, value in self.schema.items():
            self._init_indexes(field, value)

        # raw time series stored in TSHeap file at ts_offset stored
        # in field 'ts' of metadata
        # metadata stored in MetaHeap file at pk_offset stored in the
        # primary key index pks as value
        self.meta_heap = MetaHeap(self.data_dir, '/heap_meta', self.schema)
        self.ts_heap = TSHeap(self.data_dir, '/heap_ts', self.ts_length)

        # set closed to False
        self.closed = False

        # initializes file structure for isax tree
        self.fs = TreeFileStructure()

        # initializes isax tree
        # includes pointer to file structure
        self.tree = iSaxTree('root')

        # populates isax tree with any data already in memory
        for pk in self.pks.keys():
            try:
                self.tree.insert(self._get_ts(pk).values(),
                                 tsid=pk, fs=self.fs)
            except ValueError:  # shouldn't happen - data came from memory
                return ValueError('Not compatible with tree structure.')

        # triggers associated with database operations
        # dictionary of sets, defined as Index to facilitate commits
        self.triggers = TriggerIndex('triggers', self.data_dir + '/')

        # local counter for the batch commit of the indexes
        # count the remaining operations before commit
        self.next_commit = commit_step
        self.commit_step = commit_step

    def load_schema(self):
        '''
        Helper function: loads schema and fills in identity function.

        Parameters
        ----------
        Nothing

        Returns
        -------
        Nothing.
        '''

        # load file
        with open(self.data_dir + '/schema.idx', "rb", buffering=0) as fd:
            self.schema = pickle.load(fd)

        # convert identity function where necessary
        for field, specs in self.schema.items():
            if specs['convert'] == 'IDENTITY':
                specs['convert'] = identity

    def close(self):
        '''
        Close the files and set closed to True

        Parameters
        ----------
        Nothing

        Returns
        -------
        Nothing.
        '''
        self._assert_not_closed()

        # commit primary keys
        self.pks.commit()

        # closing files
        self.meta_heap.close()
        self.ts_heap.close()

        # update status
        self.closed = True

    def _assert_not_closed(self):
        '''
        Check if db is closed.

        Parameters
        ----------
        Nothing

        Returns
        -------
        Nothing
        '''
        if self.closed:
            raise ValueError('Database closed.')

    def _init_indexes(self, field, value):
        '''
        Helper to init an index from schema if needed

        Parameters
        ----------
        field : str
            Field of a schema
        value : dict
            Value corresponding to the field in the schema.
            Key index will determine which index to create.

        Returns
        -------
        Nothing, modifies in-place self.index
        '''
        # check if index not previously set
        if field in self.indexes:
            raise ValueError('Field {} already indexed'.format(field))

        # check index is needed
        if value['index'] is not None:
            # index structure depends on its cardinality
            if value['index'] == 1:  # any other cardinality
                self.indexes[field] = BinTreeIndex(
                    field, self.data_dir + '/index_')
            elif value['index'] == 2:  # boolean
                self.indexes[field] = BitMapIndex(
                    field, self.data_dir + '/index_', value['values'])
            else:
                raise ValueError('Wrong index field in schema')

    def _valid_pk(self, pk):
        '''
        Helper to check if pk is a valid primary key, i.e. a string in
        the current implementation
        '''
        if not isinstance(pk, str):
            raise ValueError('Primary key is not a hashable type.')

    def _check_presence(self, pk, present=True):
        '''
        Helper to check if pk is already present
        '''
        if present and pk in self.pks:
            raise ValueError('Primary Key {} already in the db'.format(pk))
        elif not present and pk not in self.pks:
            raise ValueError('Primary Key {} not in the db'.format(pk))

    def insert_ts(self, pk, ts):
        '''
        Inserts a time series into the database.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry
        ts : TimeSeries
            Time series to be inserted into the database.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # check db open
        self._assert_not_closed()

        # check that pk is a hashable type and not already present
        self._valid_pk(pk)
        self._check_presence(pk)

        # check that the time series is the correct length
        if len(ts) != self.ts_length:
            raise ValueError('Time series is the wrong length.')

        # insertion on heaps -->

        # insert ts
        ts_offset = self.ts_heap.write_ts(ts)

        # insert default metadata
        pk_offset = self._upsert_meta({})

        # insertion on indexes -->

        # TODO: insertion on log first and then on index by batch
        # set the primary key index with the offsets tuple
        self.pks[pk] = (ts_offset, pk_offset)
        # commit to disk the index
        if self.next_commit == 0:
            self.pks.commit()
            self.next_commit = self.commit_step
        else:
            self.next_commit -= 1

        # update indices for primary key
        self.update_indices(pk)

        # insert into isax tree
        try:
            self.tree.insert(ts.values(), tsid=pk, fs=self.fs)
        except ValueError:
            return ValueError('Not compatible with tree structure.')

    def delete_ts(self, pk):
        '''
        Marks a time series as deleted.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the entry to be deleted

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # check db open
        self._assert_not_closed()

        # check that pk is a hashable type
        self._valid_pk(pk)
        self._check_presence(pk, present=False)

        # delete from isax tree
        try:
            self.tree.delete(self._get_ts(pk).values(), fs=self.fs)
        except ValueError:
            return ValueError('Not compatible with tree structure.')

        # extract meta to update later the index
        meta = self._get_meta(pk)

        # mark as deleted
        self._upsert_meta({'deleted': True}, offset=self.pks[pk][1])

        # pop from primary key index
        self.pks.remove_pk(False, pk)

        # commit to disk the index
        if self.next_commit == 0:
            self.pks.commit()
            self.next_commit = self.commit_step
        else:
            self.next_commit -= 1

        # remove from other indexed fields (deleted field included)
        self.remove_indices(pk, meta)

    def commit(self):
        '''
        Changes are final only when commited.
        Save the current state of the indexes on disk.

        Parameters
        ----------
        Nothing

        Returns
        -------
        Nothing
        '''
        self._assert_not_closed()
        # save primary indexes
        self.pks.commit()
        # save other indexes
        for index in self.indexes.values():
            index.commit()

    def add_trigger(self, onwhat, proc, storedproc, arg, target):
        '''
        Adds a trigger (similar to an event loop in asynchronous programming,
        i.e. will take some action when a certain event occurs.)

        Note: assumes that all error-checking has already been done at
        server-level before calling this function.

        Parameters
        ----------
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        storedproc : function
            Function that defines the action to take when the trigger is met
        arg : string
            Possible additional arguments for the function
        target : string
            Array of field names to which to apply the results of the coroutine

        Returns
        -------
        Nothing, modifies in-place.
        '''

        self.triggers.add_trigger(onwhat, (proc, storedproc, arg, target))

    def remove_trigger(self, proc, onwhat, target):
        '''
        Removes a previously-set trigger.

        Note: assumes that all error-checking has already been done at
        server-level before calling this function.

        Parameters
        ----------
        proc : string
            Name of the module in procs that defines the trigger action
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        target : string
            Field name where coroutine result will be stored

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # delete all triggers associated with the action and coroutine
        if target is None:
            self.triggers.remove_all_triggers(onwhat, proc)

        # only remove a particular trigger
        # (used to delete vantage point representation)
        else:
            self.triggers.remove_one_trigger(onwhat, proc, target)

    def insert_vp(self, pk):
        '''
        Adds a vantage point (i.e. an existing time series) to the database.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry

        Returns
        -------
        dix : string
            ID of the field that will store the distance to the vantage point
        pk : string
            The primary key of the vantage point
        ts : TimeSeries
            The time series data of the vantage point
        '''
        # check db open
        self._assert_not_closed()

        # check that pk is a hashable type
        self._valid_pk(pk)

        # check that the primary key is present in the database
        self._check_presence(pk, present=False)

        # check that the primary key is not already set as a vantage point
        if pk in self.indexes['vp'][True]:
            raise ValueError('Primary key is already set as vantage point.')

        # mark time series as vantage point
        # calling upsert meta to updates the indices
        self.upsert_meta(pk, {'vp': True})

        # create new field name for distance to vantage point
        didx = 'd_vp_' + pk

        # add distance field to schema and index it
        value = {'type': 'float', 'convert': float, 'index': 1}
        self.schema[didx] = value

        # update the meta heap with the new schema
        self.meta_heap.reset_schema(self.schema, self.pks)
        # Commit to disk the index
        if self.next_commit == 0:
            self.pks.commit()
            self.next_commit = self.commit_step
        else:
            self.next_commit -= 1

        # add the new index
        self._init_indexes(didx, value)
        # update inverse-lookup index dictionary
        self.index_bulk()

        # fields for additional server-side operations:
        # add trigger to calculate distance when a new time series is added
        # calculate distance for all existing time series
        return didx, pk, self._get_ts(pk)

    def delete_vp(self, pk, raise_error=True):
        '''
        Unmarks a time series as a vantage point.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry
        raise_error : boolean
            Determines whether a ValueError is raised when trying to unmark
            a time series that is not actually marked as a vantage point.
            Used when deleting time series, to check whether it needs to
            also be unmarked.

        Returns
        -------
        dix : string
            ID of the field that previously stored the distance to the
            vantage point
        '''
        # check db open
        self._assert_not_closed()

        # check that pk is a hashable type
        self._valid_pk(pk)

        # check that the primary key is present in the database
        self._check_presence(pk, present=False)

        # check that the primary key is set as a vantage point
        if pk in self.indexes['vp'][False]:
            if raise_error:
                raise ValueError('Primary key is not set as a vantage point.')
            else:
                return

        # remove time series marker as vantage point
        # calling upsert meta to updates the indices
        self.upsert_meta(pk, {'vp': False})

        # get vantage point id
        didx = 'd_vp_' + pk

        # delete from schema
        del self.schema[didx]
        # Update the meta heap with the new schema
        self.meta_heap.reset_schema(self.schema, self.pks)
        # Commit to disk the index
        if self.next_commit == 0:
            self.pks.commit()
            self.next_commit = self.commit_step
        else:
            self.next_commit -= 1

        # erase inverse-lookup index for the distance vp
        self.indexes[didx]._erase()
        del self.indexes[didx]

        # additional server-side operation:
        # remove trigger to calculate distance when a new time series is added
        return didx

    def upsert_meta(self, pk, meta):
        '''
        Upserts (inserts/updates) metadata for a database entry. Requires
        that the metadata fields are in the schema.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the  database entry
        meta : dictionary
            Metadata to be upserted into the database.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # check db open
        self._assert_not_closed()

        # check that pk is a hashable type
        self._valid_pk(pk)
        self._check_presence(pk, present=False)

        # read previous meta
        prev_meta = self._get_meta(pk)

        self._upsert_meta(meta, offset=self.pks[pk][1])
        self.update_indices(pk, prev_meta=prev_meta)

    def _upsert_meta(self, meta, offset=None):
        '''
        Helper to write meta at offset (or append) in the heap file

        Parameters
        ----------
        meta : dictionary
            Metadata to be upserted into the database.
        [offset : int]
            Offset where to upsert the meta, if None append to the file.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # upsert meta (meta already inserted with insert_ts)
        return self.meta_heap.write_meta(meta, offset)

    def _get_meta(self, pk):
        '''
        Helper to get meta from the heap

        Parameters
        ----------
        pk : any hashable type
            Primary key for the  database entry

        Returns
        -------
        meta data dictionary
        '''
        offset = self.pks[pk][1]
        # extract meta from heap
        meta = self.meta_heap.read_meta(offset)
        # add the pkfield
        meta[self.pkfield] = pk

        return meta

    def _get_ts(self, pk):
        '''
        Helper to get the timerseries from the heap

        Parameters
        ----------
        pk : any hashable type
            Primary key for the  database entry

        Returns
        -------
        TimeSeries object
        '''
        offset = self.pks[pk][0]
        return self.ts_heap.read_ts(offset)

    def index_bulk(self, pks=[]):
        if len(pks) == 0:
            pks = self.pks.index.keys()
        for pk in pks:
            self.update_indices(pk)

    def update_indices(self, pk, prev_meta=None):
        '''
        Updates inverse-lookup index dictionary for a given database entry.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the database entry
        prev_meta: dictionary of metadata
            Previous values, need to be removed from the indices

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # read meta
        meta = self._get_meta(pk)

        # check that prev_meta is a dictionary
        if prev_meta is not None:
            if not isinstance(prev_meta, dict):
                raise ValueError(
                    'Prev_meta need to be a dictionary instead of {}'.format(type(prev_meta)))
            for field, index in self.indexes.items():
                # Remove previous index if changed
                if prev_meta[field] != meta[field]:
                    index.remove_pk(prev_meta[field], pk)

        for field, index in self.indexes.items():
            # create a new Node if needed
            if meta[field] not in index:
                index.add_key(meta[field])
            # add pk to the index
            index.add_pk(meta[field], pk)
            # commit to disk the update
            index.commit()

    def remove_indices(self, pk, meta):
        '''
        Updates inverse-lookup index dictionary for a database entry deletion.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the former database entry
        meta : dictionary
            The time series and metadata for the deleted entry

        Returns
        -------
        Nothing, modifies in-place.
        '''

        for field, value in meta.items():
            # check if field is indexed
            if field in self.indexes.keys():
                index = self.indexes[field]
                # remove pk for the previous value
                index.remove_pk(meta[field], pk)
                # commit to disk the update
                index.commit()

    def select(self, meta, fields, additional):
        '''
        Select database entries based on specified criteria.

        Parameters
        ----------
        meta : dictionary
            Criteria to apply to metadata
        fields : list
            List of fields to return
        additional : dictionary
            Additional criteria, e.g. apply sorting

        Returns
        -------
        (pks, matchedfielddicts) : (list, dictionary)
            Selected primary keys; entire selected data
        '''
        # check db open
        self._assert_not_closed()

        # start with the set of all primary keys present in the db
        pks = set(self.pks.index.keys())

        # loop through each specified metadata criterion
        for field, value in meta.items():

            # check if the field is in the schema
            if field in self.schema:

                # look up the conversion operator for that field
                conversion = self.schema[field]['convert']

                # case 1: the metadata criterion is a dictionary
                if isinstance(value, dict):

                    # loop through each sub-criterion
                    for op in value:

                        # identify the operation and the value
                        # e.g. > 2 : the operator is > and the value is 2
                        # TODO: optimize operation with BST ordering
                        operation = OPMAP[op]
                        val = conversion(value[op])

                        # identify the entries that meet the sub-criterion
                        filtered_pks = set()
                        for i in self.indexes[field].keys():
                            if operation(i, val):
                                filtered_pks.update(self.indexes[field][i])

                        # update the set of primary keys by applying an
                        # AND with the filtered primary keys
                        pks = pks.intersection(filtered_pks)

                # case 2: the metadata criterion is a list
                elif isinstance(value, list):

                    # convert the values to the appropriate type
                    converted_values = [conversion(v) for v in value]

                    # if the index is present
                    if field in self.indexes:
                        selected = set()
                        for v in converted_values:
                            selected.update(self.indexes[field][v])

                    # if the index is not present
                    else:
                        selected = set()
                        for pk in pks:
                            # need to load the meta
                            meta = self._get_meta(pk)
                            if field in meta.keys() and meta[field] in converted_values:
                                selected.add(pk)

                    # update the set of primary keys by applying an
                    # AND with the selected primary keys
                    pks = pks.intersection(selected)

                # case 3: the metadata criterion is a precise value
                elif isinstance(value, (int, float, str)):
                    # case field is pk
                    if field == self.pkfield:
                        if conversion(value) in self.pks:
                            # Selection contains only one element
                            selected = set([conversion(value)])
                        # Empty selection
                        else:
                            pks = set()
                            break
                    # case field is an index (not the primary key)
                    elif field in self.indexes:
                        if conversion(value) in self.indexes[field]:
                            selected = set(self.indexes[field][conversion(value)])
                        # Empty selection
                        else:
                            pks = set()
                            break

                    # case field is not indexed
                    # TODO: merge this case with the previous one
                    else:
                        selected = set()
                        for pk in pks:
                            # need to load the meta
                            meta = self._get_meta(pk)
                            if field in meta.keys() and meta[field] == conversion(value):
                                selected.add(pk)

                    # update the set of primary keys by applying an
                    # AND with the selected primary keys
                    pks = pks.intersection(selected)

                # case 4: some other incorrect type - return nothing
                else:
                    pks = set()

        # convert the remaining (selected) primary key ids to a list
        pks = list(pks)

        # check if additional parameters have been specified
        if additional is not None:

            # sort the return values
            if 'sort_by' in additional:

                # sort format
                # +: ascending, -: descending
                # assume ascending order if unspecified
                sort_type = additional['sort_by'][:1]
                if sort_type == '+' or sort_type == '-':
                    predicate = additional['sort_by'][1:]
                else:
                    predicate = additional['sort_by'][:]
                reverse = True if sort_type == '-' else False

                # sanity check
                if (predicate not in self.schema or
                        (predicate not in self.indexes and
                            predicate != self.pkfield)):
                    raise ValueError('Additional field {} not in schema or in '
                                     'indexes'.format(predicate))
                # case predicate is pkfield
                if predicate == self.pkfield:
                    pks.sort(reverse=reverse)
                # case predicate field of schema
                # TODO: improve sorting for indexed field
                else:
                    # Loading the meta
                    # TODO: improve because need only one meta
                    metas = {pk: self._get_meta(pk) for pk in pks}
                    # in-place sorting
                    pks.sort(key=lambda pk: metas[pk][predicate],
                             reverse=reverse)

                # limit the number of return values
                # assume this only applies when sorting, e.g. return the top 10
                if 'limit' in additional:
                    pks = pks[:additional['limit']]

        # extract the relevant sub-set of fields
        if fields is None:  # no sub-set is specified
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                matchedfielddicts = [{k: v for k, v in self._get_meta(pk).items()
                                      if k != 'ts' and k != 'deleted'}
                                     for pk in pks]  # remove ts
            else:

                # start with metadata (most common use case)
                matchedfielddicts = [{k: v for k, v in self._get_meta(pk).items()
                                      if k in fields} for pk in pks]

                # add in time series if necessary
                if 'ts' in fields:
                    for idx, pk in enumerate(pks):
                        matchedfielddicts[idx]['ts'] = self._get_ts(pk)

        # return output of select statament
        return pks, matchedfielddicts
