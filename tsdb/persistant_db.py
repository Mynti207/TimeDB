from collections import defaultdict
from operator import and_
from functools import reduce
from .indexes import PrimaryIndex, BinTreeIndex
from .heaps import TSHeap, MetaHeap
import operator
import os
import sys
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


class PersistantDB:

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
            pk

    Files on disk: [All the files are saved in the directory 'data_dir']
        - TSHeap:
            {'db_name'}_hts stores in a binary file the raw timeseries
            sequentially with ts_length stored at the beginning of the file.
            offset_in_TSHeap for each timeseries is stored in PrimaryIndex
        - MetaHeap:
            {'db_name'}_hmeta stores in a binary file the all the fields in
            meta and offset_in_TSHeap for each timeseries.
            offset_in_MetaHeap for each timeseries is stored in PrimaryIndex
        - PrimaryIndex:
            {'db_name'}_pk.idx
        - BinaryTreeIndex/BitMap index:
            {'db_name'}_{'field'}.idx

    NB:
        - For the deletion, we update the meta field 'deleted' in the meta heap
            but we then remove the pk from the indexes,both primary index and
            other. As a result, the field deleted will actually never be used
            when set to true as it's not indexed in that case.

    TODO:
        - extend to the vantage point
        - extend to isax support
        - implement the BitMap indices
        - modify the way to store on disk to use a log and commit by batch
            instead of element by element. Changes to do in indexes.py, could use
            a temporary log on memory keeping track of the last uncommited changes.

    '''

    def __init__(self, schema, pkfield, ts_length, db_name="default",
                 data_dir="db_files", verbose=False):
        '''
        args:
            schema: schema of the db, if None load from disk
            ts_length: length of the times/values sequences (all ts need same length)
        '''

        self.db_name = db_name
        # Directory to save files
        self.data_dir = data_dir+"/"+db_name
        self.schema = schema

        self.ts_length = ts_length
        self.pkfield = pkfield

        # Intialize primary key index
        self.pks = PrimaryIndex(pkfield, self.data_dir)
        print('After init pks: ', self.pks.index)

        # TODO
        # Initialize indexes defined in schema
        self.indexes = {}
        for field, value in self.schema.items():
            if value['index'] is not None:
                # Index structure depends on its cardinality
                if value['index'] == 1:
                    # TODO
                    # self.indexes[field] = BMaskIndex(field)
                    self.indexes[field] = BinTreeIndex(field, self.data_dir)
                elif value['index'] == 2:
                    self.indexes[field] = BinTreeIndex(field, self.data_dir)
                else:
                    raise ValueError('Wrong index field in schema')

        # set up directory for db data
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Raw time series stored in TSHeap file at ts_offset stored
        # in field 'ts' of metadata
        # Metdata stored in MetaHeap file at pk_offset stored in the
        # primary key index pks as value
        self.meta_heap = MetaHeap(self.data_dir + '_hmeta', schema)
        self.ts_heap = TSHeap(self.data_dir + '_hts', self.ts_length)

        # whether status updates are printed
        self.verbose = verbose

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
        Given a pk and a timeseries, insert them
        '''
        # check that pk is a hashable type and not already present
        self._valid_pk(pk)
        self._check_presence(pk)

        # DEBUG
        print('Before insert, state of pks: ', self.pks.index)

        # Insert ts
        ts_offset = self.ts_heap.write_ts(ts)

        # Create default metadata for the new timeseries
        # (using the helper because creation of meta)
        pk_offset = self._upsert_meta({})

        # Set the primary key index with the offsets tuple
        self.pks[pk] = (ts_offset, pk_offset)

        self.update_indices(pk)

        # DEBUG
        print('After insert, state of pks: ', self.pks.index)

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
        # check that pk is a hashable type
        self._valid_pk(pk)
        self._check_presence(pk, present=False)

        # Extract meta to update later the index
        meta = self._get_meta(pk)

        # mark as deleted
        self._upsert_meta({'deleted': True}, offset=self.pks[pk][1])

        # pop from primary key index
        self.pks.remove_pk(pk)
        # remove from other indexed fields (deleted field included)
        self.remove_indices(pk, meta)

    def commit(self):
        '''
        Changes are final only when commited.
        Saving the heaps and indexes.
        '''
        pass

    def upsert_meta(self, pk, meta):
        '''
        Implement upserting field values, as long as fields are in the schema
        '''
        # check that pk is a hashable type
        self._valid_pk(pk)
        self._check_presence(pk, present=False)

        # Read previous meta
        prev_meta = self._get_meta(pk)

        self._upsert_meta(meta, offset=self.pks[pk][1])
        self.update_indices(pk, prev_meta=prev_meta)

    def _upsert_meta(self, meta, offset=None):
        '''
        Helper to upsert meta in the heap.
        '''
        # Upsert meta (meta already inserted with insert_ts)
        return self.meta_heap.write_meta(meta, offset)

    def _get_meta(self, pk):
        '''
        Helper to get meta from the pk
        '''
        offset = self.pks[pk][1]
        # Extract meta from heap
        meta = self.meta_heap.read_meta(offset)
        # Add the pkfield
        meta[self.pkfield] = pk

        return meta

    def _get_ts(self, pk):
        '''
        Helper to get raw timeseries from the pk
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
        # Read meta
        meta = self._get_meta(pk)

        # check that prev_meta is a dictionary
        if prev_meta is not None:
            if not isinstance(prev_meta, dict):
                raise ValueError('Prev_meta need to be a dictionary instead of {}'.format(type(prev_meta)))
            for field, index in self.indexes.items():
                # Remove previous index if changed
                if prev_meta[field] != meta[field]:
                    index.remove_pk(prev_meta[field], pk)

        for field, index in self.indexes.items():
            # Create a new Node if needed
            if meta[field] not in index:
                index[meta[field]] = set()
            # add pk to the index
            index[meta[field]].add(pk)

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
            # Check if field is indexed
            if field in self.indexes.keys():
                index = self.indexes[field]
                # remove pk for the previous value
                index.remove_pk(meta[field], pk)

    # TODO: still being debugged
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
            if self.verbose: print('S> D> NO FIELDS')
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                if self.verbose: print('S> D> ALL FIELDS')
                matchedfielddicts = [{k: v for k, v in self._get_meta(pk).items()
                                      if k != 'ts' and k != 'deleted'}
                                     for pk in pks]  # remove ts
            else:
                if self.verbose: print('S> D> FIELDS {}'.format(fields))
                matchedfielddicts = [{k: v for k, v in self._get_meta(pk).items()
                                      if k in fields} for pk in pks]

        # return output of select statament
        return pks, matchedfielddicts
