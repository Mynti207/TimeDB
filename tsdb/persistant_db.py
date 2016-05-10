from collections import defaultdict
from operator import and_
from functools import reduce
from .indexes import PrimaryIndex
from .heaps import TSHeap, MetaHeap
import timeseries
import operator
import os
import sys

# this dictionary will help you in writing a generic select operation
OPMAP = {
    '<': operator.lt,
    '>': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}


class PersistantDB:

    '''
    Database implementation with persistency
    TODO: decide if we keep ts_length as a required argument or allow for ts with different lengths,
    changes would need to be done in the heaps implementation to allow variable lengths
    '''

    def __init__(self, schema, pkfield, ts_length, db_name="default",
                 data_dir="db_files"):
        '''
        args:
            ts_length: length of the times/values sequences (all ts need same length)
        '''
        self.db_name = db_name

        # Directory to save files
        self.data_dir = data_dir+"/"+db_name
        self.ts_length = ts_length
        self.schema = schema
        self.pkfield = pkfield

        # Intialize primary key index
        self.pks = PrimaryIndex(pkfield, self.data_dir)
        print('After init pks: ', self.pks.index)

        # TODO
        # Initialize indexes defined in schema
        # self.indexes = {}
        # for field, value in self.schema.items():
        #     if value['index'] is not None:
        #         # Index structure depends on its cardinality
        #         if value['index'] == 1:
        #             # self.indexes[field] = BMaskIndex(field)
        #             self.indexes[field] = BinTreeIndex(field)
        #         elif value['index'] == 2:
        #             self.indexes[field] = BinTreeIndex(field)
        #         else:
        #             raise ValueError('Wrong index field in schema')

        # set up directory for db data
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Raw time series stored in TSHeap file at ts_offset stored
        # in field 'ts' of metadata
        # Metdata stored in MetaHeap file at pk_offset stored in the
        # primary key index pks as value
        self.meta_heap = MetaHeap(self.data_dir + '_hmeta', schema)
        self.ts_heap = TSHeap(self.data_dir + '_hts', self.ts_length)

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

        # Create metadata for the new timeseries
        # (using the helper because creation of meta)
        pk_offset = self._upsert_meta({'ts_offset': ts_offset}, offset=None)

        # Set the primary key index with the pk_offset
        self.pks[pk] = pk_offset

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

        # mark as deleted
        self._upsert_meta({'deleted': True}, self.pks[pk])

        # pop from primary key index
        self.pks.remove(pk)

        # TODO: update index structures

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

        self._upsert_meta(meta, self.pks[pk])
        self.update_indices(pk)

    def _upsert_meta(self, meta, offset):
        '''
        Helper to upsert meta in the heap.
        '''
        # Upsert meta (meta already inserted with insert_ts)
        return self.meta_heap.write_meta(meta, offset)

    def index_bulk(self, pks=[]):
        # TODO
        pass
        # if len(pks) == 0:
        #     pks=self.rows
        # for pkid in self.pks:
        #     self.update_indices(pkid)

    def update_indices(self, pk):
        '''
        Update the field indices other than self.pks
        '''
        # TODO
        pass
        # row = self.rows[pk]
        # for field in row:
        #     v = row[field]
        #     if self.schema[field]['index'] is not None:
        #         idx = self.indexes[field]
        #         idx[v].add(pk)

    def select(self, meta, fields, additional):
        # your code here
        # if fields is None: return only pks
        # like so [pk1,pk2],[{},{}]
        # if fields is [], this means all fields
        # except for the 'ts' field. Looks like
        # ['pk1',...],[{'f1':v1, 'f2':v2},...]
        # if the names of fields are given in the list, include only those
        # fields. `ts` ia an
        # acceptable field and can be used to just return time series.
        # see tsdb_server to see how this return
        # value is used
        # additional is a dictionary. It has two possible keys:
        # (a){'sort_by':'-order'} or {'sort_by':'+order'} where order
        # must be in the schema AND have an index. (b) limit: 'limit':10
        # which will give you the top 10 in the current sort order.
        # your code here

        # Filtering of pks
        pks=set(self.rows.keys())
        for field, value in meta.items():
            # Checking field in schema
            if field in self.schema:
                # Conversion to apply
                conversion=self.schema[field]['convert']
                # Case operators
                if isinstance(value, dict):
                    for op in value:
                        operation=OPMAP[op]
                        val=conversion(value[op])
                        # linear scan
                        filtered_pks=set()
                        for i in self.indexes[field].keys():
                            if operation(i, val):
                                filtered_pks.update(self.indexes[field][i])
                        # Update current selection AND'ing on pks filterred
                        pks=pks.intersection(filtered_pks)
                # Case list
                elif isinstance(value, list):
                    converted_values=[conversion(v) for v in value]
                    # index present
                    if field in self.indexes:
                        selected=set([self.indexes[field][v]
                                     for v in converted_values])
                    # No index
                    else:
                        selected=set([pk for pk in pks if field in self.rows[
                                     pk] and self.rows[pk][field] in converted_values])
                    pks=pks.intersection(selected)
                # Case precise
                else:
                    # index present
                    if field in self.indexes:
                        selected=set(self.indexes[field][conversion(value)])
                    # No index
                    else:
                        selected=set([pk for pk in pks if field in self.rows[
                                     pk] and self.rows[pk][field] == conversion(value)])
                    pks=pks.intersection(selected)

        # Convert set to list
        pks=list(pks)

        # Sorting pks
        if additional is not None and 'sort_by' in additional:
            predicate=additional['sort_by'][1:]
            # 0: ascending, 1: descending
            reverse=0 if additional['sort_by'][0] == '-' else 1
            # Sanity check
            if predicate not in self.schema or predicate not in self.indexes:
                raise ValueError(
                    'Additional field {} not in schema or in indexes'.format(predicate))
            # inplace sorting
            pks.sort(key=lambda pk: self.rows[pk][predicate], reverse=reverse)
            # Limit (we assume limit is possible only if sorting)
            if 'limit' in additional:
                pks=pks[:additional['limit']]

        # Retrieve fields
        if fields is None:
            print('S> D> NO FIELDS')
            matchedfielddicts=[{} for pk in pks]
        else:
            if not len(fields):
                print('S> D> ALL FIELDS')
                # Removing ts
                matchedfielddicts=[{k: v for k, v in self.rows[pk].items()
                                      if k != 'ts'} for pk in pks]
            else:
                matchedfielddicts=[{k: v for k, v in self.rows[pk].items()
                                      if k in fields} for pk in pks]
                print('S> D> FIELDS {} {}'.format(fields, pks))
        return pks, matchedfielddicts
