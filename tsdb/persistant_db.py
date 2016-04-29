from collections import defaultdict
from operator import and_
from functools import reduce
from .indexes import BMaskIndex, BTreeIndex
from .heaps import TSHeapFile
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

# define the the threshold for high cardinality
CARD_THRESHOLD = 4

class DictDB:

    '''
    Database implementation in a dict
    '''

    def __init__(self, schema, pkfield, db_name="default", data_dir="db_files", ts_length=1024):
        self.db_name = db_name
        self.data_dir = data_dir+"/"+db_name
        self.ts_length = ts_length
        self.indexes = {}
        self.schema = schema
        self.pkfield = pkfield
        
        # set up indexes
        #self.pks = PKIndex(self.dbname)
        for field, value in self.schema.items():
            if (len(value['values'] <= CARD_THRESHOLD):
                self.indexes[field] = BMaskIndex(field)
            else:
                self.indexes[field] = BTreeIndex(field)
        
        # set up directory for db data 
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # open files for reading meta data and ts data
        #self.meta_heap = MeatHeapFile(self.data_dir, schema)
        self.ts_heap = TSHeapFile(self.data_dir, self.ts_length)

    def insert_ts(self, pk, ts):
        '''
        Given a pk and a timeseries, insert them
        '''
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}
        else:
            raise ValueError('Duplicate primary key found during insert')
        self.rows[pk]['ts'] = ts
        self.update_indices(pk)

    def upsert_meta(self, pk, meta):
        '''
        Implement upserting field values, as long as fields are in the schema
        '''
        # your code here
        # Insert if not already present
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}
        # Insert meta
        row = self.rows[pk]
        for field in meta:
            if self.schema[field]['index'] is not None:
                row[field] = meta[field]

        self.update_indices(pk)

    def index_bulk(self, pks=[]):
        if len(pks) == 0:
            pks = self.rows
        for pkid in self.pks:
            self.update_indices(pkid)

    def update_indices(self, pk):
        row = self.rows[pk]
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].add(pk)

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
        pks = set(self.rows.keys())
        for field, value in meta.items():
            # Checking field in schema
            if field in self.schema:
                # Conversion to apply
                conversion = self.schema[field]['convert']
                # Case operators
                if isinstance(value, dict):
                    for op in value:
                        operation = OPMAP[op]
                        val = conversion(value[op])
                        # linear scan
                        filtered_pks = set()
                        for i in self.indexes[field].keys():
                            if operation(i, val):
                                filtered_pks.update(self.indexes[field][i])
                        # Update current selection AND'ing on pks filterred
                        pks = pks.intersection(filtered_pks)
                # Case list
                elif isinstance(value, list):
                    converted_values = [conversion(v) for v in value]
                    # index present
                    if field in self.indexes:
                        selected = set([self.indexes[field][v] for v in converted_values])
                    # No index
                    else:
                        selected = set([pk for pk in pks if field in self.rows[pk] and self.rows[pk][field] in converted_values])
                    pks = pks.intersection(selected)
                # Case precise
                else:
                    # index present
                    if field in self.indexes:
                        selected = set(self.indexes[field][conversion(value)])
                    # No index
                    else:
                        selected = set([pk for pk in pks if field in self.rows[pk] and self.rows[pk][field] == conversion(value)])
                    pks = pks.intersection(selected)

        # Convert set to list
        pks = list(pks)

        # Sorting pks
        if additional is not None and 'sort_by' in additional:
            predicate = additional['sort_by'][1:]
            # 0: ascending, 1: descending
            reverse = 0 if additional['sort_by'][0] == '-' else 1
            # Sanity check
            if predicate not in self.schema or predicate not in self.indexes:
                raise ValueError('Additional field {} not in schema or in indexes'.format(predicate))
            # inplace sorting
            pks.sort(key=lambda pk: self.rows[pk][predicate], reverse=reverse)
            # Limit (we assume limit is possible only if sorting)
            if 'limit' in additional:
                pks = pks[:additional['limit']]

        # Retrieve fields
        if fields is None:
            print('S> D> NO FIELDS')
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                print('S> D> ALL FIELDS')
                # Removing ts
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k != 'ts'} for pk in pks]
            else:
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k in fields} for pk in pks]
                print('S> D> FIELDS {} {}'.format(fields, pks))
        return pks, matchedfielddicts
