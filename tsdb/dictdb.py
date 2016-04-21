from collections import defaultdict
from operator import and_
from functools import reduce
import operator

# this dictionary will help you in writing a generic select operation
OPMAP = {
    '<': operator.lt,
    '>': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}


class DictDB:
    "Database implementation in a dict"
    def __init__(self, schema, pkfield):
        self.indexes = {}
        self.rows = {}
        self.schema = schema
        self.pkfield = pkfield
        for s in schema:
            indexinfo = schema[s]['index']
            # convert = schema[s]['convert']
            # later use binary search trees for highcard/numeric
            # bitmaps for lowcard/str_or_factor
            if indexinfo is not None:
                self.indexes[s] = defaultdict(set)

    def insert_ts(self, pk, ts):
        "given a pk and a timeseries, insert them"
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}
        else:
            raise ValueError('Duplicate primary key found during insert')
        self.rows[pk]['ts'] = ts
        self.update_indices(pk)

    def upsert_meta(self, pk, meta):
        "implement upserting field values, as long as the fields are in the schema."
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

    def select(self, meta, fields):
        # your code here
        # if fields is None: return only pks
        # like so [pk1,pk2],[{},{}]
        # if fields is [], this means all fields
        #except for the 'ts' field. Looks like
        #['pk1',...],[{'f1':v1, 'f2':v2},...]
        # if the names of fields are given in the list, include only those fields. `ts` ia an
        #acceptable field and can be used to just return time series.
        #see tsdb_server to see how this return
        #value is used

        # Filtering of pks
        pks = set(self.rows.keys())
        for field in meta:
            # Case operators
            if isinstance(meta[field], dict):
                for op in meta[field]:
                    operation = OPMAP[op]
                    value = meta[field][op]
                    # linear scan
                    filtered_pks = set()
                    for i in self.indexes[field].keys():
                        if operation(i, value):
                            filtered_pks.update(self.indexes[field][i])
                    # Update current selection AND'ing on pks filterred
                    pks = pks.intersection(filtered_pks)
            # Case precise value
            elif meta[field] in self.indexes[field]:
                pks = pks.intersection(set(self.indexes[field][meta[field]]))
        # Convert set to list
        pks = list(pks)

        # Retrieve fields
        if fields is None:
            print('S> D> NO FIELDS')
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                print('S> D> ALL FIELDS')
                # Removing ts
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items() if k != 'ts'} for pk in pks]
            else:
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items() if k in fields} for pk in pks]
                print('S> D> FIELDS {} {}'.format(fields, pks))

        return pks, matchedfielddicts
