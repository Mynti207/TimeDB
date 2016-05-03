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


def metafiltered(d, schema, fieldswanted):
    d2 = {}
    if len(fieldswanted) == 0:
        keys = [k for k in d.keys() if k != 'ts']
    else:
        keys = [k for k in d.keys() if k in fieldswanted]
    for k in keys:
        if k in schema:
            d2[k] = schema[k]['convert'](d[k])
    return d2


class DictDB:

    '''
    Database implementation in a dict
    '''

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

        # insert if not already present
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}

        # insert meta
        row = self.rows[pk]
        for field in meta:
            if self.schema[field]['index'] is not None:
                row[field] = meta[field]

        self.update_indices(pk)

    def index_bulk(self, pks=[]):
        if len(pks) == 0:
            pks = self.rows
        for pkid in pks:
            self.update_indices(pkid)

    def update_indices(self, pk):
        row = self.rows[pk]
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].add(pk)

    def select(self, meta, fields, additional):

        # filtering of pks
        pks = set(self.rows.keys())
        for field, value in meta.items():
            # checking field in schema
            if field in self.schema:
                # conversion to apply
                conversion = self.schema[field]['convert']
                # case operators
                if isinstance(value, dict):
                    for op in value:
                        operation = OPMAP[op]
                        val = conversion(value[op])
                        # linear scan
                        filtered_pks = set()
                        for i in self.indexes[field].keys():
                            if operation(i, val):
                                filtered_pks.update(self.indexes[field][i])
                        # update current selection AND'ing on pks filtered
                        pks = pks.intersection(filtered_pks)
                # case list
                elif isinstance(value, list):
                    converted_values = [conversion(v) for v in value]
                    # index present
                    if field in self.indexes:
                        selected = set()
                        for v in converted_values:
                            selected.update(self.indexes[field][v])
                    # no index
                    else:
                        selected = set([pk for pk in pks
                                        if field in self.rows[pk] and
                                        self.rows[pk][field]
                                        in converted_values])
                    pks = pks.intersection(selected)
                # case precise
                else:
                    # index present
                    if field in self.indexes:
                        selected = set(self.indexes[field][conversion(value)])
                    # no index
                    else:
                        selected = set([pk for pk in pks
                                        if field in self.rows[pk] and
                                        self.rows[pk][field] == conversion(value)])
                    pks = pks.intersection(selected)

        # convert set to list
        pks = list(pks)

        # sorting pks
        if additional is not None and 'sort_by' in additional:
            predicate = additional['sort_by'][1:]
            # 0: ascending, 1: descending
            reverse = 0 if additional['sort_by'][0] == '-' else 1
            # sanity check
            if predicate not in self.schema or predicate not in self.indexes:
                raise ValueError('Additional field {} not in schema or in indexes'.format(predicate))
            # inplace sorting
            pks.sort(key=lambda pk: self.rows[pk][predicate], reverse=reverse)
            # limit (we assume limit is possible only if sorting)
            if 'limit' in additional:
                pks = pks[:additional['limit']]

        # retrieve fields
        if fields is None:
            print('S> D> NO FIELDS')
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                print('S> D> ALL FIELDS')
                # removing ts
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k != 'ts'} for pk in pks]
            else:
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k in fields} for pk in pks]
                print('S> D> FIELDS {} {}'.format(fields, pks))

        return pks, matchedfielddicts
