#!/usr/bin/env python3
from collections import defaultdict
import numpy as np
import os
import pytest

from tsdb import PersistentDB
from timeseries import TimeSeries

__author__ = "Mynti207"
__copyright__ = "Mynti207"


def test_tsdb_persistentdb():

    # synthetic data
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    v2 = np.array([8, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    a2 = TimeSeries(t, v2)

    identity = lambda x: x

    # index: 1 is binary tree index, 2 is bitmap index
    schema = {
      'pk': {'type': 'str', 'convert': identity, 'index': None, 'values': None},
      'ts': {'type': 'int', 'convert': identity, 'index': None, 'values': None},
      'order': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
      'blarg': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
      'useless': {'type': 'int', 'convert': identity, 'index': 1, 'values': None},
      'mean': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
      'std': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
      'vp': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]},
      'deleted': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]}
    }

    # Delete any default db present (otherwise the db creation will load
    # the previous one...)
    filelist = ["db_files/" + f for f in os.listdir("db_files/")
                if f[:7] == 'default']
    for f in filelist:
        os.remove(f)

    # create persistent db
    ddb = PersistentDB(schema, 'pk', len(t))

    # CHECK INSERTION/UPSERTION/DELETION -->

    # insert two new time series and metadata
    ddb.insert_ts('pk1', a1)
    ddb.upsert_meta('pk1', {'order': 1, 'blarg': 2})
    ddb.insert_ts('pk2', a2)
    ddb.upsert_meta('pk2', {'order': 2, 'blarg': 2})

    # try to insert a duplicate primary key
    with pytest.raises(ValueError):
        ddb.insert_ts('pk2', a2)

    # delete a valid time series
    ddb.delete_ts('pk1')
    # Check the deletion in the primary index
    assert ('pk1' not in ddb.pks.keys())
    # Check the deletion in other index
    for field, index in ddb.indexes.items():
        if field == 'deleted':
            continue
        for value in index.values():
            assert ('pk1' not in value)

    # # check that it isn't present any more
    pk, selected = ddb.select({'pk': 'pk1'}, [], None)
    assert pk == []
    assert len(selected) == 0
    pk, selected = ddb.select({'pk': 'pk2'}, [], None)
    assert pk == ['pk2']
    assert len(selected) == 1

    # add the time series back in
    ddb.insert_ts('pk1', a1)

    # Test consecutives meta upsert
    ddb.upsert_meta('pk1', {'order': 2, 'blarg': 3})
    for k, v in ddb.indexes['order'].items():
        if k == 2:
            assert ('pk1' in v)
        else:
            assert ('pk1' not in v)
    ddb.upsert_meta('pk1', {'order': 1, 'blarg': 2})
    for k, v in ddb.indexes['blarg'].items():
        if k == 2:
            assert ('pk1' in v)
        else:
            assert ('pk1' not in v)

    # check that it's present now
    pk, selected = ddb.select({'pk': 'pk1'}, [], None)
    assert pk == ['pk1']
    assert len(selected) == 1

    # delete an invalid time series
    with pytest.raises(ValueError):
        ddb.delete_ts('not_here')

    # try to insert metadata for a time series that isn't present
    with pytest.raises(ValueError):
        ddb.upsert_meta('pk3', {'order': 2, 'blarg': 2})

    # extract database entries for testing
    db_rows = {pk: ddb._get_meta(pk) for pk in ddb.pks.keys()}
    idx = sorted(db_rows.keys())  # sorted primary keys

    # check primary keys
    assert idx == ['pk1', 'pk2']

    # check metadata
    assert db_rows['pk1']['order'] == 1
    assert db_rows['pk2']['order'] == 2
    assert db_rows['pk1']['blarg'] == 2
    assert db_rows['pk2']['blarg'] == 2

    # CHECK SELECT OPERATIONS -->

    pk, selected = ddb.select({}, None, None)
    assert sorted(pk) == ['pk1', 'pk2']
    assert selected == [{}, {}]

    pk, selected = ddb.select({}, None, {'sort_by': '-order', 'limit': 5})
    assert sorted(pk) == ['pk1', 'pk2']
    assert selected == [{}, {}]

    pk, selected = ddb.select({}, None, {'sort_by': '+pk'})
    assert pk == ['pk1', 'pk2']
    assert selected == [{}, {}]

    pk, selected = ddb.select({'order': 1, 'blarg': 2}, [], None)
    assert pk == ['pk1']
    assert len(selected) == 1
    assert selected[0]['pk'] == 'pk1'
    assert selected[0]['order'] == 1
    assert selected[0]['blarg'] == 2

    pk, selected = ddb.select({'order': [1, 2], 'blarg': 2}, [], None)
    assert sorted(pk) == ['pk1', 'pk2']
    assert len(selected) == 2
    idx = pk.index('pk1')
    assert selected[idx]['pk'] == 'pk1'
    assert selected[idx]['order'] == 1
    assert selected[idx]['blarg'] == 2

    pk, selected = ddb.select({'order': {'>=': 4}}, ['order'], None)
    assert len(pk) == 0
    assert len(selected) == 0

    # field not in schema
    with pytest.raises(ValueError):
        ddb.select({}, None, {'sort_by': '-unknown', 'limit': 5})

    # bulk update of indices
    ddb.index_bulk()
    check = ['blarg', 'deleted', 'mean', 'order', 'std', 'useless', 'vp']
    assert sorted(ddb.indexes.keys()) == check

    # CHECK BITMAP INDICES -->

    # insert more data
    ddb.insert_ts('pk3', a1)
    ddb.insert_ts('pk4', a1)
    ddb.insert_ts('pk5', a1)
    ddb.insert_ts('pk6', a1)

    # boolean fields should be initialized as negative
    assert len(ddb.indexes['deleted'][True]) == 0
    assert len(ddb.indexes['vp'][True]) == 0

    # swap one
    ddb.upsert_meta('pk4', {'vp': True})
    assert len(ddb.indexes['vp'][True]) == 1
    assert len(ddb.indexes['vp'][False]) == 5
    assert list(ddb.indexes['vp'][True])[0] == 'pk4'

    # swap back
    ddb.upsert_meta('pk4', {'vp': False})
    assert len(ddb.indexes['vp'][True]) == 0
    assert len(ddb.indexes['vp'][False]) == 6

    # delete entry
    ddb.delete_ts('pk6')
    for (k, v) in ddb.indexes['vp'].items():
        assert 'pk6' not in v
