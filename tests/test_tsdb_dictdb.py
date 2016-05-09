from collections import defaultdict
import numpy as np
import pytest

from tsdb import DictDB
from timeseries import TimeSeries

__author__ = "Mynti207"
__copyright__ = "Mynti207"


def test_tsdb_dictdb():

    # synthetic data
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    v2 = np.array([8, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    a2 = TimeSeries(t, v2)

    identity = lambda x: x

    schema = {
      'pk':         {'convert': identity,   'index': None},
      'ts':         {'convert': identity,   'index': None},
      'order':      {'convert': int,        'index': 1},
      'blarg':      {'convert': int,        'index': 1},
      'useless':    {'convert': identity,   'index': None},
      'mean':       {'convert': float,      'index': 1},
      'std':        {'convert': float,      'index': 1},
      'vp':         {'convert': bool,       'index': 1},
      'deleted':    {'convert': bool,       'index': 1}
    }

    # create dictionary
    ddb = DictDB(schema, 'pk')

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

    # check that it isn't present any more
    pk, selected = ddb.select({'pk': 'pk1'}, [], None)
    assert pk == []
    assert len(selected) == 0

    # add the time series back in
    ddb.insert_ts('pk1', a1)
    ddb.upsert_meta('pk1', {'order': 1, 'blarg': 2})

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
    db_rows = ddb.rows
    idx = sorted(db_rows.keys())  # sorted primary keys

    # check primary keys
    assert idx == ['0DELETED_pk1', 'pk1', 'pk2']

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
    check_indexes = ['blarg', 'deleted', 'mean', 'order', 'std', 'vp']
    assert sorted(ddb.indexes.keys()) == check_indexes
    for v in ddb.indexes.values():
        assert isinstance(v, defaultdict)
