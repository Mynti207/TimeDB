import numpy as np
import pytest
import json

from tsdb import *
from timeseries import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_tsdb_serialization():
    # Creating deserializer
    deserializer = Deserializer()

    # Json data test
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    primary_key = 'v_1'
    meta = {'order': 1, 'blarg': 2}

    for msg_op in [TSDBOp_InsertTS(primary_key, a),
                   TSDBOp_UpsertMeta(primary_key, meta),
                   TSDBOp_Select(meta, None, None),
                   TSDBOp_AugmentedSelect('corr', ['dist'], None, None, None),
                   TSDBOp_Return(None, None),
                   TSDBOp_AddTrigger('corr', 'insert_ts', None, None),
                   TSDBOp_RemoveTrigger('corr', 'insert_ts')]:
        # Message serialized
        msg = msg_op.to_json()
        msg_serialized = serialize(msg)

        # Filling the deserializer
        deserializer.append(msg_serialized)

        # Positive test
        assert(deserializer.ready())
        msg_recieved = deserializer.deserialize()
        assert msg_recieved == msg

    # Negative test
    deserializer = Deserializer()
    wrong_msg = b'\x00\x00\x00"\\\\"\x00"!!IM NOT A JSON object!!"'
    deserializer.append(wrong_msg)
    assert deserializer.deserialize() == None


def test_tsdb_dictdb():
    # Synthetic data
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    v2 = np.array([8, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    a2 = TimeSeries(t, v2)

    # Dict db
    identity = lambda x: x

    schema = {
      'pk': {'convert': identity, 'index': None},  #will be indexed anyways
      'ts': {'convert': identity, 'index': None},
      'order': {'convert': int, 'index': 1},
      'blarg': {'convert': int, 'index': 1},
      'useless': {'convert': identity, 'index': None},
      'mean': {'convert': float, 'index': 1},
      'std': {'convert': float, 'index': 1},
      'vp': {'convert': bool, 'index': 1}
    }

    ddb = DictDB(schema, 'pk')

    ddb.insert_ts('pk1', a1)
    ddb.insert_ts('pk2', a2)
    ddb.upsert_meta('pk1', {'order': 1, 'blarg': 2})
    ddb.upsert_meta('pk2', {'order': 2, 'blarg': 2})

    # Select operations
    ddb.select({}, None, None)
    ddb.select({}, None, {'sort_by': '-order', 'limit': 5})
    ddb.select({'order': 1, 'blarg': 2}, [], None)
    ddb.select({'order': [1, 2], 'blarg': 2}, [], None)
    ddb.select({'order': {'>=': 4}}, ['order'], None)
    with pytest.raises(ValueError):
        ddb.select({}, None, {'sort_by': '-unknown', 'limit': 5})

