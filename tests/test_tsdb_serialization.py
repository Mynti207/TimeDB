import numpy as np
import pytest
import json

from tsdb import *
from timeseries import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_tsdb_serialization():
    # creating deserializer
    deserializer = Deserializer()

    # json data test
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
    assert deserializer.deserialize() is None
