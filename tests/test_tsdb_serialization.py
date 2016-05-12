# import numpy as np
#
# from tsdb import *
# from timeseries import TimeSeries
#
# __author__ = "Mynti207"
# __copyright__ = "Mynti207"
# __license__ = "mit"
#
#
# def test_tsdb_serialization_positive():
#
#     # create deserializer
#     deserializer = Deserializer()
#
#     # data test for a single database entry
#     t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
#     v = np.array([10, 12, -11, 1.5, 10, 13, 17])
#     a = TimeSeries(t, v)
#     primary_key = 'v_1'
#     meta = {'order': 1, 'blarg': 2}
#
#     # list of test operations
#     test_ops = [TSDBOp_InsertTS(primary_key, a),
#                 TSDBOp_UpsertMeta(primary_key, meta),
#                 TSDBOp_Select(meta, None, None),
#                 TSDBOp_AugmentedSelect('corr', ['dist'], None, None, None),
#                 TSDBOp_Return(None, None),
#                 TSDBOp_AddTrigger('corr', 'insert_ts', None, None),
#                 TSDBOp_RemoveTrigger('corr', 'insert_ts', None)]
#
#     # test various database operations
#     for msg_op in test_ops:
#
#         # serialize message
#         msg = msg_op.to_json()
#         msg_serialized = serialize(msg)
#
#         # add to deserializer
#         deserializer.append(msg_serialized)
#
#         # test deserializer functionality
#         assert(deserializer.ready())
#         msg_recieved = deserializer.deserialize()
#         assert msg_recieved == msg
#
#
# def test_tsdb_serialization_negative():
#
#     # create deserializer
#     deserializer = Deserializer()
#
#     # try to deserialize garbage data
#     wrong_msg = b'\x00\x00\x00"\\\\"\x00"!!IM NOT A JSON object!!"'
#     deserializer.append(wrong_msg)
#
#     # test that no deserialized data is returned
#     assert deserializer.deserialize() is None
