import os
import pickle
from timeseries import TimeSeries
import sys.byteorder


#use python struct library to format and save binary data
#https://docs.python.org/3.4/library/struct.html#struct-format-strings

STRUCT_TYPES = {
  'int':'i',
  'float':'f',
  'bool': "?"
}

DEFAULT_VALUES = {
  'int': 0,
  'float': 0.0,
  'bool': False
}

INT_BYTES = 8

class Heap:

  def __init__(self, file_name):
    self.heap_file = file_name
    if not os.path.exists(file_name): 
      self.fd = open(file_name, "xb+", buffering=0)
    else:
      self.fd = open(file_name, "r+b", buffering=0)

    self.readptr = self.fd.tell()
    self.fd.seek(0,2)
    self.writeptr = self.fd.tell()

  def _write(self, byte_array):
    self.fd.seek(self.writeptr)
    ts_offset = self.fd.tell()
    self.fd.write(byte_array)
    self.fd.seek(0,2)
    self.writeptr = self.fd.tell()

  def _read(self, offset):
    self.fd.seek(offset)
    buf = self.fd.read(self.len_byte_array)
    return buf

  def __del__(self):
    self.fd.close()

  def close(self):
    self.__del__

class TSHeap(Heap):

  def __init__(self, file_name, ts_length):
    super().__init__(file_name)

    self.fmt = "<%sd"%(2*self.ts_length)

    self.meta_file = self.heap_file+".met"
    if os.path.exists(self.meta_file): 
      with open(self.meta_file, "rb", buffering=0) as fd:
        self.ts_length, self.byte_array_length = pickle.load(fd)
    else: 
      self.ts_length = ts_length
      self.byte_array_length = 2 * self.ts_length * INT_BYTES
      with open(self.meta_file, "xb", buffering=0) as fd:
        meta_data = (self.ts_length,self.byte_array_length)
        pickle.dump(meta_data, fd)

  def write_ts(self, ts):
    byte_array = struct.pack(self.fmt, *ts.times, *ts.values)
    self._write(byte_array)

  def read_ts(self, offset):
    buf = self._read(offset)
    items = struct.unpack(self.fmt, buf)
    times = items[self.ts_length:]
    values = items[:self.ts_length]
    return TimeSeries(times, values)

class SchemaHeap(Heap):

  def __init__(self, file_name, schema):
    super().__init__(file_name)
    self.schema
    self.meta_file = file_name+".met"
    if os.path.exists(self.meta_file):
      with open(self.meta_file, "rb", buffering=0) as fd:
        self.fmt, self.fields, self.byte_array_length = pickle.load(fd)
    else:
      self.fmt = self._create_formating()
      with open(self.meta_file, "xb", buffering=0) as fd:
        meta_data = (self.fmt, self.fields, self.byte_array_length)
        pickle.dump(meta_data, fd)

  def _create_formating(self):
    fields = sorted(list(schema.keys()))
    fields.remove("ts") # ts is stored in heap file
    fields.remove("pk") # pk is stored in index file
    self.fields = {}
    self.fmt = ""
    for field in fields:
      field_type = self.schema[field]["type"]
      field_val = DEFAULT_VALUES[field_type]
      self.fmt += STRUCT_TYPES[field_type]
      self.fields[field] = field_val
    self.byte_array_length = len(struct.pack(self.fmt, *self.fields.values()))

  def write_ts(self, meta_data):
    byte_array = struct.pack(self.fmt, *meta_data)
    self._write(byte_array)

  def read_ts(self, offset):
    buf = self._read(offset)
    items = struct.unpack(self.fmt, buf)
    times = items[self.ts_length:]
    values = items[:self.ts_length]
    return TimeSeries(times, values)