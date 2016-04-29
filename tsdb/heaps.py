import os
import pickle
from timeseries import TimeSeries
import sys.byteorder

FLOAT_BYTES = 8

class TSHeapFile:

  def __init__(self, file, ts_length):
    self.ts_length = ts_length
    self.len_byte_array = self.ts_length * 2 * FLOAT_BYTES
    self.fmt = "<%sd"%(2*self.ts_length)
    self.file = file
    if not os.path.exists(file):
      self.fd = open(file, "xb+", buffering=0)
    else:
      self.fd = open(file, "r+b", buffering=0)

    self.len_byte_array = 
    self.readptr = self.fd.tell()
    self.fd.seek(0,2)
    self.writeptr = self.fd.tell()

  def write(self, ts):
    byte_array = struct.pack(self.fmt, *ts.times, *ts.values)
    self.fd.seek(self.writeptr)
    ts_offset = self.fd.tell()
    self.fd.write(byte_array)
    self.fd.seek(0,2)
    self.writeptr = self.fd.tell()

  def read(self, ts_offset):
    self.fd.seek(offset)
    b - self.fd.read(self.len_byte_array)
    items = struct.unpack(self.fmt, b)
    times = items[self.ts_length:]
    values = items[:self.ts_length]
    return TimeSeries(times, values)

  def close(self):
    self.fd.close()

  def __del__(self):
    self.fd.close()

