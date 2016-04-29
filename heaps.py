import pickle
from timeseries import Timeseries
import os.path

FLOAT_SIZE = 8

class HeapFile:
    def __init__(self, file_dir, heap_type):
        self.file_name = file_dir +"/"+heap_type
        
        if not os.path.exists(file_name):
            self.fd = open(file_name, "xb+", buffering=0)
        else:
            self.fd = open(fiel_name, "r+b", buffering=0)
            
        self.readptr = self.fd.tell()
        self.fd.seek(0,2)
        self.writeptr = self.fd.tell()
        
    def close(self):
        self.fd.close()
        
class TSHeapFile(HeapFile):
    def __init__(self, file_dir, ts_length):
        super().__init__(file_dir, "ts_heap")
        self.ts_length = ts_length
        self.struct_format = '<%sd'%(2*self.ts_length)
        self.len_byte_array = ts_length * 2 * FLOAT_SIZE
        
    def write_to_file(self, ts):
        byte_array = struct.pack(self.struct_format, *ts.times, *ts.values)
        
        self.fd.seek(self.writeptr)
        ts_offset = self.fd.tell()
        self.fd.write(byteArray)
        self.fd.seek(0,2)
        self.writeptr = self.fd.tell()
        
        return ts_offset
    
    def read_from_file(self, ts_offset):
        self.fd.seek(ts_offset)
        buffer = self.fd.read(self.len_byte_array)
        items = struct.unpack(self.struct_format, buffer)
        times = items[:self.ts_length]
        values = items[self.ts_length:]
        return TimeSeries(times,values)
        
class MetaHeapFile(HeapFile):
    def __init__(self, file_dir, schema):
        super().__init__(file_name, "meta_heap")
        self.schema = schema