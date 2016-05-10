import os
import pickle
import sys
import struct

from timeseries import TimeSeries


# use python struct library to format and save binary data
# https://docs.python.org/3.4/library/struct.html#struct-format-strings

STRUCT_TYPES = {
    'int': 'i',
    'float': 'f',
    'bool': '?'
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
        # Create it if new else load it
        if not os.path.exists(file_name):
            self.fd = open(file_name, "xb+", buffering=0)
        else:
            self.fd = open(file_name, "r+b", buffering=0)

        self.readptr = self.fd.tell()
        self.fd.seek(0, 2)
        self.writeptr = self.fd.tell()

    def _write(self, byte_array):
        '''
        Write the byte_array to disk and return its offset.
        '''
        self.fd.seek(self.writeptr)
        ts_offset = self.fd.tell()
        self.fd.write(byte_array)
        # Go to end of file
        self.fd.seek(0, 2)
        # Update the write pointer
        self.writeptr = self.fd.tell()

        return ts_offset

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

        self.meta_file = self.heap_file+".met"
        if os.path.exists(self.meta_file):
            with open(self.meta_file, "rb", buffering=0) as fd:
                self.ts_length, self.len_byte_array = pickle.load(fd)
        else:
            self.ts_length = ts_length
            self.len_byte_array = 2 * self.ts_length * INT_BYTES
            with open(self.meta_file, "xb", buffering=0) as fd:
                meta_data = (self.ts_length, self.len_byte_array)
                pickle.dump(meta_data, fd)
        # Define format string
        self.fmt = "<%sd" % (2*self.ts_length)

    def write_ts(self, ts):
        '''
        Write ts to the heap on disk, return its offset.
        '''
        byte_array = struct.pack(self.fmt, *ts.timesseq, *ts.valuesseq)
        return self._write(byte_array)

    def read_ts(self, offset):
        buf = self._read(offset)
        items = struct.unpack(self.fmt, buf)
        times = items[self.ts_length:]
        values = items[:self.ts_length]
        return TimeSeries(times, values)


class MetaHeap(Heap):

    def __init__(self, file_name, schema):
        '''
        Initialize if needed the format string key to pack/unpack
        or load it from file.
        '''
        super().__init__(file_name)
        self.schema = schema
        self.meta_file = file_name+".met"
        if os.path.exists(self.meta_file):
            with open(self.meta_file, "rb", buffering=0) as fd:
                self.fmt, self.fields, self.default_values, self.len_byte_array = pickle.load(fd)
        else:
            self._build_format_string()
            with open(self.meta_file, "xb", buffering=0) as fd:
                meta_data = (self.fmt, self.fields, self.default_values,
                             self.len_byte_array)
                pickle.dump(meta_data, fd)

    def _build_format_string(self):
        '''
        Build the format string to pack fields into bytes
        '''
        fields = sorted(list(self.schema.keys()))
        fields.remove("pk")  # pk is stored in primary index dictionary
        fields.remove("ts")  # ts is stored in TS heap, instead we store ts_offset
        # To keep track of the order we use list
        # Initialization with the field to store the offset of ts
        self.fields = ["ts_offset"]
        self.default_values = [0]
        # Initialize the format string
        self.fmt = "i"
        for field in fields:
            field_type = self.schema[field]["type"]
            # Update lists
            self.default_values.append(DEFAULT_VALUES[field_type])
            self.fields.append(field)
            # Update format string
            self.fmt += STRUCT_TYPES[field_type]
        self.len_byte_array = len(
            struct.pack(self.fmt, *self.default_values))

    def write_meta(self, meta_dict, offset=None):
        '''
        Helper to update meta on disk.

        Parameters
        ----------
        meta_dict: meta data dictionary
        offset : offset of the metadata in heapfile,
            if None create new metadata

        Returns
        -------
        offset
        '''
        # Initialize the meta data if new insertion
        if offset is None:
            values = self.default_values
        else:
            self.writeptr = offset
            values = list(self._read_meta(offset))

        # Update the list of values for the fields in both meta heap and meta_dict
        for i, field in enumerate(self.fields):
            if field in meta_dict.keys():
                values[i] = meta_dict[field]

        # Write the metadata
        byte_array = struct.pack(self.fmt, *values)

        return self._write(byte_array)

    def _read_meta(self, offset):
        '''
        Read the metadata in the heap at offset.
        Return the list of values to keep the order.
        '''
        buf = self._read(offset)
        # Read the list of meta values
        return struct.unpack(self.fmt, buf)

    def read_meta(self, offset):
        '''
        Read the metadata in the heap at offset.
        Return the metadata as a dictionary.
        '''
        # Get the values
        values = self._read_meta(offset)

        return {k: v for k, v in zip(self.fields, values)}
