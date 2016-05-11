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
LENGTH_OFFSET = 4


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
        offset = self.fd.tell()
        self.fd.write(byte_array)
        # Go to end of file
        self.fd.seek(0, 2)
        # Update the write pointer
        self.writeptr = self.fd.tell()

        return offset

    def _read(self, offset):
        self.fd.seek(offset)
        buf = self.fd.read(self.len_byte_array)
        return buf

    def __del__(self):
        self.fd.close()

    def close(self):
        self.__del__


class TSHeap(Heap):
    '''
    Heap file used to store raw timeseries
    '''

    def __init__(self, file_name, ts_length):
        super().__init__(file_name)

        # Check if fd is empty (case when new TSHeap)
        # Need to write the length of the ts
        if self.writeptr == 0:
            self.ts_length = ts_length
            ts_len_bytes = ts_length.to_bytes(LENGTH_OFFSET,
                                              byteorder="little")
            self._write(ts_len_bytes)
        else:
            # Read ts length 
            self.fd.seek(0)
            self.ts_length = int.from_bytes(self.fd.read(LENGTH_OFFSET),
                                            byteorder="little")

        # Define length of byte array (lists of times and list of values)
        self.len_byte_array = 2 * self.ts_length * INT_BYTES
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
    '''
    Heap file used to store metadata
    '''

    def __init__(self, file_name, schema):
        '''
        Initialize if needed the format string key to pack/unpack
        or load it from file.
        '''
        super().__init__(file_name)
        self.schema = schema
        self.meta_file = file_name+".met"
        # Build: fields, default_values, len_byte_array, fmt
        self._build_format_string()

    def _build_format_string(self):
        '''
        Build the format string to pack fields into bytes
        '''
        fields = sorted(list(self.schema.keys()))
        # we add the 'pk' in the meta after reading them
        fields.remove("pk")
        # ts is stored in TS heap
        fields.remove("ts")
        # To keep track of the order we use list
        self.fields = []
        self.default_values = []
        # Initialize the format string
        self.fmt = ""
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
