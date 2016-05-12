import os
import pickle
import sys
import struct
import copy

from timeseries import TimeSeries

identity = lambda x: x

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
    '''
    Basic heap structure (binary data). Used to define common heap functions.
    '''

    def __init__(self, data_dir, file_name):
        '''
        Initializes the Heap class.

        Parameters
        ----------
        data_dir : str
            Heap file location
        file_name : str
            Heap file name
        file_name : str
            Heap file location

        Returns
        -------
        An initialized Heap object
        '''

        self.data_dir = data_dir
        self.heap_file = data_dir + file_name + '.met'

        # create it if new, else load it
        if not os.path.exists(self.heap_file):
            self.fd = open(self.heap_file, "xb+", buffering=0)
        else:
            self.fd = open(self.heap_file, "r+b", buffering=0)

        self.readptr = self.fd.tell()
        self.fd.seek(0, 2)
        self.writeptr = self.fd.tell()

    def _write(self, byte_array):
        '''
        Write the byte_array to disk and return its offset.

        Parameters
        ----------
        byte_array: binary data
            (formatted with struct library)

        Returns
        -------
        offset: int
            offset of the byte_array in heap
        '''
        self.fd.seek(self.writeptr)
        offset = self.fd.tell()
        self.fd.write(byte_array)
        # go to end of file
        self.fd.seek(0, 2)
        # update the write pointer
        self.writeptr = self.fd.tell()

        return offset

    def _read(self, offset):
        '''
        Read byte_array from disk at the given offset.

        Parameters
        ----------
        offset: int
            offset of the byte_array on disk

        Returns
        -------
        buf: binary data
            (formatted with struct library)
        '''
        self.fd.seek(offset)
        buf = self.fd.read(self.len_byte_array)
        return buf

    def __del__(self):
        '''
        Close file.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing.
        '''
        self.fd.close()

    def close(self):
        '''
        Close file.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing.
        '''
        self.__del__

    def clear(self):
        '''
        Clear the file_name and reset the read/write pointer.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # create it if new else load it
        if not os.path.exists(self.heap_file):
            raise ValueError('File {} not found'.format(self.heap_file))
        else:
            # we clear the content of the file with arg: w+b
            self.fd = open(self.heap_file, "w+b", buffering=0)

        # read and write at the begining of the new empty file
        self.readptr = self.fd.tell()
        self.writeptr = self.fd.tell()


class TSHeap(Heap):
    '''
    Heap file used to store raw timeseries
    '''

    def __init__(self, data_dir, file_name, ts_length):
        '''
        Initializes the TSHeap class.

        Parameters
        ----------
        data_dir : str
            Heap file location
        file_name : str
            Heap file name
        ts_length : int
            Expected/permitted length of time series

        Returns
        -------
        An initialized TSHeap object
        '''
        super().__init__(data_dir, file_name)

        # check if fd is empty (case when new TSHeap)
        # need to write the length of the ts
        if self.writeptr == 0:
            self.ts_length = ts_length
            ts_len_bytes = ts_length.to_bytes(LENGTH_OFFSET,
                                              byteorder="little")
            self._write(ts_len_bytes)
        else:
            # read ts length
            self.fd.seek(0)
            self.ts_length = int.from_bytes(self.fd.read(LENGTH_OFFSET),
                                            byteorder="little")
            # check if ts_length matches
            if self.ts_length != ts_length:
                raise ValueError(
                    'Wrong length set in the db, should be {} instead of {}'.format(self.ts_length, ts_length))

        # define length of byte array (lists of times and list of values)
        self.len_byte_array = 2 * self.ts_length * INT_BYTES

        # define format string
        self.fmt = "<%sd" % (2*self.ts_length)

    def write_ts(self, ts):
        '''
        Write ts to the heap on disk, return its offset.

        Parameters
        ----------
        ts: TimeSeries
            Time series to be written to the heap

        Returns
        -------
        offset: int
            offset of the metadata in heapfile
        '''
        byte_array = struct.pack(self.fmt, *ts.timesseq, *ts.valuesseq)
        return self._write(byte_array)

    def read_ts(self, offset):
        '''
        Read ts from the heap on disk, at the given offset.

        Parameters
        ----------
        offset: int
            offset of the metadata in heapfile

        Returns
        -------
        ts: TimeSeries
            Time series retrieved from the heap
        '''
        buf = self._read(offset)
        items = struct.unpack(self.fmt, buf)
        times = items[:self.ts_length]
        values = items[self.ts_length:]
        return TimeSeries(times, values)


class MetaHeap(Heap):
    '''
    Heap file used to store metadata
    '''

    def __init__(self, data_dir, file_name, schema):
        '''
        Initializes the MetaHeap class.

        Parameters
        ----------
        data_dir : str
            Heap file location
        file_name : str
            Heap file name
        schema : dictionary
            Metadata fields and attributes

        Returns
        -------
        An initialized MetaHeap object
        '''
        super().__init__(data_dir, file_name)
        self.schema = schema
        # build: fields, default_values, len_byte_array, fmt
        self._build_format_string()

        # save schema for future loads
        self.save_schema()

    def reset_schema(self, schema, pks):
        '''
        Update the schema of the heap and rewrite each pks in the
        PrimaryIndex pks into it, with the previous meta updated with
        the new schema.

        Parameters
        ----------
        schema : dictionary
            New schema
        pks : PrimaryIndex
            Primary index of a db, used to rewrite the previous content
            of the heap in the new one.

        Returns
        -------
        Nothing, modifies in-place pks with the new offset
        '''
        # load previous meta for each pks
        # store them in metas: {pk: old_offset}
        metas = {}
        for pk, tup in pks.items():
            # read raw values
            metas[pk] = self.read_meta(tup[1])

        # reset schema
        self.schema = schema
        # update the parameters for binary file based on new schema
        self._build_format_string()
        # reset heap file
        self.clear()

        # populate the new heap file according to the new schema
        for pk, meta in metas.items():
            # the deleted fields previously present won't be written to the
            # heap as they are not in self.fields anymore
            pk_offset = self.write_meta(meta, offset=None)

            # inplace update of the offset tuple in pks index
            new_offsets = (pks[pk][0], pk_offset)
            pks[pk] = new_offsets

        # save updated schema to disk
        self.save_schema()

    def save_schema(self):
        '''
        Helper function: saves schema, while dealing with identity function
        '''

        save_schema = copy.deepcopy(self.schema)

        for field, specs in save_schema.items():
            if isinstance(specs['convert'], type(identity)):
                specs['convert'] = 'IDENTITY'

        with open(self.data_dir + '/schema.idx', "wb", buffering=0) as fd:
            pickle.dump(save_schema, fd)

    def _build_format_string(self):
        '''
        Build the format string to pack fields into bytes

        Parameters
        ----------
        Nothing

        Returns
        -------
        Nothing, modifies in place.
        '''
        fields = sorted(list(self.schema.keys()))
        # we add the 'pk' in the meta after reading them
        fields.remove("pk")
        # ts is stored in TS heap
        fields.remove("ts")
        # to keep track of the order we use list
        self.fields = []
        self.default_values = []
        # initialize the format string
        self.fmt = ""
        for field in fields:
            field_type = self.schema[field]["type"]
            # update lists
            self.default_values.append(DEFAULT_VALUES[field_type])
            self.fields.append(field)
            # update format string
            self.fmt += STRUCT_TYPES[field_type]
        self.len_byte_array = len(
            struct.pack(self.fmt, *self.default_values))

    def write_meta(self, meta_dict, offset=None):
        '''
        Helper to update meta on disk.

        Parameters
        ----------
        meta_dict: dict
            metadata dictionary
        offset : int
            offset of the metadata in heapfile

        Returns
        -------
        offset: int
            offset of the metadata in heapfile if given
            (same as arg is given, else new one)
        '''
        # initialize the meta data if new insertion
        if offset is None:
            values = self.default_values
        else:
            self.writeptr = offset
            values = list(self._read_meta(offset))

        # update list of values for the fields in both metaheap and meta_dict
        for i, field in enumerate(self.fields):
            if field in meta_dict.keys():
                values[i] = meta_dict[field]

        # write the metadata
        byte_array = struct.pack(self.fmt, *values)

        return self._write(byte_array)

    def _read_meta(self, offset):
        '''
        Helper to read the metadata in the heap at offset.

        Parameters
        ----------
        offset: int
            offset of the meta data in the heap file

        Returns
        -------
        values: tuple
            metadata read from heap as a raw tuple of values
        '''
        buf = self._read(offset)
        # read the list of meta values
        return struct.unpack(self.fmt, buf)

    def read_meta(self, offset):
        '''
        Read the metadata in the heap at offset.

        Parameters
        ----------
        offset: int
            offset of the meta data in the heap file

        Returns
        -------
        metadata: dict
            metadata read from heap wraped in a dictionary
        '''
        # get the values
        values = self._read_meta(offset)

        return {k: v for k, v in zip(self.fields, values)}
