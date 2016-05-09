import json
from collections import OrderedDict

LENGTH_FIELD_LENGTH = 4


def serialize(json_obj):
    '''
    Turn a JSON object into bytes suitable for writing out to the network.
    Includes a fixed-width length field to simplify reconstruction on the other
    end of the wire.

    Parameters
    ----------
    json_obj : object in json format
        The object to be serialized

    Returns
    -------
    buf : bytes
        The serialized object
    '''

    # serialize, i.e. return the bytes on the wire
    obj_serialized = bytearray(json.dumps(json_obj), 'utf-8')

    # start the buffer based on the fixed-width length field
    buf = (len(obj_serialized) +
           LENGTH_FIELD_LENGTH).to_bytes(LENGTH_FIELD_LENGTH,
                                         byteorder="little")

    # add the serialized json object to the buffer
    buf += obj_serialized

    # return the buffer
    return buf


class Deserializer(object):
    '''
    A buffering and bytes-to-json engine.

    Data can be received in arbitrary chunks of bytes, and we need a way to
    reconstruct variable-length JSON objects from that interface. This class
    buffers up bytes until it can detect that it has a full JSON object (via
    a length field pulled off the wire). To use this, add bytes with the
    append() function and call ready() to check if we've reconstructed a JSON
    object. If True, then call deserialize to return it. That object will be
    removed from this buffer after it is returned.
    '''

    def __init__(self):
        '''
        Initializes the Deserializer class.

        Parameters
        ----------
        None

        Returns
        -------
        An initialized Deserializer object
        '''
        # initialize blank buffer
        self.buf = b''
        self.buflen = -1

    def append(self, data):
        '''
        Appends data to the Deserializer's buffer.

        Parameters
        ----------
        data : bytes
            Binary data to be deserialized

        Returns
        -------
        Nothing, modifies in-place.
        '''
        self.buf += data
        self._maybe_set_length()

    def _maybe_set_length(self):
        '''
        Calculates and stores the length of the Deserializer's buffer.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # only calculate if there is data in the buffer
        if self.buflen < 0 and len(self.buf) >= LENGTH_FIELD_LENGTH:
            # update buffer length
            self.buflen = int.from_bytes(self.buf[0:LENGTH_FIELD_LENGTH],
                                         byteorder="little")

    def ready(self):
        '''
        Determines whether the buffer is ready to be deserialized, based
        on its length.

        Parameters
        ----------
        None

        Returns
        -------
        Boolean : whether the buffer is ready to be deserialized
        '''
        return (self.buflen > 0 and len(self.buf) >= self.buflen)

    def deserialize(self):
        '''
        Deserializes the buffer.

        Parameters
        ----------
        None

        Returns
        -------
        json : deserialized buffer
        '''
        # deserialize the data in the buffer
        json_str = self.buf[LENGTH_FIELD_LENGTH:self.buflen].decode()

        # remove the deserialized data from the buffer
        self.buf = self.buf[self.buflen:]
        self.buflen = -1

        # preserve the buffer, as there may already be more data in it
        self._maybe_set_length()

        # try to load the deserialized data as a json object
        try:
            # if it loads successfully, return it
            return json.loads(json_str, object_pairs_hook=OrderedDict)
        except json.JSONDecodeError:
            # otherwise it is not valid json data, so don't return it
            return None
