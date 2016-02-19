import numpy as np


# stores TimeSeries class
class TimeSeries:

    """ Data and methods for an object representing a general time series.

    Attributes:
        series: sequence representing time series data

    Methods:
        len: returns the length of the sequence
        getitem: given an index (key), return the item of the sequence
        setitem: given an index (key), assign item to corresponding element 
                 of the sequence
        str: returns printable representation of sequence
        resp: returns representation of sequence

    ---

    >>> import numpy as np
    >>> seq = np.arange(0, 10)
    >>> ts = TimeSeries(seq)
    >>> ts[1]==1
    True
    >>> ts[1]=0
    >>> ts[1]!=0
    False
    >>> len(ts)
    10
    >>> seq_short = np.arange(0, 5)
    >>> ts_short = TimeSeries(seq_short)
    >>> seq_large = np.arange(0, 10)
    >>> ts_large = TimeSeries(seq_large)
    >>> string_ts_short = str(ts_short)
    >>> string_ts_large = str(ts_large)
    >>> string_ts_short == '[0 1 2 3 4]'
    True
    >>> string_ts_large != 'Length: {} \\n[0, ..., 9]'.format(len(ts_large))
    False
    >>> print(string_ts_short)
    [0 1 2 3 4]
    >>> print(string_ts_large) #doctest: +NORMALIZE_WHITESPACE
    Length: 10
    [0, ..., 9]
    >>> print(TimeSeries(range(0, 1000000))) #doctest: +NORMALIZE_WHITESPACE
    Length: 1000000
    [0, ..., 999999]

    """

    # initialize TimeSeries with a sequence object
    def __init__(self, times, values):
        """ Initializes a TimeSeries instance with two given sequences,
        times index and corresponding values.

        Take as an argument a sequence object representing initial data 
        to fill the time series instance with. This argument can be any 
        object that can be treated like a sequence.
        """
        self.__times = np.array(times)
        self.__values = np.array(values)
        # Private properties used to make the lookup in times faster
        self.__times_to_index = {t: i for i, t in enumerate(times)}

    @property
    def times(self):
        """ Sequence representing time series index. """
        return self.__times

    @property
    def values(self):
        """ Sequence representing time series values. """
        return self.__values

    @property
    def times_to_index(self):
        """ Dictionnary mapping times index with integer index of the array"""
        return self.__times_to_index

    def __len__(self):
        """ Returns the length of the sequence. """
        return len(self.times)

    def __getitem__(self, time):
        """ Takes key as input and returns corresponding item in sequence. """
        return self.__values[self.__times_to_index[time]]

    def __setitem__(self, time, value):
        """ Take a key and item, and assigns item to element of sequence 
        at position identified by key. """
        self.__values[self.__times_to_index[time]] = value

    def __contains__(self, time):
        """
        Take a time and returns true if it is in the times array.
        """
        return time in self.__times_to_index.keys()

    def __iter__(self):
        """
        Iterates over the values array.
        """
        for v in self.__values:
            yield v

    def __str__(self):
        """ Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.
        """
        n = len(self)
        if n > 5:
            return('Length: {} \n[{}, ..., {}]'.format(n,
                                                       self[self.__times[0]],
                                                       self[self.__times[-1]]))
        else:
            list_str = ', '.join([str(v) for v in self])
            return('[{}]'.format(list_str))

    def __repr__(self):
        """ Identifies object as a TimeSeries and returns representation 
        of sequence.

        If the sequence has more than five items, return the first/last
        element, otherwise return the sequence.
        """
        n = len(self)
        if n > 5:
            res = '[{}, ..., {}]'.format(n, self[self.__times[0]],
                                         self[self.__times[-1]])
        else:
            list_str = ', '.join([str(v) for v in self])
            res = '[{}]'.format(list_str)
        return 'TimeSeries({})'.format(res)
