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
    def __init__(self, seq):
        """ Initializes a TimeSeries instance with a given sequence.

        Take as an argument a sequence object representing initial data 
        to fill the time series instance with. This argument can be any 
        object that can be treated like a sequence.
        """
        self.__series = list(seq)

    @property
    def series(self):
        """ Sequence representing time series data. """
        return self.__series

    def __len__(self):
        """ Returns the length of the sequence. """
        return len(self.series)

    def __getitem__(self, key):
        """ Takes key as input and returns corresponding item in sequence. """
        return self.__series[key]

    def __setitem__(self, key, item):
        """ Take a key and item, and assigns item to element of sequence 
        at position identified by key. """
        self.__series[key] = item

    def __str__(self):
        """ Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.
        """
        n = len(self)
        if n > 5:
            return('Length: {} \n[{}, ..., {}]'.format(n, self[0], self[-1]))
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
            res = '[{}, ..., {}]'.format(self[0], self[-1])
        else:
            list_str = ', '.join([str(v) for v in self])
            res = '[{}]'.format(list_str)
        return 'TimeSeries({})'.format(res)
