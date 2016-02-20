# TimeSeries2.py

import numpy as np
from lazy import *


class TimeSeries:

    """ Data and methods for an object representing a general time series.

    Attributes:
        timesseq: sequence representing time series index
        valuesseq: sequence representing time series values
        times_to_index: mapping to array index

    Methods:
        __len__: returns the length of the sequence
        __getitem__: given an index (key), return the item of the sequence
        __setitem__: given an index (key), assign item to corresponding element
                 of the sequence
        __str__: returns printable representation of sequence
        __resp__: returns representation of sequence
        __eq__: elementwise comparison of both times and values in sequence

        times: returns the time series sequence
        valuesseq: returns the time series values sequence
        items: returns a sequence of (time, value) tuples

    Doctests: (python3 -m doctest -v <this file>.py)
    ---

    >>> t = [1, 1.5, 2, 2.5, 10]
    >>> v = [0, 2, -1, 0.5, 0]
    >>> a = TimeSeries(t, v)
    >>> a[2.5] == 0.5
    True
    >>> a[1.5] == 2.5
    False
    >>> a[1.5] = 2.5
    >>> a[1.5] == 2.5
    True
    >>> a[0] = 3
    Traceback (most recent call last):
        ...
    KeyError: 0
    >>> a.__contains__(1)
    True
    >>> a.__contains__(3)
    False
    >>> len(a)
    5
    >>> for i, vals in enumerate(a):
    ...    print (vals)  #doctest: +NORMALIZE_WHITESPACE
    0.0
    2.5
    -1.0
    0.5
    0.0
    >>> str(a)
    '[0.0, 2.5, -1.0, 0.5, 0.0]'
    >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
    >>> v = [0, 2, -1, 0.5, 0, 3, 7]
    >>> a = TimeSeries(t, v)
    >>> print(a)
    Length: 7 [0.0, ..., 7.0]
    >>> a.times()
    array([  1. ,   1.5,   2. ,   2.5,  10. ,  11. ,  12. ])
    >>> a.values()
    array([ 0. ,  2. , -1. ,  0.5,  0. ,  3. ,  7. ])
    >>> a.items() #doctest: +NORMALIZE_WHITESPACE
    [(1.0, 0.0),
    (1.5, 2.0),
    (2.0, -1.0),
    (2.5, 0.5),
    (10.0, 0.0),
    (11.0, 3.0),
    (12.0, 7.0)]
    >>> a = TimeSeries([0, 5, 10], [1, 2, 3])
    >>> b = TimeSeries([2.5, 7.5], [100, -100])
    >>> a.interpolate([1]) == TimeSeries([1], [1.2])
    True
    >>> a.interpolate(b.times()) == TimeSeries([2.5, 7.5], [1.5, 2.5])
    True
    >>> a.interpolate([-100, 100]) == TimeSeries([-100, 100], [1, 3])
    True
    >>> x = TimeSeries([1, 2, 3, 4], [1, 4, 9, 16])
    >>> print (x)
    [1, 4, 9, 16]
    >>> print (x.lazy.eval())
    [1, 4, 9, 16]

    """

    # initialize TimeSeries with a sequence object
    def __init__(self, times, values):
        """ Initializes a TimeSeries instance with two given sequences,
        times index and corresponding values.

        Take as an argument a sequence object representing initial data
        to fill the time series instance with. This argument can be any
        object that can be treated like a sequence.
        """
        self.__timesseq = np.array(times)
        self.__valuesseq = np.array(values)
        # Private properties used to make the lookup in times faster
        self.__times_to_index = {t: i for i, t in enumerate(times)}

    @property
    def timesseq(self):
        """ Sequence representing time series index. """
        return self.__timesseq

    @property
    def valuesseq(self):
        """ Sequence representing time series values. """
        return self.__valuesseq

    @property
    def times_to_index(self):
        """ Dictionary mapping times index with integer index of the array. """
        return self.__times_to_index

    @property
    def lazy(self):
        """ Placeholder: Sequence representing Lazy object. """
        def f(*args, **kwargs):
            return self
        return LazyOperation(f, self)

    def __len__(self):
        """ Returns the length of the sequence. """
        return len(self.timesseq)

    def __getitem__(self, time):
        """ Takes key as input and returns corresponding item in sequence. """
        return self.__valuesseq[self.__times_to_index[time]]

    def __setitem__(self, time, value):
        """ Take a key and item, and assigns item to element of sequence
        at position identified by key. """
        self.__valuesseq[self.__times_to_index[time]] = value

    def __contains__(self, time):
        """
        Take a time and returns true if it is in the times array.
        """
        return time in self.__times_to_index.keys()

    def __iter__(self):
        """
        Iterates over the values array.
        """
        for v in self.__valuesseq:
            yield v

    def __str__(self):
        """ Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.
        """
        n = len(self)
        if n > 5:
            return('Length: {} [{}, ..., {}]'.format(n,
                                                     self[self.__timesseq[0]],
                                                     self[self.__timesseq[-1]]))
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
            res = '[{}, ..., {}]'.format(n, self[self.__timesseq[0]],
                                         self[self.__timesseq[-1]])
        else:
            list_str = ', '.join([str(v) for v in self])
            res = '[{}]'.format(list_str)
        return 'TimeSeries({})'.format(res)

    def __eq__(self, other):
        """ Determines if two TimeSeries have the same values in
        the same sequence. """

        return (np.all(self.__timesseq == other.__timesseq) &
                np.all(self.__valuesseq == other.__valuesseq))

    def values(self):
        """ Returns the values sequence. """
        return self.valuesseq

    def times(self):
        """ Returns the times sequence. """
        return self.timesseq

    def items(self):
        """ Returns sequence of (time, value) tuples. """
        return [(time, self[time]) for time in self.__timesseq]

    def get_interpolated(self, tval):
        """ Returns the value in TimeSeries corresponding to time tval.

        If tval does not exist, return interpolated value.
        If tval is beyond tval bounds, return value at boundary
        (i.e. do not extrapolate).

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected. """

        for i in range(len(self)-1):
            if tval <= self.__timesseq[i]:
                return self[self.__timesseq[i]]
            if (tval > self.__timesseq[i]) & (tval < self.__timesseq[i+1]):
                # calculate interpolated value
                time_delta = self.__timesseq[i+1] - self.__timesseq[i]
                step = (tval - self.__timesseq[i]) / time_delta
                v_delta = self.__valuesseq[i+1] - self.__valuesseq[i]
                return v_delta * step + self.__valuesseq[i]

        # above range of time values
        return self[self.__timesseq[len(self)-1]]

    def interpolate(self, tseq):
        """ Returns a TimeSeries object containing the elements
        of a new sequence tseq and interpolated values in the TimeSeries.

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected. """

        valseq = [self.get_interpolated(t) for t in tseq]

        return TimeSeries(tseq, valseq)
