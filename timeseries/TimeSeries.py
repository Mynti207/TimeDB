import numpy as np
import math
from timeseries.lazy import LazyOperation


class TimeSeries:

    '''
    Data and methods for an object representing a general time series.

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
        mean: returns mean of values
        median: returns median of values

    Doctests: (python3 -m doctest -v <this file>.py)
    Note: doctests are only intended to illustrate functionality. More
    extensive tests are included in the unit tests.
    ---

    # Short time series
    >>> t = [1, 1.5, 2, 2.5, 10]
    >>> v = [0, 2, -1, 0.5, 0]
    >>> a = TimeSeries(t, v)
    >>> a[2.5]
    0.5
    >>> a.__contains__(1)
    True
    >>> len(a)
    5
    >>> a.times()
    array([  1. ,   1.5,   2. ,   2.5,  10. ])
    >>> a.values()
    array([ 0. ,  2. , -1. ,  0.5,  0. ])
    >>> a.items()
    [(1.0, 0.0), (1.5, 2.0), (2.0, -1.0), (2.5, 0.5), (10.0, 0.0)]
    >>> str(a)
    '[0.0, 2.0, -1.0, 0.5, 0.0]'
    >>> a.mean()
    0.29999999999999999
    >>> a.median()
    0.0
    >>> a.interpolate([5])
    TimeSeries([0.333333333333])

    # Long time series
    >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
    >>> v = [0, 2, -1, 0.5, 0, 3, 7]
    >>> a = TimeSeries(t, v)
    >>> print(a)
    Length: 7 [0.0, ..., 7.0]

    '''

    def __init__(self, times, values):
        '''
        Initializes a TimeSeries instance with two given sequences,
        times index and corresponding values.

        Take as an argument a sequence object representing initial data
        to fill the time series instance with. This argument can be any
        object that can be treated like a sequence.
        '''
        self.__timesseq = np.array(times)
        self.__valuesseq = np.array(values)
        # Private properties used to make the lookup faster
        self.__times_to_index = {t: i for i, t in enumerate(times)}

    @property
    def timesseq(self):
        '''
        Sequence representing time series index.
        '''
        return self.__timesseq

    @property
    def valuesseq(self):
        '''
        Sequence representing time series values.
        '''
        return self.__valuesseq

    @property
    def times_to_index(self):
        '''
        Dictionary mapping times index with integer index of the array.
        '''
        return self.__times_to_index

    @property
    def lazy(self):
        '''
        Placeholder: Sequence representing Lazy object.
        '''
        def f(*args, **kwargs):
            return self
        return LazyOperation(f, self)

    def __len__(self):
        '''
        Returns the length of the sequence.
        '''
        return len(self.timesseq)

    def __getitem__(self, time):
        '''
        Takes key as input and returns corresponding item in sequence.
        '''
        return self.__valuesseq[self.__times_to_index[time]]

    def __setitem__(self, time, value):
        '''
        Takes a key and item, and assigns item to element of sequence
        at position identified by key.
        '''
        self.__valuesseq[self.__times_to_index[time]] = value

    def __contains__(self, time):
        '''
        Takes a time and returns true if it is in the times array.
        '''
        return time in self.__times_to_index.keys()

    def __iter__(self):
        '''
        Iterates over the values array.
        '''
        for v in self.__valuesseq:
            yield v

    def itervalues(self):
        '''
        Iterates over the values array.
        '''
        for v in self.__valuesseq:
            yield v

    def itertimes(self):
        '''
        Iterates over the times array.
        '''
        for t in self.__timesseq:
            yield t

    def iteritems(self):
        '''
        Iterates over the time-values pairs.
        '''
        for t, v in zip(self.__timesseq, self.__valuesseq):
            yield t, v

    def __str__(self):
        '''
        Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.
        '''
        n = len(self)
        if n > 5:
            return('Length: {} [{}, ..., {}]'.format(
                n, self[self.__timesseq[0]], self[self.__timesseq[-1]]))
        else:
            list_str = ', '.join([str(v) for v in self])
            return('[{}]'.format(list_str))

    def __repr__(self):
        '''
        Identifies object as a TimeSeries and returns representation
        of sequence.

        If the sequence has more than five items, return the first/last
        element, otherwise return the sequence.
        '''
        n = len(self)
        if n > 5:
            res = '[{}, ..., {}]'.format(n, self[self.__timesseq[0]],
                                         self[self.__timesseq[-1]])
        else:
            list_str = ', '.join([str(v) for v in self])
            res = '[{}]'.format(list_str)
        return 'TimeSeries({})'.format(res)

    def __eq__(self, other):
        '''
        Determines if two TimeSeries have the same values in
        the same sequence.
        '''
        try:
            return (np.all(self.__timesseq == other.__timesseq) &
                    np.all(self.__valuesseq == other.__valuesseq))
        except:
            raise NotImplemented

    def values(self):
        '''
        Returns the values sequence.
        '''
        return self.valuesseq

    def times(self):
        '''
        Returns the times sequence.
        '''
        return self.timesseq

    def items(self):
        '''
        Returns sequence of (time, value) tuples.
        '''
        return [(time, self[time]) for time in self.__timesseq]

    def get_interpolated(self, tval):
        '''
        Returns the value in TimeSeries corresponding to time tval.

        If tval does not exist, return interpolated value.
        If tval is beyond tval bounds, return value at boundary
        (i.e. do not extrapolate).

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected.
        '''

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
        '''
        Returns a TimeSeries object containing the elements
        of a new sequence tseq and interpolated values in the TimeSeries.

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected.
        '''

        valseq = [self.get_interpolated(t) for t in tseq]

        return TimeSeries(tseq, valseq)

    def mean(self):
        '''
        Returns (arithmetic) mean of the values stored in the class.
        '''

        return self.__valuesseq.mean()

    def median(self):
        '''
        Returns median of the values stored in the class.
        '''

        return np.median(self.__valuesseq)

    ###########
    #
    # new methods re: time series "mathematical" operations
    #
    ###########

    def __neg__(self):
        '''
        Returns a Time Series with the values of the Time Series negated.

        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (-a2)
        Length: 7 [-10.0, ..., -17.0]
        '''
        return TimeSeries(self.__timesseq, self.__valuesseq * -1)

    def __pos__(self):
        '''
        Returns the Time Series (identity).
        '''
        return TimeSeries(self.__timesseq, self.__valuesseq)

    def __abs__(self):
        '''
        Returns L2 norm of Time Series values

        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> abs(a2)
        30.41792234851026
        '''
        return math.sqrt(sum(x * x for x in self.__valuesseq))

    def __bool__(self):
        '''
        Returns true if L2 norm is non-zero.
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> bool(a2)
        True
        '''
        return bool(abs(self))

    def _check_length_helper(self, rhs):
        '''
        Checks if two Time Series have the same length
        '''
        if not len(self.__timesseq) == len(rhs.__timesseq):
            raise ValueError(str(self)+' and '+str(rhs)+' \
                             do not have the same lengths')

    def __add__(self, rhs):
        '''
        Takes as input two Time Series and returns a Time Series
        object with the values added if the input Time Series
        have the same times.

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 + a2)
        Length: 7 [10.0, ..., 24.0]
        '''
        try:
            self._check_length_helper(rhs)
            if not np.allclose(self.__timesseq, rhs.__timesseq):
                raise ValueError(str(self)+' and '+str(rhs)+' \
                                must have the same time points')
            return TimeSeries(self.__timesseq,
                              np.add(self.__valuesseq, rhs.__valuesseq))
        except TypeError:
            raise NotImplemented

    def __radd__(self, other):  # other + self delegates to __add__
        return self + other

    def __sub__(self, rhs):
        '''
        Takes as input two Time Series and returns a Time Series
        object with the values of the second subtracted from the
        first, if the input Time Series have the same times.

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 - a2)
        Length: 7 [-10.0, ..., -10.0]
        '''
        try:
            self._check_length_helper(rhs)
            if not np.allclose(self.__timesseq, rhs.__timesseq):
                raise ValueError(str(self)+' and '+str(rhs)+' \
                                must have the same time points')
            return TimeSeries(self.__timesseq,
                              np.subtract(self.__valuesseq, rhs.__valuesseq))
        except TypeError:
            raise NotImplemented

    def __rsub__(self, other):  # other + self delegates to __add__
        return -self + other

    def __mul__(self, rhs):
        '''
        Takes as input two Time Series and returns a Time Series
        object with the values multiplied element-wise,
        if the input Time Series have the same times.
        Note: this is NOT a dot product.

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 * a2)
        Length: 7 [0.0, ..., 119.0]
        '''
        try:
            self._check_length_helper(rhs)
            if not np.allclose(self.__timesseq, rhs.__timesseq):
                raise ValueError(str(self)+' and '+str(rhs)+' \
                                must have the same time points')
            return TimeSeries(self.__timesseq,
                              np.multiply(self.__valuesseq, rhs.__valuesseq))
        except TypeError:
            raise NotImplemented

    def __rmul__(self, other):  # other + self delegates to __mul__
        return self * other
