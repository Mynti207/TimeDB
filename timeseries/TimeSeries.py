import numpy as np
import math

from timeseries.lazy import LazyOperation
import pype

class TimeSeries:

    '''
    Data and methods for an object representing a general time series.

    Attributes:
        timesseq: sequence representing time series times (indices)
        valuesseq: sequence representing time series values
        times_to_index: mapping of time series times to index values
        lazy: sequence representing a Lazy object

    Methods:
        __len__: returns the length of the sequence
        __getitem__: given an index (key), return the item of the sequence
        __setitem__: given an index (key), assign item to corresponding element
                 of the sequence
        __contains__: given an index (key), determines if it is present
        __iter__: iterates over the time series values
        __str__: returns printable representation of sequence
        __repr__: returns representation of sequence
        __eq__: elementwise comparison of both times and values in sequence
        __neg__: returns a time series object with negated values
        __pos__: returns a time series object with positive values (identity)
        __abs__: returns the L2 norm of the time series values
        __bool__: returns whether the L2 norm of the time series values is
            non-zero
        __add__: given two time series, returns a new time series object
            with the values for common times added
        __sub__: given two time series, returns a new time series object
            with the values for common times subtracted
        __mul__: given two time series, returns a new time series object
            with the value for common times multiplied (element-wise)

        times: returns the time series times sequence
        values: returns the time series values sequence
        items: returns a sequence of (time, value) tuples
        itertimes: returns an iterator of the time series times
        itervalues: returns an iterator of the time series values
        iteritems: returns an iterator of the time series time-value pairs
        get_interpolated: given a time, returns the interpolated value
        interpolate: given a time sequences, returns a time series object
            containing the interpolated values
        mean: returns mean of time series values
        median: returns median of time series values

    Doctests: (python3 -m doctest -v TimeSeries.py)
    '''

    def __init__(self, times, values):
        '''
        Initializes a TimeSeries instance with two given sequences,
        times index and corresponding values.

        Take as an argument a sequence object representing initial data
        to fill the time series instance with. This argument can be any
        object that can be treated like a sequence.

        Parameters
        ----------
        times : sequence of ints or floats (list, array, etc.)
            A sequence of numerical times
        values : sequence of ints or floats (list, array etc.)
            A sequence of numerical values

        Returns
        -------
        TimeSeries
            A time series object with times and values equal to the parameters

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a[2.5]
        0.5
        '''

        # cast as float for consistency across multiple time series objects
        times = np.array(times, dtype=float)
        values = np.array(values, dtype=float)

        # make sure that times are monotonically increasing
        sort_order = np.argsort(times)
        times = times[sort_order]
        values = values[sort_order]

        # private properties used to make the lookup faster
        self.__timesseq = np.array(times)
        self.__valuesseq = np.array(values)
        self.__times_to_index = {t: i for i, t in enumerate(times)}

    @property
    def timesseq(self):
        '''
        Sequence representing time series index.
        Private property - cannot be called directly.
        '''

        return self.__timesseq

    @property
    def valuesseq(self):
        '''
        Sequence representing time series values.
        Private property - cannot be called directly.
        '''
        return self.__valuesseq

    @property
    def times_to_index(self):
        '''
        Dictionary mapping times index with integer index of the array.
        Private property - cannot be called directly.
        '''
        return self.__times_to_index

    def times(self):
        '''
        Returns the times sequence.

        Parameters
        ----------
        None

        Returns
        -------
        Numpy array
            Time series times

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.times()
        array([  1. ,   1.5,   2. ,   2.5,  10. ])
        '''
        return self.timesseq

    def values(self):
        '''
        Returns the values sequence.

        Parameters
        ----------
        None

        Returns
        -------
        Numpy array
            Time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.values()
        array([ 0. ,  2. , -1. ,  0.5,  0. ])
        '''
        return self.valuesseq

    def items(self):
        '''
        Returns sequence of (time, value) tuples.

        Parameters
        ----------
        None

        Returns
        -------
        Numpy array of tuples
            Time series time-value pairs

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.items()
        [(1.0, 0.0), (1.5, 2.0), (2.0, -1.0), (2.5, 0.5), (10.0, 0.0)]
        '''
        return [(time, self[time]) for time in self.__timesseq]

    @property
    def lazy(self):
        '''
        Returns sequence representing Lazy object.

        Parameters
        ----------
        None

        Returns
        -------
        Lazy operation
            A lazified operation, i.e. doesn't evaluate until called.
        '''
        def f(*args, **kwargs):
            return self
        return LazyOperation(f, self)

    def __len__(self):
        '''
        Returns the length of the sequence.

        Parameters
        ----------
        None

        Returns
        -------
        int
            Length of the time series time sequence

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> len(a)
        5
        >>> len(TimeSeries([], []))
        0
        '''
        return len(self.timesseq)

    def __getitem__(self, time):
        '''
        Takes key as input and returns corresponding item in sequence.

        Parameters
        ----------
        time : int or float
            A potential time series time

        Returns
        -------
        float
            Time series value associated with the given time

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a[2.5]
        0.5
        '''
        try:
            return self.__valuesseq[self.times_to_index[float(time)]]
        except KeyError:  # not present
            raise KeyError(str(time) + ' is not present in the TimeSeries.')

    def __setitem__(self, time, value):
        '''
        Takes a key and item, and assigns item to element of sequence
        at position identified by key.

        Parameters
        ----------
        time : int or float
            A time series time
        value : int or float
            A time series value

        Returns
        -------
        Modified in-place

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a[1] = 12.0
        >>> a[1]
        12.0
        >>> a[5] = 9.0
        >>> a[5]
        9.0
        '''
        try:
            self.__valuesseq[self.times_to_index[float(time)]] = float(value)
        except KeyError:  # not present
            times = list(self.timesseq) + [time]
            values = list(self.valuesseq) + [value]
            self.__init__(times, values)

    def __contains__(self, time):
        '''
        Takes a time and returns true if it is in the times array.

        Parameters
        ----------
        time : int or float
            A time series time

        Returns
        -------
        bool
            Whether the time is present in the time series

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> 1 in a
        True
        >>> 3 in a
        False
        '''
        return float(time) in self.times_to_index.keys()

    def __iter__(self):
        '''
        Iterates over the values array.

        Parameters
        ----------
        None

        Returns
        -------
        float(s)
            Iterator of time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> for val in a:
        ...     print(val)
        0.0
        2.0
        -1.0
        0.5
        0.0
        '''
        for v in self.__valuesseq:
            yield v

    def itertimes(self):
        '''
        Iterates over the times array.

        Parameters
        ----------
        None

        Returns
        -------
        float(s)
            Iterator of time series times

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> for val in a.itertimes():
        ...     print(val)
        1.0
        1.5
        2.0
        2.5
        10.0
        '''
        for t in self.__timesseq:
            yield t

    def itervalues(self):
        '''
        Iterates over the values array.

        Parameters
        ----------
        None

        Returns
        -------
        float(s)
            Iterator of time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> for val in a.itervalues():
        ...     print(val)
        0.0
        2.0
        -1.0
        0.5
        0.0
        '''
        for v in self.__valuesseq:
            yield v

    def iteritems(self):
        '''
        Iterates over the time-values pairs.

        Parameters
        ----------
        None

        Returns
        -------
        tuple(s) of floats
            Iterator of time series time-value pairs

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> for val in a.iteritems():
        ...     print(val)
        (1.0, 0.0)
        (1.5, 2.0)
        (2.0, -1.0)
        (2.5, 0.5)
        (10.0, 0.0)
        '''
        for t, v in zip(self.__timesseq, self.__valuesseq):
            yield t, v

    def __str__(self):
        '''
        Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.

        Parameters
        ----------
        None

        Returns
        -------
        str
            Printable representation of the sequence. Format varies based on
            sequence length.

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)  # short string
        >>> str(a)
        '[0.0, 2.0, -1.0, 0.5, 0.0]'
        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [0, 2, -1, 0.5, 0, 3, 0.7]
        >>> a = TimeSeries(t, v)  # long string
        >>> str(a)
        'Length: 7 [0.0, ..., 0.7]'
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

        Parameters
        ----------
        None

        Returns
        -------
        str
            Printable representation of the sequence. Format varies based on
            sequence length.

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)  # short string
        >>> repr(a)
        'TimeSeries([0.0, 2.0, -1.0, 0.5, 0.0])'
        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [0, 2, -1, 0.5, 0, 3, 0.7]
        >>> a = TimeSeries(t, v)  # long string
        >>> repr(a)
        'TimeSeries(Length: 7 [0.0, ..., 0.7])'
        '''
        n = len(self)
        if n > 5:
            res = 'Length: {} [{}, ..., {}]'.format(
                n, self[self.__timesseq[0]], self[self.__timesseq[-1]])
        else:
            list_str = ', '.join([str(v) for v in self])
            res = '[{}]'.format(list_str)
        return 'TimeSeries({})'.format(res)

    def _check_equal_length(self, other):
        '''
        Checks if two time series have the same length

        Parameters
        ----------
        other : TimeSeries
            Another time series to compare against

        Returns
        -------
        boolean
            Whether the time series have the same length
        '''
        return len(self.__timesseq) == len(other.__timesseq)

    def __eq__(self, other):
        '''
        Determines if two TimeSeries have the same values in
        the same sequence.

        Parameters
        ----------
        other : TimeSeries
            A time series to compare against

        Returns
        -------
        bool
            Whether the times and values are equal for both time series

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a1 = TimeSeries(t, v)
        >>> a2 = TimeSeries(t, v)
        >>> a1 == a2
        True
        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v1 = [0, 2, -1, 0.5, 0]
        >>> v2 = [0, 2, -1, 0.5, 1]
        >>> a1 = TimeSeries(t, v1)
        >>> a2 = TimeSeries(t, v2)
        >>> a1 == a2
        False
        '''
        if self._check_equal_length(other):
            return (np.all(self.__timesseq == other.__timesseq) &
                    np.all(self.__valuesseq == other.__valuesseq))
        else:
            raise ValueError('Cannot compare TimeSeries of different lengths.')

    def get_interpolated(self, tval):
        '''
        Returns the value in TimeSeries corresponding to a single time tval.

        If tval does not exist, return interpolated value.
        If tval is beyond tval bounds, return value at boundary
        (i.e. do not extrapolate).

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected.

        Parameters
        ----------
        tval : int or float
            Time series value

        Returns
        -------
        float
            Either the actual or interpolated value associated with the time

        >>> a = TimeSeries([0, 5, 10], [1, 2, 3])
        >>> b = TimeSeries([2.5, 7.5], [100, -100])
        >>> a.get_interpolated(1)
        1.2
        '''
        for i in range(len(self)-1):

            # tval less than smallest time
            if tval <= self.__timesseq[i]:
                return self[self.__timesseq[i]]

            # tval within range of time series times
            if (tval > self.__timesseq[i]) & (tval < self.__timesseq[i+1]):
                # calculate interpolated value
                time_delta = self.__timesseq[i+1] - self.__timesseq[i]
                step = (tval - self.__timesseq[i]) / time_delta
                v_delta = self.__valuesseq[i+1] - self.__valuesseq[i]
                return v_delta * step + self.__valuesseq[i]

        # tval above range of time series times
        return self[self.__timesseq[len(self)-1]]

    def interpolate(self, tseq):
        '''
        Returns a TimeSeries object containing the elements
        of a new sequence tseq and interpolated values in the TimeSeries.

        This method assume the times in timesseq are monotonically
        increasing; otherwise, results may not be as expected.

        Parameters
        ----------
        tseq : list of ints or floats
            Time series times to interpolate

        Returns
        -------
        TimeSeries
            Time series object with all the interpolated values for the
            given times.

        >>> a = TimeSeries([0, 5, 10], [1, 2, 3])
        >>> b = TimeSeries([2.5, 7.5], [100, -100])
        >>> a.interpolate([-100, 100])
        TimeSeries([1.0, 3.0])
        '''

        valseq = [self.get_interpolated(t) for t in tseq]

        return TimeSeries(tseq, valseq)

    @pype.component
    def mean(self):
        '''
        Returns (arithmetic) mean of the values stored in the class.

        Parameters
        ----------
        None

        Returns
        -------
        float
            Arithmetic mean of time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.mean()
        0.29999999999999999
        '''

        return np.mean(self.__valuesseq)

    @pype.component
    def std(self):
        '''
        Returns standard deviation of the values stored in the class.

        Parameters
        ----------
        None

        Returns
        -------
        float
            Standard deviation of time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.std()
        0.9797958971132712
        '''

        return np.std(self.__valuesseq)

    def median(self):
        '''
        Returns median of the values stored in the class.

        Parameters
        ----------
        None

        Returns
        -------
        float
            Median of time series values

        >>> t = [1, 1.5, 2, 2.5, 10]
        >>> v = [0, 2, -1, 0.5, 0]
        >>> a = TimeSeries(t, v)
        >>> a.median()
        0.0
        '''

        return np.median(self.__valuesseq)

    def __neg__(self):
        '''
        Returns a time series with the values of the time series negated.

        Parameters
        ----------
        None

        Returns
        -------
        TimeSeries
            A new time series with the same times and negated values

        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a = TimeSeries(t, v)
        >>> print (-a)
        Length: 7 [-10.0, ..., -17.0]
        '''
        return TimeSeries(self.__timesseq, self.__valuesseq * -1.0)

    def __pos__(self):
        '''
        Returns the time series (identity).

        Parameters
        ----------
        None

        Returns
        -------
        TimeSeries
            A new time series with the same times and values

        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a = TimeSeries(t, v)
        >>> print (+a)
        Length: 7 [10.0, ..., 17.0]
        '''
        return TimeSeries(self.__timesseq, self.__valuesseq)

    def __abs__(self):
        '''
        Returns L2 norm of time series values.

        Parameters
        ----------
        None

        Returns
        -------
        float
            L2 norm of the time series values

        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a = TimeSeries(t, v)
        >>> abs(a)
        30.41792234851026
        '''
        return math.sqrt(sum(x * x for x in self.__valuesseq))

    def __bool__(self):
        '''
        Returns true if L2 norm is non-zero.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            Whether the L2 norm of the time series values is non-zero

        >>> t = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a = TimeSeries(t, v)
        >>> bool(a)
        True
        '''
        return bool(abs(self))

    def __add__(self, other):
        '''
        Takes as input two time series and returns a time series object with
        the values added if the input time series have the same times.

        Alternatively, if other is an int or float, it is added to all elements
        of the time series values.

        Parameters
        ----------
        other : TimeSeries
            Another time series, to add by (element-wise).
        OR
        other: int or float
            A numeric value to add to each element of the time series values.

        Returns
        -------
        TimeSeries
            A new time series, with the same times as the two original
            time series and value equal to the sum of their values
            (or, alternatively, with values incremented by other).

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 + a2)
        Length: 7 [10.0, ..., 24.0]
        >>> print (a1 + 2)
        Length: 7 [2.0, ..., 9.0]
        '''

        if type(other) in (int, float):
            return TimeSeries(self.__timesseq, self.__valuesseq + other)

        if not isinstance(other, TimeSeries):
            raise NotImplementedError

        if self._check_equal_length(other):
            if not np.allclose(self.__timesseq, other.__timesseq):
                raise ValueError(str(self) + ' and ' + str(other) +
                                 ' must have the same time points.')
            else:
                return TimeSeries(self.__timesseq,
                                  np.add(self.__valuesseq, other.__valuesseq))
        else:
            raise ValueError('Cannot carry out arithmetic operations on \
                              TimeSeries of different lengths.')

    def __sub__(self, other):
        '''
        Takes as input two time series and returns a time series object with
        the values subtracted if the input time series have the same times.

        Alternatively, if other is an int or float, it is subtracted from all
        elements of the time series values.

        Parameters
        ----------
        other : TimeSeries
            Another time series, to subtract by (element-wise).
        OR
        other: int or float
            A numeric value to subtract from each element of the time series
            values.

        Returns
        -------
        TimeSeries
            A new time series, with the same times as the two original
            time series and value equal to the difference of their values
            (or, alternatively, with values subtracted by other).

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 - a2)
        Length: 7 [-10.0, ..., -10.0]
        >>> print (a1 - 2)
        Length: 7 [-2.0, ..., 5.0]
        '''
        if type(other) in (int, float):
            return TimeSeries(self.__timesseq, self.__valuesseq - other)

        if not isinstance(other, TimeSeries):
            raise NotImplementedError

        if self._check_equal_length(other):
            if not np.allclose(self.__timesseq, other.__timesseq):
                raise ValueError(str(self) + ' and ' + str(other) +
                                 ' must have the same time points.')
            else:
                return TimeSeries(self.__timesseq,
                                  np.subtract(self.__valuesseq,
                                              other.__valuesseq))
        else:
            raise ValueError('Cannot carry out arithmetic operations on \
                              TimeSeries of different lengths.')

    def __mul__(self, other):
        '''
        Takes as input two time series and returns a time series object with
        the values multiplied if the input time series have the same times.

        Alternatively, if other is an int or float, it is multiplied by all
        elements of the time series values.

        Parameters
        ----------
        other : TimeSeries
            Another time series, to multiply by (element-wise).
        OR
        other: int or float
            A numeric value to multiply to each element of the time series
            values.

        Returns
        -------
        TimeSeries
            A new time series, with the same times as the two original
            time series and value equal to the product of their values
            (or, alternatively, with values multiplied by other).

        >>> t1 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v1 = [0, 2, -1, 0.5, 0, 3, 7]
        >>> a1 = TimeSeries(t1, v1)
        >>> t2 = [1, 1.5, 2, 2.5, 10, 11, 12]
        >>> v2 = [10, 12, -11, 1.5, 10, 13, 17]
        >>> a2 = TimeSeries(t2, v2)
        >>> print (a1 * a2)
        Length: 7 [0.0, ..., 119.0]
        >>> print (a1 * 2)
        Length: 7 [0.0, ..., 14.0]
        '''
        if type(other) in (int, float):
            return TimeSeries(self.__timesseq, self.__valuesseq * other)

        if not isinstance(other, TimeSeries):
            raise NotImplementedError

        if self._check_equal_length(other):
            if not np.allclose(self.__timesseq, other.__timesseq):
                raise ValueError(str(self) + ' and ' + str(other) +
                                 ' must have the same time points.')
            else:
                return TimeSeries(self.__timesseq,
                                  np.multiply(self.__valuesseq,
                                              other.__valuesseq))
        else:
            raise ValueError('Cannot carry out arithmetic operations on \
                              TimeSeries of different lengths.')
