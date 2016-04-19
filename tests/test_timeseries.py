import pytest
import types
import numpy as np
from timeseries import *


def test_init():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert a[2.5] == 0.5
    assert a[1.5] != 2.5
    a[1.5] = 2.5
    assert a[1.5] == 2.5
    with pytest.raises(KeyError):
        a[0]


def test_set_get():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert a[2.5] == 0.5
    with pytest.raises(KeyError):
        a[5.0]
    a[2.5] = 8.0
    assert a[2.5] == 8.0
    a[5] = 9.0
    assert a[5] == 9.0


def test_contains():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert (1 in a)
    assert (3 not in a)


def test_len():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert len(a) == 5
    a = TimeSeries([], [])
    assert len(a) == 0


def test_eq():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a1 = TimeSeries(t, v)
    a2 = TimeSeries(t, v)
    assert a1 == a2
    v1 = [0, 2, -1, 0.5, 0]
    v2 = [0, 2, -1, 0.5, 10]
    a1 = TimeSeries(t, v1)
    a2 = TimeSeries(t, v2)
    assert a1 != a2
    t1 = [1, 1.5, 2, 2.5, 10]
    v1 = [0, 2, -1, 0.5, 0]
    t2 = [1, 1.5, 2, 2.5, 10, 11]
    v2 = [0, 2, -1, 0.5, 0, 12]
    a1 = TimeSeries(t1, v1)
    a2 = TimeSeries(t2, v2)
    with pytest.raises(ValueError):
        a1 == a2


def test_str():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)  # short string
    assert str(a) == '[0.0, 2.0, -1.0, 0.5, 0.0]'
    t = [1, 1.5, 2, 2.5, 10, 11, 12]
    v = [0, 2, -1, 0.5, 0, 3, 0.7]
    a = TimeSeries(t, v)  # long string
    assert str(a) == 'Length: 7 [0.0, ..., 0.7]'


def test_repr():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)  # short string
    assert repr(a) == 'TimeSeries([0.0, 2.0, -1.0, 0.5, 0.0])'
    t = [1, 1.5, 2, 2.5, 10, 11, 12]
    v = [0, 2, -1, 0.5, 0, 3, 0.7]
    a = TimeSeries(t, v)  # long string
    assert repr(a) == 'TimeSeries(Length: 7 [0.0, ..., 0.7])'


def test_enumerate():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    result = [val for i, val in enumerate(a)]
    assert result == [0.0, 2.0, -1.0, 0.5, 0.0]
    assert isinstance(enumerate(a), enumerate)
    result = [val for val in a]
    assert result == [0.0, 2.0, -1.0, 0.5, 0.0]


def test_iters():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)

    assert list(a.times()) == t
    assert list(a.values()) == v
    assert list(a.items()) == list(zip(t, v))

    assert list(a.itertimes()) == t
    assert list(a.itervalues()) == v
    assert list(a.iteritems()) == list(zip(t, v))

    assert isinstance(a.itertimes(), types.GeneratorType)
    assert isinstance(a.itervalues(), types.GeneratorType)
    assert isinstance(a.iteritems(), types.GeneratorType)


def test_interpolate():
    a = TimeSeries([0, 5, 10], [1, 2, 3])
    b = TimeSeries([2.5, 7.5], [100, -100])
    assert a.get_interpolated(1) == 1.2
    assert a.interpolate([1]) == TimeSeries([1], [1.2])
    assert a.interpolate(b.times()) == TimeSeries([2.5, 7.5], [1.5, 2.5])
    assert a.interpolate([-100, 100]) == TimeSeries([-100, 100], [1, 3])


def test_mean():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert a.mean() == 0.3


def test_std():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert a.std() == 0.9797958971132712


def test_median():
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    assert a.median() == 0.0


def test_lazy():
    # Note: more extensive testing of lazy operations included in test_lazy.py
    x = TimeSeries([1, 2, 3, 4], [1, 4, 9, 16])
    assert x == x.lazy.eval()


def test_addition():

    # valid addition
    t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t1, v1)
    t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a2 = TimeSeries(t2, v2)
    assert (a1 + a2) == TimeSeries(t1, v1 + v2)

    # invalid - different times
    t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
    v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 + a3

    # invalid - different lengths
    t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
    v3 = np.array([10, 12, -11, 1.5, 10, 13])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 + a3

    # right-hand addition
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    v2 = v1 + 5
    a2 = TimeSeries(t, v2)
    assert (a1 + 5) == a2

    # arrays or lists
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    with pytest.raises(NotImplementedError):
        a + [10, 12, -11, 1.5, 10, 13, 17]
    with pytest.raises(NotImplementedError):
        a + np.array([10, 12, -11, 1.5, 10, 13, 17])


def test_subtraction():

    # valid subtraction
    t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t1, v1)
    t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a2 = TimeSeries(t2, v2)
    assert (a1 - a2) == TimeSeries(t1, v1 - v2)

    # invalid - different times
    t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
    v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 - a3

    # invalid - different lengths
    t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
    v3 = np.array([10, 12, -11, 1.5, 10, 13])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 - a3

    # right-hand subtraction
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    v2 = v1 - 5
    a2 = TimeSeries(t, v2)
    assert (a1 - 5) == a2

    # arrays or lists
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    with pytest.raises(NotImplementedError):
        a - [10, 12, -11, 1.5, 10, 13, 17]
    with pytest.raises(NotImplementedError):
        a - np.array([10, 12, -11, 1.5, 10, 13, 17])


def test_multiplication():

    # valid multiplication
    t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t1, v1)
    t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a2 = TimeSeries(t2, v2)
    assert (a1 * a2) == TimeSeries(t1, v1 * v2)

    # invalid - different times
    t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
    v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 * a3

    # invalid - different lengths
    t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
    v3 = np.array([10, 12, -11, 1.5, 10, 13])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 * a3

    # right-hand multiplication
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    v2 = v1 * 5
    a2 = TimeSeries(t, v2)
    assert (a1 * 5) == a2

    # arrays or lists
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    with pytest.raises(NotImplementedError):
        a * [10, 12, -11, 1.5, 10, 13, 17]
    with pytest.raises(NotImplementedError):
        a * np.array([10, 12, -11, 1.5, 10, 13, 17])


def test_division():

    # valid multiplication
    t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t1, v1)
    t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a2 = TimeSeries(t2, v2)
    assert (a1 / a2) == TimeSeries(t1, v1 / v2)

    # invalid - different times
    t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
    v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 / a3

    # invalid - different lengths
    t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
    v3 = np.array([10, 12, -11, 1.5, 10, 13])
    a3 = TimeSeries(t3, v3)
    with pytest.raises(ValueError):
        a1 / a3

    # right-hand multiplication
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a1 = TimeSeries(t, v1)
    v2 = v1 / 5
    a2 = TimeSeries(t, v2)
    assert (a1 / 5) == a2

    # arrays or lists
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    with pytest.raises(NotImplementedError):
        a / [10, 12, -11, 1.5, 10, 13, 17]
    with pytest.raises(NotImplementedError):
        a / np.array([10, 12, -11, 1.5, 10, 13, 17])


def test_pos():
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    assert (+a) == a


def test_neg():
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v_pos = np.array([10, 12, -11, 1.5, 10, 13, 17])
    v_neg = v_pos * -1
    a_pos = TimeSeries(t, v_pos)
    a_neg = TimeSeries(t, v_neg)
    assert -a_pos == a_neg


def test_abs():
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    assert abs(a) == 30.41792234851026
    assert abs(TimeSeries([], [])) == 0


def test_bool():
    t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
    v = np.array([10, 12, -11, 1.5, 10, 13, 17])
    a = TimeSeries(t, v)
    assert bool(a)
    assert(not bool(TimeSeries([], [])))
