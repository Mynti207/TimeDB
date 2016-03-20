#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from timeseries.TimeSeries import TimeSeries
import types
import numpy as np

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


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
    assert a.__getitem__(2.5) == 0.5
    a.__setitem__(2.5, 8.0)
    assert a.__getitem__(2.5) == 8.0

#
# def test_contains(self):
#     t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v)
# 	self.assertTrue(a.__contains__(1))
#     self.assertFalse(a.__contains__(3))
#
#
# def test_len(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v)
#     self.assertEqual(len(a), 5)
#
#
# def test_eq(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a1 = TimeSeries(t, v)
# 	a2 = TimeSeries(t, v)
#     self.assertEqual(a1, a2)
#
#
# def test_str(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v) # short string
# 	self.assertEqual(str(a), '[0.0, 2.0, -1.0, 0.5, 0.0]')
# 	t = [1, 1.5, 2, 2.5, 10, 11, 12]
# 	v = [0, 2, -1, 0.5, 0, 3, 0.7]
# 	a = TimeSeries(t, v) # long string
#     self.assertEqual(str(a), 'Length: {} [0.0, ..., 0.7]'.format(len(a)))
#
#
# def test_enumerate(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v)
# 	result = list()
# 	for i, vals in enumerate(a):
# 		result.append(vals)
# 	self.assertEqual(result, [0.0, 2.0, -1.0, 0.5, 0.0])
#     self.assertIsInstance(enumerate(a), enumerate)


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

    assert isinstance(a.itertime(), types.GeneratorType)
    assert isinstance(a.itervalues(), types.GeneratorType)
    assert isinstance(a.iteritems(), types.GeneratorType)
#
#
# def test_interpolate(self):
# 	a = TimeSeries([0, 5, 10], [1, 2, 3])
# 	b = TimeSeries([2.5, 7.5], [100, -100])
# 	self.assertEqual(a.interpolate([1]), TimeSeries([1], [1.2]))
# 	self.assertEqual(a.interpolate(b.times()), TimeSeries([2.5, 7.5], [1.5, 2.5]))
#     self.assertEqual(a.interpolate([-100, 100]), TimeSeries([-100, 100], [1, 3]))
#
#
# def test_mean(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v)
#     self.assertEqual(a.mean(), 0.3)
#
#
# def test_median(self):
# 	t = [1, 1.5, 2, 2.5, 10]
# 	v = [0, 2, -1, 0.5, 0]
# 	a = TimeSeries(t, v)
#     self.assertEqual(a.median(), 0.0)
#
#
# def test_lazy(self):
# 	x = TimeSeries([1, 2, 3, 4], [1, 4, 9, 16])
#     self.assertEqual(x, x.lazy.eval())


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
#
#
# def test_subtraction(self):
#
# 	# valid subtraction
# 	t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a1 = TimeSeries(t1, v1)
# 	t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a2 = TimeSeries(t2, v2)
# 	self.assertEqual(a1 - a2, TimeSeries(t1, v1 - v2))
#
# 	# invalid - different times
# 	t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
# 	v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a3 = TimeSeries(t3, v3)
# 	with self.assertRaises(ValueError):
#         a1 - a3
#
# 	# invalid - different lengths
# 	t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
# 	v3 = np.array([10, 12, -11, 1.5, 10, 13])
# 	a3 = TimeSeries(t3, v3)
# 	with self.assertRaises(ValueError):
# 	       a1 - a3
#
#
# def test_multiplication(self):
#
# 	# valid multiplication
# 	t1 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v1 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a1 = TimeSeries(t1, v1)
# 	t2 = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v2 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a2 = TimeSeries(t2, v2)
# 	self.assertEqual(a1 * a2, TimeSeries(t1, v1 * v2))
#
# 	# invalid - different times
# 	t3 = np.array([21, 21.5, 22, 22.5, 210, 211, 212])
# 	v3 = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a3 = TimeSeries(t3, v3)
# 	with self.assertRaises(ValueError):
#         a1 * a3
#
# 	# invalid - different lengths
# 	t3 = np.array([1, 1.5, 2, 2.5, 10, 11])
# 	v3 = np.array([10, 12, -11, 1.5, 10, 13])
# 	a3 = TimeSeries(t3, v3)
# 	with self.assertRaises(ValueError):
#         a1 * a3
#
#
# def test_abs(self):
# 	t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a = TimeSeries(t, v)
# 	self.assertEqual(abs(a), 30.41792234851026)
#     self.assertEqual(abs(TimeSeries([], [])), 0)
#
#
# def test_bool(self):
# 	t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a = TimeSeries(t, v)
# 	self.assertEqual(bool(a), True)
#     self.assertFalse(bool(TimeSeries([], [])))
#
#
# def test_neg(self):
# 	t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v_pos = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	v_neg = v_pos * -1
#     self.assertEqual(-TimeSeries(t, v_pos), TimeSeries(t, v_neg))
#
#
# def test_pos(self):
# 	t = np.array([1, 1.5, 2, 2.5, 10, 11, 12])
# 	v = np.array([10, 12, -11, 1.5, 10, 13, 17])
# 	a = TimeSeries(t, v)
# 	self.assertEqual(a, TimeSeries(t, v))
