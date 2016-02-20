# test.py

from TimeSeries2 import *
from lazy import *


def add(a, b):
    return a+b


def mul(a, b):
    return a*b


def lazy(f):
    def inner(*args, **kwargs):
        output = LazyOperation(f, *args, **kwargs)
        return output
    return inner


@lazy
def lazy_add(a, b):
    return a + b


@lazy
def lazy_mul(a, b):
    return a * b

# tests

print (isinstance(lazy_add(1, 2), LazyOperation))  # True

thunk = lazy_add(1, 2)
print (thunk.eval())  # 3

thunk = lazy_mul(3, 4)
print(thunk.eval())  # 12

thunk = lazy_mul(lazy_add(1, 2), 4)
print (thunk.eval())  # 12


@lazy
def lazy_add3(a, b, c):
    return a + b + c

thunk = lazy_add3(1, 2, 3)
print(thunk.eval())  # 6


@lazy
def lazy_add3b(a, b, c, p=5):
    return a + b + c + p

thunk = lazy_add3b(1, 2, 3)
print(thunk.eval())  # 11

thunk = lazy_mul(25, 5)
thunk  # should return LazyOperation object


@lazy
def check_length(a, b):
    return len(a) == len(b)

thunk = check_length(TimeSeries(range(0, 4), range(1, 5)),
                     TimeSeries(range(1, 5), range(2, 6)))
# assert thunk.eval()==True
print (thunk.eval())  # True

x = TimeSeries([1, 2, 3, 4], [1, 4, 9, 16])
print (x)
print (x.lazy.eval())
