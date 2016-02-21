# lazytest.py

from TimeSeries import *
from lazy import *

def lazy(f):
    '''
    Lazy decorator - can be used to turn any function lazy.
    '''
    def inner(*args, **kwargs):
        output = LazyOperation(f, *args, **kwargs)
        return output
    return inner

def add(a, b):
    '''
    Standard addition of two numbers.
    '''
    return a+b

def mul(a, b):
    '''
    Standard multiplication of two numbers.
    '''
    return a*b

@lazy
def lazy_add(a, b):
    '''
    Lazy addition of two numbers.
    '''
    return a + b

@lazy
def lazy_mul(a, b):
    '''
    Lazy multiplication of two numbers.
    '''
    return a * b

@lazy
def lazy_add3(a, b, c):
    '''
    Lazy addition of three numbers.
    '''
    return a + b + c

@lazy
def lazy_add4(a, b, c, p=5):
    '''
    Lazy addition of four numbers.
    '''
    return a + b + c + p

@lazy
def check_length(a, b):
    '''
    Lazy length comparison.
    '''
    return len(a) == len(b)

# TESTS FOR LAZY ADDITION/MULTIPLICATION FUNCTIONS -->

assert isinstance(lazy_add(1, 2), LazyOperation) == True

thunk = lazy_add(1, 2)
assert thunk.eval()  == 3

thunk = lazy_mul(3, 4)
assert thunk.eval() == 12

thunk = lazy_mul(lazy_add(1, 2), 4)
assert thunk.eval() == 12

thunk = lazy_add3(1, 2, 3)
assert thunk.eval() == 6

thunk = lazy_add4(1, 2, 3)
assert thunk.eval() == 11

thunk = lazy_mul(25, 5)
assert isinstance(thunk, LazyOperation) == True

# TESTS FOR LAZY TIME SERIES FUNCTIONS -->

ts1 = TimeSeries(range(0, 4), range(1, 5))
ts2 = TimeSeries(range(1, 5), range(2, 6))

thunk = check_length(ts1, ts2)

assert thunk.eval() == True

assert ts1 == ts1.lazy.eval()
assert ts2 == ts2.lazy.eval()
