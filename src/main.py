# main file, run test cases here
# Last modified: 2/21 (based on 2/19 lab)

# general modules
import numpy as np

# project specific modules
from TimeSeries import *

def testBasicFunctionality():
    '''
    Test cases for basic time series functionality.
    '''

    # Create a non-uniform TimeSeries instance:
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    # Set the value at time 2.5
    a[2.5] == 0.5
    # Set the value at time 1.5
    a[1.5] = 2.5

    # a[0] = 3  should give key error

    # Initializing for the next tests
    a = TimeSeries(t, v)

    # get item
    if a[2.5] != 0.5:
        return False

    # contain
    if not a.__contains__(1):
        print (1)
        return False
    if a.__contains__(3):
        print (2)
        return False

    #len
    if len(a) != 5:
        print (3)
        return False

    # iter
    for i, vals in enumerate(a):
        if vals != v[i]:
            print (4)
            return False

    # str (short)
    if str(a) != '[0.0, 2.0, -1.0, 0.5, 0.0]':
        print (5)
        return False

    # str (long)
    t = [1, 1.5, 2, 2.5, 10, 11, 12]
    v = [0, 2, -1, 0.5, 0, 3, 0.7]
    a = TimeSeries(t, v)
    if str(a) != 'Length: {} [0.0, ..., 0.7]'.format(len(a)):
        print (6)
        print (str(a))
        return False

    # return sequence of times, values, items
    if np.sum(a.times() != [1, 1.5, 2, 2.5, 10, 11, 12]) > 0:
        print (7)
        return False

    if np.sum(a.values() != [0, 2, -1, 0.5, 0, 3, 0.7]) > 0:
        print (8)
        return False

    if np.sum(a.items() != [(1, 0), (1.5, 2), (2, -1),
                            (2.5, 0.5), (10, 0), (11, 3), (12, 0.7)]) > 0:
        print (9)
        return False

    # interpolation
    a = TimeSeries([0, 5, 10], [1, 2, 3])
    b = TimeSeries([2.5, 7.5], [100, -100])
    if (a.interpolate([1]) != TimeSeries([1], [1.2])):
        print (10)
        print (a.interpolate([1]))
        return False
    if (a.interpolate(b.times()) != TimeSeries([2.5, 7.5], [1.5, 2.5])):
        print (11)
        return False
    if (a.interpolate([-100, 100]) != TimeSeries([-100, 100], [1, 3])):
        print (12)
        return False

    # iter
    t = [1, 1.5, 2, 2.5, 10]
    v = [0, 2, -1, 0.5, 0]
    a = TimeSeries(t, v)
    for i, (time, val) in enumerate(a.iteritems()):
        if val != v[i] or time != t[i]:
            print (13)
            return False
    for i, val in enumerate(a.itervalues()):
        if val != v[i]:
            print (14)
            return False
    for i, time in enumerate(a.itertimes()):
        if time != t[i]:
            print (15)
            return False

    # median & mean
    if a.mean() != 0.3:
        return False

    if a.median() != 0:
        return False

    return True

def runTestCases():
    '''
    Runs all test cases.
    '''

    print('Running test cases...')

    if not testBasicFunctionality():
        print('Tests for basic functionality failed!')
    else:
        print('Tests for basic functionality passed.')

def main():
    runTestCases()

if __name__ == '__main__':
    main()
