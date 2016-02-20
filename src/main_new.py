# main file, run test cases here

# general modules
import numpy as np

# project specific modules
from TimeSeries2 import *

# test cases
def testBasicFunctionality():
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

    # Contain
    if not a.__contains__(1):
        print (1)
        return False
    if a.__contains__(3):
        print (2)
        return False

    # Len
    if len(a) != 5:
        print (3)
        return False

    # Iter
    for i, vals in enumerate(a):
        if vals != v[i]:
            print (4)
            return False

    # str
    if str(a) != '[0.0, 2.0, -1.0, 0.5, 0.0]':
        print (5)
        return False
    # long case
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

    return True

# # routine to perform testcases
# def testStringRepresentation(myclass):
#     # create simple numpy sequence
#     seq_short = np.arange(0, 5)
#     ts_short = myclass(seq_short)

#     seq_large = np.arange(0, 10)
#     ts_large = myclass(seq_large)

#     string_ts_short = str(ts_short)
#     string_ts_large = str(ts_large)

#     if string_ts_short != '[0, 1, 2, 3, 4]':
#         return False

#     if string_ts_large != 'Length: {} \n[0, ..., 9]'.format(len(ts_large)):
#         return False

#     return True


def runTestCases():
    print('running test cases...')

    if not testBasicFunctionality():
        print('test case basic functionality failed!')
    else:
        print('basic functionality, pass')
    # if not testStringRepresentation(TimeSeries):
    #     print('test case string representation failed for TimeSeries!')
    # else:
    #     print('string representation for TimeSeries, pass')
    # if not testStringRepresentation(ArrayTimeSeries):
    #     print('test case string representation failed for ArrayTimeSeries!')
    # else:
    #     print('string representation for ArrayTimeSeries, pass')


def main():
    runTestCases()

if __name__ == "__main__":
    main()
