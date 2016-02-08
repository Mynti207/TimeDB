# main file, run test cases here

# general modules
import numpy as np

# project specific modules
from TimeSeries import *


# test cases
def testBasicFunctionality():

    # create simple numpy sequence
    seq = np.arange(0, 10)

    ts = TimeSeries(seq)

    # get item
    if ts[1] != 1:
        return False

    # set item
    ts[1] = 0
    if ts[1] != 0:
        return False

    # length
    if len(ts) != 10:
        return False
    return True


# routine to perform testcases
def testStringRepresentation():
    # create simple numpy sequence
    seq_short = np.arange(0, 5)
    ts_short = TimeSeries(seq_short)

    seq_large = np.arange(0, 10)
    ts_large = TimeSeries(seq_large)

    string_ts_short = str(ts_short)
    string_ts_large = str(ts_large)

    if string_ts_short != '[0 1 2 3 4]':
        return False

    if string_ts_large != 'Length: {} \n[0, ..., 9]'.format(len(ts_large)):
        return False

    print('Print statement on short list')
    print(string_ts_short)
    print('Print statement on large list')
    print(string_ts_large)

    print('Requested print for the form')
    print(TimeSeries(range(0, 1000000)))

    return True


def runTestCases():
    print('running test cases...')
    if not testBasicFunctionality():
        print('test case basic functionality failed!')
    else:
        print('basic functionality, pass')
    if not testStringRepresentation():
        print('test case string representation failed!')
    else:
        print('string representation, pass')


def main():
    runTestCases()

if __name__ == "__main__":
    main()
