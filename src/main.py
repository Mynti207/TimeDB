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
def runTestCases():
	print('running test cases...')
	if not testBasicFunctionality():
		print('test case basic functionality failed!')
	else:
		print('basic functionality, pass')

def main():
	runTestCases()

if __name__ == "__main__":
    main()