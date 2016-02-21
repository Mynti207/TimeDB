# smoke test - 2/8 lab
# Note: no longer compatible with TimeSeries class.

# general modules
import numpy as np

# project specific modules
from TimeSeries import *

def smokeTest():

    threes = TimeSeries(range(0,1000,3))
    fives = TimeSeries(range(0,1000,5))

    s = 0
    for i in range(0,1000):
      if i in threes or i in fives:
        s += i

    print('sum', s)

def main():
    smokeTest()

if __name__ == '__main__':
    main()
