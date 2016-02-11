import numpy as np

from TimeSeries import *


class ArrayTimeSeries(TimeSeries):
    """docstring for ArrayTimeSeries"""
    def __init__(self, seq):
        super(ArrayTimeSeries, self).__init__(seq)
        self.__series = np.array(seq)
        