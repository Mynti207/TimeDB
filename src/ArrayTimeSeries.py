import numpy as np
from TimeSeries import *


class ArrayTimeSeries(TimeSeries):
	def __init__(self, seq):
		""" Initializes an ArrayTimeSeries instance with a given sequence.
		Take as an argument a sequence object representing initial data 
		to fill the time series instance with. This argument can be any 
		object that can be treated like a sequence. Internally a numpy array
		representation is used to give a speedup compared to a TimeSeries object
		"""
		super(ArrayTimeSeries, self).__init__(seq)
		self.__series = np.array(seq)
        