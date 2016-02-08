# stores TimeSeries class

class TimeSeries:

	# initialize TimeSeries with a sequence object
	def __init__(self, seq):
		self.__series = seq


	@property
	def series(self):
	    return self.__series

	def __len__(self):
		return len(self.series)

	def __getitem__(self, key):
		return self.__series[key]

	def __setitem__(self, key, item):
		self.__series[key] = item

