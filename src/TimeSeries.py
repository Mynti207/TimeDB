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

    def __str__(self):
        n = len(self)
        if n > 5:
            return('Length: {} \n[{}, ..., {}]'.format(n, self[0], self[-1]))
        else:
            return(str(self.__series))

    def __repr__(self):
        n = len(self)
        if n > 5:
            res = '[{}, ..., {}]'.format(self[0], self[-1])
        else:
            res = str(self.__series)
        return 'TimeSeries({})'.format(res)

