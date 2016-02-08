# stores TimeSeries class


class TimeSeries:
    """ Data and methods for an object representing a general time series.
    
    Attributes:
        series: sequence representing time series data

    Methods: 
        len: returns the length of the sequence
        getitem: given an index (key), return the item of the sequence
        setitem: given an index (key), assign item to corresponding element 
                 of the sequence
        str: returns printable representation of sequence
        resp: returns representation of sequence
    """

    # initialize TimeSeries with a sequence object
    def __init__(self, seq):
        """ Initializes a TimeSeries instance with a given sequence.

        Take as an argument a sequence object representing initial data 
        to fill the time series instance with. This argument can be any 
        object that can be treated like a sequence.
        """
        self.__series = seq

    @property
    def series(self):
        """ Sequence representing time series data. """
        return self.__series

    def __len__(self):
        """ Returns the length of the sequence. """
        return len(self.series)

    def __getitem__(self, key):
        """ Takes key as input and returns corresponding item in sequence. """
        return self.__series[key]

    def __setitem__(self, key, item):
        """ Take a key and item, and assigns item to element of sequence 
        at position identified by key. """
        self.__series[key] = item

    def __str__(self):
        """ Returns a printable representation of sequence.

        If the sequence has more than five items, return the length and the
        first/last element; otherwise, return the sequence.
        """
        n = len(self)
        if n > 5:
            return('Length: {} \n[{}, ..., {}]'.format(n, self[0], self[-1]))
        else:
            return(str(self.__series))

    def __repr__(self):
        """ Identifies object as a TimeSeries and returns representation 
        of sequence.

        If the sequence has more than five items, return the first/last
        element, otherwise return the sequence.
        """
        n = len(self)
        if n > 5:
            res = '[{}, ..., {}]'.format(self[0], self[-1])
        else:
            res = str(self.__series)
        return 'TimeSeries({})'.format(res)

