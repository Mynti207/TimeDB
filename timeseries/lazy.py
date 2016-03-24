class LazyOperation:

    '''
    Implement lazy evaluation.

    Note: no doctests as this class is not intended to be called directly.
    '''

    def __init__(self, function, *args, **kwargs):
        '''
        Initialize container for an arbitrary function.

        Parameters
        ----------
        function : function
            A function to be lazy-fied
        args, kwargs: function arguments
            Arguments of function to be lazy-fied

        Returns
        -------
        function
            A lazy-fied funtion
        '''
        self.__function = function
        self.__args = args
        self.__kwargs = kwargs

    def eval(self):
        '''
        Evaluate a Lazy object recursively.

        Parameters
        ----------
        None

        Returns
        -------
        Varies
            The output of the evaluated lazy-fied function
        '''

        # can't modify args (tuple) in-place
        templist = list(self.__args)
        for key, value in enumerate(templist):
            if isinstance(value, LazyOperation):
                templist[key] = value.eval()
        self.__args = tuple(templist)

        # can modify kwargs (dict) in-place
        for key, value in self.__kwargs.items():
            if isinstance(value, LazyOperation):
                self.__kwargs[key] = value.eval()

        return self.__function(*self.__args, **self.__kwargs)
