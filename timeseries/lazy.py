class LazyOperation:

    '''
    Implement lazy evaluation.
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

        templist = list(self.__args)
        for i in range(len(templist)):
            if isinstance(self.__args[i], LazyOperation):
                templist[i] = self.__args[i].eval()
        self.__args = tuple(templist)

        for i in range(len(self.__kwargs)):
            if isinstance(self.__kwargs[i], LazyOperation):
                self.__kwargs[i] = self.__kwargs[i].eval()

        return self.__function(*self.__args, **self.__kwargs)
