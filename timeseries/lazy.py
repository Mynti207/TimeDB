class LazyOperation:

    '''
    Implement lazy evaluation.
    '''

    def __init__(self, function, *args, **kwargs):
        '''
        Initialize container for an arbitrary function.
        '''
        self.__function = function
        self.__args = args
        self.__kwargs = kwargs

    def eval(self):
        '''
        Evaluate a Lazy object recursively.
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
