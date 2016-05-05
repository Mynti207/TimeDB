from timeseries import TimeSeries
from .tsdb_error import TSDBStatus


class TSDBOp(dict):
    '''
    Interface class for TSDB network operations. Used to document the database
    operations supported, including their "string name" in the typemap, and
    conversion to and from a json-like dictionary structure.
    Note: This class defines basic functionality, and is then inherited by
    sub-classes for specific database operations.
    '''

    def __init__(self, op):
        '''
        Initializes the TSDBOp class.

        Parameters
        ----------
        op : string
            Specifies the type of operation

        Returns
        -------
        An initialized TDSBOp object
        '''
        self['op'] = op

    def to_json(self, obj=None):
        '''
        Recursively converts elements in a hierarchical data structure into
        a json-encodable form. Only handles class instances if they have a
        to_json method.

        Parameters
        ----------
        obj : iterator or other hierarchical data structure
            Collection of elements to recursively convert into JSON-encodable
            format.

        Returns
        -------
        Dictionary of json-encoded elements
        '''

        # apply to self if not specified
        if obj is None:
            obj = self

        # initialize return dictionary
        json_dict = {}

        # return object if we are at the bottom of the recursion
        if isinstance(obj, str) or not hasattr(obj, '__len__') or obj is None:
            return obj

        # recursively convert into json format, based on object type
        for k, v in obj.items():
            if isinstance(v, str) or not hasattr(v, '__len__') or v is None:
                json_dict[k] = v
            elif isinstance(v, TSDBStatus):
                json_dict[k] = v.name
            elif isinstance(v, list):
                json_dict[k] = [self.to_json(i) for i in v]
            elif isinstance(v, dict):
                json_dict[k] = self.to_json(v)
            elif hasattr(v, 'to_json'):
                json_dict[k] = v.to_json()
            else:
                raise TypeError('Cannot convert object to JSON: '+str(v))

        # return overall result
        return json_dict

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''

        # check that the operation is present in the dictionary to covert
        if 'op' not in json_dict:
            raise TypeError('Not a TSDB Operation: ' + str(json_dict))

        # check that the operation is present in the dictionary of tsdb
        # network operations
        if json_dict['op'] not in typemap:
            raise TypeError('Invalid TSDB Operation: ' + str(json_dict['op']))

        # apply relevant class method
        return typemap[json_dict['op']].from_json(json_dict)


class TSDBOp_InsertTS(TSDBOp):
    '''
    TSDB network operation: inserts a time series into the database.
    '''

    def __init__(self, pk, ts):
        '''
        Initializes the class.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry
        ts : TimeSeries
            Time series to be inserted into the database.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('insert_ts')
        self['pk'], self['ts'] = pk, ts

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.
        Note: should not be used for return operation.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['pk'], TimeSeries(*(json_dict['ts'])))


class TSDBOp_Return(TSDBOp):
    '''
    TSDB network operation: returns the result of running a database operation.
    '''

    def __init__(self, status, op, payload=None):
        '''
        Initializes the class.

        Parameters
        ----------
        status : int
            One of four database status codes (see tsdb_ops)
        payload : dictionary
            Result of database operation.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__(op)
        self['status'], self['payload'] = status, payload

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.
        Note: should not be used for return operation.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['status'], json_dict['payload'])


class TSDBOp_UpsertMeta(TSDBOp):
    '''
    TSDB network operation: upserts metadata for a database entry.
    '''

    def __init__(self, pk, md):
        '''
        Initializes the class.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the  database entry
        meta : dictionary
            Metadata to be upserted into the database.

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('upsert_meta')
        self['pk'], self['md'] = pk, md

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['pk'], json_dict['md'])


class TSDBOp_Select(TSDBOp):
    '''
    TSDB network operation: selects database entries based on specified
    criteria.
    '''

    def __init__(self, md, fields, additional):
        '''
        Initializes the class.

        Parameters
        ----------
        md : dictionary
            Criteria to apply to metadata
        fields : list
            List of fields to return
        additional : dictionary
            Additional criteria, e.g. apply sorting

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('select')
        self['md'] = md
        self['fields'] = fields
        self['additional'] = additional

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['md'],
                   json_dict['fields'],
                   json_dict['additional'])


class TSDBOp_AddTrigger(TSDBOp):
    '''
    TSDB network operation: adds a trigger (similar to an event loop in
    asynchronous programming - i.e. will take some action when a certain
    event occurs.)
    '''

    def __init__(self, proc, onwhat, target, arg):
        '''
        Initializes the class.

        Parameters
        ----------
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        target : string
            Array of field names to which to apply the results of the coroutine
        arg : string
            Possible additional arguments ('sort_by' and 'order')

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('add_trigger')
        self['proc'] = proc
        self['onwhat'] = onwhat
        self['target'] = target
        self['arg'] = arg

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['proc'], json_dict['onwhat'], json_dict['target'],
                   json_dict['arg'])


class TSDBOp_RemoveTrigger(TSDBOp):
    '''
    TSDB network operation: removes a previously-set trigger
    '''

    def __init__(self, proc, onwhat):
        '''
        Initializes the class.

        Parameters
        ----------
        proc : string
            Name of the module in procs that defines the trigger action
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('remove_trigger')
        self['proc'] = proc
        self['onwhat'] = onwhat

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['proc'], json_dict['onwhat'])


class TSDBOp_AugmentedSelect(TSDBOp):
    '''
    TSDB network operation: selects database entries based on specified
    criteria, then runs a coroutine.
    Note: result of coroutine is returned to user and is not upserted.
    '''

    """
    A hybrid of select, and add trigger, we only miss the onwhat key as this op
    is used as an add on to selects. We remove the fields arg from select, as
    the only fields sent back are the ones in target, which is used as in
    add_trigger, except that instead of upserting meta with the targets, that
    data is sent back to the user.
    """

    def __init__(self, proc, target, arg, md, additional):
        '''
        Initializes the class.

        Parameters
        ----------
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        target : string
            Array of field names to which to apply the results of the
            coroutine, and to return.
        arg : string
            Possible additional arguments ('sort_by' and 'order')
        metadata_dict : dictionary
            Criteria to apply to metadata
        additional : dictionary
            Additional criteria, e.g. apply sorting (default=None)
        verbose : boolean
            Determines whether status updates are displayed

        Returns
        -------
        Nothing, modifies in-place.
        '''
        super().__init__('augmented_select')
        self['md'] = md
        self['additional'] = additional
        self['proc'] = proc
        self['arg'] = arg
        self['target'] = target

    @classmethod
    def from_json(cls, json_dict):
        '''
        Recover database operation from json-encoded dictionary.

        Parameters
        ----------
        cls : class
            TSDB network operation type
        json_dict : dictionary
            Dictionary for conversion from json format.

        Returns
        -------
        Unencoded database network operation
        '''
        return cls(json_dict['proc'], json_dict['target'], json_dict['arg'],
                   json_dict['md'], json_dict['additional'])


# dictionary of tsdb network operations
# simplifies reconstruction of tsdb operation instances from network data
typemap = {
    'insert_ts':            TSDBOp_InsertTS,
    'upsert_meta':          TSDBOp_UpsertMeta,
    'select':               TSDBOp_Select,
    'augmented_select':     TSDBOp_AugmentedSelect,
    'add_trigger':          TSDBOp_AddTrigger,
    'remove_trigger':       TSDBOp_RemoveTrigger,
}
