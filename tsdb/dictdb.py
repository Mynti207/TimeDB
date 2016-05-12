import collections
from collections import defaultdict
from .isax import *

import operator

# dictionary that maps operator functions, useful for select operations
OPMAP = {
    '<': operator.lt,
    '>': operator.gt,
    '==': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge
}


def metafiltered(d, schema, fieldswanted=[]):
    '''
    Helper function for upserting metadata.

    Parameters
    ----------
    d : dictionary
        Metadata to upsert
    schema : dictionary
        Database schema, determines which metadata fields to use
    fieldswanted : list
        The fields to extract from the d dictionary

    Returns
    -------
    d2 : dictionary
        Filtered, converted metadata
    '''

    # initialize new blank dictionary
    d2 = {}

    # if no fields are specified, use all the dictionary fields except 'ts'
    # (i.e. time series data)
    if len(fieldswanted) == 0:
        keys = [k for k in d.keys() if k != 'ts']

    # otherwise use the specified fields, as long as they are in the dictionary
    else:
        keys = [k for k in d.keys() if k in fieldswanted]

    # loop through each of the selected fields
    for k in keys:
        # if the field is in the schema, add the (converted) metadata
        # to the output dictionary
        if k in schema:
            d2[k] = schema[k]['convert'](d[k])

    # returns the new dictionary
    return d2


class DictDB:

    '''
    In-memory, dictionary based database.
    '''

    def __init__(self, schema, pkfield):
        '''
        Initializes the DictDB class.

        Parameters
        ----------
        schema : dictionary
            Specifies the fields to be included in the database and their
            formats
        pkfield : string
            Specifies the name of the primary key field

        Returns
        -------
        An initialized DictDB object
        '''

        # an inverse-lookup dictionary
        # keys = indexed fields
        # values = index for each field
        # blank at initialization
        self.indexes = {}

        # the rows of the database
        # implemented as a dictionary with primary keys as keys
        # blank at initialization
        self.rows = {}

        # specifies the fields to be included in the database
        # and their formats
        self.schema = schema

        # specifies the name of the primary key field
        self.pkfield = pkfield

        # loop through each field in the schema
        for s in schema:
            # if the field is an index field, add in to the index dictionary
            if schema[s]['index'] is not None:
                self.indexes[s] = defaultdict(set)

        # initializes file structure for isax tree
        self.fs = TreeFileStructure()

        # initializes isax tree
        # includes pointer to file structure
        self.tree = iSaxTree('root')

        # triggers (originally stored server-side)
        self.triggers = defaultdict(list)

    def insert_vp(self, pk):
        '''
        Adds a vantage point (i.e. an existing time series) to the database.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry

        Returns
        -------
        dix : string
            ID of the field that will store the distance to the vantage point
        pk : string
            The primary key of the vantage point
        ts : TimeSeries
            The time series data of the vantage point
        '''

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # check that the primary key is present in the database
        if pk not in self.rows:
            raise ValueError('Primary key not present.')
        if self.rows[pk]['deleted'] is True:
            raise ValueError('Primary key has been deleted.')

        # check that the primary key is not already set as a vantage point
        if pk in self.indexes['vp'][True]:
            raise ValueError('Primary key is already set as vantage point.')

        # mark time series as vantage point
        self.rows[pk]['vp'] = True

        # get field name for distance to vantage point
        didx = 'd_vp_' + pk

        # add distance field to schema
        self.schema[didx] = {'convert': float, 'index': 1}

        # update inverse-lookup index dictionary
        self.index_bulk()

        # fields for additional server-side operations:
        # add trigger to calculate distance when a new time series is added
        # calculate distance for all existing time series
        return didx, pk, self.rows[pk]['ts']

    def delete_vp(self, pk, raise_error=True):
        '''
        Unmarks a time series as a vantage point.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the new database entry
        raise_error : boolean
            Determines whether a ValueError is raised when trying to unmark
            a time series that is not actually marked as a vantage point.
            Used when deleting time series, to check whether it needs to
            also be unmarked.

        Returns
        -------
        dix : string
            ID of the field that previously stored the distance to the
            vantage point
        '''

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            if raise_error:
                raise ValueError('Primary key is not a hashable type.')
            else:
                return

        # check that the primary key is present in the database
        if pk not in self.rows:
            if raise_error:
                raise ValueError('Primary key not present.')
            else:
                return
        if self.rows[pk]['deleted'] is True:
            if raise_error:
                raise ValueError('Primary key has been deleted.')
            else:
                return

        # check that the primary key is set as a vantage point
        if pk not in self.indexes['vp'][True]:
            if raise_error:
                raise ValueError('Primary key is not set as a vantage point.')
            else:
                return

        # remove time series marker as vantage point and update index
        self.rows[pk]['vp'] = False
        self.update_indices(pk, prev_meta={'vp': True})

        # get vantage point id
        didx = 'd_vp_' + pk

        # delete from schema
        del self.schema[didx]

        # update inverse-lookup index dictionary
        del self.indexes[didx]

        # remove previously calculated distances from database
        for r in self.rows:
            if self.rows[r]['deleted'] is False:
                del self.rows[r][didx]

        # additional server-side operation:
        # remove trigger to calculate distance when a new time series is added
        return didx

    def insert_ts(self, pk, ts):
        '''
        Inserts a time series into the database.

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

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # check if the primary key is present in the database
        if pk not in self.rows:
            # if not present, create a new entry
            self.rows[pk] = {'pk': pk}
        else:
            # if present, raise an error
            raise ValueError('Duplicate primary key found during insert')

        # add the time series to new database entry
        self.rows[pk]['ts'] = ts

        # initialize the tombstone to False
        self.rows[pk]['deleted'] = False

        # update inverse-lookup index dictionary
        self.update_indices(pk)

        # insert into isax tree
        try:
            self.tree.insert(ts.values(), tsid=pk, fs=self.fs)
        except ValueError:
            return ValueError('Not compatible with tree structure.')

    def delete_ts(self, pk):
        '''
        Marks a time series as deleted.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the entry to be deleted

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # if not present, raise an error
        if pk not in self.rows:
            raise ValueError('Primary key not found during deletion.')
        if self.rows[pk]['deleted'] is True:
            raise ValueError('Primary key has already been deleted.')

        # delete from isax tree
        try:
            self.tree.delete(self.rows[pk]['ts'].values(), fs=self.fs)
        except ValueError:
            return ValueError('Not compatible with tree structure.')

        # mark as deleted
        self.rows[pk]['deleted'] = True

        # rename to avoid key clashes
        new_pk = '0DELETED_' + pk
        while new_pk in self.rows:
            idx, new_pk = int(new_pk[:1]), new_pk[1:]
            new_pk = str(idx + 1) + new_pk
        self.rows[new_pk] = self.rows.pop(pk)

        # update inverse-lookup index dictionary
        for s in self.schema:
            if self.schema[s]['index'] is not None:
                if s == 'deleted':
                    self.indexes['deleted'][False].remove(pk)
                    self.indexes['deleted'][True].add(new_pk)
                else:
                    for val in self.indexes[s]:
                        if pk in self.indexes[s][val]:
                            self.indexes[s][val].remove(pk)
                            self.indexes[s][val].add(new_pk)

    def upsert_meta(self, pk, meta):
        '''
        Upserts (inserts/updates) metadata for a database entry. Requires
        that the metadata fields are in the schema.

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

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # if not present, raise an error
        if pk not in self.rows:
            raise ValueError('Primary key not found during insert')
        if self.rows[pk]['deleted'] is True:
            raise ValueError('Primary key has been deleted.')

        # extract the rows corresponding to the primary key
        row = self.rows[pk]

        # use helper function to extract metadata for upsertion
        mf = metafiltered(meta, self.schema)

        # add filtered metadata and remember the previous meta updated
        prev_meta = {}
        for field in mf:
            # Case old value present for the upserted field
            if field in row:
                prev_meta[field] = row[field]
            row[field] = mf[field]

        # update inverse-lookup index dictionary
        self.update_indices(pk, prev_meta=prev_meta)

    def add_trigger(self, onwhat, proc, storedproc, arg, target):
        '''
        Adds a trigger (similar to an event loop in asynchronous programming,
        i.e. will take some action when a certain event occurs.)

        Parameters
        ----------
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        proc : string
            Name of the module in procs with a coroutine that defines the
            action to take when the trigger is met
        storedproc : function
            Function that defines the action to take when the trigger is met
        arg : string
            Possible additional arguments for the function
        target : string
            Array of field names to which to apply the results of the coroutine

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # assumes all error-checking has been done at server-level
        self.triggers[onwhat].append((proc, storedproc, arg, target))

    def remove_trigger(self, proc, onwhat, target):
        '''
        Removes a previously-set trigger.

        Parameters
        ----------
        proc : string
            Name of the module in procs that defines the trigger action
        onwhat : string
            Operation that triggers the coroutine (e.g. 'insert_ts')
        target : string
            Field name where coroutine result will be stored

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # delete all triggers associated with the action and coroutine
        if target is None:

            # look up all triggers associated with that operation
            trigs = self.triggers[onwhat]

            # keep track of number of triggers removed
            removed = 0

            # remove all instances of the particular coroutine associated
            # with that operation
            for t in trigs:
                if t[0] == proc:
                    trigs.remove(t)
                    removed += 1

            # confirm that at least one trigger has been removed
            if removed == 0:
                raise ValueError('No triggers removed.')

        # only remove a particular trigger
        # (used to delete vantage point representation)
        else:

            # look up all triggers associated with that operation
            trigs = self.triggers[onwhat]

            # delete the relevant trigger
            for t in trigs:
                if t[0] == proc:  # matches coroutine
                    if t[3] == target:  # matches target
                        trigs.remove(t)

    def index_bulk(self, pks=[]):
        '''
        Updates inverse-lookup index dictionary for all database entries
        (default), or for multiple primary keys.

        Parameters
        ----------
        pks : list
            List of primary keys, specifies the database entries for which
            to update the inverse-lookup index dictionary.
            Default = all primary keys.

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # if unspecified, update for all database entries
        if len(pks) == 0:
            pks = self.rows

        # loop through and update indices for all relevant entries
        for pkid in pks:
            # update indices
            self.update_indices(pkid)

    def update_indices(self, pk, prev_meta=None):
        '''
        Updates inverse-lookup index dictionary for a given database entry.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the database entry
        prev_meta: dictionary of metadata
            Previous values, need to be removed from the indices

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # Remove indices associated to the prev_meta
        if prev_meta is not None:
            # check that prev_meta is a dictionary
            if not isinstance(prev_meta, dict):
                raise ValueError('Prev_meta need to be a dictionary instead of {}'.format(type(prev_meta)))
            # Remove indices associated to prev_meta
            self.remove_indices(pk, prev_meta)

        # extract data for the given primary key
        row = self.rows[pk]

        # loop through the data fields, and add to the inverse-lookup
        # dictionary if the field has an index value
        for field, value in row.items():
            if self.schema[field]['index'] is not None:
                if field not in self.indexes:
                    self.indexes[field] = defaultdict(set)
                idx = self.indexes[field]
                idx[value].add(pk)

    def remove_indices(self, pk, row):
        '''
        Updates inverse-lookup index dictionary for a database entry deletion.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the former database entry
        row : dictionary
            The time series and metadata for the deleted entry

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # check that pk is a hashable type
        if not isinstance(pk, collections.Hashable):
            raise ValueError('Primary key is not a hashable type.')

        # loop through the data fields, and remove from the inverse-lookup
        # dictionary if the field has an index value
        for field, value in row.items():
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[value].remove(pk)
                # Remove the node if now empty
                if len(idx[value]) == 0:
                    idx.pop(value)

    def select(self, meta, fields, additional):
        '''
        Select database entries based on specified criteria.

        Parameters
        ----------
        meta : dictionary
            Criteria to apply to metadata
        fields : list
            List of fields to return
        additional : dictionary
            Additional criteria, e.g. apply sorting

        Returns
        -------
        (pks, matchedfielddicts) : (list, dictionary)
            Selected primary keys; entire selected data
        '''

        # start with the set of all primary keys
        pks = set(self.rows.keys())

        # remove those that have been deleted
        not_deleted = self.indexes['deleted'][False]
        pks = pks.intersection(not_deleted)

        # loop through each specified metadata criterion
        for field, value in meta.items():

            # check if the field is in the schema
            if field in self.schema:

                # look up the conversion operator for that field
                conversion = self.schema[field]['convert']

                # case 1: the metadata criterion is a dictionary
                if isinstance(value, dict):

                    # loop through each sub-criterion
                    for op in value:

                        # identify the operation and the value
                        # e.g. > 2 : the operator is > and the value is 2
                        operation = OPMAP[op]
                        val = conversion(value[op])

                        # identify the entries that meet the sub-criterion
                        filtered_pks = set()
                        for i in self.indexes[field].keys():
                            if operation(i, val):
                                filtered_pks.update(self.indexes[field][i])

                        # update the set of primary keys by applying an
                        # AND with the filtered primary keys
                        pks = pks.intersection(filtered_pks)

                # case 2: the metadata criterion is a list
                elif isinstance(value, list):

                    # convert the values to the appropriate type
                    converted_values = [conversion(v) for v in value]

                    # if the index is present
                    if field in self.indexes:
                        selected = set()
                        for v in converted_values:
                            selected.update(self.indexes[field][v])

                    # if the index is not present
                    else:
                        selected = set([pk for pk in pks
                                        if field in self.rows[pk] and
                                        self.rows[pk][field]
                                        in converted_values])

                    # update the set of primary keys by applying an
                    # AND with the selected primary keys
                    pks = pks.intersection(selected)

                # case 3: the metadata criterion is a precise value
                elif isinstance(value, (int, float, str)):

                    # if the index is present
                    if field in self.indexes:
                        selected = set(self.indexes[field][conversion(value)])

                    # if the index is not present
                    else:
                        selected = set([pk for pk in pks
                                        if field in self.rows[pk] and
                                        self.rows[pk][field] ==
                                        conversion(value)])

                    # update the set of primary keys by applying an
                    # AND with the selected primary keys
                    pks = pks.intersection(selected)

                # case 4: some other incorrect type - return nothing
                else:
                    pks = set()

        # convert the remaining (selected) primary key ids to a list
        pks = list(pks)

        # check if additional parameters have been specified
        if additional is not None:

            # sort the return values
            if 'sort_by' in additional:

                # sort format
                # +: ascending, -: descending
                # assume ascending order if unspecified
                sort_type = additional['sort_by'][:1]
                if sort_type == '+' or sort_type == '-':
                    predicate = additional['sort_by'][1:]
                else:
                    predicate = additional['sort_by'][:]
                reverse = True if sort_type == '-' else False

                # sanity check
                if (predicate not in self.schema or
                        (predicate not in self.indexes and
                            predicate != self.pkfield)):
                    raise ValueError('Additional field {} not in schema or in '
                                     'indexes'.format(predicate))

                # in-place sorting
                pks.sort(key=lambda pk: self.rows[pk][predicate],
                         reverse=reverse)

                # limit the number of return values
                # assume this only applies when sorting, e.g. return the top 10
                if 'limit' in additional:
                    pks = pks[:additional['limit']]

        # extract the relevant sub-set of fields
        if fields is None:  # no sub-set is specified
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k != 'ts' and k != 'deleted'}
                                     for pk in pks]  # remove ts
            else:
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k in fields} for pk in pks]

        # return output of select statament
        return pks, matchedfielddicts
