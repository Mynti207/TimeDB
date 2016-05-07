from collections import defaultdict

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

    def __init__(self, schema, pkfield, verbose=False):
        '''
        Initializes the DictDB class.

        Parameters
        ----------
        schema : dictionary
            Specifies the fields to be included in the database and their
            formats
        pkfield : string
            Specifies the name of the primary key field
        verbose : boolean
            Determines whether status updates are printed.

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

        # whether status updates are printed
        self.verbose = verbose

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
        # check if the primary key is present in the database
        if pk not in self.rows:
            # if not present, create a new entry
            self.rows[pk] = {'pk': pk}
        else:
            # if present, raise an error
            raise ValueError('Duplicate primary key found during insert')

        # add the time series to new database entry
        self.rows[pk]['ts'] = ts

        # update inverse-lookup index dictionary
        self.update_indices(pk)

    def delete_ts(self, pk):
        '''
        Deletes a time series (and all associated metadata) from the database.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the entry to be deleted

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # if not present, raise an error
        if pk not in self.rows:
            raise ValueError('Primary key not found during insert')

        # temperarily store the database entry, for use in updating the indices
        row = self.rows[pk]

        # delete the time series from the database
        del self.rows[pk]

        # update inverse-lookup index dictionary
        self.remove_indices(pk, row)

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

        # insert the primary key if it is not already present
        if pk not in self.rows:
            self.rows[pk] = {'pk': pk}

        # extract the rows corresponding to the primary key
        row = self.rows[pk]

        # use helper function to extract metadata for upsertion
        mf = metafiltered(meta, self.schema)

        # add filtered metadata
        for field in mf:
            row[field] = mf[field]

        # update inverse-lookup index dictionary
        self.update_indices(pk)

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
            self.update_indices(pkid)

    def update_indices(self, pk):
        '''
        Updates inverse-lookup index dictionary for a given database entry.

        Parameters
        ----------
        pk : any hashable type
            Primary key for the database entry

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # extract data for the given primary key
        row = self.rows[pk]

        # loop through the data fields, and add to the inverse-lookup
        # dictionary if the field has an index value
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].add(pk)

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

        # loop through the data fields, and remove from the inverse-lookup
        # dictionary if the field has an index value
        for field in row:
            v = row[field]
            if self.schema[field]['index'] is not None:
                idx = self.indexes[field]
                idx[v].remove(pk)

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
                # 0: ascending, 1: descending
                predicate = additional['sort_by'][1:]
                reverse = 0 if additional['sort_by'][0] == '-' else 1

                # sanity check
                if (predicate not in self.schema or
                        predicate not in self.indexes):
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
            if self.verbose: print('S> D> NO FIELDS')
            matchedfielddicts = [{} for pk in pks]
        else:
            if not len(fields):
                if self.verbose: print('S> D> ALL FIELDS')
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k != 'ts'} for pk in pks]  # remove ts
            else:
                if self.verbose: print('S> D> FIELDS {}'.format(fields))
                matchedfielddicts = [{k: v for k, v in self.rows[pk].items()
                                      if k in fields} for pk in pks]

        # return output of select statament
        return pks, matchedfielddicts
