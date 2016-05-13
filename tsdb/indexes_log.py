import pickle
import os
import copy

from bintrees import FastAVLTree


class Index:
    '''
    Generic class for index object
    '''

    def __init__(self, field, directory):
        '''
        Initializes the Index class.

        Parameters
        ----------
        field : str
            The metadata field name that the index represents
        directory : str
            The directory location where the index file will be saved

        Returns
        -------
        An initialized Index object
        '''

        # intialize index properties
        self.field = field
        self.directory = directory
        self.file = self.directory + self.field + '.idx'

        # load if already present
        if os.path.exists(self.file):
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)

        # otherwise initialize
        else:
            self.index = {}

    def __getitem__(self, key):
        '''
        Returns the primary key values associated with an index key.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Set of primary keys associated with the index key.
        '''
        return self.index[key]

    def __setitem__(self, key, value):
        '''
        Sets an index key equal to a given value (i.e. primary key cluster).
        Set on memory, need to call commit to save on disk.

        Parameters
        ----------
        key : str
            The metadata field value
        value : any type
            The values to associate with the index key. Generally a set
            of primary keys.

        Returns
        -------
        Set of primary keys associated with the index key.
        '''
        self.index[key] = value

    def __contains__(self, key):
        '''
        Checks whether a given key is present in the index.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Whether the key is present in the index.
        '''
        return key in self.index.keys()

    def keys(self):
        '''
        Returns the index keys (i.e. possible metadata values).

        Parameters
        ----------
        None

        Returns
        -------
        List of index keys.
        '''
        return self.index.keys()

    def values(self):
        '''
        Returns the index values (i.e. primary keys associated with metadata).

        Parameters
        ----------
        None

        Returns
        -------
        List of index values.
        '''
        return self.index.values()

    def items(self):
        '''
        Returns the index items (i.e. possible metadata values, and the
        primary keys associated with each of them).

        Parameters
        ----------
        None

        Returns
        -------
        List of index items.
        '''
        return self.index.items()

    def commit(self):
        '''
        Commits state to disk.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.index, fd)

    def add_key(self, key):
        '''
        Adds a new index key (i.e. possible metadata field value) and
        initializes as empty (i.e. primary keys associated with it).

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        self.index[key] = set()

    def remove_key(self, key):
        '''
        Removes an index key (i.e. possible metadata field value) and
        all primary keys associated with it.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        del self.index[key]

    def add_pk(self, key, pk):
        '''
        Adds a primary key to an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # defaultdict behavior
        if key not in self.index:
            self.add_key(key)

        self.index[key].add(pk)

    def remove_pk(self, key, pk):
        '''
        Removes a primary key from an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''
        self.index[key].pop(pk)

    def _erase(self):
        '''
        Erase from disk the index files if exists.
        '''
        if os.path.exists(self.file):
            os.remove(self.file)


class IndexLog(Index):
    '''
    Generic class for index object with log
    '''

    def __init__(self, field, directory):
        '''
        Initializes the PrimaryIndex class.

        Parameters
        ----------
        field : str
            The metadata field name that the index represents
        directory : str
            The directory location where the index file will be saved

        Returns
        -------
        An initialized PrimaryIndex object
        '''
        super().__init__(field, directory)
        self._init_log()

    def commit_log(self):
        '''
        Commits log state to disk.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''
        with open(self.file_log, "wb", buffering=0) as fd:
            pickle.dump(self.log, fd)

    def commit_log_to_index(self):
        '''
        Commit the log into the index.
        Write ahead strategy applied: changes written into a log and commit
        afterwards to disk for persistency

        Parameters
        -------
        Nothing

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # Remove the special key
        self.log.pop('$COMMITED$')

        # Copy log to index on disk
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.log, fd)

        # Copy log to index on memory
        self.index = self.log.copy()

        # Update special key of log on memory and disk
        self.log['$COMMITED$'] = True
        self.commit_log()

    def _init_log(self):
        '''
        Initialize the log attributes, if loaded commit it to the index if
        needed

        Parameters
        -------
        Nothing

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # Filename of the log, will be identical to the index
        self.file_log = self.directory + self.field + '_log.idx'
        if os.path.exists(self.file_log):
            with open(self.file_log, "rb", buffering=0) as fd:
                self.log = pickle.load(fd)
            # Check if log was commited into index before closing last session,
            # else commit it.
            # This will happen in case of previous failure or if some updates
            # have not been commited into index in the previous session
            if not self.log['$COMMITED$']:
                self.commit_log_to_index()
                self.commit()

        # otherwise initialize
        else:
            self.log = {}

    def _erase(self):
        '''
        Erase from disk the index and log files if exists.
        '''
        if os.path.exists(self.file):
            os.remove(self.file)

        if os.path.exists(self.file_log):
            os.remove(self.file_log)


class PrimaryIndex(IndexLog):
    '''
    Primary index classes using dictionary {'pk': offset}
    '''

    def __init__(self, field, directory):
        '''
        Initializes the PrimaryIndex class.

        Parameters
        ----------
        field : str
            The metadata field name that the index represents
        directory : str
            The directory location where the index file will be saved

        Returns
        -------
        An initialized PrimaryIndex object
        '''
        super().__init__(field, directory)
        self._init_log()

    def remove_pk(self, key, pk):
        '''
        Removes a primary key from an index key (i.e. metadata field value).

        Parameters
        ----------
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        del self.log[pk]
        self.commit_log()

        # set change on index
        del self.index[pk]

    def __setitem__(self, key, value):
        '''
        Sets an index key equal to a given value (i.e. primary key cluster).
        Set on memory, need to call commit to save on disk.

        Parameters
        ----------
        key : str
            The metadata field value
        value : any type
            The values to associate with the index key. Generally a set
            of primary keys.

        Returns
        -------
        Set of primary keys associated with the index key.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        self.log[key] = value
        self.commit_log()

        self.index[key] = value

    def add_key(self, key):
        '''
        Adds a new index key (i.e. possible metadata field value) and
        initializes as empty (i.e. primary keys associated with it).

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        self.log[key] = set()
        self.commit_log()

        self.index[key] = set()

    def remove_key(self, key):
        '''
        Removes an index key (i.e. possible metadata field value) and
        all primary keys associated with it.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        del self.log[key]
        self.commit_log()

        del self.index[key]

    def add_pk(self, key, pk):
        '''
        Adds a primary key to an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        # will do the same on index
        if key not in self.log:
            self.add_key(key)
        self.log[key].add(pk)
        self.commit_log()

        self.index[key].add(pk)


class TriggerIndex(IndexLog):
    '''
    Simple dictionary structure for making triggers persistent.
    Defined as index to facilitate commit behavior.
    '''

    def __init__(self, field, directory):
        super().__init__(field, directory)

    def __getitem__(self, key):
        if key in self.index:
            return self.index[key]
        else:
            return []

    def add_key(self, key):
        '''
        Adds a new index key (i.e. possible metadata field value) and
        initializes as empty (i.e. primary keys associated with it).

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        self.log[key] = []
        self.commit_log()

        self.index[key] = []

    def remove_key(self, key):
        '''
        Removes an index key (i.e. possible metadata field value) and
        all primary keys associated with it.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        del self.log[key]
        self.commit_log()

        del self.index[key]

    def add_trigger(self, key, pk):
        '''
        Adds a new trigger. Equivalent to adding a primary key to an index key.

        Parameters
        ----------
        key : str
            Action that triggers the coroutine
        pk : tuple
            Coroutine specifications

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        if key not in self.log:
            self.add_key(key)

        self.log[key].append(pk)
        self.commit_log()

        self.index[key].append(pk)

    def remove_all_triggers(self, key, proc):
        '''
        Removes all triggers associated with a particular database action
        and coroutine.

        Parameters
        ----------
        key : str
            Action that triggers the coroutine
        proc : str
            Coroutine name

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # Persist on log first

        # look up all triggers associated with that operation
        trigs = self.log[key]

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

        self.commit_log()

        # Change index
        # look up all triggers associated with that operation
        trigs = self.index[key]

        # keep track of number of triggers removed
        removed = 0

        # remove all instances of the particular coroutine associated
        # with that operation
        for t in trigs:
            if t[0] == proc:
                trigs.remove(t)
                removed += 1

    def remove_one_trigger(self, key, proc, target):
        '''
        Removes a specific instance of a trigger associated with a particular
        database action and coroutine.

        Parameters
        ----------
        key : str
            Action that triggers the coroutine
        proc : str
            Coroutine name
        target : list
            Database fields that store result of running coroutine

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # log is not commited into index anymore
        self.log['$COMMITED$'] = False

        # persist on the log
        # look up all triggers associated with that operation
        trigs = self.log[key]

        # delete the relevant trigger
        for t in trigs:
            if t[0] == proc:  # matches coroutine
                if t[3] == target:  # matches target
                    trigs.remove(t)
        self.commit_log()

        # look up all triggers associated with that operation
        trigs = self.index[key]

        # delete the relevant trigger
        for t in trigs:
            if t[0] == proc:  # matches coroutine
                if t[3] == target:  # matches target
                    trigs.remove(t)


class BinTreeIndex(Index):
    '''
    Binary tree to index high cardinality fields.
    Uses bintrees package: https://pypi.python.org/pypi/bintrees/2.0.2
    We use a set of values for each key, to allow multiple (but unique) values
    '''

    def __init__(self, field, directory):
        '''
        Initializes the BinTreeIndex class.

        Parameters
        ----------
        field : str
            The metadata field name that the index represents
        directory : str
            The directory location where the index file will be saved

        Returns
        -------
        An initialized BinTreeIndex object
        '''

        # initialize index properties
        self.field = field
        self.directory = directory
        self.file = self.directory + self.field + '.idx'

        # load if already present
        if os.path.exists(self.file):
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)

        # otherwise initialize
        else:
            self.index = FastAVLTree()

    def add_key(self, key):
        '''
        Adds a new index key (i.e. possible metadata field value) and
        initializes as empty (i.e. primary keys associated with it).

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''
        # initialize new field index as an empty set
        # will contain all pks that match this value
        self.index[key] = set()

    def add_pk(self, key, pk):
        '''
        Adds a primary key to an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''
        self.index[key].add(pk)

    def remove_pk(self, key, pk):
        '''
        Removes a primary key from an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''
        self.index[key].discard(pk)

        # clear key if no further primary keys left
        if len(self.index[key]) == 0:
            self.remove_key(key)

    def keys(self):
        '''
        Returns the index keys (i.e. possible metadata values).

        Parameters
        ----------
        None

        Returns
        -------
        List of index keys.
        '''
        return list(self.index.keys())

    def values(self):
        '''
        Returns the index values (i.e. primary keys associated with metadata).

        Parameters
        ----------
        None

        Returns
        -------
        List of index values.
        '''
        return list(self.index.values())

    def items(self):
        '''
        Returns the index items (i.e. possible metadata values, and the
        primary keys associated with each of them).

        Parameters
        ----------
        None

        Returns
        -------
        List of index items.
        '''
        return list(self.index.items())


class BitMapIndex(Index):
    '''
    Bitmap index for low cardinality fields.
    Each index entry represents a one-hot encoding for all primary keys
    e.g. if pk1 and pk2 are 1, and pk3 is 2, and pk4 is 3 then
    {1: '1100', 2: '0010', 3: '0001'}
    '''

    def __init__(self, field, directory, values):
        '''
        Initializes the BitMapIndex class.

        Parameters
        ----------
        field : str
            The metadata field name that the index represents
        directory : str
            The directory location where the index file will be saved
        values : list
            The list of values that the field can take (specified in schema)

        Returns
        -------
        An initialized BitMapIndex object
        '''

        # metadata field (e.g. mean, std, ...)
        self.field = field

        # file locations for persistence
        self.directory = directory
        self.file = self.directory + self.field + '.idx'
        self.file_pks = self.directory + self.field + '_pks.idx'

        # load existing index data
        if os.path.exists(self.file):

            # index
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)

            # list of possible values that the field can take
            self.possible_values = list(self.index.keys())

            # keep track of primary key location {pk: position}
            with open(self.file_pks, "rb", buffering=0) as fd:
                self.pks = pickle.load(fd)

            # list position for next new data
            # can pick any entry because all are the same length
            self.max_position = len(self.index[self.possible_values[0]])

        # create from scratch
        else:

            # index
            self.index = {}

            # list of possible values that the field can take
            self.possible_values = values

            # keep track of primary key location {pk: position}
            self.pks = {}

            # populate index dictionary for all possible values
            for v in self.possible_values:
                self.add_key(v)

            # list position for next new data
            self.max_position = 0

    def add_key(self, key):
        '''
        Adds a new index key (i.e. possible metadata field value) and
        initializes as empty (i.e. primary keys associated with it).

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # initialize to zero for all primary keys
        self.index[key] = '0' * len(self.pks)

    def add_pk(self, key, pk):
        '''
        Adds a primary key to an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # update value if the primary key is already present
        if pk in self.pks:

            # offset position to change
            idx = self.pks[pk]

            # update binary values
            for v in self.possible_values:
                if v == key:
                    self.index[v] = (self.index[v][:idx] + '1' +
                                     self.index[v][idx+1:])
                else:
                    self.index[v] = (self.index[v][:idx] + '0' +
                                     self.index[v][idx+1:])

        # insert if not already present
        else:

            # update primary key locations
            self.pks[pk] = self.max_position
            self.max_position += 1

            # add binary values
            for v in self.possible_values:
                if v == key:
                    self.index[v] += '1'
                else:
                    self.index[v] += '0'

    def remove_pk(self, key, pk):
        '''
        Removes a primary key from an index key (i.e. metadata field value).

        Parameters
        ----------
        key : str
            The metadata field value
        pk : str
            Primary key identifier

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # bitmap position to remove
        idx = self.pks[pk]

        # update primary key offsets
        del self.pks[pk]
        for pk in self.pks:
            if self.pks[pk] > idx:
                self.pks[pk] -= 1

        # update bitmaps
        for v in self.possible_values:
            self.index[v] = self.index[v][:idx] + self.index[v][idx+1:]

        # log reduction in bitmap size
        self.max_position -= 1

    def __getitem__(self, key):
        '''
        Returns the primary key values associated with an index key.

        Parameters
        ----------
        key : str
            The metadata field value

        Returns
        -------
        Set of primary keys associated with the index key.
        '''

        # build up result
        result = set()

        # loop through primary keys, will exclude deleted data
        for pk in self.pks:
            if self.index[key][self.pks[pk]] == '1':
                result.add(pk)
        return result

    def keys(self):
        '''
        Returns the primary key values associated with all index keys.

        Parameters
        ----------
        None

        Returns
        -------
        Index dictionary values (formatted as set of pks)
        '''
        result = [(k, self.__getitem__(k)) for k in self.index.keys()]
        result = [r[0] for r in result if len(r[1]) > 0]
        return result

    def values(self):
        '''
        Returns a list of tuples of all index keys and the primary key values
        associated with them.

        Parameters
        ----------
        None

        Returns
        -------
        Index dictionary items (formatted as set of pks)
        '''
        result = [self.__getitem__(k) for k in self.index.keys()]
        result = [r for r in result if len(r) > 0]
        return result

    def items(self):
        '''
        Returns the primary key values associated with all index keys.

        Parameters
        ----------
        None

        Returns
        -------
        Index dictionary values (formatted as set of pks)
        '''
        result = [(k, self.__getitem__(k)) for k in self.index.keys()]
        result = [r for r in result if len(r[1]) > 0]
        return result

    def commit(self):
        '''
        Commits state to disk.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing, modifies in-place.
        '''

        # index
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.index, fd)

        # primary key offsets
        with open(self.file_pks, "wb", buffering=0) as fd:
            pickle.dump(self.pks, fd)
