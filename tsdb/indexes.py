import pickle
import os

from bintrees import FastAVLTree


class Index:
    '''
    Generic class for index object
    '''

    def __init__(self, field, directory):
        self.field = field
        self.directory = directory
        self.file = self.directory + '_' + self.field + '.idx'
        if os.path.exists(self.file):
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)
        else:
            self.index = {}

    def __getitem__(self, key):
        return self.index[key]

    def __setitem__(self, key, value):
        self.index[key] = value
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

    def __contains__(self, key):
        return key in self.index.keys()

    def keys(self):
        return self.index.keys()

    def values(self):
        return self.index.values()

    def items(self):
        return self.index.items()

    def commit(self):
        '''
        Commit state to disk.
        '''
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.index, fd)

    def add_field(self, field):
        self.index[field] = set()

    def add_pk(self, pk, value):
        self.index[value].add(pk)
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

class PrimaryIndex(Index):
    '''
    Primary index classes using dictionary {'pk': offset}
    '''

    def __init__(self, field, directory):
        super().__init__(field, directory)

    def remove_pk(self, pk):
        '''
        Remove pk in the index.
        '''
        self.index.pop(pk)
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()


# TODO

class BinTreeIndex(Index):
    '''
    Binary tree to index high cardinality fields.
    use bintrees package https://pypi.python.org/pypi/bintrees/2.0.2.
    For each key use list of values to allow multiple values
    '''

    def __init__(self, field, directory):
        super().__init__(field, directory)

    def remove_pk(self, value, pk):
        '''
        Remove pk for field in the index. Used only to delete element
        from the db, no need to re assign a default to pk for this field.
        '''
        if value in self.index:
            self.index[value].remove(pk)
            # Remove the node if empty
            if len(self.index[value]) == 0:
                self.index.pop(value)
        else:
            raise ValueError('Value {} not present in the index keys'.
                             format(value))
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()


# TODO
class BitMapIndex(Index):
    '''
    Bitmap index for low cardinality fields.
    '''

    def __init__(self, field, directory, values):

        # metadata field (e.g. mean, std, ...)
        self.field = field

        # file locations for persistence
        self.directory = directory
        self.file = self.directory + '_' + self.field + '.idx'
        self.file_pks = self.directory + '_' + self.field + '_pks.idx'

        # load existing index data
        if os.path.exists(self.file):

            # index
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)

            # list of possible values that the field can take
            self.values = self.index.keys()

            # keep track of primary key location {pk: position}
            with open(self.file, "rb", buffering=0) as fd:
                self.pks = pickle.load(fd)

            # list position for next new data
            # can pick any entry because all are the same length
            self.max_position = len(self.index[self.values[0]])

        # create from scratch
        else:

            # index
            self.index = {}

            # list of possible values that the field can take
            self.values = values

            # populate index dictionary for all possible values
            # each dictionary entry will represent a one-hot encoding
            # for all primary keys
            # e.g. if pk1 and pk2 are 1, and pk3 is 2, and pk4 is 3 then
            # {1: [1100], 2: [0010], 3: [0001]}
            for v in self.values:
                self.index[v] = []

            # keep track of primary key location {pk: position}
            self.pks = {}

            # list position for next new data
            self.max_position = 0

    def add_pk(self, pk, value):

        # update value if the primary key is already present
        if pk in self.pks:

            # update binary values
            for v in self.values:
                if v == value:
                    self.index[v][self.pks[pk]] = 1
                else:
                    self.index[v][self.pks[pk]] = 0

        # insert if not already present
        else:

            # update primary key locations
            self.pks[pk] = self.max_position
            # self.inverse_pks[self.max_position] = pk
            self.max_position += 1

            # add binary values
            for v in self.values:
                if v == value:
                    self.index[v].append(1)
                else:
                    self.index[v].append(0)

        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

    def remove_pk(self, value, pk):

        # lazy approach: can result in a lot of dead data

        # # only delete the value from the dictionaries
        # # don't need to worry about deleting the binary codes
        # # del self.inverse_pks[self.pks[pk]]
        # del self.pks[pk]

        # smarter approach :)

        # bitmap position to remove
        idx = self.pks[pk]

        # update primary key offsets
        del self.pks[pk]
        for pk in self.pks:
            if self.pks[pk] > idx:
                self.pks[pk] -= 1

        # update bitmaps
        for v in self.values:
            self.index[v] = self.index[v][:idx] + self.index[v][idx+1:]

        # log reduction in bitmap size
        self.max_position -= 1

        # TODO: add a log to commit the changes by batch and not at each
        # deletion
        self.commit()

    def add_field(self, field):

        # initialize to zero for all primary keys
        self.index[field] = [0] * len(self.pks)
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

    def __getitem__(self, key):

        # build up result
        result = set()

        # loop through primary keys, will exclude deleted data
        for pk in self.pks:
            if self.index[key][self.pks[pk]] == 1:
                result.add(pk)
        return result

    def values(self):
        # TODO
        pass

    def items(self):
        # TODO
        pass

    def commit(self):
        '''
        Commit state to disk.
        '''

        # index
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.index, fd)

        # primary key offsets
        with open(self.file_pks, "wb", buffering=0) as fd:
            pickle.dump(self.pks, fd)
