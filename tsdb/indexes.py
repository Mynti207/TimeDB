import pickle
import os

from bintrees import FastAVLTree


class Index:
    '''
    Generic class for index object
    '''

    def __init__(self, field, directory):
        self.field = field
        self.file = directory + '_' + field + '.idx'
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
            raise ValueError('Value {} not present in the index keys'.format(value))
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()




# TODO
# class BitMapIndex: