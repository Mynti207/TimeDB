import pickle
import os

from bintrees import FastAVLTree


class PrimaryIndex:
    '''
    Primary index classes using dictionary {'pk': offset}
    '''

    def __init__(self, field, directory):
        self.field = field
        self.file = directory + '_' + field + '.idx'
        if os.path.exists(self.file):
            with open(self.file, "rb", buffering=0) as fd:
                self.index = pickle.load(fd)
        else:
            self.index = {}

    def __setitem__(self, key, value):
        self.index[key] = value
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

    def __getitem__(self, key):
        return self.index[key]

    def __contains__(self, key):
        return key in self.index.keys()

    def remove(self, key):
        self.index.pop(key)
        # TODO: add a log to commit the changes by batch and not at each
        # insertion
        self.commit()

    def commit(self):
        '''
        Commit state to disk.
        '''
        with open(self.file, "wb", buffering=0) as fd:
            pickle.dump(self.index, fd)


# TODO

# class BinTreeIndex:


# TODO
# class BitMapIndex: