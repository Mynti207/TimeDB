import numpy as np
import scipy as sp
from scipy import stats

####################
#
# HELPER FUNCTIONS
#
####################


def stand(x):
    '''
    Standardize a data series.

    Input: a 1-D numpy array
    Returns: the 1-D numpy array with values standardized
    '''

    return (x-np.mean(x))/np.std(x, ddof=0)


def get_breakpoints(a):
    '''
    Return list of breakpoints given cardinality a, used in SAX indexing.

    Input: integer a, representing number of breakpoints
    Returns: list of floats representing standard deviations from 0 mean,
    length (a - 1)
    '''

    avec = np.zeros(a - 1)  # 1 less breakpoint than length
    for i in range(a - 1):
        avec[i] = sp.stats.norm.ppf((i + 1)/a, loc=0, scale=1)
    return avec


def get_isax_word(ts, w, a):
    ''' Given a time series, return iSAX word representation.

    Inputs:
    ts: time series
    w: number of chunks to divide time series
    a: cardinality (number of possible index values/levels per chunk)

    Returns: list of strings of length w,
             each string represents a binary number having log2(a) digits

    Notes:
    Unexpected results may occur if cardinality is not 2^n for some n.
    Rounding errors may occur if w does not divide into length of time series
    evenly.
    '''

    # standardize time series
    series = stand(ts)

    # divide series into chunks
    if len(series) >= w:
        lenchunk = int(len(series)/w)
    else:
        # we choose to divide into unit chunks if length of series is not
        # at least w
        lenchunk = 1

    # get averages of each bin
    means = [np.mean(series[int(lenchunk * chunk):int(lenchunk * (chunk + 1))])
             for chunk in range(w)]

    breakpoints = get_breakpoints(a)
    assert len(breakpoints) == (a-1)

    # reverse list so that 0 value is lowest y (most negative on std scale)
    labels = np.arange(a)[::-1]
    assert len(labels) == a

    # convert to SAX code
    sax = np.empty(w, dtype=int)
    for i, item in enumerate(means):
        for j, b in enumerate(breakpoints):
            if item < b:
                sax[i] = int(labels[j])
                break
            elif j == len(breakpoints)-1:
                # that was the last breakpoint, value must be greater
                sax[i] = int(labels[j+1])

    # convert to binary format
    digits = int(np.log2(len(labels)))
    binarysax = [format(item, '0' + str(digits) + 'b') for item in sax]

    return binarysax


def distance(ts1, ts2):
    '''
    Calculates Euclidian distance between two time series.

    Takes time series in the form of two 1-D numpy arrays. Assumes time series
    are of same length.
    Used by `find_nbr` in `iSaxTree` class to compare time series, but other
    distance measure can
    be substituted here.
    '''

    assert len(ts1) == len(ts2)

    d = (ts1 - ts2)**2
    d = d.sum(axis=-1)
    d = np.sqrt(d)

    return d

####################
#
# FILE SYSTEM FUNCTIONS
#
# currently iSAX word (string) is used as the filename;
# these functions store the "files" in memory
#
# Note that the storage structure (alldata) works different from the
# `keyhashes` dictionary for the iSaxTree classes; the former only has an
# iSAX word index if there are existing time series being stored for it
# (e.g. upon deletion of all time series for that word, the entry from
# `alldata` is deleted.  However, in the `keyhashes` dictionary, iSAX words
# are stored for internal nodes (to which data items are not associated) and
# occasionally, for leaf nodes where all the corresponding time series have
# been deleted (i.e. nodes in the iSaxTree are NOT deleted even when they
# are empty.)
#
####################


class TreeFileStructure:

    def __init__(self):
        self.alldata = {}  # can be re-initialized if desired

    def read_file(self, filename):
        # returns list of tuples indexed by filename
        # [(time series as numpy array, time series ID), ...]
        return self.alldata[filename]

    def isax_exists(self, filename):
        # returns True if iSAX word has any time series stored for it
        return filename in self.alldata

    def already_in_file(self, filename, ts):
        # returns True if a specific time series corresponding to an iSAX
        # word is stored with the iSAX word
        if self.isax_exists(filename):
            data = self.read_file(filename)  # read data
            exists = np.any([np.array_equal(item[0], ts) for item in data])
            if exists:
                return True
        return False  # not in file

    def write_to_file(self, filename, ts, tsid=""):
        # assumes already checked not in file
        # write time series to `filename` with optional ID
        if self.isax_exists(filename):
            self.alldata[filename] += [(ts, tsid)]
        else:
            # write new file / entry
            self.alldata[filename] = [(ts, tsid)]

    def delete_from_file(self, filename, ts):
        # deletes time series from storage
        self.alldata[filename] = [item for item in self.alldata[filename]
                                  if not np.array_equal(item[0], ts)]
        if len(self.alldata[filename]) == 0:
            del self.alldata[filename]  # delete if empty


####################
#
# TREE STRUCTURES
#
####################

class BasicTree:
    '''
    Basic n-ary tree class.
    '''

    def __init__(self, data, parent=None):
        self.data = data
        self.parent = parent
        self.child_pointers = []  # stores of pointers to child nodes

    def add_child(self, data, level):
        n = self.__class__(data, self, level)
        self.child_pointers += [n]
        return n

    def num_children(self):
        return len(self.child_pointers)

    def isRoot(self):
        return not self.parent


class iSaxTree(BasicTree):
    '''
    Implements modified version of iSAX tree.

    See: http://www.cs.ucr.edu/%7Eeamonn/iSAX_2.0.pdf for reference.
    In original form, during construction of iSAX indexing tree, multiple iSAX
    words have the root as parent, whereas internal nodes are limited to at
    most two children (see II. C.), and additionally, a node splitting policy
    is used to increase the likelihood that a given leaf being converted into
    an internal node will have its data more evenly distributed resulting in
    a more balanced tree.

    However, that implementation does not guarantee better balance,
    particularly if two series are very close in value throughout. Furthermore,
    the requirement that internal nodes be roots of binary subtrees appears to
    be needlessly restrictive.

    Accordingly, we adapt the n-ary nature of the top level of the iSAX tree
    for use at each level; when a leaf node is to be converted into an internal
    node, the data to be re-stored in a child node can take on any iSAX word
    value of the higher cardinality (i.e. we can have more than 2 leaves per
    internal node, are are not limited to binary splits.) This is just as
    likely (if not more likely) to keep the tree balanced, while controlling
    the height of the tree. As new leaves do not result in the creation of a
    new file when empty, no additional file space is required to implement the
    n-ary subtrees.

    Notes:

    1. Currently only permits one instance of a time series to be stored
    (should be rare if values are floats). Duplicate time series are ignored,
    even if they have different labels.

    2. Deleting a time series from tree requires series to be provided as
    input; i.e. time series <-> series ID conversion not provided by this
    class.

    3. Assumes `alldata` is an accessible dictionary that stores time series
    data, with iSAX word as key, and value is a list of tuples [(time series as
    a numpy array, time series ID), ...]  Currently, ISAX-indexed time series
    data is stored in memory; modifications are required to implement writes
    to persistent storage.

    Methods available:

    1.  (constructor) - initialize an instance of a tree with given label
            Usage: myTree = iSaxTree("root")
    2.  insert - insert a time series into the tree, optionally with a given
    ID (must identify with `tsid` if used)
            Notes: format of time series should be a 1-D numpy array
                   (e.g. array([ 26.02,  25.9 ,  25.71,  25.84,  25.98,  ...);
                   may not work correctly if time series of different lengths
                   are inserted
            Usage: myTree.insert(ts, tsid="Time Series ID")
    3.  delete - delete a time series from the tree; actual time series
    required as input
            Usage: myTree.delete(ts)
    4.  preorder - outputs as text structure of tree with counts of time series
    stored in each leaf node
            Usage: myTree.preorder()
    5.  preorder_ids - same as preorder, but list of IDs output along with
    counts
            Usage: myTree.preorder_ids()
    6.  find_nbr - performs an approximate search for nearest neighbor of input
    time series
            Notes: format of input/reference time series should be a 1-D numpy
            array; the input time series need not be an existing time series
            in tree
            Usage: myTree.find_nbr(reference_ts)
    '''

    def __init__(self, data, parent=None, level=0):
        '''
        Initializes tree (or subtree).

        Note: leave level unspecified to create root node of a new tree.
        '''

        super().__init__(data, parent)

        # dictionary with isax words associated with this level as keys
        # and list of child_pointers as value
        self.keyhashes = {}

        # current level
        self.level = level

        # adjustable parameters

        # number of chunks
        self.w = 4

        # base cardinality
        self.a = 4

        # threshold number of series per file
        # (can be increased for larger datasets)
        self.TH = 5

        # maximum depth (number of levels) in tree to split
        self.maxlevel = 10

        # initialize file structure (separately, in case we want to re-init)
        self.initialize_file_structure()

    def initialize_file_structure(self):
        self.fs = TreeFileStructure()

    def insert(self, ts, level=1, tsid=""):
        '''
        Attempt insertion at a given level of the tree (default is below root).

        Example usage: myTree.insert(ts, tsid="Time Series ID")

        Notes:
        1. Format of time series should be a 1-D numpy array
               (e.g. array([ 26.2,  25.9 ,  25.71,  25.84,  25.98,  25.73, ...)
               may not work correctly if time series of different lengths
               are inserted
        2. Series ID (string) is optional, but must be specified with `tsid`
        if used.
        3. Currently, threshold for maximum number of series per file is
        applied up to `maxlevel` to control height of tree; at `maxlevel`
        series is added to a child node even if threshold number of series
        is exceeded
        '''

        print ("attempting insert of", tsid)

        # level to add node must be one below current node's level
        assert level == (self.level + 1)

        # get iSAX representation of input time series (as string)
        isax_word = str(get_isax_word(ts, self.w, self.a*(2**(level-1))))

        if self.fs.already_in_file(isax_word, ts):
            # exact match already in file
            print ("did not insert - already in file")
            return

        if isax_word in self.keyhashes:
            # a node for the same iSAX word has previously been created;
            # can be leaf or an internal node
            # identify the pointer that points to the correct child
            idx = self.keyhashes[isax_word]
            node = self.child_pointers[idx]

            if node.num_children() == 0:
                # child is a terminal node / leaf
                assert node.data == isax_word
                # there is space to add series to the leaf
                if len(self.fs.read_file(isax_word)) < self.TH:
                    self.fs.write_to_file(isax_word, ts, tsid=tsid)
                # add to leaf if maximum depth reached
                # (i.e. do not split further)
                elif level == self.maxlevel:
                    self.fs.write_to_file(isax_word, ts, tsid=tsid)
                # additional insert warrants a split, create an internal node
                else:
                    print ("creating new internal node at level", level)

                    # get all time series associated with this node and
                    # reinsert into new subtree
                    ts_list = self.fs.alldata[isax_word]
                    for ts_to_move, itemid in ts_list:
                        node.insert(ts_to_move, level + 1, tsid=itemid)
                        self.fs.delete_from_file(str(get_isax_word(
                            ts_to_move, self.w, self.a * (2 ** (level - 1)))),
                            ts_to_move)

                    print ("completed moving series from internal node into ",
                           "new nodes")
                    # insert input time series that triggered split
                    # into a node in the new subtree (i.e. one level down)
                    node.insert(ts, level + 1, tsid=tsid)
            else:
                # child is an internal node (i.e. not a terminal node);
                # traverse to next level
                node.insert(ts, level + 1, tsid=tsid)
        else:
            # new node to be created; add pointer to new terminal node
            # in self's list
            self.keyhashes[isax_word] = self.num_children()  # 0-index
            self.fs.write_to_file(isax_word, ts, tsid=tsid)
            self.add_child(isax_word, level)

        return

    def delete(self, ts, level=1):
        '''
        Delete a time series from the tree.

        Usage: myTree.delete(ts)
        '''

        # get iSAX representation of input time series (as string)
        isax_word = str(get_isax_word(ts, self.w, self.a * (2 ** (level - 1))))

        if self.fs.already_in_file(isax_word, ts):
            # exact match already in file
            print("match found - deleting")
            self.fs.delete_from_file(isax_word, ts)
            return

        # a node for the same iSAX word has previously been created;
        # can be leaf or an internal node
        if isax_word in self.keyhashes:
            # identify the pointer that points to the correct child
            idx = self.keyhashes[isax_word]
            node = self.child_pointers[idx]

            if node.num_children() == 0:  # child is a terminal node
                assert node.data == isax_word
                # if code reaches here, word is not in node otherwise it would
                # have already been deleted since it should be stored filed
                # under `isax_word`
                print ("delete failed -- time series not in store")
            else:
                # child is an internal (i.e. not a terminal node); traverse
                node.delete(ts, level + 1)
        else:
            # there was no node created for this isax_word;
            # therefore it cannot be in tree
            print ("delete failed -- time series not in store")
        return

    def preorder(self):
        '''
        Outputs structure of tree with counts of time series stored in each
        leaf node.

        Usage: myTree.preorder()
        '''

        if self.isRoot():
            print (self.data)
        else:
            if self.fs.isax_exists(self.data):
                count = len(self.fs.read_file(self.data))
            else:
                count = 0
            paddedstr = ("---" * self.level + ">" + self.data + ": " +
                         str(count))
            print (paddedstr)

        # recursively traverse tree
        for child_link in self.child_pointers:
            child_link.preorder()

    def preorder_ids(self):
        '''
        Same as preorder, but list of IDs are output along with counts.

        Usage: myTree.preorder_ids()
        '''

        if self.isRoot():
            print (self.data)
        else:
            if self.fs.isax_exists(self.data):
                count = len(self.fs.read_file(self.data))
                listing = [item[1] for item in self.fs.read_file(self.data)]
            else:
                count = 0
                listing = []
            paddedstr = ("---" * self.level + ">" + self.data + ": " +
                         str(count))
            print (paddedstr, sorted(listing))

        # recursively traverse tree
        for child_link in self.child_pointers:
            child_link.preorder_ids()

    def find_nbr(self, ts, level=1):
        ''' Performs an approximate search for nearest neighbor of input time series.

        Example usage: myTree.find_nbr(reference_ts)

        Notes:

        1. Nearest neighbor is 'approximate' since we are taking advantage of
        the format of the natural clustering provided by the iSAX tree to
        determine potential close matches. The intuition is that two similar
        time series are often represented by the same iSAX word. Another
        algorithm that potentially performs an exhaustive search should be
        used if an exact search is required.
        2. In the event that no neighbors with the same iSAX word can be found,
        neighboring series that share the same parent iSAX word (but not the
        root) are considered to find the nearest neighbor. Otherwise, a null
        suggestion is returned.
        3. The input/reference time series should be a 1-D numpy array; the
        input time series need not be an existing time series in tree.
        '''

        # get iSAX representation of input time series (as string)
        isax_word = str(get_isax_word(ts, self.w, self.a * (2 ** (level - 1))))

        if self.fs.isax_exists(isax_word):
            # exact isax word match found in file, retrieve all series
            ts_list = self.fs.read_file(isax_word)
            print(len(ts_list), "neighbors with same iSAX word found")
            assert len(ts_list) > 0

            if len(ts_list) == 1:  # one entry, return it
                print ("closest neighbor:", ts_list[0][1])
                return ts_list[0][0]
            else:
                # calculate distances from reference series to each located
                # series and return closest
                mindist = np.inf
                best_id = ""
                best_ts = None
                for item in ts_list:
                    tempdist = distance(ts, item[0])
                    if tempdist < mindist:
                        mindist = tempdist
                        best_id = item[1]
                        best_ts = item[0]
                print ("closest neighbor:", best_id)
                return best_ts

        # if isax_word is in keyhashes dictionary, but not in file system
        # then this means we need to check the children for potential matches
        if isax_word in self.keyhashes:
            # identify the pointer that points to the correct child
            idx = self.keyhashes[isax_word]
            node = self.child_pointers[idx]

            # child is a terminal node
            if node.num_children() == 0:
                assert node.data == isax_word
                # if code reaches here, node was created but it is empty
                # create list of neighboring series from nodes that have
                # shared parent
                ts_list = []
                for child in self.child_pointers:
                    try:
                        ts_list += self.fs.read_file(child.data)
                    except:
                        # node may exist, but no series are stored in the
                        # file system
                        pass

                if len(ts_list) > 0:
                    # calculate distances to each time series; return minimum
                    print(len(ts_list),
                          "neighbors with same parent iSAX word found")
                    mindist = np.inf
                    best_id = ""
                    best_ts = None
                    for item in ts_list:
                        tempdist = distance(ts, item[0])
                        if tempdist < mindist:
                            mindist = tempdist
                            best_id = item[1]
                            best_ts = item[0]
                    print ("closest neighbor:", best_id)
                    return best_ts
                else:
                    print ("no suggestions: no neighbors found at same level")
                    return None
            else:
                # child is an internal (i.e. not a terminal node); traverse
                return node.find_nbr(ts, level + 1)
        else:
            # there was no node created for this isax_word
            print ("no suggestions: no time series corresponds to the same ",
                   "isax_word")
