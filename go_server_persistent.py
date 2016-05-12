#!/usr/bin/env python3
from tsdb import TSDBServer, PersistentDB
import os
import sys
import getopt
import asyncio

identity = lambda x: x

# default schema
schema = {
  'pk': {'type': 'str', 'convert': identity, 'index': None, 'values': None},
  'ts': {'type': 'int', 'convert': identity, 'index': None, 'values': None},
  'order': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
  'blarg': {'type': 'int', 'convert': int, 'index': 1, 'values': None},
  'useless': {'type': 'int', 'convert': identity, 'index': 1, 'values': None},
  'mean': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
  'std': {'type': 'float', 'convert': float, 'index': 1, 'values': None},
  'vp': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]},
  'deleted': {'type': 'bool', 'convert': bool, 'index': 2, 'values': [True, False]}
}

########################################
#
# NOTE: this file initializes the database server. This needs to be run
# before any other database commands.
#
# For basic client functionality, you will also need to initialize a
# TSDBClient object.
#
# For webserver client functionality (i.e. REST API), you will also need
# to initialize a WebInterface object.
#
########################################


def main(ts_length, db_name, data_dir):
    '''
    Runs the persistent database server.

    Parameters
    ----------
    ts_length : int
        Length of time series stored in the database
    db_name : str
        Name of the database
    data_dir : str
        Folder location of the database

    Returns
    -------
    Nothing, modifies in-place.
    '''

    # set up directory for db data
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # initialize the database
    db = PersistentDB(schema=schema, pkfield='pk', ts_length=ts_length,
                      db_name=db_name, data_dir=data_dir)

    # initialize & run the server
    server = TSDBServer(db)

    # initialize ayncio event loop
    loop = asyncio.get_event_loop()

    # run database server
    try:
        loop.run_until_complete(server.run())

    # this script is generally called as a sub-process
    # make sure that the database is closed and all data committed
    # if the sub-process is interrupted
    except RuntimeError:
        loop.close()
        db.close()
    except Exception:
        loop.close()
        db.close()
    except KeyboardInterrupt:
        loop.close()
        db.close()


def unpack_args(argv):
    '''
    Parse command line parameters (if any).

    Command line parameters are expected to take the form:
    -l : time series length
    -d : database name
    -f : database directory

    Default values are applied where arguments are not passed.
    https://docs.python.org/2/library/getopt.html

    Parameters
    ----------
    argv : list
        Command line arguments

    Returns
    -------
    Tuple of time series length, database name, and database directory
    '''

    # default values - use if not overridden
    ts_length = 100
    db_name = 'default'
    data_dir = 'db_files'

    # read in command line parameters (if any)
    try:
        opts, args = getopt.getopt(
            argv,
            'l:d:f:',
            ['ts_length=', 'db_name=', 'data_dir='])
    except getopt.GetoptError:
        pass

    # parse command line parameters
    for opt, arg in opts:
        if opt in ('-l', '--ts_length'):
            ts_length = int(arg)
        elif opt in ('-d', '--db_name'):
            db_name = str(arg)
        elif opt in ('-f', '--data_dir'):
            data_dir = str(arg)

    # return command line parameters (or default values if not provided)
    return ts_length, db_name, data_dir

if __name__ == '__main__':

    # unpack database specifications
    ts_length, db_name, data_dir = unpack_args(sys.argv[1:])

    # run persistent database server
    main(ts_length, db_name, data_dir)
