#!/usr/bin/env python3
from tsdb import TSDBServer, DictDB

identity = lambda x: x

schema = {
  'pk':         {'convert': identity,   'index': None},
  'ts':         {'convert': identity,   'index': None},
  'order':      {'convert': int,        'index': 1},
  'blarg':      {'convert': int,        'index': 1},
  'useless':    {'convert': identity,   'index': None},
  'mean':       {'convert': float,      'index': 1},
  'std':        {'convert': float,      'index': 1},
  'vp':         {'convert': bool,       'index': 1}
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


def main():

    # initialize the database
    db = DictDB(schema, 'pk', verbose=False)

    # initialize & run the server
    server = TSDBServer(db, verbose=False)
    server.run()

if __name__ == '__main__':
    main()
