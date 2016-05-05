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

# number of vantage points
NUMVPS = 5


def main():

    # augment the schema by adding columns for five vantage points
    for i in range(NUMVPS):
        schema["d_vp-{}".format(i)] = {'convert': float, 'index': 1}

    # initialize the database
    db = DictDB(schema, 'pk')

    # initialize & run the server
    server = TSDBServer(db)
    server.run()

if __name__ == '__main__':
    main()
