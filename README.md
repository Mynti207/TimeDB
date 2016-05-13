
Mynti207
================================================


Persistent Time Series Database
------------------------------------------------

[![Build Status](https://travis-ci.org/Mynti207/cs207project.svg?branch=master)](https://travis-ci.org/Mynti207/cs207project) [![Coverage Status](https://coveralls.io/repos/github/Mynti207/cs207project/badge.svg?branch=master)](https://coveralls.io/github/Mynti207/cs207project?branch=master)


**Note:** Unit tests are carried out by loading the server and webserver as sub-processes, which are not reflected in the coverage statistics. We estimate that our unit tests achieve at least 90% coverage when taking into consideration code that is run via sub-processes.




Description
-----------

This package implements a persistent time series database. Our sample use case involves two types of similarity searches on daily stock market data.



![Database structure diagram](docs/img/db_structure.png)



### Persistence Architecture

**Heaps**

The timeseries and the metadata are stored in heap files. When loading or creating a database, the heap files are opened and every operation occurs in the heap. The data are formatted with the python `struct` library and saved in the two following binary files.

- `TSHeap` to store the raw timeseries

All the timeseries stored need to have the same length (this parameter is set when opening a database and is checked when inserting new timeseries). This length is stored at the begining of the heap, followed by each timeseries as the encoded concatenation of the times and values sequences. The offset of each timeseries is stored in the `PrimaryIndex` `pks` of the `PersistentDB` object. In case of a deletion, the offset is removed from the indexes and the `deleted` metadata field associated with the time series is updated.

- `MetaHeap` to store the metadata:

For each timeseries that is inserted, all the fields of the schema are initialized to their default values and saved into the heap. The size of each `struct` is directly computed from the schema and is common to each time series' metadata. In case of deletion/insertion into the schema, the heap file is reset with the new `struct` for all the metadata. This design decision causes heavy computation, but it rarely occurs (only upon insertion/deletion of vantage points) and allows a memory optimized heap file to be maintained. The offset of each element is stored in the `PrimaryIndex` associated with the corresponding time series' primary key.


**Indices**

A primary index is used by default on the primary key (which is a string type in the current implementation) and stores the offsets for the heap files. Additional indexes can be set by the user on schema fields. This field can take 3 different values: `1` for a Binary Tree index in case of high cardinality, `2` for a BitMap index in case of low cardinality and `None` if no index is required.
The following indexes are saved on disk with pickle and inherit from the same `Index` class:

- `PrimaryIndex`: stores the primary index in a dictionary using the following format: 
```python
{'pk': ('offset_in_TSHeap', 'offset_in_MetaHeap')}
```

- `BinTreeIndex`: uses the Python `bintrees` library (https://pypi.python.org/pypi/bintrees/2.0.2).

- `BitMapIndex`: uses a dictionary with possible values as keys and bitmap vectors over the timeseries as values.

  e.g. if `pk1` and `pk2` are `1`, `pk3` is `2`, and `pk4` is `3`, then the index will be ``{1: '1100', 2: '0010', 3: '0001'}``




**Files saved on disk**

All the following files are saved in the local directory `data_dir` under the sub-directory `db_name`, which is an attribute of the PersistentDB object:

- `heap_ts.met`: stores the raw timeseries sequentially in a binary file, with `ts_length` stored at the beginning of the file.
- `heap_meta.met`: stores all the metadata fields in a binary file.
- `pk.idx`: stores the `PrimaryIndex`.
- `index_{'field'}.idx`: stores `BinTreeIndex` and `BitMapIndex` objects. In the latter case, an additional file `index_{'field'}_pks.idx` is also saved, which defines the primary key offsets for conversion to/from the bitmap representation.
- `triggers.idx`: Stores database triggers as a dictionary with possible trigger actions as keys and a list of tuple as values, where each tuple represents the parameters of a trigger associated with the given database action.`
- `schema.idx`: Stores the database schema, allowing the user to make and store changes to the fields.



**Atomic transactions**

Transactions are guaranteed to be atomic by using a logging system. The `PrimaryKey` index uses a log that is identical to its dictionary index, except for an additional key that keeps track of synchronization on disk. Writes first occur in the log in memory, then in the log on disk, and only then in the index in memory and later on disk (based on a frequency parameter that is set as a `PersistentDB` attribute).

As a result of this architecture, if the system crashes during an insertion then the log may differ from the saved index at the time of recovery. In this case, the log can be used to recover the primary index. We also protect the system from the loss of an index that is only stored in memory.

The log is also used for the triggers that are associated with the `PersistentDB` object, as they cannot be restored from the heap files.

The secondary indexes are stored on disk and are committed at will. These can be rebuilt from scratch using the heaps and the primary keys, so when reloading a database we load the secondary index files from disk and update them if necessary. This may add some overload at initialization in case of previous failure, but it reduces both the disk storage and the number of logs that must be tracked. In addition, the full recovery process should only be necessary on an infrequent basis.



### Additional Feature: iSAX Similarity Searches

A time series of fixed length can be reduced in dimensionality using a SAX encoding. The collective encodings for a time series can be used to index multiple time series, which can then be represented by an iSAX tree (see, e.g. [http://www.cs.ucr.edu/~eamonn/iSAX_2.0.pdf](http://www.cs.ucr.edu/~eamonn/iSAX_2.0.pdf)). This tree structure effectively clusters "similar" time series according to some distance measure, such as
Euclidean distance.

We implemented a modified version of the iSAX tree using a true n-ary tree structure (i.e. n splits are permitted at all internal nodes rather than the binary splits of a typical iSAX tree), thus alleviating certain balancing concerns associated with the original iSAX tree model. Database functionality that utilizes the iSAX tree structure includes returning a time series that is similar to an input time series, as well as outputting a hierarchical representation of the contents of the iSAX tree that illustrates the clustering of similar time series as indexed by the tree.



### REST API

The REST API allows the database client to be accessed through http requests, once the server and webserver are online. We use both `GET` and `POST` requests, transferring data in `JSON` format through the wire. The API is accessible on a web app that was build with the `aiohttp` library and that uses an asynchronous handler to process the requests

We recommend accessing the API through our user-friendly web interface (see demos below), rather than by manually building http requests. This allows users to communicate with the REST API through easy-to-use and robust Python scripts.

Alternatively, the following paths can be used for direct access.

**POST**

- [http://127.0.0.1:8080/tsdb/insert_ts](http://127.0.0.1:8080/tsdb/insert_ts): inserts a new timeseries

  parameters: `pk` (primary key), `ts` (timeseries)

- [http://127.0.0.1:8080/tsdb/upsert_meta](http://127.0.0.1:8080/tsdb/upsert_meta): upserts (inserts/updates) timeseries metadata

  parameters: `pk` (primary key), `md` (metadata)

- [http://127.0.0.1:8080/tsdb/delete_ts](http://127.0.0.1:8080/tsdb/delete_ts): deletes time series and associated metadata

  parameters: `pk` (primary key)

-  [http://127.0.0.1:8080/tsdb/add_trigger](http://127.0.0.1:8080/tsdb/add_trigger): adds a new trigger, that runs a stored procedure when a specified database operation is called

  parameters: `onwhat` (database operation trigger), `proc` (stored procedure name), `target` (fields to store procedure results)

- [http://127.0.0.1:8080/tsdb/remove_trigger](http://127.0.0.1:8080/tsdb/remove_trigger): removes a trigger

  parameters: `onwhat` (database operation trigger), `proc` (stored procedure name), `target` (fields that store procedure results; optional)

- [http://127.0.0.1:8080/tsdb/insert_vp](http://127.0.0.1:8080/tsdb/insert_vp): inserts a vantage point

  parameters: `pk` (primary key)

- [http://127.0.0.1:8080/tsdb/delete_vp](http://127.0.0.1:8080/tsdb/delete_vp): removes a vantage point

  parameters: `pk` (primary key)



**GET**

- [http://127.0.0.1:8080/tsdb](http://127.0.0.1:8080/tsdb): displays a presentation of the REST API

  parameters: None

- [http://127.0.0.1:8080/tsdb/select](http://127.0.0.1:8080/tsdb/select): selects (queries) time series data

  parameters: `md` (select criteria; optional), `fields` (return fields; optional), `additional` (order by, limit; optional)

- [http://127.0.0.1:8080/tsdb/augmented_select](http://127.0.0.1:8080/tsdb/augmented_select): selects (queries) time series data, and runs a stored procedure on the results of the query

  parameters: `md` (select criteria; optional), `proc` (stored procedure name), `arg` (procedure arguments; optional), `target` (fields to display procedure results), `additional` (order by, limit; optional)

- [http://127.0.0.1:8080/tsdb/vp_similarity_search](http://127.0.0.1:8080/tsdb/vp_similarity_search): vantage point similarity search

  parameters: `query` (comparison timeseries), `top` (number of similar time series to return; optional)

- [http://127.0.0.1:8080/tsdb/isax_similarity_search](http://127.0.0.1:8080/tsdb/isax_similarity_search): iSAX tree similarity search

  parameters: `query` (comparison timeseries)

- [http://127.0.0.1:8080/tsdb/isax_tree](http://127.0.0.1:8080/tsdb/isax_tree): text representation of iSAX tree

  parameters: None




## Technical Details

### Installation

The package can be downloaded by running `git clone git@github.com:Mynti207/cs207project.git` from the terminal.

it can then be installed by running `python setup.py install` from the root folder.

Installation will make the following packages available: `procs`, `pype`, `timeseries`, `tsdb` and `webserver`.

**Note**: Python 3.5 is required to use this package.




### Running the Server
The database server can be loaded by running `python go_server_persistent.py` (with the appropriate arguments), followed by `python go_webserver.py`. It is good practice to pause for a few seconds after each of those commands, to ensure that the server/webserver has loaded fully. Terminating `go_server_persistent.py` will result in the database being committed to disk. Please refer to our documentation for examples and more detailed instructions.

For example, `python go_server_persistent.py --ts_length 244 --db_name 'stock_prices'` followed by `python go_webserver.py` will load our database of daily stock price data, which includes a year of daily price data for 379 S&P 500 stocks (source: [www.stockwiz.com](http://www.stockwiz.com)).

We recommend using our web interface to interact with the REST API. Available functions, each of which represents a database operation, are listed below. Our database function demonstration provides more detail on how to load and use the web interface to interact with the REST API.



### Functions
* `insert_ts`: Insert time series data. May be followed by running a pre-defined function (trigger), if previously specified.
* `upsert_meta`: Upsert (insert/update) time series metadata.
* `delete_ts`: Delete time series data and all associated metadata.
* `select`: Perform select (query) of time series data and/or metadata.
* `augmented_select`: Perform augmented select (query, followed by a pre-defined function) of time series data and/or metadata.
* `add_trigger`: Add a trigger that will cause a pre-defined function to be run upon execution of a particular database operation (e.g. calculate metadata fields after adding a new time series).
* `remove_trigger`: Remove a trigger associated with a database operation and a pre-defined function.
* `insert_vp`: Add a vantage point (necessary to run vantage point similarity searches).
* `delete_vp`: Remove a vantage point and all associated data.
* `vp_similarity_search`: Run a vantage point similarity search, to find the closest (most similar) time series in the database.
* `isax_similarity_search`: Run an iSAX tree-based similarity search, to find the closest (most similar) time series in the database. This is a faster search technique, but it only returns an approximate answer and may not always find a match.
* `isax_tree`: Visualize the iSAX tree.

Please refer to our database function demonstration below for full details on the function signatures and usage.




### Examples
* [Database function demonstration](docs/demo.ipynb)
* [Database persistence demonstration](docs/persistence_demo.ipynb)
* Stock market examples: [daily stock prices](docs/stock_example_prices.ipynb) | [daily stock returns](docs/stock_example_returns.ipynb)
* [Presentation](docs/presentation.ipynb)





Developers
----------

Gioia Dominedo  |  [@dominedo](https://github.com/dominedo)  |  dominedo@g.harvard.edu

Nicolas Drizard  |  [@nicodri](https://github.com/nicodri)  |  nicolasdrizard@g.harvard.edu

Kendrick Lo  |  [@ppgmg](https://github.com/ppgmg)  |  klo@g.harvard.edu
