Mynti207:
================================================

Persistent Time Series Database
------------------------------------------------

[![Build Status](https://travis-ci.org/Mynti207/cs207project.svg?branch=master)](https://travis-ci.org/Mynti207/cs207project) [![Coverage Status](https://coveralls.io/repos/github/Mynti207/cs207project/badge.svg?branch=master)](https://coveralls.io/github/Mynti207/cs207project?branch=master)


**Note:** Unit tests are carried out by loading the server and webserver as sub-processes, which are not reflected in the coverage statistics. We estimate that our unit tests achieve at least 90% coverage when taking into consideration code that is run via sub-processes.


Description
-----------

This package implements a persistent time series database. Our sample use case involves different types of similarity searches on daily stock market data.

### Persistence Architecture

**Indices**

**TODO**



**Heaps**

**TODO**



**Atomic transactions**

**TODO**


### Additional Feature: iSAX Similarity Searches

A time series of fixed length can be reduced in dimensionality using a SAX
encoding. The collective encodings for a time series can be used to index
multiple time series, which can then be represented by an iSAX tree.
(see e.g. http://www.cs.ucr.edu/~eamonn/iSAX_2.0.pdf) This tree structure
effectively clusters like time series according to some distance measure
(e.g. Euclidean distance). We implemented a modified version of the iSAX tree
using a true n-ary tree structure (i.e. n splits are permitted at all internal
nodes rather than the binary splits of a typical iSAX tree) thus alleviating
certain balancing concerns associated with the original iSAX tree model.
Functionality that utilizes the iSAX tree structure includes returning a
time series that is similar to an input time series, as well as outputting a
hierarchical representation of the contents of the iSAX tree that illustrates
the clustering of similar time series as indexed by the tree.

### REST API

**TODO**


## Technical Details

### Installation

The package can be installed by running `python setup.py install` from the root folder.

Installation will make the following packages available: `procs`, `pype`, `timeseries`, `tsdb` and `webserver`.


### Running the Server
The database server can be loaded by running **<TODO.SH>**. This will load our database of daily stock price data, which includes a year of daily price data for 379 S&P 500 stocks (source: [www.stockwiz.com](http://www.stockwiz.com) ).

We recommend using our web interface to interact with the REST API. Available functions, each of which represents a database operation, are listed below. Our database function demonstration provides more detail on how to load and use the web interface to interact with the REST API.

**REMEMBER TO PUSH DB FILES**



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
* Stock market examples: [daily stock prices](docs/stock_example_prices.ipynb) | [daily stock returns](docs/stock_example_returns.ipynb)


Developers
----------

Gioia Dominedo  |  [@dominedo](https://github.com/dominedo)  |  dominedo@g.harvard.edu

Nicolas Drizard  |  [@nicodri](https://github.com/nicodri)  |  nicolasdrizard@g.harvard.edu

Kendrick Lo  |  [@ppgmg](https://github.com/ppgmg)  |  klo@g.harvard.edu
