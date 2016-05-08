========
Time Series Database
========

Summary
========
This package implements a persistent time series database with the following functionality:

* Insert time series data. May be followed by running a pre-defined function (trigger), if previously specified.
* Upsert (insert/update) time series metadata.
* Delete time series data and all associated metadata.
* Perform select (query) of time series data and/or metadata.
* Perform augmented select (query, followed by a pre-defined function) of time series data and/or metadata.
* Add a trigger that will cause a pre-defined function to be run upon execution of a particular database operation (e.g. after adding a new time series).
* Remove a trigger associated with a database operation and a pre-defined function.
* Run a basic similarity search, to find the closest (most similar) time series in the database.
* Run an iSAX tree-based similarity search, which returns a faster but only returns an approximate result.

Initialization
========

The time series database can be accessed through a web interface, which directly executes database operations via the webserver (REST API).

Before running any database operations, you must:

* Load the server by running `python go_server.py` from the root folder.
* Load the webserver by running `python go_webserver.py` from the root folder.
* Import the web interface (`import webserver`) and initialize it, e.g. `web_interface = WebInterface()`

The instructions below assume that these three steps have been carried out.

Database Operations
==================

Please refer to the `demonstration <demo.ipynb>`_ for examples of how to load and use the database.

Insert Time Series
------------------
Inserts a new time series into the database. If any triggers are associated with time series insertion, then these are run and the results of their operations are also stored in the database.

**Function signature:**

insert_ts(pk, ts)

**Parameters:**

pk : any hashable type

Primary key for the new database entry

ts : TimeSeries

Time series to be inserted into the database

**Returns:**

Nothing, modifies database in-place

Upsert Metadata
------------------
Inserts or updates metadata associated with a time series.

**Function signature:**

upsert_meta(pk, md)

**Parameters:**

pk : any hashable type

Primary key for the  database entry

md : dictionary

Metadata to be upserted into the database

**Returns:**

Nothing, modifies database in-place

Delete Time Series
------------------
Deletes a time series and all associated metadata from the database.

**Function signature:**

delete_ts(pk)

**Parameters:**

pk : any hashable type

Primary key for the database entry to be deleted

**Returns:**

Nothing, modifies database in-place

Select
------------------
Queries the database for time series and/or associated metadata.

**Function signature:**

select(md={}, fields=None, additional=None)

**Parameters:**

md : dictionary (default={})

Criteria to apply to metadata

fields : list (default=None)

List of fields to return

additional : dictionary (default=None)

Additional criteria (e.g. 'sort_by' and 'limit')

**Returns:**

Query results

**Additional search criteria:**

* sort_by: Sorts the query results in either ascending or descending order. Use + to denote ascending order and - to denote descending order. e.g. {'sort_by': '+pk'}; {'sort_by': '-order'}

* limit: Caps the number of fields that are returned when used in conjunction with sort_by. e.g. {'sort_by': '+pk', 'limit': 5} for the top 5 primary keys

Augmented Select
------------------
Queries the database for time series and/or associated metadata, then executes a pre-specified function on the data that is returned.

**Function signature:**

augmented_select(proc, target, arg=None, md={}, additional=None)

**Parameters:**

proc : string

Name of the function to run when the trigger is met

target : string

Field names used to identify the results of the function.

arg : string (default=None)

Possible additional arguments (e.g. time series for similarity search)

md : dictionary (default={})

Criteria to apply to metadata

additional : dictionary (default=None)

Additional criteria ('sort_by' and 'order')

**Returns:**

Query results

**Additional search criteria:**

* sort_by: Sorts the query results in either ascending or descending order. Use + to denote ascending order and - to denote descending order. e.g. {'sort_by': '+pk'}; {'sort_by': '-order'}

* limit: Caps the number of fields that are returned when used in conjunction with sort_by. e.g. {'sort_by': '+pk', 'limit': 5} for the top 5 primary keys

**Available trigger functions:**

* corr: Calculates the distance between two time series, using the normalize kernelized cross-correlation metric. Required argument: a TimeSeries object.

* stats: Calculates the mean and standard deviation of time series values. No arguments required.

Add Trigger
------------------
Adds a trigger that will cause a pre-defined function to be run upon execution of a particular database operation. For example, additional metadata fields may be calculated upon insertion of new time series data.

**Function signature:**

add_trigger(proc, onwhat, target, arg=None)

**Parameters:**

proc : string

Name of the function to run when the trigger is hit

onwhat : string

Operation that triggers the function (e.g. 'insert_ts')

target : string

Array of field names to which to apply the results of the function

arg : string (default=None)

Possible additional arguments for the function

**Returns:**

Nothing, modifies database in-place

**Available trigger functions:**

* corr: Calculates the distance between two time series, using the normalize kernelized cross-correlation metric. Required argument: a TimeSeries object.

* stats: Calculates the mean and standard deviation of time series values. No arguments required.

Remove Trigger
------------------
Removes a trigger associated with a database operation and a pre-defined function.

**Function signature:**

remove_trigger(proc, onwhat)

**Parameters:**

proc : string

Name of the function that is run when the trigger is hit

onwhat : string

Operation that triggers the function (e.g. 'insert_ts')

**Returns:**

Nothing, modifies database in-place

Basic Similarity Search
------------------
Runs a basic similarity search, to find the closest (most similar) time series in the database.

**Function signature:**

similarity_search(self, query, top=1)

**Parameters:**

query : TimeSeries

The time series being compared to those in the database

top : int

The number of closest time series to return (default=1)

**Returns:**

Primary key and distance to the closest time series.

Enhanced Similarity Search
------------------
Runs an iSAX tree-based similarity search, which returns a faster but only returns an approximate result.

[TODO: add function signature and examples]
