========
Time Series Database
========

Summary
========
This package implements a persistent time series database with the following functionality:
* Insert time series data. May be followed by running a pre-defined function, if previously specified.
* Insert/update time series metadata.
* Delete time series data and all associated metadata.
* Perform select (query) of time series data and/or metadata.
* Perform augmented select (query, followed by a pre-defined function) of time series data and/or metadata.
* Add a trigger that will cause a pre-defined function to be run upon execution of a particular database operation (e.g. after adding a new time series).
* Remove a trigger associated with a database operation and a pre-defined function.
* Run a basic similarity search, to find the closest (most similar) time series in the database.
* Run an enhanced iSAX tree-based similarity search.

Initialization
========

The time series database can be accessed through a web interface, which directly executes database operations via the webserver (REST API).

Before running any database operations, you must:
* Load the server by running `python go_server.py` from the root folder.
* Load the webserver by running `python go_webserver.py` from the root folder.
* Import the web interface (`import webserver`) and initialize it,
e.g. `web_interface = WebInterface()`
The instructions below assume that these three steps have been carried out.

Database Operations
==================

Insert Time Series
------------------
Inserts a new time series into the database. If any triggers are associated with time series insertion, then these are run and the results of their operations are also stored in the database.

TODO: add function signature and examples

Upsert Metadata
------------------
Inserts or updates metadata associated with a time series.

TODO: add function signature and examples

Delete Time Series
------------------
Deletes a time series and all associated metadata from the database.

TODO: add function signature and examples

Select
------------------
Queries the database for time series and/or associated metadata.

TODO: add function signature and examples

Augmented Select
------------------
Queries the database for time series and/or associated metadata, then executes a pre-specified function on the data that is returned.

TODO: add function signature and examples

Add Trigger
------------------
Adds a trigger that will cause a pre-defined function to be run upon execution of a particular database operation. For example, additional metadata fields may be calculated upon insertion of new time series data.

TODO: add function signature and examples

Remove Trigger
------------------
Removes a trigger associated with a database operation and a pre-defined function.

TODO: add function signature and examples

Basic Similarity Search
------------------
Runs a basic similarity search, to find the closest (most similar) time series in the database.

TODO: add function signature and examples

Enhanced Similarity Search
------------------
Runs an enhanced iSAX tree-based similarity search, which runs faster than the basic similarity search.

TODO: add function signature and examples
