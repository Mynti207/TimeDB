========
Mynti207
========

.. image:: https://travis-ci.org/Mynti207/cs207project.svg?branch=master
    :target: https://travis-ci.org/Mynti207/cs207project

.. image:: https://coveralls.io/repos/github/Mynti207/cs207project/badge.svg?branch=master
    :target: https://coveralls.io/github/Mynti207/cs207project?branch=master

Description
===========

**TimeSeries module**

Data and methods for an object representing a general time series. A TimeSeries object consists of two sequences of matching length: times and values. Typical sequence methods are implemented as part of the interface; please see documentation for more detail.

**PYPE module**

A compiler for PyPE, a DSL built around computational pipelines. Includes a lexer, parser, AST, and semantic analysis.

**procs module**

Contains several submodules to analyze time series.

**tsdb module**

A memory database implementation for time series with both client and server implementation to interact with it.

**webserver module**

The webserver of the REST API, built with aiohttp. The web server creates its own client which communicates with the server to interact
with the db.

**Note**

This project has been set up using PyScaffold 2.5.5. For details and usage
information on PyScaffold see http://pyscaffold.readthedocs.org/.

REST API
===========

**Set Up**

You need to run 'go_server.py' to set up the server and 'go_webserver' to set up the webserver (containing its own client).

**Communications**

Once the server and webservers are set up , you can interact with the db locally on the following routes:

* GET on http://127.0.0.1:8080/tsdb: display a presentation of the REST API

* POST on http://127.0.0.1:8080/tsdb/insert_ts: insert a TimeSeries object.
json data required: 'pk', 'ts'

* POST on http://127.0.0.1:8080/tsdb/upsert_meta: insert metadata related to a stored Timeseries.
json data required: 'pk', 'md'

* GET on http://127.0.0.1:8080/tsdb/select: select timeseries from the database
json data optionnal: 'md', 'fields', 'additional'

* GET on http://127.0.0.1:8080/tsdb/augmented_select: select timeseries with computations from the procs module.
json data required: 'procs', 'target'
json data optionnal: 'md', 'arg', 'additional'

* POST on http://127.0.0.1:8080/tsdb/add_trigger: add a trigger
json data required: 'proc', 'onwhat', 'target'
json data optionnal: 'additional'

* POST on http://127.0.0.1:8080/tsdb/remove_trigger: remove a trigger
json data required: 'proc', 'onwhat'

* GET on http://127.0.0.1:8080/tsdb/similarity_search: retrieve the most similar timeseries from the query timeseries
json data required: 'query'
json data optionnal: 'top'



Team Members
===========

Gioia Dominedo | @dominedo | dominedo@g.harvard.edu

Nicolas Drizard | @nicodri | nicolasdrizard@g.harvard.edu

Kendrick Lo | @ppgmg | klo@g.harvard.edu

Malcolm Mason Rodriguez | @malcolmjmr | mmasonrodriguez@college.harvard.edu
