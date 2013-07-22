import_riak
===========

Utility to enable import of data into Riak using Elixir based application modules

## [Import](https://github.com/jbsmith/import_riak/blob/master/lib/import.ex)
## [Import.Cli](https://github.com/jbsmith/import_riak/blob/master/lib/import/import_cli.ex)
## [Import.Riak](https://github.com/jbsmith/import_riak/blob/master/lib/import/import_riak.ex)
## [Import.Riak.Csv](https://github.com/jbsmith/import_riak/blob/master/lib/import/riak/import_riak_csv.ex)
## [Import.Riak.Csv.Ipgeo](https://github.com/jbsmith/import_riak/blob/master/lib/import/riak/csv/import_riak_csv_ipgeoParser.ex)


This is a project built with Elixir

## Prerequisites ##
### Install Elixir and Erlang ###

### [Elixir Installation](http://elixir-lang.org/getting_started/1.html)

### Clone this repo from github ###

### [import_riak](https://github.com/jbsmith/import_riak.git)

### Prepare the ENV vars using export RECOMMENDED ###

	export ELIXIR_ERL_OPTS='+S 2 +P 134217727'

### Prepare the dependencies REQUIRED ###
	
	mix deps.get

### Prepare the command line binary RECOMMENDED ###

	mix escriptize

### OR Just compile the modules for use with iex RECOMMENDED ###

	mix compile
	iex -S mix
	>


### Examples of command line usage with the compiled binary 'import' from 'mix escriptize' ###

	./import PATH_TO_DATA/data.csv 127.0.0.1:8097 4 0 10 20
	./import PATH_TO_DATA/data.csv 127.0.0.1:8097 10 1000 1000 0		# import the file with 10 pocesses starting at line 1000 for 1000 lines as fast as possible (0)
	./import PATH_TO_DATA/data.csv                  # just import the whole file as fast as possible defaults to using 4 processes to riak at 127.0.0.1:8097
	./import PATH_TO_DATA/data.csv 127.0.0.1:8097 20 0 100000000 256
	./import PATH_TO_DATA/data.csv 127.0.0.1:8097 1 4 30 1


### Prepare the documentation RECOMMENDED ###
	this will generate the docs into /docs path
	
	mix docs
	

=======


