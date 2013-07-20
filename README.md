# Import
# Import.Riak
# Import.Riak.Csv
# Import.Riak.Csv.Ipgeo


This is a project built with Elixir

## Prepate the ENV vars using export ##

export ELIXIR_ERL_OPTS='+S 2 +P 134217727'

## Prepare the command line binary ##

	mix escriptize

## Just compile the modules for use with iex ##

	mix compile
	iex -S mix


Examples of command line usage ith the compiled binary 'import'

./import PATH_TO_DATA/data.csv 0 10 20

./import PATH_TO_DATA/data.csv 1000 1000 0		# import the file starting at line 1000 for 1000 lines as fast as possible (0)

./import PATH_TO_DATA/data.csv                  # just import the whole file as fast as possible

./import PATH_TO_DATA/data.csv 0 100000000 256

./import PATH_TO_DATA/data.csv 4 30 1

