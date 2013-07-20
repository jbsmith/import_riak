defmodule Import.Cli do 

	@default_start 0
	@default_qty   100000000
	@default_rate  0         
	@type start :: integer
	@type qty :: integer
	@type rate :: integer

	@moduledoc """
	Handle the command line parsing and the dispatch to 
	the various functions that end up importing a table from csv
	"""

	def run(argv) do 
		argv
		|> parse_args
		|> process
	end


	@doc """
	`argv` can be -h or --help, which returns :help.
	
	Return a tuple of `{ file, start, qty, rate }`, or `:help` if help was given. 
	"""

	def parse_args(argv) do
  		parse = OptionParser.parse(argv, switches: [ help: :boolean],
                                   		 aliases:  [ h:    :help   ])
		case parse do
			{ [ help: true ], _ } 			-> :help
			{ _, [ file, start, qty, rate ] } -> { file, binary_to_integer(start), binary_to_integer(qty), binary_to_integer(rate)  }
			{ _, [ file, start, qty ] } -> { file, binary_to_integer(start), binary_to_integer(qty), @default_rate  }
			{ _, [ file, start ] } 		-> { file, binary_to_integer(start), @default_count, @default_rate}
			{ _, [ file ] } 		-> { file, @default_start, @default_count, @default_rate}
			  _  							-> :help
		end
	end 


	@doc """
	
	"""

	def process(:help) do
		IO.puts """

		usage: import <file> [ start | #{@default_start} ] [ qty | #{@default_qty} ] [ rate | #{@default_rate} ] 
		
		e.g.

		mix run -e 'Import.run(["PATH_TO_DATA/data.csv",0,100,1000])'

		OR

		./import PATH_TO_DATA/data.csv 0 100 0

		where file is a local file path
		where start is the line number to start processing on
		where qty is the number of lines to process
		where rate is the sleep time in ms applied to every 10 line set 
		increase the rate from 0 to slow the processing of lines and reduce load
		"""

		System.halt(0)
	end

	def process({file, start, qty, rate}) do
		Import.Riak.Csv.file(file, Import.Riak.Csv.Ipgeo_parser, start, qty, rate)
	end

end