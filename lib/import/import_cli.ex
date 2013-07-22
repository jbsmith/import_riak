defmodule Import.Cli do 

	@default_riak_host "127.0.0.1:8097"
	@default_procs 4
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

	Return a tuple of `{ file, procs, start, qty, rate }`, or `:help` if help was given. 
	"""

	def parse_args(argv) do
  		parse = OptionParser.parse(argv, switches: [ help: :boolean],
                                   		 aliases:  [ h:    :help   ])
		case parse do
			{ [ help: true ], _ } 					 -> :help
			{ _, [ file, host, procs, start, qty, rate ] } -> {   file, host, 				binary_to_integer(procs), binary_to_integer(start), binary_to_integer(qty), binary_to_integer(rate)}
			{ _, [ file, host, procs, start, qty ] } 		 -> { file, host, 				binary_to_integer(procs), binary_to_integer(start), binary_to_integer(qty), @default_rate}
			{ _, [ file, host, procs, start ] } 			 -> { file, host, 				binary_to_integer(procs), binary_to_integer(start), @default_qty, 			@default_rate}
			{ _, [ file, host, procs ] } 					 -> { file, host, 				binary_to_integer(procs), @default_start, 			@default_qty, 			@default_rate}
			{ _, [ file, host ] } 							 -> { file, host, 				@default_procs, 		  @default_start, 			@default_qty, 			@default_rate}
			{ _, [ file ] } 							 	 -> { file, @default_riak_host, @default_procs, 		  @default_start, 			@default_qty, 			@default_rate}
			  _  									 		 -> :help
		end
	end 


	@doc """
	
	"""

	def process(:help) do
		IO.puts """

		usage: import <file> [ host | #{@default_riak_host} ] [ procs | #{@default_procs} ] [ start | #{@default_start} ] [ qty | #{@default_qty} ] [ rate | #{@default_rate} ] 
		
		e.g.

		mix run -e 'Import.Cli.run(["PATH_TO_DATA/data.csv","127.0.0.1:8097",4,0,100,1000])'

		OR

		./import PATH_TO_DATA/data.csv 127.0.0.1:8097 4 0 100 0

		DO NOT USE NEGATIVE sign in front of numbers, as they will be interpreted incorrectly

		@file is a local file path
		@host is the host:port to connect with riak
		@procs is the number of parallel processes to run 			*<unsigned int>
		@start is the line number to start processing on  			*<unsigned int>
		@qty is the number of lines to process            			*<unsigned int>
		@rate is the sleep time in ms applied to every 10 line set  *<unsigned int>
		increase the rate from 0 to slow the processing of lines and reduce load
		"""

		System.halt(0)
	end

	def process({file, host, procs, start, qty, rate}) do
		Import.Riak.Csv.file(HashDict.new([{:file,file}, {:host,host}, {:procs, procs}, {:parser, Import.Riak.Csv.Ipgeo_parser}, {:start, start}, {:qty, qty}, {:rate, rate}]))
	end

end