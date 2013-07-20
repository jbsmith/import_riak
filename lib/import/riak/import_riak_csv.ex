defmodule Import.Riak.Csv do
	
	#_old_priority = :erlang.process_flag(:priority,:low)
	#_old_trapexit = :erlang.process_flag(:trap_exit,:true)

	@headers [ "User-agent": "Import.Riak.CSV", "Content-Type": "application/json"]
	@max_lines 100000000

	def file(filename, parser, start_index // 0, qty // 100000000, rate // 0) do
		if(qty > @max_lines) do qty = @max_lines end
		started_at = inspect(:erlang.localtime())
		{:ok, riak_pid} = Import.Riak.init()
		IO.write "\n#{inspect(self)} Seeking started at #{started_at}"
		File.open(filename,[:read], 
		  fn(file) ->
		  	processlines(file, parser, riak_pid, start_index, qty, rate) 
		  end
		)
		IO.write "\n#{inspect(self)} Completed at #{inspect(:erlang.localtime())}"
		System.halt(0)
	end

    defp processlines(file, parser, rpid, start_index // 0, qty // 100000000, rate // 0) do
		IO.puts "\n#{inspect(self)} Seeking line #{start_index} ..."
	    IO.stream(file)
	    |> Stream.drop(start_index)
	    |> Stream.with_index()
	    |> Stream.map(
	    	fn({line,idx}) ->
			    if(idx==0) do IO.puts "\n#{inspect(self)} :#{start_index} Importing started at #{inspect(:erlang.localtime())}" end
	    		ratelimit(idx, rate)
	    		spawn(Import.Riak, :post, []) <- {self, parser.parse(rpid, :"ipgeo", line)}
	    		receive do
	    			{_,msg} ->
	    				if(idx==0) do 
			    			IO.puts "\n#{inspect(self)} key:#{msg} is first line processed"
					    end
	    				IO.write "\033[K\033[80D#{inspect(self)} key:#{msg} @line:#{1 + idx + start_index} processed"
	    		end
	    	 end
	       )
	    |> Enum.take(qty)
    end

    defp ratelimit(count, pause) do
    	if(rem(count,10) == 0 && count>0) do 
			# pausing briefly 
	    	:timer.sleep(pause)
		end
    end
end