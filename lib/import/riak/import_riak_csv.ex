defmodule Import.Riak.Csv do
	
	_old_priority = :erlang.process_flag(:priority,:low)
	#_old_trapexit = :erlang.process_flag(:trap_exit,:true)

	@headers [ "User-agent": "Import.Riak.CSV", "Content-Type": "application/json"]
	@max_lines 100000000

    def file(options) do
        IO.write "\nMuxing"
        parent = Process.self()
        { filename, procs, parser, start_index, qty, rate } = options

        0..(procs-1)
         |> Enum.map( fn (cidx) ->
                mult = Import.Utils.Math.floor(qty / procs)
                start_index = start_index + (mult * cidx) + 1
                :timer.sleep( Import.Utils.Math.floor(start_index/100) )
                spawn_link(Import.Riak.Csv, :processlines, []) <- { self, { filename, procs, parser, start_index, mult, rate , cidx} }
            end)
         |> Enum.map( fn (pid) ->
                receive do 
                    { ^pid, result } ->
                        result
                        #=IO.write "\mDone" 
                        #Process.exit(pid,"done")
                end
            end
            )
         |> 
         IO.write "\n#{inspect(self)} Completed at #{inspect(:erlang.localtime())}"
         System.halt(0)
    end

    def processlines() do
        _old_priority = :erlang.process_flag(:priority,:low)
        receive do
            {sender, options}  -> 
                me = self
                {filename, procs, parser, start_index, qty, rate, cidx} = options
                if(qty > @max_lines) do qty = @max_lines end
                if(start_index <=0) do start_index = 1 end
                outline = (procs-cidx) + 1
                IO.write "\033[50;1H\033[1F\033[2K\033[0G#{inspect(self)} \033[20GSeeking line #{start_index} ..."
                {:ok, rpid} = Import.Riak.link()
                File.open(filename,[:read,{:read_ahead,4096000}],
                    fn(file) ->
                        file
                        |> IO.stream()
                        |> Stream.drop(start_index)
                        |> Stream.with_index()
                        |> Stream.map(
                                fn({line,idx}) ->
                                    
                                    pclose({self,qty-idx})
                                    ratelimit(idx, rate)
                                    spawn(Import.Riak, :post, []) <- {self, parser.parse(rpid, :"ipgeo", line)}
                                    #if(rem(idx,100) == 0) do IO.write "." end
                                    receive do
                                        {_,msg} ->
                                            if(idx==0) do IO.write "\n\033[50;1H\033[2F\033[2K\033[0GImporting started at #{inspect(:erlang.localtime())} for lines #{start_index} through #{start_index + qty}" end
                                            IO.write "\033[50;1H\033[#{outline}F\033[2K\033[0G#{inspect(self)} \033[20Gkey:#{msg} \033[42G@line:#{start_index + idx} \033[60Gprocessed"                   
                                    end
                                 end
                           )
                        |> Enum.take(qty)
                        |> File.close()
                        IO.write "\nDone"
                    end
                )
            
        end
    end

    defp ratelimit(count, pause) do
	    if(rem(count,10) == 0 && count>0) do 
        # pausing briefly 
        :timer.sleep(pause)
        end
    end

    defp pclose({pid,0}) do
        Process.exit(pid,"done")
    end

    defp pclose({pid,_}) do
        false
    end

end
