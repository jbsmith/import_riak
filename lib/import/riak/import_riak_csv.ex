defmodule Import.Riak.Csv do

    alias :erlang, as: Erl
    alias :timer,  as: Timer

    _old_priority = Erl.process_flag(:priority,:low)
    #_old_trapexit = Erl.process_flag(:trap_exit,:true)

    @headers [ "User-agent": "Import.Riak.CSV", "Content-Type": "application/json"]
    @max_lines 100000000
    @cursor_home "\033[50;1H"
    @cursor_erase "\033[2K"

    defp csl(options,contents) do
        "#{@cursor_home}\033[#{HashDict.get(options,:procs)+1}S#{contents}#{@cursor_home}"
    end

    defp cil(options,contents) do
        line_number = (HashDict.get(options,:procs)-HashDict.get(options,:midx)) + 1
        "#{@cursor_home}\033[#{line_number}F#{@cursor_erase}#{contents}"
    end

    def file(options) do
        IO.write "\nMuxing"
        message = "#{inspect(self)} began processing of records at #{inspect(:erlang.localtime())}\n"
        IO.write "#{csl(options,message)}"
        0..(HashDict.get(options,:procs)-1)
         |> Enum.map( fn (cidx) ->
                spawn_link(Import.Riak.Csv, :processlines, []) <- { self, options |> mux_options(cidx) }
            end)
         |> Enum.map( fn ({pid,_options}) ->
                receive do 
                    { ^pid, msg } ->
                        IO.write msg
                end
            end
            )
         IO.write "\n\033[50;1HCompleted all processing\n#{@cursor_home}"
    end

    defp announce(options,idx) when idx == 0 do
        IO.write "\n\033[50;1H\033[2F\033[2K\033[0GImporting started at #{inspect(:erlang.localtime())} for lines #{HashDict.get(options,:start)} through #{HashDict.get(options,:start) + HashDict.get(options,:qty)}#{@cursor_home}" 
        options
    end

    defp announce(options,_) do
        options
    end

    defp update(options,idx,iend,result) when idx !== iend  do
        message = "#{inspect(self)} \033[20Gkey:#{result} \033[42G@line:#{HashDict.get(options,:start) + idx} \033[60Gprocessed"
        IO.write "#{cil(options,message)}" 
        options
    end 

    def processlines() do
        #_old_priority = :erlang.process_flag(:priority,:low)
        receive do
            {sender, options}  -> 
                {:ok, rpid} = Import.Riak.link(parse_hostport(options))
                message = "#{inspect(self)} \033[20GSeeking line #{HashDict.get(options,:start)} ..."
                IO.write "#{cil(options,message)}"
                File.open(HashDict.get(options,:file),[:read,{:read_ahead,4096000}],
                    fn(file) ->
                        file
                        |> IO.stream()
                        |> Stream.drop(HashDict.get(options,:start))
                        |> Stream.with_index()
                        |> Stream.map(
                                fn({line,idx}) ->
                                    ratelimit(options,idx)
                                    spawn(Import.Riak, :post, []) <- {self, HashDict.get(options,:parser).parse(rpid, :"ipgeo", line)}
                                    receive do
                                        {_,msg} ->
                                            options
                                            |> announce(idx)
                                            |> update(idx,HashDict.get(options,:qty),msg)  
                                    end
                                 end
                           )
                        |> Enum.take(HashDict.get(options,:qty))
                        File.close(file)
                    end
                )
                message = "#{inspect(self)} completed processing of records at #{inspect(:erlang.localtime())}\n"
                sender <- {sender,"#{cil(options,message)}"}
        end
    end

    defp ratelimit(options, count) do
        if(rem(count,10) == 0 && count>0) do 
        Timer.sleep(HashDict.get(options,:rate))
        end
    end

    defp regex_piped_split(string,pattern) do
        Regex.split(pattern,string)
    end

    defp parse_hostport(options) do
        options
        |> HashDict.get(:host)
        |> regex_piped_split(%r/\:/)
        |> list_to_tuple 
    end

    defp is_under(x,floor) when x > floor do
        x
    end

    defp is_under(_,floor) do
        floor
    end

    defp start_at(x) do
        is_under(x,1)
    end

    defp mux_options(options, midx) do
        mult = Import.Utils.Math.floor( HashDict.get(options,:qty) / HashDict.get(options,:procs) )
        options 
            |> HashDict.put(:start, start_at(HashDict.get(options, :start) + ( mult * midx) + 1 ) )
            |> HashDict.put(:qty, mult)
            |> HashDict.put_new(:midx, midx)
    end   

end
