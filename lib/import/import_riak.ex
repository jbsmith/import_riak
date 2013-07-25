defmodule Import.Riak do
    @moduledoc """
    Riak Client Module
    """
    alias :riakc_pb_socket, as: Socket
    alias :riakc_obj, as: Doc
    alias :erlang, as: Erl

    def link(options) do
        {host, port} = options
        bind( :host, binary_to_atom(host) )
            |> push( :port, binary_to_integer(port) )
            |> linkstart([
                    {:"connect_timeout",1000000},
                    {:"queue_if_disconnected",true},
                    {:"auto_reconnect",true}
               ])
        
    end

    defp linkstart(opts, sopts) do
        case Socket.start_link( get(opts,:host), get(opts,:port), sopts ) do
            {:ok, link_pid} ->
                {:ok, link_pid}
            {_, reason } ->
                {:error, reason}
        end
    end


    @doc """
    receive transmissions with message body as a tuple of
    `{_stat, pid, bucket, key, json, six}`
    where pid is a process created with init
    where bucket and key are the path to the record
    where json is the record body
    where six is the specially crafted 2i representation 
    used with :riakc_pb_socket.set_secondary_index(six)

    Return a tuple of `{ status, msg }` to the sender. 
    """
    def post do
        _old_priority = Erl.process_flag(:priority,:low)
        receive do
            {sender, hash_dict} ->
                nro = Doc.new( "#{get( hash_dict, :bucket)}", "#{get( hash_dict, :net)}", get( hash_dict, :json ) )
                nro
                |> Doc.get_update_metadata
                |> Doc.set_secondary_index(  get( hash_dict, :six ) )
                |> update_metadata( nro )
                |> respond( sender, hash_dict )
        end
    end

    defp update_metadata(md, nro) do
        Doc.update_metadata(nro, md)
    end

    defp make_key(k) when is_binary(k), do: binary_to_atom(k)
    defp make_key(k) when is_atom(k), do: k

    defp bind(k,v) do
        HashDict.new([{make_key(k),v}])
    end

    defp get(d,k) do
        HashDict.get(d,k,:none)
    end
    
    defp set(d,k,v) do
        HashDict.put(d,k,v)
    end
    
    defp push(d,k,v) do
        HashDict.put_new(d,k,v)
    end

    defp respond(nro,sender,d) do
        case Socket.put( get(d, :pid), nro, :infinity) do
            :ok ->
                sender <- { :ok, "#{ get( d, :bucket ) }/#{ get( d, :key) }" }
            _ ->
                sender <- { :error, "*#{ get( d, :bucket ) }/#{ get( d, :key ) }" }
        end
    end


end




