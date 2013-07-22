defmodule Import.Riak do
	
	#_old_priority = :erlang.process_flag(:priority,:low)
	
	@moduledoc """
	Handle the creation of riak connections and updates via post
	"""

	@doc """
	return a new riak pid as a riakc_pb_socket
	options can accept the host and port as a tuple
	"""
	def link(options) do
		{host, port} = options
		socket_opts = [{:"connect_timeout",1000000},{:"queue_if_disconnected",true},{:"auto_reconnect",true}]
		case :riakc_pb_socket.start_link(binary_to_atom(host), binary_to_integer(port), socket_opts) do
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
		_old_priority = :erlang.process_flag(:priority,:low)
		receive do
			{sender, msg} ->
				{_stat, pid, bucket, key, json, six} = msg
		        riak_obj = :riakc_obj.new("#{bucket}","#{key}", json) # OBJ
		        riak_md  = :riakc_obj.get_update_metadata(riak_obj)   # MD
		        	|> :riakc_obj.set_secondary_index(six)            # MD 
		        riak_obj = :riakc_obj.update_metadata(riak_obj,riak_md)
		        		

		        case :riakc_pb_socket.put(pid, riak_obj, :infinity) do
		        	:ok ->
		        		sender <- {:ok, "#{bucket}/#{key}"}
		        	_ ->
		        		sender <- {:error, "*#{bucket}/#{key}"}
		        end
		end
	end
end