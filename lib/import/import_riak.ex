defmodule Import.Riak do
	
	#_old_priority = :erlang.process_flag(:priority,:low)
	
	@moduledoc """
	Handle the creation of riak connections and updates via post
	"""

	@doc """
	return a new riak pid as a riakc_pb_socket
	"""
	def init() do
		{:ok, link_pid} = :riakc_pb_socket.start_link(:"10.1.92.40", 8087)
		socket_opts = [{:"connect_timeout",1000000000},{:"queue_if_disconnected",true},{:"auto_reconnect",true}]
		:riakc_pb_socket.set_options(link_pid,socket_opts,:infinity)
		{:ok, link_pid}
	end

	@doc """
	receive transmissions with message body as a tuple of
	`{_stat, pid, bucket, key, json}`

	Return a tuple of `{ status, msg }`. 
	"""
	def post do
		_old_priority = :erlang.process_flag(:priority,:low)
		receive do
			{sender, msg} ->
				{_stat, pid, bucket, key, json, six} = msg
		        riak_obj = :riakc_obj.new("#{bucket}","#{key}", json) # OBJ
		        riak_md  = :riakc_obj.get_update_metadata(riak_obj) # MD
		        	|> :riakc_obj.set_secondary_index(six) # MD 
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