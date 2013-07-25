defmodule Import.Riak.Csv.Ipgeo_parser do

    alias :erlang, as: Erl
    alias :math,   as: Math

    @moduledoc """
        Here we are parsing network ranges and calculating the size and shift of the netmasks
        When comparing an IP address to find what range it is in, you can compare 32 versions
        of the address, and ONE of those versions should hit upon a network start address

        If you take the ip address, in decimal, and bitshift right X places, then bitshift left
        the same number of places, you eliminate the part of the ip number that points to 
        that host, and get just that part which matches the base network it is in.

        You are discarding the host specific part, or unmasked part, and only retrieving
        the network behind the mask. This is how network masks operate.

        The problem is, you cannot know ahead of time, what the netmask is unless it is provided.
        Deriving the netmask, from the simple address, requires this lookup technique of comparing
        the 32 possible netmasks agains the known ip, and then comparing that to the known network
        start addresses.

        |----- netmask 32 bits wide -----|
        |11111111111111111111111111111111|
        | 32 bit mask is a HOST no subnet|
        
        |----- netmask 28 bits wide -----|
        |11111111111111111111111111110000|
        | MASK ->>                  |<<-s|
		
        |----- netmask 20 bits wide -----|
        |11111111111111111111000000000000|
        | MASK ->>          | <<-shift   |

        Any position of the 32 possibilities can be represented by a number
        24 bit mask is 255.255.255.0 is 2^24
        16 bit mask is 255.255.0.0   is 2^16

        Subtracting the network start from the last (broadcast) gives you the (size)
        taking the log2(size) will give you a float 
        when rounded up to the next integer it gives the (shift)
        the shift is how many bits from 0 that the network mask stops e.g. 28 bits
        a shift of 4 would be a 28 bit mask
        a shift of 10 would be a 22 bit mask

        shift measures the position from the right

        mask measures the position from the left

        this case calculates the subnet mask shift size from 0 bits
        subtracting this value from 32 will give you the mask size in bits
        """

        @doc """
        ETL Extract Transform and Load from CSV line format
        parse the input into an Elixir HashDict that can then be output as JSON
        pid is the process for the riak socket
        bucket is the bucket to store the data into
        term is the line from the file in CSV format
        NOTE: some use of erlang libraries for the math functions

        six is used for secondary indexes with riak 

        This library could easily be repurposed to handle parsing of nearly any text format
        """
        def parse(pid, bucket, term) do 
            [a,b,c,d,e,f,g,h,i] = String.split(term, %r{","})
            net = binary_to_integer(String.replace(a,"\"",""))
            broadcast = binary_to_integer(b)
            g = binary_to_float(g)
            h = binary_to_float(h)
            pc = String.replace(i,"\"","")
                   |>String.replace("\n","")

            case broadcast - net do
                0 ->
                    ip_shift = 0
                _ ->
                    ip_shift = Import.Utils.Math.ceiling(Math.log(broadcast - net) / Math.log(2))
            end

		

            ip_mask  = (32 - ip_shift)
            ip_net   = Erl.bsl( Erl.bsr( net, ip_shift ), ip_shift )

            ip  = HashDict.new(from: net, to: broadcast, mask: ip_mask, shift: ip_shift, next: broadcast + 1, prev: net - 1)
            iso = HashDict.new(iso2: c, name: d)
            rgn = HashDict.new(name: e)
            cty = HashDict.new(name: f, post: pc)
            geo = HashDict.new(lat:  g, lon: h)

            result = HashDict.new(ip: Dict.to_list(ip), country: Dict.to_list(iso), region: Dict.to_list(rgn), city: Dict.to_list(cty), geo: Dict.to_list(geo))
            json = Jsonex.encode(Dict.to_list(result))
		
            six  = [
                { {:integer_index, "shift"},      [ip_shift] },
                { {:integer_index, "mask"},       [ip_mask] },
                { {:integer_index, "network"},    [ip_net] },
                { {:integer_index, "broadcast"},  [broadcast] },
                { {:binary_index,  "post"},       [pc] }
            ]

            HashDict.new([{:stat,:ok}, {:pid, pid}, {:bucket, bucket}, {:key, net}, {:json, json}, {:six, six}])
        end
end
