defmodule Import.Riak.Csv.Ipgeo_parser do
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
				ip_shift = ceiling(:math.log(broadcast - net) / :math.log(2))
		end
		ip_mask  = (32 - ip_shift)
		ip_net   = :erlang.bsl( :erlang.bsr( net, ip_shift ), ip_shift )

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

		{:ok, pid, bucket, net, json, six}
	end

	defp floor(x) when x < 0 do
		t = trunc(x)
		case (x - t) == 0 do
			true -> t
			false -> t - 1
		end
	end

	defp floor(x) do
		trunc(x)
	end

	defp ceiling(x) when x < 0 do
		trunc(x)
	end

	defp ceiling(x) do
		t = trunc(x)
		case (x - t) == 0 do
			true -> t
			false -> t + 1
		end
	end

end
