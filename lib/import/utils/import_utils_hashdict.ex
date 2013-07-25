defmodule Import.Utils.HashDict do

	defp make_key(k) when is_binary(k), do: binary_to_atom(k)
    defp make_key(k) when is_atom(k), do: k

	def bind(k,v) do
        HashDict.new([{make_key(k),v}])
    end

    def get(d,k) do
        HashDict.get(d,k,:none)
    end
    
    def set(d,k,v) do
        HashDict.put(d,k,v)
    end
    
    def push(d,k,v) do
        HashDict.put_new(d,k,v)
    end
end