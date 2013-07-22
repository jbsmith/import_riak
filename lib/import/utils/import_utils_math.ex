defmodule Import.Utils.Math do

    def floor(x) when x < 0 do
        t = trunc(x)
        case (x - t) == 0 do
            true -> t
            false -> t - 1
        end
    end

    def floor(x) do
        trunc(x)
    end


    def ceiling(x) when x < 0 do
            trunc(x)
    end

    def ceiling(x) do
        t = trunc(x)
        case (x - t) == 0 do
            true -> t
            false -> t + 1
        end
    end

end