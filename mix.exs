defmodule Import.Mixfile do
  use Mix.Project

  def project do
    [ app: :import,
      version: "0.1.0",
      name: "Import",
      deps: deps ]
  end

  # Configuration for the OTP application
  def application do
    []
  end

  # Be sure to update or delete entires in the mix.lock file 
  # if you have changed this section
  defp deps do
    [ 
      { :jsonex, "2.0", [github: "marcelog/jsonex", tag: "2.0"]},
      { :riakc,"1.4.0", [github: "basho/riak-erlang-client"]},
      { :ex_doc, [github: "elixir-lang/ex_doc"] }
    ]
  end
end
