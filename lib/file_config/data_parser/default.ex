defmodule FileConfig.DataParser.Default do
  @moduledoc "No-op parser"

  @spec parse_value(atom, term, term) :: term
  def parse_value(_name, _key, value), do: value
end

