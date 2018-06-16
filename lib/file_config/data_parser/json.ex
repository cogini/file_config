defmodule FileConfig.DataParser.Json do
  @moduledoc "Parser for JSON"
  require Lager

  @spec parse_value(atom, term, term) :: term
  def parse_value(name, key, value) when is_binary(value) do
    case Jason.decode(value, keys: :atoms) do
      {:ok, value} ->
        value
      {:error, reason} ->
        Lager.debug("Invalid binary JSON for table #{name} key #{key}: #{inspect reason}")
        value
    end
  end
  def parse_value(_name, _key, value), do: value

end
