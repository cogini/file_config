defmodule FileConfig.Lib do
  @moduledoc "Common functions"

  require Logger

  @doc "Get nth element from reversed list"
  @spec rnth(non_neg_integer, list) :: term
  def rnth(n, list) do
    l = length(list)
    rnth(n, list, l)
  end

  @spec rnth(non_neg_integer, list, non_neg_integer) :: term
  def rnth(n, list, l) do
    i = n - 1
    :lists.nth(l - i, list)
  end

  @spec decode_json!(:ets.tid, atom, term, term) :: term
  def decode_json!(tid, name, key, value) when is_binary(value) do
    # case Jason.decode(value, keys: :atoms, strings: :copy) do
    case Jason.decode(value, keys: :atoms) do
      {:ok, value} ->
        true = :ets.insert(tid, {key, value})
        value
      {:error, reason} ->
        Logger.debug("Invalid binary JSON for table #{name} key #{key}: #{inspect reason}")
        value
    end
    # if :jsx.is_json(value) do
    #   value = :jsx.decode(value, [{:labels, :atom}, :return_maps])
    #   true = :ets.insert(tid, {key, value})
    #   value
    # else
    #   Logger.debug("Invalid binary JSON for table #{name} key #{key}")
    #   value
    # end
  end
  def decode_json!(_tid, _name, _key, value), do: value

end
