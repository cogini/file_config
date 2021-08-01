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

  @doc "Get modification time of file, or Unix epoch on error"
  @spec file_mtime(Path.t()) :: :calendar.datetime()
  def file_mtime(path) do
    case File.stat(path) do
      {:ok, %{mtime: mtime}} ->
        mtime

      {:error, reason} ->
        Logger.debug("Could not stat file #{path}: #{inspect(reason)}")
        {{1970, 1, 1}, {0, 0, 0}}
    end
  end

  # Create function which selects key and value fields from parsed CSV row
  def make_fetch_fn(key_field, value_field) do
    key_index = key_field - 1
    value_index = value_field - 1
    fn row -> [Enum.at(row, key_index), Enum.at(row, value_index)] end
  end

  def make_fetch_fn(_) do
    fn [key, value | _rest] -> [key, value] end
  end


  @doc ~S"""
  Uniformly allocate data to one of a fixed set of buckets.

  https://stats.stackexchange.com/questions/26344/how-to-uniformly-project-a-hash-to-a-fixed-number-of-buckets
  https://en.wikipedia.org/wiki/MurmurHash
  https://hex.pm/packages/murmur

  ```python
  def hash_to_bucket(e, B):
    i = murmurhash3.to_long128(str(e))
    p = i / float(2**128)
    for j in range(0, B):
      if j/float(B) <= p and (j+1)/float(B) > p:
        return j+1
    return B
  ```
  """
  @spec hash_to_bucket(term(), pos_integer()) :: pos_integer()
  def hash_to_bucket(_e, 1), do: 1
  def hash_to_bucket(e, buckets) do
    i = Murmur.hash_x86_128(e)
    p = i / :math.pow(2, 128)
    b = buckets / 1.0
    for j <- Range.new(0, buckets - 1), reduce: buckets do
      _acc when j / b <= p and (j + 1) / b > p -> j + 1
      acc -> acc
    end
  end

end
