defmodule FileConfig.Handler.Dat do
  @moduledoc "Handler for dat files"

  require Lager

  alias FileConfig.Loader

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name, lazy_parse: true, data_parser: data_parser}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        parsed_value = data_parser.parse_value(name, key, value)
        # Cache parsed value
        true = :ets.insert(tid, [{key, parsed_value}])
        {:ok, parsed_value}
    end
  end
  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        {:ok, value}
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    Lager.debug("Loading #{name} dat #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
    Lager.notice("Loaded #{name} dat #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__}
  end

  # @impl true
  @spec insert_records(:ets.tab, tuple() | [tuple()]) :: true
  def insert_records(tid, records) do
    :ets.insert(tid, records)
  end

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, _config) do
    {:ok, fh} = :file.open(path, [:read])
    {:ok, count} = decode(fh, tid, 0)
    :ok = :file.close(fh)
    {:ok, count}
  end

  @spec decode(:file.io_device(), :ets.tid, non_neg_integer) :: {:ok, non_neg_integer}
  defp decode(fh, tid, count) do
    case :file.read_line(fh) do
      {:ok, line} ->
        case :re.run(line, '^\s*$|^//.*$') do # TODO: regex string
          :nomatch ->
            split = :re.split(line, "[\.|\n]", [:trim, {:return, :binary}])
            key = :lists.reverse(split)
            # TODO: convert to Jason
            value = :jsx.encode(Enum.join(split, "."))
            :ets.insert(tid, [{key, value}])
            decode(fh, tid, count + 1)
          _ ->
            decode(fh, tid, count)
        end
      _ -> {:ok, count}
    end
  end
end
