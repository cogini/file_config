defmodule FileConfig.Handler.Csv do
  @moduledoc "Handler for CSV files"

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name, lazy_parse: true, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{_key, bin}] when is_binary(bin) ->
        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            # Cache parsed value
            true = :ets.insert(tid, [{key, value}])
            {:ok, value}
          {:error, reason} ->
            Lager.debug("Error parsing table #{name} key #{key}: #{inspect reason}")
            {:ok, bin}
        end
      [{_key, value}] ->
        {:ok, value}
    end
  end
  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{_key, value}] ->
        {:ok, value}
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)
    config = update.config

    # TODO: handle parse errors

    Lager.debug("Loading #{name} #{config.format} #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, config])
    Lager.notice("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")

    Map.merge(%{name: name, id: tid, mod: update.mod, handler: __MODULE__},
      Map.take(config, [:lazy_parse, :parser, :parser_opts]))
  end

  # @impl true
  @spec insert_records(Loader.table_state, tuple | [tuple]) :: true
  def insert_records(table, records) do
    :ets.insert(table.id, records)
  end

  # Internal functions

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, config) do
    evt = make_cb(tid, config)
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)

    # {:ok, bin} = File.read(path)
    # r = :file_config_csv2.pparse(bin, parser_processes, evt, 0)
    {tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    Lager.debug("Loaded CSV file #{path} tread #{tread / 1000000} sec")
    {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, parser_processes, evt, 0])
    num_records = Enum.reduce(r, 0, fn(x, a) -> a + x end)
    Lager.info("Loaded CSV file to ETS #{inspect tid} from #{path} #{num_records} records in #{tparse / 1000000} sec")
    {:ok, num_records}
  end

  # Make callback function for CSV parser
  defp make_cb(tid, %{lazy_parse: true} = config) do
    {k, v} = config[:csv_fields] || {1, 2}
    fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tid, {key, value})
        acc + 1
    ({:shard, _shard}, acc) -> # Called before parsing shard
      acc
    (:eof, acc) -> # Called after parsing shard
      acc
    end
  end
  defp make_cb(tid, %{parser: parser} = config) do
    {k, v} = config[:csv_fields] || {1, 2}
    name  = config[:name]
    parser_opts = config[:parser_opts] || []
    fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        bin = Lib.rnth(v, line, len)

        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            true = :ets.insert(tid, {key, value})
          {:error, reason} ->
            Lager.debug("Error parsing table #{name} key #{key}: #{inspect reason}")
            true = :ets.insert(tid, {key, bin})
        end
        acc + 1
    ({:shard, _shard}, acc) -> # Called before parsing shard
      acc
    (:eof, acc) -> # Called after parsing shard
      acc
    end
  end
  defp make_cb(tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tid, {key, value})
        acc + 1
    ({:shard, _shard}, acc) -> # Called before parsing shard
      acc
    (:eof, acc) -> # Called after parsing shard
      acc
    end
  end

end
