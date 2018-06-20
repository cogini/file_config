defmodule FileConfig.Handler.Csv do
  @moduledoc "Handler for CSV files"

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

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
    config = update.config

    Lager.debug("Loading #{name} csv #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, config])
    Lager.notice("Loaded #{name} csv #{path} #{rec} rec #{time / 1_000_000} sec")

    %{
      name: name,
      id: tid,
      mod: update.mod,
      handler: __MODULE__,
      data_parser: config[:data_parser]
    }
  end

  # @impl true
  @spec insert_records(Loader.table_state, tuple | [tuple]) :: true
  def insert_records(table, records) do
    :ets.insert(table.id, records)
  end

  # Internal functions

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)
    lazy_parse = config[:lazy_parse]
    data_parser = config[:data_parser]
    name  = config[:name]

    evt = fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = if lazy_parse do
          Lib.rnth(v, line, len)
        else
          data_parser.parse_value(name, key, Lib.rnth(v, line, len))
        end
        true = :ets.insert(tid, {key, value})
        acc + 1
      ({:shard, _shard}, acc) -> # Called before parsing shard
        acc
      (:eof, acc) -> # Called after parsing shard
        acc
    end

    # {_tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    # {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, :erlang.system_info(:schedulers_online), evt, 0])
    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, parser_processes, evt, 0)
    num_records = Enum.reduce(r, 0, fn(x, a) -> a + x end)
    # Lager.info("Loaded CSV file to ETS #{inspect tid} from #{path} #{num_records} records in #{tparse / 1000000} sec")
    {:ok, num_records}
  end

end
