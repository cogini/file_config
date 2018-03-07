defmodule FileConfig.Handler.Csv do
  @moduledoc "Handler for CSV files"

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  def create_table(config) do
    Lib.create_ets_table(config)
  end

  @spec load_update(Loader.update, :ets.tid) :: Loader.table_state
  def load_update(update, tid) do
    # Assume updated files contain all records
    {path, config, _mod} = hd(update.files)
    name = config.name

    Lager.debug("Loading #{name} csv #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, config])
    Lager.notice("Loaded #{name} csv #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, type: :ets, handler: __MODULE__}
  end

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}

    evt = fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)
        insert_records(tid, {key, value})
        acc + 1
      ({:shard, _shard}, acc) -> # Called before parsing shard
        acc
      (:eof, acc) -> # Called after parsing shard
        acc
    end

    # {_tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    # {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, :erlang.system_info(:schedulers_online), evt, 0])
    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, :erlang.system_info(:schedulers_online), evt, 0)
    num_records = Enum.reduce(r, 0, fn(x, a) -> a + x end)
    # Lager.info("Loaded CSV file to ETS #{inspect tid} from #{path} #{num_records} records in #{tparse / 1000000} sec")
    {:ok, num_records}
  end

  @spec insert_records(:ets.tab, tuple() | [tuple()]) :: true
  def insert_records(tid, records) do
    :ets.insert(tid, records)
  end

  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        {:ok, Lib.decode_binary(tid, name, key, value)}
    end
  end

end
