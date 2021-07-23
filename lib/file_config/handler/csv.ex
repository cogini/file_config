defmodule FileConfig.Handler.Csv do
  @moduledoc "Handler for CSV files"

  require Logger

  alias FileConfig.Loader
  alias FileConfig.Lib

  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(%{id: tid, name: name, lazy_parse: true, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []

    case :ets.lookup(tid, key) do
      [] ->
        :undefined

      [{_key, bin}] when is_binary(bin) ->
        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            # Cache parsed value
            true = :ets.insert(tid, [{key, value}])
            {:ok, value}

          {:error, reason} ->
            Logger.debug("Error parsing table #{name} key #{key}: #{inspect(reason)}")
            {:ok, bin}
        end

      [{_key, value}] ->
        {:ok, value}
    end
  end

  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] ->
        :undefined

      [{_key, value}] ->
        {:ok, value}
    end
  end

  # @spec load_update(Loader.name(), Loader.update(), :ets.tid()) :: Loader.table_state()
  # def load_update(name, update, tid) do
  #   load_update(name, update, tid, nil)
  # end

  @spec load_update(Loader.name(), Loader.update(), :ets.tid(), Loader.update() | nil) :: Loader.table_state()
  def load_update(name, update, tid, prev) do
    config = update.config

    files =
      update
      |> Loader.changed_files?(prev)
      |> Loader.latest_file?()
      # Files stored latest first, process in chronological order
      |> Enum.reverse()

    for {path, state} <- files do
      Logger.debug("Loading #{name} #{config.format} #{path} #{inspect(state.mod)}")
      # TODO: handle parse errors
      {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, config])
      Logger.info("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")
    end

    Loader.make_table_state(name, update, tid)
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) do
    :ets.insert(state.id, records)
  end

  # Internal functions

  @spec parse_file(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file(path, tid, config) do
    evt = make_cb(tid, config)
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)

    # {:ok, bin} = File.read(path)
    # r = :file_config_csv2.pparse(bin, parser_processes, evt, 0)
    # Logger.warning("File: #{path}")
    {tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, parser_processes, evt, 0])
    num_records = Enum.reduce(r, 0, fn x, a -> a + x end)

    Logger.debug(
      "Loaded #{config.format} #{path} read #{tread / 1_000_000} parse #{tparse / 1_000_000}"
    )

    {:ok, num_records}
  end

  # Make callback function for CSV parser
  defp make_cb(tid, %{lazy_parse: true} = config) do
    {k, v} = config[:csv_fields] || {1, 2}

    fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tid, {key, value})
        acc + 1

      # Called before parsing shard
      {:shard, _shard}, acc ->
        acc

      # Called after parsing shard
      :eof, acc ->
        acc
    end
  end

  defp make_cb(tid, %{parser: parser} = config) do
    {k, v} = config[:csv_fields] || {1, 2}
    name = config[:name]
    parser_opts = config[:parser_opts] || []

    fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        bin = Lib.rnth(v, line, len)

        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            true = :ets.insert(tid, {key, value})

          {:error, reason} ->
            Logger.debug("Error parsing table #{name} key #{key}: #{inspect(reason)}")
            true = :ets.insert(tid, {key, bin})
        end

        acc + 1

      # Called before parsing shard
      {:shard, _shard}, acc ->
        acc

      # Called after parsing shard
      :eof, acc ->
        acc
    end
  end

  defp make_cb(tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}

    fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tid, {key, value})
        acc + 1

      # Called before parsing shard
      {:shard, _shard}, acc ->
        acc

      # Called after parsing shard
      :eof, acc ->
        acc
    end
  end
end
