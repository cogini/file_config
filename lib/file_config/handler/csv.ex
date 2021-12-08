defmodule FileConfig.Handler.Csv do
  @moduledoc "Handler for CSV files"

  require Logger

  alias FileConfig.Loader
  alias FileConfig.Lib

  @type reason :: FileConfig.reason()

  @spec init_config(map(), Keyword.t()) :: {:ok, map()} | {:error, reason()}
  def init_config(config, _args) do
    {:ok, config}
  end

  @spec read(Loader.table_state(), term()) :: {:ok, term()} | nil | {:error, reason()}
  def read(%{id: tab, lazy_parse: true, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []

    case :ets.lookup(tab, key) do
      [{_key, bin}] when is_binary(bin) ->
        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            # Cache parsed value
            true = :ets.insert(tab, [{key, value}])
            {:ok, value}

          {:error, reason} ->
            {:error, {:parse, bin, reason}}
        end

      [{_key, value}] ->
        # Cached result
        {:ok, value}

      [] ->
        nil
    end
  end

  def read(%{id: tab}, key) do
    case :ets.lookup(tab, key) do
      [{_key, value}] ->
        # Cached result
        {:ok, value}

      [] ->
        # Not found
        nil
    end
  end

  @deprecated "Use read/2 instead"
  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(state, key) do
    case read(state, key) do
      {:ok, value} ->
        value

      nil ->
        :undefined
    end
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) ::
          :ok | {:error, FileConfig.reason()}
  # @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) do
    # Always succeeds or perhaps throws
    :ets.insert(state.id, records)
    :ok
  end

  @spec load_update(Loader.name(), Loader.update(), :ets.tab(), Loader.update()) ::
          Loader.table_state()
  def load_update(name, update, tab, prev) do
    config = update.config

    files =
      update
      |> Loader.changed_files?(prev)
      |> Loader.latest_file?()
      # Files stored latest first, process in chronological order
      |> Enum.reverse()

    for {path, state} <- files do
      Logger.debug("Loading #{name} #{config.format} #{path} #{inspect(state.mod)}")
      {time, {:ok, result}} = :timer.tc(&parse_file/3, [path, tab, config])
      rec = result.record_count
      Logger.info("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")
    end

    Loader.make_table_state(__MODULE__, name, update, tab)
  end

  # Internal functions

  @spec parse_file(Path.t(), :ets.tab(), map()) :: {:ok, map()}
  def parse_file(path, tab, config) do
    evt = make_cb(tab, config)
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)

    {tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    {tparse, recs} = :timer.tc(:file_config_csv2, :pparse, [bin, parser_processes, evt, 0])
    record_count = Enum.reduce(recs, 0, &(&1 + &2))

    {:ok, %{record_count: record_count, read_duration: tread, parse_duration: tparse}}
  end

  # Make callback function for CSV parser
  # @spec make_cb(:ets.tab(), map()) ::
  defp make_cb(tab, %{lazy_parse: true} = config) do
    {k, v} = config[:csv_fields] || {1, 2}

    fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tab, {key, value})
        acc + 1

      # Called before parsing shard
      {:shard, _shard}, acc ->
        acc

      # Called after parsing shard
      :eof, acc ->
        acc
    end
  end

  defp make_cb(tab, %{parser: parser} = config) do
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
            true = :ets.insert(tab, {key, value})

          {:error, reason} ->
            Logger.warning("Error parsing table #{name} key #{key}: #{inspect(reason)}")
            # true = :ets.insert(tab, {key, {:error, {:parse, bin, reason}}})
            true = :ets.insert(tab, {key, bin})
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

  defp make_cb(tab, config) do
    {k, v} = config[:csv_fields] || {1, 2}

    fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        true = :ets.insert(tab, {key, value})
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
