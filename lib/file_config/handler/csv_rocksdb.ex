defmodule FileConfig.Handler.CsvRocksdb do
  @moduledoc "Handler for CSV files with RocksDB backend"
  @app :file_config

  NimbleCSV.define(FileConfig.Handler.CsvRocksdb.Parser, separator: "\t", escape: "\0", header: false)

  alias FileConfig.Handler.CsvRocksdb.Parser

  require Lager

  alias FileConfig.Loader

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(table, key) do
    %{id: tid, name: name, db_path: db_path, data_parser: data_parser} = table
    case :ets.lookup(tid, key) do
      [{^key, :undefined}] -> # Cached "not found" result from db
        :undefined;
      [{^key, value}] -> # Found result
        {:ok, value}
      [] ->
        # TODO: somehow reuse db handle
        {:ok, db} = :rocksdb.open(db_path, create_if_missing: false)
        return = case :rocksdb.get(db, key, []) do
          {:ok, value} ->
            parsed_value = data_parser.parse_value(name, key, value)
            # Cache parsed value
            true = :ets.insert(tid, [{key, parsed_value}])
            {:ok, parsed_value}
          :not_found ->
            # Cache not found result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
          error ->
            Lager.warning("Error reading from rocksdb #{name} #{key}: #{inspect error}")
            :undefined
        end
        :ok = :rocksdb.close(db)
        return
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    db_path = db_path(name)
    config = update.config
    chunk_size = config[:chunk_size] || 100

    if update_db?(db_path, update.mod) do
      Lager.debug("Loading #{name} rocksdb #{path} #{db_path}")
      {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, db_path, chunk_size])
      Lager.notice("Loaded #{name} rocksdb #{path} #{rec} rec #{time / 1_000_000} sec")
    else
      Lager.notice("Loaded #{name} rocksdb #{db_path} up to date")
    end

    %{
      name: name,
      id: tid,
      mod: update.mod,
      handler: __MODULE__,
      data_parser: config[:data_parser],
      db_path: to_charlist(db_path),
      chunk_size: chunk_size
    }
  end

  # @impl true
  @spec insert_records(Loader.table_state, [tuple]) :: true
  def insert_records(table, records) do
    {:ok, db} = :rocksdb.open(table.db_path, create_if_missing: false)

    records
    |> Enum.sort
    |> Enum.chunk_every(table.chunk_size)
    |> Enum.map(&insert_chunk(&1, table.id, db))

    :ok = :rocksdb.close(db)
  end

  # Internal functions

  defp parse_file(path, db_path, chunk_size) do
    {topen, {:ok, db}} = :timer.tc(:rocksdb, :open, [to_charlist(db_path), [create_if_missing: true]])

    stream = path
    |> File.stream!(read_ahead: 100_000)
    |> Parser.parse_stream
    |> Stream.chunk_every(chunk_size)
    |> Stream.map(&write_chunk(&1, db))

    start_time = :os.timestamp()
    results = Enum.to_list(stream)
    process_duration = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    # :ok = :rocksdb.close(db)
    {tclose, :ok} = :timer.tc(:rocksdb, :close, [db])

    Lager.debug("open time: #{topen / 1_000_000}, process time: #{process_duration}, close time: #{tclose / 1_000_000}")
    # Lager.debug("results: #{inspect results}")

    rec = Enum.reduce(results, 0, fn {count, _duration}, acc -> acc + count end)
    {:ok, rec}
  end

  # @doc "Get path to db for name"
  @spec db_path(atom) :: Path.t
  defp db_path(name) do
    state_dir = Application.get_env(@app, :state_dir)
    Path.join(state_dir, to_string(name))
  end

  def write_chunk(chunk, db) do
    batch = for [_id, key, value] <- chunk, do: {:put, key, value}
    # :rocksdb.write(db, batch, sync: true)
    {duration, :ok} = :timer.tc(:rocksdb, :write, [db, batch, []])
    {length(batch), duration}
  end

  defp insert_chunk(chunk, tab, db) do
    batch = for {key, value} <- chunk, do: {:put, key, value}
    {duration, :ok} = :timer.tc(:rocksdb, :write, [db, batch, []])
    :ok = :ets.insert(tab, chunk)

    {length(batch), duration}
  end

  @spec update_db?(Path.t, :calendar.datetime) :: boolean
  defp update_db?(path, update_mtime) do
    case File.stat(path) do
      {:ok, _dir_stat} ->
        case File.stat(Path.join(path, "CURRENT")) do
          {:ok, %{mtime: file_mtime}} ->
            file_mtime < update_mtime
          {:error, _reason} ->
            true
        end

        # case File.ls(path) do
        #   {:ok, files} ->
        #     file_times = for file <- files, file_path <- Path.join(path, file),
        #       {:ok, %{mtime: file_mtime}} <- File.stat(file_path), do: file_mtime
        #     Enum.all?(file_times, &(&1 < mod))
        #   {:error, reason} ->
        #     Lager.warning("Error reading path #{path}: #{reason}")
        #     true
        # end
      {:error, :enoent} ->
        true
    end
  end

end
