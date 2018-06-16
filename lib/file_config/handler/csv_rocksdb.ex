defmodule FileConfig.Handler.CsvRocksdb do
  @moduledoc "Handler for CSV files with RocksDB backend"
  @app :file_config

  NimbleCSV.define(FileConfig.Handler.CsvRocksdb.Parser, separator: "\t", escape: "\0", headers: false)

  alias FileConfig.Handler.CsvRocksdb.Parser

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

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
        {:ok, db} = :rocksdb.open(db_path, create_if_missing: false)
        case :rocksdb.get(db, key, []) do
          {:ok, value} ->
            {:ok, data_parser.parse_value(name, key, value)}
          :not_found ->
            # Cache not found result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
          error ->
            Lager.warning("Error reading from rocksdb #{name} #{key}: #{inspect error}")
            :undefined
        end
        :ok = :rocksdb.close(db)
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

    {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, db_path, chunk_size])
    Lager.notice("Loaded #{name} csv #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__, db_path: db_path}
  end

  # @impl true
  # def insert_records(records) do
  #   chunks = Enum.chunk_every(records, chunk_size)
  #   write_chunks(chunks)
  #   :ok
  # end

  # Internal functions

  defp parse_file(path, db_path, chunk_size) do
    {topen, {:ok, db}} = :timer.tc(:rocksdb, :open, [db_path, [create_if_missing: true]])

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
    to_charlist(Path.join(state_dir, to_string(name)))
  end

  def write_chunk(chunk, db) do
    batch = for [_id, key, value] <- chunk, do: {:put, key, value}
    # :rocksdb.write(db, batch, sync: true)
    {duration, :ok} = :timer.tc(:rocksdb, :write, [db, batch, []])
    {length(batch), duration}
  end

end
