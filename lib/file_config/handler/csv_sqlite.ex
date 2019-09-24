defmodule FileConfig.Handler.CsvSqlite do
  @moduledoc "Handler for CSV files with sqlite backend"
  @app :file_config

  NimbleCSV.define(FileConfig.Handler.CsvSqlite.Parser, separator: "\t", escape: "\0")
  alias FileConfig.Handler.CsvSqlite.Parser

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name, parser: parser, db_path: db_path} = state, key) do
    parser_opts = state[:parser_opts] || []
    case :ets.lookup(tid, key) do
      [{_key, :undefined}] -> # Cached "not found" result
        :undefined
      [{_key, value}] -> # Cached result
        {:ok, value}
      [] ->
        {:ok, results} = Sqlitex.with_db(db_path, fn(db) ->
          Sqlitex.query(db, "SELECT value FROM kv_data where key = $1", bind: [key], into: %{})
        end)
        case results do
          [%{value: bin}] ->
            case parser.decode(bin, parser_opts) do
              {:ok, value} ->
                # Cache parsed value
                true = :ets.insert(tid, [{key, value}])
                {:ok, value}
              {:error, reason} ->
                Lager.debug("Error parsing table #{name} key #{key}: #{inspect reason}")
                {:ok, bin}
            end
          [] ->
            # Cache not found result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
        end
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tab) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)
    config = update.config

    db_path = db_path(name)
    if update_db?(File.stat(flag_path(name)), update.mod) do
      maybe_create_db(db_path)
      Lager.debug("Loading #{name} db #{path} #{db_path}")
      {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, tid, config])
      Lager.notice("Loaded #{name} db #{path} #{rec} rec #{time / 1_000_000} sec")
    else
      Lager.notice("Loaded #{name} db #{path} up to date")
    end

    Map.merge(%{
      name: name,
      id: tid,
      mod: update.mod,
      handler: __MODULE__,
      db_path: to_charlist(db_path)
    }, Map.take(config, [:parser, :parser_opts, :commit_cycle]))
  end

  # @impl true
  @spec insert_records(Loader.table_state, {term, term} | [{term, term}]) :: true
  def insert_records(%{commit_cycle: commit_cycle} = state, records) do
    chunks = Enum.chunk_every(records, commit_cycle)
    for chunk <- chunks do
      {:ok, db} = Sqlitex.open(state.db_path)
      {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)
      :ok = :esqlite3.exec("begin;", db)
      for {key, value} <- chunk, do: insert_row(statement, [key, value])
      :ok = :esqlite3.exec("commit;", db)
      :ok = :esqlite3.close(db)
    end
    true
  end
  def insert_records(state, records) do
    {:ok, db} = Sqlitex.open(state.db_path)
    {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)
    :ok = :esqlite3.exec("begin;", db)
    for {key, value} <- records, do: insert_row(statement, [key, value])
    :ok = :esqlite3.exec("commit;", db)
    :ok = :esqlite3.close(db)
    true
  end

  # Internal functions

  @spec update_db?({:ok, File.Stat.t} | {:error, File.posix}, :calendar.datetime) :: boolean
  defp update_db?({:error, :enoent}, _mod), do: true
  defp update_db?({:ok, %{mtime: mtime}}, mod) when mod > mtime, do: true
  defp update_db?({:ok, _stat}, _mod), do: false

  # Create function which selects key and value fields from parsed CSV row
  defp make_fetch_fn(%{csv_fields: {key_field, value_field}}) do
    key_index = key_field - 1
    value_index = value_field - 1
    fn row -> [Enum.at(row, key_index), Enum.at(row, value_index)] end
  end
  defp make_fetch_fn(_) do
    fn [key, value | _rest] -> [key, value] end
  end

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  defp parse_file(path, _tid, config) do
    fetch_fn = make_fetch_fn(config)
    db_path = db_path(config.name)

    {topen, {:ok, db}} = :timer.tc(&Sqlitex.open/1, [db_path])
    {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)
    :ok = :esqlite3.exec("begin;", db)
    start_time = :os.timestamp()

    stream = path
             |> File.stream!(read_ahead: 100_000)
             |> Parser.parse_stream(headers: false)
             |> Stream.map(&(insert_row(statement, fetch_fn.(&1))))
    results = Enum.to_list(stream)

    tprocess = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    #:ok = :esqlite3.exec("commit;", db)
    {tcommit, :ok} = :timer.tc(:esqlite3, :exec, ["commit;", db])
    :ok = :esqlite3.close(db)

    :ok = File.touch(flag_path(config.name))

    Lager.debug("Loaded #{config.name} #{config.format} open #{topen / 1_000_000} process #{tprocess} commit #{tcommit / 1_000_000}")

    {:ok, length(results)}
  end

  @spec parse_file_incremental(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file_incremental(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    commit_cycle = config[:commit_cycle] || 10000
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)

    db_path = db_path(config.name)

    # {_tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    # {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, :erlang.system_info(:schedulers_online), evt, 0])

    evt = fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        # Commit in the middle to avoid timeouts as write transactions wait for sync
        record_num = acc.record_num
        if rem(record_num, acc.cycle) == 0 do
          db = acc.db
          :esqlite3.exec("commit;", db)
          :esqlite3.exec("begin;", db)
        end
        statement = acc.statement
        :ok = insert_row(statement, [key, value])
        %{acc | record_num: record_num + 1}
      ({:shard, _shard}, acc) -> # Called before parsing shard
        {:ok, db} = Sqlitex.open(db_path)
        # {:ok, statement} = :esqlite3.prepare(
        #   """
        #   INSERT OR IGNORE INTO kv_data (key, value) VALUES(?1, ?2);
        #   UPDATE kv_data SET key = ?1, value = ?2 WHERE key = ?1 AND (Select Changes() = 0);
        #   """, db)
        {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)

        :ok = :esqlite3.exec("begin;", db)
        Map.merge(acc, %{db: db, statement: statement, cycle: commit_cycle + :rand.uniform(commit_cycle)})
      (:eof, acc) -> # Called after parsing shard
        db = acc.db
        :ok = :esqlite3.exec("commit;", db)
        :ok = :esqlite3.close(db)
        acc
    end

    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, parser_processes, evt, %{record_num: 0})
    num_records = Enum.reduce(r, 0, fn(x, a) -> a + x.record_num end)
    {:ok, num_records}
  end

  defp insert_row(statement, params), do: insert_row(statement, params, :first, 1)

  defp insert_row(statement, params, :first, count) do
    :ok = :esqlite3.bind(statement, params)
    insert_row(statement, params, :esqlite3.step(statement), count)
  end
  defp insert_row(statement, params, :"$busy", count) do
    :timer.sleep(10)
    insert_row(statement, params, :esqlite3.step(statement), count + 1)
  end
  defp insert_row(_statement, _params, :"$done", count) do
    if count > 1 do
      Lager.debug("sqlite3 busy count: #{count}")
    end
    :ok
  end
  defp insert_row(_statement, params, {:error, reason}, _count) do
    Lager.error("esqlite: Error inserting #{inspect params}: #{inspect reason}")
    :ok
  end

  # @doc "Get path to db for name"
  @spec db_path(atom) :: Path.t
  defp db_path(name) do
    state_dir = Application.get_env(@app, :state_dir)
    Path.join(state_dir, "#{name}.db")
  end

  # @doc "Get path to flag file for name"
  @spec flag_path(atom) :: Path.t
  defp flag_path(name) do
    state_dir = Application.get_env(@app, :state_dir)
    Path.join(state_dir, "#{name}.flag")
  end

  @spec maybe_create_db(Path.t) :: [[]] | Sqlitex.sqlite_error
  defp maybe_create_db(db_path) do
    if File.exists?(db_path) do
      [[]]
    else
      create_db(db_path)
    end
  end

  @spec create_db(Path.t) :: [[]] | Sqlitex.sqlite_error
  defp create_db(db_path) do
    Lager.debug("Creating db #{db_path}")
    Sqlitex.with_db(db_path, fn(db) ->
      # TODO: make field sizes configurable
      Sqlitex.query(db, "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")
    end)
  end

  # defp commit(db) do
  #   try do
  #     :ok = :esqlite3.exec("commit;", db)
  #   catch
  #     {:error, :timeout, _ref} ->
  #       Lager.warning("sqlite3 timeout")
  #       commit(db)
  #     error ->
  #       Lager.warning("sqlite3 error #{inspect error}")
  #   end
  # end

end
