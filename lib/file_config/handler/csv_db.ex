defmodule FileConfig.Handler.CsvDb do
  @moduledoc "Handler for CSV files"
  @app :file_config

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  @spec lookup(Loader.table_state, term) :: term
  def lookup(table, key) do
    %{id: tid, name: name, db_path: db_path} = table
    case :ets.lookup(tid, key) do
      [{^key, :undefined}] -> # Cached "not found" result from db
        :undefined;
      [{^key, value}] -> # Found result
        {:ok, value}
      [] ->
        # {:ok, db} = :esqlite3.open(db_path)
        # result = :esqlite3.q("SELECT value FROM kv_data where key = ?1", [key], db)
        # :esqlite3.close(db)

        {:ok, results} = Sqlitex.with_db(db_path, fn(db) ->
          Sqlitex.query(db, "SELECT value FROM kv_data where key = $1", bind: [key], into: %{})
        end)
        case results do
          [result] ->
            {:ok, Lib.decode_binary(tid, name, key, result.value)}
          [] ->
            # Cache not found result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
        end
    end
  end

  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    db_path = db_path(name)
    if update_db?(db_path, File.stat(db_path), update.mod) do
      Lager.debug("Loading #{name} db #{path} #{db_path}")
      {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
      Lager.notice("Loaded #{name} db #{path} #{rec} rec #{time / 1_000_000} sec")
    else
      Lager.notice("Loaded #{name} db #{path} up to date")
    end

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__, db_path: to_charlist(db_path)}
  end

  defp update_db?(db_path, {:error, :enoent}, _mod) do
    create_db(db_path)
    true
  end
  defp update_db?(_db_path, {:ok, %{mtime: mtime}}, mod) when mod > mtime, do: true
  defp update_db?(_db_path, {:ok, _stat}, _mod), do: false

  @spec parse_file_incremental(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file_incremental(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    commit_cycle = config[:commit_cycle] || 1000
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
        {:ok, db} = :esqlite3.open(to_charlist(db_path))
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

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    commit_cycle = config[:commit_cycle] || 100
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)
    db_path = db_path(config.name)

    evt = fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)
        [{key, value} | acc]
      ({:shard, _shard}, acc) -> # Called before parsing shard
        acc
      (:eof, acc) -> # Called after parsing shard
        acc
    end

    # {_tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    # {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, :erlang.system_info(:schedulers_online), evt, 0])

    # {:ok, bin} = File.read(path)
    # r = :file_config_csv2.pparse(bin, parser_processes, evt, [])
    # records = List.flatten(r)
    # num_records = length(records)
    {tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    Lager.debug("#{path} read time: #{tread / 1_000_000}")
    {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, parser_processes, evt, []])
    Lager.debug("#{path} parse time: #{tparse / 1_000_000}")
    # {tflatten, records} = :timer.tc(List, :flatten, [r])
    # Lager.debug("tflatten: #{tflatten / 1000000}")
    # {tlength, num_records} = :timer.tc(&length/1, [records])
    # Lager.debug("tlength: #{tlength / 1000000}")
    records = List.flatten(r)
    num_records = length(records)

    {time, _r} = :timer.tc(&insert_records/3, [records, db_path, commit_cycle])
    Lager.debug("#{path} insert time #{time / 1_000_000}")
    {:ok, num_records}
  end

  def insert_records(records, db_path, commit_cycle) do
    chunks = Enum.chunk_every(records, commit_cycle)
    for chunk <- chunks do
        {:ok, db} = :esqlite3.open(to_charlist(db_path))
        {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)
        :ok = :esqlite3.exec("begin;", db)
        for {key, value} <- chunk, do: insert_row(statement, [key, value])
        :ok = :esqlite3.exec("commit;", db)
        :ok = :esqlite3.close(db)
    end
    :ok
  end

  def commit(db) do
    try do
      :ok = :esqlite3.exec("commit;", db)
    catch
      {:error, :timeout, _ref} ->
        Lager.warning("sqlite3 timeout")
        commit(db)
      error ->
        Lager.warning("sqlite3 error #{inspect error}")
    end
  end

  def insert_row(statement, params), do: insert_row(statement, params, :first, 1)

  def insert_row(statement, params, :first, count) do
    :ok = :esqlite3.bind(statement, params)
    insert_row(statement, params, :esqlite3.step(statement), count)
  end
  def insert_row(statement, params, :"$busy", count) do
    :timer.sleep(10)
    insert_row(statement, params, :esqlite3.step(statement), count + 1)
  end
  def insert_row(_statement, _params, :"$done", count) do
    if count > 1 do
      Lager.debug("sqlite3 busy count: #{count}")
    end
    :ok
  end
  def insert_row(_statement, params, {:error, reason}, _count) do
    Lager.error("esqlite: Error inserting #{inspect params}: #{inspect reason}")
    :ok
  end

  @doc "Get path to db for name"
  @spec db_path(atom) :: Path.t
  def db_path(name) do
    state_dir = Application.get_env(@app, :state_dir)
    Path.join(state_dir, "#{name}.db")
  end

  defp create_db(db_path) do
    Lager.debug("Creating db #{db_path}")
    Sqlitex.with_db(db_path, fn(db) ->
      Sqlitex.query(db, "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")
    end)
    # TODO: make field sizes configurable
    # {:ok, db} = :esqlite3.open(to_charlist(db_path))
    # :ok = :esqlite3.exec("CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));", db)
    # :ok = :esqlite3.close(db)
  end

end
