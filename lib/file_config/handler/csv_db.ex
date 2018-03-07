defmodule FileConfig.Handler.CsvDb do
  @moduledoc "Handler for CSV files"

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
        {:ok, db} = :esqlite3.open(db_path)
        result = :esqlite3.q("SELECT value FROM kv_data where key = ?1", [key], db)
        :esqlite3.close(db)

        case result do
          [{value}] ->
            {:ok, Lib.decode_binary(tid, name, key, value)}
          [] ->
            # Cache not found result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
        end
    end
  end

  def create_table(config) do
    Lib.maybe_create_db(config.name)
    Lib.create_ets_table(config)
  end

  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    db_path = Lib.db_path(name)
    {:ok, stat} = File.stat(db_path)
    db_mod = stat.mtime

    Lager.debug("#{name} update #mod: #{inspect update.mod} db_mod: #{inspect db_mod}")

    if update.mod >= db_mod do
      Lager.debug("Loading #{name} db #{path} #{db_path}")
      {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
      Lager.notice("Loaded #{name} db #{path} #{rec} rec #{time / 1_000_000} sec")
    else
      Lager.notice("Loaded #{name} db #{path} up to date")
    end

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__, db_path: to_charlist(db_path)}
  end

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}

    db_path = Lib.db_path(config.name)

    evt = fn
      ({:line, line}, acc) -> # Called for each line
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        # Commit in the middle to avoid timeouts as write transactions wait for sync
        record_num = acc.record_num
        cycle = acc.cycle
        if rem(record_num, cycle) == 0 do
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

        # Lager.debug("Statement ~p", [Statement]),
        :ok = :esqlite3.exec("begin;", db)
        cycle = 1000 + :rand.uniform(1000)
        Map.merge(acc, %{db: db, statement: statement, cycle: cycle})
      (:eof, acc) -> # Called after parsing shard
        db = acc.db
        :ok = :esqlite3.exec("commit;", db)
        :ok = :esqlite3.close(db)
        acc
    end

    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, :erlang.system_info(:schedulers_online), evt, %{record_num: 0})
    num_records = Enum.reduce(r, 0, fn(x, a) -> a + x.record_num end)
    {:ok, num_records}
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

end
