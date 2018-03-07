defmodule FileConfig.Lib do
  @moduledoc "Common functions"
  @app :file_config

  require Lager

  @spec create_ets_table(map) :: :ets.tid
  def create_ets_table(%{name: name, ets_opts: ets_opts}) do
    :ets.new(name, ets_opts)
  end
  def create_ets_table(%{name: name}) do
    :ets.new(name, [:set, :public, {:read_concurrency, true}, {:write_concurrency, true}])
  end

  def maybe_create_db(name) do
    db_path = db_path(name)
    if not File.exists?(db_path) do
      create_db(db_path)
    end
  end

  @doc "Get path to db for name"
  @spec db_path(atom) :: Path.t
  def db_path(name) do
    state_dir = Application.get_env(@app, :state_dir)
    Path.join(state_dir, "#{name}.db")
  end

  def create_db(db_path) do
    Lager.debug("Creating db #{db_path}")
    {:ok, db} = :esqlite3.open(to_charlist(db_path))
    # TODO: make field sizes configurable
    :ok = :esqlite3.exec("CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));", db)
    :esqlite3.close(db)
  end

  @doc "Get nth element from reversed list"
  @spec rnth(non_neg_integer, list) :: term
  def rnth(n, list) do
    l = length(list)
    rnth(n, list, l)
  end

  @spec rnth(non_neg_integer, list, non_neg_integer) :: term
  def rnth(n, list, l) do
    i = n - 1
    :lists.nth(l - i, list)
  end

  @spec decode_binary(:ets.tid, atom, term, term) :: term
  def decode_binary(tid, name, key, value) when is_binary(value) do
    if :jsx.is_json(value) do
      value = :jsx.decode(value, [{:labels, :atom}, :return_maps])
      true = :ets.insert(tid, {key, value})
      value
    else
      Lager.debug("Invalid binary JSON for table #{name} key #{key}")
      value
    end
  end
  def decode_binary(_tid, _name, _key, value), do: value

end
