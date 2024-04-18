defmodule FileConfig do
  @moduledoc "Public API"

  require Logger

  @table FileConfig.Loader
  @match_limit 500

  @type table_name :: atom()
  @opaque version :: {:vsn, term()}
  @type reason :: atom() | binary()
  # :ets.continuation()
  @type continuation :: term()

  @doc "Read value from table"
  @spec read(table_name(), term()) :: {:ok, term()} | nil | {:error, reason()}
  # @spec read(table_name(), term()) :: {:ok, term()} | :undefined
  def read(table_name, key) do
    case table_info(table_name) do
      {:ok, %{handler: handler} = table_state} ->
        handler.read(table_state, key)

      err ->
        err
    end
  end

  @doc "Insert one or more records"
  @spec insert(table_name(), {term(), term()} | [{term(), term()}]) :: :ok | {:error, reason()}
  # @spec insert(table_name(), {atom(), term()} | [{atom(), term()}]) :: true
  def insert(table_name, records) do
    case table_info(table_name) do
      {:ok, %{handler: handler} = table_state} ->
        handler.insert_records(table_state, records)

      err ->
        err
    end
  end

  # @deprecated "Use read_all/2 instead"
  @spec all(table_name(), pos_integer()) :: list(term())
  def all(table_name, match_limit \\ @match_limit) do
    {:ok, value} = read_all(table_name, match_limit)
    value
  end

  @doc "Return all records"
  @spec read_all(table_name(), pos_integer()) :: {:ok, list()}
  def read_all(table_name, match_limit \\ @match_limit) do
    case table(table_name) do
      {:ok, tab} ->
        loop_all(tab, :_, match_limit)
    end
  end

  @spec loop_all(:ets.tab(), :ets.match_pattern(), pos_integer()) :: {:ok, list()}
  defp loop_all(tab, pat, limit) do
    loop_all(:ets.match_object(tab, pat, limit), [])
  end

  @spec loop_all({list(), continuation()} | :"$end_of_table", list()) :: {:ok, list()}
  defp loop_all({match, continuation}, acc) do
    loop_all(:ets.match_object(continuation), [match | acc])
  end

  defp loop_all(:"$end_of_table", acc) do
    {:ok, List.flatten(Enum.reverse(acc))}
  end

  @spec flush(table_name()) :: :ok | {:error, reason()}
  # @spec flush(table_name()) :: true
  def flush(table_name) do
    case table_info(table_name) do
      {:ok, %{handler: _handler} = table_state} ->
        # TODO: This should call flush on handler
        # handler.flush(table_state)
        :ets.delete_all_objects(table_state.id)
        :ok

      err ->
        err
    end
  end

  # The version is the table id, which should be swapped on
  # any update. This is a very scary thing to use, but it works
  # as long as we use it as an opaque data type.
  @spec version(table_name()) :: version()
  def version(table_name), do: {:vsn, table(table_name)}

  @spec version(table_name(), version()) :: :current | :old
  def version(table_name, {:vsn, version}) do
    if table(table_name) == version do
      :current
    else
      :old
    end
  end

  # Private

  # @doc "Get table id from index"
  @spec table(table_name()) :: {:ok, :ets.tab()} | {:error, :unknown_table}
  defp table(table_name) do
    case :ets.lookup(@table, table_name) do
      [{^table_name, %{id: tab}}] ->
        {:ok, tab}

      [] ->
        {:error, :unknown_table}
    end
  catch
    :error, :badarg ->
      {:error, :unknown_table}
  end

  @doc "Get all data from index"
  @spec table_info(table_name()) :: {:ok, FileConfig.Loader.table_state()} | {:error, :unknown_table}
  # @spec table_info(table_name()) :: FileConfig.Loader.table_state() | :undefined
  def table_info(table_name) do
    case :ets.lookup(@table, table_name) do
      [{^table_name, value}] ->
        {:ok, value}

      [] ->
        {:error, :unknown_table}
    end
  catch
    :error, :badarg ->
      {:error, :unknown_table}
  end
end
