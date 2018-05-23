defmodule FileConfig do
  @moduledoc """
  FileConfig public API.
  """

  require Lager

  @table FileConfig.Loader
  @match_limit 500

  @type name :: atom
  @type version :: {:vsn, term}

  # -opaque version() :: {vsn,term()}.
  # -export_type([namespace/0, version/0]).

  # Public API

  @doc "Read value from named table"
  @spec read(name, term) :: :undefined | {:ok, term}
  def read(name, key) do
    case table_info(name) do
      :undefined ->
        :undefined
      %{handler: handler} = table_state ->
        handler.lookup(table_state, key)
      # %{id: tid} -> # fallback
      #   case :ets.lookup(tid, key) do
      #     [] -> :undefined
      #     [{^key, value}] ->
      #       {:ok, value}
      #   end
    end
  end

  @doc "Insert records"
  @spec insert(name, {atom, term} | [{atom, term}]) :: true
  def insert(name, records) do
    case table_info(name) do
      :undefined ->
        true
      %{handler: handler, id: tid} ->
        handler.insert_records(tid, records)
    end
  end

  @doc "Return all records in table"
  @spec all(name, non_neg_integer) :: list(term)
  def all(name, match_limit) do
    loop_all({table(name), :"_", match_limit})
  end

  @doc "Return all records in table, default match limit 500"
  @spec all(name) :: list(term)
  def all(name) do
    loop_all({table(name), :"_", @match_limit})
  end

  # The version is the table id, which should be swapped on
  # any update. This is a very scary thing to use, but it works
  # as long as we use it as an opaque data type.
  @spec version(name) :: version
  def version(name), do: {:vsn, table(name)}

  @spec version(name, version) :: :current | :old
  def version(name, {:vsn, version}) do
    case {table(name), version} do
      {x, x} -> :current
      _ -> :old
    end
  end

  # Private

  # @typep match :: list(term)
  # @spec loop_all(args) :: [match]
  #     args :: {[match], :ets.continuation}
  #           | {:ets.tid, :ets.match_pattern, non_neg_integer}
  #           | :"$end_of_table",
  # Collect results from ets all lookup
  defp loop_all(:"$end_of_table"), do: []
  defp loop_all({match, continuation}) do
    [match | loop_all(:ets.match_object(continuation))]
  end
  defp loop_all({:undefined, _, _}), do: []
  defp loop_all({tid, pat, limit}) do
    :lists.append(loop_all(:ets.match_object(tid, pat, limit)))
  end

  # Get table id for name from index
  @spec table(name) :: :ets.tid | :undefined
  defp table(name) do
    try do
      case :ets.lookup(@table, name) do
        [{^name, %{id: tid}}] -> tid
        [] -> :undefined
      end
    catch
      :error, :badarg ->
        Lager.warning("ets.lookup error for #{name} table #{@table}")
        :undefined
    end
  end

  # Get all data for name from index
  @spec table_info(name) :: Loader.table_state | :undefined
  defp table_info(name) do
    try do
      case :ets.lookup(@table, name) do
        [{^name, tab}] -> tab
        [] -> :undefined
      end
    catch
      :error, :badarg ->
        Lager.warning("ets.lookup error for #{name} table #{@table}")
        :undefined
    end
  end

end
