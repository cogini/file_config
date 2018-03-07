defmodule FileConfig do
  @moduledoc """
  Documentation for FileConfig.
  """

  require Lager

  @table FileConfig.Loader
  @match_limit 500

   @doc """
   Hello world.
  
   ## Examples
  
       iex> FileConfig.hello
       :world
  
   """
   def hello do
     :world
   end

  @type name :: term
  @type version :: {:vsn, term}

  # -opaque version() :: {vsn,term()}.
  # -export_type([namespace/0, version/0]).

  # PUBLIC INTERFACE

  @spec read(name, term) :: :undefined | {:ok, term}
  def read(name, key) do
    case table_info(name) do
      :undefined ->
        :undefined
      %{handler: handler} = table_state ->
        handler.lookup(table_state, key)
      %{id: tid} -> # fallback
        case :ets.lookup(tid, key) do
          [] -> :undefined
          [{^key, value}] ->
            {:ok, value}
        end
    end
  end

  @spec all(atom) :: list
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

  # PRIVATE

  @spec table(name) :: :ets.tid | :undefined
  defp table(name) do
    try do
      case :ets.lookup(@table, name) do
        [{^name, %{id: tid}}] -> tid
        [] -> :undefined
      end
    catch
      :error, :badarg ->
        Lager.warning("Could not lookup name #{name} in table #{@table}")
        :undefined
    end
  end

  @spec table_info(name) :: Loader.table_state | :undefined
  defp table_info(name) do
    try do
      case :ets.lookup(@table, name) do
        [{^name, tab}] -> tab
        [] -> :undefined
      end
    catch
      :error, :badarg ->
        Lager.warning("Could not lookup name #{name} in table #{@table}")
        :undefined
    end
  end

  # @typep match :: list(term)
  # @spec loop_all(args) :: [match]
  #     Args :: {[match], :ets.continuation}
  #           | {:ets.tid, :ets.match_pattern, non_neg_integer}
  #           | :"$end_of_table",
  defp loop_all(:"$end_of_table"), do: []
  defp loop_all({match, continuation}) do
    [match | loop_all(:ets.match_object(continuation))]
  end
  defp loop_all({tid, pat, limit}) do
    :lists.append(loop_all(:ets.match_object(tid, pat, limit)))
  end

  # @spec decode_binary(:ets.tid, name, term, term) :: term
  # defp decode_binary(tid, name, key, value) when is_binary(value) do
  #   if :jsx.is_json(value) do
  #     value = :jsx.decode(value, [{:labels, :atom}, :return_maps])
  #     # |> merge_defaults(name, value)
  #     true = :ets.insert(tid, {key, value})
  #     value
  #   else
  #     Lager.debug("Invalid binary JSON for table #{name} key #{key}")
  #     value
  #   end
  # end
  # defp decode_binary(_tid, _name, _key, value), do: value

  # @spec parse_json(binary | term) :: term
  # def parse_json(value) when is_binary(value) do
  #   :jsx.decode(value, [{:labels, :atom}, :return_maps])
  # end
  # def parse_json(value), do: value

  # @spec merge_defaults(map, atom) :: map
  # def merge_defaults(value, name) when is_map(value) do
  #   config = Application.get_env(@app, :bertconf, [])
  #   ns_config = config[:namespaces] || %{}
  #   defaults = ns_config[name] || %{}
  #   Map.merge(defaults, value)
  # end
  # def merge_defaults(_, value), do: value

  # @spec read_tc(namespace, term) :: :undefined | {:ok, term}
  # def read_tc(name_space, key) do
  #   Metrics.tc([:bertconf, :read, :duration], [name: name_space], :bertconf, :read, [name_space, key])
  # end

end
