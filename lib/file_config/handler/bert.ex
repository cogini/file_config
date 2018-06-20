defmodule FileConfig.Handler.Bert do
  @moduledoc "Handler for BERT files"

  require Lager

  alias FileConfig.Loader

  @type namespace :: atom
  @type nrecs :: {namespace, [tuple]}

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name, lazy_parse: true, data_parser: data_parser}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        parsed_value = data_parser.parse_value(name, key, value)
        # Cache parsed value
        true = :ets.insert(tid, [{key, parsed_value}])
        {:ok, parsed_value}
    end
  end
  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        {:ok, value}
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    Lager.debug("Loading #{name} bert #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
    Lager.notice("Loaded #{name} bert #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__}
  end

  # @impl true
  @spec insert_records(Loader.table_state, tuple | [tuple]) :: true
  def insert_records(table, records) do
    :ets.insert(table.id, records)
  end

  # Internal functions
  @spec parse_file(Path.t, :ets.tid, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, config) do
    {:ok, bin} = File.read(path)
    {:ok, terms} = decode(bin)

    {_name, records} = terms
              |> List.flatten()
              |> Enum.sort() # TODO: why are we sorting?
              |> parse_data(config)
              # |> validate()

    true = :ets.insert(tid, records)
    {:ok, length(records)}
  end

  @spec decode(binary) :: {:ok, term} | {:error, term}
  def decode(bin) when is_binary(bin) do
    try do
      {:ok, :erlang.binary_to_term(bin)}
    catch
      _type, exception ->
        {:error, exception}
    end
  end

  # @doc "Optionally apply a transformation function to data"
  # @spec transform([nrecs] | nrecs, map) :: nrecs
  # defp transform([nrecs], config), do: transform(nrecs, config)
  # defp transform({name, records}, %{transform_fun: {m, f, a}}), do: apply(m, f, [{name, records}] ++ a)
  # defp transform({name, records}, _config), do: {name, records}

  @spec parse_data(list({atom, list}) | {atom, list}, map) :: list
  defp parse_data([nrecs], config), do: parse_data(nrecs, config)
  defp parse_data({_name, _recs} = nrecs, %{lazy_parse: true}), do: nrecs
  defp parse_data({_name, _recs} = nrecs, %{data_parser: nil}), do: nrecs
  defp parse_data({_name, _recs} = nrecs, %{data_parser: FileConfig.DataParser.Noop}), do: nrecs
  defp parse_data({name, recs}, %{data_parser: data_parser}) do
    {name, Enum.map(recs, &(data_parser.parse_value(&1)))}
  end

  # Validate data to make sure it matches the format
  # [{Namespace::atom(), [{Key, Val}]}]
  @spec validate({atom, list(term)}) :: {atom, list(term)} | :no_return
  def validate({name, records} = u) when is_atom(name) and is_list(records), do: u
  def validate(_), do: throw(:bad_config_format)

  # @spec validate_namespaces(Keyword.t) :: boolean
  # #  def validate_namespaces([]), do: true
  # def validate_namespaces([{ns, _val} | rest]) when is_atom(ns) do
  #   validate_namespaces(rest)
  # end
  # def validate_namespaces(_), do: false

  # @spec validate_keyval(Keyword.t) :: boolean
  # def validate_keyval([]), do: true
  # def validate_keyval([{_ns, l=[_|_]} | rest]) do
  #   validate_keyval1(l) and validate_keyval(rest)
  # end
  # def validate_keyval(_), do: false

  # @spec validate_keyval1(Keyword.t) :: boolean
  # def validate_keyval1([]), do: true
  # def validate_keyval1([{_k,_v} | rest]), do: validate_keyval1(rest)
  # def validate_keyval1(_), do: false

end
