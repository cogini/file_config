defmodule FileConfig.Handler.Bert do
  @moduledoc "Handler for BERT files"
  # @app :file_config

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  @type namespace :: atom
  @type nrecs :: {namespace, [tuple]}

  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{^key, value}] ->
        {:ok, value}
    end
  end

  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    Lager.debug("Loading #{name} bert #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
    Lager.notice("Loaded #{name} bert #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__}
  end

  def create_table(config) do
    Lib.create_ets_table(config)
  end

  @spec parse_file(Path.t, :ets.tab, map) :: non_neg_integer
  def parse_file(path, tid, config) do
    {:ok, bin} = File.read(path)
    {:ok, terms} = decode(bin)

    {_name, records} = terms
                      |> List.flatten()
                      |> Enum.sort() # TODO: why are we sorting?
                      |> transform(config)
                      # |> validate()
    true = insert_records(tid, records)
    {:ok, length(records)}
  end

  @spec insert_records(:ets.tab, tuple() | [tuple()]) :: true
  def insert_records(tid, records) do
    :ets.insert(tid, records)
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
  @spec transform([nrecs] | nrecs, map) :: nrecs
  def transform([nrecs], config), do: transform(nrecs, config)
  def transform({name, records}, %{transform_fun: {m, f, a}}), do: apply(m, f, [{name, records}] ++ a)
  def transform({name, records}, _config), do: {name, records}

  # Validate data to make sure it matches the format
  # [{Namespace::atom(), [{Key, Val}]}]
  @spec validate(Keyword.t) :: Keyword.t | :no_return
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
