defmodule FileConfig.Handler.Bert do
  @moduledoc "Handler for BERT files"

  require Lager

  alias FileConfig.Loader

  @type namespace :: atom
  @type nrecs :: {namespace, [tuple]}

  # @impl true
  @spec lookup(Loader.table_state, term) :: term
  def lookup(%{id: tid, name: name, lazy_parse: true, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{_key, bin}] when is_binary(bin) ->
        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            # Cache parsed value
            true = :ets.insert(tid, [{key, value}])
            {:ok, value}
          {:error, reason} ->
            Lager.debug("Error parsing table #{name} key #{key}: #{inspect reason}")
            {:ok, bin}
        end
      [{_key, value}] ->
        {:ok, value}
    end
  end
  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] -> :undefined
      [{_key, value}] ->
        {:ok, value}
    end
  end

  # @impl true
  @spec load_update(Loader.name, Loader.update, :ets.tab) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)
    config = update.config

    # TODO: handle parse errors

    Lager.debug("Loading #{name} #{config.format} #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, config])
    Lager.notice("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")

    Map.merge(%{name: name, id: tid, mod: update.mod, handler: __MODULE__},
      Map.take(config, [:lazy_parse, :parser, :parser_opts]))
  end

  # @impl true
  @spec insert_records(Loader.table_state, {term, term} | [{term, term}]) :: true
  def insert_records(state, records) do
    :ets.insert(state.id, records)
  end

  # Internal functions

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, config) do
    {:ok, bin} = File.read(path)
    {:ok, terms} = decode(bin)

    {_name, records} = terms
                       |> List.flatten()
                       |> Enum.sort() # TODO: why are we sorting?
                       |> parse_records(config)
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

  @spec parse_records(list({atom, list}) | {atom, list}, map) :: {atom, list}
  defp parse_records([recs], config), do: parse_records(recs, config)
  defp parse_records(recs, %{lazy_parse: true}), do: recs
  defp parse_records({name, recs}, %{parser: parser} = config) do
    parser_opts = config[:parser_opts] || []

    values = for {key, value} <- recs do
      case parser.decode(value, parser_opts) do
        {:ok, new} ->
          {key, new}
        {:error, reason} ->
          Lager.debug("Error parsing table #{name} key #{key}: #{inspect reason}")
          {key, value}
      end
    end
    {name, values}
  end
  defp parse_records(recs, _), do: recs

  # Validate data to make sure it matches the format
  # [{Namespace::atom(), [{Key, Val}]}]
  @spec validate({atom, list(term)}) :: {atom, list(term)} | :no_return
  def validate({name, records} = u) when is_atom(name) and is_list(records), do: u
  def validate(_), do: throw(:bad_config_format)
end
