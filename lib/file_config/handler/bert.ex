defmodule FileConfig.Handler.Bert do
  @moduledoc "Handler for BERT files"

  require Logger

  alias FileConfig.Loader

  @type reason :: FileConfig.reason()

  @type namespace :: atom()
  @type nrecs :: {namespace(), [tuple()]}

  @spec init_config(map(), Keyword.t()) :: {:ok, map()} | {:error, reason()}
  def init_config(config, _args), do: {:ok, config}

  @spec read(Loader.table_state(), term()) :: {:ok, term()} | nil | {:error, reason()}
  def read(%{id: tab, lazy_parse: true, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []

    case :ets.lookup(tab, key) do
      [] ->
        nil

      [{_key, bin}] when is_binary(bin) ->
        case parser.decode(bin, parser_opts) do
          {:ok, value} ->
            # Cache parsed value
            true = :ets.insert(tab, [{key, value}])
            {:ok, value}

          {:error, reason} ->
            {:error, {:parse, bin, reason}}
        end

      [{_key, value}] ->
        # Cached result
        {:ok, value}
    end
  end

  def read(%{id: tab}, key) do
    case :ets.lookup(tab, key) do
      [] ->
        nil

      [{_key, value}] ->
        {:ok, value}
    end
  end

  @deprecated "Use read/2 instead"
  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(state, key) do
    case read(state, key) do
      {:ok, value} ->
        value

      nil ->
        :undefined
    end
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) ::
          :ok | {:error, FileConfig.reason()}
  # @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) do
    # Always succeeds or perhaps throws
    :ets.insert(state.id, records)
    :ok
  end

  @spec load_update(Loader.name(), Loader.update(), :ets.tab(), Loader.update()) ::
          Loader.table_state()
  def load_update(name, update, tab, prev) do
    config = update.config

    files =
      update
      |> Loader.changed_files?(prev)
      |> Loader.latest_file?()
      # Files stored latest first, process in chronological order
      |> Enum.reverse()

    for {path, state} <- files do
      Logger.debug("Loading #{name} #{config.format} #{path} #{inspect(state.mod)}")
      # TODO: handle parse errors
      {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, tab, config])
      Logger.info("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")
    end

    Loader.make_table_state(__MODULE__, name, update, tab)
  end

  # Internal functions

  @spec parse_file(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file(path, tab, config) do
    {:ok, bin} = File.read(path)
    {:ok, terms} = decode(bin)

    {_name, records} =
      terms
      |> List.flatten()
      # TODO: why are we sorting?
      |> Enum.sort()
      |> parse_records(config)

    # |> validate()

    true = :ets.insert(tab, records)
    {:ok, length(records)}
  end

  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(bin) when is_binary(bin) do
    try do
      {:ok, :erlang.binary_to_term(bin)}
    catch
      _type, exception ->
        {:error, exception}
    end
  end

  @spec parse_records(list({atom(), list()}) | {atom(), list()}, map()) :: {atom(), list()}
  defp parse_records([recs], config), do: parse_records(recs, config)

  defp parse_records(recs, %{lazy_parse: true}), do: recs

  defp parse_records({name, recs}, %{parser: parser} = config) do
    parser_opts = config[:parser_opts] || []

    values =
      for {key, value} <- recs do
        case parser.decode(value, parser_opts) do
          {:ok, new} ->
            {key, new}

          {:error, reason} ->
            Logger.warning("Error parsing table #{name} key #{key}: #{inspect(reason)}")
            # {key, {:error, {:parse, bin, reason}}}
            {key, value}
        end
      end

    {name, values}
  end

  defp parse_records(recs, _), do: recs

  # Validate data to make sure it matches the format
  # [{Namespace::atom(), [{Key, Val}]}]
  @spec validate({atom(), list(term())}) :: {atom(), list(term())} | :no_return
  def validate({name, records} = u) when is_atom(name) and is_list(records), do: u
  def validate(_), do: throw(:bad_config_format)
end
