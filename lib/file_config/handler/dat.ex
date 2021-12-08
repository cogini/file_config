defmodule FileConfig.Handler.Dat do
  @moduledoc "Handler for dat files"

  require Logger

  alias FileConfig.Loader
  # alias FileConfig.Lib

  @type reason :: FileConfig.reason()

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
      {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, tab, config])
      Logger.info("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")
    end

    Loader.make_table_state(__MODULE__, name, update, tab)
  end


  # Internal functions

  @spec parse_file(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file(path, tab, _config) do
    {:ok, fh} = :file.open(path, [:read])
    {:ok, count} = decode(fh, tab, 0)
    :ok = :file.close(fh)
    {:ok, count}
  end

  @spec decode(:file.io_device(), :ets.tab(), non_neg_integer()) :: {:ok, non_neg_integer()}
  defp decode(fh, tab, count) do
    case :file.read_line(fh) do
      {:ok, line} ->
        # TODO: regex string
        case :re.run(line, '^\s*$|^//.*$') do
          :nomatch ->
            split = :re.split(line, "[\.|\n]", [:trim, {:return, :binary}])
            key = :lists.reverse(split)
            value = Jason.encode(Enum.join(split, "."))
            :ets.insert(tab, [{key, value}])
            decode(fh, tab, count + 1)

          _ ->
            decode(fh, tab, count)
        end

      _ ->
        {:ok, count}
    end
  end
end
