defmodule FileConfig.Handler.Dat do
  @moduledoc "Handler for dat files"

  require Logger

  alias FileConfig.Loader
  # alias FileConfig.Lib

  @spec init_config(map(), Keyword.t()) :: {:ok, map()} | {:error, term()}
  def init_config(config, _args), do: {:ok, config}

  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(%{id: tid, name: _name, config: %{lazy_parse: true, parser: parser}} = state, key) do
    parser_opts = state[:parser_opts] || []

    case :ets.lookup(tid, key) do
      [] ->
        :undefined

      [{^key, value}] ->
        parsed_value = parser.decode(value, parser_opts)
        # TODO: error handling

        # Cache parsed value
        true = :ets.insert(tid, [{key, parsed_value}])
        {:ok, parsed_value}
    end
  end

  def lookup(%{id: tid}, key) do
    case :ets.lookup(tid, key) do
      [] ->
        :undefined

      [{^key, value}] ->
        {:ok, value}
    end
  end

  @spec load_update(Loader.name(), Loader.update(), :ets.tid(), Loader.update()) ::
          Loader.table_state()
  def load_update(name, update, tid, prev) do
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
      {time, {:ok, rec}} = :timer.tc(&parse_file/3, [path, tid, config])
      Logger.info("Loaded #{name} #{config.format} #{path} #{rec} rec #{time / 1_000_000} sec")
    end

    Loader.make_table_state(__MODULE__, name, update, tid)
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) do
    :ets.insert(state.id, records)
  end

  # Internal functions

  @spec parse_file(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file(path, tid, _config) do
    {:ok, fh} = :file.open(path, [:read])
    {:ok, count} = decode(fh, tid, 0)
    :ok = :file.close(fh)
    {:ok, count}
  end

  @spec decode(:file.io_device(), :ets.tab(), non_neg_integer()) :: {:ok, non_neg_integer()}
  defp decode(fh, tid, count) do
    case :file.read_line(fh) do
      {:ok, line} ->
        # TODO: regex string
        case :re.run(line, '^\s*$|^//.*$') do
          :nomatch ->
            split = :re.split(line, "[\.|\n]", [:trim, {:return, :binary}])
            key = :lists.reverse(split)
            # value = :jsx.encode(Enum.join(split, "."))
            value = Jason.encode(Enum.join(split, "."))
            :ets.insert(tid, [{key, value}])
            decode(fh, tid, count + 1)

          _ ->
            decode(fh, tid, count)
        end

      _ ->
        {:ok, count}
    end
  end
end
