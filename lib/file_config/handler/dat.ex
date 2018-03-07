defmodule FileConfig.Handler.Dat do
  @moduledoc "Handler for dat files"
  # @app :file_config

  require Lager

  alias FileConfig.Loader
  alias FileConfig.Lib

  def create_table(config) do
    Lib.create_ets_table(config)
  end

  @spec load_update(Loader.name, Loader.update, :ets.tid) :: Loader.table_state
  def load_update(name, update, tid) do
    # Assume updated files contain all records
    {path, _state} = hd(update.files)

    Lager.debug("Loading #{name} dat #{path}")
    {time, {:ok, rec}} = :timer.tc(__MODULE__, :parse_file, [path, tid, update.config])
    Lager.notice("Loaded #{name} dat #{path} #{rec} rec #{time / 1_000_000} sec")

    %{name: name, id: tid, mod: update.mod, handler: __MODULE__}
  end

  @spec parse_file(Path.t, :ets.tab, map) :: {:ok, non_neg_integer}
  def parse_file(path, tid, _config) do
    {:ok, fh} = :file.open(path, [:read])
    {:ok, count} = decode(fh, tid, 0)
    :ok = :file.close(fh)
    {:ok, count}
  end

  @spec decode(:file.io_device(), :ets.tid, non_neg_integer) :: {:ok, non_neg_integer}
  def decode(fh, tid, count) do
    case :file.read_line(fh) do
      {:ok, line} ->
        case :re.run(line, '^\s*$|^//.*$') do # TODO: regex string
          :nomatch ->
            split = :re.split(line, "[\.|\n]", [:trim, {:return, :binary}])
            key = :lists.reverse(split)
            value = :jsx.encode(Enum.join(split, "."))
            :ets.insert(tid, [{key, value}])
            decode(fh, tid, count + 1)
          _ ->
            decode(fh, tid, count)
        end
      _ -> {:ok, count}
    end
  end
end
