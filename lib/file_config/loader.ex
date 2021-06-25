defmodule FileConfig.Loader do
  @moduledoc "Load files"
  @extensions [".bert", ".csv", ".dat"]

  use GenServer
  require Logger

  # @typedoc ""
  @type files :: map()
  @type file_config :: map()
  @type table_state :: map()
  @type update :: map()
  @type name :: atom()

  # GenServer callbacks

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  def init(config) do
    Process.flag(:trap_exit, true) # Die gracefully
    __MODULE__ = :ets.new(__MODULE__, [:set, :public, :named_table, {:read_concurrency, true}])

    data_dirs = config[:data_dirs] || []
    file_configs = process_file_configs(config[:files] || [])
    check_delay = config[:check_delay] || 5000

    {old_tables, new_files} = check_files(%{}, %{data_dirs: data_dirs, file_configs: file_configs}, true)
    # new_files = Enum.reject(new_files, &is_async/1) |> Enum.into(%{})

    free_binary_memory()
    {:ok, %{ref: :erlang.start_timer(check_delay, self(), :reload),
      old_tables: old_tables, files: new_files,
      file_configs: file_configs, data_dirs: data_dirs, check_delay: check_delay}}
  end

  @spec handle_info(term, map) :: {:noreply, map}
  def handle_info({:timeout, ref, :reload}, state) do
    %{ref: ^ref, files: files, old_tables: old_old_tables} = state # TODO check ref matching
    {old_tables, new_files} = check_files(files, state)
    delete_tables(old_old_tables)

    free_binary_memory()
    {:noreply, %{state | ref: :erlang.start_timer(state.check_delay, self(), :reload),
      files: new_files, old_tables: old_tables}}
  end
  def handle_info(_event, state) do
    {:noreply, state}
  end

  # API

  @doc "Check for changes to configured files"
  @spec check_files(files(), map()) :: {[:ets.tid()], files()}
  def check_files(old_files, state, boot) do
    new_files = get_files(state.data_dirs, state.file_configs, boot)
    # for {name, value} <- new_files do
    #   Logger.warning("new_files: #{name} #{inspect(value)}")
    # end

    changed_files = get_changed_files(new_files, old_files)
    # for {name, value} <- changed_files do
    #   Logger.error("changed_file: #{name} #{inspect value}")
    # end

    new_tables = process_changed_files(changed_files)
    # for new_table <- new_tables do
    #   Logger.debug("new_table: #{inspect new_table}")
    # end

    old_tables = update_table_index(new_tables)

    notify_update(new_tables)

    {old_tables, new_files}
  end

  @doc "Process file configs to set defaults"
  @spec process_file_configs(list({name, map})) :: list(file_config)
  def process_file_configs(files) do
    for {config_name, config} <- files do
      # Logger.info("Loading #{config_name} #{inspect config}")
      name = config[:name] || config_name
      file = config.file
      format = config[:format] || ext_to_format(Path.extname(file))
      handler = config[:handler] || format_to_handler(format)
      regex = Regex.compile!("/#{file}$")
      Map.merge(config, %{
        name: name,
        format: format,
        handler: handler,
        regex: regex,
      })
    end
  end

  @doc "Look for files in data dirs"
  @spec get_files(list(Path.t()), list(file_config)) :: files()
  def get_files(data_dirs, file_configs, boot \\ false) do
    path_configs =
      for data_dir <- data_dirs,
        path <- list_files(data_dir),
        config <- file_configs,
        Regex.match?(config.regex, path), do: {path, config}

    async =
      fn
        {path, %{config: %{async: true}}} -> boot
        _ -> false
      end

    path_configs = Enum.reject(path_configs, &async/1) |> Enum.into(%{})

    files =
      for {path, config} <- path_configs,
        {:ok, stat} = File.stat(path),
        stat.size > 0, do: {path, config, %{mod: stat.mtime}}

    # Logger.warning("files: #{inspect(files)}")

    # for {path, config, mtime} <- files do
    #   Logger.warning("file: #{inspect(path)} #{inspect(config)} #{inspect(mtime)}")
    # end

    files
    |> Enum.reduce(%{}, &group_by_name/2)
    |> Enum.reduce(%{}, &sort_by_mod/2)
  end

  @doc "List files in dir with config file extensions"
  @spec list_files(Path.t) :: [Path.t]
  def list_files(dir) do
    {:ok, files} = File.ls(dir)
    for file <- files, Path.extname(file) in @extensions, do: Path.join(dir, file)
  end

  @doc "Collect multiple files for the same name"
  def group_by_name({path, %{name: name} = config, state}, acc) do
    case Map.fetch(acc, name) do
      :error ->
        Map.put(acc, name, %{files: [{path, state}], config: config})
      {:ok, %{files: files}} ->
        put_in(acc[name].files, [{path, state} | files])
    end
  end

  @doc "Sort files by modification time and set overall latest time"
  @spec sort_by_mod({name(), update()}, map()) :: map()
  def sort_by_mod({name, v}, acc) do
    # Sort files by modification time (newer to older)
    files = Enum.sort(v.files, fn({_, %{mod: a}}, {_, %{mod: b}}) -> a > b end)
    {_path, %{mod: mod}} = hd(files)
    Map.put(acc, name, Map.merge(v, %{files: files, mod: mod}))
  end

  @doc "Determine if files have changed since last run"
  @spec get_changed_files(files(), files()) :: files()
  def get_changed_files(new_files, old_files) do
    Enum.reduce(new_files, %{}, fn({name, v}, acc) ->
      case Map.fetch(old_files, name) do
        :error -> # New file
          Map.put(acc, name, v)
        {:ok, %{mod: prev_mod}} -> # Existing file
          # Get files that have been modified since last time
          mod_files = for {_p, %{mod: mod}} = f <- v.files, mod > prev_mod, do: f
          if length(mod_files) > 0 do
            # Map.put(acc, name, %{v | files: mod_files}) # only modified files
            Map.put(acc, name, v) # keep all files
          else
            acc
          end
      end
    end)
  end

  @doc "Load data from changed files"
  @spec process_changed_files(files) :: list(table_state)
  def process_changed_files(changed_files) do
    for {name, update} <- changed_files do
      config = update.config
      tid = maybe_create_table(name, update.mod, config)
      Logger.debug("Loading file #{name}")
      config.handler.load_update(name, update, tid)
    end
  end

  @doc "Create table if new/update"
  @spec maybe_create_table(name, :calendar.datetime, map) :: :ets.tid
  def maybe_create_table(name, mod, config) do
    case :ets.lookup(__MODULE__, name) do
      [] ->
        Logger.debug("Creating ETS table #{name} new")
        create_ets_table(config)
      [{_name, %{id: tid, mod: m}}] when m == mod ->
        Logger.debug("Using existing ETS table #{name}")
        tid
      [{_name, %{}}] ->
        Logger.debug("Creating ETS table #{name} update")
        create_ets_table(config)
    end
  end

  @spec create_ets_table(map) :: :ets.tid
  def create_ets_table(%{name: name, ets_opts: ets_opts}) do
    :ets.new(name, ets_opts)
  end
  def create_ets_table(%{name: name}) do
    :ets.new(name, [:set, :public, {:read_concurrency, true}, {:write_concurrency, true}])
  end

  @spec update_table_index([table_state]) :: [:ets.tid]
  def update_table_index(new_tables) do
    # Get ids of tables which already exist and we are replacing
    old_tables = Enum.reduce(new_tables, [], fn(%{name: name}, acc) ->
      case :ets.lookup(__MODULE__, name) do
        [] ->
          # Logger.debug("ETS new_table: #{name}")
          acc
        [{_name, %{id: tid}}] ->
          # Logger.debug("ETS old_table: #{name} #{inspect tid}")
          [{name, tid} | acc]
      end
    end)

    # Update index with ids of current tables
    table_tuples = for %{name: name} = table <- new_tables, do: {name, table}
    :ets.insert(__MODULE__, table_tuples)

    # The replaced tables need to be deleted. To avoid having a window where
    # data is not available, we don't delete them immediately, we do it on
    # the next cycle.
    old_tables
  end

  @spec delete_tables(list(:ets.tab)) :: :ok
  def delete_tables(tables) do
    for {name, tid} <- tables do
      Logger.debug("Deleting old ETS table: #{inspect name} #{inspect tid}")
      :ets.delete(tid)
    end
    :ok
  end

  @doc "Notify subscribers about updates"
  @spec notify_update([map]) :: :ok
  def notify_update(tables) do
    for %{name: name} <- tables do
      # Logger.debug("notify_update: #{inspect name}")
      FileConfig.EventProducer.sync_notify({:load, name})
    end
    :ok
  end

  @doc "Get format from extension"
  @spec ext_to_format(Path.t) :: atom
  def ext_to_format(ext)
  def ext_to_format(".bert"), do: :bert
  def ext_to_format(".csv"), do: :csv
  def ext_to_format(".dat"), do: :dat

  @spec format_to_handler(atom) :: module
  def format_to_handler(ext)
  def format_to_handler(:bert), do: FileConfig.Handler.Bert
  def format_to_handler(:csv), do: FileConfig.Handler.Csv
  def format_to_handler(:dat), do: FileConfig.Handler.Dat

  def list_index do
    :ets.foldl(fn({key, value}, acc) -> [{key, value} | acc] end, [], __MODULE__)
  end

  # defp is_async({_name, %{config: %{async: true}}}), do: true
  # defp is_async(_), do: false

  def free_binary_memory do
    {:binary_memory, binary_memory} = :recon.info(self(), :binary_memory)
    if binary_memory > 50_000_000 do
      # Manually trigger garbage collection to clear refc binary memory
      Logger.debug("Forcing garbage collection")
      :erlang.garbage_collect(self())
    end
  end
end
