defmodule FileConfig.Loader do
  @moduledoc "Load files"
  @extensions [".bert", ".csv", ".dat", ".log", ".json"]

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

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true) # Die gracefully
    __MODULE__ = :ets.new(__MODULE__, [:set, :public, :named_table, {:read_concurrency, true}])

    # Directories to look for files in
    data_dirs = args[:data_dirs] || []

    {:ok, file_configs} = init_config(args)
    # for file_config <- file_configs do
    #   Logger.info("config: #{inspect(file_config)}")
    # end

    # How often to check for new files, in ms
    check_delay = args[:check_delay] || 5000

    {old_tables, new_files} = check_files(%{}, %{data_dirs: data_dirs, file_configs: file_configs}, true)

    free_binary_memory()

    state = %{
      old_tables: old_tables,
      files: new_files,
      file_configs: file_configs,
      data_dirs: data_dirs,
      check_delay: check_delay,
    }

    {:ok, state, check_delay}
  end

  @impl true
  @spec handle_info(term(), map()) :: {:noreply, map()}
  def handle_info(:timeout, state) do
    # %{ref: ^ref, files: files, old_tables: old_old_tables} = state # TODO check ref matching
    %{files: files, old_tables: old_old_tables} = state # TODO check ref matching
    {old_tables, new_files} = check_files(files, state)
    delete_tables(old_old_tables)

    free_binary_memory()

    {:noreply, %{state | files: new_files, old_tables: old_tables}, state.check_delay}
  end

  def handle_info(msg, state) do
    Logger.debug("Unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  # API

  @doc "Check for changes to configured files"
  @spec check_files(files(), map()) :: {[:ets.tid()], files()}
  def check_files(old_files, state, init \\ false) do
    new_files = get_files(state.data_dirs, state.file_configs, init)

    new_tables =
      for {name, update} <- new_files,
          {:ok, prev} = get_prev(name, old_files),
          modified?(name, update, prev)
      do
        tid = maybe_create_table(update)
        config = update.config
        config.handler.load_update(name, update, tid, prev)
      end

    for table <- new_tables do
      Logger.debug("table_state: #{inspect(table)}")
    end

    notify_update(new_tables)

    old_tables = update_table_index(new_tables)

    {old_tables, new_files}
  end

  defp get_prev(name, old_files)
  defp get_prev(_name, nil), do: {:ok, nil}
  defp get_prev(name, old_files), do: {:ok, old_files[name]}

  @doc "Set config defaults"
  @spec init_config(Keyword.t()) :: {:ok, list(file_config())}
  def init_config(args) do
    files = args[:files] || []
    state_dir = args[:state_dir]

    results =
      for {config_name, config} <- files do
        # Logger.info("Loading config #{config_name} #{inspect(config)}")

        # Name of table
        name = config[:name] || config_name

        # Pattern matching input files
        file = config[:file]
        regex = config[:regex] || "/#{file}$"
        regex = Regex.compile!(regex)

        format = config[:format] || ext_to_format(Path.extname(file))

        # Module to handle file
        handler = config[:handler] || format_to_handler(format)

        derived_config = %{
          state_dir: state_dir,
          name: name,
          format: format,
          handler: handler,
          regex: regex,
        }

        config = Map.merge(config, derived_config)
        {:ok, config} = handler.init_config(config, args)
        config
      end

    {:ok, results}
  end

  @doc "Find files matching pattern."
  @spec get_files(list(Path.t()), list(file_config)) :: files()
  def get_files(data_dirs, file_configs, init \\ false) do
    path_configs =
      for data_dir <- data_dirs,
        path <- list_files(data_dir),
        config <- file_configs,
        Regex.match?(config.regex, path), do: {path, config}

    # Skip files marked as async on initial startup pass.
    # This allows the program to start up more quickly and
    # handle reuests, processing the files on the next scheduled run.
    is_async =
      fn
        {path, %{async: true}} ->
          if init do
            Logger.debug("Skipping async file #{path}")
            true
          else
            false
          end
        _ ->
          false
      end

    path_configs = Enum.reject(path_configs, is_async)

    files =
      for {path, config} <- path_configs,
        {:ok, stat} = File.stat(path),
        stat.size > 0, do: {path, config, %{mod: stat.mtime}}

    # for {path, config, mtime} <- files do
    #   Logger.debug("file: #{inspect(path)} #{inspect(config)} #{inspect(mtime)}")
    # end

    files
    |> Enum.reduce(%{}, &group_by_name/2)
    |> Enum.reduce(%{}, &sort_by_mod/2)
  end

  @doc "List files in dir matching configured file extensions."
  @spec list_files(Path.t()) :: [Path.t()]
  def list_files(dir) do
    with {:ok, _stat} <- File.stat(dir),
         {:ok, files} <- File.ls(dir)
    do
      for file <- files,
          Path.extname(file) in @extensions
      do
        Path.join(dir, file)
      end
    else
      err ->
        Logger.debug("#{inspect(err)}")
        []
    end
  end

  @doc "Collect multiple files for the same name"
  @spec group_by_name({Path.t(), map(), map()}, map()) :: map()
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
    files = Enum.sort(v.files, fn {_, %{mod: a}}, {_, %{mod: b}} -> a >= b end)
    {_path, %{mod: mod}} = hd(files)
    Map.put(acc, name, Map.merge(v, %{files: files, mod: mod}))
  end

  @doc "Determine if files have changed since last run"
  @spec get_changed_files(files(), files()) :: files()
  def get_changed_files(new_files, old_files) do
    Enum.reduce(new_files, %{},
      fn {name, v}, acc ->
        case Map.fetch(old_files, name) do
          :error -> # New file
            Map.put(acc, name, v)

          {:ok, %{mod: prev_mod}} -> # Existing file
            # Get files that have been modified since last time
            mod_files = for {_p, %{mod: mod}} = f <- v.files, mod > prev_mod, do: f
            # TODO: check for empty not length
            if Enum.empty?(mod_files) do
              acc
            else
              # Map.put(acc, name, %{v | files: mod_files}) # only modified files
                Map.put(acc, name, v) # keep all files
            end
        end
      end)
  end

  @doc "Get just changed files or all based on config"
  @spec changed_files?(map(), map()) :: map()
  def changed_files?(update, prev)

  # All files
  def changed_files?(%{config: %{changed: false}} = update, _), do: update

  # Only files which have been changed since previous run
  def changed_files?(update, %{mod: prev_mod}) do
    files = for {_p, %{mod: mod}} = f <- update.files, mod > prev_mod, do: f
    %{update | files: files}
  end

  def changed_files?(update, _), do: update

  @doc "Get latest file or all based on config"
  @spec latest_file?(map()) :: list({Path.t(), map()})
  def latest_file?(%{config: %{update: :latest}} = update), do: [hd(update.files)]

  def latest_file?(update), do: update.files

  # @doc "Load data from changed files"
  # @spec process_changed_files(files()) :: list(table_state())
  # def process_changed_files(changed_files) do
  #   for {name, update} <- changed_files do
  #     config = update.config
  #     tid = maybe_create_table(name, update.mod, config)
  #     Logger.debug("Loading file #{name}")
  #     config.handler.load_update(name, update, tid)
  #   end
  # end
  #   # changed_files = get_changed_files(new_files, old_files)
  #   # new_tables = process_changed_files(changed_files)

  # Create table_sate data
  @spec make_table_state(module(), name(), map(), :ets.tid()) :: table_state()
  def make_table_state(handler, name, update, tid) do
    %{config: config, mod: mod} = update
    Map.merge(%{name: name, id: tid, mod: mod, handler: handler},
      Map.take(config, [:lazy_parse, :parser, :parser_opts]))
  end

  # @doc "Load data from files"
  # @spec process_files(files(), files()) :: list(table_state())
  # def process_files(new_files, old_files) do
  #   for {name, update} <- new_files do
  #     prev = old_files[name]
  #     Logger.debug("#{name}: #{inspect(update)} #{inspect(prev)}")
  #
  #     config = update.config
  #     tid = maybe_create_table(update)
  #
  #     if modified?(name, update, prev) do
  #       config.handler.load_update(name, update, tid, prev)
  #
  #       FileConfig.EventProducer.sync_notify({:load, name})
  #     end
  #
  #   end
  # end

  @doc "Whether files have been modified/created since last run"
  @spec modified?(map) :: boolean()
  def modified?(%{name: name, update: update, prev: prev}) do
    modified?(name, update, prev)
  end

  @spec modified?(name(), map(), map()) :: boolean()
  def modified?(name, update, prev)

  def modified?(_name, %{mod: update_mod}, %{mod: prev_mod}) when update_mod == prev_mod do
    # Logger.debug("#{name}: Files not modified")
    false
  end

  def modified?(name, _ , _) do
    Logger.debug("#{name}: Files modified")
    true
  end


  @doc "Create table if new/update"
  def maybe_create_table(update) do
    maybe_create_table(update.mod, update.config)
  end

  @spec maybe_create_table(:calendar.datetime(), map()) :: :ets.tid()
  def maybe_create_table(mod, config) do
    name = config.name
    case :ets.lookup(__MODULE__, name) do
      [] ->
        tid = create_ets_table(config)
        Logger.debug("Created ETS table #{name} new #{inspect(tid)}")
        tid

      [{_name, %{id: tid, mod: m}}] when m == mod ->
        Logger.debug("Using existing ETS table #{name} #{inspect(tid)}")
        tid

      [{_name, %{}}] ->
        tid = create_ets_table(config)
        Logger.debug("Created ETS table #{name} update #{inspect(tid)}")
        tid
    end
  end

  @spec create_ets_table(map()) :: :ets.tid()
  def create_ets_table(%{name: name, ets_opts: ets_opts}) do
    Logger.debug("Creating ETS table with opts #{inspect(ets_opts)}")
    :ets.new(name, ets_opts)
  end

  def create_ets_table(%{name: name}) do
    ets_opts = [:set, :public, {:read_concurrency, true}, {:write_concurrency, true}]
    Logger.debug("Creating ETS table with default opts #{inspect(ets_opts)}")
    :ets.new(name, ets_opts)
  end

  @spec update_table_index([table_state()]) :: [:ets.tid()]
  def update_table_index(new_tables) do
    # Get ids of tables which already exist and we are replacing
    old_tables =
      Enum.reduce(new_tables, [],
        fn %{name: name}, acc ->
          case :ets.lookup(__MODULE__, name) do
            [] ->
              Logger.debug("ETS new table: #{name}")
              acc

            [{_name, %{id: tid}}] ->
              Logger.debug("ETS old table: #{name} #{inspect(tid)}")
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

  @spec delete_tables(list(:ets.tab())) :: :ok
  def delete_tables(tables) do
    for {name, tid} <- tables do
      Logger.debug("Deleting ETS table: #{inspect(name)} #{inspect(tid)}")
      :ets.delete(tid)
    end

    :ok
  end

  @doc "Notify subscribers about updates"
  @spec notify_update([map()]) :: :ok
  def notify_update(tables) do
    for %{name: name} <- tables do
      # Logger.debug("notify_update: #{inspect name}")
      FileConfig.EventProducer.sync_notify({:load, name})
    end

    :ok
  end

  @doc "Get format from extension"
  @spec ext_to_format(Path.t()) :: atom()
  def ext_to_format(ext)
  def ext_to_format(".bert"), do: :bert
  def ext_to_format(".csv"), do: :csv
  def ext_to_format(".dat"), do: :dat
  def ext_to_format(".json"), do: :json

  @spec format_to_handler(atom()) :: module()
  def format_to_handler(ext)
  def format_to_handler(:bert), do: FileConfig.Handler.Bert
  def format_to_handler(:csv), do: FileConfig.Handler.Csv
  def format_to_handler(:dat), do: FileConfig.Handler.Dat

  def list_index do
    :ets.foldl(fn({key, value}, acc) -> [{key, value} | acc] end, [], __MODULE__)
  end

  # defp is_async({_name, %{config: %{async: true}}}), do: true
  # defp is_async(_), do: false

  # Manually trigger garbage collection to clear refc binary memory
  def free_binary_memory do
    {:binary_memory, binary_memory} = :recon.info(self(), :binary_memory)
    if binary_memory > 50_000_000 do
      Logger.debug("Forcing garbage collection")
      :erlang.garbage_collect(self())
    end
  end
end
