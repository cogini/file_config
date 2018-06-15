defmodule FileConfig.Loader do
  @moduledoc "Load files"
  @app :file_config
  @extensions [".bert", ".csv", ".dat"]

  use GenServer
  require Lager

  # @typedoc ""
  @type files :: map
  @type file_config :: map
  @type table_state :: map
  @type update :: map
  @type name :: atom

  # GenServer callbacks

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  def init(_args) do
    Process.flag(:trap_exit, true) # Die gracefully
    __MODULE__ = :ets.new(__MODULE__, [:set, :public, :named_table, {:read_concurrency, true}])

    {old_tables, new_files} = check_files(%{})

    free_binary_memory()
    {:ok, %{ref: :erlang.start_timer(check_delay(), self(), :reload),
            old_tables: old_tables, files: new_files}}
  end

  @spec handle_info(term, map) :: {:noreply, map}
  def handle_info({:timeout, ref, :reload}, %{ref: ref, files: files, old_tables: old_old_tables} = state) do # TODO check ref matching
    {old_tables, new_files} = check_files(files)
    delete_tables(old_old_tables)

    free_binary_memory()
    {:noreply, %{state | ref: :erlang.start_timer(check_delay(), self(), :reload),
      files: new_files, old_tables: old_tables}}
  end
  def handle_info(_event, state) do
    {:noreply, state}
  end

  # API

  @doc "Check for changes to configured files"
  @spec check_files(files) :: {[:ets.tid], files}
  def check_files(old_files) do
    file_configs = get_file_configs()
    data_dirs = data_dirs()

    new_files = get_files(data_dirs, file_configs)
    changed_files = get_changed_files(new_files, old_files)

    new_tables = process_changed_files(changed_files)

    old_tables = update_table_index(new_tables)
    notify_update(new_tables)

    {old_tables, new_files}
  end

  @doc "Get list of configured files"
  @spec get_file_configs() :: list(file_config)
  def get_file_configs do
    process_file_configs(Application.get_env(@app, :files, []))
  end

  @doc "Process file configs to set defaults"
  @spec process_file_configs(list({name, map})) :: list(file_config)
  def process_file_configs(files) do
    for {config_name, config} <- files do
      # Lager.info("Loading #{config_name} #{inspect config}")
      name = config[:name] || config_name
      file = config.file
      format = config[:format] || ext_to_format(Path.extname(file))
      handler = config[:handler] || format_to_handler(format)
      regex = Regex.compile!("/#{file}$")
      Map.merge(config, %{name: name, format: format, regex: regex, handler: handler})
    end
  end

  @doc "Look for files in data dirs"
  @spec get_files(list(Path.t), list(file_config)) :: files
  def get_files(data_dirs, file_configs) do
    path_configs = for data_dir <- data_dirs,
      path <- list_files(data_dir),
      config <- file_configs,
      Regex.match?(config.regex, path), do: {path, config}

    files = for {path, config} <- path_configs,
      {:ok, stat} = File.stat(path),
      stat.size > 0, do: {path, config, %{mod: stat.mtime}}

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
  @spec sort_by_mod({name, update}, map) :: map
  def sort_by_mod({name, v}, acc) do
    # Sort files by modification time (newer to older)
    files = Enum.sort(v.files, fn({_, %{mod: a}}, {_, %{mod: b}}) -> a > b end)
    {_path, %{mod: mod}} = hd(files)
    Map.put(acc, name, Map.merge(v, %{files: files, mod: mod}))
  end

  @doc "Determine if files have changed since last run"
  @spec get_changed_files(files, files) :: files
  def get_changed_files(new_files, old_files) do
    Enum.reduce(new_files, %{}, fn({name, v}, acc) ->
      case Map.fetch(old_files, name) do
        :error -> # New file
          Map.put(acc, name, v)
        {:ok, %{mod: prev_mod}} -> # Existing
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
      config.handler.load_update(name, update, tid)
    end
  end

  @doc "Create table if new/update"
  @spec maybe_create_table(name, :calendar.datetime, map) :: :ets.tid
  def maybe_create_table(name, mod, config) do
    case :ets.lookup(__MODULE__, name) do
      [] ->
        Lager.debug("Creating ETS table #{name} new")
        create_ets_table(config)
      [{^name, %{id: tid, mod: m}}] when m == mod ->
        Lager.debug("Using existing ETS table #{name}")
        tid
      [{^name, %{}}] ->
        Lager.debug("Creating ETS table #{name} update")
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
    old_tables = for table <- new_tables,
      [{_name, %{id: tid}}] <- :ets.lookup(__MODULE__, table.name), do: tid

    # Update index with ids of current tables
    table_tuples = for %{name: name} = table <- new_tables, do: {name, table}
    :ets.insert(__MODULE__, table_tuples)

    # The replaced tables need to be deleted. To avoid having a window where
    # data is not available, we don't delete them immediately, we do it on
    # the next cycle.
    old_tables
  end

  @spec delete_tables(list(:ets.tid)) :: :ok
  def delete_tables(tables) do
    Enum.each(tables, &(:ets.delete(&1)))
  end

  @doc "Notify subscribers about updates"
  @spec notify_update([map]) :: :ok
  def notify_update(new_tables) do
    Enum.each(new_tables, fn(%{name: name}) ->
      FileConfig.EventProducer.sync_notify({:load, name})
    end)
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

  @spec check_delay() :: non_neg_integer()
  defp check_delay(), do: Application.get_env(@app, :check_delay, 5000)

  @spec data_dirs() :: [Path.t]
  defp data_dirs(), do: Application.get_env(@app, :data_dirs, [])

  def list_index do
    :ets.foldl(fn({key, value}, acc) -> [{key, value} | acc] end, [], __MODULE__)
  end

  def free_binary_memory do
    {:binary_memory, binary_memory} = :recon.info(self(), :binary_memory)
    if binary_memory > 500_000_000 do
      # Manually trigger garbage collection to clear refc binary memory
      Lager.debug("Forcing garbage collection")
      :erlang.garbage_collect(self())
    end
  end

end
