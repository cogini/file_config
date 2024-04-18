defmodule FileConfig.Application do
  @moduledoc false
  use Application

  @app :file_config

  @impl true
  def start(_type, _args) do
    # {:ok, _} = Application.ensure_all_started(:file_config_rocksdb, :permanent)
    # {:ok, _} = Application.ensure_all_started(:file_config_sqlite, :permanent)

    children = [
      {FileConfig.EventProducer, []},
      # {FileConfig.EventConsumer, []},
      {FileConfig.Loader,
       [
         state_dir: Application.get_env(@app, :state_dir),
         files: Application.get_env(@app, :files, []),
         data_dirs: Application.get_env(@app, :data_dirs, []),
         check_delay: Application.get_env(@app, :check_delay, 5000)
       ]}
    ]

    opts = [strategy: :one_for_one, name: FileConfig.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
