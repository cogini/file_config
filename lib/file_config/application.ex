defmodule FileConfig.Application do
  @moduledoc false
  @app :file_config

  use Application

  def start(_type, _args) do

    {:ok, _} = Application.ensure_all_started(:file_config_rocksdb, :permanent)

    children = [
      {FileConfig.EventProducer, []},
      # {FileConfig.EventConsumer, []},
      {FileConfig.Loader, [
        files: Application.get_env(@app, :files, []),
        data_dirs: Application.get_env(@app, :data_dirs, []),
        check_delay: Application.get_env(@app, :check_delay, 5000)
      ]},
    ]

    opts = [strategy: :one_for_one, name: FileConfig.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
