defmodule FileConfig.Supervisor do
  @moduledoc """
  Top level supervisor for module.

  Start this in your app supervision tree.
  """
  use Supervisor, restart: :permanent

  require Logger

  def start_link(args, opts \\ []) do
    Supervisor.start_link(__MODULE__, args, opts)
  end

  # state_dir: Application.get_env(@app, :state_dir),
  # files: Application.get_env(@app, :files, []),
  # data_dirs: Application.get_env(@app, :data_dirs, []),
  # check_delay: Application.get_env(@app, :check_delay, 5000)

  @impl true
  def init(args) do
    # Logger.debug("args: #{inspect(args)}")

    children = [
      {FileConfig.EventProducer, []},
      # {FileConfig.EventConsumer, []},
      {FileConfig.Loader,
       [
         state_dir: args[:state_dir],
         files: args[:files] || [],
         data_dirs: args[:data_dirs] || [],
         check_delay: args[:check_delay] || 60_000
       ]}
    ]

    options = [
      strategy: :one_for_one
      # max_restarts: 100,
      # max_seconds: 30,
    ]

    Supervisor.init(children, options)
  end
end
