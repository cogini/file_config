defmodule FileConfig.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {FileConfig.EventProducer, []},
      # {FileConfig.EventConsumer, []},
      {FileConfig.Loader, []},
    ]

    opts = [strategy: :one_for_one, name: FileConfig.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
