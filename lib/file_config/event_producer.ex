defmodule FileConfig.EventProducer do
  @moduledoc "Notifiy interested parties when files are loaded"

  use GenStage

  @doc "Start broadcaster"
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc "Send event and return only after it is dispatched"
  def sync_notify(event, timeout \\ 5000) do
    GenStage.call(__MODULE__, {:notify, event}, timeout)
  end

  # Callbacks

  @impl true
  def init(args) do
    {:producer, args, dispatcher: GenStage.BroadcastDispatcher}
  end

  @impl true
  def handle_call({:notify, event}, _from, state) do
    {:reply, :ok, [event], state} # Dispatch immediately
  end

  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state} # We don't care about the demand
  end
end
