defmodule FileConfig.EventProducer do
  use GenStage

  @doc "Starts the broadcaster."
  def start_link(state) do
    GenStage.start_link(__MODULE__, state, name: __MODULE__)
  end

  @doc "Sends an event and returns only after the event is dispatched."
  def sync_notify(event, timeout \\ 5000) do
    GenStage.call(__MODULE__, {:notify, event}, timeout)
  end

  def init(state) do
    {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
  end

  def handle_call({:notify, event}, _from, state) do
    {:reply, :ok, [event], state} # Dispatch immediately
  end

  def handle_demand(_demand, state) do
    {:noreply, [], state} # We don't care about the demand
  end
end

