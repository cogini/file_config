defmodule FileConfig.EventConsumer do
  use GenStage

  require Lager

  @doc "Starts the consumer."
  def start_link(state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    # Starts a permanent subscription to the broadcaster
    # which will automatically start requesting items.
    {:consumer, state, subscribe_to: [FileConfig.EventProducer]}
  end

  def handle_events(events, _from, state) do
    for event <- events do
      Lager.debug("Received event #{inspect event}")
    end
    {:noreply, [], state}
  end
end

