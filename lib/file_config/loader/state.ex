defmodule FileConfig.Loader.State do
  @moduledoc false

  alias FileConfig.Loader.Tab

  defstruct [
    ref: nil,
    files: %{},
    old_tables: []
  ]

  @type t :: %__MODULE__{
    ref: reference,
    files: map,
    old_tables: list(Tab)
  }
end
