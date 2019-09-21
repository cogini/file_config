defmodule FileConfig.Loader.Tab do
  @moduledoc "Table"

  require Record
  Record.defrecord :tab, name: nil, id: nil, timestamp: {{0, 0, 0}, {0, 0, 0}}, type: :ets, db_path: ""

  @type tab :: record(:tab,
                      name: atom,
                      id: :ets.tid,
                      timestamp: :calendar.datetime,
                      type: :ets | :db,
                      db_path: :file.filename_all)

  # defstruct [
  #   name: nil,
  #   id: nil
  #   timestamp: {{0, 0, 0}, {0, 0, 0}},
  #   type: :ets,
  #   db_path: ""
  # ]

  # @type t :: %__MODULE__{
  #   name: atom,             # name of table
  #   id: :ets.tid,
  #   timestamp: :calendar.datetime,
  #   type: :ets | :db,
  #   db_path: :file.filename_all
  # }
end
