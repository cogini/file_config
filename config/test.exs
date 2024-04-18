import Config

config :file_config,
  data_dirs: ["/var/foo/data"],
  state_dir: "/var/foo/state",
  check_delay: 10_000,
  files: [
    foo_file: %{
      # file: name of file or regex
      file: "foo.csv",
      # name: name of table, defaults to key
      # format: :csv, # csv | bert | dat, default from extension
      # handler: name of module to handle update, default from format
      data_parser: FileConfig.DataParser.Json
    },
    foo_config: %{
      file: "foo_config.bert"
    },
    foo_db: %{
      file: "foo_db.csv",
      csv_fields: {2, 3},
      data_parser: FileConfig.DataParser.Json,
      handler: FileConfig.Handler.CsvRocksdb,
      chunk_size: 1000
    }
  ]

config :logger,
  level: :warning,
  always_evaluate_messages: true

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:file, :line]

config :junit_formatter,
  report_dir: "#{Mix.Project.build_path()}/junit-reports",
  automatic_create_dir?: true,
  print_report_file: true,
  # prepend_project_name?: true,
  include_filename?: true,
  include_file_line?: true
