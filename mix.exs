defmodule FileConfig.MixProject do
  use Mix.Project

  @github "https://github.com/cogini/file_config"
  @version "0.1.0"

  def project do
    [
      app: :file_config,
      version: @version,
      elixir: "~> 1.8",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      dialyzer: [
        # plt_add_deps: :project,
        # plt_add_apps: [:ssl, :mnesia, :compiler, :xmerl, :inets, :disk_log],
        plt_add_deps: true
        # flags: ["-Werror_handling", "-Wrace_conditions"],
        # flags: ["-Wunmatched_returns", :error_handling, :race_conditions, :underspecs],
        # ignore_warnings: "dialyzer.ignore-warnings"
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.lcov": :test,
        quality: :test,
        "quality.ci": :test
      ],
      description: description(),
      package: package(),
      source_url: @github,
      homepage_url: @github,
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
      # mod: {FileConfig.Application, []}
    ]
  end

  # defp extra_applications(:test), do: []
  # defp extra_applications(_),     do: []

  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:castore, "~> 1.0", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.32.0", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.0", only: [:dev, :test], runtime: false},
      # {:file_config_rocksdb, github: "cogini/file_config_rocksdb"},
      # {:file_config_rocksdb, path: "../file_config_rocksdb"},
      # {:file_config_sqlite, github: "cogini/file_config_sqlite"},
      # {:file_config_sqlite, path: "../file_config_sqlite"},
      {:gen_stage, "~> 1.0"},
      {:jason, "~> 1.2"},
      {:junit_formatter, "~> 3.3", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false},
      {:murmur, "~> 1.0"},
      {:styler, "~> 0.11.0", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "Dynamically load config files into ETS tables."
  end

  defp package do
    [
      description: description(),
      maintainers: ["Jake Morrison"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @github,
        "Changelog" => "#{@github}/blob/#{@version}/CHANGELOG.md##{String.replace(@version, ".", "")}"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      # skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      source_url: @github,
      source_ref: @version,
      extras: [
        "README.md",
        "CHANGELOG.md": [title: "Changelog"],
        "LICENSE.md": [title: "License (Apache-2.0)"],
        "CONTRIBUTING.md": [title: "Contributing"],
        "CODE_OF_CONDUCT.md": [title: "Code of Conduct"]
      ],
      # api_reference: false,
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      quality: [
        "test",
        "format --check-formatted",
        # "credo",
        "credo --mute-exit-status",
        # mix deps.clean --unlock --unused
        "deps.unlock --check-unused",
        # mix deps.update
        # "hex.outdated",
        # "hex.audit",
        "deps.audit",
        "dialyzer --quiet-with-result"
      ],
      "quality.ci": [
        "format --check-formatted",
        "deps.unlock --check-unused",
        # "hex.outdated",
        "hex.audit",
        "deps.audit",
        "credo",
        "dialyzer --quiet-with-result"
      ]
    ]
  end
end
