defmodule FileConfig.Mixfile do
  use Mix.Project

  def project do
    [
      app: :file_config,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      dialyzer: [
        plt_add_deps: :project,
        plt_add_apps: [:ssl, :mnesia, :compiler, :xmerl, :inets, :disk_log],
        # plt_add_deps: true,
        # flags: ["-Werror_handling", "-Wrace_conditions"],
        # flags: ["-Wunmatched_returns", :error_handling, :race_conditions, :underspecs],
        # ignore_warnings: "dialyzer.ignore-warnings"
      ],
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :lager],
      mod: {FileConfig.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 0.5.1", only: [:dev, :test], runtime: false},
      {:exlager, github: "khia/exlager"},
      # {:esqlite, github: "mmzeeman/esqlite"},
      {:gen_stage, "~> 0.12"},
      {:jason, "~> 1.0"},
      # {:jsx, github: "talentdeficit/jsx", override: true},
      {:lager, github: "basho/lager", override: true},
      # {:recon, github: "ferd/recon", override: true},
      {:nimble_csv, "~> 0.3"},
      {:recon, "~> 2.3"},
      {:rocksdb, git: "https://gitlab.com/barrel-db/erlang-rocksdb.git"},
      {:sqlitex, "~> 1.3"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
    ]
  end
end
