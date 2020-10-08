defmodule Cockroach.MixProject do
  use Mix.Project

  def project do
    [
      app: :debug,
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [{:postgrex, "~> 0.13", hex: :postgrex_cdb, override: true}]
  end
end
