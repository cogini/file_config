defmodule FileConfigTest do
  use ExUnit.Case
  doctest FileConfig

  test "greets the world" do
    assert FileConfig.hello() == :world
  end
end
