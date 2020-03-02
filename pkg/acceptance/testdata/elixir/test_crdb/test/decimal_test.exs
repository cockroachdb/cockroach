defmodule Cockroach.TestDecimal do
  use ExUnit.Case

  test "handles decimal correctly" do
    {start_ok, pid} = Postgrex.start_link(
      hostname: System.get_env("PGHOST") || "localhost",
      username: System.get_env("PGUSER") || "root",
      password: "",
      database: "test",
      port: System.get_env("PGHOST") || 26257
    )
    assert start_ok
    for dec <- [
      Decimal.new("0"),
      Decimal.new("0.0"),
      Decimal.new("0.0000"),
      Decimal.new("0.00000"),
      Decimal.new("0.00001"),
      Decimal.new("1"),
      Decimal.new("1.0"),
      Decimal.new("1.000"),
      Decimal.new("1.0000"),
      Decimal.new("1.00000"),
      Decimal.new("1.000001"),
      Decimal.new("12345"),
      Decimal.new("12345.0"),
      Decimal.new("12345.000"),
      Decimal.new("12345.0000"),
      Decimal.new("12345.00000"),
      Decimal.new("12345.000001"),
    ] do
      {result_ok, result} = Postgrex.query(pid, "SELECT CAST($1 AS DECIMAL)", [dec])
      assert result_ok
      [[ret]] = result.rows
      assert ret == dec
    end
  end
end
