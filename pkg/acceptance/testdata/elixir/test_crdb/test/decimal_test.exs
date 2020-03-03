defmodule Cockroach.TestDecimal do
  use ExUnit.Case

  test "handles decimal correctly" do
    ssl_opts = [certfile: System.get_env("PGSSLCERT"), keyfile: System.get_env("PGSSLKEY")]
    {port, _} = Integer.parse(System.get_env("PGPORT") || "26257")
    {start_ok, pid} = Postgrex.start_link(
      hostname: System.get_env("PGHOST") || "localhost",
      username: System.get_env("PGUSER") || "root",
      password: "",
      database: "testdb",
      port: port,
      ssl: (System.get_env("PGSSLCERT") != nil && true) || false,
      ssl_opts: ssl_opts
    )
    assert start_ok
    for dec <- [
      Decimal.new("0"),
      Decimal.new("0.0"),
      Decimal.new("0.00"),
      Decimal.new("0.000"),
      Decimal.new("0.0000"),
      Decimal.new("0.00000"),
      Decimal.new("0.00001"),
      Decimal.new("0.000012"),
      Decimal.new("0.0000012"),
      Decimal.new("0.00000012"),
      Decimal.new("0.00000012"),
      Decimal.new(".00000012"),
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
      Decimal.new("12340"),
      Decimal.new("123400"),
      Decimal.new("1234000"),
      Decimal.new("12340000"),
      Decimal.new("12340.00"),
      Decimal.new("123400.00"),
      Decimal.new("1234000.00"),
      Decimal.new("12340000.00"),
      Decimal.new("1.2345e-10"),
      Decimal.new("1.23450e-10"),
      Decimal.new("1.234500e-10"),
      Decimal.new("1.2345000e-10"),
      Decimal.new("1.23450000e-10"),
      Decimal.new("1.234500000e-10"),
    ] do
      {result_ok, result} = Postgrex.query(pid, "SELECT CAST($1 AS DECIMAL)", [dec])
      assert result_ok
      [[ret]] = result.rows
      assert ret == dec
    end
  end
end
