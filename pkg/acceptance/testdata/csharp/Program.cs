using System;
using System.Data;
using System.Linq;
using Npgsql;
using System.Security.Cryptography.X509Certificates;
using System.Security.Cryptography;

namespace CockroachDrivers
{
    class MainClass
    {
        static void Main(string[] args)
        {
            var connStringBuilder = new NpgsqlConnectionStringBuilder();
            connStringBuilder.Port = int.Parse(Environment.GetEnvironmentVariable("PGPORT"));
            connStringBuilder.Host = Environment.GetEnvironmentVariable("PGHOST");
            connStringBuilder.Username = Environment.GetEnvironmentVariable("PGUSER");
            connStringBuilder.SslMode = SslMode.Require;
            connStringBuilder.TrustServerCertificate = true;
            // Npgsql needs to connect to a database that already exists.
            connStringBuilder.Database = "system";
            Simple(connStringBuilder.ConnectionString);
            ArrayTest(connStringBuilder.ConnectionString);
            TxnSample(connStringBuilder.ConnectionString);
            UnitTest(connStringBuilder.ConnectionString);
        }

        static void ProvideClientCertificatesCallback(X509CertificateCollection clientCerts)
        {
              clientCerts.Add(new X509Certificate2("/certs/client.root.pk12"));
        }

        static void Simple(string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.ProvideClientCertificatesCallback += new ProvideClientCertificatesCallback(ProvideClientCertificatesCallback);
                conn.Open();

                // Create the database if it doesn't exist.
                new NpgsqlCommand("CREATE DATABASE IF NOT EXISTS bank", conn).ExecuteNonQuery();

                // Create the "accounts" table.
                new NpgsqlCommand("CREATE TABLE IF NOT EXISTS bank.accounts (id INT PRIMARY KEY, balance INT)", conn).ExecuteNonQuery();

                // Insert two rows into the "accounts" table.
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = "UPSERT INTO bank.accounts(id, balance) VALUES(@id1, @val1), (@id2, @val2)";
                    cmd.Parameters.AddWithValue("id1", 1);
                    cmd.Parameters.AddWithValue("val1", 1000);
                    cmd.Parameters.AddWithValue("id2", 2);
                    cmd.Parameters.AddWithValue("val2", 250);
                    cmd.ExecuteNonQuery();
                }

                // Print out the balances.
                System.Console.WriteLine("Initial balances:");
                using (var cmd = new NpgsqlCommand("SELECT id, balance FROM bank.accounts", conn))
                using (var reader = cmd.ExecuteReader())
                    while (reader.Read())
                        Console.Write("\taccount {0}: {1}\n", reader.GetString(0), reader.GetString(1));
            }
        }

        static void TransferFunds(NpgsqlConnection conn, NpgsqlTransaction tran, int from, int to, int amount)
        {
            int balance = 0;
            using (var cmd = new NpgsqlCommand(String.Format("SELECT balance FROM bank.accounts WHERE id = {0}", from), conn, tran))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    balance = reader.GetInt32(0);
                }
                else
                {
                    throw new DataException(String.Format("Account id={0} not found", from));
                }
            }
            if (balance < amount)
            {
                throw new DataException(String.Format("Insufficient balance in account id={0}", from));
            }
            using (var cmd = new NpgsqlCommand(String.Format("UPDATE bank.accounts SET balance = balance - {0} where id = {1}", amount, from), conn, tran))
            {
                cmd.ExecuteNonQuery();
            }
            using (var cmd = new NpgsqlCommand(String.Format("UPDATE bank.accounts SET balance = balance + {0} where id = {1}", amount, to), conn, tran))
            {
                cmd.ExecuteNonQuery();
            }
        }

        static void TxnSample(string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.ProvideClientCertificatesCallback += new ProvideClientCertificatesCallback(ProvideClientCertificatesCallback);
                conn.Open();
                try
                {
                    using (var tran = conn.BeginTransaction())
                    {
                        tran.Save("cockroach_restart");
                        while (true)
                        {
                            try
                            {
                                TransferFunds(conn, tran, 1, 2, 100);
                                tran.Commit();
                                break;
                            }
                            catch (NpgsqlException e)
                            {
                                // Check if the error code indicates a SERIALIZATION_FAILURE.
                                if (e.ErrorCode == 40001)
                                {
                                    // Signal the database that we will attempt a retry.
                                    tran.Rollback("cockroach_restart");
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }
                    }
                }
                catch (DataException e)
                {
                    Console.WriteLine(e.Message);
                }

                // Now printout the results.
                Console.WriteLine("Final balances:");
                using (var cmd = new NpgsqlCommand("SELECT id, balance FROM bank.accounts", conn))
                using (var reader = cmd.ExecuteReader())
                    while (reader.Read())
                        Console.Write("\taccount {0}: {1}\n", reader.GetString(0), reader.GetString(1));
            }
        }

        static void UnitTest(string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.ProvideClientCertificatesCallback += new ProvideClientCertificatesCallback(ProvideClientCertificatesCallback);
                conn.Open();

                using (var cmd = new NpgsqlCommand("DROP DATABASE IF EXISTS test CASCADE", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("DROP DATABASE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("CREATE DATABASE test", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE DATABASE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("CREATE TABLE test.f(x INT, ts TIMESTAMP)", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("INSERT INTO test.f VALUES (42, timestamp '2015-05-07 18:20:00')", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != 1)
                    {
                        throw new DataException(String.Format("INSERT reports {0} rows changed, expecting 1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("SELECT * FROM test.f", conn))
                {
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        var a = reader.GetInt32(0);
                        if (a != 42)
                        {
                            throw new DataException(String.Format("SELECT can't find inserted value: read {0}, expecting 42", a));
                        }
                        var ts = reader.GetTimeStamp(1);
                        var expectedTs = new NpgsqlTypes.NpgsqlDateTime(2015, 5, 7, 18, 20, 0, DateTimeKind.Unspecified);
                        if (ts != expectedTs)
                        {
                            throw new DataException(String.Format("SELECT unexpected value for ts: read {0}, expecting {1}", ts, expectedTs));
                        }
                    }
                }

                using (var cmd = new NpgsqlCommand("INSERT INTO test.f VALUES (@x, @ts)", conn))
                {
                    cmd.Parameters.Add("x", NpgsqlTypes.NpgsqlDbType.Integer).Value = 1;
                    cmd.Parameters.Add("ts", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = new NpgsqlTypes.NpgsqlDateTime(DateTime.Now);
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != 1)
                    {
                        throw new DataException(String.Format("INSERT reports {0} rows changed, expecting 1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("DROP TABLE test.f", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("DROP TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("SELECT 1, 2 > @comp, @int::int, @bool::string, @long::string, @real::string, @double::string, @short::string", conn))
                {
                    cmd.Parameters.Add("comp", NpgsqlTypes.NpgsqlDbType.Integer).Value = 3;
                    cmd.Parameters.Add("int", NpgsqlTypes.NpgsqlDbType.Integer).Value = 3;
                    cmd.Parameters.Add("bool", NpgsqlTypes.NpgsqlDbType.Boolean).Value = true;
                    cmd.Parameters.Add("long", NpgsqlTypes.NpgsqlDbType.Bigint).Value = -4L;
                    cmd.Parameters.Add("real", NpgsqlTypes.NpgsqlDbType.Real).Value = 5.31f;
                    cmd.Parameters.Add("double", NpgsqlTypes.NpgsqlDbType.Double).Value = -6.21d;
                    cmd.Parameters.Add("short", NpgsqlTypes.NpgsqlDbType.Smallint).Value = (short)7;
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        var a = reader.GetInt32(0);
                        if (a != 1)
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected 1", a));
                        }
                        var b = reader.GetBoolean(1);
                        if (b)
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected false", b));
                        }
                        var c = reader.GetInt32(2);
                        if (c != 3)
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected 3", c));
                        }
                        var d = reader.GetString(3);
                        if (!d.Equals("true"))
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected true", d));
                        }
                        var e = reader.GetString(4);
                        if (!e.Equals("-4"))
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected -4", e));
                        }
                        var f = reader.GetString(5);
                        if (!f.StartsWith("5.3", StringComparison.Ordinal))
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected that it starts with 5.3", f));
                        }
                        var g = reader.GetString(6);
                        if (!g.StartsWith("-6.2", StringComparison.Ordinal))
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected that it starts with -6.2", g));
                        }
                        var h = reader.GetString(7);
                        if (!h.Equals("7"))
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected 7", h));
                        }
                    }
                }

                using (var cmd = new NpgsqlCommand("USE test", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("USE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT, cdate DATE)", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("INSERT INTO accounts (id, balance, cdate) VALUES ( @id, @balance, @cdate )", conn))
                {
                    cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Integer).Value = 1;
                    cmd.Parameters.Add("balance", NpgsqlTypes.NpgsqlDbType.Integer).Value = 1000;
                    cmd.Parameters.Add("cdate", NpgsqlTypes.NpgsqlDbType.Date).Value = new NpgsqlTypes.NpgsqlDate(DateTime.Now);
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != 1)
                    {
                        throw new DataException(String.Format("INSERT reports {0} rows changed, expecting 1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("CREATE TABLE empty()", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("SELECT * from empty", conn))
                {
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.FieldCount != 0)
                        {
                            throw new DataException(String.Format("SELECT returns {0} columns, expected 0", reader.FieldCount));
                        }
                    }
                }

                using (var cmd = new NpgsqlCommand("CREATE TABLE str (s STRING)", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("UPDATE str SET s = @s", conn))
                {
                    cmd.Parameters.Add("s", NpgsqlTypes.NpgsqlDbType.Varchar).Value = "hello";
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != 0)
                    {
                        throw new DataException(String.Format("UPDATE reports {0} rows changed, expecting 0", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("SELECT @x", conn))
                {
                    var uuid = new Guid();
                    cmd.Parameters.Add("x", NpgsqlTypes.NpgsqlDbType.Uuid).Value = uuid;
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        var actualUUID = reader.GetGuid(0);
                        if (actualUUID != uuid)
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected {1}", actualUUID, uuid));
                        }
                    }
                }

                // Check that imprecise placeholder typing works correctly. See issues
                // #14245 and #14311 for more detail.
                using (var cmd = new NpgsqlCommand("CREATE TABLE t (price decimal(5,2) NOT NULL)", conn))
                {
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != -1)
                    {
                        throw new DataException(String.Format("CREATE TABLE reports {0} rows changed, expecting -1", rows));
                    }
                }

                using (var cmd = new NpgsqlCommand("INSERT INTO test.t VALUES(@f)", conn))
                {
                    cmd.Parameters.Add("f", NpgsqlTypes.NpgsqlDbType.Real).Value = 3.3f;
                    cmd.Prepare();
                    var rows = cmd.ExecuteNonQuery();
                    if (rows != 1)
                    {
                        throw new DataException(String.Format("INSERT reports {0} rows changed, expecting 1", rows));
                    }
                }
            }
        }

        static void ArrayTest(string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.ProvideClientCertificatesCallback += new ProvideClientCertificatesCallback(ProvideClientCertificatesCallback);
                conn.Open();

                new NpgsqlCommand("CREATE DATABASE IF NOT EXISTS test", conn).ExecuteNonQuery();
                new NpgsqlCommand("CREATE TABLE IF NOT EXISTS test.arrays (a INT[])", conn).ExecuteNonQuery();
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = "INSERT INTO test.arrays(a) VALUES(@val)";
                    cmd.Parameters.AddWithValue("val", new int[] {1, 2, 3});
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new NpgsqlCommand("SELECT a FROM test.arrays", conn))
                using (var reader = cmd.ExecuteReader())
                    while (reader.Read()) {
                      var ary = reader["a"] as long[];
                      if (!ary.SequenceEqual(new long[] {1, 2, 3})) {
                        throw new DataException(String.Format("Expected result to be [1, 2, 3], was {0}", String.Join(", ", ary)));
                      }
                    }
            }
        }
    }
}
