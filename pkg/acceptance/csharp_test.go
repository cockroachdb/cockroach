// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"golang.org/x/net/context"
)

func TestDockerCSharp(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "csharp", []string{"/bin/sh", "-c", strings.Replace(csharp, "%v", "test", 1)})
	testDockerFail(ctx, t, "csharp", []string{"/bin/sh", "-c", strings.Replace(csharp, "%v", "other", 1)})
}

const csharp = `
set -ex
mkdir csharp
cd csharp
dotnet new console
dotnet restore
dotnet add package npgsql -s /nuget/.

# In dotnet, to get a cert with a private key, we have to use a pfx file.
# See:
# http://www.sparxeng.com/blog/software/x-509-self-signed-certificate-for-cryptography-in-net
# https://stackoverflow.com/questions/808669/convert-a-cert-pem-certificate-to-a-pfx-certificate
openssl pkcs12 -inkey ${PGSSLKEY} -in ${PGSSLCERT} -export -out node.pfx -passout pass:

cat > Program.cs << 'EOF'
using System;
using System.Data;
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
            connStringBuilder.SslMode = SslMode.Require;
            connStringBuilder.TrustServerCertificate = true;
            Simple(connStringBuilder.ConnectionString);
            TxnSample(connStringBuilder.ConnectionString);

            connStringBuilder.Database = "test";
            UnitTest(connStringBuilder.ConnectionString);
        }

        static void ProvideClientCertificatesCallback(X509CertificateCollection clientCerts)
        {
              clientCerts.Add(new X509Certificate2("node.pfx"));
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

                using (var cmd = new NpgsqlCommand("SELECT * FROM %v.f", conn))
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

                using (var cmd = new NpgsqlCommand("SELECT nspname FROM pg_catalog.pg_namespace WHERE oid=@oid", conn))
                {
                    cmd.Parameters.Add("oid", NpgsqlTypes.NpgsqlDbType.Bigint).Value = 1782195457;
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        var nspName = reader.GetString(0);
                        if (nspName != "pg_catalog")
                        {
                            throw new DataException(String.Format("SELECT returns {0}, expected pg_catalog", nspName));
                        }
                    }
                }
            }
        }
    }
}
EOF

dotnet run
`
