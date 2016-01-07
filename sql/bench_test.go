// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
)

func benchmarkCockroach(b *testing.B, f func(b *testing.B, db *sql.DB)) {
	s := server.StartTestServer(b)
	defer s.Stop()

	pgUrl, cleanupFn := sqlutils.PGUrl(s, b, security.RootUser, os.TempDir(), "benchmarkCockroach")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgUrl.String())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkPostgres(b *testing.B, f func(b *testing.B, db *sql.DB)) {
	// Note: the following uses SSL. To run this, make sure your local
	// Postgres server has SSL enabled. To use Cockroach's checked-in
	// testing certificates for Postgres' SSL, first determine the
	// location of your Postgres server's configuration file:
	// ```
	// $ psql -h localhost -p 5432 -c 'SHOW config_file'
	//                config_file
	// -----------------------------------------
	//  /usr/local/var/postgres/postgresql.conf
	// (1 row)
	//```
	//
	// Now open this file and set the following values:
	// ```
	// $ cat /usr/local/var/postgres/postgresql.conf | grep ssl
	// ssl = on                            # (change requires restart)
	// ssl_cert_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/node.server.crt'             # (change requires restart)
	// ssl_key_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/node.server.key'              # (change requires restart)
	// ssl_ca_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/ca.crt'                        # (change requires restart)
	// ```
	// Where `/Users/tamird/src/go/src/github.com/cockroachdb/cockroach`
	// is replaced with your local Cockroach source directory.
	// Be sure to restart Postgres for this to take effect.

	db, err := sql.Open("postgres", "sslmode=require host=localhost port=5432")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMySQL(b *testing.B, f func(b *testing.B, db *sql.DB)) {
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func runBenchmarkSelect1(b *testing.B, db *sql.DB) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT 1`)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
	b.StopTimer()
}

// benchmarkSelect1 is a benchmark of the simplest SQL query: SELECT 1. This
// query requires no tables, expression analysis, etc. As such, it is measuring
// the overhead of parsing and other non-table processing (e.g. reading
// requests, writing responses).
func benchmarkSelect1(b *testing.B, scheme string) {
	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.Insecure = (scheme == "http" || scheme == "rpc")
	if err := s.Start(); err != nil {
		b.Fatal(err)
	}
	defer s.Stop()

	db, err := sql.Open("cockroach",
		scheme+"://node@"+s.ServingAddr()+"?certs="+s.Ctx.Certs)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	runBenchmarkSelect1(b, db)
}

func BenchmarkSelect1_HTTP(b *testing.B) {
	benchmarkSelect1(b, "http")
}

func BenchmarkSelect1_HTTPS(b *testing.B) {
	benchmarkSelect1(b, "https")
}

func BenchmarkSelect1_RPC(b *testing.B) {
	benchmarkSelect1(b, "rpc")
}

func BenchmarkSelect1_RPCS(b *testing.B) {
	benchmarkSelect1(b, "rpcs")
}

func BenchmarkSelect1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkSelect1)
}

func BenchmarkSelect1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkSelect1)
}

func BenchmarkSelect1_MySQL(b *testing.B) {
	benchmarkMySQL(b, runBenchmarkSelect1)
}

// runBenchmarkInsert benchmarks inserting count rows into a table.
func runBenchmarkInsert(b *testing.B, db *sql.DB, count int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.insert (k INT PRIMARY KEY)`); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var buf bytes.Buffer
	val := 0
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.insert VALUES `)
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d)", val)
			val++
		}
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if _, err := db.Exec(`DROP TABLE bench.insert`); err != nil {
		b.Fatal(err)
	}
}

func runBenchmarkInsert1(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 1)
}

func runBenchmarkInsert10(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 10)
}

func runBenchmarkInsert100(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 100)
}

func BenchmarkInsert1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert1)
}

func BenchmarkInsert1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert1)
}

func BenchmarkInsert10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert10)
}

func BenchmarkInsert10_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert10)
}

func BenchmarkInsert100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert100)
}

func BenchmarkInsert100_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert100)
}

// runBenchmarkScan benchmarks scanning a table containing count rows.
func runBenchmarkScan(b *testing.B, db *sql.DB, count int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.scan`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.scan (k INT PRIMARY KEY)`); err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.scan VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d)", i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT * FROM bench.scan`)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		if count != n {
			b.Fatalf("unexpected result count: %d != %d", count, n)
		}
	}
	b.StopTimer()

	if _, err := db.Exec(`DROP TABLE bench.scan`); err != nil {
		b.Fatal(err)
	}
}

func runBenchmarkScan1(b *testing.B, db *sql.DB) {
	runBenchmarkScan(b, db, 1)
}

func runBenchmarkScan10(b *testing.B, db *sql.DB) {
	runBenchmarkScan(b, db, 10)
}

func runBenchmarkScan100(b *testing.B, db *sql.DB) {
	runBenchmarkScan(b, db, 100)
}

func BenchmarkScan1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkScan1)
}

func BenchmarkScan1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkScan1)
}

func BenchmarkScan10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkScan10)
}

func BenchmarkScan10_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkScan10)
}

func BenchmarkScan100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkScan100)
}

func BenchmarkScan100_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkScan100)
}
