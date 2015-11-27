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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"net"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/server"
)

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

func BenchmarkSelect1_PGWire(b *testing.B) {
	s := server.StartTestServer(b)
	defer s.Stop()

	host, port, err := net.SplitHostPort(s.PGAddr())
	if err != nil {
		b.Fatal(err)
	}
	datasource := fmt.Sprintf("sslmode=disable host=%s port=%s", host, port)

	db, err := sql.Open("postgres", datasource)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	runBenchmarkSelect1(b, db)
}

func BenchmarkSelect1_Postgres(b *testing.B) {
	db, err := sql.Open("postgres", "sslmode=disable host=localhost port=5432")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	runBenchmarkSelect1(b, db)
}

func BenchmarkSelect1_MySQL(b *testing.B) {
	db, err := sql.Open("mysql", "tcp(localhost:3306)/test")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	runBenchmarkSelect1(b, db)
}
