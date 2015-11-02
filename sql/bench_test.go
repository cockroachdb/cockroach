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
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util/stop"
)

func driverInit() func() {
	driver.Stopper = stop.NewStopper()
	return func() {
		driver.Stopper.Stop()
		driver.Stopper = nil
	}
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
	defer driverInit()()

	db, err := sql.Open("cockroach",
		scheme+"://node@"+s.ServingAddr()+"?certs="+s.Ctx.Certs)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
