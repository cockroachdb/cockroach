// Copyright 2016 The Cockroach Authors.
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
// Author: David Taylor (david@cockroachlabs.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgbench"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func runPgbenchQuery(b *testing.B, db *sql.DB) {
	if err := pgbench.SetupBenchDB(db, 20000, true /*quiet*/); err != nil {
		b.Fatal(err)
	}
	src := rand.New(rand.NewSource(5432))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pgbench.RunOne(db, src, 20000); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func runPgbenchQueryParallel(b *testing.B, db *sql.DB) {
	if err := pgbench.SetupBenchDB(db, 20000, true /*quiet*/); err != nil {
		b.Fatal(err)
	}

	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		src := rand.New(rand.NewSource(5432))
		r := retry.Start(retryOpts)
		var err error
		for pb.Next() {
			r.Reset()
			for r.Next() {
				err = pgbench.RunOne(db, src, 20000)
				if err == nil {
					break
				}
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
}

func execPgbench(b *testing.B, pgUrl url.URL) {
	c, err := pgbench.SetupExec(pgUrl, "bench", 20000, b.N)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	out, err := c.CombinedOutput()
	if testing.Verbose() || err != nil {
		fmt.Println(string(out))
	}
	if err != nil {
		b.Log(c)
		b.Fatal(err)
	}
	b.StopTimer()
}

func BenchmarkPgbenchExec_Cockroach(b *testing.B) {
	defer tracing.Disable()()
	s := server.StartInsecureTestServer(b)
	defer s.Stop()

	pgUrl, cleanupFn := sqlutils.PGUrl(b, s, security.RootUser, "benchmarkCockroach")
	pgUrl.RawQuery = "sslmode=disable"
	defer cleanupFn()

	execPgbench(b, pgUrl)
}

func BenchmarkPgbenchExec_Postgres(b *testing.B) {
	pgUrl, err := url.Parse("postgres://localhost:5432?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	execPgbench(b, *pgUrl)
}

func BenchmarkPgbenchQuery_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runPgbenchQuery)
}

func BenchmarkPgbenchQuery_Postgres(b *testing.B) {
	benchmarkPostgres(b, runPgbenchQuery)
}

func BenchmarkParallelPgbenchQuery_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runPgbenchQueryParallel)
}

func BenchmarkParallelPgbenchQuery_Postgres(b *testing.B) {
	benchmarkPostgres(b, runPgbenchQueryParallel)
}
