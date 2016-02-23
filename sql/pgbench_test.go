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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/sql/pgbench"
)

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func runPgbenchQuery(b *testing.B, db *sql.DB) {
	if err := pgbench.SetupBenchDB(db, *numAccounts); err != nil {
		b.Fatal(err)
	}
	src := rand.New(rand.NewSource(5432))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pgbench.RunOne(db, src, *numAccounts); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Tests a batch of queries very similar to those that that PGBench runs
// in its TPC-B(ish) mode.
func runPgbenchQueryParallel(b *testing.B, db *sql.DB) {
	if err := pgbench.SetupBenchDB(db, *numAccounts); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		src := rand.New(rand.NewSource(5432))
		for pb.Next() {
			if err := pgbench.RunOne(db, src, *numAccounts); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
}

func BenchmarkPgbenchQuery_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkPgbenchQuery_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkParallelPgbenchQuery_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkParallelPgbenchQuery_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkBank)
}
