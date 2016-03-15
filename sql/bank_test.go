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
	"flag"
	"fmt"
	"math/rand"
	"testing"
)

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts int

// runBenchmarkBank mirrors the SQL performed by examples/sql_bank, but
// structured as a benchmark for easier usage of the Go performance analysis
// tools like pprof, memprof and trace.
func runBenchmarkBank(b *testing.B, db *sql.DB) {
	{
		// Initialize the "bank" table.
		schema := `
CREATE TABLE IF NOT EXISTS bench.bank (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)`
		if _, err := db.Exec(schema); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec("TRUNCATE TABLE bench.bank"); err != nil {
			b.Fatal(err)
		}

		var placeholders bytes.Buffer
		var values []interface{}
		for i := 0; i < numAccounts; i++ {
			if i > 0 {
				placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "($%d, 10000)", i+1)
			values = append(values, i)
		}
		stmt := `INSERT INTO bench.bank (id, balance) VALUES ` + placeholders.String()
		if _, err := db.Exec(stmt, values...); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			from := rand.Intn(numAccounts)
			to := from
			for to == from {
				to = rand.Intn(numAccounts)
			}

			amount := rand.Intn(*maxTransfer)

			const update = `
UPDATE bench.bank
  SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
  WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bench.bank WHERE id = $1)`
			if _, err := db.Exec(update, from, to, amount); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
}

func BenchmarkBank2_Cockroach(b *testing.B) {
	numAccounts = 2
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank2_Postgres(b *testing.B) {
	numAccounts = 2
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkBank4_Cockroach(b *testing.B) {
	numAccounts = 4
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank4_Postgres(b *testing.B) {
	numAccounts = 4
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkBank8_Cockroach(b *testing.B) {
	numAccounts = 8
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank8_Postgres(b *testing.B) {
	numAccounts = 8
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkBank16_Cockroach(b *testing.B) {
	numAccounts = 16
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank16_Postgres(b *testing.B) {
	numAccounts = 16
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkBank32_Cockroach(b *testing.B) {
	numAccounts = 32
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank32_Postgres(b *testing.B) {
	numAccounts = 32
	benchmarkPostgres(b, runBenchmarkBank)
}

func BenchmarkBank64_Cockroach(b *testing.B) {
	numAccounts = 64
	benchmarkCockroach(b, runBenchmarkBank)
}

func BenchmarkBank64_Postgres(b *testing.B) {
	numAccounts = 64
	benchmarkPostgres(b, runBenchmarkBank)
}
