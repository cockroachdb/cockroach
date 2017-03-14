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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

// maxTransfer is the maximum amount to transfer in one transaction.
const maxTransfer = 999

// runBenchmarkBank mirrors the SQL performed by examples/sql_bank, but
// structured as a benchmark for easier usage of the Go performance analysis
// tools like pprof, memprof and trace.
func runBenchmarkBank(b *testing.B, db *gosql.DB, numAccounts int) {
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
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			from := rand.Intn(numAccounts)
			to := rand.Intn(numAccounts - 1)
			for from == to {
				to = numAccounts - 1
			}

			amount := rand.Intn(maxTransfer)

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

func BenchmarkBank(b *testing.B) {
	for _, numAccounts := range []int{2, 4, 8, 32, 64} {
		b.Run(strconv.Itoa(numAccounts), func(b *testing.B) {
			forEachDB(b, func(b *testing.B, db *gosql.DB) {
				runBenchmarkBank(b, db, numAccounts)
			})
		})
	}
}
