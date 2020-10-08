// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/bench"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// maxTransfer is the maximum amount to transfer in one transaction.
const maxTransfer = 999

// runBenchmarkBank mirrors the SQL performed by examples/sql_bank, but
// structured as a benchmark for easier usage of the Go performance analysis
// tools like pprof, memprof and trace.
func runBenchmarkBank(b *testing.B, db *sqlutils.SQLRunner, numAccounts int) {
	{
		// Initialize the "bank" table.
		schema := `
CREATE TABLE IF NOT EXISTS bench.bank (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)`
		db.Exec(b, schema)
		db.Exec(b, "TRUNCATE TABLE bench.bank")

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
		db.Exec(b, stmt, values...)
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
			db.Exec(b, update, from, to, amount)
		}
	})
	b.StopTimer()
}

func BenchmarkBank(b *testing.B) {
	bench.ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		for _, numAccounts := range []int{2, 4, 8, 32, 64} {
			b.Run(fmt.Sprintf("numAccounts=%d", numAccounts), func(b *testing.B) {
				runBenchmarkBank(b, db, numAccounts)
			})
		}
	})
}
