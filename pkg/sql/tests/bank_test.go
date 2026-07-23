// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/bench"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

			// Query each account to determine whether it is eligible to take
			// part in the transfer. We include these checks as filters in the
			// UPDATE statement to avoid a multi-statement transaction. If
			// either check fails, the UPDATE will be a no-op.
			//
			// The SELECT ... FOR UPDATE ensures that the accounts are locked
			// before filtering, to avoid thrashing under heavy contention.
			lock1 := `AND (SELECT balance >= $3 FROM bench.bank WHERE id = $1 FOR UPDATE)`
			lock2 := `AND (SELECT true          FROM bench.bank WHERE id = $2 FOR UPDATE)`
			if to < from {
				// Enforce a consistent lock ordering across accounts,
				// regardless of transfer direction. This prevents deadlocks,
				// which will be broken by the database but harm performance.
				lock1, lock2 = lock2, lock1
			}

			update := fmt.Sprintf(`
UPDATE bench.bank
  SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
  WHERE id IN ($1, $2) %s %s`, lock1, lock2)

			db.Exec(b, update, from, to, amount)
		}
	})
	b.StopTimer()
}

func BenchmarkBank(b *testing.B) {
	defer log.Scope(b).Close(b)
	bench.ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		for _, numAccounts := range []int{2, 4, 8, 32, 64} {
			b.Run(fmt.Sprintf("numAccounts=%d", numAccounts), func(b *testing.B) {
				runBenchmarkBank(b, db, numAccounts)
			})
		}
	})
}
