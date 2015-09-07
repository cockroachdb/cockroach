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
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/log"
)

var maxTransfer = flag.Int("max-transfer", 999, "Maximum amount to transfer in one transaction.")
var numAccounts = flag.Int("num-accounts", 999, "Number of accounts.")

// BenchmarkBank mirrors the SQL performed by examples/sql_bank, but structured
// as a benchmark for easier usage of the Go performance analysis tools like
// pprof, memprof and trace.
func BenchmarkBank(b *testing.B) {
	s := server.StartTestServer(b)
	defer s.Stop()

	db, err := sql.Open("cockroach", "https://root@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bank`); err != nil {
		b.Fatal(err)
	}

	{
		// Initialize the "accounts" table.
		schema := `
CREATE TABLE IF NOT EXISTS bank.accounts (
  id INT PRIMARY KEY,
  balance INT NOT NULL
)`
		if _, err = db.Exec(schema); err != nil {
			b.Fatal(err)
		}
		if _, err = db.Exec("TRUNCATE TABLE bank.accounts"); err != nil {
			b.Fatal(err)
		}

		var placeholders bytes.Buffer
		var values []interface{}
		for i := 0; i < *numAccounts; i++ {
			if i > 0 {
				_, _ = placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "($%d, 0)", i+1)
			values = append(values, i)
		}
		stmt := `INSERT INTO bank.accounts (id, balance) VALUES ` + placeholders.String()
		if _, err = db.Exec(stmt, values...); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			from := rand.Intn(*numAccounts)
			var to int
			for {
				to = rand.Intn(*numAccounts)
				if from != to {
					break
				}
			}

			amount := rand.Intn(*maxTransfer)

			update := `
UPDATE bank.accounts
  SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
  WHERE id IN ($1, $2)
`
			if _, err = db.Exec(update, from, to, amount); err != nil {
				if log.V(1) {
					log.Warning(err)
				}
				continue
			}
		}
	})
	b.StopTimer()
}
