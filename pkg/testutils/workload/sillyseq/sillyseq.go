// Copyright 2017 The Cockroach Authors.
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

package sillyseq

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
)

const (
	schema   = `(k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)`
	rowCount = 100
)

type sillyseq struct {
	flags *pflag.FlagSet
}

func init() {
	workload.Register(generator)
}

func generator() workload.Generator {
	g := &sillyseq{}
	g.flags = pflag.NewFlagSet(g.Name(), pflag.ContinueOnError)
	return g
}

// Name implements the Generator interface.
func (*sillyseq) Name() string { return `sillyseq` }

// Flags implements the Generator interface.
func (g *sillyseq) Flags() *pflag.FlagSet {
	return g.flags
}

// Configure implements the Generator interface.
func (g *sillyseq) Configure(flags []string) error {
	return nil
}

// Tables implements the Generator interface.
func (g *sillyseq) Tables() []workload.Table {
	table := workload.Table{
		Name:            g.Name(),
		Schema:          schema,
		InitialRowCount: rowCount,
		InitialRowFn: func(i int) []string {
			s := strconv.Itoa(i)
			return []string{s, "0"}
		},
	}
	return []workload.Table{table}
}

// Ops implements the Generator interface.
func (g *sillyseq) Ops() []workload.Operation {
	table := g.Name()
	// No RWU:
	// qRead1 = `SELECT 1`
	// Lots of RWU with distsql=auto, very few with distsql=on
	qRead1 := fmt.Sprintf(`SELECT k FROM test.%s LIMIT 1`, table)
	// Need to test:
	// qRead1 := "SELECT cluster_logical_timestamp()"
	// Nearly no RWUs, surprisingly:
	// qRead1 := fmt.Sprintf(`SELECT COUNT(*) FROM test.%s`, table) // 6-10 over 1.6k ops. less with distsql off (0-3)
	qRead2 := fmt.Sprintf(`SELECT COUNT(*) FROM test.%s`, table)
	qWrite := fmt.Sprintf(`INSERT INTO test.%s VALUES ($1, $2)`, table)
	//qWrite := fmt.Sprintf(`INSERT INTO test.%s VALUES ($1, $2) ON CONFLICT(k) DO UPDATE SET v = %s.v + 1`, table, table)
	opFn := func(db *gosql.DB) (func(context.Context) error, error) {
		f := func(ctx context.Context) error {
			txn, err := db.Begin()

			if err != nil {
				return err
			}
			defer func() {
				// Always close the txn, or many early returns leak a connection and
				// things will soon lock up.
				_ = txn.Rollback()
			}()

			// Touch-all read phase that should run into lots of read uncertainty.
			// Do it twice so that the second time around, the gateway can't do the
			// retry for us.
			if rand.Intn(2) == 0 {
				var n int
				if err := txn.QueryRow(qRead1).Scan(&n); err != nil {
					return errors.Wrap(err, "on read 1")
				}
				time.Sleep(100 * time.Millisecond)
				if err := txn.QueryRow(qRead2).Scan(&n); err != nil {
					return errors.Wrap(err, "on read 2")
				}
				return errors.Wrap(txn.Commit(), "read-commit")
			}

			fmt.Print(",")
			// Write phase.
			//i := rand.Intn(rowCount)
			if _, err := txn.Exec(qWrite, rand.Int63(), 1); err != nil {
				// if _, err := txn.Exec(qWrite, i, 1); err != nil {
				return errors.Wrap(err, "on write")
			}
			return errors.Wrap(txn.Commit(), "write-commit")
		}
		return f, nil
	}

	return []workload.Operation{{
		Name: g.Name(),
		Fn:   opFn,
	}}
}
