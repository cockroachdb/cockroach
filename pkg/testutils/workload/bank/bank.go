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
// permissions and limitations under the License.

package bank

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	bankSchema = `(
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`

	defaultRows         = 1000
	defaultPayloadBytes = 100
	defaultRanges       = 10
	maxTransfer         = 999
)

type bank struct {
	flags *pflag.FlagSet

	seed                       int64
	rows, payloadBytes, ranges int
}

func init() {
	workload.Register(bankMeta)
}

var bankMeta = workload.Meta{
	Name:        `bank`,
	Description: `Bank models a set of accounts with currency balances`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &bank{flags: pflag.NewFlagSet(`kv`, pflag.ContinueOnError)}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.rows, `rows`, defaultRows, `Initial number of accounts in bank table.`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each initial row.`)
		g.flags.IntVar(&g.ranges, `ranges`, defaultRanges, `Initial number of ranges in bank table.`)
		return g
	},
}

// FromRows returns Bank testdata with the given number of rows and default
// payload size and range count.
func FromRows(rows int) workload.Generator {
	return FromConfig(rows, defaultPayloadBytes, defaultRanges)
}

// FromConfig returns a one table testdata with three columns: an `id INT
// PRIMARY KEY` representing an account number, a `balance` INT, and a `payload`
// BYTES to pad the size of the rows for various tests.
func FromConfig(rows int, payloadBytes int, ranges int) workload.Generator {
	if ranges > rows {
		ranges = rows
	}
	return &bank{
		seed:         timeutil.Now().UnixNano(),
		rows:         rows,
		payloadBytes: payloadBytes,
		ranges:       ranges,
	}
}

// Meta implements the Generator interface.
func (*bank) Meta() workload.Meta { return bankMeta }

// Flags implements the Generator interface.
func (b *bank) Flags() *pflag.FlagSet {
	return b.flags
}

// Configure implements the Generator interface.
func (b *bank) Configure(flags []string) error {
	if b.flags.Parsed() {
		return errors.New("Configure was already called")
	}
	return b.flags.Parse(flags)
}

// Tables implements the Generator interface.
func (b *bank) Tables() []workload.Table {
	rng := rand.New(rand.NewSource(b.seed))
	table := workload.Table{
		Name:            `bank`,
		Schema:          bankSchema,
		InitialRowCount: b.rows,
		InitialRowFn: func(rowIdx int) []string {
			const initialPrefix = `initial-`
			bytes := hex.EncodeToString(randutil.RandBytes(rng, b.payloadBytes/2))
			// Minus 2 for the single quotes
			bytes = bytes[:b.payloadBytes-len(initialPrefix)-2]
			return []string{
				strconv.Itoa(rowIdx), // id
				`0`,                  // balance
				`'` + initialPrefix + bytes + `'`, // payload
			}
		},
	}
	return []workload.Table{table}
}

// Tables implements the Generator interface.
func (b *bank) Ops() []workload.Operation {
	// TODO(dan): Move the various queries in the backup/restore tests here.
	op := workload.Operation{
		Name: `balance transfers`,
		Fn: func(sqlDB *gosql.DB) (func(context.Context) error, error) {
			rng := rand.New(rand.NewSource(b.seed))
			updateStmt, err := sqlDB.Prepare(`
				UPDATE bank
				SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
				WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bank WHERE id = $1)
			`)
			if err != nil {
				return nil, err
			}

			return func(ctx context.Context) error {
				from := rng.Intn(b.rows)
				to := rng.Intn(b.rows - 1)
				for from == to && b.rows != 1 {
					to = rng.Intn(b.rows - 1)
				}
				amount := rand.Intn(maxTransfer)
				_, err := updateStmt.ExecContext(ctx, from, to, amount)
				return err
			}, nil
		},
	}
	return []workload.Operation{op}
}

// Split creates the configured number of ranges in an already created version
// of the table represented by `g`.
func Split(db *gosql.DB, g workload.Generator) error {
	// TODO(dan): Make this general and move it into the workload package.
	b, ok := g.(*bank)
	if !ok {
		return errors.Errorf("don't know how to split: %T", g)
	}
	bankTable := b.Tables()[0]

	var stmt bytes.Buffer
	fmt.Fprintf(&stmt, `ALTER TABLE %s SPLIT AT VALUES `, bankTable.Name)
	splitIdx, splits := 0, b.ranges-1
	for ; splitIdx < splits; splitIdx++ {
		if splitIdx != 0 {
			stmt.WriteRune(',')
		}
		fmt.Fprintf(&stmt, `(%d)`, (splitIdx+1)*(b.rows/b.ranges))
	}
	if splitIdx > 0 {
		if _, err := db.Exec(stmt.String()); err != nil {
			return err
		}
	}
	return nil
}
