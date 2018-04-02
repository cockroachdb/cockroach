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
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
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
	flags     workload.Flags
	connFlags *workload.ConnFlags

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
		g := &bank{}
		g.flags.FlagSet = pflag.NewFlagSet(`bank`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.rows, `rows`, defaultRows, `Initial number of accounts in bank table.`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each initial row.`)
		g.flags.IntVar(&g.ranges, `ranges`, defaultRanges, `Initial number of ranges in bank table.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
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
	b := bankMeta.New().(*bank)
	b.seed = timeutil.Now().UnixNano()
	b.rows = rows
	b.payloadBytes = payloadBytes
	b.ranges = ranges
	return b
}

// Meta implements the Generator interface.
func (*bank) Meta() workload.Meta { return bankMeta }

// Flags implements the Flagser interface.
func (b *bank) Flags() workload.Flags { return b.flags }

// Tables implements the Generator interface.
func (b *bank) Tables() []workload.Table {
	table := workload.Table{
		Name:   `bank`,
		Schema: bankSchema,
		InitialRows: workload.Tuples(
			b.rows,
			func(rowIdx int) []interface{} {
				rng := rand.New(rand.NewSource(b.seed + int64(rowIdx)))
				const initialPrefix = `initial-`
				bytes := hex.EncodeToString(randutil.RandBytes(rng, b.payloadBytes/2))
				// Minus 2 for the single quotes
				bytes = bytes[:b.payloadBytes-len(initialPrefix)-2]
				return []interface{}{
					rowIdx, // id
					0,      // balance
					initialPrefix + bytes, // payload
				}
			},
		),
		Splits: workload.Tuples(
			b.ranges-1,
			func(splitIdx int) []interface{} {
				return []interface{}{
					(splitIdx + 1) * (b.rows / b.ranges),
				}
			},
		),
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (b *bank) Ops(urls []string, reg *workload.HistogramRegistry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(b, b.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(b.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(b.connFlags.Concurrency + 1)

	// TODO(dan): Move the various queries in the backup/restore tests here.
	updateStmt, err := db.Prepare(`
		UPDATE bank
		SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
		WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bank WHERE id = $1)
	`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < b.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(b.seed))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			from := rng.Intn(b.rows)
			to := rng.Intn(b.rows - 1)
			for from == to && b.rows != 1 {
				to = rng.Intn(b.rows - 1)
			}
			amount := rand.Intn(maxTransfer)
			start := timeutil.Now()
			_, err := updateStmt.Exec(from, to, amount)
			hists.Get(`transfer`).Record(timeutil.Since(start))
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
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
