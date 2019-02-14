// Copyright 2018 The Cockroach Authors.
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
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	indexStressSchema = `(
		a INT,
		b INT,
		c INT,
		payload STRING,
		PRIMARY KEY (a, b, c)
	)`

	indexStressSchemaWithIndex = `(
		a INT,
		b INT,
		c INT,
		payload STRING,
		PRIMARY KEY (a, b, c),
		INDEX (b, c, a) STORING (payload)
	)`

	defaultPayloadBytes = 100
)

type idxstress struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                                 int64
	aCount, bCount, cCount, payloadBytes int
}

func init() {
	workload.Register(idxstressMeta)
}

var idxstressMeta = workload.Meta{
	Name:        `idxstress`,
	Description: `IdxStress uses a multi-column PK which is also indexed in the opposite order to maximize the spread of index entries`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &idxstress{}
		g.flags.FlagSet = pflag.NewFlagSet(`idxstress`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.aCount, `a`, 10, `number of values of a (i.e. pk prefix)`)
		g.flags.IntVar(&g.bCount, `b`, 1000, `number of values of b (i.e. idx prefix)`)
		g.flags.IntVar(&g.cCount, `c`, 1000, `number of values of c (i.e. rows per a/b pair)`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each row.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// FromRows returns idxstress testdata with the approx number of rows and
// default payload size.
func FromRows(rows int) workload.Generator {
	a := 10
	b := 100
	c := rows / a * b
	return FromConfig(a, b, c, defaultPayloadBytes)
}

// FromConfig returns a one table testdata with three-col PK (a, b c) and a byte
// payload, with an index on (b, c, a) that also stores the payload.
func FromConfig(a, b, c int, payloadBytes int) workload.Generator {
	return workload.FromFlags(idxstressMeta,
		fmt.Sprintf(`--a=%d`, a),
		fmt.Sprintf(`--b=%d`, b),
		fmt.Sprintf(`--c=%d`, c),
		fmt.Sprintf(`--payload-bytes=%d`, payloadBytes),
	)
}

// Meta implements the Generator interface.
func (*idxstress) Meta() workload.Meta { return idxstressMeta }

// Flags implements the Flagser interface.
func (w *idxstress) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *idxstress) Hooks() workload.Hooks {
	return workload.Hooks{}
}

// Tables implements the Generator interface.
func (w *idxstress) Tables() []workload.Table {
	table := workload.Table{
		Name:   `idxstress`,
		Schema: indexStressSchemaWithIndex,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.aCount * w.bCount,
			NumTotal:   w.aCount * w.bCount * w.cCount,
			Batch: func(ab int) [][]interface{} {
				a := ab / w.bCount
				b := ab % w.bCount

				rng := rand.New(rand.NewSource(w.seed + int64(ab)))
				batch := make([][]interface{}, w.cCount)
				payload := make([]byte, w.cCount*w.payloadBytes)
				randutil.ReadTestdataBytes(rng, payload)
				for c := 0; c < w.cCount; c++ {
					off := c * w.payloadBytes
					batch[c] = []interface{}{a, b, c, payload[off : off+w.payloadBytes]}
				}
				return batch
			},
		},
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *idxstress) Ops(
	urls []string, reg *workload.HistogramRegistry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	updateStmt, err := db.Prepare(`
		UPDATE idxstress
		SET payload = $4
		WHERE a = $1 AND b = $2 AND c = $3
	`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(w.seed))
		hists := reg.GetHandle()
		pad := make([]byte, w.payloadBytes)
		workerFn := func(ctx context.Context) error {
			a := rng.Intn(w.aCount)
			b := rng.Intn(w.bCount)
			c := rng.Intn(w.cCount)
			if err := randutil.ReadTestdataBytes(rng, pad); err != nil {
				return err
			}

			start := timeutil.Now()
			res, err := updateStmt.Exec(a, b, c, pad)
			elapsed := timeutil.Since(start)
			hists.Get(`update-payload`).Record(elapsed)
			if err != nil {
				return err
			}
			if affected, err := res.RowsAffected(); err != nil {
				return err
			} else if affected != 1 {
				return errors.Errorf("expected 1 row affected, got %d", affected)
			}
			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}
