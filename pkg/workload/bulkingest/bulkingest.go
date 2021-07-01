// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package bulkingest defines a workload that is intended to stress some edge cases
in our bulk-ingestion infrastructure.

In both IMPORT and indexing, many readers scan though the source data (i.e. CSV
files or PK rows, respectively) and produce KVs to be ingested. However a given
range of that source data could produce any KVs -- i.e. in some schemas or
workloads, the produced KVs could have the same ordering or in some they could
be random and uniformly distributed in the keyspace. Additionally, both of the
processes often include concurrent producers, each scanning their own input
files or ranges of a table, and there the distribution could mean that
concurrent producers all produce different keys or all produce similar keys at
the same time, etc.

This workload is intended to produce testdata that emphasizes these cases. The
multi-column PK is intended to make it easy to independently control the prefix
of keys. Adding an index on the same columns with the columns reordered can then
control the flow of keys between prefixes, stressing any buffering, sorting or
other steps in the middle. This can be particularly interesting when concurrent
producers are a factor, as the distribution (or lack there of) of their output
prefixes at a given moment can cause hotspots.

The workload's schema is a table with columns a, b, and c plus a padding payload
string, with the primary key being (a,b,c).

Creating indexes on the different columns in this schema can then trigger
different distributions of produced index KVs -- i.e. an index on (b, c) would
see each range of PK data produce tightly grouped output that overlaps with the
output of A other ranges of the table.

The workload's main parameters are number of distinct values of a, b and c.
Initial data batches each correspond to one a/b pair containing c rows. By
default, batches are ordered by a then b (a=1/b=1, a=1/b=2, a=1,b=3, ...) though
this can optionally be inverted (a=1/b=1, a=2,b=1, a=3,b=1,...).

*/
package bulkingest

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	bulkingestSchemaPrefix = `(
		a INT,
		b INT,
		c INT,
		payload STRING,
		PRIMARY KEY (a, b, c)`

	indexOnBCA = ",\n INDEX (b, c, a) STORING (payload)"

	defaultPayloadBytes = 100
)

type bulkingest struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                                 int64
	aCount, bCount, cCount, payloadBytes int

	generateBsFirst bool
	indexBCA        bool
}

func init() {
	workload.Register(bulkingestMeta)
}

var bulkingestMeta = workload.Meta{
	Name:        `bulkingest`,
	Description: `bulkingest testdata is designed to produce a skewed distribution of KVs when ingested (in initial import or during later indexing)`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &bulkingest{}
		g.flags.FlagSet = pflag.NewFlagSet(`bulkingest`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.aCount, `a`, 10, `number of values of A (i.e. pk prefix)`)
		g.flags.IntVar(&g.bCount, `b`, 10, `number of values of B (i.e. idx prefix)`)
		g.flags.IntVar(&g.cCount, `c`, 1000, `number of values of C (i.e. rows per A/B pair)`)
		g.flags.BoolVar(&g.generateBsFirst, `batches-by-b`, false, `generate all B batches for given A first`)
		g.flags.BoolVar(&g.indexBCA, `index-b-c-a`, true, `include an index on (B, C, A)`)
		g.flags.IntVar(&g.payloadBytes, `payload-bytes`, defaultPayloadBytes, `Size of the payload field in each row.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*bulkingest) Meta() workload.Meta { return bulkingestMeta }

// Flags implements the Flagser interface.
func (w *bulkingest) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *bulkingest) Hooks() workload.Hooks {
	return workload.Hooks{}
}

// Tables implements the Generator interface.
func (w *bulkingest) Tables() []workload.Table {
	schema := bulkingestSchemaPrefix
	if w.indexBCA {
		schema += indexOnBCA
	}
	schema += ")"

	var bulkingestTypes = []*types.T{
		types.Int,
		types.Int,
		types.Int,
		types.Bytes,
	}

	table := workload.Table{
		Name:   `bulkingest`,
		Schema: schema,
		InitialRows: workload.BatchedTuples{
			NumBatches: w.aCount * w.bCount,
			FillBatch: func(ab int, cb coldata.Batch, alloc *bufalloc.ByteAllocator) {
				a := ab / w.bCount
				b := ab % w.bCount
				if w.generateBsFirst {
					b = ab / w.aCount
					a = ab % w.aCount
				}

				cb.Reset(bulkingestTypes, w.cCount, coldata.StandardColumnFactory)
				aCol := cb.ColVec(0).Int64()
				bCol := cb.ColVec(1).Int64()
				cCol := cb.ColVec(2).Int64()
				payloadCol := cb.ColVec(3).Bytes()

				rng := rand.New(rand.NewSource(w.seed + int64(ab)))
				var payload []byte
				*alloc, payload = alloc.Alloc(w.cCount*w.payloadBytes, 0 /* extraCap */)
				randutil.ReadTestdataBytes(rng, payload)
				payloadCol.Reset()
				for rowIdx := 0; rowIdx < w.cCount; rowIdx++ {
					c := rowIdx
					off := c * w.payloadBytes
					aCol[rowIdx] = int64(a)
					bCol[rowIdx] = int64(b)
					cCol[rowIdx] = int64(c)
					payloadCol.Set(rowIdx, payload[off:off+w.payloadBytes])
				}
			},
		},
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *bulkingest) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
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
		UPDATE bulkingest
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
			randutil.ReadTestdataBytes(rng, pad)

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
