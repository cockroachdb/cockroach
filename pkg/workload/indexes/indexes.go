// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexes

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	schemaBase = `(
		key     UUID  NOT NULL PRIMARY KEY,
		col0    INT   NOT NULL,
		col1    INT   NOT NULL,
		col2    INT   NOT NULL,
		col3    INT   NOT NULL,
		col4    INT   NOT NULL,
		col5    INT   NOT NULL,
		col6    INT   NOT NULL,
		col7    INT   NOT NULL,
		col8    INT   NOT NULL,
		col9    INT   NOT NULL,
		payload BYTES NOT NULL`
)

type indexes struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        int64
	idxs        int
	unique      bool
	payload     int
	cycleLength uint64
}

func init() {
	workload.Register(indexesMeta)
}

var indexesMeta = workload.Meta{
	Name:        `indexes`,
	Description: `Indexes writes to a table with a variable number of secondary indexes`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &indexes{}
		g.flags.FlagSet = pflag.NewFlagSet(`indexes`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.idxs, `secondary-indexes`, 1, `Number of indexes to add to the table.`)
		g.flags.BoolVar(&g.unique, `unique-indexes`, false, `Use UNIQUE secondary indexes.`)
		g.flags.IntVar(&g.payload, `payload`, 64, `Size of the unindexed payload column.`)
		g.flags.Uint64Var(&g.cycleLength, `cycle-length`, math.MaxUint64,
			`Number of keys repeatedly accessed by each writer through upserts.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*indexes) Meta() workload.Meta { return indexesMeta }

// Flags implements the Flagser interface.
func (w *indexes) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *indexes) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.idxs < 0 || w.idxs > 99 {
				return errors.Errorf(`--secondary-indexes must be in range [0, 99]`)
			}
			if w.payload < 1 {
				return errors.Errorf(`--payload size must be equal to or greater than 1`)
			}
			return nil
		},
		PostLoad: func(sqlDB *gosql.DB) error {
			// Prevent the merge queue from immediately discarding our splits.
			if err := maybeDisableMergeQueue(sqlDB); err != nil {
				return err
			}
			// Split at the beginning of each index so that as long as the
			// table has a single index, all writes will be multi-range.
			for i := 0; i < w.idxs; i++ {
				split := fmt.Sprintf(`ALTER INDEX idx%d SPLIT AT VALUES (%d)`, i, math.MinInt64)
				if _, err := sqlDB.Exec(split); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func maybeDisableMergeQueue(sqlDB *gosql.DB) error {
	var ok bool
	if err := sqlDB.QueryRow(
		`SELECT count(*) > 0 FROM [ SHOW ALL CLUSTER SETTINGS ] AS _ (v) WHERE v = 'kv.range_merge.queue_enabled'`,
	).Scan(&ok); err != nil || !ok {
		return err
	}
	_, err := sqlDB.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false")
	return err
}

// Tables implements the Generator interface.
func (w *indexes) Tables() []workload.Table {
	// Construct the schema with all indexes.
	var unique string
	if w.unique {
		unique = "UNIQUE "
	}
	var b strings.Builder
	b.WriteString(schemaBase)
	for i := 0; i < w.idxs; i++ {
		col1, col2 := i/10, i%10
		if col1 == col2 {
			fmt.Fprintf(&b, ",\n\t\t%sINDEX idx%d (col%d)", unique, i, col1)
		} else {
			fmt.Fprintf(&b, ",\n\t\t%sINDEX idx%d (col%d, col%d)", unique, i, col1, col2)
		}
	}
	b.WriteString("\n)")

	return []workload.Table{{
		Name:   `indexes`,
		Schema: b.String(),
	}}
}

// Ops implements the Opser interface.
func (w *indexes) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: w.connFlags.Concurrency + 1,
	}
	mcp, err := workload.NewMultiConnPool(cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	const stmt = `UPSERT INTO indexes VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := &indexesOp{
			config: w,
			hists:  reg.GetHandle(),
			rand:   rand.New(rand.NewSource(int64((i + 1)) * w.seed)),
			buf:    make([]byte, w.payload),
		}
		op.stmt = op.sr.Define(stmt)
		if err := op.sr.Init(ctx, "indexes", mcp, w.connFlags); err != nil {
			return workload.QueryLoad{}, err
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

type indexesOp struct {
	config *indexes
	hists  *histogram.Histograms
	rand   *rand.Rand
	sr     workload.SQLRunner
	stmt   workload.StmtHandle
	buf    []byte
}

func (o *indexesOp) run(ctx context.Context) error {
	keyLo := o.rand.Uint64() % o.config.cycleLength
	_, _ = o.rand.Read(o.buf[:])
	args := []interface{}{
		uuid.FromUint128(uint128.FromInts(0, keyLo)).String(), // key
		int64(keyLo + 0), // col0
		int64(keyLo + 1), // col1
		int64(keyLo + 2), // col2
		int64(keyLo + 3), // col3
		int64(keyLo + 4), // col4
		int64(keyLo + 5), // col5
		int64(keyLo + 6), // col6
		int64(keyLo + 7), // col7
		int64(keyLo + 8), // col8
		int64(keyLo + 9), // col9
		o.buf[:],         // payload
	}

	start := timeutil.Now()
	_, err := o.stmt.Exec(ctx, args...)
	elapsed := timeutil.Since(start)
	o.hists.Get(`write`).Record(elapsed)
	return err
}
