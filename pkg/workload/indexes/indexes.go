// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

var RandomSeed = workload.NewInt64RandomSeed()

type indexes struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	idxs        int
	unique      bool
	payload     int
	cycleLength uint64
	workload    string
}

func init() {
	workload.Register(indexesMeta)
}

var indexesMeta = workload.Meta{
	Name:        `indexes`,
	Description: `Indexes writes to a table with a variable number of secondary indexes`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &indexes{}
		g.flags.FlagSet = pflag.NewFlagSet(`indexes`, pflag.ContinueOnError)
		g.flags.IntVar(&g.idxs, `secondary-indexes`, 1, `Number of indexes to add to the table.`)
		g.flags.BoolVar(&g.unique, `unique-indexes`, false, `Use UNIQUE secondary indexes.`)
		g.flags.IntVar(&g.payload, `payload`, 64, `Size of the unindexed payload column.`)
		g.flags.Uint64Var(&g.cycleLength, `cycle-length`, math.MaxUint64,
			`Number of keys repeatedly accessed by each writer through upserts.`)
		g.flags.StringVar(&g.workload, `workload`, `upsert`,
			`Statement for workers to run [upsert, insert, update]. Defaults to upsert. `+
				`Insert statements will fail if run after an insert or upsert workload with the same --seed. `+
				`Update statements will fail unless run after an insert or upsert workload with the same --seed.`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*indexes) Meta() workload.Meta { return indexesMeta }

// Flags implements the Flagser interface.
func (w *indexes) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *indexes) ConnFlags() *workload.ConnFlags { return w.connFlags }

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
		PostLoad: func(_ context.Context, sqlDB *gosql.DB) error {
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
		`SELECT count(*) > 0 FROM [ SHOW ALL CLUSTER SETTINGS ] AS _ (v) WHERE v = 'kv.range_merge.queue.enabled'`,
	).Scan(&ok); err != nil || !ok {
		return err
	}
	_, err := sqlDB.Exec("SET CLUSTER SETTING kv.range_merge.queue.enabled = false")
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
	cfg := workload.NewMultiConnPoolCfgFromFlags(w.connFlags)
	cfg.MaxTotalConnections = w.connFlags.Concurrency + 1
	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	var stmt string
	switch strings.ToLower(w.workload) {
	case `upsert`:
		stmt = `UPSERT INTO indexes VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	case `insert`:
		stmt = `INSERT INTO indexes VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	case `update`:
		stmt = `UPDATE indexes
                SET col0 = $2, col1 = $3, col2 = $4, col3 = $5, col4 = $6, col5 = $7,
                    col6 = $8, col7 = $9, col8 = $10, col9 = $11, payload = $12
                WHERE key = $1`
	default:
		return workload.QueryLoad{}, errors.Errorf("unknown workload: %q", w.workload)
	}

	ql := workload.QueryLoad{}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := &indexesOp{
			config: w,
			hists:  reg.GetHandle(),
			rand:   rand.New(rand.NewSource(int64((i + 1)) * RandomSeed.Seed())),
			buf:    make([]byte, w.payload),
		}
		op.stmt = op.sr.Define(stmt)
		if err := op.sr.Init(ctx, "indexes", mcp); err != nil {
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
	res, err := o.stmt.Exec(ctx, args...)
	elapsed := timeutil.Since(start)
	o.hists.Get(`write`).Record(elapsed)
	if err != nil {
		return err
	}
	if rows := res.RowsAffected(); rows != 1 {
		return errors.Errorf("expected 1 row affected, saw %d", rows)
	}
	return nil
}
