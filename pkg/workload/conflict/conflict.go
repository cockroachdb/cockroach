// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/spf13/pflag"
)

const (
	conflictSchema = `(
		id INT PRIMARY KEY,
		data TEXT NOT NULL
	)`
)

// TODO(jeffswenson): figure out how to support computed columns. Supporting
// them within the primary key is difficult since we use the primary key columns
// to identify which row is being upserted or deleted. We could maybe support
// them by tracking all columns that the computed column depends on and treating
// them as immutable in the same way we do for primary key columns.
var RandTableOpt = func() randgen.TableOpt {
	opts := []randgen.TableOpt{
		// Require a primary index since LDR semantics are defined in terms of
		// the primary key.
		randgen.TableOptPrimaryIndexRequired,
		// Disable computed columns. They are supported by LDR, but the conflict
		// workload does not know how to handle them.
		randgen.TableOptDisableComputedColumns,
		// Require valid random data. Otherwise we expect many of the operations
		// to fail with errors that are not interseting for LDR.
		randgen.TableOptRequireValidRandomData,
		// LDR does not support column family mutations.
		randgen.TableOptSkipColumnFamilyMutations,
		// The immediate mode writer does not support expression indexes.
		randgen.TableOptDisableExpressionIndex,
	}
	var opt randgen.TableOpt
	for _, option := range opts {
		opt |= option
	}
	return opt
}()

var RandomSeed = workload.NewInt64RandomSeed()

// conflict is a workload that is designed to generate a high conflict rate for
// testing LDR with random schemas. It is not intended to be a stable benchmark.
type conflict struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	conflicts int
	tableName string
}

func init() {
	workload.Register(conflictMeta)
}

var conflictMeta = workload.Meta{
	Name:        `conflict`,
	Description: `Conflict workload generates transaction conflicts by having multiple workers update the same rows`,
	Version:     `1.0.0`,
	RandomSeed:  RandomSeed,
	New: func() workload.Generator {
		g := &conflict{}
		g.flags.FlagSet = pflag.NewFlagSet(`conflict`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`conflicts`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.conflicts, `conflicts`, 10, `Number of rows to contend over for conflicts (smaller = more conflicts)`)
		g.flags.StringVar(&g.tableName, `table`, `conflict`, `Name of the table to use for the conflict workload`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*conflict) Meta() workload.Meta { return conflictMeta }

// Flags implements the Flagser interface.
func (w *conflict) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *conflict) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *conflict) Hooks() workload.Hooks {
	return workload.Hooks{}
}

// Tables implements the Generator interface.
func (w *conflict) Tables() []workload.Table {
	rng := rand.New(rand.NewSource(RandomSeed.Seed()))
	statement := randgen.RandCreateTable(context.Background(), rng, "conflict", 1, RandTableOpt)
	ctx := tree.NewFmtCtx(tree.FmtParsable)
	statement.FormatBody(ctx)
	table := workload.Table{
		Name:   w.tableName,
		Schema: ctx.CloseAndGetString(),
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *conflict) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	log.Infof(ctx, "running conflict workload, concurrency: %d, urls: %v", w.connFlags.Concurrency, urls)

	dbs := make([]*gosql.DB, len(urls))
	for i, url := range urls {
		db, err := gosql.Open(`cockroach`, url)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		db.SetMaxOpenConns(w.connFlags.Concurrency)
		db.SetMaxIdleConns(w.connFlags.Concurrency)
		dbs[i] = db
	}

	workerFns := make([]func(context.Context) error, len(urls))
	for i := range workerFns {
		worker, err := newConflictWorker(ctx, dbs, w.tableName)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		workerFns[i] = worker.RunOp
	}

	return workload.QueryLoad{
		WorkerFns: workerFns,
	}, nil
}
