// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/spf13/pflag"
)

type sqlSmith struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed   int64
	tables int
}

func init() {
	workload.Register(sqlSmithMeta)
}

var sqlSmithMeta = workload.Meta{
	Name:        `sqlsmith`,
	Description: `sqlsmith is a random SQL query generator`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &sqlSmith{}
		g.flags.FlagSet = pflag.NewFlagSet(`sqlsmith`, pflag.ContinueOnError)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.tables, `tables`, 1, `Number of tables.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*sqlSmith) Meta() workload.Meta { return sqlSmithMeta }

// Flags implements the Flagser interface.
func (g *sqlSmith) Flags() workload.Flags { return g.flags }

// Tables implements the Generator interface.
func (g *sqlSmith) Tables() []workload.Table {
	rng := rand.New(rand.NewSource(g.seed))
	var tables []workload.Table
	for idx := 0; idx < g.tables; idx++ {
		schema := sqlbase.RandCreateTable(rng, "table", idx)
		table := workload.Table{
			Name:   schema.Table.String(),
			Schema: tree.Serialize(schema),
		}
		// workload expects the schema to be missing the CREATE TABLE "name", so
		// drop everything before the first `(`.
		table.Schema = table.Schema[strings.Index(table.Schema, `(`):]
		tables = append(tables, table)
	}
	return tables
}

// Ops implements the Opser interface.
func (g *sqlSmith) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(g, g.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(g.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(g.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(g.seed + int64(i)))
		smither, err := sqlsmith.NewSmither(db, rng)
		if err != nil {
			return workload.QueryLoad{}, err
		}

		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			start := timeutil.Now()
			query := smither.Generate()
			elapsed := timeutil.Since(start)
			hists.Get(`generate`).Record(elapsed)

			start = timeutil.Now()
			if _, err := db.ExecContext(ctx, query); err != nil {
				return err
			}
			elapsed = timeutil.Since(start)
			hists.Get(`exec`).Record(elapsed)

			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}
