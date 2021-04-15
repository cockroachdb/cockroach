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
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type sqlSmith struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed          int64
	tables        int
	errorSettings int
}

type errorSettingTypes int

const (
	ignoreExecErrors errorSettingTypes = iota
	returnOnInternalError
	returnOnError
)

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
		g.flags.IntVar(&g.errorSettings, `error-sensitivity`, 0,
			`SQLSmith's sensitivity to errors. 0=ignore all errors. 1=quit on internal errors. 2=quit on any error.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*sqlSmith) Meta() workload.Meta { return sqlSmithMeta }

// Flags implements the Flagser interface.
func (g *sqlSmith) Flags() workload.Flags { return g.flags }

func (g *sqlSmith) Hooks() workload.Hooks {
	return workload.Hooks{
		PreCreate: func(db *gosql.DB) error {
			if _, err := db.Exec(`
SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled = true;
SET CLUSTER SETTING sql.defaults.drop_enum_value.enabled = true;
SET enable_drop_enum_value = true;
`); err != nil {
				return err
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (g *sqlSmith) Tables() []workload.Table {
	rng := rand.New(rand.NewSource(g.seed))
	var tables []workload.Table
	for idx := 0; idx < g.tables; idx++ {
		schema := randgen.RandCreateTable(rng, "table", idx)
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

func (g *sqlSmith) handleError(err error) error {
	if err != nil {
		switch errorSettingTypes(g.errorSettings) {
		case ignoreExecErrors:
			return nil
		case returnOnInternalError:
			if strings.Contains(err.Error(), "internal error") {
				return err
			}
		case returnOnError:
			return err
		}
	}
	return nil
}

func (g *sqlSmith) validateErrorSetting() error {
	switch errorSettingTypes(g.errorSettings) {
	case ignoreExecErrors:
	case returnOnInternalError:
	case returnOnError:
	default:
		return errors.Newf("invalid value for error-sensitivity: %d", g.errorSettings)
	}
	return nil
}

// Ops implements the Opser interface.
func (g *sqlSmith) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if err := g.validateErrorSetting(); err != nil {
		return workload.QueryLoad{}, err
	}
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
			_, err := db.ExecContext(ctx, query)
			if handledErr := g.handleError(err); handledErr != nil {
				return handledErr
			}
			elapsed = timeutil.Since(start)

			hists.Get(`exec`).Record(elapsed)

			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}
