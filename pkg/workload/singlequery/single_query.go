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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package singlequery

import (
	"context"
	gosql "database/sql"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type singleQuery struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	query string
}

func init() {
	workload.Register(singleQueryMeta)
}

var singleQueryMeta = workload.Meta{
	Name:        `singlequery`,
	Description: `SingleQuery runs the specified query repeatedly`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &singleQuery{}
		g.flags.FlagSet = pflag.NewFlagSet(`singlequery`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`query`: {RuntimeOnly: true},
		}
		g.flags.StringVar(&g.query, `query`, ``, `Query to run`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*singleQuery) Meta() workload.Meta { return singleQueryMeta }

// Flags implements the Flagser interface.
func (g *singleQuery) Flags() workload.Flags { return g.flags }

// Hooks implements the Hookser interface.
func (g *singleQuery) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if g.query == "" {
				return errors.Errorf("Missing required argument '--query'")
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (*singleQuery) Tables() []workload.Table {
	// Assume the necessary tables are already present.
	return []workload.Table{}
}

// Ops implements the Opser interface.
func (g *singleQuery) Ops(
	urls []string, reg *workload.HistogramRegistry,
) (workload.QueryLoad, error) {
	if g.query == "" {
		panic("Missing required argument --query")
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

	stmt, err := db.Prepare(g.query)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		op := singleQueryOp{
			hists: reg.GetHandle(),
			db:    db,
			stmt:  stmt,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

type singleQueryOp struct {
	hists *workload.Histograms
	db    *gosql.DB
	stmt  *gosql.Stmt
}

func (o *singleQueryOp) run(ctx context.Context) error {
	start := timeutil.Now()
	rows, err := o.stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return err
	}
	o.hists.Get(`query`).Record(timeutil.Since(start))
	return nil
}
