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

package querybench

import (
	"bufio"
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type queryBench struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
	queryFile string
	useOpt    bool

	queries []string
}

func init() {
	workload.Register(queryBenchMeta)
}

var queryBenchMeta = workload.Meta{
	Name: `querybench`,
	Description: `QueryBench runs queries from the specified file. The queries are run ` +
		`sequentially in each concurrent worker.`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &queryBench{}
		g.flags.FlagSet = pflag.NewFlagSet(`querybench`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`query-file`: {RuntimeOnly: true},
		}
		g.flags.StringVar(&g.queryFile, `query-file`, ``, `File of newline separated queries to run`)
		g.flags.BoolVar(&g.useOpt, `use-opt`, true, `Use cost-based optimizer`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*queryBench) Meta() workload.Meta { return queryBenchMeta }

// Flags implements the Flagser interface.
func (g *queryBench) Flags() workload.Flags { return g.flags }

// Hooks implements the Hookser interface.
func (g *queryBench) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if g.queryFile == "" {
				return errors.Errorf("Missing required argument '--query-file'")
			}
			queries, err := getQueries(g.queryFile)
			if err != nil {
				return err
			}
			if len(queries) < 1 {
				return errors.New("no queries found in file")
			}
			g.queries = queries
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (*queryBench) Tables() []workload.Table {
	// Assume the necessary tables are already present.
	return []workload.Table{}
}

// Ops implements the Opser interface.
func (g *queryBench) Ops(
	urls []string, reg *workload.HistogramRegistry,
) (workload.QueryLoad, error) {
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

	if !g.useOpt {
		_, err := db.Exec("SET optimizer=off")
		if err != nil {
			return workload.QueryLoad{}, err
		}
	}

	stmts := make([]namedStmt, len(g.queries))
	for i, query := range g.queries {
		stmt, err := db.Prepare(query)
		if err != nil {
			return workload.QueryLoad{}, errors.Wrapf(err, "failed to prepare query %q", query)
		}
		stmts[i] = namedStmt{
			// TODO(solon): Allow specifying names in the query file rather than using
			// the entire query as the name.
			name: fmt.Sprintf("%2d: %s", i+1, query),
			stmt: stmt,
		}
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		op := queryBenchWorker{
			hists: reg.GetHandle(),
			db:    db,
			stmts: stmts,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

// getQueries returns the lines of a file as a string slice. Ignores lines
// beginning with '#' or '--'.
func getQueries(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] != '#' && !strings.HasPrefix(line, "--") {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

type namedStmt struct {
	name string
	stmt *gosql.Stmt
}

type queryBenchWorker struct {
	hists *workload.Histograms
	db    *gosql.DB
	stmts []namedStmt

	stmtIdx int
}

func (o *queryBenchWorker) run(ctx context.Context) error {
	start := timeutil.Now()
	stmt := o.stmts[o.stmtIdx]
	o.stmtIdx++
	o.stmtIdx %= len(o.stmts)

	rows, err := stmt.stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return err
	}
	o.hists.Get(stmt.name).Record(timeutil.Since(start))
	return nil
}
