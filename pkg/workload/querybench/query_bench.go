// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type queryBench struct {
	flags           workload.Flags
	connFlags       *workload.ConnFlags
	queryFile       string
	numRunsPerQuery int
	vectorize       string
	verbose         bool

	queryGroups [][]string
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
			`optimizer`:  {RuntimeOnly: true},
			`vectorize`:  {RuntimeOnly: true},
			`num-runs`:   {RuntimeOnly: true},
		}
		g.flags.StringVar(&g.queryFile, `query-file`, ``, `File of newline separated queries to run. `+
			`Note that multiple queries can be appear on the same line but need to be separated with a semicolon`)
		g.flags.IntVar(&g.numRunsPerQuery, `num-runs`, 0, `Specifies the number of times each query in the query file to be run `+
			`(note that --duration and --max-ops take precedence, so if duration or max-ops is reached, querybench will exit without honoring --num-runs)`)
		g.flags.StringVar(&g.vectorize, `vectorize`, "", `Set vectorize session variable`)
		g.flags.BoolVar(&g.verbose, `verbose`, true, `Prints out the queries being run as well as histograms`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// vectorizeSetting19_2Translation is a mapping from the 20.1+ vectorize session
// variable value to the 19.2 syntax.
var vectorizeSetting19_2Translation = map[string]string{
	"on": "experimental_on",
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
			gueryGroups, err := GetQueryGroups(g.queryFile)
			if err != nil {
				return err
			}
			if len(gueryGroups) < 1 {
				return errors.New("no queries found in file")
			}
			g.queryGroups = gueryGroups
			if g.numRunsPerQuery < 0 {
				return errors.New("negative --num-runs specified")
			}
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
func (g *queryBench) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
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

	if g.vectorize != "" {
		_, err := db.Exec("SET vectorize=" + g.vectorize)
		if err != nil && strings.Contains(err.Error(), "invalid value") {
			if _, ok := vectorizeSetting19_2Translation[g.vectorize]; ok {
				// Fall back to using the pre-20.1 vectorize options.
				_, err = db.Exec("SET vectorize=" + vectorizeSetting19_2Translation[g.vectorize])
			}
		}
		if err != nil {
			return workload.QueryLoad{}, err
		}
	}

	queryGroups := make([]namedQueryGroup, len(g.queryGroups))
	for i, queryGroup := range g.queryGroups {
		queryGroups[i] = namedQueryGroup{
			// TODO(solon): Allow specifying names in the query file rather
			// than using the entire query group as the name.
			name: fmt.Sprintf("%2d: %s", i+1, strings.Join(queryGroup, "; ")),
		}
		prepareFailed := false
		for _, query := range queryGroup {
			stmt, err := db.Prepare(query)
			if err != nil {
				prepareFailed = true
				queryGroups[i].preparedStmts = queryGroups[i].preparedStmts[:0]
				break
			}
			queryGroups[i].preparedStmts = append(queryGroups[i].preparedStmts, stmt)
		}
		if prepareFailed {
			queryGroups[i].queries = queryGroup
		}
	}

	maxNumStmts := 0
	if g.numRunsPerQuery > 0 {
		maxNumStmts = g.numRunsPerQuery * len(g.queryGroups)
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		op := queryBenchWorker{
			hists:       reg.GetHandle(),
			db:          db,
			queryGroups: queryGroups,
			verbose:     g.verbose,
			maxNumStmts: maxNumStmts,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

// GetQueryGroups returns the lines of a file as "grouped" string slices.
// Multiple queries can be placed on a single line separated by ';'. Ignores
// lines beginning with '#' or '--'.
func GetQueryGroups(path string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Read lines up to 1 MB in size.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var queryGroups [][]string
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] != '#' && !strings.HasPrefix(line, "--") {
			queriesInLine := strings.Split(line, ";")
			for i := range queriesInLine {
				queriesInLine[i] = strings.TrimSpace(queriesInLine[i])
			}
			queryGroups = append(queryGroups, queriesInLine)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return queryGroups, nil
}

type namedQueryGroup struct {
	name string
	// If all queries in the group can be prepared, then they will be stored
	// in preparedStmts, otherwise "raw" queries will be stored in queries.
	preparedStmts []*gosql.Stmt
	queries       []string
}

type queryBenchWorker struct {
	hists       *histogram.Histograms
	db          *gosql.DB
	queryGroups []namedQueryGroup

	queryGroupIdx int
	verbose       bool

	// maxNumStmts indicates the maximum number of statements for the worker to
	// execute. It is non-zero only when --num-runs flag is specified for the
	// workload.
	maxNumStmts int
}

func (o *queryBenchWorker) run(ctx context.Context) error {
	if o.maxNumStmts > 0 {
		if o.queryGroupIdx >= o.maxNumStmts {
			// This worker has already reached the maximum number of statements to
			// execute.
			return nil
		}
	}
	start := timeutil.Now()
	queryGroup := o.queryGroups[o.queryGroupIdx%len(o.queryGroups)]
	o.queryGroupIdx++

	exhaustRows := func(execFn func() (*gosql.Rows, error)) error {
		rows, err := execFn()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	}
	if len(queryGroup.preparedStmts) > 0 {
		for _, preparedStmt := range queryGroup.preparedStmts {
			if err := exhaustRows(func() (*gosql.Rows, error) {
				return preparedStmt.Query()
			}); err != nil {
				return err
			}
		}
	} else {
		for _, query := range queryGroup.queries {
			if err := exhaustRows(func() (*gosql.Rows, error) {
				return o.db.Query(query)
			}); err != nil {
				return err
			}
		}
	}
	elapsed := timeutil.Since(start)
	if o.verbose {
		o.hists.Get(queryGroup.name).Record(elapsed)
	} else {
		o.hists.Get("").Record(elapsed)
	}
	return nil
}
