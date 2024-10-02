// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package querybench

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"os"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type queryBench struct {
	flags           workload.Flags
	connFlags       *workload.ConnFlags
	queryFile       string
	separator       string
	numRunsPerQuery int
	verbose         bool

	stmts []namedStmt
}

func init() {
	workload.Register(queryBenchMeta)
}

var queryBenchMeta = workload.Meta{
	Name:        `querybench`,
	Description: `QueryBench runs queries from the specified file.`,
	Details: `
Queries are run sequentially in each concurrent worker.

The file should contain one query per line, or alternatively specify a query
separator (e.g. ;) via --separator. Comments are specified with # or -- at the
start of the line (with whitespace). Each query can be prefixed by a name and
colon. For example:

-- This is a comment.
name: SELECT foo FROM bar

# Another comment. The query name is optional.
UPDATE bar SET foo = 'baz'
`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &queryBench{}
		g.flags.FlagSet = pflag.NewFlagSet(`querybench`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`query-file`: {RuntimeOnly: true},
			`separator`:  {RuntimeOnly: true},
			`num-runs`:   {RuntimeOnly: true},
		}
		g.flags.StringVar(&g.queryFile, `query-file`, ``, `File of newline separated queries to run`)
		g.flags.StringVar(&g.separator, `separator`, ``, `String separating queries (defaults to newline)`)
		g.flags.IntVar(&g.numRunsPerQuery, `num-runs`, 0, `Specifies the number of times each query in the query file to be run `+
			`(note that --duration and --max-ops take precedence, so if duration or max-ops is reached, querybench will exit without honoring --num-runs)`)
		g.flags.BoolVar(&g.verbose, `verbose`, true, `Prints out the queries being run as well as histograms`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*queryBench) Meta() workload.Meta { return queryBenchMeta }

// Flags implements the Flagser interface.
func (g *queryBench) Flags() workload.Flags { return g.flags }

// ConnFlags implements the ConnFlagser interface.
func (g *queryBench) ConnFlags() *workload.ConnFlags { return g.connFlags }

// Hooks implements the Hookser interface.
func (g *queryBench) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if g.queryFile == "" {
				return errors.Errorf("Missing required argument '--query-file'")
			}
			stmts, err := GetQueries(g.queryFile, g.separator)
			if err != nil {
				return err
			}
			if len(stmts) < 1 {
				return errors.New("no queries found in file")
			}
			g.stmts = stmts
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
func (g *queryBench) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(g.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(g.connFlags.Concurrency + 1)

	stmts := g.stmts
	for i := range stmts {
		if prep, err := db.Prepare(stmts[i].query); err == nil {
			stmts[i].preparedStmt = prep
		}
	}

	maxNumStmts := 0
	if g.numRunsPerQuery > 0 {
		maxNumStmts = g.numRunsPerQuery * len(stmts)
	}

	ql := workload.QueryLoad{}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		op := queryBenchWorker{
			hists:       reg.GetHandle(),
			db:          db,
			stmts:       stmts,
			verbose:     g.verbose,
			maxNumStmts: maxNumStmts,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

// GetQueries returns the queries in a file as a slice of named statements. If
// no separator is given, splits by newlines.
func GetQueries(path, separator string) ([]namedStmt, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// reComments removes comments at start of line. Don't attempt to parse the
	// query, just naïvely interpret # and -- as comments.
	reComments := regexp.MustCompile(`(?m)^\s*(#|--).*`)

	// reNamedQuery handles optional query names, e.g.:
	// name: SELECT foo FROM bar
	// If no name is given, the entire query is used as the name.
	reNamedQuery := regexp.MustCompile(`(?s)^\s*(\S+):\s*(.*)$`)

	// Scan queries up to 1 MB in size with the given separator.
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	if separator := []byte(separator); len(separator) > 0 {
		scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
			if i := bytes.Index(data, separator); i >= 0 {
				return i + len(separator), data[0:i], nil
			} else if atEOF && len(data) > 0 {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
	}

	var stmts []namedStmt
	for scanner.Scan() {
		query := scanner.Text()
		query = reComments.ReplaceAllLiteralString(query, ``)
		query = strings.TrimSpace(query)

		name := query
		if m := reNamedQuery.FindStringSubmatch(query); m != nil {
			name, query = m[1], m[2]
		}
		if len(query) > 0 {
			stmts = append(stmts, namedStmt{
				name:  name,
				query: query,
			})
		}
	}
	return stmts, scanner.Err()
}

type namedStmt struct {
	name string
	// We will try to Prepare the statement, and if that succeeds, the prepared
	// statement will be stored in `preparedStmt', otherwise, we will store
	// plain query in 'query'.
	preparedStmt *gosql.Stmt
	query        string
}

type queryBenchWorker struct {
	hists *histogram.Histograms
	db    *gosql.DB
	stmts []namedStmt

	stmtIdx int
	verbose bool

	// maxNumStmts indicates the maximum number of statements for the worker to
	// execute. It is non-zero only when --num-runs flag is specified for the
	// workload.
	maxNumStmts int
}

func (o *queryBenchWorker) run(ctx context.Context) error {
	if o.maxNumStmts > 0 {
		if o.stmtIdx >= o.maxNumStmts {
			// This worker has already reached the maximum number of statements to
			// execute.
			return nil
		}
	}
	start := timeutil.Now()
	stmt := o.stmts[o.stmtIdx%len(o.stmts)]
	o.stmtIdx++

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
	if stmt.preparedStmt != nil {
		if err := exhaustRows(func() (*gosql.Rows, error) {
			return stmt.preparedStmt.Query()
		}); err != nil {
			return err
		}
	} else {
		if err := exhaustRows(func() (*gosql.Rows, error) {
			return o.db.Query(stmt.query)
		}); err != nil {
			return err
		}
	}
	elapsed := timeutil.Since(start)
	if o.verbose {
		o.hists.Get(stmt.name).Record(elapsed)
	} else {
		o.hists.Get("").Record(elapsed)
	}
	return nil
}
