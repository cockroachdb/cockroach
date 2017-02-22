// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"bufio"
	"bytes"
	"crypto/md5"
	gosql "database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
)

// This file is home to TestLogic, a general-purpose engine for
// running SQL logic tests.
//
// TestLogic implements the infrastructure that runs end-to-end tests
// against CockroachDB's SQL layer. It is typically used to run
// CockroachDB's own tests (stored in the `testdata` directory) during
// development and CI, and a subset of SQLite's "Sqllogictest" during
// nightly CI runs. However, any test input can be specified via
// command-line flags (see below).
//
// In a nutshell, TestLogic reads one or more test input files
// containing sequences of SQL statements and queries. Each input file
// is meant to test a feature group. The reason why tests can/should
// be split across multiple files is that each test input file gets
// its own fresh, empty database.
//
// Input files for unit testing are stored alongside the source code
// in the `testdata` subdirectory. The input files for the larger
// `bigtest` are stored in a separate repository.
//
// The test input is expressed using a domain-specific language, called
// Test-Script, defined by SQLite's "Sqllogictest".  The official home
// of Sqllogictest and Test-Script is
//      https://www.sqlite.org/sqllogictest/
//
// (CockroachDB's `bigtest` is actually a fork of the Sqllogictest
// test files; its input files are hosted at
// https://github.com/cockroachdb/sqllogictest )
//
// The Test-Script language is extended here for use with CockroachDB,
// for example it introduces the "traceon" and "traceoff"
// directives. See processTestFile() for all supported test
// directives.
//
// Test-Script is line-oriented. It supports both statements which
// generate no result rows, and queries that produce result rows. The
// result of queries can be checked either using an explicit reference
// output in the test file, or using the expected row count and a hash
// of the expected output. A test can also check for expected column
// names for query results, or expected errors.
//
// The overall architecture of TestLogic is as follows:

// - TestLogic() selects the input files and instantiates
//   a `logicTest` object for each input file.
//
// - logicTest.run() sets up a new database.
// - logicTest.processTestFile() runs all tests from that input file.
//
// - each individual test in an input file is instantiated either as a
//   logicStatement or logicQuery object. These are then processed by
//   either logicTest.execStatement() or logicTest.execQuery().
//
// TestLogic has three main parameter groups:
//
// - Which input files are processed.
// - How and when to stop when tests fail.
// - Which results are reported.
//
// The parameters are typically set using the TESTFLAGS `make`
// parameter, as in:
//
//   make test PKG=./pkg/sql TESTS=TestLogic TESTFLAGS='....'
//
// Input file selection:
//
// -d <glob>  selects all files matching <glob>. This can mix and
//            match wildcards (*/?) or groups like {a,b,c}.
//
// -bigtest   cancels any -d setting and selects all relevant input
//            files from CockroachDB's fork of Sqllogictest.
//
// Error mode:
//
// -max-errors N  stop testing after N errors have been
//                encountered. Default 1. Set to 0 for no limit.
//
// -allow-prepare-fail
//                tolerate / ignore errors that occur during query
//                preparation. With -allow-prepare-fail you can
//                indicate that it is OK as long as the database
//                reports early to the client that it does not support
//                a given query.  Errors are still reported if queries
//                fail during execution only or if a statement fails.
//
// -flex-types    tolerate when a result column is produced with a
//                different numeric type than the one expected by the
//                test. This enables reusing tests designed for
//                database with sligtly different typing semantics.
//
// Test output:
//
// -v             (or -test.v if the test is compiled as a standalone
//                binary). Go `testing`'s `verbose` flag.
//
// The output generated by the following flags is suppressed unless
// either -v is given or a test fails.
//
// -show-sql      show SQL statements/queries immediately before they
//                are tested. This can be useful for example when
//                troubleshooting errors that cause the database/test
//                to stop before the test completes.  When -show-sql
//                is set, individual test results are annoted with
//                either "OK" (test passed as expected), "XFAIL"
//                (expected failure, test failed as expected), or
//                "FAIL" to indicate an unexpected/undesired test
//                failure.
//
// -show-logs     shows logs (by default, logs go to temporary files which are
//                kept only for tests that fail)
//
// -error-summary produces a report organized by error message
//                of all queries that have caused that error.  Useful
//                with -allow-prepare-fail and/or -flex-types.
//
// -full-messages by default -error-summary shortens error messages
//                and queries so they fit in a moderately large
//                terminal screen. With this parameter, the
//                full text of errors and queries is printed.
//
// Suggested use:
//
// - For validation testing: just -d or -bigtest.
// - For compatibility testing: add -allow-prepare-fail -flex-types.
// - For troubleshooting / analysis: add -v -show-sql -error-summary.

var (
	resultsRE = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	errorRE   = regexp.MustCompile(`^(?:statement|query)\s+error\s+(?:pgcode\s+([[:alnum:]]+)\s+)?(.*)$`)

	// Input selection
	logictestdata = flag.String("d", "testdata/[^.]*", "test data glob")
	bigtest       = flag.Bool("bigtest", false, "use the big set of logic test files (overrides testdata)")

	// Testing mode
	maxErrs = flag.Int("max-errors", 1,
		"stop processing input files after this number of errors (set to 0 for no limit)")
	allowPrepareFail = flag.Bool("allow-prepare-fail", false, "tolerate unexpected errors when preparing a query")
	flexTypes        = flag.Bool("flex-types", false,
		"do not fail when a test expects a column of a numeric type but the query provides another type")

	// Output parameters
	showSQL = flag.Bool("show-sql", false,
		"print the individual SQL statement/queries before processing")
	showLogs = flag.Bool("show-logs", false,
		"print logs instead of saving them in files")
	printErrorSummary = flag.Bool("error-summary", false,
		"print a per-error summary of failing queries at the end of testing, when -allow-prepare-fail is set")
	fullMessages = flag.Bool("full-messages", false,
		"do not shorten the error or SQL strings when printing the summary for -allow-prepare-fail or -flex-types.")
)

// lineScanner handles reading from input test files.
type lineScanner struct {
	*bufio.Scanner
	line int
	skip bool
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

// logicStatement represents a single statement test in Test-Script.
type logicStatement struct {
	// file and line number of the test.
	pos string
	// SQL string to be sent to the database.
	sql string
	// expected error, if any. "" indicates the statement should
	// succeed.
	expectErr string
	// expected pgcode for the error, if any. "" indicates the
	// test does not check the pgwire error code.
	expectErrCode string
}

// logicSorter sorts result rows (or not) depending on Test-Script's
// sorting option for a "query" test. See the implementation of the
// "query" directive below for details.
type logicSorter func(numCols int, values []string)

type rowSorter struct {
	numCols int
	numRows int
	values  []string
}

func (r rowSorter) row(i int) []string {
	return r.values[i*r.numCols : (i+1)*r.numCols]
}

func (r rowSorter) Len() int {
	return r.numRows
}

func (r rowSorter) Less(i, j int) bool {
	a := r.row(i)
	b := r.row(j)
	for k := range a {
		if a[k] < b[k] {
			return true
		}
		if a[k] > b[k] {
			return false
		}
	}
	return false
}

func (r rowSorter) Swap(i, j int) {
	a := r.row(i)
	b := r.row(j)
	for i := range a {
		a[i], b[i] = b[i], a[i]
	}
}

func rowSort(numCols int, values []string) {
	sort.Sort(rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	})
}

func valueSort(numCols int, values []string) {
	sort.Strings(values)
}

// partialSort rearranges consecutive rows that have the same values on a
// certain set of columns (orderedCols).
//
// More specifically: rows are partitioned into groups of consecutive rows that
// have the same values for columns orderedCols. Inside each group, the rows are
// sorted. The relative order of any two rows that differ on orderedCols is
// preserved.
//
// This is useful when comparing results for a statement that guarantees a
// partial, but not a total order. Consider:
//
//   SELECT a, b FROM ab ORDER BY a
//
// Some possible outputs for the same data:
//   1 2        1 5        1 2
//   1 5        1 4        1 4
//   1 4   or   1 2   or   1 5
//   2 3        2 2        2 3
//   2 2        2 3        2 2
//
// After a partialSort with orderedCols = {0} all become:
//   1 2
//   1 4
//   1 5
//   2 2
//   2 3
//
// An incorrect output like:
//   1 5                          1 2
//   1 2                          1 5
//   2 3          becomes:        2 2
//   2 2                          2 3
//   1 4                          1 4
// and it is detected as different.
func partialSort(numCols int, orderedCols []int, values []string) {
	// We use rowSorter here only as a container.
	c := rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	}

	// Sort the group of rows [rowStart, rowEnd).
	sortGroup := func(rowStart, rowEnd int) {
		sort.Sort(rowSorter{
			numCols: numCols,
			numRows: rowEnd - rowStart,
			values:  values[rowStart*numCols : rowEnd*numCols],
		})
	}

	groupStart := 0
	for rIdx := 1; rIdx < c.numRows; rIdx++ {
		// See if this row belongs in the group with the previous row.
		row := c.row(rIdx)
		start := c.row(groupStart)
		differs := false
		for _, i := range orderedCols {
			if start[i] != row[i] {
				differs = true
				break
			}
		}
		if differs {
			// Sort the group and start a new group with just this row in it.
			sortGroup(groupStart, rIdx)
			groupStart = rIdx
		}
	}
	sortGroup(groupStart, c.numRows)
}

// logicQuery represents a single query test in Test-Script.
type logicQuery struct {
	// pos and sql are as in logicStatement.
	pos string
	sql string
	// colTypes indicates the expected result column types.
	colTypes string
	// colNames controls the inclusion of column names in the query result.
	colNames bool
	// some tests require the output to match modulo sorting.
	sorter logicSorter
	// expectedErr and expectedErrCode are as in logicStatement.
	expectErr     string
	expectErrCode string
	// expectedResults indicates the expected sequence of text words
	// when flattening a query's results.
	expectedResults []string
	// expectedResultsRaw is the same as expectedResults, but
	// retaining the original formatting (whitespace, indentation) as
	// the test input file. This is used for pretty-printing unexpected
	// results.
	expectedResultsRaw []string
	// expectedHash indicates the expected hash of all result rows
	// combined. "" indicates hash checking is disabled.
	expectedHash string
	// expectedValues indicates the number of rows expected when
	// expectedHash is set.
	expectedValues int

	// rawOpts are the query options, before parsing. Used to display in error
	// messages.
	rawOpts string
}

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
type logicTest struct {
	t *testing.T
	// the number of nodes in the cluster.
	numNodes int
	// the cluster instantiated for this input file.
	cluster serverutils.TestClusterInterface
	// the index of the node (within the cluster) against which we run the test
	// statements.
	nodeIdx         int
	useFakeResolver bool
	// map of built clients. Needs to be persisted so that we can
	// re-use them and close them all on exit.
	clients map[string]*gosql.DB
	// client currently in use. This can change during processing
	// of a test input file when encountering the "user" directive.
	// see setUser() for details.
	user            string
	db              *gosql.DB
	cleanupRootUser func()
	// progress holds the number of tests executed so far.
	progress int
	// failures holds the number of tests failed so far, when
	// -try-harder is set.
	failures int
	// unsupported holds the number of queries ignored due
	// to prepare errors, when -allow-prepare-fail is set.
	unsupported int
	// lastProgress is used for the progress indicator message.
	lastProgress time.Time
	// traceFile holds the current trace file between "traceon"
	// and "traceoff" directives.
	traceFile *os.File
	// verbose indicate whether -v was passed.
	verbose bool
	// perErrorSummary retains the per-error list of failing queries
	// when -error-summary is set.
	perErrorSummary map[string][]string
	// labelMap retains the expected result hashes that have
	// been marked using a result label in the input. See the
	// explanation for labels in processInputFiles().
	labelMap map[string]string

	// logScope binds the lifetime of the log files to this test.
	logScope log.TestLogScope
}

func (t *logicTest) close() {
	defer t.logScope.Close(t.t)

	t.traceStop()

	if t.cleanupRootUser != nil {
		t.cleanupRootUser()
		t.cleanupRootUser = nil
	}
	if t.cluster != nil {
		t.cluster.Stopper().Stop()
		t.cluster = nil
	}
	if t.clients != nil {
		for _, c := range t.clients {
			c.Close()
		}
		t.clients = nil
	}
	t.db = nil
}

// out emits a message both on stdout and the log files if
// verbose is set.
func (t *logicTest) outf(format string, args ...interface{}) {
	if t.verbose {
		fmt.Printf(format, args...)
		fmt.Println()
		log.Infof(context.Background(), format, args...)
	}
}

// setUser sets the DB client to the specified user.
// It returns a cleanup function to be run when the credentials
// are no longer needed.
func (t *logicTest) setUser(user string) func() {
	var outDBName string

	if t.db != nil {
		var inDBName string

		if err := t.db.QueryRow("SHOW DATABASE").Scan(&inDBName); err != nil {
			t.Fatal(err)
		}

		defer func() {
			if inDBName != outDBName {
				// Propagate the DATABASE setting to the newly-live connection.
				if _, err := t.db.Exec(fmt.Sprintf("SET DATABASE = '%s'", inDBName)); err != nil {
					t.Fatal(err)
				}
			}
		}()
	}

	if t.clients == nil {
		t.clients = map[string]*gosql.DB{}
	}
	if db, ok := t.clients[user]; ok {
		t.db = db
		t.user = user

		if err := t.db.QueryRow("SHOW DATABASE").Scan(&outDBName); err != nil {
			t.Fatal(err)
		}

		// No cleanup necessary, but return a no-op func to avoid nil pointer dereference.
		return func() {}
	}

	addr := t.cluster.Server(t.nodeIdx).ServingAddr()
	pgURL, cleanupFunc := sqlutils.PGUrl(t.t, addr, "TestLogic", url.User(user))
	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.clients[user] = db
	t.db = db
	t.user = user

	t.outf("--- new user: %s", user)

	return cleanupFunc
}

func (t *logicTest) setup() {
	if !*showLogs {
		t.logScope = log.Scope(t.t, "TestLogic")
	}

	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	// TODO(andrei): if createTestServerParams() is used here, the command filter
	// it installs detects a transaction that doesn't have
	// modifiedSystemConfigSpan set even though it should, for
	// "testdata/rename_table". Figure out what's up with that.
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WaitForGossipUpdate:   true,
					CheckStmtStringChange: true,
				},
			},
		},
		// For distributed SQL tests, we use the fake span resolver; it doesn't
		// matter where the data really is.
		ReplicationMode: base.ReplicationManual,
	}
	t.cluster = serverutils.StartTestCluster(t.t, t.numNodes, params)
	if t.useFakeResolver {
		fakeResolver := distsqlutils.FakeResolverForTestCluster(t.cluster)
		t.cluster.Server(t.nodeIdx).SetDistSQLSpanResolver(fakeResolver)
	}

	// db may change over the lifetime of this function, with intermediate
	// values cached in t.clients and finally closed in t.close().
	t.cleanupRootUser = t.setUser(security.RootUser)

	if _, err := t.db.Exec(`
CREATE DATABASE test;
SET DATABASE = test;
`); err != nil {
		t.Fatal(err)
	}

	if _, err := t.db.Exec(fmt.Sprintf("CREATE USER %s;", server.TestUser)); err != nil {
		t.Fatal(err)
	}

	t.labelMap = make(map[string]string)

	t.progress = 0
	t.failures = 0
	t.unsupported = 0
}

// TODO(tschottdorf): some logic tests currently take a long time to run.
// Probably a case of heartbeats timing out or many restarts in some tests.
// Need to investigate when all moving parts are in place.
func (t *logicTest) processTestFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	defer t.traceStop()

	if t.verbose {
		t.outf("--- queries start here")
		defer t.printCompletion(path)
	}

	t.lastProgress = timeutil.Now()

	repeat := 1
	s := newLineScanner(file)
	for s.Scan() {
		if *maxErrs > 0 && t.failures >= *maxErrs {
			t.Fatalf("%s:%d: too many errors encountered, skipping the rest of the input",
				path, s.line)
		}

		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		if len(fields) == 2 && fields[1] == "error" {
			return fmt.Errorf("%s:%d: no expected error provided", path, s.line)
		}
		switch cmd {
		case "repeat":
			// A line "repeat X" makes the test repeat the following statement or query X times.
			var err error
			count := 0
			if len(fields) != 2 {
				err = errors.New("invalid line format")
			} else if count, err = strconv.Atoi(fields[1]); err == nil && count < 2 {
				err = errors.New("invalid count")
			}
			if err != nil {
				return fmt.Errorf("%s:%d invalid repeat line: %s", path, s.line, err)
			}
			repeat = count

		case "sleep":
			var err error
			var duration time.Duration
			// A line "sleep Xs" makes the test sleep for X seconds.
			if len(fields) != 2 {
				err = errors.New("invalid line format")
			} else if duration, err = time.ParseDuration(fields[1]); err != nil {
				err = errors.New("invalid duration")
			}
			if err != nil {
				return fmt.Errorf("%s:%d invalid sleep line: %s", path, s.line, err)
			}
			time.Sleep(duration)

		case "statement":
			stmt := logicStatement{pos: fmt.Sprintf("\n%s:%d", path, s.line)}
			// Parse "statement error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectErrCode = m[1]
				stmt.expectErr = m[2]
			}
			var buf bytes.Buffer
			for s.Scan() {
				line := s.Text()
				if line == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			stmt.sql = strings.TrimSpace(buf.String())
			if !s.skip {
				for i := 0; i < repeat; i++ {
					if ok := t.execStatement(stmt); !ok {
						return fmt.Errorf("%s: error in statement, skipping to next file", stmt.pos)
					}
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "query":
			query := logicQuery{pos: fmt.Sprintf("\n%s:%d", path, s.line)}
			label := ""
			// Parse "query error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				query.expectErrCode = m[1]
				query.expectErr = m[2]
			} else if len(fields) < 2 {
				return fmt.Errorf("%s: invalid test statement: %s", query.pos, s.Text())
			} else {
				// Parse "query <type-string> <sort-mode> <label>"
				// The type string specifies the number of columns and their types:
				//   - T for text
				//   - I for integer
				//   - R for floating point or decimal
				//   - B for boolean
				// The sort mode is one of:
				//   - "nosort" (default)
				//   - "rowsort": sorts both the returned and the expected rows assuming
				//         one white-space separated word per column.
				//   - "valuesort": sorts all values on all rows as one big set of
				//         strings (for both the returned and the expected rows).
				//   - "partialsort(x,y,..)": performs a partial sort on both the
				//         returned and the expected results, preserving the relative
				//         order of rows that differ on the specified columns
				//         (1-indexed); for results that are expected to be already
				//         ordered according to these columns. See partialSort() for
				//         more information.
				//
				//   - "colnames": column names are verified (the expected column names
				//                 are the first line in the expected results).
				//
				// The label is optional. If specified, the test runner stores a hash
				// of the results of the query under the given label. If the label is
				// reused, the test runner verifies that the results are the
				// same. This can be used to verify that two or more queries in the
				// same test script that are logically equivalent always generate the
				// same output.
				query.colTypes = fields[1]
				if len(fields) >= 3 {
					query.rawOpts = fields[2]
					for _, opt := range strings.Split(query.rawOpts, ",") {

						if strings.HasPrefix(opt, "partialsort(") && strings.HasSuffix(opt, ")") {
							s := opt
							s = strings.TrimPrefix(s, "partialsort(")
							s = strings.TrimSuffix(s, ")")

							var orderedCols []int
							for _, c := range strings.Split(s, ",") {
								colIdx, err := strconv.Atoi(c)
								if err != nil || colIdx < 1 {
									return fmt.Errorf("%s: invalid sort mode: %s", query.pos, opt)
								}
								orderedCols = append(orderedCols, colIdx-1)
							}
							if len(orderedCols) == 0 {
								return fmt.Errorf("%s: invalid sort mode: %s", query.pos, opt)
							}

							query.sorter = func(numCols int, values []string) {
								partialSort(numCols, orderedCols, values)
							}
							continue
						}

						switch opt {
						case "nosort":
							query.sorter = nil

						case "rowsort":
							query.sorter = rowSort

						case "valuesort":
							query.sorter = valueSort

						case "colnames":
							query.colNames = true

						default:
							return fmt.Errorf("%s: unknown sort mode: %s", query.pos, opt)
						}
					}
				}
				if len(fields) >= 4 {
					label = fields[3]
				}
			}

			var buf bytes.Buffer
			for s.Scan() {
				line := s.Text()
				if line == "----" {
					if query.expectErr != "" {
						return fmt.Errorf("%s: invalid ---- delimiter after a query expecting an error: %s", query.pos, query.expectErr)
					}
					break
				}
				if strings.TrimSpace(s.Text()) == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			query.sql = strings.TrimSpace(buf.String())

			if query.expectErr == "" {
				// Query results are either a space separated list of values up to a
				// blank line or a line of the form "xx values hashing to yyy". The
				// latter format is used by sqllogictest when a large number of results
				// match the query.
				if s.Scan() {
					if m := resultsRE.FindStringSubmatch(s.Text()); m != nil {
						var err error
						query.expectedValues, err = strconv.Atoi(m[1])
						if err != nil {
							return err
						}
						query.expectedHash = m[2]
					} else {
						for {
							query.expectedResultsRaw = append(query.expectedResultsRaw, s.Text())
							results := strings.Fields(s.Text())
							if len(results) == 0 {
								break
							}
							query.expectedResults = append(query.expectedResults, results...)
							if !s.Scan() {
								break
							}
						}
						query.expectedValues = len(query.expectedResults)
					}

					if label != "" {
						expectedHash := query.expectedHash
						if expectedHash == "" {
							hash, err := t.hashResults(query.expectedResults)
							if err != nil {
								t.Error(err)
								continue
							}
							expectedHash = hash
						}
						if prevHash, ok := t.labelMap[label]; ok {
							if prevHash != expectedHash {
								t.Errorf("%s: error in input: previous reference values for label %s (hash %s) do not match new definition (hash %s)", query.pos, label, prevHash, expectedHash)
								continue
							}
						} else {
							t.labelMap[label] = expectedHash
						}
					}
				}
			}

			if !s.skip {
				for i := 0; i < repeat; i++ {
					if err := t.execQuery(query); err != nil {
						t.Error(err)
					}
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "halt", "hash-threshold":

		case "user":
			if len(fields) < 2 {
				return fmt.Errorf("user command requires one argument, found: %v", fields)
			}
			if len(fields[1]) == 0 {
				return errors.New("user command requires a non-blank argument")
			}
			cleanupUserFunc := t.setUser(fields[1])
			defer cleanupUserFunc()

		case "skipif":
			if len(fields) < 2 {
				return fmt.Errorf("skipif command requires one argument, found: %v", fields)
			}
			switch fields[1] {
			case "":
				return errors.New("skipif command requires a non-blank argument")
			case "mysql":
			case "postgresql", "cockroachdb":
				s.skip = true
				continue
			default:
				return fmt.Errorf("unimplemented test statement: %s", s.Text())
			}

		case "onlyif":
			if len(fields) < 2 {
				return fmt.Errorf("onlyif command requires one argument, found: %v", fields)
			}
			switch fields[1] {
			case "":
				return errors.New("onlyif command requires a non-blank argument")
			case "cockroachdb":
			case "mysql":
				s.skip = true
				continue
			default:
				return fmt.Errorf("unimplemented test statement: %s", s.Text())
			}

		case "traceon":
			if len(fields) != 2 {
				return fmt.Errorf("traceon requires a filename argument, found: %v", fields)
			}
			t.traceStart(fields[1])

		case "traceoff":
			if t.traceFile == nil {
				return errors.New("no trace active")
			}
			t.traceStop()

		case "kv-batch-size":
			// kv-batch-size limits the kvfetcher batch size. It can be used to
			// trigger certain error conditions around limited batches.
			if len(fields) != 2 {
				return fmt.Errorf("kv-batch-size needs an integer argument, found: %v", fields[1:])
			}
			batchSize, err := strconv.Atoi(fields[1])
			if err != nil {
				return fmt.Errorf("kv-batch-size needs an integer argument; %s", err)
			}
			t.outf("Setting kv batch size %d", batchSize)
			defer sqlbase.SetKVBatchSize(int64(batchSize))()

		default:
			return fmt.Errorf("%s:%d: unknown command: %s", path, s.line, cmd)
		}
	}

	return s.Err()
}

// verifyError checks that either no error was found where none was
// expected, or that an error was found when one was expected. Returns
// "true" to indicate the behavior was as expected.
func (t *logicTest) verifyError(sql, pos, expectErr, expectErrCode string, err error) bool {
	if expectErr == "" && expectErrCode == "" && err != nil {
		return t.unexpectedError(sql, pos, err)
	}
	if !testutils.IsError(err, expectErr) {
		t.Errorf("%s: expected %q, but found %v", pos, expectErr, err)
		if err != nil && strings.Contains(err.Error(), expectErr) {
			t.t.Logf("The output string contained the input regexp. Perhaps you meant to write:\n"+
				"query error %s", regexp.QuoteMeta(err.Error()))
		}
		return false
	}
	if expectErrCode != "" {
		if err != nil {
			pqErr, ok := err.(*pq.Error)
			if !ok {
				t.Errorf("%s: expected error code %q, but the error we found is not "+
					"a libpq error: %s", pos, expectErrCode, err)
				return false
			}
			if pqErr.Code != pq.ErrorCode(expectErrCode) {
				t.Errorf("%s: expected error code %q, but found code %q (%s)",
					pos, expectErrCode, pqErr.Code, pqErr.Code.Name())
				return false
			}
		} else {
			t.Errorf("%s: expected error code %q, but found success",
				pos, expectErrCode)
			return false
		}
	}
	return true
}

// unexpectedError handles ignoring queries that fail during prepare
// when -allow-prepare-fail is specified. The argument "sql" is "" to indicate the
// work is done on behalf of a statement, which always fail upon an
// unexpected error.
func (t *logicTest) unexpectedError(sql string, pos string, err error) bool {
	if *allowPrepareFail && sql != "" {
		// This is a query and -allow-prepare-fail is set.  Try to prepare
		// the query. If prepare fails, this means we (probably) do not
		// support the input syntax, and -allow-prepare-fail instructs us
		// to ignore the unexpected error.
		stmt, err := t.db.Prepare(sql)
		if err != nil {
			if *showSQL {
				t.outf("\t-- fails prepare: %s", err)
			}
			t.signalIgnoredError(err, pos, sql)
			return true
		}
		if err := stmt.Close(); err != nil {
			t.Errorf("%s: error when closing prepared statement: %s", pos, err)
		}
	}
	t.Errorf("%s: expected success, but found\n%s", pos, err)
	return false
}

func (t *logicTest) execStatement(stmt logicStatement) bool {
	if *showSQL {
		t.outf("%s;", stmt.sql)
	}
	_, err := t.db.Exec(stmt.sql)

	// General policy for failing vs. continuing:
	// - we want to do as much work as possible;
	// - however, a statement that fails when it should succeed or
	//   a statement that succeeds when it should fail may have left
	//   the database in an improper state, so we stop there;
	// - error on expected error is worth going further, even
	//   if the obtained error does not match the expected error.
	ok := t.verifyError("", stmt.pos, stmt.expectErr, stmt.expectErrCode, err)
	if ok {
		t.finishOne("OK")
	}
	return ok
}

func (t *logicTest) hashResults(results []string) (string, error) {
	// Hash the values using MD5. This hashing precisely matches the hashing in
	// sqllogictest.c.
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (t *logicTest) execQuery(query logicQuery) error {
	if *showSQL {
		t.outf("%s;", query.sql)
	}
	rows, err := t.db.Query(query.sql)
	if ok := t.verifyError(query.sql, query.pos, query.expectErr, query.expectErrCode, err); !ok {
		return nil
	}
	if err != nil {
		// An error occurred, but it was expected.
		t.finishOne("XFAIL")
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(query.colTypes) != len(cols) {
		return fmt.Errorf("%s: expected %d columns based on type-string, but found %d",
			query.pos, len(query.colTypes), len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}

	var results []string
	var resultLines [][]string
	if query.colNames {
		for _, col := range cols {
			// We split column names on whitespace and append a separate "result"
			// for each string. A bit unusual, but otherwise we can't match strings
			// containing whitespace.
			results = append(results, strings.Fields(col)...)
		}
		resultLines = append(resultLines, cols)
	}
	for rows.Next() {
		var resultLine []string
		if err := rows.Scan(vals...); err != nil {
			return err
		}
		for i, v := range vals {
			if val := *v.(*interface{}); val != nil {
				valT := reflect.TypeOf(val).Kind()
				colT := query.colTypes[i]
				switch colT {
				case 'T':
					if valT != reflect.String && valT != reflect.Slice && valT != reflect.Struct {
						return fmt.Errorf("%s: expected text value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'I':
					if valT != reflect.Int64 {
						if *flexTypes && (valT == reflect.Float64 || valT == reflect.Slice) {
							t.signalIgnoredError(fmt.Errorf("result type mismatch: expected I, got %T", val), query.pos, query.sql)
							return nil
						}
						return fmt.Errorf("%s: expected int value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'R':
					if valT != reflect.Float64 && valT != reflect.Slice {
						if *flexTypes && (valT == reflect.Int64) {
							t.signalIgnoredError(fmt.Errorf("result type mismatch: expected R, got %T", val), query.pos, query.sql)
							return nil
						}
						return fmt.Errorf("%s: expected float/decimal value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'B':
					if valT != reflect.Bool {
						return fmt.Errorf("%s: expected boolean value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'O':
					if valT != reflect.Slice {
						return fmt.Errorf("%s: expected oid value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				default:
					return fmt.Errorf("%s: unknown type in type string: %c in %s", query.pos, colT, query.colTypes)
				}

				if byteArray, ok := val.([]byte); ok {
					// The postgres wire protocol does not distinguish between
					// strings and byte arrays, but our tests do. In order to do
					// The Right Thing™, we replace byte arrays which are valid
					// UTF-8 with strings. This allows byte arrays which are not
					// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
					// while printing valid strings naturally.
					if str := string(byteArray); utf8.ValidString(str) {
						val = str
					}
				}
				// We split string results on whitespace and append a separate result
				// for each string. A bit unusual, but otherwise we can't match strings
				// containing whitespace.
				valStr := fmt.Sprint(val)
				results = append(results, strings.Fields(valStr)...)
				resultLine = append(resultLine, valStr)
			} else {
				results = append(results, "NULL")
				resultLine = append(resultLine, "NULL")
			}
		}
		resultLines = append(resultLines, resultLine)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if query.sorter != nil {
		query.sorter(len(cols), results)
		query.sorter(len(cols), query.expectedResults)
	}

	if query.expectedHash != "" {
		n := len(results)
		if query.expectedValues != n {
			return fmt.Errorf("%s: expected %d results, but found %d", query.pos, query.expectedValues, n)
		}
		hash, err := t.hashResults(results)
		if err != nil {
			return err
		}
		if query.expectedHash != hash {
			return fmt.Errorf("%s: expected %s, but found %s", query.pos, query.expectedHash, hash)
		}
	} else if !reflect.DeepEqual(query.expectedResults, results) {
		var buf bytes.Buffer
		tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

		fmt.Fprintf(&buf, "%s: \nexpected:\n", query.pos)
		for _, expectedResultRaw := range query.expectedResultsRaw {
			fmt.Fprintf(tw, "    %s\n", expectedResultRaw)
		}
		sortMsg := ""
		if query.sorter != nil {
			// We performed an order-insensitive comparison of "actual" vs "expected"
			// rows by sorting both, but we'll display the error with the expected
			// rows in the order in which they were put in the file, and the actual
			// rows in the order in which the query returned them.
			sortMsg = " -> ignore the following ordering of rows"
		}
		fmt.Fprintf(&buf, "but found (query options: %q%s) :\n", query.rawOpts, sortMsg)
		for _, resultLine := range resultLines {
			fmt.Fprint(tw, "    ")
			for _, value := range resultLine {
				fmt.Fprintf(tw, "%s\t", value)
			}
			fmt.Fprint(tw, "\n")
		}
		_ = tw.Flush()
		return errors.New(buf.String())
	}

	t.finishOne("OK")
	return nil
}

func (t *logicTest) success(file string) {
	t.progress++
	now := timeutil.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		t.outf("--- progress: %s: %d statements/queries", file, t.progress)
	}
}

func makeLogicTest(t *testing.T, numNodes int, useFakeSpanResolver bool) *logicTest {
	return &logicTest{
		t:               t,
		numNodes:        numNodes,
		useFakeResolver: true,
		verbose:         testing.Verbose() || log.V(1),
		perErrorSummary: make(map[string][]string),
	}
}

// run runs the logic tests indicated by the bigtest and logictestdata flags.
// A new cluster is set up for each separate file in the test.
// This function must be called from within the testing.T associated with the
// logicTest.
func (l *logicTest) run() {
	t := l.t
	var globs []string
	if *bigtest {
		const logicTestPath = "../../sqllogictest"
		if _, err := os.Stat(logicTestPath); os.IsNotExist(err) {
			fullPath, err := filepath.Abs(logicTestPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("unable to find sqllogictest repo: %s\n"+
				"git clone https://github.com/cockroachdb/sqllogictest %s",
				logicTestPath, fullPath)
			return
		}
		globs = []string{
			logicTestPath + "/test/index/between/*/*.test",
			logicTestPath + "/test/index/commute/*/*.test",
			logicTestPath + "/test/index/delete/*/*.test",
			logicTestPath + "/test/index/in/*/*.test",
			logicTestPath + "/test/index/orderby/*/*.test",
			logicTestPath + "/test/index/orderby_nosort/*/*.test",

			// TODO(pmattis): We don't support aggregate functions.
			// logicTestPath + "/test/random/expr/*.test",

			// TODO(pmattis): We don't support tables without primary keys.
			// logicTestPath + "/test/select*.test",

			// TODO(pmattis): We don't support views.
			// logicTestPath + "/test/index/view/*/*.test",

			// TODO(pmattis): We don't support joins.
			// [uses joins] logicTestPath + "/test/index/random/*/*.test",
			// [uses joins] logicTestPath + "/test/random/aggregates/*.test",
			// [uses joins] logicTestPath + "/test/random/groupby/*.test",
			// [uses joins] logicTestPath + "/test/random/select/*.test",
		}
	} else {
		globs = []string{*logictestdata}
	}

	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, match...)
	}

	if len(paths) == 0 {
		t.Fatalf("No testfiles found (globs: %v)", globs)
	}

	total := 0
	totalFail := 0
	totalUnsupported := 0
	lastProgress := timeutil.Now()
	if *printErrorSummary {
		defer l.printErrorSummary()
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			// the `t` given to this anonymous function may be different
			// from the t above, so re-bind it to `l` for the duration of
			// the test.
			l.t = t
			defer l.close()
			l.setup()

			defer func() {
				if r := recover(); r != nil {
					// Translate panics during the test to test errors.
					t.Fatalf("panic: %v\n%s", r, string(debug.Stack()))
				}
			}()

			if err := l.processTestFile(path); err != nil {
				t.Fatal(err)
			}
		})

		total += l.progress
		totalFail += l.failures
		totalUnsupported += l.unsupported
		now := timeutil.Now()
		if now.Sub(lastProgress) >= 2*time.Second {
			lastProgress = now
			l.outf("--- total progress: %d statements/queries", total)
		}
	}

	unsupportedMsg := ""
	if totalUnsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", totalUnsupported)
	}

	l.outf("--- total: %d tests, %d failures%s", total, totalFail, unsupportedMsg)
}

func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testutils.Stress() {
		t.Skip()
	}

	l := makeLogicTest(t, 1 /* numNodes */, false /* useFakeSpanResolver */)
	l.run()
}

// TestLogicDistSQL is a variant of TestLogic that uses DistSQL for all
// supported queries.
func TestLogicDistSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	if testutils.Stress() {
		t.Skip()
	}

	defer sql.SetDefaultDistSQLMode("ON")()

	// TODO(radu): make this run on 3 nodes (#13377)
	l := makeLogicTest(t, 1 /* numNodes */, true /* useFakeSpanResolver */)
	l.run()
}

type errorSummaryEntry struct {
	errmsg string
	sql    []string
}
type errorSummary []errorSummaryEntry

func (e errorSummary) Len() int { return len(e) }
func (e errorSummary) Less(i, j int) bool {
	if len(e[i].sql) == len(e[j].sql) {
		return e[i].errmsg < e[j].errmsg
	}
	return len(e[i].sql) < len(e[j].sql)
}
func (e errorSummary) Swap(i, j int) {
	t := e[i]
	e[i] = e[j]
	e[j] = t
}

// printErrorSummary shows the final per-error list of failing queries when
// -allow-prepare-fail or -flex-types are specified.
func (t *logicTest) printErrorSummary() {
	if t.unsupported == 0 {
		return
	}

	t.outf("--- summary of ignored errors:")
	summary := make(errorSummary, len(t.perErrorSummary))
	i := 0
	for errmsg, sql := range t.perErrorSummary {
		summary[i] = errorSummaryEntry{errmsg: errmsg, sql: sql}
	}
	sort.Sort(summary)
	for _, s := range summary {
		t.outf("%s (%d entries)", s.errmsg, len(s.sql))
		var buf bytes.Buffer
		for _, q := range s.sql {
			buf.WriteByte('\t')
			buf.WriteString(strings.Replace(q, "\n", "\n\t", -1))
		}
		t.outf("%s", buf.String())
	}
}

// shortenString cuts its argument on the right so that it more likely
// fits onto the developer's screen.  The behavior can be disabled by
// the command-line flag "-full-messages".
func shortenString(msg string) string {
	if *fullMessages {
		return msg
	}

	shortened := false

	nlPos := strings.IndexRune(msg, '\n')
	if nlPos >= 0 {
		shortened = true
		msg = msg[:nlPos]
	}

	if len(msg) > 80 {
		shortened = true
		msg = msg[:80]
	}

	if shortened {
		msg = msg + "..."
	}

	return msg
}

// simplifyError condenses long error strings to the shortest form
// that still explains the origin of the error.
func simplifyError(msg string) (string, string) {
	prefix := strings.Split(msg, ": ")

	// Split:  "a: b: c"-> "a: b", "c"
	expected := ""
	if len(prefix) > 1 {
		expected = prefix[len(prefix)-1]
		prefix = prefix[:len(prefix)-1]
	}

	// Simplify: "a: b: c: d" -> "a: d"
	if !*fullMessages && len(prefix) > 2 {
		prefix[1] = prefix[len(prefix)-1]
		prefix = prefix[:2]
	}

	// Mark the error message as shortened if necessary.
	if expected != "" {
		prefix = append(prefix, "...")
	}

	return strings.Join(prefix, ": "), expected
}

// signalIgnoredError registers a failing but ignored query.
func (t *logicTest) signalIgnoredError(err error, pos string, sql string) {
	t.unsupported++

	if !*printErrorSummary {
		return
	}

	// Save the error for later reporting.
	errmsg, expected := simplifyError(fmt.Sprintf("%s", err))
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "-- %s (%s)\n%s", pos, shortenString(expected), shortenString(sql+";"))
	errmsg = shortenString(errmsg)
	t.perErrorSummary[errmsg] = append(t.perErrorSummary[errmsg], buf.String())
}

// Error is a wrapper around testing.T.Error that handles printing the per-query
// "FAIL" marker when -show-sql is set. It also registers the error to the
// failure counter.
func (t *logicTest) Error(args ...interface{}) {
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	t.t.Error(args...)
	t.failures++
}

// Errorf is a wrapper around testing.T.Errorf that handles printing the
// per-query "FAIL" marker when -show-sql is set. It also registers the error to
// the failure counter.
func (t *logicTest) Errorf(format string, args ...interface{}) {
	if *showSQL {
		t.outf("\t-- FAIL")
	}
	t.t.Errorf(format, args...)
	t.failures++
}

// Fatal is a wrapper around testing.T.Fatal that ensures the fatal error message
// is printed on its own line when -show-sql is set.
func (t *logicTest) Fatal(args ...interface{}) {
	if *showSQL {
		fmt.Println()
	}
	t.t.Fatal(args...)
}

// Fatalf is a wrapper around testing.T.Fatalf that ensures the fatal error
// message is printed on its own line when -show-sql is set.
func (t *logicTest) Fatalf(format string, args ...interface{}) {
	if *showSQL {
		fmt.Println()
	}
	t.t.Fatalf(format, args...)
}

// finishOne marks completion of a single test. It handles
// printing the success marker then -show-sql is set.
func (t *logicTest) finishOne(msg string) {
	if *showSQL {
		t.outf("\t-- %s;", msg)
	}
}

// printCompletion reports on the completion of all tests in a given
// input file.
func (t *logicTest) printCompletion(path string) {
	unsupportedMsg := ""
	if t.unsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", t.unsupported)
	}
	t.outf("--- done: %s: %d tests, %d failures%s", path, t.progress, t.failures,
		unsupportedMsg)
}
