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
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/pq"
)

var (
	resultsRE     = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	errorRE       = regexp.MustCompile(`^(?:statement|query)\s+error\s+(?:pgcode\s+([[:alnum:]]+)\s+)?(.*)$`)
	logictestdata = flag.String("d", "testdata/[^.]*", "test data glob")
	bigtest       = flag.Bool("bigtest", false, "use the big set of logic test files (overrides testdata)")
)

const logicMaxOffset = 50 * time.Millisecond

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

type logicStatement struct {
	pos           string
	sql           string
	expectErr     string
	expectErrCode string
}

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

type logicQuery struct {
	pos             string
	sql             string
	colNames        bool
	colTypes        string
	label           string
	sorter          logicSorter
	expectErrCode   string
	expectErr       string
	expectedValues  int
	expectedHash    string
	expectedResults []string
}

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
type logicTest struct {
	*testing.T
	srv *testServer
	// map of built clients. Needs to be persisted so that we can
	// re-use them and close them all on exit.
	clients map[string]*sql.DB
	// client currently in use.
	user            string
	db              *sql.DB
	progress        int
	lastProgress    time.Time
	traceFile       *os.File
	cleanupRootUser func()
}

func (t *logicTest) close() {
	if t.cleanupRootUser != nil {
		t.cleanupRootUser()
	}
	if t.srv != nil {
		cleanupTestServer(t.srv)
		t.srv = nil
	}
	if t.clients != nil {
		for _, c := range t.clients {
			c.Close()
		}
		t.clients = nil
	}
	t.db = nil
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
				if _, err := t.db.Exec(fmt.Sprintf("SET DATABASE = %s", inDBName)); err != nil {
					t.Fatal(err)
				}
			}
		}()
	}

	if t.clients == nil {
		t.clients = map[string]*sql.DB{}
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

	pgURL, cleanupFunc := sqlutils.PGUrl(t.T, &t.srv.TestServer, user, "TestLogic")
	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.clients[user] = db
	t.db = db
	t.user = user
	return cleanupFunc
}

func (t *logicTest) run(path string) {
	defer t.close()
	t.setup()
	if err := t.processTestFile(path); err != nil {
		t.Error(err)
	}
}

func (t *logicTest) setup() {
	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	ctx := server.NewTestContext()
	ctx.MaxOffset = logicMaxOffset
	ctx.TestingKnobs.ExecutorTestingKnobs.WaitForGossipUpdate = true
	ctx.TestingKnobs.ExecutorTestingKnobs.CheckStmtStringChange = true
	t.srv = setupTestServerWithContext(t.T, ctx)

	// db may change over the lifetime of this function, with intermediate
	// values cached in t.clients and finally closed in t.close().
	t.cleanupRootUser = t.setUser(security.RootUser)

	if _, err := t.db.Exec(`
CREATE DATABASE test;
SET DATABASE = test;
`); err != nil {
		t.Fatal(err)
	}
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

	t.lastProgress = timeutil.Now()

	testingKnobs := &t.srv.Ctx.TestingKnobs

	repeat := 1
	s := newLineScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
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

		case "statement":
			stmt := logicStatement{pos: fmt.Sprintf("%s:%d", path, s.line)}
			// Parse "query error <regexp>"
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
					t.execStatement(stmt)
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "query":
			query := logicQuery{pos: fmt.Sprintf("%s:%d", path, s.line)}
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
				//   - R for floating point
				//   - B for boolean
				// The sort mode is one of:
				//   - "nosort" (default)
				//   - "rowsort"
				//   - "valuesort"
				//   - "colnames"
				//
				// The label is optional. If specified, the test runner stores a hash
				// of the results of the query under the given label. If the label is
				// reused, the test runner verifies that the results are the
				// same. This can be used to verify that two or more queries in the
				// same test script that are logically equivalent always generate the
				// same output.
				query.colTypes = fields[1]
				if len(fields) >= 3 {
					for _, opt := range strings.Split(fields[2], ",") {
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
					query.label = fields[3]
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
				}
			}

			if !s.skip {
				for i := 0; i < repeat; i++ {
					t.execQuery(query)
				}
			} else {
				s.skip = false
			}
			repeat = 1
			t.success(path)

		case "halt", "hash-threshold":
			break

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
			case "postgresql":
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

		case "fix-txn-priorities":
			// fix-txn-priorities causes future transactions to have hardcoded
			// priority values (based on the priority level), (replacing the
			// probabilistic generation).
			// The change stays in effect for the duration of that particular
			// test file.
			if len(fields) != 1 {
				return fmt.Errorf("fix-txn-priority takes no arguments, found: %v", fields[1:])
			}
			fmt.Println("Setting deterministic priorities.")

			testingKnobs.ExecutorTestingKnobs.FixTxnPriority = true
			defer func() { testingKnobs.ExecutorTestingKnobs.FixTxnPriority = false }()
		default:
			return fmt.Errorf("%s:%d: unknown command: %s", path, s.line, cmd)
		}
	}

	if err := s.Err(); err != nil {
		return err
	}

	fmt.Printf("%s: %d\n", path, t.progress)
	return nil
}

func (t *logicTest) verifyError(
	pos string, expectErr string, expectErrCode string, err error) {
	if expectErr == "" && expectErrCode == "" && err != nil {
		t.Fatalf("%s: expected success, but found\n%s", pos, err)
	}
	if expectErr != "" && !testutils.IsError(err, expectErr) {
		if err != nil {
			t.Fatalf("%s: expected %q, but found\n%s", pos, expectErr, err)
		} else {
			t.Fatalf("%s: expected %q, but found success", pos, expectErr)
		}
	}
	if expectErrCode != "" {
		if err != nil {
			pqErr, ok := err.(*pq.Error)
			if !ok {
				t.Fatalf("%s: expected error code %q, but the error we found is not "+
					"a libpq error: %s", pos, expectErrCode, err)
			}
			if pqErr.Code != pq.ErrorCode(expectErrCode) {
				t.Fatalf("%s: expected error code %q, but found code %q (%s)",
					pos, expectErrCode, pqErr.Code, pqErr.Code.Name())
			}
		} else {
			t.Fatalf("%s: expected error code %q, but found success",
				pos, expectErrCode)
		}
	}
}

func (t *logicTest) execStatement(stmt logicStatement) {
	if testing.Verbose() || log.V(1) {
		fmt.Printf("%s %s: %s\n", stmt.pos, t.user, stmt.sql)
	}
	_, err := t.db.Exec(stmt.sql)
	t.verifyError(stmt.pos, stmt.expectErr, stmt.expectErrCode, err)
}

func (t *logicTest) execQuery(query logicQuery) {
	if testing.Verbose() || log.V(1) {
		fmt.Printf("%s %s: %s\n", query.pos, t.user, query.sql)
	}
	rows, err := t.db.Query(query.sql)
	t.verifyError(query.pos, query.expectErr, query.expectErrCode, err)
	if err != nil {
		// An error occurred, but it was expected.
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(query.colTypes) != len(cols) {
		t.Fatalf("%s: expected %d columns based on type-string, but found %d",
			query.pos, len(query.colTypes), len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}

	var results []string
	if query.colNames {
		for _, col := range cols {
			// We split column names on whitespace and append a separate "result"
			// for each string. A bit unusual, but otherwise we can't match strings
			// containing whitespace.
			results = append(results, strings.Fields(col)...)
		}
	}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		for i, v := range vals {
			if val := *v.(*interface{}); val != nil {
				valT := reflect.TypeOf(val).Kind()
				colT := query.colTypes[i]
				switch colT {
				case 'T':
					if valT != reflect.String && valT != reflect.Slice && valT != reflect.Struct {
						t.Fatalf("%s: expected text value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'I':
					if valT != reflect.Int64 {
						t.Fatalf("%s: expected int value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'R':
					if valT != reflect.Float64 && valT != reflect.Slice {
						t.Fatalf("%s: expected float value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				case 'B':
					if valT != reflect.Bool {
						t.Fatalf("%s: expected boolean value for column %d, but found %T: %#v", query.pos, i, val, val)
					}
				default:
					t.Fatalf("%s: unknown type in type string: %c in %s", query.pos, colT, query.colTypes)
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
				results = append(results, strings.Fields(fmt.Sprint(val))...)
			} else {
				results = append(results, "NULL")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if query.sorter != nil {
		query.sorter(len(cols), results)
	}

	if query.expectedHash != "" {
		n := len(results)
		if query.expectedValues != n {
			t.Fatalf("%s: expected %d results, but found %d", query.pos, query.expectedValues, n)
		}
		// Hash the values using MD5. This hashing precisely matches the hashing in
		// sqllogictest.c.
		h := md5.New()
		for _, r := range results {
			if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
				t.Fatal(err)
			}
		}
		hash := fmt.Sprintf("%x", h.Sum(nil))
		if query.expectedHash != hash {
			t.Fatalf("%s: expected %s, but found %s", query.pos, query.expectedHash, hash)
		}
	} else if !reflect.DeepEqual(query.expectedResults, results) {
		var buf bytes.Buffer
		tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

		fmt.Fprintf(tw, "%s: expected:\n", query.pos)
		for i := 0; i < len(query.expectedResults); i += len(cols) {
			end := i + len(cols)
			if end > len(query.expectedResults) {
				end = len(query.expectedResults)
			}
			for _, value := range query.expectedResults[i:end] {
				fmt.Fprintf(tw, "%q\t", value)
			}
			fmt.Fprint(tw, "\n")
		}
		fmt.Fprint(tw, "but found:\n")
		for i := 0; i < len(results); i += len(cols) {
			end := i + len(cols)
			if end > len(results) {
				end = len(results)
			}
			for _, value := range results[i:end] {
				fmt.Fprintf(tw, "%q\t", value)
			}
			fmt.Fprint(tw, "\n")
		}
		_ = tw.Flush()
		t.Fatal(buf.String())
	}
}

func (t *logicTest) success(file string) {
	t.progress++
	now := timeutil.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		fmt.Printf("%s: %d\n", file, t.progress)
	}
}

func (t *logicTest) traceStart(filename string) {
	if t.traceFile != nil {
		t.Fatalf("tracing already active")
	}
	var err error
	t.traceFile, err = os.Create(filename)
	if err != nil {
		t.Fatalf("unable to open trace output file: %s", err)
	}
	if err := trace.Start(t.traceFile); err != nil {
		t.Fatalf("unable to start tracing: %s", err)
	}
}

func (t *logicTest) traceStop() {
	if t.traceFile != nil {
		trace.Stop()
		t.traceFile.Close()
		t.traceFile = nil
	}
}

func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(marc): splitting ranges at table boundaries causes
	// a blocked task and won't drain. Investigate and fix.
	defer config.TestingDisableTableSplits()()

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
	for _, p := range paths {
		if testing.Verbose() || log.V(1) {
			fmt.Printf("Running logic test on file: %s\n", p)
		}
		l := logicTest{T: t}
		l.run(p)
		total += l.progress
	}
	fmt.Printf("%d tests passed\n", total)
}
