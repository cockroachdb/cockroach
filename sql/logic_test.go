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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var (
	resultsRE = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	errorRE   = regexp.MustCompile(`^(?:statement|query)\s+error\s+(.*)$`)
	testdata  = flag.String("d", "testdata/*", "test data glob")
	bigtest   = flag.Bool("bigtest", false, "use the big set of logic test files (overrides testdata)")
)

type lineScanner struct {
	*bufio.Scanner
	line int
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
	pos       string
	sql       string
	expectErr string
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
	srv *server.TestServer
	// map of built clients. Needs to be persisted so that we can
	// re-use them and close them all on exit.
	clients map[string]*sql.DB
	// client currently in use.
	db           *sql.DB
	progress     int
	lastProgress time.Time
}

func (t *logicTest) close() {
	if t.srv != nil {
		t.srv.Stop()
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

// setUser sets the DB client to the specified user, creating it if needed.
func (t *logicTest) setUser(user string) {
	if t.clients == nil {
		t.clients = map[string]*sql.DB{}
	}
	if c, ok := t.clients[user]; ok {
		t.db = c
		return
	}
	db, err := sql.Open("cockroach", "https://"+user+"@"+t.srv.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	t.clients[user] = db
	t.db = db
}

func (t logicTest) run(path string) {
	defer t.close()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	t.lastProgress = time.Now()

	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	t.srv = server.StartTestServer(t)

	// db may change over the lifetime of this function, with intermediate
	// values cached in t.clients and finally closed in t.close().
	t.setUser("root")

	if _, err := t.db.Exec(`
CREATE DATABASE test;
SET DATABASE = test;
`); err != nil {
		t.Fatal(err)
	}

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
		case "statement":
			stmt := logicStatement{pos: fmt.Sprintf("%s:%d", path, s.line)}
			// Parse "query error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				stmt.expectErr = m[1]
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
			t.execStatement(stmt)
			t.success(path)

		case "query":
			query := logicQuery{pos: fmt.Sprintf("%s:%d", path, s.line)}
			// Parse "query error <regexp>"
			if m := errorRE.FindStringSubmatch(s.Text()); m != nil {
				query.expectErr = m[1]
			} else if len(fields) < 2 {
				t.Fatalf("%s: invalid test statement: %s", query.pos, s.Text())
			} else {
				// TODO(pmattis): Parse "query <type-string> <sort-mode> <label>". The
				// type string specifies the number of columns and their types: T for
				// text, I for integer and R for floating point. The sort mode is one
				// of "nosort", "rowsort" or "valuesort". The default is "nosort".
				//
				// The label is optional. If specified, the test runner stores a hash
				// of the results of the query under the given label. If the label is
				// reused, the test runner verifieds that the results are the
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
					break
				}
				fmt.Fprintln(&buf, line)
			}
			query.sql = strings.TrimSpace(buf.String())

			// Query results are either a space separated list of values up to a
			// blank line or a line of the form "xx values hashing to yyy". The
			// latter format is used by sqllogictest when a large number of results
			// match the query.
			if s.Scan() {
				if m := resultsRE.FindStringSubmatch(s.Text()); m != nil {
					var err error
					query.expectedValues, err = strconv.Atoi(m[1])
					if err != nil {
						t.Fatal(err)
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

			t.execQuery(query)
			t.success(path)

		case "halt":
			break

		case "user":
			if len(fields) != 2 {
				t.Fatalf("user command requires one argument, found: %v", fields)
			}
			if len(fields[1]) == 0 {
				t.Fatal("user command requires a non-blank argument")
			}
			t.setUser(fields[1])

		case "skipif", "onlyif":
			t.Fatalf("unimplemented test statement: %s", s.Text())
		}
	}

	if err := s.Err(); err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%s: %d\n", path, t.progress)
}

func (t *logicTest) execStatement(stmt logicStatement) {
	if testing.Verbose() {
		fmt.Printf("%s: %s\n", stmt.pos, stmt.sql)
	}
	_, err := t.db.Exec(stmt.sql)
	switch {
	case stmt.expectErr == "":
		if err != nil {
			t.Fatalf("%s: expected success, but found %q", stmt.pos, err)
		}
	case !testutils.IsError(err, stmt.expectErr):
		if err != nil {
			t.Fatalf("%s: expected %q, but found %q", stmt.pos, stmt.expectErr, err)
		} else {
			t.Fatalf("%s: expected %q, but found success", stmt.pos, stmt.expectErr)
		}
	}
}

func (t *logicTest) execQuery(query logicQuery) {
	if testing.Verbose() {
		fmt.Printf("%s: %s\n", query.pos, query.sql)
	}
	rows, err := t.db.Query(query.sql)
	if query.expectErr == "" {
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", query.pos, err)
		}
	} else if !testutils.IsError(err, query.expectErr) {
		t.Fatalf("%s: expected %s, but found %v", query.pos, query.expectErr, err)
	} else {
		// An error occurred, but it was expected.
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(sql.NullString)
	}

	var results []string
	if query.colNames {
		results = append(results, cols...)
	}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		for _, v := range vals {
			nullStr := v.(*sql.NullString)
			if nullStr.Valid {
				results = append(results, nullStr.String)
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
			_, _ = io.WriteString(h, r)
			_, _ = io.WriteString(h, "\n")
		}
		hash := fmt.Sprintf("%x", h.Sum(nil))
		if query.expectedHash != hash {
			t.Fatalf("%s: expected %s, but found %s", query.pos, query.expectedHash, hash)
		}
	} else if !reflect.DeepEqual(query.expectedResults, results) {
		t.Fatalf("%s: expected %q, but found %q\n", query.pos, query.expectedResults, results)
	}
}

func (t *logicTest) success(file string) {
	t.progress++
	now := time.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		fmt.Printf("%s: %d\n", file, t.progress)
	}
}

func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)

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
			logicTestPath + "/test/index/in/*/*.test",
		}
	} else {
		globs = []string{*testdata}
	}

	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, match...)
	}

	for _, p := range paths {
		logicTest{T: t}.run(p)
	}
}
