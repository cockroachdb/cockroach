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

type logicQuery struct {
	pos             string
	sql             string
	colNames        bool
	colTypes        string // TODO(pmattis): not (yet) implemented.
	label           string // TODO(pmattis): not (yet) implemented.
	expectErr       string
	expectedValues  int
	expectedHash    string
	expectedResults []string
}

// TODO(pmattis): #1961 is adding a similar type to cli/sql.go. Perhaps move
// this type into the sql or sql/driver packages and export it so that it can
// be shared.
type logicValue string

func (v *logicValue) Scan(value interface{}) error {
	switch t := value.(type) {
	case nil:
		*v = "NULL"
	case bool:
		*v = logicValue(strconv.FormatBool(t))
	case int64:
		*v = logicValue(strconv.FormatInt(t, 10))
	case float64:
		*v = logicValue(strconv.FormatFloat(t, 'g', -1, 64))
	case []byte:
		*v = logicValue(t)
	case string:
		*v = logicValue(t)
	case time.Time:
		*v = logicValue(t.String())
	default:
		return fmt.Errorf("unexpected type: %T", value)
	}
	return nil
}

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
//
// TODO(pmattis): We currently cannot run the tests from sqllogictest due to
// insufficient SQL coverage (e.g. lack of subqueries and aggregation
// functions). We should work towards fixing that.
type logicTest struct {
	*testing.T
}

func (t logicTest) run(path string) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	base := filepath.Base(path)

	// TODO(pmattis): Add a flag to make it easy to run the tests against a local
	// MySQL or Postgres instance.
	srv := server.StartTestServer(nil)

	// TODO(marc): Allow the user to be specified somehow so that we can
	// test permissions.
	db, err := sql.Open("cockroach", "https://root@"+srv.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer srv.Stop()

	if _, err := db.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET DATABASE = test"); err != nil {
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
			stmt := logicStatement{pos: fmt.Sprintf("%s:%d", base, s.line)}
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
			t.execStatement(db, stmt)

		case "query":
			query := logicQuery{pos: fmt.Sprintf("%s:%d", base, s.line)}
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
						// TODO(pmattis): The sort options are not yet implemented.
						case "nosort":
						case "rowsort":
						case "valuesort":

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

			t.execQuery(db, query)

		case "halt":
			break

		case "skipif", "onlyif":
			t.Fatalf("unimplemented test statement: %s", s.Text())
		}
	}

	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
}

func (t logicTest) execStatement(db *sql.DB, stmt logicStatement) {
	fmt.Printf("%s: %s\n", stmt.pos, stmt.sql)
	_, err := db.Exec(stmt.sql)
	switch {
	case stmt.expectErr == "":
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", stmt.pos, err)
		}
	case !testutils.IsError(err, stmt.expectErr):
		t.Fatalf("%s: expected %q, but found %q", stmt.pos, stmt.expectErr, err)
	}
}

func (t logicTest) execQuery(db *sql.DB, query logicQuery) {
	fmt.Printf("%s: %s\n", query.pos, query.sql)
	rows, err := db.Query(query.sql)
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
		vals[i] = new(logicValue)
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
			results = append(results, string(*v.(*logicValue)))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
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

func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)

	l := logicTest{T: t}
	paths, err := filepath.Glob(*testdata)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		l.run(p)
	}
}
