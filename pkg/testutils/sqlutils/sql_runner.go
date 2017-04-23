// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
)

// SQLRunner wraps a testing.TB and *gosql.DB connection and provides
// convenience functions to run SQL statements and fail the test on any errors.
type SQLRunner struct {
	testing.TB
	DB *gosql.DB
}

// MakeSQLRunner returns a SQLRunner for the given database connection.
func MakeSQLRunner(tb testing.TB, db *gosql.DB) *SQLRunner {
	return &SQLRunner{TB: tb, DB: db}
}

// Exec is a wrapper around gosql.Exec that kills the test on error.
func (sr *SQLRunner) Exec(query string, args ...interface{}) gosql.Result {
	r, err := sr.DB.Exec(query, args...)
	if err != nil {
		file, line, _ := caller.Lookup(1)
		sr.Fatalf("%s:%d: error executing '%s': %s", file, line, query, err)
	}
	return r
}

// ExecRowsAffected executes the statement and verifies that RowsAffected()
// matches the expected value. It kills the test on errors.
func (sr *SQLRunner) ExecRowsAffected(expRowsAffected int, query string, args ...interface{}) {
	r := sr.Exec(query, args...)
	numRows, err := r.RowsAffected()
	if err != nil {
		sr.Fatal(err)
	}
	if numRows != int64(expRowsAffected) {
		sr.Fatalf("expected %d affected rows, got %d on '%s'", expRowsAffected, numRows, query)
	}
}

// Query is a wrapper around gosql.Query that kills the test on error.
func (sr *SQLRunner) Query(query string, args ...interface{}) *gosql.Rows {
	r, err := sr.DB.Query(query, args...)
	if err != nil {
		sr.Fatalf("error executing '%s': %s", query, err)
	}
	return r
}

// Row is a wrapper around gosql.Row that kills the test on error.
type Row struct {
	testing.TB
	row *gosql.Row
}

// Scan is a wrapper around (*gosql.Row).Scan that kills the test on error.
func (r *Row) Scan(dest ...interface{}) {
	if err := r.row.Scan(dest...); err != nil {
		file, line, _ := caller.Lookup(1)
		r.Fatalf("%s:%d: error scanning '%v': %+v", file, line, r.row, err)
	}
}

// QueryRow is a wrapper around gosql.QueryRow that kills the test on error.
func (sr *SQLRunner) QueryRow(query string, args ...interface{}) *Row {
	return &Row{sr.TB, sr.DB.QueryRow(query, args...)}
}

// QueryStr runs a Query and converts the result to a string matrix; nulls are
// represented as "NULL". Empty results are represented by an empty (but
// non-nil) slice. Kills the test on errors.
func (sr *SQLRunner) QueryStr(query string, args ...interface{}) [][]string {
	rows := sr.Query(query)
	cols, err := rows.Columns()
	if err != nil {
		sr.Fatal(err)
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	res := [][]string{}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			sr.Fatal(err)
		}
		row := make([]string, len(vals))
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				switch t := val.(type) {
				case []byte:
					row[j] = string(t)
				default:
					row[j] = fmt.Sprint(val)
				}
			} else {
				row[j] = "NULL"
			}
		}
		res = append(res, row)
	}
	return res
}

// CheckQueryResults checks that the rows returned by a query match the expected
// response.
func (sr *SQLRunner) CheckQueryResults(query string, expected [][]string) {
	res := sr.QueryStr(query)
	if !reflect.DeepEqual(res, expected) {
		file, line, _ := caller.Lookup(1)
		sr.Errorf("%s:%d query '%s': expected:\n%v\ngot:%v\n", file, line, query, expected, res)
	}
}
