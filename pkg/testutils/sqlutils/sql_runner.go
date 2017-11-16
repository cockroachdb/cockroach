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

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"
)

// SQLRunner wraps a testing.TB and *gosql.DB connection and provides
// convenience functions to run SQL statements and fail the test on any errors.
type SQLRunner struct {
	DB *gosql.DB
}

// MakeSQLRunner returns a SQLRunner for the given database connection.
func MakeSQLRunner(db *gosql.DB) *SQLRunner {
	return &SQLRunner{DB: db}
}

// Exec is a wrapper around gosql.Exec that kills the test on error.
func (sr *SQLRunner) Exec(t testing.TB, query string, args ...interface{}) gosql.Result {
	t.Helper()
	r, err := sr.DB.Exec(query, args...)
	if err != nil {
		t.Fatalf("error executing '%s': %s", query, err)
	}
	return r
}

// ExecRowsAffected executes the statement and verifies that RowsAffected()
// matches the expected value. It kills the test on errors.
func (sr *SQLRunner) ExecRowsAffected(
	t testing.TB, expRowsAffected int, query string, args ...interface{},
) {
	t.Helper()
	r := sr.Exec(t, query, args...)
	numRows, err := r.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if numRows != int64(expRowsAffected) {
		t.Fatalf("expected %d affected rows, got %d on '%s'", expRowsAffected, numRows, query)
	}
}

// Query is a wrapper around gosql.Query that kills the test on error.
func (sr *SQLRunner) Query(t testing.TB, query string, args ...interface{}) *gosql.Rows {
	t.Helper()
	r, err := sr.DB.Query(query, args...)
	if err != nil {
		t.Fatalf("error executing '%s': %s", query, err)
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
	r.Helper()
	if err := r.row.Scan(dest...); err != nil {
		r.Fatalf("error scanning '%v': %+v", r.row, err)
	}
}

// QueryRow is a wrapper around gosql.QueryRow that kills the test on error.
func (sr *SQLRunner) QueryRow(t testing.TB, query string, args ...interface{}) *Row {
	t.Helper()
	return &Row{t, sr.DB.QueryRow(query, args...)}
}

// QueryStr runs a Query and converts the result using RowsToStrMatrix. Kills
// the test on errors.
func (sr *SQLRunner) QueryStr(t testing.TB, query string, args ...interface{}) [][]string {
	t.Helper()
	rows := sr.Query(t, query, args...)
	r, err := RowsToStrMatrix(rows)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// RowsToStrMatrix converts the given result rows to a string matrix; nulls are
// represented as "NULL". Empty results are represented by an empty (but
// non-nil) slice.
func RowsToStrMatrix(rows *gosql.Rows) ([][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	res := [][]string{}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return nil, err
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
	return res, nil
}

// CheckQueryResults checks that the rows returned by a query match the expected
// response.
func (sr *SQLRunner) CheckQueryResults(t testing.TB, query string, expected [][]string) {
	t.Helper()
	res := sr.QueryStr(t, query)
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("query '%s': expected:\n%v\ngot:%v\n", query, expected, res)
	}
}
