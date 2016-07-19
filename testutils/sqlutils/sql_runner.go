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
	"testing"
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
		sr.Fatalf("error executing '%s': %s", query, err)
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
