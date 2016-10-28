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
// Author: Marc berhault (marc@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package sqlutil

import (
	"database/sql/driver"

	"github.com/pkg/errors"
)

// SQLRowsI models a driver.Rows for the use of SQLRows.
type SQLRowsI interface {
	driver.Rows
	Result() driver.Result
	Tag() string
}

// SQLConnI models the control SQLRows needs over a connection.
type SQLConnI interface {
	Close()
	Reset()
}

// SQLRows wraps a driver.Rows and attempts to give it an interface more similar
// to that of a golang sql.Rows.
type SQLRows struct {
	rows    SQLRowsI
	conn    SQLConnI
	vals    []driver.Value
	lastErr error
}

// MakeSQLRows wraps a rows.
func MakeSQLRows(rows SQLRowsI, conn SQLConnI) SQLRows {
	return SQLRows{rows: rows, conn: conn}
}

// Columns returns the list of columns.
func (r *SQLRows) Columns() []string {
	return r.rows.Columns()
}

// Result returns the query result.
func (r *SQLRows) Result() driver.Result {
	return r.rows.Result()
}

// Tag returns the result tag.
func (r *SQLRows) Tag() string {
	return r.rows.Tag()
}

// Close releases resources.
func (r *SQLRows) Close() error {
	err := r.rows.Close()
	if err == driver.ErrBadConn {
		r.conn.Close()
	}
	return err
}

// Next populates values with the next row of results. []byte values are copied
// so that subsequent calls to Next and Close do not mutate values. This
// makes it slower than theoretically possible but the safety concerns
// (since this is unobvious and unexpected behavior) outweigh.
func (r *SQLRows) Next() bool {
	if r.vals == nil {
		r.vals = make([]driver.Value, len(r.rows.Columns()))
	}
	err := r.rows.Next(r.vals)
	if err != nil {
		r.lastErr = err
		if err == driver.ErrBadConn {
			r.conn.Reset()
		}
		return false
	}
	for i, v := range r.vals {
		if b, ok := v.([]byte); ok {
			r.vals[i] = append([]byte{}, b...)
		}
	}
	return true
}

// ScanRaw returns the current row as []driver.Value.
// Every call to ScanRaw, even the first one, must be preceded by a call to
// Next.
func (r *SQLRows) ScanRaw() ([]driver.Value, error) {
	if r.lastErr != nil {
		return nil, r.lastErr
	}
	return r.vals, nil
}

// Scan converts and returns the current row.
// Every call to Scan, even the first one, must be preceded by a call to Next.
func (r *SQLRows) Scan(dest ...interface{}) error {
	if len(dest) != len(r.vals) {
		return errors.Errorf("expected %d destination arguments in Scan, not %d",
			len(r.vals), len(dest))
	}
	if r.lastErr != nil {
		return r.lastErr
	}
	for i, val := range r.vals {
		err := convertAssign(dest[i], val)
		if err != nil {
			return errors.Errorf("Scan error on column index %d: %v", i, err)
		}
	}
	return nil
}
