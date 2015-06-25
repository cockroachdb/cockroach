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

package driver

import (
	"database/sql/driver"
	"io"
)

type row []driver.Value

type rows struct {
	columns []string
	rows    []row
	pos     int // Next iteration index into rows.
}

// newSingleColumnRows returns a rows structure initialized with a single
// column of values using the specified column name and values. This is a
// convenience routine used by operations which return only a single column.
func newSingleColumnRows(column string, vals []string) *rows {
	r := make([]row, len(vals))
	for i, v := range vals {
		r[i] = row{v}
	}
	return &rows{
		columns: []string{column},
		rows:    r,
	}
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	for i, v := range r.rows[r.pos] {
		dest[i] = v
	}
	r.pos++
	return nil
}
