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
	"fmt"
	"time"
)

// TODO(pmattis):
//
// - This file contains the experimental Cockroach sql driver. The driver
//   currently parses SQL and executes key/value operations in order to execute
//   the SQL. The execution will fairly quickly migrate to the server with the
//   driver performing RPCs.
//
// - Flesh out basic insert, update, delete and select operations.
//
// - Figure out transaction story.

// conn implements the sql/driver.Conn interface. Note that conn is assumed to
// be stateful and is not used concurrently by multiple goroutines; See
// https://golang.org/pkg/database/sql/driver/#Conn.
type conn struct {
	sender  Sender
	session []byte
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, stmt: query}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{conn: c}, nil
}

func (c *conn) Exec(stmt string, args []driver.Value) (driver.Result, error) {
	rows, err := c.Query(stmt, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(len(rows.rows)), nil
}

func (c *conn) Query(stmt string, args []driver.Value) (*rows, error) {
	params := make([]Datum, 0, len(args))
	for _, arg := range args {
		var param Datum
		switch value := arg.(type) {
		case int64:
			param.IntVal = &value
		case float64:
			param.FloatVal = &value
		case bool:
			param.BoolVal = &value
		case []byte:
			param.BytesVal = value
		case string:
			param.StringVal = &value
		case time.Time:
			// TODO(vivek): pass in time as an input that can be interpreted
			// by the server.
			time, err := value.MarshalBinary()
			if err != nil {
				return nil, err
			}
			param.BytesVal = time
		}
		params = append(params, param)
	}
	return c.send(Request{
		RequestHeader: RequestHeader{Session: c.session},
		Sql:           stmt,
		Params:        params,
	})
}

// send sends the call to the server.
func (c *conn) send(args Request) (*rows, error) {
	resp, err := c.sender.Send(args)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	c.session = resp.Session
	// Translate into rows
	r := &rows{}
	// Only use the last result to populate the response
	index := len(resp.Results) - 1
	if index < 0 {
		return r, nil
	}
	result := resp.Results[index]
	r.columns = make([]string, len(result.Columns))
	for i, column := range result.Columns {
		r.columns[i] = column
	}
	r.rows = make([]row, len(result.Rows))
	for i, p := range result.Rows {
		t := make(row, len(p.Values))
		for j, datum := range p.Values {
			if datum.BoolVal != nil {
				t[j] = *datum.BoolVal
			} else if datum.IntVal != nil {
				t[j] = *datum.IntVal
			} else if datum.FloatVal != nil {
				t[j] = *datum.FloatVal
			} else if datum.BytesVal != nil {
				t[j] = datum.BytesVal
			} else if datum.StringVal != nil {
				t[j] = []byte(*datum.StringVal)
			}
			if !driver.IsScanValue(t[j]) {
				panic(fmt.Sprintf("unsupported type %T returned by database", t[j]))
			}
		}
		r.rows[i] = t
	}
	return r, nil
}
