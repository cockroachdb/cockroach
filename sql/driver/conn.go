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
	"errors"
	"fmt"
	"math"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
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
	db       *client.DB
	sender   *httpSender
	database string
}

func (c *conn) Close() error {
	return nil
}

func createCall(sql string, args []driver.Value) sqlwire.Call {
	// TODO(vivek): Add arguments later
	return sqlwire.Call{Args: &sqlwire.Request{Sql: sql}, Reply: &sqlwire.Response{}}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	s, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, stmt: s}, nil
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return c.exec(stmt, args)
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return c.query(stmt, args)
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{conn: c}, nil
}

func (c *conn) exec(stmt parser.Statement, args []driver.Value) (driver.Result, error) {
	rows, err := c.query(stmt, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(len(rows.rows)), nil
}

func (c *conn) query(stmt parser.Statement, args []driver.Value) (*rows, error) {
	// TODO(pmattis): Apply the args to the statement.
	switch p := stmt.(type) {
	case *parser.CreateDatabase:
		return c.CreateDatabase(p, args)
	case *parser.CreateTable:
		return c.CreateTable(p, args)
	case *parser.Delete:
		return c.Delete(p, args)
	case *parser.Insert:
		return c.Insert(p, args)
	case *parser.Select:
		return c.Select(p, args)
	case *parser.ShowColumns:
		return c.Send(createCall(stmt.String(), args))
	case *parser.ShowDatabases:
		return c.Send(createCall(stmt.String(), args))
	case *parser.ShowIndex:
		return c.Send(createCall(stmt.String(), args))
	case *parser.ShowTables:
		return c.Send(createCall(stmt.String(), args))
	case *parser.Update:
		return c.Update(p, args)
	case *parser.Use:
		c.database = p.Name
		return c.Send(createCall(stmt.String(), args))
	case *parser.AlterTable:
	case *parser.AlterView:
	case *parser.CreateIndex:
	case *parser.CreateView:
	case *parser.DropDatabase:
	case *parser.DropIndex:
	case *parser.DropTable:
	case *parser.DropView:
	case *parser.RenameTable:
	case *parser.Set:
	case *parser.TruncateTable:
	case *parser.Union:
		// Various unimplemented statements.

	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}

	return nil, fmt.Errorf("TODO(pmattis): unimplemented: %T %s", stmt, stmt)
}

// Send sends the call to the server.
func (c *conn) Send(call sqlwire.Call) (*rows, error) {
	c.sender.Send(context.TODO(), call)
	resp := call.Reply
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Error())
	}
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
			} else if datum.UintVal != nil {
				// uint64 not supported by the driver.Value interface.
				if *datum.UintVal >= math.MaxInt64 {
					return &rows{}, fmt.Errorf("cannot convert very large uint64 %d returned by database", *datum.UintVal)
				}
				t[j] = int64(*datum.UintVal)
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
