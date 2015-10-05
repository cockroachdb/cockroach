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
	"strings"

	"github.com/cockroachdb/cockroach/util"
)

var _ driver.Conn = &conn{}
var _ driver.Queryer = &conn{}
var _ driver.Execer = &conn{}

// conn implements the sql/driver.Conn interface. Note that conn is assumed to
// be stateful and is not used concurrently by multiple goroutines; See
// https://golang.org/pkg/database/sql/driver/#Conn.
type conn struct {
	sender           Sender
	session          []byte
	beginTransaction bool
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, stmt: query}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	c.beginTransaction = true
	return &tx{conn: c}, nil
}

func (c *conn) Exec(stmt string, args []driver.Value) (driver.Result, error) {
	result, err := c.internalQuery(stmt, args)
	if err != nil {
		return nil, err
	}
	switch t := result.GetUnion().(type) {
	case nil:
		return nil, nil
	case *Response_Result_DDL_:
		return driver.ResultNoRows, nil
	case *Response_Result_RowsAffected:
		return driver.RowsAffected(int(t.RowsAffected)), nil
	case *Response_Result_Rows_:
		return driver.RowsAffected(len(t.Rows.Rows)), nil
	default:
		return nil, util.Errorf("unexpected result %s of type %T", t, t)
	}
}

func (c *conn) Query(stmt string, args []driver.Value) (driver.Rows, error) {
	result, err := c.internalQuery(stmt, args)
	if err != nil {
		return nil, err
	}

	driverRows := &rows{}

	switch t := result.GetUnion().(type) {
	case *Response_Result_Rows_:
		resultRows := t.Rows

		driverRows.columns = resultRows.Columns

		driverRows.rows = make([][]driver.Value, 0, len(resultRows.Rows))
		for _, row := range resultRows.Rows {
			values := make([]driver.Value, 0, len(row.Values))
			for _, datum := range row.Values {
				val, err := datum.Value()
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
			driverRows.rows = append(driverRows.rows, values)
		}
	}

	return driverRows, nil
}

func (c *conn) internalQuery(stmt string, args []driver.Value) (*Response_Result, error) {
	if c.beginTransaction {
		stmt = "BEGIN TRANSACTION; " + stmt
		c.beginTransaction = false
	}
	dArgs := make([]Datum, 0, len(args))
	for _, arg := range args {
		datum, err := makeDatum(arg)
		if err != nil {
			return nil, err
		}
		dArgs = append(dArgs, datum)
	}

	return c.send(stmt, dArgs)
}

// send sends the statement to the server.
func (c *conn) send(stmt string, dArgs []Datum) (*Response_Result, error) {
	args := Request{
		Session: c.session,
		Sql:     stmt,
		Params:  dArgs,
	}
	// Forget the session state, and use the one provided in the server
	// response for the next request.
	c.session = nil

	resp, err := c.sender.Send(args)
	if err != nil {
		return nil, err
	}
	// Set the session state even if the server returns an application error.
	// The server is responsible for constructing the correct session state
	// and sending it back.
	c.session = resp.Session

	// Check for any application errors.
	// TODO(vivek): We might want to bunch all errors found here into
	// a single error.
	for _, result := range resp.Results {
		if result.Error != nil {
			return nil, errors.New(*result.Error)
		}
	}

	// Only use the last result.
	if index := len(resp.Results); index != 0 {
		return &resp.Results[index-1], nil
	}
	return nil, nil
}

// Execute all the URL settings against the db to create
// the correct session state.
func (c *conn) applySettings(params map[string]string) error {
	var commands []string
	for _, setting := range []struct {
		name     string
		operator string
	}{
		{name: "DATABASE", operator: " = "},
		{name: "TIME ZONE", operator: " "},
	} {
		if val, ok := params[strings.ToLower(strings.Replace(setting.name, " ", "_", -1))]; ok {
			commands = append(commands, strings.Join([]string{"SET " + setting.name, val}, setting.operator))
		}
	}
	_, err := c.Exec(strings.Join(commands, ";"), nil)
	return err
}
