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

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"net"

	"github.com/olekukonko/tablewriter"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/pq"
)

type sqlConnI interface {
	driver.Conn
	driver.Queryer
	Next() (driver.Rows, error)
}

type sqlConn struct {
	url  string
	conn sqlConnI
}

func (c *sqlConn) ensureConn() error {
	if c.conn == nil {
		conn, err := pq.Open(c.url)
		if err != nil {
			return err
		}
		c.conn = conn.(sqlConnI)
	}
	return nil
}

func (c *sqlConn) Query(query string, args []driver.Value) (*sqlRows, error) {
	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	rows, err := c.conn.Query(query, args)
	if err == driver.ErrBadConn {
		c.Close()
	}
	if err != nil {
		return nil, err
	}
	return &sqlRows{Rows: rows, conn: c}, nil
}

func (c *sqlConn) Next() (*sqlRows, error) {
	if c.conn == nil {
		return nil, driver.ErrBadConn
	}
	rows, err := c.conn.Next()
	if err == driver.ErrBadConn {
		c.Close()
	}
	if err != nil {
		return nil, err
	}
	return &sqlRows{Rows: rows, conn: c}, nil
}

func (c *sqlConn) Close() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil && err != driver.ErrBadConn {
			log.Info(err)
		}
		c.conn = nil
	}
}

type sqlRows struct {
	driver.Rows
	conn *sqlConn
}

func (r *sqlRows) Close() error {
	err := r.Rows.Close()
	if err == driver.ErrBadConn {
		r.conn.Close()
	}
	return err
}

func (r *sqlRows) Next(values []driver.Value) error {
	err := r.Rows.Next(values)
	if err == driver.ErrBadConn {
		r.conn.Close()
	}
	return err
}

func makeSQLConn(url string) *sqlConn {
	return &sqlConn{
		url: url,
	}
}

func makeSQLClient() *sqlConn {
	sqlURL := connURL
	if len(connURL) == 0 {
		tmpCtx := cliContext
		tmpCtx.PGAddr = net.JoinHostPort(connHost, connPGPort)
		sqlURL = tmpCtx.PGURL(connUser)
	}
	return makeSQLConn(sqlURL)
}

// fmtMap is a mapping from column name to a function that takes the raw input,
// and outputs the string to be displayed.
type fmtMap map[string]func(driver.Value) string

type queryFunc func(conn *sqlConn) (*sqlRows, error)

func nextResult(conn *sqlConn) (*sqlRows, error) {
	return conn.Next()
}

func makeQuery(query string, parameters ...driver.Value) queryFunc {
	return func(conn *sqlConn) (*sqlRows, error) {
		// driver.Value is an alias for interface{}, but must adhere to a restricted
		// set of types when being passed to driver.Queryer.Query (see
		// driver.IsValue). We use driver.DefaultParameterConverter to perform the
		// necessary conversion. This is usually taken care of by the sql package,
		// but we have to do so manually because we're talking directly to the
		// driver.
		for i := range parameters {
			var err error
			parameters[i], err = driver.DefaultParameterConverter.ConvertValue(parameters[i])
			if err != nil {
				return nil, err
			}
		}
		return conn.Query(query, parameters)
	}
}

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
func runQuery(conn *sqlConn, fn queryFunc) ([]string, [][]string, error) {
	return runQueryWithFormat(conn, nil, fn)
}

// runQueryWithFormat takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
// If 'format' is not nil, the values with column name
// found in the map are run through the corresponding callback.
func runQueryWithFormat(conn *sqlConn, format fmtMap, fn queryFunc) (
	[]string, [][]string, error) {
	rows, err := fn(conn)
	if err != nil {
		return nil, nil, err
	}

	defer func() { _ = rows.Close() }()
	return sqlRowsToStrings(rows, format)
}

// runPrettyQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to 'w'.
func runPrettyQuery(conn *sqlConn, w io.Writer, fn queryFunc) error {
	for {
		cols, allRows, err := runQuery(conn, fn)
		if err != nil {
			if err == pq.ErrNoMoreResults {
				return nil
			}
			return err
		}
		printQueryOutput(w, cols, allRows)
		fn = nextResult
	}
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a  list of column values.
// 'rows' should be closed by the caller.
// If 'format' is not nil, the values with column name
// found in the map are run through the corresponding callback.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
func sqlRowsToStrings(rows *sqlRows, format fmtMap) ([]string, [][]string, error) {
	cols := rows.Columns()

	if len(cols) == 0 {
		return nil, nil, nil
	}

	vals := make([]driver.Value, len(cols))
	allRows := [][]string{}

	for {
		err := rows.Next(vals)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rowStrings := make([]string, len(cols))
		for i, v := range vals {
			if f, ok := format[cols[i]]; ok {
				rowStrings[i] = f(v)
			} else {
				rowStrings[i] = formatVal(v)
			}
		}
		allRows = append(allRows, rowStrings)
	}

	return cols, allRows, nil
}

// printQueryOutput takes a list of column names and a list of row contents
// writes a pretty table to 'w', or "OK" if empty.
func printQueryOutput(w io.Writer, cols []string, allRows [][]string) {
	if len(cols) == 0 {
		// This operation did not return rows, just show success.
		fmt.Fprintln(w, "OK")
		return
	}

	// Initialize tablewriter and set column names as the header row.
	table := tablewriter.NewWriter(w)
	table.SetAutoFormatHeaders(false)
	table.SetAutoWrapText(false)
	table.SetHeader(cols)

	for _, row := range allRows {
		table.Append(row)
	}

	table.Render()
}

func formatVal(val driver.Value) string {
	switch t := val.(type) {
	case nil:
		return "NULL"
	case []byte:
		// We don't escape strings that contain only printable ASCII characters.
		if len(bytes.TrimLeftFunc(t, func(r rune) bool { return r >= 0x20 && r < 0x80 })) == 0 {
			return string(t)
		}
		// We use %+q to ensure the output contains only ASCII (see issue #4315).
		return fmt.Sprintf("%+q", t)
	}
	return fmt.Sprint(val)
}
