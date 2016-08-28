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
	"strings"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/context"

	"github.com/olekukonko/tablewriter"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/pq"
)

type sqlConnI interface {
	driver.Conn
	driver.Execer
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

func (c *sqlConn) Exec(query string, args []driver.Value) error {
	if err := c.ensureConn(); err != nil {
		return err
	}
	_, err := c.conn.Exec(query, args)
	return err
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
	return &sqlRows{rows: rows.(sqlRowsI), conn: c}, nil
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
	return &sqlRows{rows: rows.(sqlRowsI), conn: c}, nil
}

func (c *sqlConn) QueryRow(query string, args []driver.Value) ([]driver.Value, error) {
	rows, err := makeQuery(query, args...)(c)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	vals := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(vals)
	return vals, err
}

func (c *sqlConn) Close() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil && err != driver.ErrBadConn {
			log.Info(context.TODO(), err)
		}
		c.conn = nil
	}
}

type sqlRowsI interface {
	driver.Rows
	Result() driver.Result
	Tag() string
}

type sqlRows struct {
	rows sqlRowsI
	conn *sqlConn
}

func (r *sqlRows) Columns() []string {
	return r.rows.Columns()
}

func (r *sqlRows) Result() driver.Result {
	return r.rows.Result()
}

func (r *sqlRows) Tag() string {
	return r.rows.Tag()
}

func (r *sqlRows) Close() error {
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
func (r *sqlRows) Next(values []driver.Value) error {
	err := r.rows.Next(values)
	if err == driver.ErrBadConn {
		r.conn.Close()
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = append([]byte{}, b...)
		}
	}
	return err
}

func makeSQLConn(url string) *sqlConn {
	return &sqlConn{
		url: url,
	}
}

func makeSQLClient() (*sqlConn, error) {
	sqlURL := connURL
	if len(connURL) == 0 {
		u, err := sqlCtx.PGURL(connUser)
		if err != nil {
			return nil, err
		}
		u.Path = connDBName
		sqlURL = u.String()
	}
	return makeSQLConn(sqlURL), nil
}

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
func runQuery(conn *sqlConn, fn queryFunc, pretty bool) ([]string, [][]string, string, error) {
	rows, err := fn(conn)
	if err != nil {
		return nil, nil, "", err
	}

	defer func() { _ = rows.Close() }()
	return sqlRowsToStrings(rows, pretty)
}

// runQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
func runQueryAndFormatResults(
	conn *sqlConn, w io.Writer, fn queryFunc, pretty bool,
) error {
	for {
		cols, allRows, result, err := runQuery(conn, fn, pretty)
		if err != nil {
			if err == pq.ErrNoMoreResults {
				return nil
			}
			return err
		}
		printQueryOutput(w, cols, allRows, result, pretty)
		fn = nextResult
	}
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a  list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
// If pretty is true, then more characters are not escaped.
func sqlRowsToStrings(rows *sqlRows, pretty bool) ([]string, [][]string, string, error) {
	srcCols := rows.Columns()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = formatVal(c, pretty, false)
	}

	var allRows [][]string
	var vals []driver.Value
	if len(cols) > 0 {
		vals = make([]driver.Value, len(cols))
	}

	for {
		err := rows.Next(vals)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, "", err
		}
		rowStrings := make([]string, len(cols))
		for i, v := range vals {
			rowStrings[i] = formatVal(v, pretty, pretty)
		}
		allRows = append(allRows, rowStrings)
	}

	result := rows.Result()
	tag := rows.Tag()
	switch tag {
	case "":
		tag = "OK"
	case "DELETE", "INSERT", "UPDATE":
		if n, err := result.RowsAffected(); err == nil {
			tag = fmt.Sprintf("%s %d", tag, n)
		}
	}

	return cols, allRows, tag, nil
}

// expandTabsAndNewLines ensures that multi-line row strings that may
// contain tabs are properly formatted: tabs are expanded to spaces,
// and newline characters are marked visually. Marking newline
// characters is especially important in single-column results where
// the underlying TableWriter would not otherwise show the difference
// between one multi-line row and two one-line rows.
func expandTabsAndNewLines(s string) string {
	var buf bytes.Buffer
	// 4-wide columns, 1 character minimum width.
	w := tabwriter.NewWriter(&buf, 4, 0, 1, ' ', 0)
	fmt.Fprint(w, strings.Replace(s, "\n", "␤\n", -1))
	_ = w.Flush()
	return buf.String()
}

// printQueryOutput takes a list of column names and a list of row contents
// writes a pretty table to 'w', or "OK" if empty.
func printQueryOutput(
	w io.Writer, cols []string, allRows [][]string, tag string, pretty bool,
) {
	if len(cols) == 0 {
		// This operation did not return rows, just show the tag.
		fmt.Fprintln(w, tag)
		return
	}

	if pretty {
		// Initialize tablewriter and set column names as the header row.
		table := tablewriter.NewWriter(w)
		table.SetAutoFormatHeaders(false)
		table.SetAutoWrapText(false)
		table.SetHeader(cols)
		for _, row := range allRows {
			for i, r := range row {
				row[i] = expandTabsAndNewLines(r)
			}
			table.Append(row)
		}
		table.Render()
		nRows := len(allRows)
		fmt.Fprintf(w, "(%d row%s)\n", nRows, util.Pluralize(int64(nRows)))
	} else {
		if len(cols) == 0 {
			// No result selected, inform the user.
			fmt.Fprintln(w, tag)
		} else {
			// Some results selected, inform the user about how much data to expect.
			fmt.Fprintf(w, "%d row%s\n", len(allRows),
				util.Pluralize(int64(len(allRows))))

			// Then print the results themselves.
			fmt.Fprintln(w, strings.Join(cols, "\t"))
			for _, row := range allRows {
				fmt.Fprintln(w, strings.Join(row, "\t"))
			}
		}
	}
}

func isNotPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7e || r == '"' || r == '\\' }
func isNotGraphicUnicode(r rune) bool { return !unicode.IsGraphic(r) }
func isNotGraphicUnicodeOrTabOrNewline(r rune) bool {
	return r != '\t' && r != '\n' && !unicode.IsGraphic(r)
}

func formatVal(
	val driver.Value, showPrintableUnicode bool, showNewLinesAndTabs bool,
) string {
	switch t := val.(type) {
	case nil:
		return "NULL"
	case string:
		if showPrintableUnicode {
			pred := isNotGraphicUnicode
			if showNewLinesAndTabs {
				pred = isNotGraphicUnicodeOrTabOrNewline
			}
			if utf8.ValidString(t) && strings.IndexFunc(t, pred) == -1 {
				return t
			}
		} else {
			if strings.IndexFunc(t, isNotPrintableASCII) == -1 {
				return t
			}
		}
		return fmt.Sprintf("%+q", t)

	case []byte:
		if showPrintableUnicode {
			pred := isNotGraphicUnicode
			if showNewLinesAndTabs {
				pred = isNotGraphicUnicodeOrTabOrNewline
			}
			if utf8.Valid(t) && bytes.IndexFunc(t, pred) == -1 {
				return string(t)
			}
		} else {
			if bytes.IndexFunc(t, isNotPrintableASCII) == -1 {
				return string(t)
			}
		}
		return fmt.Sprintf("%+q", t)
	}
	return fmt.Sprint(val)
}
