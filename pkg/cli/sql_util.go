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
	"net/url"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lib/pq"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type sqlConnI interface {
	driver.Conn
	driver.Execer
	driver.Queryer
}

type sqlConn struct {
	url          string
	conn         sqlConnI
	reconnecting bool
}

func (c *sqlConn) ensureConn() error {
	if c.conn == nil {
		if c.reconnecting && isInteractive {
			fmt.Fprintf(stderr, "connection lost; opening new connection and resetting session parameters...\n")
		}
		conn, err := pq.Open(c.url)
		if err != nil {
			return err
		}
		c.reconnecting = false
		c.conn = conn.(sqlConnI)
	}
	return nil
}

// ExecTxn runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// NOTE: the supplied closure should not have external side
// effects beyond changes to the database.
//
// NB: this code is cribbed from cockroach-go/crdb and has been copied
// because this code, pre-dating go1.8, deals with multiple result sets
// direct with the driver. See #14964.
func (c *sqlConn) ExecTxn(fn func(*sqlConn) error) (err error) {
	// Start a transaction.
	if err = c.Exec(`BEGIN`, nil); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = c.Exec(`COMMIT`, nil)
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = c.Exec(`ROLLBACK`, nil)
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if err = c.Exec(`SAVEPOINT cockroach_restart`, nil); err != nil {
		return err
	}

	for {
		err = fn(c)
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			if err = c.Exec(`RELEASE SAVEPOINT cockroach_restart`, nil); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
			return err
		}
		if err = c.Exec(`ROLLBACK TO SAVEPOINT cockroach_restart`, nil); err != nil {
			return err
		}
	}
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
		c.reconnecting = true
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

	// Go 1.8 multiple result set interfaces.
	// TODO(mjibson): clean this up after 1.8 is released.
	HasNextResultSet() bool
	NextResultSet() error
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
		r.conn.reconnecting = true
		r.conn.Close()
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = append([]byte{}, b...)
		}
	}
	return err
}

// NextResultSet prepares the next result set for reading.
func (r *sqlRows) NextResultSet() (bool, error) {
	if !r.rows.HasNextResultSet() {
		return false, nil
	}
	return true, r.rows.NextResultSet()
}

func makeSQLConn(url string) *sqlConn {
	return &sqlConn{
		url: url,
	}
}

// getPasswordAndMakeSQLClient prompts for a password if running in secure mode
// and no certificates have been supplied. security.RootUser won't be prompted
// for a password as the only authentication method available for this user is
// certificate authentication.
func getPasswordAndMakeSQLClient() (*sqlConn, error) {
	if len(sqlConnURL) != 0 {
		return makeSQLConn(sqlConnURL), nil
	}
	var user *url.Userinfo
	if !baseCfg.Insecure && sqlConnUser != security.RootUser &&
		!baseCfg.ClientHasValidCerts(sqlConnUser) {
		pwd, err := security.PromptForPassword()
		if err != nil {
			return nil, err
		}
		user = url.UserPassword(sqlConnUser, pwd)
	} else {
		user = url.User(sqlConnUser)
	}
	return makeSQLClient(user)
}

func makeSQLClient(user *url.Userinfo) (*sqlConn, error) {
	sqlURL := sqlConnURL
	if len(sqlConnURL) == 0 {
		u, err := sqlCtx.PGURL(user)
		if err != nil {
			return nil, err
		}
		u.Path = sqlConnDBName
		sqlURL = u.String()
	}
	return makeSQLConn(sqlURL), nil
}

type queryFunc func(conn *sqlConn) (*sqlRows, error)

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
func runQuery(
	conn *sqlConn, fn queryFunc, showMoreChars bool,
) ([]string, [][]string, string, error) {
	rows, err := fn(conn)
	if err != nil {
		return nil, nil, "", err
	}

	defer func() { _ = rows.Close() }()
	return sqlRowsToStrings(rows, showMoreChars)
}

// runQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
func runQueryAndFormatResults(
	conn *sqlConn, w io.Writer, fn queryFunc, displayFormat tableDisplayFormat,
) error {
	rows, err := fn(conn)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	for {
		cols := getColumnStrings(rows)
		if len(cols) == 0 {
			// When no columns are returned, we want to render a summary of the
			// number of rows that were returned or affected. To do this this, the
			// driver needs to "consume" all the rows so that the RowsAffected()
			// method returns the correct number of rows (it only reports the number
			// of rows that the driver consumes).
			if err := consumeAllRows(rows); err != nil {
				return err
			}
		}
		formattedTag := getFormattedTag(rows.Tag(), rows.Result())
		if err := printQueryOutput(w, cols, newRowIter(rows, true), formattedTag, displayFormat); err != nil {
			return err
		}

		if more, err := rows.NextResultSet(); err != nil {
			return err
		} else if !more {
			return nil
		}
	}
}

// consumeAllRows consumes all of the rows from the network. Used this method
// when the driver needs to consume all the rows, but you don't care about the
// rows themselves.
func consumeAllRows(rows *sqlRows) error {
	for {
		err := rows.Next(nil)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a  list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
// If showMoreChars is true, then more characters are not escaped.
func sqlRowsToStrings(rows *sqlRows, showMoreChars bool) ([]string, [][]string, string, error) {
	cols := getColumnStrings(rows)
	allRows, err := getAllRowStrings(rows, showMoreChars)
	if err != nil {
		return nil, nil, "", err
	}
	tag := getFormattedTag(rows.Tag(), rows.Result())

	return cols, allRows, tag, nil
}

func getColumnStrings(rows *sqlRows) []string {
	srcCols := rows.Columns()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = formatVal(c, true, false)
	}
	return cols
}

func getAllRowStrings(rows *sqlRows, showMoreChars bool) ([][]string, error) {
	var allRows [][]string

	for {
		rowStrings, err := getNextRowStrings(rows, showMoreChars)
		if err != nil {
			return nil, err
		}
		if rowStrings == nil {
			break
		}
		allRows = append(allRows, rowStrings)
	}

	return allRows, nil
}

func getNextRowStrings(rows *sqlRows, showMoreChars bool) ([]string, error) {
	cols := rows.Columns()
	var vals []driver.Value
	if len(cols) > 0 {
		vals = make([]driver.Value, len(cols))
	}

	err := rows.Next(vals)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rowStrings := make([]string, len(cols))
	for i, v := range vals {
		rowStrings[i] = formatVal(v, showMoreChars, showMoreChars)
	}
	return rowStrings, nil
}

func getFormattedTag(tag string, result driver.Result) string {
	switch tag {
	case "":
		tag = "OK"
	case "SELECT", "DELETE", "INSERT", "UPDATE":
		if n, err := result.RowsAffected(); err == nil {
			tag = fmt.Sprintf("%s %d", tag, n)
		}
	}
	return tag
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

func isNotPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7e || r == '"' || r == '\\' }
func isNotGraphicUnicode(r rune) bool { return !unicode.IsGraphic(r) }
func isNotGraphicUnicodeOrTabOrNewline(r rune) bool {
	return r != '\t' && r != '\n' && !unicode.IsGraphic(r)
}

func formatVal(val driver.Value, showPrintableUnicode bool, showNewLinesAndTabs bool) string {
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

	case time.Time:
		return t.Format(parser.TimestampOutputFormat)
	}

	return fmt.Sprint(val)
}
