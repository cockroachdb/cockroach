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
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	// dbName is the last known current database, to be reconfigured in
	// case of automatic reconnects.
	dbName string

	serverVersion string // build.Info.Tag (short version, like 1.0.3)
	serverBuild   string // build.Info.Short (version, platform, etc summary)

	// clusterID and serverBuildInfo are the last known corresponding
	// values from the server, used to report any changes upon
	// (re)connects.
	clusterID           string
	clusterOrganization string
}

func (c *sqlConn) ensureConn() error {
	if c.conn == nil {
		if c.reconnecting && isInteractive {
			fmt.Fprintf(stderr, "connection lost; opening new connection: all session settings will be lost\n")
		}
		conn, err := pq.Open(c.url)
		if err != nil {
			return err
		}
		if c.reconnecting && c.dbName != "" {
			// Attempt to reset the current database.
			if _, err := conn.(sqlConnI).Exec(
				`SET DATABASE = `+parser.Name(c.dbName).String(), nil,
			); err != nil {
				fmt.Fprintf(stderr, "unable to restore current database: %v\n", err)
			}
		}
		c.conn = conn.(sqlConnI)
		if err := c.checkServerMetadata(); err != nil {
			c.Close()
			return err
		}
		c.reconnecting = false
	}
	return nil
}

// checkServerMetadata reports the server version and cluster ID
// upon the initial connection or if either has changed since
// the last connection, based on the last known values in the sqlConn
// struct.
func (c *sqlConn) checkServerMetadata() error {
	if !isInteractive {
		// Version reporting is just noise in non-interactive sessions.
		return nil
	}

	newServerVersion := ""
	newClusterID := ""

	// Retrieve the node ID and server build info.
	rows, err := c.Query("SELECT * FROM crdb_internal.node_build_info", nil)
	if err == driver.ErrBadConn {
		return err
	}
	if err != nil {
		fmt.Fprintln(stderr, "unable to retrieve the server's version")
	} else {
		defer func() { _ = rows.Close() }()

		// Read the node_build_info table as an array of strings.
		rowVals, err := getAllRowStrings(rows, true /* showMoreChars */)
		if err != nil || len(rowVals) == 0 || len(rowVals[0]) != 3 {
			fmt.Fprintln(stderr, "error while retrieving the server's version")
			// It is not an error that the server version cannot be retrieved.
			return nil
		}

		// Extract the version fields from the query results.
		for _, row := range rowVals {
			switch row[1] {
			case "ClusterID":
				newClusterID = row[2]
			case "Version":
				newServerVersion = row[2]
			case "Build":
				c.serverBuild = row[2]
			case "Organization":
				c.clusterOrganization = row[2]
			}

		}
	}

	// Report the server version only if it the revision has been
	// fetched successfully, and the revision has changed since the last
	// connection.
	if newServerVersion != c.serverVersion {
		c.serverVersion = newServerVersion

		isSame := ""
		// We compare just the version (`build.Info.Tag`), whereas we *display* the
		// the full build summary (version, platform, etc) string
		// (`build.Info.Short()`). This is because we don't care if they're
		// different platforms/build tools/timestamps. The important bit exposed by
		// a version mismatch is the wire protocol and SQL dialect.
		if client := build.GetInfo(); c.serverVersion != client.Tag {
			fmt.Println("# Client version:", client.Short())
		} else {
			isSame = " (same version as client)"
		}
		fmt.Printf("# Server version: %s%s\n", c.serverBuild, isSame)
	}

	// Report the cluster ID only if it it could be fetched
	// successfully, and it has changed since the last connection.
	if old := c.clusterID; newClusterID != c.clusterID {
		c.clusterID = newClusterID
		if old != "" {
			return errors.Errorf("the cluster ID has changed!\nPrevious ID: %s\nNew ID: %s",
				old, newClusterID)
		}
		c.clusterID = newClusterID
		fmt.Println("# Cluster ID:", c.clusterID)
		if c.clusterOrganization != "" {
			fmt.Println("# Organization:", c.clusterOrganization)
		}
	}

	return nil
}

// getServerValue retrieves the first driverValue returned by the
// given sql query. If the query fails or does not return a single
// column, `false` is returned in the second result.
func (c *sqlConn) getServerValue(what, sql string) (driver.Value, bool) {
	var dbVals [1]driver.Value

	rows, err := c.Query(sql, nil)
	if err != nil {
		fmt.Fprintf(stderr, "error retrieving the %s: %v\n", what, err)
		return nil, false
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) == 0 {
		fmt.Fprintf(stderr, "cannot get the %s\n", what)
		return nil, false
	}

	err = rows.Next(dbVals[:])
	if err != nil {
		fmt.Fprintf(stderr, "invalid %s: %v\n", what, err)
		return nil, false
	}

	return dbVals[0], true
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
		pqErr, ok := pgerror.GetPGCause(err)
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
	if sqlCtx.echo {
		fmt.Fprintln(stderr, ">", query)
	}
	_, err := c.conn.Exec(query, args)
	if err == driver.ErrBadConn {
		c.reconnecting = true
		c.Close()
	}
	return err
}

func (c *sqlConn) Query(query string, args []driver.Value) (*sqlRows, error) {
	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	if sqlCtx.echo {
		fmt.Fprintln(stderr, ">", query)
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
		r.conn.reconnecting = true
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
// and no certificates have been supplied.
// Attempting to use security.RootUser without valid certificates will return an error.
func getPasswordAndMakeSQLClient() (*sqlConn, error) {
	if len(sqlConnURL) != 0 {
		return makeSQLConn(sqlConnURL), nil
	}
	var user *url.Userinfo
	if !baseCfg.Insecure && !baseCfg.ClientHasValidCerts(sqlConnUser) {
		if sqlConnUser == security.RootUser {
			return nil, errors.Errorf("connections with user %s must use a client certificate", security.RootUser)
		}

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
func runQuery(conn *sqlConn, fn queryFunc, showMoreChars bool) ([]string, [][]string, error) {
	rows, err := fn(conn)
	if err != nil {
		return nil, nil, err
	}

	defer func() { _ = rows.Close() }()
	return sqlRowsToStrings(rows, showMoreChars)
}

// handleCopyError ensures the user is properly informed when they issue
// a COPY statement somewhere in their input.
func handleCopyError(conn *sqlConn, err error) error {
	if !strings.HasPrefix(err.Error(), "pq: unknown response for simple query: 'G'") {
		return err
	}

	// The COPY statement has hosed the connection by putting the
	// protocol in a state that lib/pq cannot understand any more. Reset
	// it.
	conn.Close()
	conn.reconnecting = true
	return errors.New("woops! COPY has confused this client! Suggestion: use 'psql' for COPY")
}

// All tags where the RowsAffected value should be reported to
// the user.
var tagsWithRowsAffected = map[string]struct{}{
	"INSERT":    {},
	"UPDATE":    {},
	"DELETE":    {},
	"DROP USER": {},
}

// runQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
func runQueryAndFormatResults(conn *sqlConn, w io.Writer, fn queryFunc) error {
	startTime := timeutil.Now()
	rows, err := fn(conn)
	if err != nil {
		return handleCopyError(conn, err)
	}
	defer func() {
		_ = rows.Close()
	}()
	for {
		// lib/pq is not able to tell us before the first call to Next()
		// whether a statement returns either
		// - a rows result set with zero rows (e.g. SELECT on an empty table), or
		// - no rows result set, but a valid value for RowsAffected (e.g. INSERT), or
		// - doesn't return any rows whatsoever (e.g. SET).
		//
		// To distinguish them we must go through Next() somehow, which is what the
		// render() function does. So we ask render() to call this noRowsHook
		// when Next() has completed its work and no rows where observed, to decide
		// what to do.
		noRowsHook := func() (bool, error) {
			res := rows.Result()
			if ra, ok := res.(driver.RowsAffected); ok {
				// This may be either something like INSERT with a valid
				// RowsAffected value, or a statement like SET. The pq driver
				// uses both driver.RowsAffected for both.  So we need to be a
				// little more manual.
				tag := rows.Tag()
				if tag == "SELECT" {
					// The driver unhelpfully "optimizes" a SELECT returning no rows
					// into a driver.RowsAffected instance with value 0. In that
					// case, we still want the reporter to do its job properly.
					return false, nil
				} else if _, ok := tagsWithRowsAffected[tag]; ok {
					// INSERT, DELETE, etc.: print the row count.
					nRows, err := ra.RowsAffected()
					if err != nil {
						return false, err
					}
					fmt.Fprintf(w, "%s %d\n", tag, nRows)
				} else {
					// SET, etc.: just print the tag, or OK if there's no tag.
					if tag == "" {
						tag = "OK"
					}
					fmt.Fprintln(w, tag)
				}
				return true, nil
			}
			// Other cases: this is a statement with a rows result set, but
			// zero rows (e.g. SELECT on empty table). Let the reporter
			// handle it.
			return false, nil
		}

		cols := getColumnStrings(rows)
		reporter, err := makeReporter()
		if err != nil {
			return nil
		}
		if err := render(reporter, w, cols, newRowIter(rows, true), noRowsHook); err != nil {
			return err
		}

		if cliCtx.showTimes {
			// Present the time since the last result, or since the
			// beginning of execution. Currently the execution engine makes
			// all the work upfront so most of the time is accounted for by
			// the 1st result; this is subject to change once CockroachDB
			// evolves to stream results as statements are executed.
			newNow := timeutil.Now()
			fmt.Fprintf(w, "\nTime: %s\n\n", newNow.Sub(startTime))
			startTime = newNow
		}

		if more, err := rows.NextResultSet(); err != nil {
			return err
		} else if !more {
			return nil
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
func sqlRowsToStrings(rows *sqlRows, showMoreChars bool) ([]string, [][]string, error) {
	cols := getColumnStrings(rows)
	allRows, err := getAllRowStrings(rows, showMoreChars)
	if err != nil {
		return nil, nil, err
	}
	return cols, allRows, nil
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
