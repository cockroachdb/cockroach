// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

type sqlConnI interface {
	driver.Conn
	//lint:ignore SA1019 TODO(mjibson): clean this up to use go1.8 APIs
	driver.Execer
	//lint:ignore SA1019 TODO(mjibson): clean this up to use go1.8 APIs
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

// initialSQLConnectionError signals to the error decorator in
// error.go that we're failing during the initial connection set-up.
type initialSQLConnectionError struct {
	err error
}

// Error implements the error interface.
func (i *initialSQLConnectionError) Error() string { return i.err.Error() }

// Cause implements causer.
func (i *initialSQLConnectionError) Cause() error { return i.err }

// Format implements fmt.Formatter.
func (i *initialSQLConnectionError) Format(s fmt.State, verb rune) { errors.FormatError(i, s, verb) }

// FormatError implements errors.Formatter.
func (i *initialSQLConnectionError) FormatError(p errors.Printer) error {
	if p.Detail() {
		p.Print("error while establishing the SQL session")
	}
	return i.err
}

// wrapConnError detects TCP EOF errors during the initial SQL handshake.
// These are translated to a message "perhaps this is not a CockroachDB node"
// at the top level.
// EOF errors later in the SQL session should not be wrapped in that way,
// because by that time we've established that the server is indeed a SQL
// server.
func wrapConnError(err error) error {
	errMsg := err.Error()
	if errMsg == "EOF" || errMsg == "unexpected EOF" {
		return &initialSQLConnectionError{err}
	}
	return err
}

func (c *sqlConn) ensureConn() error {
	if c.conn == nil {
		if c.reconnecting && cliCtx.isInteractive {
			fmt.Fprintf(stderr, "warning: connection lost!\n"+
				"opening new connection: all session settings will be lost\n")
		}
		conn, err := pq.Open(c.url)
		if err != nil {
			return wrapConnError(err)
		}
		if c.reconnecting && c.dbName != "" {
			// Attempt to reset the current database.
			if _, err := conn.(sqlConnI).Exec(
				`SET DATABASE = `+tree.NameStringP(&c.dbName), nil,
			); err != nil {
				fmt.Fprintf(stderr, "warning: unable to restore current database: %v\n", err)
			}
		}
		c.conn = conn.(sqlConnI)
		if err := c.checkServerMetadata(); err != nil {
			c.Close()
			return wrapConnError(err)
		}
		c.reconnecting = false
	}
	return nil
}

func (c *sqlConn) getServerMetadata() (version, clusterID string, err error) {
	// Retrieve the node ID and server build info.
	rows, err := c.Query("SELECT * FROM crdb_internal.node_build_info", nil)
	if err == driver.ErrBadConn {
		return "", "", err
	}
	if err != nil {
		return "", "", err
	}
	defer func() { _ = rows.Close() }()

	// Read the node_build_info table as an array of strings.
	rowVals, err := getAllRowStrings(rows, true /* showMoreChars */)
	if err != nil || len(rowVals) == 0 || len(rowVals[0]) != 3 {
		return "", "", errors.New("incorrect data while retrieving the server version")
	}

	// Extract the version fields from the query results.
	var v10fields [5]string
	for _, row := range rowVals {
		switch row[1] {
		case "ClusterID":
			clusterID = row[2]
		case "Version":
			version = row[2]
		case "Build":
			c.serverBuild = row[2]
		case "Organization":
			c.clusterOrganization = row[2]

			// Fields for v1.0 compatibility.
		case "Distribution":
			v10fields[0] = row[2]
		case "Tag":
			v10fields[1] = row[2]
		case "Platform":
			v10fields[2] = row[2]
		case "Time":
			v10fields[3] = row[2]
		case "GoVersion":
			v10fields[4] = row[2]
		}
	}

	if version == "" {
		// The "Version" field was not present, this indicates a v1.0
		// CockroachDB. Use that below.
		version = "v1.0-" + v10fields[1]
		c.serverBuild = fmt.Sprintf("CockroachDB %s %s (%s, built %s, %s)",
			v10fields[0], version, v10fields[2], v10fields[3], v10fields[4])
	}
	return version, clusterID, nil
}

// checkServerMetadata reports the server version and cluster ID
// upon the initial connection or if either has changed since
// the last connection, based on the last known values in the sqlConn
// struct.
func (c *sqlConn) checkServerMetadata() error {
	if !cliCtx.isInteractive {
		// Version reporting is just noise if the user is not present to
		// change their mind upon seeing the information.
		return nil
	}

	newServerVersion, newClusterID, err := c.getServerMetadata()
	if err == driver.ErrBadConn {
		return err
	}
	if err != nil {
		// It is not an error that the server version cannot be retrieved.
		fmt.Fprintf(stderr, "warning: unable to retrieve the server's version: %s\n", err)
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
		client := build.GetInfo()
		if c.serverVersion != client.Tag {
			fmt.Println("# Client version:", client.Short())
		} else {
			isSame = " (same version as client)"
		}
		fmt.Printf("# Server version: %s%s\n", c.serverBuild, isSame)

		sv, err := version.Parse(c.serverVersion)
		if err == nil {
			cv, err := version.Parse(client.Tag)
			if err == nil {
				if sv.Compare(cv) == -1 { // server ver < client ver
					fmt.Fprintln(stderr, "\nwarning: server version older than client! "+
						"proceed with caution; some features may not be available.\n")
				}
			}
		}
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

// requireServerVersion returns an error if the version of the connected server
// is not at least the given version.
func (c *sqlConn) requireServerVersion(required *version.Version) error {
	versionString, _, err := c.getServerMetadata()
	if err != nil {
		return err
	}
	vers, err := version.Parse(versionString)
	if err != nil {
		return fmt.Errorf("unable to parse server version %q", versionString)
	}
	if !vers.AtLeast(required) {
		return fmt.Errorf("incompatible client and server versions (detected server version: %s, required: %s)",
			vers, required)
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
		fmt.Fprintf(stderr, "warning: error retrieving the %s: %v\n", what, err)
		return nil, false
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) == 0 {
		fmt.Fprintf(stderr, "warning: cannot get the %s\n", what)
		return nil, false
	}

	err = rows.Next(dbVals[:])
	if err != nil {
		fmt.Fprintf(stderr, "warning: invalid %s: %v\n", what, err)
		return nil, false
	}

	return dbVals[0], true
}

// sqlTxnShim implements the crdb.Tx interface.
//
// It exists to support crdb.ExecuteInTxn. Normally, we'd hand crdb.ExecuteInTxn
// a sql.Txn, but sqlConn predates go1.8's support for multiple result sets and
// so deals directly with the lib/pq driver. See #14964.
type sqlTxnShim struct {
	conn *sqlConn
}

func (t sqlTxnShim) Commit() error {
	return t.conn.Exec(`COMMIT`, nil)
}

func (t sqlTxnShim) Rollback() error {
	return t.conn.Exec(`ROLLBACK`, nil)
}

func (t sqlTxnShim) ExecContext(
	_ context.Context, query string, values ...interface{},
) (gosql.Result, error) {
	if len(values) != 0 {
		panic(fmt.Sprintf("sqlTxnShim.ExecContext must not be called with values"))
	}
	return nil, t.conn.Exec(query, nil)
}

// ExecTxn runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// NOTE: the supplied closure should not have external side
// effects beyond changes to the database.
func (c *sqlConn) ExecTxn(fn func(*sqlConn) error) (err error) {
	if err := c.Exec(`BEGIN`, nil); err != nil {
		return err
	}
	return crdb.ExecuteInTx(context.TODO(), sqlTxnShim{c}, func() error {
		return fn(c)
	})
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

	// Assert that there is just one row.
	if err == nil {
		nextVals := make([]driver.Value, len(rows.Columns()))
		nextErr := rows.Next(nextVals)
		if nextErr != io.EOF {
			if nextErr != nil {
				return nil, err
			}
			return nil, fmt.Errorf("programming error: %q: expected just 1 row of result, got more", query)
		}
	}

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
	driver.RowsColumnTypeScanType
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

func (r *sqlRows) ColumnTypeScanType(index int) reflect.Type {
	return r.rows.ColumnTypeScanType(index)
}

func makeSQLConn(url string) *sqlConn {
	return &sqlConn{
		url: url,
	}
}

// getPasswordAndMakeSQLClient prompts for a password if running in secure mode
// and no certificates have been supplied.
// Attempting to use security.RootUser without valid certificates will return an error.
func getPasswordAndMakeSQLClient(appName string) (*sqlConn, error) {
	return makeSQLClient(appName)
}

var sqlConnTimeout = envutil.EnvOrDefaultString("COCKROACH_CONNECT_TIMEOUT", "5")

// makeSQLClient connects to the database using the connection
// settings set by the command-line flags.
//
// The appName given as argument is added to the URL even if --url is
// specified, but only if the URL didn't already specify
// application_name. It is prefixed with '$ ' to mark it as internal.
func makeSQLClient(appName string) (*sqlConn, error) {
	baseURL, err := cliCtx.makeClientConnURL()
	if err != nil {
		return nil, err
	}

	// If there is no user in the URL already, fill in the default user.
	if baseURL.User.Username() == "" {
		baseURL.User = url.User(security.RootUser)
	}

	options, err := url.ParseQuery(baseURL.RawQuery)
	if err != nil {
		return nil, err
	}

	// Insecure connections are insecure and should never see a password. Reject
	// one that may be present in the URL already.
	if options.Get("sslmode") == "disable" {
		if _, pwdSet := baseURL.User.Password(); pwdSet {
			return nil, errors.Errorf("cannot specify a password in URL with an insecure connection")
		}
	} else {
		if baseURL.User.Username() == security.RootUser {
			// Disallow password login for root.
			if options.Get("sslcert") == "" || options.Get("sslkey") == "" {
				return nil, errors.Errorf("connections with user %s must use a client certificate",
					baseURL.User.Username())
			}
			// If we can go on (we have a certificate spec), clear the password.
			baseURL.User = url.User(security.RootUser)
		} else if options.Get("sslcert") == "" || options.Get("sslkey") == "" {
			// If there's no password in the URL yet and we don't have a client
			// certificate, ask for it and populate it in the URL.
			if _, pwdSet := baseURL.User.Password(); !pwdSet {
				pwd, err := security.PromptForPassword()
				if err != nil {
					return nil, err
				}
				baseURL.User = url.UserPassword(baseURL.User.Username(), pwd)
			}
		}
	}

	// Load the application name. It's not a command-line flag, so
	// anything already in the URL should take priority.
	if options.Get("application_name") == "" && appName != "" {
		options.Set("application_name", sqlbase.ReportableAppNamePrefix+appName)
	}

	// Set a connection timeout if none is provided already. This
	// ensures that if the server was not initialized or there is some
	// network issue, the client will not be left to hang forever.
	//
	// This is a lib/pq feature.
	if options.Get("connect_timeout") == "" {
		options.Set("connect_timeout", sqlConnTimeout)
	}

	baseURL.RawQuery = options.Encode()
	sqlURL := baseURL.String()

	if log.V(2) {
		log.Infof(context.Background(), "connecting with URL: %s", sqlURL)
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
	// This one is used with e.g. CREATE TABLE AS (other SELECT
	// statements have type Rows, not RowsAffected).
	"SELECT": {},
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
				nRows, err := ra.RowsAffected()
				if err != nil {
					return false, err
				}

				// This may be either something like INSERT with a valid
				// RowsAffected value, or a statement like SET. The pq driver
				// uses both driver.RowsAffected for both.  So we need to be a
				// little more manual.
				tag := rows.Tag()
				if tag == "SELECT" && nRows == 0 {
					// As explained above, the pq driver unhelpfully does not
					// distinguish between a statement returning zero rows and a
					// statement returning an affected row count of zero.
					// noRowsHook is called non-discriminatingly for both
					// situations.
					//
					// TODO(knz): meanwhile, there are rare, non-SELECT
					// statements that have tag "SELECT" but are legitimately of
					// type RowsAffected. CREATE TABLE AS is one. pq's inability
					// to distinguish those two cases means that any non-SELECT
					// statement that legitimately returns 0 rows affected, and
					// for which the user would expect to see "SELECT 0", will
					// be incorrectly displayed as an empty row result set
					// instead. This needs to be addressed by ensuring pq can
					// distinguish the two cases, or switching to an entirely
					// different driver altogether.
					//
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

		cols := getColumnStrings(rows, true)
		reporter, cleanup, err := makeReporter(w)
		if err != nil {
			return err
		}

		var queryCompleteTime time.Time
		completedHook := func() { queryCompleteTime = timeutil.Now() }

		if err := func() error {
			if cleanup != nil {
				defer cleanup()
			}
			return render(reporter, w, cols, newRowIter(rows, true), completedHook, noRowsHook)
		}(); err != nil {
			return err
		}

		if sqlCtx.showTimes {
			// Present the time since the last result, or since the
			// beginning of execution. Currently the execution engine makes
			// all the work upfront so most of the time is accounted for by
			// the 1st result; this is subject to change once CockroachDB
			// evolves to stream results as statements are executed.
			fmt.Fprintf(w, "\nTime: %s\n", queryCompleteTime.Sub(startTime))
			// Make users better understand any discrepancy they observe.
			renderDelay := timeutil.Now().Sub(queryCompleteTime)
			if renderDelay >= 1*time.Second {
				fmt.Fprintf(w,
					"Note: an additional delay of %s was spent formatting the results.\n"+
						"You can use \\set display_format to change the formatting.\n",
					renderDelay)
			}
			fmt.Fprintln(w)
			// Reset the clock. We ignore the rendering time.
			startTime = timeutil.Now()
		}

		if more, err := rows.NextResultSet(); err != nil {
			return err
		} else if !more {
			return nil
		}
	}
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
// If showMoreChars is true, then more characters are not escaped.
func sqlRowsToStrings(rows *sqlRows, showMoreChars bool) ([]string, [][]string, error) {
	cols := getColumnStrings(rows, showMoreChars)
	allRows, err := getAllRowStrings(rows, showMoreChars)
	if err != nil {
		return nil, nil, err
	}
	return cols, allRows, nil
}

func getColumnStrings(rows *sqlRows, showMoreChars bool) []string {
	srcCols := rows.Columns()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = formatVal(c, showMoreChars, showMoreChars)
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
		s := fmt.Sprintf("%+q", t)
		// Strip the start and final quotes. The surrounding display
		// format (e.g. CSV/TSV) will add its own quotes.
		return s[1 : len(s)-1]

	case []byte:
		// Format the bytes as per bytea_output = escape.
		//
		// We use the "escape" format here because it enables printing
		// readable strings as-is -- the default hex format would always
		// render as hexadecimal digits. The escape format is also more
		// compact.
		//
		// TODO(knz): this formatting is unfortunate/incorrect, and exists
		// only because lib/pq incorrectly interprets the bytes received
		// from the server. The proper behavior would be for the driver to
		// not interpret the bytes and for us here to print that as-is, so
		// that we can let the user see and control the result using
		// `bytea_output`.
		return lex.EncodeByteArrayToRawBytes(string(t),
			sessiondata.BytesEncodeEscape, false /* skipHexPrefix */)

	case time.Time:
		return t.Format(tree.TimestampOutputFormat)
	}

	return fmt.Sprint(val)
}
