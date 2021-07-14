// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/lib/pq/auth/kerberos"
)

func init() {
	// Ensure that the CLI client commands can use GSSAPI authentication.
	pq.RegisterGSSProvider(func() (pq.GSS, error) { return kerberos.NewGSS() })
}

type sqlConn struct {
	url          string
	conn         DriverConn
	reconnecting bool

	// passwordMissing is true iff the url is missing a password.
	passwordMissing bool

	pendingNotices []*pq.Error

	// delayNotices, if set, makes notices accumulate for printing
	// when the SQL execution completes. The default (false)
	// indicates that notices must be printed as soon as they are received.
	// This is used by the Query() interface to avoid interleaving
	// notices with result rows.
	delayNotices bool

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

	// isInteractive indicates whether the input is interactive.
	isInteractive bool

	// enableServerExecutionTimings indicates whether to measure query
	// latency server-side.
	// This is implemented as a pointer for now until we properly
	// support connection reconfiguration in the SQL shell.
	enableServerExecutionTimings *bool

	// embeddedMode indicates whether to print informational
	// messages.
	embeddedMode bool

	// echo indicates whether to print out executed SQL statements.
	// This is implemented as a pointer for now until we properly
	// support connection reconfiguration in the SQL shell.
	echo *bool

	// showTimes indicates whether to print out connection latency.
	// This is implemented as a pointer for now until we properly
	// support connection reconfiguration in the SQL shell.
	showTimes *bool

	// verboseTimings indicates whether to display detailed latency
	// details on SQL statements.
	// This is implemented as a pointer for now until we properly
	// support connection reconfiguration in the SQL shell.
	verboseTimings *bool
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
		return &InitialSQLConnectionError{err}
	}
	return err
}

func (c *sqlConn) flushNotices() {
	for _, notice := range c.pendingNotices {
		clierror.OutputError(stderr, notice, true /*showSeverity*/, false /*verbose*/)
	}
	c.pendingNotices = nil
	c.delayNotices = false
}

func (c *sqlConn) handleNotice(notice *pq.Error) {
	c.pendingNotices = append(c.pendingNotices, notice)
	if !c.delayNotices {
		c.flushNotices()
	}
}

// GetURL implements the Conn interface.
func (c *sqlConn) GetURL() string {
	return c.url
}

// SetURL implements the Conn interface.
func (c *sqlConn) SetURL(url string) {
	c.url = url
}

// GetDriverConn implements the Conn interface.
func (c *sqlConn) GetDriverConn() DriverConn {
	return c.conn
}

// SetCurrentDatabase implements the Conn interface.
func (c *sqlConn) SetCurrentDatabase(dbName string) {
	c.dbName = dbName
}

// SetMissingPassword implements the Conn interface.
func (c *sqlConn) SetMissingPassword(missing bool) {
	c.passwordMissing = missing
}

// EnsureConn (re-)establishes the connection to the server.
func (c *sqlConn) EnsureConn() error {
	if c.conn == nil {
		if c.reconnecting && c.isInteractive {
			fmt.Fprintf(stderr, "warning: connection lost!\n"+
				"opening new connection: all session settings will be lost\n")
		}
		base, err := pq.NewConnector(c.url)
		if err != nil {
			return wrapConnError(err)
		}
		// Add a notice handler - re-use the cliOutputError function in this case.
		connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
			c.handleNotice(notice)
		})
		// TODO(cli): we can't thread ctx through ensureConn usages, as it needs
		// to follow the gosql.DB interface. We should probably look at initializing
		// connections only once instead. The context is only used for dialing.
		conn, err := connector.Connect(context.TODO())
		if err != nil {
			// Connection failed: if the failure is due to a mispresented
			// password, we're going to fill the password here.
			//
			// TODO(knz): CockroachDB servers do not properly fill SQLSTATE
			// (28P01) for password auth errors, so we have to "make do"
			// with a string match. This should be cleaned up by adding
			// the missing code server-side.
			errStr := strings.TrimPrefix(err.Error(), "pq: ")
			if strings.HasPrefix(errStr, "password authentication failed") && c.passwordMissing {
				if pErr := c.fillPassword(); pErr != nil {
					return errors.CombineErrors(err, pErr)
				}
				// Recurse, once. We recurse to ensure that pq.NewConnector
				// and ConnectorWithNoticeHandler get called with the new URL.
				// The recursion only occurs once because fillPassword()
				// resets c.passwordMissing, so we cannot get into this
				// conditional a second time.
				return c.EnsureConn()
			}
			// Not a password auth error, or password already set. Simply fail.
			return wrapConnError(err)
		}
		if c.reconnecting && c.dbName != "" {
			// Attempt to reset the current database.
			if _, err := conn.(DriverConn).Exec(
				`SET DATABASE = `+tree.NameStringP(&c.dbName), nil,
			); err != nil {
				fmt.Fprintf(stderr, "warning: unable to restore current database: %v\n", err)
			}
		}
		c.conn = conn.(DriverConn)
		if err := c.checkServerMetadata(); err != nil {
			err = errors.CombineErrors(err, c.Close())
			return wrapConnError(err)
		}
		c.reconnecting = false
	}
	return nil
}

// tryEnableServerExecutionTimings attempts to check if the server supports the
// SHOW LAST QUERY STATISTICS statements. This allows the CLI client to report
// server side execution timings instead of timing on the client.
func (c *sqlConn) tryEnableServerExecutionTimings() {
	_, _, _, _, _, _, err := c.getLastQueryStatistics()
	if err != nil {
		fmt.Fprintf(stderr, "warning: cannot show server execution timings: unexpected column found\n")
		*c.enableServerExecutionTimings = false
	} else {
		*c.enableServerExecutionTimings = true
	}
}

func (c *sqlConn) GetServerMetadata() (
	nodeID roachpb.NodeID,
	version, clusterID string,
	err error,
) {
	// Retrieve the node ID and server build info.
	rows, err := c.Query("SELECT * FROM crdb_internal.node_build_info", nil)
	if errors.Is(err, driver.ErrBadConn) {
		return 0, "", "", err
	}
	if err != nil {
		return 0, "", "", err
	}
	defer func() { _ = rows.Close() }()

	// Read the node_build_info table as an array of strings.
	rowVals, err := getAllRowStrings(rows, rows.ColumnTypeNames(), true /* showMoreChars */)
	if err != nil || len(rowVals) == 0 || len(rowVals[0]) != 3 {
		return 0, "", "", errors.New("incorrect data while retrieving the server version")
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
			id, err := strconv.Atoi(row[0])
			if err != nil {
				return 0, "", "", errors.New("incorrect data while retrieving node id")
			}
			nodeID = roachpb.NodeID(id)

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
	return nodeID, version, clusterID, nil
}

// checkServerMetadata reports the server version and cluster ID
// upon the initial connection or if either has changed since
// the last connection, based on the last known values in the sqlConn
// struct.
func (c *sqlConn) checkServerMetadata() error {
	if !c.isInteractive {
		// Version reporting is just noise if the user is not present to
		// change their mind upon seeing the information.
		return nil
	}
	if c.embeddedMode {
		// Version reporting is non-actionable if the user does
		// not have control over how the server and client are run.
		return nil
	}

	_, newServerVersion, newClusterID, err := c.GetServerMetadata()
	if errors.Is(err, driver.ErrBadConn) {
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
	// Try to enable server execution timings for the CLI to display if
	// supported by the server.
	c.tryEnableServerExecutionTimings()

	return nil
}

// GetServerValue retrieves the first driverValue returned by the
// given sql query. If the query fails or does not return a single
// column, `false` is returned in the second result.
func (c *sqlConn) GetServerValue(what, sql string) (driver.Value, string, bool) {
	rows, err := c.Query(sql, nil)
	if err != nil {
		fmt.Fprintf(stderr, "warning: error retrieving the %s: %v\n", what, err)
		return nil, "", false
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) == 0 {
		fmt.Fprintf(stderr, "warning: cannot get the %s\n", what)
		return nil, "", false
	}

	dbColType := rows.ColumnTypeDatabaseTypeName(0)
	dbVals := make([]driver.Value, len(rows.Columns()))

	err = rows.Next(dbVals[:])
	if err != nil {
		fmt.Fprintf(stderr, "warning: invalid %s: %v\n", what, err)
		return nil, "", false
	}

	return dbVals[0], dbColType, true
}

// parseLastQueryStatistics runs the "SHOW LAST QUERY STATISTICS" statements,
// performs sanity checks, and returns the exec latency and service latency from
// the sql row parsed as time.Duration.
func (c *sqlConn) getLastQueryStatistics() (
	parseLat, planLat, execLat, serviceLat, jobsLat time.Duration,
	containsJobLat bool,
	err error,
) {
	rows, err := c.Query("SHOW LAST QUERY STATISTICS", nil)
	if err != nil {
		return 0, 0, 0, 0, 0, false, err
	}
	defer func() {
		closeErr := rows.Close()
		err = errors.CombineErrors(err, closeErr)
	}()

	// TODO(arul): In 21.1, SHOW LAST QUERY STATISTICS returned 4 columns. In 21.2,
	// it returns 5. Depending on which server version the CLI is connected to,
	// both are valid. We won't have to account for this mixed version state in
	// 22.1. All this logic can be simplified in 22.1.
	if len(rows.Columns()) == 5 {
		containsJobLat = true
	} else if len(rows.Columns()) != 4 {
		return 0, 0, 0, 0, 0, false,
			errors.Newf("unexpected number of columns in SHOW LAST QUERY STATISTICS")
	}

	if rows.Columns()[0] != "parse_latency" ||
		rows.Columns()[1] != "plan_latency" ||
		rows.Columns()[2] != "exec_latency" ||
		rows.Columns()[3] != "service_latency" ||
		(containsJobLat && rows.Columns()[4] != "post_commit_jobs_latency") {
		return 0, 0, 0, 0, 0, containsJobLat,
			errors.New("unexpected columns in SHOW LAST QUERY STATISTICS")
	}

	iter := newRowIter(rows, true /* showMoreChars */)
	nRows := 0
	var parseLatencyRaw string
	var planLatencyRaw string
	var execLatencyRaw string
	var serviceLatencyRaw string
	var jobsLatencyRaw string
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, 0, 0, 0, 0, containsJobLat, err
		}

		parseLatencyRaw = FormatVal(row[0], iter.colTypes[0], false, false)
		planLatencyRaw = FormatVal(row[1], iter.colTypes[1], false, false)
		execLatencyRaw = FormatVal(row[2], iter.colTypes[2], false, false)
		serviceLatencyRaw = FormatVal(row[3], iter.colTypes[3], false, false)
		if containsJobLat {
			jobsLatencyRaw = FormatVal(row[4], iter.colTypes[4], false, false)
		}

		nRows++
	}

	if nRows != 1 {
		return 0, 0, 0, 0, 0, containsJobLat,
			errors.Newf("unexpected number of rows in SHOW LAST QUERY STATISTICS: %d", nRows)
	}

	// This should really be the same as the session's IntervalStyle
	// but that only effects negative intervals in the magnitude
	// of days - and all these latencies should be positive.
	parsedExecLatency, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, execLatencyRaw)
	parsedServiceLatency, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, serviceLatencyRaw)
	parsedPlanLatency, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, planLatencyRaw)
	parsedParseLatency, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, parseLatencyRaw)

	if containsJobLat {
		parsedJobsLatency, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, jobsLatencyRaw)
		jobsLat = time.Duration(parsedJobsLatency.Duration.Nanos())
	}

	return time.Duration(parsedParseLatency.Duration.Nanos()),
		time.Duration(parsedPlanLatency.Duration.Nanos()),
		time.Duration(parsedExecLatency.Duration.Nanos()),
		time.Duration(parsedServiceLatency.Duration.Nanos()),
		jobsLat,
		containsJobLat,
		nil
}

// sqlTxnShim implements the crdb.Tx interface.
//
// It exists to support crdb.ExecuteInTxn. Normally, we'd hand crdb.ExecuteInTxn
// a sql.Txn, but sqlConn predates go1.8's support for multiple result sets and
// so deals directly with the lib/pq driver. See #14964.
//
// TODO(knz): This code is incorrect, see
// https://github.com/cockroachdb/cockroach/issues/67261
type sqlTxnShim struct {
	conn *sqlConn
}

var _ crdb.Tx = sqlTxnShim{}

func (t sqlTxnShim) Commit(context.Context) error {
	return t.conn.Exec(`COMMIT`, nil)
}

func (t sqlTxnShim) Rollback(context.Context) error {
	return t.conn.Exec(`ROLLBACK`, nil)
}

func (t sqlTxnShim) Exec(_ context.Context, query string, values ...interface{}) error {
	if len(values) != 0 {
		panic("sqlTxnShim.ExecContext must not be called with values")
	}
	return t.conn.Exec(query, nil)
}

// ExecTxn runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// NOTE: the supplied closure should not have external side
// effects beyond changes to the database.
func (c *sqlConn) ExecTxn(fn func(TxBoundConn) error) (err error) {
	if err := c.Exec(`BEGIN`, nil); err != nil {
		return err
	}
	return crdb.ExecuteInTx(context.TODO(), sqlTxnShim{c}, func() error {
		return fn(c)
	})
}

func (c *sqlConn) Exec(query string, args []driver.Value) error {
	if err := c.EnsureConn(); err != nil {
		return err
	}
	if *c.echo {
		fmt.Fprintln(stderr, ">", query)
	}
	_, err := c.conn.Exec(query, args)
	c.flushNotices()
	if errors.Is(err, driver.ErrBadConn) {
		c.reconnecting = true
		c.silentClose()
	}
	return err
}

func (c *sqlConn) Query(query string, args []driver.Value) (Rows, error) {
	if err := c.EnsureConn(); err != nil {
		return nil, err
	}
	if *c.echo {
		fmt.Fprintln(stderr, ">", query)
	}
	rows, err := c.conn.Query(query, args)
	if errors.Is(err, driver.ErrBadConn) {
		c.reconnecting = true
		c.silentClose()
	}
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows.(sqlRowsI), conn: c}, nil
}

func (c *sqlConn) QueryRow(query string, args []driver.Value) ([]driver.Value, error) {
	rows, _, err := MakeQuery(query, args...)(c)
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

func (c *sqlConn) Close() error {
	c.flushNotices()
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

func (c *sqlConn) silentClose() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

type sqlRowsI interface {
	driver.RowsColumnTypeScanType
	driver.RowsColumnTypeDatabaseTypeName
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
	r.conn.flushNotices()
	err := r.rows.Close()
	if errors.Is(err, driver.ErrBadConn) {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	return err
}

// Next implements the Rows interface.
func (r *sqlRows) Next(values []driver.Value) error {
	err := r.rows.Next(values)
	if errors.Is(err, driver.ErrBadConn) {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = append([]byte{}, b...)
		}
	}
	// After the first row was received, we want to delay all
	// further notices until the end of execution.
	r.conn.delayNotices = true
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

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.rows.ColumnTypeDatabaseTypeName(index)
}

func (r *sqlRows) ColumnTypeNames() []string {
	colTypes := make([]string, len(r.Columns()))
	for i := range colTypes {
		colTypes[i] = r.ColumnTypeDatabaseTypeName(i)
	}
	return colTypes
}

// MakeSQLConn creates a connection object from a connection URL.
// The booleans passed by reference are parameters that
// can be changed in the SQL interactive shell after the connection
// has been created.
// TODO(knz): This should evolve to become methods on the connection
// object.
func MakeSQLConn(
	url string,
	isInteractive bool,
	enableServerExecutionTimings *bool,
	embeddedMode bool,
	echo *bool,
	showTimes *bool,
	verboseTimings *bool,
) Conn {
	return &sqlConn{
		url: url,

		isInteractive:                isInteractive,
		enableServerExecutionTimings: enableServerExecutionTimings,
		embeddedMode:                 embeddedMode,
		echo:                         echo,
		showTimes:                    showTimes,
		verboseTimings:               verboseTimings,
	}
}

// fillPassword is called the first time the server complains that the
// password authentication has failed, if no password was supplied to
// start with. It asks the user for a password interactively.
func (c *sqlConn) fillPassword() error {
	connURL, err := url.Parse(c.url)
	if err != nil {
		return err
	}

	// Password can be safely encrypted, or the user opted in
	// manually to non-encryption. All good.

	pwd, err := security.PromptForPassword()
	if err != nil {
		return err
	}
	connURL.User = url.UserPassword(connURL.User.Username(), pwd)
	c.url = connURL.String()
	c.passwordMissing = false
	return nil
}

// QueryFn is the type of functions produced by MakeQuery.
type QueryFn func(conn Conn) (rows Rows, isMultiStatementQuery bool, err error)

// MakeQuery encapsulates a SQL query and its parameter into a
// function that can be applied to a connection object.
func MakeQuery(query string, parameters ...driver.Value) QueryFn {
	return func(conn Conn) (Rows, bool, error) {
		isMultiStatementQuery := parser.HasMultipleStatements(query)
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
				return nil, isMultiStatementQuery, err
			}
		}
		rows, err := conn.Query(query, parameters)
		return rows, isMultiStatementQuery, err
	}
}

// RunQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
func RunQuery(conn Conn, fn QueryFn, showMoreChars bool) ([]string, [][]string, error) {
	rows, _, err := fn(conn)
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
	_ = conn.Close()
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

// RunQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
func RunQueryAndFormatResults(
	c Conn, w io.Writer, fn QueryFn, tableDisplayFormat TableDisplayFormat, tableBorderMode int,
) (err error) {
	conn := c.(*sqlConn)
	startTime := timeutil.Now()
	rows, isMultiStatementQuery, err := fn(conn)
	if err != nil {
		return handleCopyError(conn, err)
	}
	defer func() {
		closeErr := rows.Close()
		err = errors.CombineErrors(err, closeErr)
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
		reporter, cleanup, err := makeReporter(w, tableDisplayFormat, tableBorderMode)
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

		maybeShowTimes(conn, w, isMultiStatementQuery, startTime, queryCompleteTime)

		if more, err := rows.NextResultSet(); err != nil {
			return err
		} else if !more {
			return nil
		}
	}
}

// maybeShowTimes displays the execution time if show_times has been set.
func maybeShowTimes(
	conn *sqlConn, w io.Writer, isMultiStatementQuery bool, startTime,
	queryCompleteTime time.Time,
) {
	if !*conn.showTimes {
		return
	}

	defer func() {
		// If there was noticeable overhead, let the user know.
		renderDelay := timeutil.Now().Sub(queryCompleteTime)
		if renderDelay >= 1*time.Second && conn.isInteractive {
			fmt.Fprintf(stderr,
				"\nNote: an additional delay of %s was spent formatting the results.\n"+
					"You can use \\set display_format to change the formatting.\n",
				renderDelay)
		}
		// An additional empty line as separator.
		fmt.Fprintln(w)
	}()

	clientSideQueryLatency := queryCompleteTime.Sub(startTime)
	// We don't print timings for multi-statement queries as we don't have an
	// accurate way to measure them currently. See #48180.
	if isMultiStatementQuery {
		// No need to print if no one's watching.
		if conn.isInteractive {
			fmt.Fprintf(stderr, "\nNote: timings for multiple statements on a single line are not supported. See %s.\n",
				build.MakeIssueURL(48180))
		}
		return
	}

	// Print a newline early. This provides a discreet visual
	// feedback that execution finished, and that the next line of
	// output will be a warning or execution time(s).
	fmt.Fprintln(w)

	// We accumulate the timing details into a buffer prior to emitting
	// them to the output stream, so as to avoid interleaving warnings
	// or SQL notices with the full timing string.
	var stats strings.Builder

	// Print a newline so that there is a visual separation between a notice and
	// the timing information.
	fmt.Fprintln(&stats)

	// Suggested by Radu: for sub-second results, show simplified
	// timings in milliseconds.
	unit := "s"
	multiplier := 1.
	precision := 3
	if clientSideQueryLatency.Seconds() < 1 {
		unit = "ms"
		multiplier = 1000.
		precision = 0
	}

	if *conn.verboseTimings {
		fmt.Fprintf(&stats, "Time: %s", clientSideQueryLatency)
	} else {
		// Simplified displays: human users typically can't
		// distinguish sub-millisecond latencies.
		fmt.Fprintf(&stats, "Time: %.*f%s", precision, clientSideQueryLatency.Seconds()*multiplier, unit)
	}

	if !*conn.enableServerExecutionTimings {
		fmt.Fprintln(w, stats.String())
		return
	}

	// If discrete server/network timings are available, also print them.
	parseLat, planLat, execLat, serviceLat, jobsLat, containsJobLat, err := conn.getLastQueryStatistics()
	if err != nil {
		fmt.Fprint(w, stats.String())
		fmt.Fprintf(stderr, "\nwarning: %v", err)
		return
	}

	fmt.Fprint(&stats, " total")

	networkLat := clientSideQueryLatency - (serviceLat + jobsLat)
	// serviceLat can be greater than clientSideQueryLatency for some extremely quick
	// statements (eg. BEGIN). So as to not confuse the user, we attribute all of
	// the clientSideQueryLatency to the network in such cases.
	if networkLat.Seconds() < 0 {
		networkLat = clientSideQueryLatency
	}
	otherLat := serviceLat - parseLat - planLat - execLat
	if *conn.verboseTimings {
		// Only display schema change latency if the server provided that
		// information to not confuse users.
		// TODO(arul): this can be removed in 22.1.
		if containsJobLat {
			fmt.Fprintf(&stats, " (parse %s / plan %s / exec %s / schema change %s / other %s / network %s)",
				parseLat, planLat, execLat, jobsLat, otherLat, networkLat)
		} else {
			fmt.Fprintf(&stats, " (parse %s / plan %s / exec %s / other %s / network %s)",
				parseLat, planLat, execLat, otherLat, networkLat)
		}
	} else {
		// Simplified display: just show the execution/network breakdown.
		//
		// Note: we omit the report details for queries that
		// last for a millisecond or less. This is because for such
		// small queries, the detail is just noise to the human observer.
		sep := " ("
		reportTiming := func(label string, lat time.Duration) {
			fmt.Fprintf(&stats, "%s%s %.*f%s", sep, label, precision, lat.Seconds()*multiplier, unit)
			sep = " / "
		}
		reportTiming("execution", serviceLat+jobsLat)
		reportTiming("network", networkLat)
		fmt.Fprint(&stats, ")")
	}
	fmt.Fprintln(w, stats.String())
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
// If showMoreChars is true, then more characters are not escaped.
func sqlRowsToStrings(rows Rows, showMoreChars bool) ([]string, [][]string, error) {
	cols := getColumnStrings(rows, showMoreChars)
	allRows, err := getAllRowStrings(rows, rows.ColumnTypeNames(), showMoreChars)
	if err != nil {
		return nil, nil, err
	}
	return cols, allRows, nil
}

func getColumnStrings(rows Rows, showMoreChars bool) []string {
	srcCols := rows.Columns()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = FormatVal(c, "NAME", showMoreChars, showMoreChars)
	}
	return cols
}

func getAllRowStrings(rows Rows, colTypes []string, showMoreChars bool) ([][]string, error) {
	var allRows [][]string

	for {
		rowStrings, err := getNextRowStrings(rows, colTypes, showMoreChars)
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

func getNextRowStrings(rows Rows, colTypes []string, showMoreChars bool) ([]string, error) {
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
		rowStrings[i] = FormatVal(v, colTypes[i], showMoreChars, showMoreChars)
	}
	return rowStrings, nil
}

// ParseBool parses a boolean string for use in CLI SQL commands.
// It recognizes booleans in a similar way as 'psql'.
func ParseBool(s string) (bool, error) {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "true", "on", "yes", "1":
		return true, nil
	case "false", "off", "no", "0":
		return false, nil
	default:
		return false, errors.Newf("invalid boolean value %q", s)
	}
}
