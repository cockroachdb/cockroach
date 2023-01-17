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
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/security/pprompt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/otan/gopgkrb5"
)

func init() {
	// Ensure that the CLI client commands can use GSSAPI authentication.
	pgconn.RegisterGSSProvider(func() (pgconn.GSS, error) { return gopgkrb5.NewGSS() })
}

type sqlConn struct {
	// connCtx links this connection to a connection configuration
	// context, to be specified by the code that instantiates the
	// connection.
	connCtx *Context

	url          string
	conn         *pgx.Conn
	reconnecting bool

	// passwordMissing is true iff the url is missing a password.
	passwordMissing bool

	// alwaysInferResultTypes is true iff the client should always use the
	// underlying driver to infer result types. If it is true, multiple statements
	// in a single string cannot be executed.
	alwaysInferResultTypes bool

	pendingNotices []*pgconn.Notice

	// delayNotices, if set, makes notices accumulate for printing
	// when the SQL execution completes. The default (false)
	// indicates that notices must be printed as soon as they are received.
	// This is used by the Query() interface to avoid interleaving
	// notices with result rows.
	delayNotices bool

	// showLastQueryStatsMode determines how to implement query timings.
	lastQueryStatsMode showLastQueryStatsMode

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

	// infow and errw are the streams where informational, error and
	// warning messages are printed.
	// Echoed queries, if Echo is enabled, are printed to errw too.
	// Notices are also printed to errw.
	infow, errw io.Writer
}

var _ Conn = (*sqlConn)(nil)

// ErrConnectionClosed is returned when an operation fails because the
// connection was closed.
var ErrConnectionClosed = errors.New("connection closed unexpectedly")

// wrapConnError detects TCP EOF errors during the initial SQL handshake.
// These are translated to a message "perhaps this is not a CockroachDB node"
// at the top level.
// EOF errors later in the SQL session should not be wrapped in that way,
// because by that time we've established that the server is indeed a SQL
// server.
func wrapConnError(err error) error {
	errMsg := err.Error()
	// pgconn wraps some of these errors with the private connectError struct.
	isPgconnConnectError := strings.HasPrefix(errMsg, "failed to connect to")
	isEOF := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if errMsg == "EOF" || errMsg == "unexpected EOF" || (isPgconnConnectError && isEOF) {
		return &InitialSQLConnectionError{err}
	}
	return err
}

func (c *sqlConn) flushNotices() {
	for _, notice := range c.pendingNotices {
		clierror.OutputError(c.errw, (*pgconn.PgError)(notice), true /*showSeverity*/, false /*verbose*/)
	}
	c.pendingNotices = nil
	c.delayNotices = false
}

func (c *sqlConn) handleNotice(notice *pgconn.Notice) {
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
	return &driverConnAdapter{c}
}

func (c *sqlConn) Cancel(ctx context.Context) error {
	return c.conn.PgConn().CancelRequest(ctx)
}

// SetCurrentDatabase implements the Conn interface.
func (c *sqlConn) SetCurrentDatabase(dbName string) {
	c.dbName = dbName
}

// SetMissingPassword implements the Conn interface.
func (c *sqlConn) SetMissingPassword(missing bool) {
	c.passwordMissing = missing
}

// SetAlwaysInferResultTypes implements the Conn interface.
func (c *sqlConn) SetAlwaysInferResultTypes(b bool) {
	c.alwaysInferResultTypes = b
}

// EnsureConn (re-)establishes the connection to the server.
func (c *sqlConn) EnsureConn(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}

	if c.reconnecting && c.connCtx.IsInteractive() {
		fmt.Fprintf(c.errw, "warning: connection lost!\n"+
			"opening new connection: all session settings will be lost\n")
	}
	base, err := pgx.ParseConfig(c.url)
	if err != nil {
		return wrapConnError(err)
	}
	// Add a notice handler - re-use the cliOutputError function in this case.
	base.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		c.handleNotice(notice)
	}
	// The default pgx dialer uses a KeepAlive of 5 minutes, which we don't want.
	dialer := &net.Dialer{}
	dialer.Timeout = 0
	if base.ConnectTimeout > 0 {
		dialer.Timeout = base.ConnectTimeout
	}
	base.DialFunc = dialer.DialContext
	// Override LookupFunc to be a no-op, so that the Dialer is responsible for
	// resolving hostnames. This fixes an issue where pgx would error out too
	// quickly if using TLS when an ipv6 address is resolved, but the networking
	// stack does not support ipv6.
	base.LookupFunc = func(ctx context.Context, host string) (addrs []string, err error) {
		return []string{host}, nil
	}
	conn, err := pgx.ConnectConfig(ctx, base)
	if err != nil {
		// Connection failed: if the failure is due to a missing
		// password, we're going to fill the password here.
		pgErr := (*pgconn.PgError)(nil)
		if errors.As(err, &pgErr) && pgErr.Code == pgcode.InvalidPassword.String() && c.passwordMissing {
			if pErr := c.fillPassword(); pErr != nil {
				return errors.CombineErrors(err, pErr)
			}
			// Recurse, once. We recurse to ensure that pq.NewConnector
			// and ConnectorWithNoticeHandler get called with the new URL.
			// The recursion only occurs once because fillPassword()
			// resets c.passwordMissing, so we cannot get into this
			// conditional a second time.
			return c.EnsureConn(ctx)
		}
		// Not a password auth error, or password already set. Simply fail.
		return wrapConnError(err)
	}
	if c.reconnecting && c.dbName != "" {
		// Attempt to reset the current database.
		if _, err := conn.Exec(ctx, `SET DATABASE = $1`, c.dbName); err != nil {
			fmt.Fprintf(c.errw, "warning: unable to restore current database: %v\n", err)
		}
	}
	c.conn = conn
	if err := c.checkServerMetadata(ctx); err != nil {
		err = errors.CombineErrors(err, c.Close())
		return wrapConnError(err)
	}
	c.reconnecting = false
	return nil
}

type showLastQueryStatsMode int

const (
	modeDisabled showLastQueryStatsMode = iota
	modeModern
	modeSimple // Remove this when pre-21.2 compatibility is not needed any more.
)

// tryEnableServerExecutionTimings attempts to check if the server supports the
// SHOW LAST QUERY STATISTICS statements. This allows the CLI client to report
// server side execution timings instead of timing on the client.
func (c *sqlConn) tryEnableServerExecutionTimings(ctx context.Context) error {
	// Starting in v21.2 servers, clients can request an explicit set of
	// values which makes them compatible with any post-21.2 column
	// additions.
	_, err := c.QueryRow(ctx, "SHOW LAST QUERY STATISTICS RETURNING x")
	if err != nil && !clierror.IsSQLSyntaxError(err) {
		return err
	}
	if err == nil {
		c.connCtx.EnableServerExecutionTimings = true
		c.lastQueryStatsMode = modeModern
		return nil
	}
	// Pre-21.2 servers may have SHOW LAST QUERY STATISTICS.
	// Note: this branch is obsolete, remove it when compatibility
	// with pre-21.2 servers is not required any more.
	_, err = c.QueryRow(ctx, "SHOW LAST QUERY STATISTICS")
	if err != nil && !clierror.IsSQLSyntaxError(err) {
		return err
	}
	if err == nil {
		c.connCtx.EnableServerExecutionTimings = true
		c.lastQueryStatsMode = modeSimple
		return nil
	}

	fmt.Fprintln(c.errw, "warning: server does not support query statistics, cannot enable verbose timings")
	c.lastQueryStatsMode = modeDisabled
	c.connCtx.EnableServerExecutionTimings = false
	return nil
}

func (c *sqlConn) GetServerMetadata(
	ctx context.Context,
) (nodeID int32, version, clusterID string, retErr error) {
	// Retrieve the node ID and server build info.
	// Be careful to query against the empty database string, which avoids taking
	// a lease against the current database (in case it's currently unavailable).
	rows, err := c.Query(ctx, `SELECT * FROM "".crdb_internal.node_build_info`)
	if c.conn.IsClosed() {
		return 0, "", "", MarkWithConnectionClosed(err)
	}
	if err != nil {
		return 0, "", "", err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()

	// Read the node_build_info table as an array of strings.
	rowVals, err := getServerMetadataRows(rows)
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
				return 0, "", "", errors.Wrap(err, "incorrect data while retrieving node id")
			}
			nodeID = int32(id)

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

func getServerMetadataRows(rows Rows) (data [][]string, err error) {
	var vals []driver.Value
	cols := rows.Columns()
	if len(cols) > 0 {
		vals = make([]driver.Value, len(cols))
	}
	for {
		err = rows.Next(vals)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rowStrings := make([]string, len(cols))
		for i, v := range vals {
			rowStrings[i] = toString(v)
		}
		data = append(data, rowStrings)
	}

	return data, nil
}

func toString(v driver.Value) string {
	switch x := v.(type) {
	case []byte:
		return fmt.Sprint(string(x))
	default:
		return fmt.Sprint(x)
	}
}

// checkServerMetadata reports the server version and cluster ID
// upon the initial connection or if either has changed since
// the last connection, based on the last known values in the sqlConn
// struct.
func (c *sqlConn) checkServerMetadata(ctx context.Context) error {
	if !c.connCtx.IsInteractive() {
		// Version reporting is just noise if the user is not present to
		// change their mind upon seeing the information.
		return nil
	}
	if c.connCtx.EmbeddedMode() {
		// Version reporting is non-actionable if the user does
		// not have control over how the server and client are run.
		return nil
	}

	// The checks below use statements that don't support being
	// prepared. So disable result type inference for the duration
	// of these checks.
	defer func(prev bool) { c.alwaysInferResultTypes = prev }(c.alwaysInferResultTypes)
	c.alwaysInferResultTypes = false

	_, newServerVersion, newClusterID, err := c.GetServerMetadata(ctx)
	if c.conn.IsClosed() {
		return MarkWithConnectionClosed(err)
	}
	if err != nil {
		// It is not an error that the server version cannot be retrieved.
		fmt.Fprintf(c.errw, "warning: unable to retrieve the server's version: %s\n", err)
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
			fmt.Fprintln(c.infow, "# Client version:", client.Short())
		} else {
			isSame = " (same version as client)"
		}
		fmt.Fprintf(c.infow, "# Server version: %s%s\n", c.serverBuild, isSame)

		sv, err := version.Parse(c.serverVersion)
		if err == nil {
			cv, err := version.Parse(client.Tag)
			if err == nil {
				if sv.Compare(cv) == -1 { // server ver < client ver
					fmt.Fprint(c.errw, "\nwarning: server version older than client! "+
						"proceed with caution; some features may not be available.\n\n")
				}
			}
		}
	}

	// Report the cluster ID only if it it could be fetched
	// successfully, and it has changed since the last connection.
	if old := c.clusterID; newClusterID != c.clusterID {
		label := ""
		if old != "" {
			label = "New "
			fmt.Fprintf(c.errw, "\nwarning: the cluster ID has changed!\n# Previous ID: %s\n", old)
		}
		c.clusterID = newClusterID
		fmt.Fprintf(c.infow, "# %sCluster ID: %v\n", label, c.clusterID)
		if c.clusterOrganization != "" {
			fmt.Fprintln(c.infow, "# Organization:", c.clusterOrganization)
		}
	}
	// Try to enable server execution timings for the CLI to display if
	// supported by the server.
	return c.tryEnableServerExecutionTimings(ctx)
}

// GetServerInfo returns a copy of the remote server details.
func (c *sqlConn) GetServerInfo() ServerInfo {
	return ServerInfo{
		ServerExecutableVersion: c.serverBuild,
		ClusterID:               c.clusterID,
		Organization:            c.clusterOrganization,
	}
}

// GetServerValue retrieves the first driverValue returned by the
// given sql query. If the query fails or does not return a single
// column, `false` is returned in the second result.
func (c *sqlConn) GetServerValue(
	ctx context.Context, what, sql string,
) (retVal driver.Value, retOk bool) {
	rows, err := c.Query(ctx, sql)
	if err != nil {
		fmt.Fprintf(c.errw, "warning: error retrieving the %s: %v\n", what, err)
		return nil, false
	}
	defer func() {
		if err := rows.Close(); err != nil {
			fmt.Fprintf(c.errw, "warning: error retrieving the %s: %v\n", what, err)
			retVal, retOk = nil, false
		}
	}()

	if len(rows.Columns()) == 0 {
		fmt.Fprintf(c.errw, "warning: cannot get the %s\n", what)
		return nil, false
	}

	dbVals := make([]driver.Value, len(rows.Columns()))

	err = rows.Next(dbVals)
	if err != nil {
		fmt.Fprintf(c.errw, "warning: invalid %s: %v\n", what, err)
		return nil, false
	}

	return dbVals[0], true
}

func (c *sqlConn) GetLastQueryStatistics(ctx context.Context) (results QueryStats, resErr error) {
	if !c.connCtx.EnableServerExecutionTimings || c.lastQueryStatsMode == modeDisabled {
		return results, nil
	}

	stmt := `SHOW LAST QUERY STATISTICS RETURNING parse_latency, plan_latency, exec_latency, service_latency, post_commit_jobs_latency`
	if c.lastQueryStatsMode == modeSimple {
		// Note: remove this case when compatibility with pre-21.2 clients
		// is not needed any more.
		stmt = `SHOW LAST QUERY STATISTICS`
	}

	vals, cols, err := c.queryRowInternal(ctx, stmt, nil)
	if err != nil {
		return results, err
	}

	// The following code extracts the values from whichever set of
	// columns was reported by the server. This ensures compatibility
	// with pre-21.2 servers which would return 4 or 5 columns
	// depending on version.
	for i, c := range cols {
		var dst *QueryStatsDuration
		switch c {
		case "parse_latency":
			dst = &results.Parse
		case "plan_latency":
			dst = &results.Plan
		case "exec_latency":
			dst = &results.Exec
		case "service_latency":
			dst = &results.Service
		case "post_commit_jobs_latency":
			dst = &results.PostCommitJobs
		}
		if vals[i] != nil {
			rawVal := toString(vals[i])
			parsedLat, err := stringToDuration(rawVal)
			if err != nil {
				return results, errors.Wrapf(err, "invalid interval value in SHOW LAST QUERY STATISTICS, column %q", c)
			}
			dst.Valid = true
			dst.Value = parsedLat
		}
	}

	results.Enabled = true
	return results, nil
}

// ExecTxn runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// NOTE: the supplied closure should not have external side
// effects beyond changes to the database.
func (c *sqlConn) ExecTxn(
	ctx context.Context, fn func(context.Context, TxBoundConn) error,
) (err error) {
	if err := c.Exec(ctx, `BEGIN`); err != nil {
		return err
	}
	return crdb.ExecuteInTx(ctx, sqlTxnShim{c}, func() error {
		return fn(ctx, c)
	})
}

func (c *sqlConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	if err := c.EnsureConn(ctx); err != nil {
		return err
	}
	if c.connCtx.Echo {
		fmt.Fprintln(c.errw, ">", query)
	}
	_, err := c.conn.Exec(ctx, query, args...)
	c.flushNotices()
	if c.conn.IsClosed() {
		c.reconnecting = true
		c.silentClose()
		return MarkWithConnectionClosed(err)
	}
	return err
}

func (c *sqlConn) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	if err := c.EnsureConn(ctx); err != nil {
		return nil, err
	}
	if c.connCtx.Echo {
		fmt.Fprintln(c.errw, ">", query)
	}

	// If there are placeholder args, then we must use a prepared statement,
	// which is only possible using pgx.Conn.
	// Or, if alwaysInferResultTypes is set, then we must use pgx.Conn so the
	// result types are automatically inferred.
	if len(args) > 0 || c.alwaysInferResultTypes {
		rows, err := c.conn.Query(ctx, query, args...)
		if c.conn.IsClosed() {
			c.reconnecting = true
			c.silentClose()
			return nil, MarkWithConnectionClosed(err)
		}
		if err != nil {
			return nil, err
		}
		return &sqlRows{rows: rows, connInfo: c.conn.ConnInfo(), conn: c}, nil
	}

	// Otherwise, we use pgconn. This allows us to add support for multiple
	// queries in a single string, which wouldn't be possible at the pgx level.
	multiResultReader := c.conn.PgConn().Exec(ctx, query)
	if c.conn.IsClosed() {
		c.reconnecting = true
		c.silentClose()
		return nil, MarkWithConnectionClosed(multiResultReader.Close())
	}
	rs := &sqlRowsMultiResultSet{
		rows:     multiResultReader,
		connInfo: c.conn.ConnInfo(),
		conn:     c,
	}
	if _, err := rs.NextResultSet(); err != nil {
		return nil, err
	}
	return rs, nil
}

func (c *sqlConn) QueryRow(
	ctx context.Context, query string, args ...interface{},
) ([]driver.Value, error) {
	results, _, err := c.queryRowInternal(ctx, query, args)
	return results, err
}

func (c *sqlConn) queryRowInternal(
	ctx context.Context, query string, args []interface{},
) (vals []driver.Value, colNames []string, resErr error) {
	rows, _, err := MakeQuery(query, args...)(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	defer func() { resErr = errors.CombineErrors(resErr, rows.Close()) }()
	colNames = rows.Columns()
	vals = make([]driver.Value, len(colNames))
	err = rows.Next(vals)

	// Assert that there is just one row.
	if err == nil {
		nextVals := make([]driver.Value, len(colNames))
		nextErr := rows.Next(nextVals)
		if nextErr != io.EOF {
			if nextErr != nil {
				return nil, nil, nextErr
			}
			return nil, nil, errors.AssertionFailedf("programming error: %q: expected just 1 row of result, got more", query)
		}
	}

	return vals, colNames, err
}

func (c *sqlConn) Close() error {
	c.flushNotices()
	if c.conn != nil {
		err := c.conn.Close(context.Background())
		if err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

func (c *sqlConn) silentClose() {
	if c.conn != nil {
		_ = c.conn.Close(context.Background())
		c.conn = nil
	}
}

// MakeSQLConn creates a connection object from a connection URL.
// Server informational messages are printed to 'w'.
// Errors or warnings, when they do not block an API call, are printed to 'ew'.
// Echoed queries, when Echo is enabled, are also printed to 'ew'.
// Server out-of-band notices are also printed to 'ew'.
func (connCtx *Context) MakeSQLConn(w, ew io.Writer, url string) Conn {
	return &sqlConn{
		connCtx: connCtx,
		url:     url,
		infow:   w,
		errw:    ew,
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

	// Tell the user where we are connecting to, for context.
	fmt.Fprintf(c.infow, "Connecting to server %q as user %q.\n",
		connURL.Host,
		connURL.User.Username())

	pwd, err := pprompt.PromptForPassword("" /* prompt */)
	if err != nil {
		return err
	}
	connURL.User = url.UserPassword(connURL.User.Username(), pwd)
	c.url = connURL.String()
	c.passwordMissing = false
	return nil
}

// MarkWithConnectionClosed is a mix of errors.CombineErrors() and errors.Mark().
// If err is nil, the result is like errors.CombineErrors().
// If err is non-nil, the result is that of errors.Mark(), with the error
// message added as a prefix.
//
// In both cases, errors.Is(..., ErrConnectionClosed) returns true on the
// result.
func MarkWithConnectionClosed(err error) error {
	if err == nil {
		return ErrConnectionClosed
	}
	// Disable the linter since we are intentionally adding the error text.
	// nolint:errwrap
	errWithMsg := errors.WithMessagef(err, "%v", ErrConnectionClosed)
	return errors.Mark(errWithMsg, ErrConnectionClosed)
}
