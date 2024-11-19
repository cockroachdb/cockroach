// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"context"
	"database/sql/driver"
	"io"
	"time"
)

// Conn represents a connection to a SQL server.
type Conn interface {
	// The user code is required to call the Close() method when the
	// connection is not used any more.
	Close() error

	// EnsureConn (re-)establishes the connection to the server.
	EnsureConn(ctx context.Context) error

	// Exec executes a statement.
	Exec(ctx context.Context, query string, args ...interface{}) error

	// ExecWithRowsAffected is like Exec but returns the number of
	// affected rows.
	ExecWithRowsAffected(ctx context.Context, query string, args ...interface{}) (int64, error)

	// Query returns one or more SQL statements and returns the
	// corresponding result set(s).
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)

	// QueryRow execute a SQL query returning exactly one row
	// and retrieves the returned values. An error is returned
	// if the query returns zero or more than one row.
	QueryRow(ctx context.Context, query string, args ...interface{}) ([]driver.Value, error)

	// ExecTxn runs fn inside a transaction and retries it as needed.
	ExecTxn(ctx context.Context, fn func(context.Context, TxBoundConn) error) error

	// GetLastQueryStatistics returns the detailed latency stats for the
	// last executed query, if supported by the server and enabled by
	// configuration.
	GetLastQueryStatistics(ctx context.Context) (result QueryStats, err error)

	// SetURL changes the URL field in the connection object, so that the
	// next connection (re-)establishment will use the new URL.
	SetURL(url string)

	// GetURL return the current connection URL.
	GetURL() string

	// SetCurrentDatabase sets the current database name, so that the
	// connection can preserve the current database in case of
	// auto-reconnects.
	//
	// Note that this is only used for auto-reconnects after the initial
	// connection. During the initial connection, the database name in
	// the URL is used. During auto-reconnects, the configured current
	// database prevails and the URL database name is ignored.
	SetCurrentDatabase(dbName string)

	// SetMissingPassword configures the password missing flag,
	// which indicates whether to prompt for a password if the
	// server requests one.
	SetMissingPassword(missing bool)

	// SetAlwaysInferResultTypes configures the alwaysInferResultTypes flag, which
	// determines if the client should use the underlying driver to infer result
	// types. It returns a method that can be used to reset the configuration to
	// its previous value.
	SetAlwaysInferResultTypes(b bool) func()

	// GetServerMetadata returns details about the CockroachDB node
	// this connection is connected to.
	GetServerMetadata(ctx context.Context) (
		nodeID int32,
		version, clusterID string,
		err error,
	)

	// GetServerValue retrieves the first driver.Value returned by the
	// given sql query. If the query fails or does not return a single
	// column, `false` is returned in the second result.
	//
	// The what argument is a descriptive label for the value being
	// retrieved, for inclusion inside warning or error message.
	// The sql argument is the SQL query to use to retrieve the value.
	GetServerValue(ctx context.Context, what, sql string) (driver.Value, bool)

	// GetDriverConn exposes the underlying driver connection object
	// for use by the cli package.
	GetDriverConn() DriverConn

	// Cancel sends a query cancellation request to the server.
	Cancel(ctx context.Context) error

	// GetServerInfo retrieves details about the remote server. Some of
	// the fields may be empty if there were errors while retrieving
	// them when the connection was established.
	GetServerInfo() ServerInfo
}

// ServerInfo describes the remote server.
type ServerInfo struct {
	// ServerExecutableVersion is the build version string of the remote server.
	ServerExecutableVersion string
	// ClusterID is the ID of the cluste the remote server is a part of.
	ClusterID string
	// Organization is the cluster organization of the remote server.
	Organization string
	// VirtualClusterName is the name of the virtual cluster the SQL client is
	// connected to. Empty if no application VCs have been defined.
	VirtualClusterName string
}

// Rows describes a result set.
type Rows interface {
	driver.Rows

	// The caller must call Close() when done with the
	// result and check the error.
	Close() error

	// Columns returns the column labels of the current result set.
	// The implementation of this method should cache the result so that the
	// result does not need to be constructed on each invocation.
	Columns() []string

	// ColumnTypeDatabaseTypeName returns the database type name
	// of the column at the given column index.
	ColumnTypeDatabaseTypeName(index int) string

	// Tag retrieves the statement tag for the current result set.
	Tag() (CommandTag, error)

	// Next populates values with the next row of results. []byte values are copied
	// so that subsequent calls to Next and Close do not mutate values. This
	// makes it slower than theoretically possible but the safety concerns
	// (since this is unobvious and unexpected behavior) outweigh.
	Next(values []driver.Value) error

	// NextResultSet prepares the next result set for reading.
	// Returns false if there is no more result set to read.
	//
	// TODO(mjibson): clean this up after 1.8 is released.
	NextResultSet() (bool, error)
}

// CommandTag represents the result of a SQL command.
type CommandTag interface {
	RowsAffected() int64
	String() string
}

// QueryStatsDuration represents a duration value retrieved by
// GetLastQueryStatistics.
type QueryStatsDuration struct {
	// Value is the duration statistic.
	Value time.Duration
	// Valid is false if the server does not know how to compute this
	// duration.
	Valid bool
}

// QueryStats is the result package for GetLastQueryStatistics.
type QueryStats struct {
	// Parse is the parsing time.
	Parse QueryStatsDuration
	// Plan is the planning time.
	Plan QueryStatsDuration
	// Exec is the execution time.
	Exec QueryStatsDuration
	// Service is the total server-side latency.
	Service QueryStatsDuration
	// PostCommitJobs is the post-commit job execution latency.
	PostCommitJobs QueryStatsDuration
	// Enabled is false if statistics retrieval was disabled.
	Enabled bool
}

// TxBoundConn is the type of a connection object
// visible to the closure passed to (Conn).ExecTxn.
type TxBoundConn interface {
	// Exec executes a statement inside the transaction.
	Exec(ctx context.Context, query string, args ...interface{}) error

	// Query returns one or more SQL statements and returns the
	// corresponding result set(s).
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
}

// DriverConn is the type of the connection object returned by
// (Conn).GetDriverConn(). It gives access to the underlying sql
// driver.
type DriverConn interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) error
	ExecWithRowsAffected(ctx context.Context, query string, args ...interface{}) (int64, error)
	CopyFrom(ctx context.Context, reader io.Reader, query string) (int64, error)
	CopyTo(ctx context.Context, w io.Writer, query string) error
}

type driverConnAdapter struct {
	c *sqlConn
}

var _ DriverConn = (*driverConnAdapter)(nil)

func (d *driverConnAdapter) Query(
	ctx context.Context, query string, args ...interface{},
) (driver.Rows, error) {
	rows, err := d.c.Query(ctx, query, args...)
	return driver.Rows(rows), err
}

func (d *driverConnAdapter) Exec(ctx context.Context, query string, args ...interface{}) error {
	return d.c.Exec(ctx, query, args...)
}

func (d *driverConnAdapter) ExecWithRowsAffected(
	ctx context.Context, query string, args ...interface{},
) (int64, error) {
	return d.c.ExecWithRowsAffected(ctx, query, args...)
}

func (d *driverConnAdapter) CopyFrom(
	ctx context.Context, reader io.Reader, query string,
) (int64, error) {
	cmdTag, err := d.c.conn.PgConn().CopyFrom(ctx, reader, query)
	if err != nil {
		return -1, err
	}
	return cmdTag.RowsAffected(), nil
}

func (d *driverConnAdapter) CopyTo(ctx context.Context, w io.Writer, query string) error {
	_, err := d.c.conn.PgConn().CopyTo(ctx, w, query)
	return err
}
