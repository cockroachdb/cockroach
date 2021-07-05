// Copyright 2021 The Cockroach Authors.
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
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Conn represents a connection to a SQL server.
type Conn = sqlConn

// QueryFn is the type of a callback that can be applied to
// a Conn object to produce tabular results.
type QueryFn = queryFunc

// RowStrIter is an iterator interface for the PrintQueryOutput function. It is
// used so that results can be streamed to the row formatters as they arrive
// to the CLI.
type RowStrIter = rowStrIter

// InitialSQLConnnectionError exports initialSQLConnectionError for
// use inside the cli package.
type InitialSQLConnectionError = initialSQLConnectionError

// stderr aliases log.OrigStderr; we use an alias here so that tests
// in this package can redirect the output of CLI commands to stdout
// to be captured.
var stderr = log.OrigStderr

// TestingSetStderr is exported for use in tests.
func TestingSetStderr(newStderr *os.File) {
	stderr = newStderr
}

// GetURL exposes the URL field in the connection object for use by
// the cli package.
func (c *sqlConn) GetURL() string {
	return c.url
}

// SetURL changes the URL field in the connection object, so
// that the next connection establishment will use the new URL.
func (c *sqlConn) SetURL(url string) {
	c.url = url
}

// GetDriverConn exposes the underlying SQL driver connection object
// for use by the cli package.
func (c *sqlConn) GetDriverConn() sqlConnI {
	return c.conn
}

// SetCurrentDatabase sets the current database name, so that the
// connection can preserve the current database in case of reconnects.
func (c *sqlConn) SetCurrentDatabase(dbName string) {
	c.dbName = dbName
}

// SetMissingPassword configures the password missing flag,
// which indicates whether to prompt for a password if the
// server requests one.
func (c *sqlConn) SetMissingPassword(missing bool) {
	c.passwordMissing = missing
}
