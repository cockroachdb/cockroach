// Copyright 2021 The Cockroach Authors.
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
	"database/sql/driver"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
)

// sqlConn re-exposes the original sqlConn prior to moving the code to
// a subpackage, to minimize changes in the commit where the move is
// taking place.
type sqlConn = clisqlclient.Conn

// rowStrIter re-exposes the original rowStrIter prior to moving the
// code to a subpackage, to minimize changes in the commit where the
// move is taking place.
type rowStrIter = clisqlclient.RowStrIter

// initialSQLConnectionError re-exposes the original
// initialSQLConnectionError prior to moving the code to a subpackage,
// to minimize changes in the commit where the move is taking place.
type initialSQLConnectionError = clisqlclient.InitialSQLConnectionError

// makeSQLConn creates a connection object from
// a connection URL.
//
// This is the local variant of the function in the clisqlclient
// package.
func makeSQLConn(url string) *sqlConn {
	return clisqlclient.MakeSQLConn(url,
		cliCtx.isInteractive,
		&sqlCtx.enableServerExecutionTimings,
		sqlCtx.embeddedMode,
		&sqlCtx.echo,
		&sqlCtx.showTimes,
		&sqlCtx.verboseTimings,
	)
}

// PrintQueryOutput takes a list of column names and a list of row
// contents writes a formatted table to 'w'.
//
// This is the local variant of the function in the clisqlclient
// package.
func PrintQueryOutput(w io.Writer, cols []string, allRows rowStrIter) error {
	return clisqlclient.PrintQueryOutput(w, cols, allRows,
		cliCtx.tableDisplayFormat,
		cliCtx.tableBorderMode,
	)
}

// runQueryAndFormatResults takes a 'query' with optional 'parameters'.
// It runs the sql query and writes output to 'w'.
//
// This is the local variant of the function in the clisqlclient
// package.
func runQueryAndFormatResults(conn *sqlConn, w io.Writer, fn clisqlclient.QueryFn) (err error) {
	return clisqlclient.RunQueryAndFormatResults(conn, w, fn,
		cliCtx.tableDisplayFormat,
		cliCtx.tableBorderMode,
	)
}

// NewRowSliceIter is an implementation of the rowStrIter interface and it is
// used when the rows have not been buffered into memory yet and we want to
// stream them to the row formatters as they arrive over the network.
//
// This is the local variant of the function in the clisqlclient
// package.
func NewRowSliceIter(allRows [][]string, align string) clisqlclient.RowStrIter {
	return clisqlclient.NewRowSliceIter(allRows, align)
}

// makeQuery encapsulates a SQL query and its parameter into a
// function that can be applied to a connection object.
//
// This is the local variant of the function in the clisqlclient
// package.
func makeQuery(query string, parameters ...driver.Value) clisqlclient.QueryFn {
	return clisqlclient.MakeQuery(query, parameters...)
}

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
//
// This is the local variant of the function in the clisqlclient
// package.
func runQuery(
	conn *sqlConn, fn clisqlclient.QueryFn, showMoreChars bool,
) ([]string, [][]string, error) {
	return clisqlclient.RunQuery(conn, fn, showMoreChars)
}
