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
	"io"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
)

// makeSQLConn creates a connection object from
// a connection URL.
//
// This is the local variant of the function in the clisqlclient
// package.
func makeSQLConn(url string) clisqlclient.Conn {
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
func PrintQueryOutput(w io.Writer, cols []string, allRows clisqlclient.RowStrIter) error {
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
func runQueryAndFormatResults(
	conn clisqlclient.Conn, w io.Writer, fn clisqlclient.QueryFn,
) (err error) {
	return clisqlclient.RunQueryAndFormatResults(conn, w, fn,
		cliCtx.tableDisplayFormat,
		cliCtx.tableBorderMode,
	)
}
