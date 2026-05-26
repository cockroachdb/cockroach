// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import "github.com/cockroachdb/cockroach/pkg/cli/clicfg"

// Context represents configuration for running SQL query
// and presenting results to the screen.
// (Note: the execution of the interactive SQL shell
// is configured via a separate shell configuration.)
//
// Note: when adding new configuration fields, consider adding
// them to the most specific configuration context possible.
type Context struct {
	// CliCtx links this connection context to a CLI configuration
	// environment.
	CliCtx *clicfg.Context

	// TerminalOutput indicates whether output is going to a terminal,
	// that is, it is not going to a file, another program for automated
	// processing, etc.: the standard output is a terminal.
	//
	// Refer to cli/README.md to understand the general design guidelines for
	// CLI utilities with terminal vs non-terminal output.
	TerminalOutput bool

	// TableDisplayFormat indicates how to format result tables.
	TableDisplayFormat TableDisplayFormat

	// TableBorderMode indicates how to format tables when the display
	// format is 'table'. This exists for compatibility
	// with psql: https://www.postgresql.org/docs/12/app-psql.html
	TableBorderMode int

	// ShowTimes indicates whether to display query times after each result line.
	ShowTimes bool

	// VerboseTimings determines whether to show raw durations when reporting query latencies..
	VerboseTimings bool
}

// IsInteractive returns true if the connection configuration
// is for an interactive session. This exposes the field
// from clicfg.Context if available.
func (sqlExecCtx *Context) IsInteractive() bool {
	return sqlExecCtx.CliCtx != nil && sqlExecCtx.CliCtx.IsInteractive
}
