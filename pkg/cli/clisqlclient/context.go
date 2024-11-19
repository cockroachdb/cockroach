// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import "github.com/cockroachdb/cockroach/pkg/cli/clicfg"

// Context represents configuration for establishing SQL
// connections to servers.
// (Note: the execution of SQL queries and presenting results
// is configured via a separate exec configuration.)
//
// Note: when adding new configuration fields, consider adding
// them to the most specific configuration context possible.
type Context struct {
	// CliConfig links this connection context to a CLI configuration
	// environment.
	CliCtx *clicfg.Context

	// Echo, when set, requests that SQL queries sent to the server are
	// also printed out on the client.
	Echo bool

	// DebugMode, when set, overrides the defaults to disable as much
	// "intelligent behavior" in the SQL shell as possible and become
	// more verbose (sets echo).
	DebugMode bool

	// EnableServerExecutionTimings determines whether to request (and
	// display) server-side execution timings in the CLI.
	EnableServerExecutionTimings bool
}

// IsInteractive returns true if the connection configuration
// is for an interactive session. This exposes the field
// from clicfg.Context if available.
func (sqlConnCtx *Context) IsInteractive() bool {
	return sqlConnCtx.CliCtx != nil && sqlConnCtx.CliCtx.IsInteractive
}

// EmbeddedMode returns true if the connection configuration
// is for an embedded session. This exposes the field
// from clicfg.Context if available.
func (sqlConnCtx *Context) EmbeddedMode() bool {
	return sqlConnCtx.CliCtx != nil && sqlConnCtx.CliCtx.EmbeddedMode
}
