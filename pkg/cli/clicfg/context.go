// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clicfg

// Context represents configuration common to all CLI commands.
//
// Note: when adding new configuration fields, consider adding
// them to the most specific configuration context possible.
// This one is not a dumping ground.
type Context struct {
	// IsInteractive indicates whether the session is interactive, that
	// is, the commands executed are extremely likely to be *input* from
	// a human user: the standard input is a terminal and `-e` was not
	// used (the shell has a prompt).
	//
	// Refer to cli/README.md to understand the general design
	// guidelines for CLI utilities with interactive vs non-interactive
	// input.
	IsInteractive bool

	// EmbeddedMode, when set, reduces the amount of informational
	// messages printed out to exclude details that are not under user's
	// control when the client command is run by a playground
	// environment.
	EmbeddedMode bool
}
