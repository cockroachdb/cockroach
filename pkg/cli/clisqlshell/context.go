// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"os"
	"time"

	democlusterapi "github.com/cockroachdb/cockroach/pkg/cli/democluster/api"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Context represents the external configuration of the interactive
// SQL shell.
type Context struct {
	// SetStmts is a list of \set commands to execute before entering the sql shell.
	SetStmts StatementsValue

	// ExecStmts is a list of statements to execute.
	// Only valid if inputFile is empty.
	ExecStmts StatementsValue

	// RepeatDelay indicates that the execStmts should be "watched"
	// at the specified time interval. Zero disables
	// the watch.
	RepeatDelay time.Duration

	// DemoCluster is the interface to the in-memory cluster for the
	// `demo` command, if that is the command being run.
	DemoCluster democlusterapi.DemoCluster

	// ParseURL is a custom URL parser.
	//
	// When left undefined, the code defaults to pgurl.Parse.
	// CockroachDB's own CLI package has a more advanced URL
	// parser that is used instead.
	ParseURL URLParser
}

// internalContext represents the internal configuration state of the
// interactive shell. It can be changed via `\set` commands but not
// from the command line and thus needs not be exported to other packages.
type internalContext struct {
	// stdout and stderr are where messages and errors/warnings go.
	stdout *os.File
	stderr *os.File

	// quitAfterExecStmts tells the shell whether to quit
	// after processing the execStmts.
	quitAfterExecStmts bool

	// Determines whether to stop the client upon encountering an error.
	errExit bool

	// Determines whether to perform client-side syntax checking.
	checkSyntax bool

	// autoTrace, when non-empty, encloses the executed statements
	// by suitable SET TRACING and SHOW TRACE FOR SESSION statements.
	autoTrace string

	// The string used to produce the value of fullPrompt.
	customPromptPattern string

	// current database name, if known. This is maintained on a best-effort basis.
	dbName string

	// state about the current query.
	mu struct {
		syncutil.Mutex
		cancelFn func()
		doneCh   chan struct{}
	}
}
