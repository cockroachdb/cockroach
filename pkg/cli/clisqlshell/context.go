// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	democlusterapi "github.com/cockroachdb/cockroach/pkg/cli/democluster/api"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

	// DisableLineEditor, if set, causes the shell to use a dumb line editor
	// (disable the interactive one), which simplifies testing by avoiding
	// escape sequences in the output.
	DisableLineEditor bool

	// ParseURL is a custom URL parser.
	//
	// When left undefined, the code defaults to pgurl.Parse.
	// CockroachDB's own CLI package has a more advanced URL
	// parser that is used instead.
	ParseURL URLParser

	// CertsDir is an extra directory to look for client certs in,
	// when the \c command is used.
	CertsDir string
}

// internalContext represents the internal configuration state of the
// interactive shell. It can be changed via `\set` commands but not
// from the command line and thus needs not be exported to other packages.
type internalContext struct {
	// stdout and stderr are where messages and errors/warnings go.
	stdout *os.File
	stderr *os.File

	// queryOutputFile is the output file configured via \o.
	// This can be the same as stdout (\o without argument).
	// Note: we use .queryOutput for query execution, which
	// is buffered (via queryOutputBuf).
	queryOutputFile *os.File
	queryOutputBuf  *bufio.Writer
	queryOutput     io.Writer

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

	// reflowMaxWidth is the maximum reflow width.
	reflowMaxWidth int

	// reflowAlignMode is the SQL prettify alignment mode.
	reflowAlignMode int

	// reflowCaseMode is the SQL prettify case mode.
	reflowCaseMode int

	// reflowTabWidth is the tab width used by the prettify_statement function.
	reflowTabWidth int

	// current database name, if known. This is maintained on a best-effort basis.
	dbName string

	// hook to run once, then clear, after running the next batch of statements.
	afterRun func()

	// file where the history is saved.
	histFile string

	statementWrappers []statementWrapper

	// state about the current query.
	mu struct {
		syncutil.Mutex
		cancelFn func(ctx context.Context) error
		doneCh   chan struct{}
	}
}

// StatementWrapper allows custom client-side logic to be executed when a statement
// matches a given pattern.
// The cli will call Pattern.FindStringSubmatch on every statement before
// executing it. If it returns non-nil, execution will stop and Wrapper will
// be called with the statement and match data.
type statementWrapper struct {
	Pattern statementType
	Wrapper func(ctx context.Context, statement string, c *cliState) error
}

// AddStatementWrapper adds a StatementWrapper to the specified context.
func (c *internalContext) addStatementWrapper(w statementWrapper) {
	c.statementWrappers = append(c.statementWrappers, w)
}

func (c *internalContext) maybeWrapStatement(
	ctx context.Context, statement string, state *cliState,
) (err error) {
	var s scanner.SQLScanner
	for _, sw := range c.statementWrappers {
		s.Init(statement)
		if sw.Pattern.matches(s) {
			err = errors.CombineErrors(err, sw.Wrapper(ctx, statement, state))
		}
	}
	return
}
