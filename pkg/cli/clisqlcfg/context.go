// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clisqlcfg defines configuration settings and mechanisms for
// instances of the SQL shell.
package clisqlcfg

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/errors"
	isatty "github.com/mattn/go-isatty"
)

// Context represents the configuration of a SQL shell instance.
type Context struct {
	// CliCtx is the CLI configuration.
	CliCtx *clicfg.Context
	// ConnCtx is the connection configuration.
	ConnCtx *clisqlclient.Context
	// ExecCtx is the query execution / result formatting configuration.
	ExecCtx *clisqlexec.Context
	// ShellCtx is the interactive shell configuration.
	ShellCtx clisqlshell.Context

	// ApplicationName is the application_name to use if not
	// provided in the connection URL.
	ApplicationName string

	// Database is the database to use if not provided in the
	// connection URL.
	Database string

	// User is the user to use if not provided in the connection URL.
	User string

	// ConnectTimeout is the connection timeout in seconds,
	// if the connect_timeout is not provided in the connection URL.
	ConnectTimeout int

	// CmdOut is where the results and informational messages are
	// emitted.
	CmdOut *os.File

	// CmdErr is where errors, warnings and notices are printed.
	CmdErr *os.File

	// InputFile is the file to read from.
	// If empty, os.Stdin is used.
	InputFile string

	// SafeUpdates indicates whether to set sql_safe_updates in the CLI
	// shell prior to running it.
	// If the boolean is left unspecified, the default depends
	// on whether the session is interactive.
	SafeUpdates OptBool

	// ReadOnly indicates where to set default_transaction_read_only in the
	// CLI shell prior to running it.
	ReadOnly bool

	// The following fields are populated during Open().
	opened bool
	cmdIn  *os.File
}

// LoadDefaults loads default values.
func (c *Context) LoadDefaults(cmdOut, cmdErr *os.File) {
	*c = Context{CliCtx: c.CliCtx, ConnCtx: c.ConnCtx, ExecCtx: c.ExecCtx}
	c.ExecCtx.TerminalOutput = isatty.IsTerminal(cmdOut.Fd())
	c.ExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTSV
	c.ExecCtx.TableBorderMode = 0 /* no outer lines + no inside row lines */
	if c.ExecCtx.TerminalOutput {
		// If a human is seeing results, use a tabular result format.
		c.ExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTable
	}
	if cmdOut == nil {
		cmdOut = os.Stdout
	}
	if cmdErr == nil {
		cmdErr = os.Stderr
	}
	c.CmdOut = cmdOut
	c.CmdErr = cmdErr
}

// Open binds the context to its input file/stream.
// This should be called after customizations have been
// populated into the configuration fields.
// The specified input stream is only used if the configuration
// does not otherwise specify a file to read from.
func (c *Context) Open(defaultInput *os.File) (closeFn func(), err error) {
	if c.opened {
		return nil, errors.AssertionFailedf("programming error: Open called twice")
	}

	c.cmdIn, closeFn, err = c.getInputFile(defaultInput)
	if err != nil {
		return nil, err
	}
	c.checkInteractive()
	c.opened = true
	return closeFn, err
}

// getInputFile establishes where we are reading from.
func (c *Context) getInputFile(defaultIn *os.File) (cmdIn *os.File, closeFn func(), err error) {
	if c.InputFile == "" {
		return defaultIn, func() {}, nil
	}

	if len(c.ShellCtx.ExecStmts) != 0 {
		return nil, nil, errors.New("cannot specify both an input file and discrete statements")
	}

	f, err := os.Open(c.InputFile)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

// checkInteractive sets the isInteractive parameter depending on the
// execution environment and the presence of -e flags.
func (c *Context) checkInteractive() {
	// We don't consider sessions interactive unless we have a
	// serious hunch they are. For now, only `cockroach sql` *without*
	// `-e` has the ability to input from a (presumably) human user,
	// and we'll also assume that there is no human if the standard
	// input is not terminal-like -- likely redirected from a file,
	// etc.
	c.CliCtx.IsInteractive = len(c.ShellCtx.ExecStmts) == 0 && isatty.IsTerminal(c.cmdIn.Fd())
}

// MakeConn provides a shorthand interface to ConnCtx.MakeConn.
func (c *Context) MakeConn(url string) (clisqlclient.Conn, error) {
	baseURL, err := pgurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if c.Database != "" {
		baseURL.WithDefaultDatabase(c.Database)
	}

	baseURL.WithDefaultUsername(c.User)

	// Load the application name. It's not a command-line flag, so
	// anything already in the URL should take priority.
	if prevAppName := baseURL.GetOption("application_name"); prevAppName == "" && c.ApplicationName != "" {
		_ = baseURL.SetOption("application_name", c.ApplicationName)
	}

	// Set a connection timeout if none is provided already. This
	// ensures that if the server was not initialized or there is some
	// network issue, the client will not be left to hang forever.
	//
	// This is a lib/pq feature.
	if baseURL.GetOption("connect_timeout") == "" && c.ConnectTimeout != 0 {
		_ = baseURL.SetOption("connect_timeout", strconv.Itoa(c.ConnectTimeout))
	}

	usePw, pwdSet, _ := baseURL.GetAuthnPassword()
	url = baseURL.ToPQ().String()

	conn := c.ConnCtx.MakeSQLConn(c.CmdOut, c.CmdErr, url)
	conn.SetMissingPassword(!usePw || !pwdSet)

	return conn, nil
}

// Run executes the SQL shell.
func (c *Context) Run(conn clisqlclient.Conn) error {
	if !c.opened {
		return errors.AssertionFailedf("programming error: Open not called yet")
	}

	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.EnsureConn(); err != nil {
		return err
	}

	c.maybeSetSafeUpdates(conn)
	if err := c.maybeSetReadOnly(conn); err != nil {
		return err
	}

	shell := clisqlshell.NewShell(c.CliCtx, c.ConnCtx, c.ExecCtx, &c.ShellCtx, conn)
	return shell.RunInteractive(c.cmdIn, c.CmdOut, c.CmdErr)
}

// maybeSetSafeUpdates sets the session variable for safe updates to true
// if the flag is specified or this is an interactive session. It prints
// but does not do anything drastic if an error occurs.
func (c *Context) maybeSetSafeUpdates(conn clisqlclient.Conn) {
	hasSafeUpdates, safeUpdates := c.SafeUpdates.Get()
	if !hasSafeUpdates {
		safeUpdates = c.CliCtx.IsInteractive
	}
	if safeUpdates {
		if err := conn.Exec(context.Background(),
			"SET sql_safe_updates = TRUE"); err != nil {
			// We only enable the setting in interactive sessions. Ignoring
			// the error with a warning is acceptable, because the user is
			// there to decide what they want to do if it doesn't work.
			fmt.Fprintf(c.CmdErr, "warning: cannot enable safe updates: %v\n", err)
		}
	}
}

// maybeSetReadOnly sets the session variable default_transaction_read_only to
// true if the user has requested it.
func (c *Context) maybeSetReadOnly(conn clisqlclient.Conn) error {
	if !c.ReadOnly {
		return nil
	}
	return conn.Exec(context.Background(),
		"SET default_transaction_read_only = TRUE")
}
