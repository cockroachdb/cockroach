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
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/errors"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against a cockroach database.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runTerm),
}

// checkInteractive sets the isInteractive parameter depending on the
// execution environment and the presence of -e flags.
func checkInteractive(stdin *os.File) {
	// We don't consider sessions interactives unless we have a
	// serious hunch they are. For now, only `cockroach sql` *without*
	// `-e` has the ability to input from a (presumably) human user,
	// and we'll also assume that there is no human if the standard
	// input is not terminal-like -- likely redirected from a file,
	// etc.
	cliCtx.IsInteractive = len(sqlCtx.ExecStmts) == 0 && isatty.IsTerminal(stdin.Fd())
}

// getInputFile establishes where we are reading from.
func getInputFile() (cmdIn *os.File, closeFn func(), err error) {
	if sqlCtx.inputFile == "" {
		return os.Stdin, func() {}, nil
	}

	if len(sqlCtx.ExecStmts) != 0 {
		return nil, nil, errors.Newf("unsupported combination: --%s and --%s", cliflags.Execute.Name, cliflags.File.Name)
	}

	f, err := os.Open(sqlCtx.inputFile)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

func runTerm(cmd *cobra.Command, args []string) (resErr error) {
	cmdIn, closeFn, err := getInputFile()
	if err != nil {
		return err
	}
	defer closeFn()

	checkInteractive(cmdIn)

	if cliCtx.IsInteractive {
		// The user only gets to see the welcome message on interactive sessions.
		// Refer to README.md to understand the general design guidelines for
		// help texts.
		const welcomeMessage = `#
# Welcome to the CockroachDB SQL shell.
# All statements must be terminated by a semicolon.
# To exit, type: \q.
#
`
		fmt.Print(welcomeMessage)
	}

	conn, err := makeSQLClient("cockroach sql", useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	return runClient(cmd, conn, cmdIn)
}

func runClient(cmd *cobra.Command, conn clisqlclient.Conn, cmdIn *os.File) error {
	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.EnsureConn(); err != nil {
		return err
	}

	// Enable safe updates, unless disabled.
	setupSafeUpdates(cmd, conn)

	c := clisqlshell.NewShell(&cliCtx.Context, &sqlConnCtx, &sqlExecCtx, &sqlCtx.Context, conn)
	return c.RunInteractive(cmdIn, os.Stdout, stderr)
}

// setupSafeUpdates attempts to enable "safe mode" if the session is
// interactive and the user is not disabling this behavior with
// --safe-updates=false.
func setupSafeUpdates(cmd *cobra.Command, conn clisqlclient.Conn) {
	pf := cmd.Flags()
	vf := pf.Lookup(cliflags.SafeUpdates.Name)

	if !vf.Changed {
		// If `--safe-updates` was not specified, we need to set the default
		// based on whether the session is interactive. We cannot do this
		// earlier, because "session is interactive" depends on knowing
		// whether `-e` is also provided.
		sqlCtx.safeUpdates = cliCtx.IsInteractive
	}

	if !sqlCtx.safeUpdates {
		// nothing to do.
		return
	}

	if err := conn.Exec("SET sql_safe_updates = TRUE", nil); err != nil {
		// We only enable the setting in interactive sessions. Ignoring
		// the error with a warning is acceptable, because the user is
		// there to decide what they want to do if it doesn't work.
		fmt.Fprintf(stderr, "warning: cannot enable safe updates: %v\n", err)
	}
}
