// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// cockroach-sql is an entry point for a CockroachDB binary that only
// includes the SQL shell and does not include any server components.
// It also does not include CCL features.
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var url = "postgresql://localhost:26257/defaultdb"
var cfg = func() *clisqlcfg.Context {
	cliCtx := &clicfg.Context{}
	c := &clisqlcfg.Context{
		CliCtx:  cliCtx,
		ConnCtx: &clisqlclient.Context{CliCtx: cliCtx},
		ExecCtx: &clisqlexec.Context{CliCtx: cliCtx},
	}
	c.LoadDefaults(os.Stdout, os.Stderr)
	return c
}()

func main() {
	// TODO(knz): We should deprecate auto-connecting to 'defaultdb'
	// and instead do something like psql, i.e. use the current
	// unix username as default target database.
	cfg.Database = "defaultdb"
	// TODO(knz): We should deprecate auto-connecting as 'root'
	// and instead do something like psql, i.e. use the current
	// unix username to log into the database.
	cfg.User = "root"
	cfg.ApplicationName = "$ cockroach sql"
	cfg.ConnectTimeout = 15

	f := sqlCmd.PersistentFlags()
	f.StringVar(&url, cliflags.URL.Name, url, "Connection URL.")
	f.StringVarP(&cfg.Database, cliflags.Database.Name, cliflags.Database.Shorthand, cfg.Database, cliflags.Database.Description)
	f.StringVarP(&cfg.User, cliflags.User.Name, cliflags.User.Shorthand, cfg.User, cliflags.User.Description)
	f.VarP(&cfg.ShellCtx.ExecStmts, cliflags.Execute.Name, cliflags.Execute.Shorthand, cliflags.Execute.Description)
	f.StringVarP(&cfg.InputFile, cliflags.File.Name, cliflags.File.Shorthand, cfg.InputFile, cliflags.File.Description)
	f.DurationVar(&cfg.ShellCtx.RepeatDelay, cliflags.Watch.Name, cfg.ShellCtx.RepeatDelay, cliflags.Watch.Description)
	f.Var(&cfg.SafeUpdates, cliflags.SafeUpdates.Name, cliflags.SafeUpdates.Description)
	f.Lookup(cliflags.SafeUpdates.Name).NoOptDefVal = "true" // allow the flag to not be given any value
	f.BoolVar(&cfg.ConnCtx.DebugMode, cliflags.CliDebugMode.Name, cfg.ConnCtx.DebugMode, cliflags.CliDebugMode.Description)
	f.BoolVar(&cfg.ConnCtx.Echo, cliflags.EchoSQL.Name, cfg.ConnCtx.Echo, cliflags.EchoSQL.Description)

	errCode := exit.Success()
	if err := sqlCmd.Execute(); err != nil {
		clierror.OutputError(os.Stderr, err, true /*showSeverity*/, false /*verbose*/)
		// Finally, extract the error code, as optionally specified
		// by the sub-command.
		errCode = exit.UnspecifiedError()
		var cliErr *clierror.Error
		if errors.As(err, &cliErr) {
			errCode = cliErr.GetExitCode()
		}
	}
	exit.WithCode(errCode)
}

var sqlCmd = &cobra.Command{
	Use:   "sql [[--url] <url>] [-f <inputfile>] [-e <stmt>]",
	Short: "start the SQL shell",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runSQL,
	// Disable automatic printing of usage information whenever an error
	// occurs. Many errors are not the result of a bad command invocation,
	// e.g. attempting to start a node on an in-use port, and printing the
	// usage information in these cases obscures the cause of the error.
	// Commands should manually print usage information when the error is,
	// in fact, a result of a bad invocation, e.g. too many arguments.
	SilenceUsage: true,
	// Disable automatic printing of the error. We want to also print
	// details and hints, which cobra does not do for us. Instead
	// we do the printing in Main().
	SilenceErrors: true,
}

func runSQL(_ *cobra.Command, args []string) (resErr error) {
	if url == "" && len(args) > 0 {
		url = args[0]
	}
	if url == "" {
		return errors.New("no connection URL specified")
	}

	closeFn, err := cfg.Open(os.Stdin)
	if err != nil {
		return err
	}
	defer closeFn()

	if cfg.CliCtx.IsInteractive {
		const welcomeMessage = `#
# Welcome to the CockroachDB SQL shell.
# All statements must be terminated by a semicolon.
# To exit, type: \q.
#
`
		fmt.Print(welcomeMessage)
	}

	conn, err := cfg.MakeConn(url)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	return cfg.Run(conn)
}
