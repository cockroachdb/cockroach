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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clienturl"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

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

// copts is the set of options that is used to configure the
// connection URL.
var copts clientsecopts.ClientOptions

func main() {
	// URL defaults.
	copts.ServerHost = "localhost"
	copts.ServerPort = "26257"
	// TODO(knz): We should deprecate auto-connecting to 'defaultdb'
	// and instead do something like psql, i.e. use the current
	// unix username as default target database.
	copts.Database = "defaultdb"
	// TODO(knz): We should deprecate auto-connecting as 'root'
	// and instead do something like psql, i.e. use the current
	// unix username to log into the database.
	copts.User = "root"

	// Defaults for the SQL shell.
	cfg.User = copts.User
	cfg.Database = copts.Database
	cfg.ApplicationName = "$ cockroach sql"
	// TODO(knz): Make the timeout configurable.
	cfg.ConnectTimeout = 15

	// The connection parameters can be configured either via a URL or
	// discrete flags.

	f := sqlCmd.PersistentFlags()

	// URL configuration. This is the main recommended connection method
	// and the only one documented for this command.
	//
	// The complexity of urlParser{} exists because --url
	// can be interleaved with other discrete parameters. The URL
	// parser needs to integrate values from earlier discrete/URL flags
	// and extract defaults for subsequent parameters.
	f.Var(clienturl.NewURLParser(sqlCmd, &copts, false /*sslStrict*/, nil /*warnFn*/),
		cliflags.URL.Name, cliflags.URL.Description)

	// Discrete flags follow.
	// We support more flags than are documented, so that the cockroach-sql binary
	// can be used in place of 'cockroach sql'.
	//
	// Remote SQL parameters.
	f.StringVarP(&copts.Database, cliflags.Database.Name, cliflags.Database.Shorthand, cfg.Database, cliflags.Database.Description)
	f.StringVarP(&copts.User, cliflags.User.Name, cliflags.User.Shorthand, cfg.User, cliflags.User.Description)
	// Hidden parameters for compatibility with 'cockroach sql'.
	f.Var(addr.NewAddrSetter(&copts.ServerHost, &copts.ServerPort), cliflags.ClientHost.Name, cliflags.ClientHost.Description)
	_ = f.MarkHidden(cliflags.ClientHost.Name)
	f.StringVar(&copts.ServerPort, cliflags.ClientPort.Name, copts.ServerPort, cliflags.ClientPort.Description)
	_ = f.MarkHidden(cliflags.ClientPort.Name)
	f.StringVar(&copts.CertsDir, cliflags.CertsDir.Name, copts.CertsDir, cliflags.CertsDir.Description)
	_ = f.MarkHidden(cliflags.CertsDir.Name)
	// NB: Insecure is deprecated. See #53404.
	f.BoolVar(&copts.Insecure, cliflags.ClientInsecure.Name, copts.Insecure, cliflags.ClientInsecure.Description)
	_ = f.MarkHidden(cliflags.ClientInsecure.Name)

	// Parameters specific to the SQL shell.
	f.VarP(&cfg.ShellCtx.ExecStmts, cliflags.Execute.Name, cliflags.Execute.Shorthand, cliflags.Execute.Description)
	f.StringVarP(&cfg.InputFile, cliflags.File.Name, cliflags.File.Shorthand, cfg.InputFile, cliflags.File.Description)
	f.DurationVar(&cfg.ShellCtx.RepeatDelay, cliflags.Watch.Name, cfg.ShellCtx.RepeatDelay, cliflags.Watch.Description)
	f.Var(&cfg.SafeUpdates, cliflags.SafeUpdates.Name, cliflags.SafeUpdates.Description)
	f.BoolVar(&cfg.ReadOnly, cliflags.ReadOnly.Name, cfg.ReadOnly, cliflags.ReadOnly.Description)
	f.Lookup(cliflags.SafeUpdates.Name).NoOptDefVal = "true" // allow the flag to not be given any value
	f.BoolVar(&cfg.ConnCtx.DebugMode, cliflags.CliDebugMode.Name, cfg.ConnCtx.DebugMode, cliflags.CliDebugMode.Description)
	f.BoolVar(&cfg.ConnCtx.Echo, cliflags.EchoSQL.Name, cfg.ConnCtx.Echo, cliflags.EchoSQL.Description)

	// We want to simplify tutorials, docs etc by allowing
	// 'cockroach-sql' to be symlinked to 'cockroach' and
	// ensure that the resulting symlink still works when invoked
	// as 'cockroach sql' (with a 'sql' sub-command).
	subCmd := *sqlCmd
	sqlCmd.AddCommand(&subCmd)

	// Now is time to run the command.
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
	Use:   "sql [[--url] <url>] [-f <inputfile>] [-e <stmt>] [options]",
	Short: "start the SQL shell",
	Args:  cobra.MaximumNArgs(1),
	Long: `
Open a SQL shell running against a CockroachDB database.
`,
	RunE: runSQL,
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
	// Prevent cobra from auto-generating a completions command,
	// since we provide our own.
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	// Support --version automatically.
	Version: func() string {
		info := build.GetInfo()
		return "details:\n" + info.Long()
	}(),
}

func runSQL(cmd *cobra.Command, args []string) (resErr error) {
	fs := clienturl.FlagSetForCmd(cmd)

	if len(args) > 0 {
		urlFlag := fs.Lookup(cliflags.URL.Name)
		if urlFlag.Changed {
			return errors.New("cannot pass both --url and positional URL")
		}
		// Calling Set() on the URL flag propagates the changes to `copts`,
		// where it will be picked up by MakeClientConnURL below.
		if err := urlFlag.Value.Set(args[0]); err != nil {
			return err
		}
	}

	// Translate the connection arguments to a connection URL.
	connURL, err := clientsecopts.MakeClientConnURL(copts)
	if err != nil {
		return err
	}

	// Detect conflicting configuration with regards to authentication.
	// (This check exists to mirror the behavior of 'cockroach sql'.)
	usePassword, _, _ := connURL.GetAuthnPassword()
	if usePassword && copts.Insecure {
		// There's a password already configured.
		// In insecure mode, we don't want the user to get the mistaken
		// idea that a password is worth anything.
		return errors.New("password authentication not enabled in insecure mode")
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

	conn, err := cfg.MakeConn(connURL.ToPQ().String())
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	cfg.ShellCtx.ParseURL = clienturl.MakeURLParserFn(cmd, copts)
	return cfg.Run(conn)
}
