// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// cockroach-sql is an entry point for a CockroachDB binary that only
// includes the SQL shell and does not include any server components.
// It also does not include CCL features.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clientflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clienturl"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
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
	errCode := exit.Success()
	if err := runMain(); err != nil {
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

func runMain() error {
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

	// Configure the command-line flags.
	clientflags.AddBaseFlags(sqlCmd, &copts, &copts.Insecure, &copts.CertsDir)
	clientflags.AddSQLFlags(sqlCmd, &copts, cfg, true /* isShell */, false /* isDemo */)
	// Configure the format flag
	cliflagcfg.VarFlagDepth(1, sqlCmd.PersistentFlags(), &cfg.ExecCtx.TableDisplayFormat, cliflags.TableDisplayFormat)

	// Apply the configuration defaults from environment variables.
	// This must occur before the parameters are parsed by cobra, so
	// that the command-line flags can override the defaults in
	// environment variables.
	if err := cliflagcfg.ProcessEnvVarDefaults(sqlCmd); err != nil {
		return err
	}

	// We want to simplify tutorials, docs etc by allowing
	// 'cockroach-sql' to be symlinked to 'cockroach' and
	// ensure that the resulting symlink still works when invoked
	// as 'cockroach sql' (with a 'sql' sub-command).
	subCmd := *sqlCmd
	sqlCmd.AddCommand(&subCmd)

	// Now is time to run the command.
	return sqlCmd.Execute()
}

var sqlCmd = &cobra.Command{
	Use:   "sql [[--url] <url>] [-f <inputfile>] [-e <stmt>]",
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
	fs := cliflagcfg.FlagSetForCmd(cmd)

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

	cfg.ShellCtx.CertsDir = copts.CertsDir
	cfg.ShellCtx.ParseURL = clienturl.MakeURLParserFn(cmd, copts)
	return cfg.Run(context.Background(), conn)
}
