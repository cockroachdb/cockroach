// Copyright 2015 The Cockroach Authors.
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
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"text/tabwriter"

	_ "github.com/benesch/cgosymbolizer" // calls runtime.SetCgoTraceback on import
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	// intentionally not all the workloads in pkg/ccl/workloadccl/allccl
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"       // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/bulkingest" // registers workloads
	workloadcli "github.com/cockroachdb/cockroach/pkg/workload/cli"
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples" // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/kv"       // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/movr"     // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"     // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpch"     // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/ycsb"     // registers workloads
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Main is the entry point for the cli, with a single line calling it intended
// to be the body of an action package main `main` func elsewhere. It is
// abstracted for reuse by duplicated `main` funcs in different distributions.
func Main() {
	// Seed the math/rand RNG from crypto/rand.
	rand.Seed(randutil.NewPseudoSeed())

	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}

	// Change the logging defaults for the main cockroach binary.
	// The value is overridden after command-line parsing.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("NONE"); err != nil {
		panic(err)
	}

	cmdName := commandName(os.Args[1:])

	log.SetupCrashReporter(
		context.Background(),
		cmdName,
	)

	defer log.RecoverAndReportPanic(context.Background(), &serverCfg.Settings.SV)

	err := Run(os.Args[1:])

	errCode := 0
	if err != nil {
		// Display the error and its details/hints.
		cliOutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)

		// Remind the user of which command was being run.
		fmt.Fprintf(stderr, "Failed running %q\n", cmdName)

		// Finally, extract the error code, as optionally specified
		// by the sub-command.
		errCode = 1
		var cliErr *cliError
		if errors.As(err, &cliErr) {
			errCode = cliErr.exitCode
		}
	}

	os.Exit(errCode)
}

// commandName computes the name of the command that args would invoke. For
// example, the full name of "cockroach debug zip" is "debug zip". If args
// specify a nonexistent command, commandName returns "cockroach".
func commandName(args []string) string {
	rootName := cockroachCmd.CommandPath()
	// Ask Cobra to find the command so that flags and their arguments are
	// ignored. The name of "cockroach --log-dir foo start" is "start", not
	// "--log-dir" or "foo".
	if cmd, _, _ := cockroachCmd.Find(os.Args[1:]); cmd != nil {
		return strings.TrimPrefix(cmd.CommandPath(), rootName+" ")
	}
	return rootName
}

type cliError struct {
	exitCode int
	severity log.Severity
	cause    error
}

func (e *cliError) Error() string { return e.cause.Error() }

// Cause implements causer.
func (e *cliError) Cause() error { return e.cause }

// Format implements fmt.Formatter.
func (e *cliError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *cliError) FormatError(p errors.Printer) error {
	if p.Detail() {
		p.Printf("error with exit code: %d", e.exitCode)
	}
	return e.cause
}

// stderr aliases log.OrigStderr; we use an alias here so that tests
// in this package can redirect the output of CLI commands to stdout
// to be captured.
var stderr = log.OrigStderr

// stdin aliases os.Stdin; we use an alias here so that tests in this
// package can redirect the input of the CLI shell.
var stdin = os.Stdin

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		info := build.GetInfo()
		tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "Build Tag:    %s\n", info.Tag)
		fmt.Fprintf(tw, "Build Time:   %s\n", info.Time)
		fmt.Fprintf(tw, "Distribution: %s\n", info.Distribution)
		fmt.Fprintf(tw, "Platform:     %s", info.Platform)
		if info.CgoTargetTriple != "" {
			fmt.Fprintf(tw, " (%s)", info.CgoTargetTriple)
		}
		fmt.Fprintln(tw)
		fmt.Fprintf(tw, "Go Version:   %s\n", info.GoVersion)
		fmt.Fprintf(tw, "C Compiler:   %s\n", info.CgoCompiler)
		fmt.Fprintf(tw, "Build SHA-1:  %s\n", info.Revision)
		fmt.Fprintf(tw, "Build Type:   %s\n", info.Type)
		return tw.Flush()
	},
}

var cockroachCmd = &cobra.Command{
	Use:   "cockroach [command] (flags)",
	Short: "CockroachDB command-line interface and server",
	// TODO(cdo): Add a pointer to the docs in Long.
	Long: `CockroachDB command-line interface and server.`,
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

func init() {
	cobra.EnableCommandSorting = false

	// Set an error function for flag parsing which prints the usage message.
	cockroachCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		if err := c.Usage(); err != nil {
			return err
		}
		fmt.Fprintln(c.OutOrStderr()) // provide a line break between usage and error
		return err
	})

	cockroachCmd.AddCommand(
		startCmd,
		startSingleNodeCmd,
		initCmd,
		certCmd,
		quitCmd,

		sqlShellCmd,
		authCmd,
		nodeCmd,
		dumpCmd,
		nodeLocalCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		demoCmd,
		genCmd,
		versionCmd,
		DebugCmd,
		sqlfmtCmd,
		workloadcli.WorkloadCmd(true /* userFacing */),
		systemBenchCmd,
	)
}

// AddCmd adds a command to the cli.
func AddCmd(c *cobra.Command) {
	cockroachCmd.AddCommand(c)
}

// Run ...
func Run(args []string) error {
	cockroachCmd.SetArgs(args)
	return cockroachCmd.Execute()
}

// usageAndErr informs the user about the usage of the command
// and returns an error. This ensures that the top-level command
// has a suitable exit status.
func usageAndErr(cmd *cobra.Command, args []string) error {
	if err := cmd.Usage(); err != nil {
		return err
	}
	return fmt.Errorf("unknown sub-command: %q", strings.Join(args, " "))
}
