// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	workloadcli "github.com/cockroachdb/cockroach/pkg/workload/cli"

	// intentionally not all the workloads in pkg/ccl/workloadccl/allccl
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
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

	errCode := 0
	if err := Run(os.Args[1:]); err != nil {
		fmt.Fprintf(stderr, "Failed running %q\n", cmdName)
		errCode = 1
		if ec, ok := errors.Cause(err).(*cliError); ok {
			errCode = ec.exitCode
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
		StartCmd,
		initCmd,
		certCmd,
		quitCmd,

		sqlShellCmd,
		userCmd,
		zoneCmd,
		nodeCmd,
		dumpCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		demoCmd,
		genCmd,
		versionCmd,
		DebugCmd,
		sqlfmtCmd,
		workloadcli.WorkloadCmd(true /* userFacing */),
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
