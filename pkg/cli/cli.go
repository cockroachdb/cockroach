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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloudimpl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
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

	// We ignore the error in this lookup, because
	// we want cobra to handle lookup errors with a verbose
	// help message in Run() below.
	cmd, _, _ := cockroachCmd.Find(os.Args[1:])

	cmdName := commandName(cmd)

	err := doMain(cmd, cmdName)
	errCode := exit.Success()
	if err != nil {
		// Display the error and its details/hints.
		cliOutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)

		// Remind the user of which command was being run.
		fmt.Fprintf(stderr, "Failed running %q\n", cmdName)

		// Finally, extract the error code, as optionally specified
		// by the sub-command.
		errCode = exit.UnspecifiedError()
		var cliErr *cliError
		if errors.As(err, &cliErr) {
			errCode = cliErr.exitCode
		}
	}

	exit.WithCode(errCode)
}

func doMain(cmd *cobra.Command, cmdName string) error {
	if cmd != nil {
		// Apply the configuration defaults from environment variables.
		// This must occur before the parameters are parsed by cobra, so
		// that the command-line flags can override the defaults in
		// environment variables.
		if err := processEnvVarDefaults(cmd); err != nil {
			return err
		}

		if !cmdHasCustomLoggingSetup(cmd) {
			// the customLoggingSetupCmds do their own calls to setupLogging().
			//
			// We use a PreRun function, to ensure setupLogging() is only
			// called after the command line flags have been parsed.
			//
			// NB: we cannot use PersistentPreRunE,like in flags.go, because
			// overriding that here will prevent the persistent pre-run from
			// running on parent commands. (See the difference between PreRun
			// and PersistentPreRun in `(*cobra.Command) execute()`.)
			wrapped := cmd.PreRunE
			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				// We call setupLogging before the PreRunE function since
				// that function may perform logging.
				err := setupLogging(context.Background(), cmd,
					false /* isServerCmd */, true /* applyConfig */)

				if wrapped != nil {
					if err := wrapped(cmd, args); err != nil {
						return err
					}
				}

				return err
			}
		}
	}

	logcrash.SetupCrashReporter(
		context.Background(),
		cmdName,
	)

	defer logcrash.RecoverAndReportPanic(context.Background(), &serverCfg.Settings.SV)

	return Run(os.Args[1:])
}

func cmdHasCustomLoggingSetup(thisCmd *cobra.Command) bool {
	if thisCmd == nil {
		return false
	}
	for _, cmd := range customLoggingSetupCmds {
		if cmd == thisCmd {
			return true
		}
	}
	hasCustomLogging := false
	thisCmd.VisitParents(func(parent *cobra.Command) {
		for _, cmd := range customLoggingSetupCmds {
			if cmd == parent {
				hasCustomLogging = true
			}
		}
	})
	return hasCustomLogging
}

// commandName computes the name of the command that args would invoke. For
// example, the full name of "cockroach debug zip" is "debug zip". If args
// specify a nonexistent command, commandName returns "cockroach".
func commandName(cmd *cobra.Command) string {
	rootName := cockroachCmd.CommandPath()
	if cmd != nil {
		return strings.TrimPrefix(cmd.CommandPath(), rootName+" ")
	}
	return rootName
}

type cliError struct {
	exitCode exit.Code
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

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if cliCtx.showVersionUsingOnlyBuildTag {
			info := build.GetInfo()
			fmt.Println(info.Tag)
		} else {
			fmt.Println(fullVersionString())
		}
		return nil
	},
}

func fullVersionString() string {
	info := build.GetInfo()
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "Build Tag:        %s\n", info.Tag)
	fmt.Fprintf(tw, "Build Time:       %s\n", info.Time)
	fmt.Fprintf(tw, "Distribution:     %s\n", info.Distribution)
	fmt.Fprintf(tw, "Platform:         %s", info.Platform)
	if info.CgoTargetTriple != "" {
		fmt.Fprintf(tw, " (%s)", info.CgoTargetTriple)
	}
	fmt.Fprintln(tw)
	fmt.Fprintf(tw, "Go Version:       %s\n", info.GoVersion)
	fmt.Fprintf(tw, "C Compiler:       %s\n", info.CgoCompiler)
	fmt.Fprintf(tw, "Build Commit ID:  %s\n", info.Revision)
	fmt.Fprintf(tw, "Build Type:       %s", info.Type) // No final newline: cobra prints one for us.
	_ = tw.Flush()
	return buf.String()
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
	// Version causes cobra to automatically support a --version flag
	// that reports this string.
	Version: "details:\n" + fullVersionString() +
		"\n(use '" + os.Args[0] + " version --build-tag' to display only the build tag)",
}

var workloadCmd = workloadcli.WorkloadCmd(true /* userFacing */)

func init() {
	cobra.EnableCommandSorting = false

	// Set an error function for flag parsing which prints the usage message.
	cockroachCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		if err := c.Usage(); err != nil {
			return err
		}
		fmt.Fprintln(c.OutOrStderr()) // provide a line break between usage and error
		return &cliError{
			exitCode: exit.CommandLineFlagError(),
			cause:    err,
		}
	})

	cockroachCmd.AddCommand(
		startCmd,
		startSingleNodeCmd,
		connectCmd,
		initCmd,
		certCmd,
		quitCmd,

		sqlShellCmd,
		stmtDiagCmd,
		authCmd,
		nodeCmd,
		nodeLocalCmd,
		userFileCmd,
		importCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		demoCmd,
		convertURLCmd,
		genCmd,
		versionCmd,
		DebugCmd,
		sqlfmtCmd,
		workloadCmd,
	)
}

// isWorkloadCmd returns true iff cmd is a sub-command of 'workload'.
func isWorkloadCmd(cmd *cobra.Command) bool {
	return hasParentCmd(cmd, workloadCmd)
}

// isDemoCmd returns true iff cmd is a sub-command of `demo`.
func isDemoCmd(cmd *cobra.Command) bool {
	return hasParentCmd(cmd, demoCmd)
}

// hasParentCmd returns true iff cmd is a sub-command of refParent.
func hasParentCmd(cmd, refParent *cobra.Command) bool {
	if cmd == refParent {
		return true
	}
	hasParent := false
	cmd.VisitParents(func(thisParent *cobra.Command) {
		if thisParent == refParent {
			hasParent = true
		}
	})
	return hasParent
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
