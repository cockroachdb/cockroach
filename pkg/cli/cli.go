// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cli is the command-line library used by the cockroach binary and
// other utilities. See README.md.
package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/testutils/bazelcodecover"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	// intentionally not all the workloads in pkg/ccl/workloadccl/allccl
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"       // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/bulkingest" // registers workloads
	workloadcli "github.com/cockroachdb/cockroach/pkg/workload/cli"
	_ "github.com/cockroachdb/cockroach/pkg/workload/debug"     // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples"  // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/insights"  // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/kv"        // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/movr"      // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/sqlstats"  // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"      // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpch"      // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttlbench"  // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttllogger" // registers workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/ycsb"      // registers workloads
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Main is the entry point for the cli, with a single line calling it intended
// to be the body of an action package main `main` func elsewhere. It is
// abstracted for reuse by duplicated `main` funcs in different distributions.
func Main() {
	bazelcodecover.MaybeInitCodeCoverage()
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
		clierror.OutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)

		// Remind the user of which command was being run.
		fmt.Fprintf(stderr, "Failed running %q\n", cmdName)

		// Finally, extract the error code, as optionally specified
		// by the sub-command.
		errCode = getExitCode(err)
	}
	// Finally, gracefully shutdown logging facilities.
	cliCtx.logShutdownFn()

	exit.WithCode(errCode)
}

func getExitCode(err error) (errCode exit.Code) {
	errCode = exit.UnspecifiedError()
	var cliErr *clierror.Error
	if errors.As(err, &cliErr) {
		errCode = cliErr.GetExitCode()
	}
	return errCode
}

func doMain(cmd *cobra.Command, cmdName string) error {
	defer debugSignalSetup()()

	if cmd != nil {
		// Apply the configuration defaults from environment variables.
		// This must occur before the parameters are parsed by cobra, so
		// that the command-line flags can override the defaults in
		// environment variables.
		if err := cliflagcfg.ProcessEnvVarDefaults(cmd); err != nil {
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
	return info.Long()
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
	// Prevent cobra from auto-generating a completions command,
	// since we provide our own.
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
}

// CockroachCmd returns the root cockroach Command object.
func CockroachCmd() *cobra.Command {
	return cockroachCmd
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
		return clierror.NewError(err, exit.CommandLineFlagError())
	})

	cockroachCmd.AddCommand(
		startCmd,
		startSingleNodeCmd,
		initCmd,
		certCmd,

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
		GenCmd,
		versionCmd,
		DebugCmd,
		sqlfmtCmd,
		workloadCmd,
		encodeURICmd,
	)
}

// isWorkloadCmd returns true iff cmd is a sub-command of 'workload'.
func isWorkloadCmd(cmd *cobra.Command) bool {
	return hasParentCmd(cmd, workloadCmd)
}

// isDemoCmd returns true iff cmd is a sub-command of `demo`.
func isDemoCmd(cmd *cobra.Command) bool {
	return hasParentCmd(cmd, demoCmd) || hasParentCmd(cmd, debugStatementBundleCmd)
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

// UsageAndErr informs the user about the usage of the command
// and returns an error. This ensures that the top-level command
// has a suitable exit status.
func UsageAndErr(cmd *cobra.Command, args []string) error {
	if err := cmd.Usage(); err != nil {
		return err
	}
	return fmt.Errorf("unknown sub-command: %q", strings.Join(args, " "))
}

// debugSignalSetup sets up signal handlers for SIGQUIT and SIGUSR2 to enable
// debugging a stuck or misbehaving process, with the former logging all stacks
// (but not killing the process, unlike its default go handler) and the latter
// opening an http server that exposes the pprof endpoints on localhost. The
// resturned shutdown function should be called at process exit to stop any
// associated goroutines.
func debugSignalSetup() func() {
	exit := make(chan struct{})
	ctx := context.Background()

	// For SIGQUIT we spawn a goroutine and we always handle it, no matter at
	// which point during execution we are. This makes it possible to use SIGQUIT
	// to inspect a running process and determine what it is currently doing, even
	// if it gets stuck somewhere.
	if quitSignal != nil {
		quitSignalCh := make(chan os.Signal, 1)
		signal.Notify(quitSignalCh, quitSignal)
		go func() {
			for {
				select {
				case <-exit:
					return
				case <-quitSignalCh:
					log.DumpStacks(ctx, "SIGQUIT received")
				}
			}
		}()
	}

	// For SIGUSR, we spawn a goroutine that when signaled will then start an http
	// server, bound to localhost, which serves the go pprof endpoints. While a
	// cockroach server process already serves theese on its HTTP port, other CLI
	// commands, particularly short-lived client commands, do not open HTTP ports
	// by default. The pprof endpoints however can be invaluable when inspecting
	// a process that is behaving unexpectedly, thus this mechanism to request any
	// cockroach process begin serving them when needed.
	if debugSignal != nil {
		debugSignalCh := make(chan os.Signal, 1)
		signal.Notify(debugSignalCh, debugSignal)
		go func() {
			for {
				select {
				case <-exit:
					return
				case <-debugSignalCh:
					log.Shout(ctx, severity.INFO, "setting up localhost debugging endpoint...")
					mux := http.NewServeMux()
					mux.HandleFunc("/debug/pprof/", pprof.Index)
					mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
					mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
					mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
					mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

					listenAddr := "localhost:0"
					listener, err := net.Listen("tcp", listenAddr)
					if err != nil {
						log.Shoutf(ctx, severity.WARNING, "debug server could not start listening on %s: %v", listenAddr, err)
						continue
					}

					server := http.Server{Handler: mux}
					go func() {
						if err := server.Serve(listener); err != nil {
							log.Warningf(ctx, "debug server: %v", err)
						}
					}()
					log.Shoutf(ctx, severity.INFO, "debug server listening on %s", listener.Addr())
					<-exit
					if err := server.Shutdown(ctx); err != nil {
						log.Warningf(ctx, "error shutting down debug server: %s", err)
					}
				}
			}
		}()
	}
	return func() {
		close(exit)
	}
}
