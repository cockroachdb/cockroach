// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	buildSubcmd = "build"
	testSubcmd  = "test"
)

var (
	artifactsDir    string
	configs         []string
	compilationMode string

	rootCmd = &cobra.Command{
		Use:   "bazci",
		Short: "A glue binary for making Bazel usable in Teamcity",
		Long: `bazci is glue code to make debugging Bazel builds and
tests in Teamcity as painless as possible.`,
		Args: func(cmd *cobra.Command, args []string) error {
			_, err := parseArgs(args, cmd.ArgsLenAtDash())
			return err
		},
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          bazciImpl,
	}
)

func init() {
	rootCmd.Flags().StringVar(
		&artifactsDir,
		"artifacts_dir",
		"/artifacts",
		"path where artifacts should be staged")
	rootCmd.Flags().StringVar(
		&compilationMode,
		"compilation_mode",
		"dbg",
		"compilation mode to pass down to Bazel (dbg or opt)")
	rootCmd.Flags().StringSliceVar(
		&configs,
		"config",
		[]string{},
		"list of build configs to apply to bazel calls")
}

// parsedArgs looks basically like the `args` slice that Cobra gives us, but
// a little more tightly structured.
// e.g. the args ["test", "//pkg:small_tests", "--" "--verbose_failures"]
// get converted to parsedArgs {
//   subcmd: "test",
//   targets: ["//pkg:small_tests"],
//   additional: ["--verbose_failures"]
// }
type parsedArgs struct {
	// The subcommand: either "build" or "test".
	subcmd string
	// The list of targets being built or tested. May include test suites.
	targets []string
	// Additional arguments to pass along to Bazel.
	additional []string
}

// Returned by parseArgs on some bad inputs.
var errUsage = errors.New("At least 2 arguments required (e.g. `bazci build TARGET`)")

// parseArgs converts a raw list of arguments from Cobra to a parsedArgs. The second argument,
// `argsLenAtDash`, should be the value returned by `cobra.Command.ArgsLenAtDash()`.
func parseArgs(args []string, argsLenAtDash int) (*parsedArgs, error) {
	// The minimum number of arguments needed is 2: the first is the
	// subcommand to run (`build` or `test`), and the second is the
	// first label (e.g. `//pkg/cmd/cockroach-short`). An arbitrary
	// number of additional labels can follow.
	if len(args) < 2 {
		return nil, errUsage
	}
	if args[0] != buildSubcmd && args[0] != testSubcmd {
		return nil, errors.Newf("First argument must be `build` or `test`; got %v", args[0])
	}
	var splitLoc int
	if argsLenAtDash < 0 {
		// Cobra sets the value of `ArgsLenAtDash()` to -1 if there's no
		// dash in the args.
		splitLoc = len(args)
	} else if argsLenAtDash < 2 {
		return nil, errUsage
	} else {
		splitLoc = argsLenAtDash
	}
	return &parsedArgs{
		subcmd:     args[0],
		targets:    args[1:splitLoc],
		additional: args[splitLoc:],
	}, nil
}

// buildInfo captures more specific, granular data about the build or test
// request. We query bazel for this data before running the build and use it to
// find output artifacts.
type buildInfo struct {
	// Location of the bazel-bin directory.
	binDir string
	// Location of the bazel-testlogs directory.
	testlogsDir string
	// Expanded list of Go binary targets to be built.
	goBinaries []string
	// Expanded list of Go test targets to be run. Test suites are split up
	// into their component tests and all put in this list, so this may be
	// considerably longer than the argument list.
	tests []string
}

func runBazelReturningStdout(subcmd string, arg ...string) (string, error) {
	if subcmd != "query" {
		arg = append(configArgList(), arg...)
		arg = append(arg, "-c", compilationMode)
	}
	arg = append([]string{subcmd}, arg...)
	buf, err := exec.Command("bazel", arg...).Output()
	if err != nil {
		fmt.Println("Failed to run Bazel with args: ", arg)
		return "", err
	}
	return strings.TrimSpace(string(buf)), nil
}

func getBuildInfo(args parsedArgs) (buildInfo, error) {
	binDir, err := runBazelReturningStdout("info", "bazel-bin")
	if err != nil {
		return buildInfo{}, err
	}
	testlogsDir, err := runBazelReturningStdout("info", "bazel-testlogs")
	if err != nil {
		return buildInfo{}, err
	}

	ret := buildInfo{
		binDir:      binDir,
		testlogsDir: testlogsDir,
	}

	for _, target := range args.targets {
		output, err := runBazelReturningStdout("query", "--output=label_kind", target)
		if err != nil {
			return buildInfo{}, err
		}
		// The format of the output is `[kind] rule [full_target_name].
		outputSplit := strings.Fields(output)
		if len(outputSplit) != 3 {
			return buildInfo{}, errors.Newf("Could not parse bazel query output: %v", output)
		}
		targetKind := outputSplit[0]
		fullTarget := outputSplit[2]

		switch targetKind {
		case "go_binary":
			ret.goBinaries = append(ret.goBinaries, fullTarget)
		case "go_test":
			ret.tests = append(ret.tests, fullTarget)
		case "test_suite":
			// Expand the list of tests from the test suite with another query.
			allTests, err := runBazelReturningStdout("query", "tests("+fullTarget+")")
			if err != nil {
				return buildInfo{}, err
			}
			ret.tests = append(ret.tests, strings.Fields(allTests)...)
		default:
			return buildInfo{}, errors.Newf("Got unexpected target kind %v", targetKind)
		}
	}

	return ret, nil
}

func bazciImpl(cmd *cobra.Command, args []string) error {
	parsedArgs, err := parseArgs(args, cmd.ArgsLenAtDash())
	if err != nil {
		return err
	}

	info, err := getBuildInfo(*parsedArgs)
	if err != nil {
		return err
	}

	// Run the build in a background thread and ping the `completion`
	// channel when done.
	completion := make(chan error)
	go func() {
		processArgs := []string{parsedArgs.subcmd}
		processArgs = append(processArgs, parsedArgs.targets...)
		processArgs = append(processArgs, configArgList()...)
		processArgs = append(processArgs, "-c", compilationMode)
		processArgs = append(processArgs, parsedArgs.additional...)
		fmt.Println("running bazel w/ args: ", processArgs)
		cmd := exec.Command("bazel", processArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err != nil {
			completion <- err
			return
		}
		completion <- cmd.Wait()
	}()

	return makeWatcher(completion, info).Watch()
}

func configArgList() []string {
	ret := []string{}
	for _, config := range configs {
		ret = append(ret, "--config="+config)
	}
	return ret
}

func usingCrossWindowsConfig() bool {
	for _, config := range configs {
		if config == "crosswindows" {
			return true
		}
	}
	return false
}
