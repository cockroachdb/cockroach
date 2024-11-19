// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"log"
	stdos "os"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/spf13/cobra"
)

type dev struct {
	log  *log.Logger
	cli  *cobra.Command
	os   *os.OS
	exec *exec.Exec

	debug bool

	knobs struct { // testing knobs
		devBinOverride string
	}
}

func makeDevCmd() *dev {
	var ret dev
	ret.log = log.New(io.Discard, "DEBUG: ", 0) // used for debug logging (see --debug)
	ret.exec = exec.New(exec.WithLogger(ret.log))
	ret.os = os.New(os.WithLogger(ret.log))

	ret.cli = &cobra.Command{
		Use:     "dev [command] (flags)",
		Short:   "Dev is the general-purpose dev tool for working on cockroach/cockroachdb.",
		Version: "v0.0",
		Long: `Dev is the general-purpose dev tool for working on cockroachdb/cockroach. With dev you can:

- build various binaries (cockroach, optgen, ...)
- run arbitrary tests (unit tests, logic tests, ...) under various configurations (stress, race, ...)
- generate code (bazel files, docs, protos, ...)

Typical usage:
    dev build
        Build the full cockroach binary.

    dev build short
        Build the cockroach binary without UI.

    dev generate go
        Regenerate all generated go code (protos, stringer, ...)

    dev generate bazel
        Regenerate all BUILD.bazel files.

    dev lint
        Run all style checkers and linters.

    dev lint --short
        Run a fast subset of the style checkers and linters.

    dev bench pkg/sql/parser -f=BenchmarkParse
        Run BenchmarkParse in pkg/sql/parser.

    dev test pkg/sql
        Run all unit tests in pkg/sql.

    dev test pkg/sql/parser -f=TestParse
        Run TestParse in pkg/sql/parser.

    dev testlogic
        Run all base, opt exec builder, and ccl logic tests.

    dev testlogic ccl
        Run all ccl logic tests.

    dev testlogic opt
        Run all opt exec builder logic tests.

    dev testlogic base
        Run all OSS logic tests.

    dev testlogic --files='prepare|fk'
        Run the logic tests in the files named prepare and fk (the full path is not required).

    dev testlogic --files=fk --subtests='20042|20045'
        Run the logic tests within subtests 20042 and 20045 in the file named fk.

    dev testlogic --config=local
        Run the logic tests for the cluster configuration 'local'.

    dev build short -- --verbose_failures --profile=prof.gz
        Pass additional arguments directly to bazel (after the stand alone '--').
`,
		// Disable automatic printing of usage information whenever an error
		// occurs. We presume that most errors will not the result of bad
		// command invocation; they'll be due to legitimate build/test errors.
		// Printing out the usage information in these cases obscures the real
		// cause of the error. Commands should manually print usage information
		// when the error is, in fact, a result of a bad invocation, e.g. too
		// many arguments.
		SilenceUsage: true,
		// Disable automatic printing of the error. We want to also print
		// details and hints, which cobra does not do for us. Instead we do the
		// printing in the command implementation.
		SilenceErrors: true,
	}

	// Create all the sub-commands.
	ret.cli.AddCommand(
		makeAcceptanceCmd(ret.acceptance),
		makeBenchCmd(ret.bench),
		makeBuildCmd(ret.build),
		makeBuilderCmd(ret.builder),
		makeCacheCmd(ret.cache),
		makeComposeCmd(ret.compose),
		makeDoctorCmd(ret.doctor),
		makeGenerateCmd(ret.generate),
		makeGoCmd(ret.gocmd),
		makeTestLogicCmd(ret.testlogic),
		makeLintCmd(ret.lint),
		makeTestCmd(ret.test),
		makeUICmd(&ret),
		makeRoachprodStressCmd(ret.roachprodStress),
		makeTestBinariesCmd(ret.testBinaries),
	)

	// Add all the shared flags.
	ret.cli.PersistentFlags().BoolVar(&ret.debug, "debug", false, "enable debug logging for dev")
	ret.cli.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		skipDoctorCheckCommands := []string{
			"builder",
			"doctor",
			"go",
			"help",
		}
		var skipDoctorCheck bool
		for _, skipDoctorCheckCommand := range skipDoctorCheckCommands {
			skipDoctorCheck = skipDoctorCheck || cmd.Name() == skipDoctorCheckCommand
		}
		if !skipDoctorCheck {
			if err := ret.checkDoctorStatus(cmd.Context()); err != nil {
				return err
			}
		}

		ctx := cmd.Context()
		skipCacheCheckCommands := []string{
			"cache",
		}
		skipCacheCheckCommands = append(skipCacheCheckCommands, skipDoctorCheckCommands...)
		skipCacheCheck := buildutil.CrdbTestBuild || ret.os.Getenv("DEV_NO_REMOTE_CACHE") != ""
		for _, skipCacheCheckCommand := range skipCacheCheckCommands {
			skipCacheCheck = skipCacheCheck || cmd.Name() == skipCacheCheckCommand
		}
		// Check if we're running in remote mode: we don't want to setup
		// the cache in this case.
		if !skipCacheCheck {
			workspace, err := ret.getWorkspace(ctx)
			if err != nil {
				return err
			}
			if ret.checkUsingConfig(workspace, "engflow") {
				skipCacheCheck = true
			}
		}
		if !skipCacheCheck {
			_, err := ret.setUpCache(ctx)
			if err != nil {
				return err
			}
		}

		if ret.debug {
			ret.log.SetOutput(stdos.Stderr)
		}
		return nil
	}

	return &ret
}
