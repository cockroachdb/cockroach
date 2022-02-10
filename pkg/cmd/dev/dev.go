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
	"io/ioutil"
	"log"
	stdos "os"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/os"
	"github.com/spf13/cobra"
)

type dev struct {
	log  *log.Logger
	cli  *cobra.Command
	os   *os.OS
	exec *exec.Exec

	knobs struct { // testing knobs
		skipDoctorCheck bool
		devBinOverride  string
	}
}

func makeDevCmd() *dev {
	var ret dev
	ret.log = log.New(ioutil.Discard, "DEBUG: ", 0) // used for debug logging (see --debug)
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
		makeMergeTestXMLsCmd(ret.mergeTestXMLs),
		makeTestLogicCmd(ret.testlogic),
		makeLintCmd(ret.lint),
		makeTestCmd(ret.test),
		makeUICmd(&ret),
	)

	// Add all the shared flags.
	var debugVar bool
	ret.cli.PersistentFlags().BoolVar(&debugVar, "debug", false, "enable debug logging for dev")
	ret.cli.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		skipDoctorCheck := cmd.Name() == "doctor" || cmd.Name() == "merge-test-xmls"
		if !skipDoctorCheck {
			if err := ret.checkDoctorStatus(cmd.Context()); err != nil {
				return err
			}
		}
		if debugVar {
			ret.log.SetOutput(stdos.Stderr)
		}
		return nil
	}

	return &ret
}
