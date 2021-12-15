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
		Long: `
Dev is the general-purpose dev tool for working on cockroachdb/cockroach. With dev you can:

- build various binaries (cockroach, optgen, ...)
- run arbitrary tests (unit tests, logic tests, ...)
- run tests under arbitrary configurations (under stress, using race builds, ...)
- generate code (bazel files, docs, ...)
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
		makeBenchCmd(ret.bench),
		makeBuildCmd(ret.build),
		makeBuilderCmd(ret.builder),
		makeDoctorCmd(ret.doctor),
		makeGenerateCmd(ret.generate),
		makeGoCmd(ret.gocmd),
		makeTestLogicCmd(ret.testlogic),
		makeLintCmd(ret.lint),
		makeTestCmd(ret.test),
		makeRunCmd(ret.run),
	)
	// Add all the shared flags.
	var debugVar bool
	for _, subCmd := range ret.cli.Commands() {
		subCmd.Flags().BoolVar(&debugVar, "debug", false, "enable debug logging for dev")
	}
	for _, subCmd := range ret.cli.Commands() {
		subCmd.PreRun = func(cmd *cobra.Command, args []string) {
			if debugVar {
				ret.log.SetOutput(stdos.Stderr)
			}
		}
	}

	return &ret
}
