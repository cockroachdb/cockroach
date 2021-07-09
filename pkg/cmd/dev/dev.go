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

var (
	// Shared flags.
	remoteCacheAddr string
	numCPUs         int
)

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
Dev is the general-purpose dev tool for working cockroachdb/cockroach. It
lets engineers do a few things:

- build various binaries (cockroach, optgen, ...)
- run arbitrary tests (unit tests, logic tests, ...)
- run tests under arbitrary configurations (under stress, using race builds, ...)
- generate code (bazel files, protobufs, ...)

...and much more.
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
		makeGenerateCmd(ret.generate),
		makeLintCmd(ret.lint),
		makeTestCmd(ret.test),
	)

	// Add all the shared flags.
	var debugVar bool
	for _, subCmd := range ret.cli.Commands() {
		subCmd.Flags().BoolVar(&debugVar, "debug", false, "enable debug logging for dev")
		subCmd.Flags().IntVar(&numCPUs, "cpus", 0, "cap the number of cpu cores used")
		// This points to the grpc endpoint of a running `buchr/bazel-remote`
		// instance. We're tying ourselves to the one implementation, but that
		// seems fine for now. It seems mature, and has (very experimental)
		// support for the  Remote Asset API, which helps speed things up when
		// the cache sits across the network boundary.
		subCmd.Flags().StringVar(&remoteCacheAddr, "remote-cache", "", "remote caching grpc endpoint to use")
	}
	for _, subCmd := range ret.cli.Commands() {
		subCmd.PreRun = func(cmd *cobra.Command, args []string) {
			if debugVar {
				ret.log.SetOutput(stdos.Stderr)
			}
		}
	}

	// Hide the `help` sub-command.
	ret.cli.SetHelpCommand(&cobra.Command{
		Use:    "noop-help",
		Hidden: true,
	})

	return &ret
}
