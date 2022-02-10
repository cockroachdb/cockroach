// Copyright 2020 The Cockroach Authors.
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
	"strings"

	"github.com/spf13/cobra"
)

// makeLintCmd constructs the subcommand used to run the specified linters.
func makeLintCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	lintCmd := &cobra.Command{
		Use:   "lint",
		Short: `Run the specified linters`,
		Long:  `Run the specified linters.`,
		Example: `
	dev lint --filter=TestLowercaseFunctionNames --short --timeout=1m
	dev lint pkg/cmd/dev
`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	addCommonBuildFlags(lintCmd)
	addCommonTestFlags(lintCmd)
	return lintCmd
}

func (d *dev) lint(cmd *cobra.Command, commandLine []string) error {
	pkgs, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	filter := mustGetFlagString(cmd, filterFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)
	short := mustGetFlagBool(cmd, shortFlag)

	var args []string
	// NOTE the --config=test here. It's very important we compile the test binary with the
	// appropriate stuff (gotags, etc.)
	args = append(args, "run", "--config=test", "//build/bazelutil:lint")
	if numCPUs != 0 {
		args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	args = append(args, additionalBazelArgs...)
	args = append(args, "--", "-test.v")
	if short {
		args = append(args, "-test.short")
	}
	if timeout > 0 {
		args = append(args, "-test.timeout", timeout.String())
	}
	if filter != "" {
		args = append(args, "-test.run", fmt.Sprintf("Lint/%s", filter))
	}
	logCommand("bazel", args...)
	if len(pkgs) > 1 {
		return fmt.Errorf("can only lint a single package (found %s)", strings.Join(pkgs, ", "))
	}
	if len(pkgs) == 1 {
		pkg := strings.TrimRight(pkgs[0], "/")
		if !strings.HasPrefix(pkg, "./") {
			pkg = "./" + pkg
		}
		env := os.Environ()
		envvar := fmt.Sprintf("PKG=%s", pkg)
		d.log.Printf("export %s", envvar)
		env = append(env, envvar)
		return d.exec.CommandContextWithEnv(ctx, env, "bazel", args...)
	}
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
