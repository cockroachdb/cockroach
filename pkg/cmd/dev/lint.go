// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
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

	// It's quite easy to _mistype_ "dev lint short" instead of "dev lint --short". In that case, 'short'
	// is parsed as a package name, which results in skipping a number of linters. Since 'short' is not a valid
	// package, we bail out.
	for _, pkg := range pkgs {
		if pkg == "short" {
			return fmt.Errorf("invalid package name: %q; did you mean to type '--short'?", pkg)
		}
	}

	lintEnv := os.Environ()

	if !short {
		// First, generate code to make sure GCAssert and any other
		// tests that depend on generated code still work.
		if err := d.generateGo(cmd); err != nil {
			return err
		}
		// We also need `CC` and `CXX` set appropriately.
		cc, err := d.exec.LookPath("cc")
		if err != nil {
			return fmt.Errorf("`cc` is not installed; needed for `TestGCAssert` (%w)", err)
		}
		cc = strings.TrimSpace(cc)
		d.log.Printf("export CC=%s", cc)
		d.log.Printf("export CXX=%s", cc)
		envWithCc := []string{"CC=" + cc, "CXX=" + cc}
		lintEnv = append(envWithCc, lintEnv...)
	}

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
		envvar := fmt.Sprintf("PKG=%s", pkg)
		d.log.Printf("export %s", envvar)
		lintEnv = append(lintEnv, envvar)
	}
	err := d.exec.CommandContextWithEnv(ctx, lintEnv, "bazel", args...)
	if err != nil {
		return err
	}
	if !short && filter == "" {
		args := []string{
			"build",
			"//pkg/cmd/cockroach-short",
			"//pkg/cmd/dev",
			"//pkg/cmd/roachprod",
			"//pkg/cmd/roachtest",
			"--//build/toolchains:nogo_flag",
		}
		if numCPUs != 0 {
			args = append(args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
		}
		logCommand("bazel", args...)
		return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	} else if !short {
		log.Printf("Skipping building cockroach-short with nogo due to provided test filter")
	}
	return nil
}
