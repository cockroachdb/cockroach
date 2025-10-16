// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"path/filepath"
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

	var args []string
	args = append(args, "test", "//pkg/testutils/lint:lint_test")
	addCommonBazelArguments(&args)
	args = append(args, additionalBazelArgs...)
	args = append(args, "--nocache_test_results", "--test_arg", "-test.v")
	if short {
		args = append(args, "--test_arg", "-test.short")
	}
	if timeout > 0 {
		args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
	}
	if filter != "" {
		args = append(args, fmt.Sprintf("--test_filter=Lint/%s", filter))
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	args = append(args, fmt.Sprintf("--test_env=COCKROACH_WORKSPACE=%s", workspace))
	home, err := d.os.HomeDir()
	if err != nil {
		return err
	}
	args = append(args,
		fmt.Sprintf("--test_env=HOME=%s", home),
		fmt.Sprintf("--sandbox_writable_path=%s", home))
	args = append(args, "--test_output", "streamed")
	// We need the location of the Go SDK for staticcheck and gcassert.
	out, err := d.exec.CommandContextSilent(ctx, "bazel", "run", "@go_sdk//:bin/go", "--run_under=//build/bazelutil/whereis")
	if err != nil {
		return fmt.Errorf("could not find location of Go SDK (%w)", err)
	}
	goBin := strings.TrimSpace(string(out))
	args = append(args, fmt.Sprintf("--test_env=GO_SDK=%s", filepath.Dir(filepath.Dir(goBin))))
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
		args = append(args,
			fmt.Sprintf("--test_env=CC=%s", cc),
			fmt.Sprintf("--test_env=CXX=%s", cc))
	}

	logCommand("bazel", args...)
	if len(pkgs) > 1 {
		return fmt.Errorf("can only lint a single package (found %s)", strings.Join(pkgs, ", "))
	}
	var pkg string
	if len(pkgs) == 1 {
		pkg = strings.TrimRight(pkgs[0], "/")
		if !strings.HasPrefix(pkg, "./") {
			pkg = "./" + pkg
		}
		args = append(args, fmt.Sprintf("--test_env=PKG=%s", pkg))
	}
	err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	if err != nil {
		return err
	}
	if pkg != "" && filter == "" {
		toLint := strings.TrimPrefix(pkg, "./")
		args := []string{"build", toLint, "--run_validations"}
		addCommonBazelArguments(&args)
		logCommand("bazel", args...)
		return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	} else if !short && filter == "" {
		args := []string{
			"build",
			"//pkg/cmd/cockroach-short",
			"//pkg/cmd/dev",
			"//pkg/cmd/roachprod",
			"//pkg/cmd/roachtest",
			"--run_validations",
		}
		addCommonBazelArguments(&args)
		logCommand("bazel", args...)
		return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
	} else if filter != "" {
		log.Printf("Skipping running extra lint checks with `nogo` due to provided test filter")
	} else if short {
		log.Printf("Skipping running extra lint checks with `nogo` due to --short")
	}
	return nil
}
