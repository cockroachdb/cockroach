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
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	// ossFlag is the name of the boolean long (GNU-style) flag that builds only the open-source parts of the UI
	ossFlag = "oss"
)

// makeUICmd initializes the top-level 'ui' subcommand
func makeUICmd(d *dev) *cobra.Command {
	uiCmd := &cobra.Command{
		Use:   "ui",
		Short: "Runs UI-related tasks",
		Long:  "Builds UI & runs UI-related commands to ease development flows",
	}

	uiCmd.AddCommand(makeUIWatchCmd(d))
	uiCmd.AddCommand(makeUILintCmd(d))
	uiCmd.AddCommand(makeUITestCmd(d))

	return uiCmd
}

// UIDirectories contains the absolute path to the root of each UI sub-project.
type UIDirectories struct {
	// clusterUI is the absolute path to ./pkg/ui/workspaces/cluster-ui
	clusterUI string
	// dbConsole is the absolute path to ./pkg/ui/workspaces/db-console
	dbConsole string
}

// getUIDirs computes the absolute path to the root of each UI sub-project.
func getUIDirs(d *dev) (*UIDirectories, error) {
	workspace, err := d.getWorkspace(d.cli.Context())
	if err != nil {
		return nil, err
	}

	return &UIDirectories{
		clusterUI: path.Join(workspace, "./pkg/ui/workspaces/cluster-ui"),
		dbConsole: path.Join(workspace, "./pkg/ui/workspaces/db-console"),
	}, nil
}

// makeUIWatchCmd initializes the 'ui watch' subcommand, which sets up a live-reloading HTTP server for db-console and a
// file-watching rebuilder for cluster-ui.
func makeUIWatchCmd(d *dev) *cobra.Command {
	const (
		// portFlag is the name of the long (GNU-style) flag that controls which port webpack's dev server listens on
		portFlag = "port"
		// dbTargetFlag is the name of the long (GNU-style) flag that determines which DB instance to proxy to
		dbTargetFlag = "db"
		// secureFlag is the name of the boolean long (GNU-style) flag that makes webpack's dev server use HTTPS
		secureFlag = "secure"
	)

	watchCmd := &cobra.Command{
		Use:   "watch",
		Short: "Watches for file changes and automatically rebuilds",
		Long: `Starts a local web server that watches for JS file changes, automatically regenerating the UI

Replaces 'make ui-watch'.`,
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			// Create a context that cancels when OS signals come in
			ctx, stop := signal.NotifyContext(d.cli.Context(), os.Interrupt, os.Kill)
			defer stop()

			isOss, err := cmd.Flags().GetBool(ossFlag)
			if err != nil {
				return err
			}

			// Build prerequisites for db-console and cluster-ui
			args := []string{
				"build",
				"//pkg/ui/workspaces/db-console/src/js:crdb-protobuf-client",
			}
			if !isOss {
				args = append(args, "//pkg/ui/workspaces/db-console/ccl/src/js:crdb-protobuf-client-ccl")
			}
			logCommand("bazel", args...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

			if err != nil {
				log.Fatalf("failed to build UI watch prerequisites: %v", err)
				return err
			}

			// Extract remaining flags
			portNumber, err := cmd.Flags().GetInt16(portFlag)
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
				return err
			}
			port := fmt.Sprint(portNumber)
			dbTarget, err := cmd.Flags().GetString(dbTargetFlag)
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
				return err
			}
			_, err = url.Parse(dbTarget)
			if err != nil {
				log.Fatalf("invalid format for --%s argument: %v", dbTargetFlag, err)
				return err
			}
			secure := mustGetFlagBool(cmd, secureFlag)

			dirs, err := getUIDirs(d)
			if err != nil {
				log.Fatalf("unable to find cluster-ui and db-console directories: %v", err)
				return err
			}

			// Start the cluster-ui watch task
			nbExec := d.exec.AsNonBlocking()
			argv := buildBazelYarnArgv(
				"--silent", "--cwd", dirs.clusterUI, "build:watch",
			)
			err = nbExec.CommandContextInheritingStdStreams(ctx, "bazel", argv...)
			if err != nil {
				log.Fatalf("Unable to watch cluster-ui for changes: %v", err)
				return err
			}

			var webpackDist string
			if isOss {
				webpackDist = "oss"
			} else {
				webpackDist = "ccl"
			}

			args = []string{
				"--silent",
				"--cwd",
				dirs.dbConsole,
				"webpack-dev-server",
				"--config", "webpack.app.js",
				"--mode", "development",
				// Polyfill WEBPACK_SERVE for webpack v4; it's set in webpack v5 via `webpack serve`
				"--env.WEBPACK_SERVE",
				"--env.dist=" + webpackDist,
				"--env.target=" + dbTarget,
				"--port", port,
			}
			if secure {
				args = append(args, "--https")
			}

			argv = buildBazelYarnArgv(args...)

			// Start the db-console web server + watcher
			err = nbExec.CommandContextInheritingStdStreams(ctx, "bazel", argv...)
			if err != nil {
				log.Fatalf("Unable to serve db-console: %v", err)
				return err
			}

			// Wait for OS signals to cancel if we're not in test-mode
			if !d.exec.IsDryrun() {
				<-ctx.Done()
			}
			return nil
		},
	}

	watchCmd.Flags().Int16P(portFlag, "p", 3000, "port to serve UI on")
	watchCmd.Flags().String(dbTargetFlag, "http://localhost:8080", "url to proxy DB requests to")
	watchCmd.Flags().Bool(secureFlag, false, "serve via HTTPS")
	watchCmd.Flags().Bool(ossFlag, false, "build only the open-source parts of the UI")

	return watchCmd
}

func makeUILintCmd(d *dev) *cobra.Command {
	const (
		verboseFlag = "verbose"
	)

	lintCmd := &cobra.Command{
		Use:   "lint",
		Short: "Runs linters, format-checkers, and other static analysis on UI-related files",
		Long: `Runs linters (e.g. eslint), format-checkers (e.g. prettier), and other static analysis on UI-related files.

Replaces 'make ui-lint'.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			ctx := cmd.Context()

			isVerbose, err := cmd.Flags().GetBool(verboseFlag)
			if err != nil {
				return err
			}

			// Build prerequisites for db-console and cluster-ui
			args := []string{
				"test",
				"//pkg/ui:lint",
			}
			args = append(args,
				d.getTestOutputArgs(false /* stream */, isVerbose, false /* showLogs */)...,
			)

			logCommand("bazel", args...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

			if err != nil {
				log.Fatalf("failed to lint UI projects: %v", err)
				return err
			}

			return nil
		},
	}

	lintCmd.Flags().Bool(verboseFlag, false, "show all linter output")

	return lintCmd
}

// arrangeFilesForTestWatchers moves files from Bazel's build output directory
// into the locations they'd be found during a non-Bazel build, so that test
// watchers can successfully operate outside of the Bazel sandbox.
//
// Unfortunately, Bazel's reliance on symlinks conflicts with Jest's refusal to
// traverse symlinks during `--watch` mode. Ibazel is unable to keep `jest`
// running between changes, since Jest won't find changes. But having ibazel
// kill Jest when a file changes defeat the purpose of Jest's watch mode, and
// makes devs pay the cost of node + jest startup times after every file change.
// As a workaround, arrangeFilesForTestWatchers copies files out of the Bazel
// sandbox and allows Jest (in watch mode) to be executed from directly within
// a pkg/ui/workspaces/... directory.
//
// See https://github.com/bazelbuild/rules_nodejs/issues/2028
func arrangeFilesForTestWatchers(d *dev) error {
	bazelBin, err := d.getBazelBin(d.cli.Context())
	if err != nil {
		return err
	}

	ossProtobufSrc := filepath.Join(bazelBin, "pkg", "ui", "workspaces", "db-console")
	cclProtobufSrc := filepath.Join(ossProtobufSrc, "ccl")

	dstDirs, err := getUIDirs(d)
	if err != nil {
		return err
	}
	ossProtobufDst := dstDirs.dbConsole
	cclProtobufDst := filepath.Join(ossProtobufDst, "ccl")

	protoFiles := []string{
		filepath.Join("src", "js", "protos.js"),
		filepath.Join("src", "js", "protos.d.ts"),
	}

	// Recreate protobuf client files that were previously copied out of the sandbox
	for _, relPath := range protoFiles {
		ossDst := filepath.Join(ossProtobufDst, relPath)
		cclDst := filepath.Join(cclProtobufDst, relPath)
		if err := d.os.CopyFile(filepath.Join(ossProtobufSrc, relPath), ossDst); err != nil {
			return err
		}
		if err := d.os.CopyFile(filepath.Join(cclProtobufSrc, relPath), cclDst); err != nil {
			return err
		}
	}

	// Delete cluster-ui output tree that was previously copied out of the sandbox
	err = d.os.RemoveAll(filepath.Join(dstDirs.clusterUI, "dist"))
	if err != nil {
		return err
	}

	// Copy the cluster-ui output tree back out of the sandbox
	err = d.os.CopyAll(
		filepath.Join(bazelBin, "pkg", "ui", "workspaces", "cluster-ui", "dist"),
		filepath.Join(dstDirs.clusterUI, "dist"),
	)
	if err != nil {
		return err
	}

	return nil
}

// makeUIWatchCmd initializes the 'ui test' subcommand, which runs unit tests for both db-console and cluster-ui.
func makeUITestCmd(d *dev) *cobra.Command {
	const (
		// watchFlag is the name of the boolean long (GNU-style) flag that determines whether tests should be re-run when
		// source files and test files change
		watchFlag        = "watch"
		verboseFlag      = "verbose"
		streamOutputFlag = "stream-output"
	)

	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Runs tests for UI subprojects",
		Long: `Runs unit tests for db-console and cluster-ui, optionally setting up watchers to rerun those tests when files change.

Replaces 'make ui-test' and 'make ui-test-watch'.`,
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			isDebug, _ := cmd.PersistentFlags().GetBool("debug")

			// Create a context that cancels when OS signals come in
			ctx, stop := signal.NotifyContext(d.cli.Context(), os.Interrupt, os.Kill)
			defer stop()

			isWatch := mustGetFlagBool(cmd, watchFlag)
			isVerbose := mustGetFlagBool(cmd, verboseFlag)
			isStreamOutput := mustGetFlagBool(cmd, streamOutputFlag)

			if isWatch {
				// Build prerequisites for watch-mode only. Non-watch tests are run through bazel, so it'll take care of this
				// for us.
				args := []string{
					"build",
					"//pkg/ui/workspaces/cluster-ui:cluster-ui",
				}
				logCommand("bazel", args...)
				err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

				if err != nil {
					log.Fatalf("failed to build UI test prerequisites: %v", err)
					return err
				}

				err = arrangeFilesForTestWatchers(d)
				if err != nil {
					// nolint:errwrap
					return fmt.Errorf("unable to arrange files properly for watch-mode testing: %+v", err)
				}

				dirs, err := getUIDirs(d)
				if err != nil {
					log.Fatalf("unable to find cluster-ui and db-console directories: %v", err)
					return err
				}

				nbExec := d.exec.AsNonBlocking()

				argv := buildBazelYarnArgv(
					"--silent",
					"--cwd",
					dirs.dbConsole,
					"karma:watch",
				)

				env := append(os.Environ(), "BAZEL_TARGET=fake")
				logCommand("yarn", args...)
				err = nbExec.CommandContextWithEnv(ctx, env, "bazel", argv...)
				if err != nil {
					// nolint:errwrap
					return fmt.Errorf("unable to start db-console tests in watch mode: %+v", err)
				}

				argv = buildBazelYarnArgv(
					"--silent",
					"--cwd",
					dirs.clusterUI,
					"jest",
					"--watch",
				)
				logCommand("yarn", args...)
				env = append(os.Environ(), "BAZEL_TARGET=fake", "CI=1")
				err = nbExec.CommandContextWithEnv(ctx, env, "bazel", argv...)
				if err != nil {
					// nolint:errwrap
					return fmt.Errorf("unable to start cluster-ui tests in watch mode: %+v", err)
				}

				// Wait for OS signals to cancel if we're not in test-mode
				if !d.exec.IsDryrun() {
					<-ctx.Done()
				}
			} else {
				testOutputArg := d.getTestOutputArgs(
					isWatch || isStreamOutput,
					isVerbose,
					false, // showLogs
				)
				args := append([]string{
					"test",
					"//pkg/ui/workspaces/db-console:karma",
					"//pkg/ui/workspaces/cluster-ui:jest",
				}, testOutputArg...)

				if isDebug {
					args = append(args, "--subcommands")
				}

				logCommand("bazel", args...)
				err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
				if err != nil {
					return err
				}
			}

			return nil
		},
	}

	testCmd.Flags().Bool(watchFlag, false, "watch source and test files for changes and rerun tests")
	testCmd.Flags().BoolP(verboseFlag, "v", false, "show all test results when tests complete")
	testCmd.Flags().Bool(streamOutputFlag, false, "stream test output during run (default: true with --watch)")

	return testCmd
}

// buildBazelYarnArgv returns the provided argv formatted so it can be run with
// the bazel-provided version of yarn via `d.exec.CommandContextWithEnv`, e.g.:
//
//     argv := buildBazelYarnArgv("--cwd", "/path/to/dir", "run", "some-target")
//     d.exec.CommandContextWithEnv(ctx, env, "bazel", argv)
func buildBazelYarnArgv(argv ...string) []string {
	return append([]string{
		"run", "@nodejs//:yarn", "--",
	}, argv...)
}
