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
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

const (
	// ossFlag is the name of the boolean long (GNU-style) flag that builds only
	// the open-source parts of the UI.
	ossFlag = "oss"
)

// makeUICmd initializes the top-level 'ui' subcommand.
func makeUICmd(d *dev) *cobra.Command {
	uiCmd := &cobra.Command{
		Use:   "ui",
		Short: "Runs UI-related tasks",
		Long:  "Builds UI & runs UI-related commands to ease development flows",
	}

	uiCmd.AddCommand(makeUICleanCmd(d))
	uiCmd.AddCommand(makeUILintCmd(d))
	uiCmd.AddCommand(makeUITestCmd(d))
	uiCmd.AddCommand(makeUIWatchCmd(d))
	uiCmd.AddCommand(makeUIE2eCmd(d))
	uiCmd.AddCommand(makeMirrorDepsCmd(d))
	uiCmd.AddCommand(makeUIStorybookCmd(d))

	return uiCmd
}

// UIDirectories contains the absolute path to the root of each UI sub-project.
type UIDirectories struct {
	workspace string
	// workspace is the absolute path to ./ .
	// root is the absolute path to ./pkg/ui.
	root string
	// clusterUI is the absolute path to ./pkg/ui/workspaces/cluster-ui.
	clusterUI string
	// dbConsole is the absolute path to ./pkg/ui/workspaces/db-console.
	dbConsole string
	// e2eTests is the absolute path to ./pkg/ui/workspaces/e2e-tests.
	e2eTests string
	// eslintPlugin is the absolute path to ./pkg/ui/workspaces/eslint-plugin-crdb.
	eslintPlugin string
	// protoOss is the absolute path to ./pkg/ui/workspaces/db-console/src/js/.
	protoOss string
	// protoCcl is the absolute path to ./pkg/ui/workspaces/db-console/ccl/src/js/.
	protoCcl string
}

// getUIDirs computes the absolute path to the root of each UI sub-project.
func getUIDirs(d *dev) (*UIDirectories, error) {
	workspace, err := d.getWorkspace(d.cli.Context())
	if err != nil {
		return nil, err
	}

	return &UIDirectories{
		workspace:    workspace,
		root:         filepath.Join(workspace, "./pkg/ui"),
		clusterUI:    filepath.Join(workspace, "./pkg/ui/workspaces/cluster-ui"),
		dbConsole:    filepath.Join(workspace, "./pkg/ui/workspaces/db-console"),
		e2eTests:     filepath.Join(workspace, "./pkg/ui/workspaces/e2e-tests"),
		eslintPlugin: filepath.Join(workspace, "./pkg/ui/workspaces/eslint-plugin-crdb"),
		protoOss:     filepath.Join(workspace, "./pkg/ui/workspaces/db-console/src/js"),
		protoCcl:     filepath.Join(workspace, "./pkg/ui/workspaces/db-console/ccl/src/js"),
	}, nil
}

// assertNoLinkedNpmDeps looks for JS packages linked outside the Bazel
// workspace (typically via `pnpm link`). It returns an error if:
//
// 'targets' contains a Bazel target that requires the web UI
// AND
// a node_modules/ tree exists within pkg/ui (or its subtrees)
// AND
// a @cockroachlabs-scoped package is symlinked to an external directory
//
// (or if any error occurs while performing one of those checks).
func (d *dev) assertNoLinkedNpmDeps(targets []buildTarget) error {
	uiWillBeBuilt := false
	for _, target := range targets {
		// TODO: This could potentially be a bazel query, e.g.
		// 'somepath(${target.fullName}, //pkg/ui/workspaces/db-console:*)' or
		// similar, but with only two eligible targets it doesn't seem quite
		// worth it.
		if target.fullName == cockroachTarget || target.fullName == cockroachTargetOss {
			uiWillBeBuilt = true
			break
		}
	}
	if !uiWillBeBuilt {
		// If no UI build is required, the presence of an externally-linked
		// package doesn't matter.
		return nil
	}

	// Find the current workspace and build some relevant absolute paths.
	uiDirs, err := getUIDirs(d)
	if err != nil {
		return fmt.Errorf("could not check for linked NPM dependencies: %w", err)
	}

	jsPkgRoots := []string{
		uiDirs.root,
		uiDirs.eslintPlugin,
		uiDirs.protoOss,
		uiDirs.protoCcl,
		uiDirs.clusterUI,
		uiDirs.dbConsole,
		uiDirs.e2eTests,
	}

	type LinkedPackage struct {
		name string
		dir  string
	}

	anyPackageEscapesWorkspace := false

	// Check for symlinks in each package's node_modules/@cockroachlabs/ dir.
	for _, jsPkgRoot := range jsPkgRoots {
		crlModulesPath := filepath.Join(jsPkgRoot, "node_modules/@cockroachlabs")
		crlDeps, err := d.os.ReadDir(crlModulesPath)

		// If node_modules/@cockroachlabs doesn't exist, it's likely that JS
		// dependencies haven't been installed outside the Bazel workspace.
		// This is expected for non-UI devs, and is a safe state.
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("could not @cockroachlabs/ packages: %w", err)
		}

		linkedPackages := []LinkedPackage{}

		// For each dependency in node_modules/@cockroachlabs/ ...
		for _, depName := range crlDeps {
			// Ignore empty strings, which are produced by d.os.ReadDir in
			// dry-run mode.
			if depName == "" {
				continue
			}

			depPath := filepath.Join(crlModulesPath, depName)

			// Determine if depName is a symlink. It should be when dependencies
			// are installed via pnpm, but there are cases involving stale
			// node_modules/ trees that can result in it being a regular
			// directory.
			isSymlink, err := d.os.IsSymlink(depPath)
			if err != nil {
				return fmt.Errorf("could not determine if %s is a symlink: %w", depPath, err)
			}

			// Regular directories cannot (by definition) link outside the Bazel
			// workspace, so are safe to ignore.
			if !isSymlink {
				continue
			}

			// Resolve the symlink.
			resolved, err := d.os.Readlink(depPath)
			if err != nil {
				return fmt.Errorf("could not evaluate symlink %s: %w", depPath, err)
			}

			// Convert it to a path relative to the Bazel workspace root to make
			// it obvious when a link escapes the workspace.
			relativeToWorkspace, err := filepath.Rel(
				uiDirs.workspace,
				filepath.Join(crlModulesPath, resolved),
			)
			if err != nil {
				return fmt.Errorf("could not relativize path %s: %w", resolved, err)
			}

			// If it doesn't start with '..', it doesn't escape the Bazel
			// workspace.
			// TODO: Once Go 1.20 is supported here, switch to filepath.IsLocal.
			if !strings.HasPrefix(relativeToWorkspace, "..") {
				continue
			}

			// This package escapes the Bazel workspace! Add it to the queue
			// with its absolute path for simpler presentation to users.
			abs, err := filepath.Abs(relativeToWorkspace)
			if err != nil {
				return fmt.Errorf("could not absolutize path %s: %w", resolved, err)
			}

			linkedPackages = append(
				linkedPackages,
				LinkedPackage{
					name: "@cockroachlabs/" + depName,
					dir:  abs,
				},
			)
		}

		// If this internal package has no dependencies provided by pnpm link,
		// move on without logging anything.
		if len(linkedPackages) == 0 {
			continue
		}

		if !anyPackageEscapesWorkspace {
			anyPackageEscapesWorkspace = true
			log.Println("Externally-linked package(s) detected:")
		}

		log.Printf("pkg/ui/workspaces/%s:", filepath.Base(jsPkgRoot))
		for _, pkg := range linkedPackages {
			log.Printf("    %s <- %s\n", pkg.name, pkg.dir)
		}
		log.Println()
	}

	if anyPackageEscapesWorkspace {
		msg := strings.TrimSpace(`
At least one JS dependency is linked to another directory on your machine.
Bazel cannot see changes in these packages, which could lead to both
false-positive and false-negative behavior in the UI.
This build has been pre-emptively failed.

To build without the UI, run:
    dev build short
To remove all linked dependencies, run:
    dev ui clean --all
`) + "\n"

		return fmt.Errorf("%s", msg)
	}

	return nil
}

// makeUIWatchCmd initializes the 'ui watch' subcommand, which sets up a
// live-reloading HTTP server for db-console and a file-watching rebuilder for
// cluster-ui.
func makeUIWatchCmd(d *dev) *cobra.Command {
	const (
		// portFlag is the name of the long (GNU-style) flag that controls which
		// port webpack's dev server listens on.
		portFlag = "port"
		// dbTargetFlag is the name of the long (GNU-style) flag that determines
		// which DB instance to proxy to.
		dbTargetFlag = "db"
		// secureFlag is the name of the boolean long (GNU-style) flag that makes
		// webpack's dev server use HTTPS.
		secureFlag = "secure"
		// clusterUiDestinationsFlag is the name of the long (GNU-style) flag that
		// tells webpack where to copy emitted files during watch mode.
		clusterUiDestinationsFlag = "cluster-ui-dst"
	)

	watchCmd := &cobra.Command{
		Use:   "watch",
		Short: "Watches for file changes and automatically rebuilds",
		Long: `Starts a local web server that watches for JS file changes, automatically regenerating the UI

Replaces 'make ui-watch'.`,
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			// Create a context that cancels when OS signals come in.
			ctx, stop := signal.NotifyContext(d.cli.Context(), os.Interrupt, os.Kill)
			defer stop()

			dirs, err := getUIDirs(d)
			if err != nil {
				log.Fatalf("unable to find cluster-ui and db-console directories: %v", err)
				return err
			}

			isOss, err := cmd.Flags().GetBool(ossFlag)
			if err != nil {
				return err
			}

			// Ensure node dependencies are up-to-date.
			err = d.exec.CommandContextInheritingStdStreams(
				ctx,
				"pnpm",
				"--dir",
				dirs.root,
				"install",
			)
			if err != nil {
				log.Fatalf("failed to fetch node dependencies: %v", err)
			}

			// Build prerequisites for db-console
			args := []string{
				"build",
				"//pkg/ui/workspaces/cluster-ui:cluster-ui-lib",
			}
			if !isOss {
				args = append(args, "//pkg/ui/workspaces/db-console/ccl/src/js:crdb-protobuf-client-ccl-lib")
			}
			logCommand("bazel", args...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

			if err != nil {
				log.Fatalf("failed to build UI watch prerequisites: %v", err)
				return err
			}

			if err := arrangeFilesForWatchers(d, isOss); err != nil {
				log.Fatalf("failed to arrange files for watchers: %v", err)
				return err
			}

			// Extract remaining flags.
			portNumber, err := cmd.Flags().GetInt16(portFlag)
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
				return err
			}
			port := fmt.Sprint(portNumber)
			dbTarget := mustGetFlagString(cmd, dbTargetFlag)
			_, err = url.Parse(dbTarget)
			if err != nil {
				log.Fatalf("invalid format for --%s argument: %v", dbTargetFlag, err)
				return err
			}
			secure := mustGetFlagBool(cmd, secureFlag)
			clusterUiDestinations := mustGetFlagStringArray(cmd, clusterUiDestinationsFlag)

			// Start the cluster-ui watch tasks
			nbExec := d.exec.AsNonBlocking()
			{
				argv := []string{
					"--dir", dirs.clusterUI, "run", "build:watch",
				}

				// Add additional webpack args to copy cluster-ui output to external directories.
				for _, dst := range clusterUiDestinations {
					argv = append(argv, "--env.copy-to="+dst)
				}

				err = nbExec.CommandContextInheritingStdStreams(ctx, "pnpm", argv...)
				if err != nil {
					log.Fatalf("Unable to start webpack watcher cluster-ui for changes: %v", err)
					return err
				}
			}

			{
				argv := []string{
					"--dir", dirs.clusterUI, "run", "tsc:watch",
				}

				// Add additional webpack args to copy cluster-ui output to external directories.
				for _, dst := range clusterUiDestinations {
					argv = append(argv, "--copy-to="+dst)
				}

				err = nbExec.CommandContextInheritingStdStreams(ctx, "pnpm", argv...)
				if err != nil {
					log.Fatalf("Unable to start typescript cluster-ui for changes: %v", err)
					return err
				}
			}

			var webpackDist string
			if isOss {
				webpackDist = "oss"
			} else {
				webpackDist = "ccl"
			}

			args = []string{
				"--dir",
				dirs.dbConsole,
				"exec",
				"webpack-dev-server",
				"--config", "webpack.config.js",
				"--mode", "development",
				// Polyfill WEBPACK_SERVE for webpack v4; it's set in webpack v5 via
				// `webpack serve`.
				"--env.WEBPACK_SERVE",
				"--env.dist=" + webpackDist,
				"--env.target=" + dbTarget,
				"--port", port,
			}
			if secure {
				args = append(args, "--https")
			}

			// Start the db-console web server + watcher
			err = nbExec.CommandContextInheritingStdStreams(ctx, "pnpm", args...)
			if err != nil {
				log.Fatalf("Unable to serve db-console: %v", err)
				return err
			}

			// Wait for OS signals to cancel if we're not in test-mode.
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
	watchCmd.Flags().StringArray(
		clusterUiDestinationsFlag,
		[]string{},
		"directory to copy emitted cluster-ui files to. Can be set multiple times.",
	)

	return watchCmd
}

// makeUIStorybookCmd initializes the 'ui storybook' subcommand, which runs
// storybook for either db-console or cluster-ui projects.
func makeUIStorybookCmd(d *dev) *cobra.Command {
	const (
		// projectFlag indicates whether storybook should be run for db-console or
		// cluster-ui projects
		projectFlag = "project"
		// portFlag defines the port to run Storybook
		portFlag = "port"
	)

	storybookCmd := &cobra.Command{
		Use:   "storybook",
		Short: "Runs Storybook in watch mode",
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			// Create a context that cancels when OS signals come in.
			ctx, stop := signal.NotifyContext(d.cli.Context(), os.Interrupt, os.Kill)
			defer stop()

			// Fetch all node dependencies (through Bazel) to ensure things are up-to-date
			err := d.exec.CommandContextInheritingStdStreams(
				ctx,
				"bazel",
				"fetch",
				"//pkg/ui/workspaces/cluster-ui:cluster-ui",
				"//pkg/ui/workspaces/db-console:db-console-ccl",
			)
			if err != nil {
				log.Fatalf("failed to fetch node dependencies: %v", err)
			}

			// Build prerequisites for Storybook. It runs Db Console with CCL license.
			args := []string{
				"build",
				"//pkg/ui/workspaces/cluster-ui:cluster-ui",
				"//pkg/ui/workspaces/db-console/ccl/src/js:crdb-protobuf-client-ccl",
			}

			logCommand("bazel", args...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
			if err != nil {
				log.Fatalf("failed to build UI watch prerequisites: %v", err)
				return err
			}

			if err := arrangeFilesForWatchers(d /* ossOnly */, false); err != nil {
				log.Fatalf("failed to arrange files for watchers: %v", err)
				return err
			}

			dirs, err := getUIDirs(d)
			if err != nil {
				log.Fatalf("unable to find cluster-ui and db-console directories: %v", err)
				return err
			}

			portNumber, err := cmd.Flags().GetInt16(portFlag)
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
				return err
			}
			port := fmt.Sprint(portNumber)

			project, err := cmd.Flags().GetString(projectFlag)
			if err != nil {
				log.Fatalf("unexpected error: %v", err)
				return err
			}

			projectToPath := map[string]string{
				"db-console": dirs.dbConsole,
				"cluster-ui": dirs.clusterUI,
			}

			cwd, ok := projectToPath[project]
			if !ok {
				err = fmt.Errorf("unexpected project name (%s) provided with --project flag", project)
				log.Fatalf("%v", err)
				return err
			}

			nbExec := d.exec.AsNonBlocking()

			args = []string{
				"--dir", cwd,
				"exec",
				"start-storybook",
				"-p", port,
			}

			err = nbExec.CommandContextInheritingStdStreams(ctx, "pnpm", args...)
			if err != nil {
				log.Fatalf("Unable to run Storybook for %s : %v", project, err)
				return err
			}

			// Wait for OS signals to cancel if we're not in test-mode.
			if !d.exec.IsDryrun() {
				<-ctx.Done()
			}
			return nil
		},
	}

	storybookCmd.Flags().Int16P(portFlag, "p", 6006, "port to run Storybook")
	storybookCmd.Flags().String(projectFlag, "db-console", "db-console, cluster-ui")

	return storybookCmd
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

			bazelTestArgs := d.getTestOutputArgs(
				isVerbose,
				false, /* showLogs */
				false, /* streamOutput */
			)

			// Check for unmirrored dependencies before running the lint target, since that lint
			// target requires all dependencies to be mirrored. The presence of any unmirrored
			// dependencies during //pkg/ui:lint results in cryptic errors that don't help users.
			targets := []string{"//pkg/cmd/mirror/npm:list_unmirrored_dependencies", "//pkg/ui:lint"}
			for _, target := range targets {
				args := append(
					[]string{"test", target},
					bazelTestArgs...,
				)

				logCommand("bazel", args...)
				err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

				if err != nil {
					log.Fatalf("failed to lint UI projects: %v", err)
					return err
				}
			}
			return nil
		},
	}

	lintCmd.Flags().Bool(verboseFlag, false, "show all linter output")

	return lintCmd
}

func makeUICleanCmd(d *dev) *cobra.Command {
	const (
		allFlag = "all"
	)

	cleanCmd := &cobra.Command{
		Use:   "clean [--all]",
		Short: "clean artifacts produced by `dev ui`",
		Long:  "Clean the workspace of artifacts produced by `dev ui`. Pass the --all option to also delete all installed dependencies.",
		Args:  cobra.MaximumNArgs(0),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			all := mustGetFlagBool(cmd, allFlag)
			uiDirs, err := getUIDirs(d)
			if err != nil {
				return err
			}
			pathsToDelete := []string{
				filepath.Join(uiDirs.dbConsole, "src", "js", "protos.js"),
				filepath.Join(uiDirs.dbConsole, "src", "js", "protos.d.ts"),
				filepath.Join(uiDirs.dbConsole, "ccl", "src", "js", "protos.js"),
				filepath.Join(uiDirs.dbConsole, "ccl", "src", "js", "protos.d.ts"),
				filepath.Join(uiDirs.dbConsole, "dist"),
				filepath.Join(uiDirs.clusterUI, "dist"),
				filepath.Join(uiDirs.eslintPlugin, "dist"),
			}
			if all {
				workspace, err := d.getWorkspace(d.cli.Context())
				if err != nil {
					return err
				}

				pathsToDelete = append(
					pathsToDelete,
					filepath.Join(workspace, "pkg", "ui", "node_modules"),
					filepath.Join(uiDirs.dbConsole, "node_modules"),
					filepath.Join(uiDirs.dbConsole, "src", "js", "node_modules"),
					filepath.Join(uiDirs.dbConsole, "ccl", "src", "js", "node_modules"),
					filepath.Join(uiDirs.clusterUI, "node_modules"),
					filepath.Join(uiDirs.e2eTests, "node_modules"),
					filepath.Join(uiDirs.eslintPlugin, "node_modules"),
				)
			}

			for _, toDelete := range pathsToDelete {
				err := d.os.RemoveAll(toDelete)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cleanCmd.Flags().Bool(allFlag, false, "additionally clean installed dependencies")

	return cleanCmd
}

// arrangeFilesForWatchers moves files from Bazel's build output directory
// into the locations they'd be found during a non-Bazel build, so that test
// watchers can successfully operate outside of the Bazel sandbox.
//
// Unfortunately, Bazel's reliance on symlinks conflicts with Jest's refusal to
// traverse symlinks during `--watch` mode. Ibazel is unable to keep `jest`
// running between changes, since Jest won't find changes. But having ibazel
// kill Jest when a file changes defeat the purpose of Jest's watch mode, and
// makes devs pay the cost of node + jest startup times after every file change.
// Similar issues apply to webpack's watch-mode, as compiled output doesn't
// exist outside of the Bazel sandbox. As a workaround, arrangeFilesForWatchers
// copies files out of the Bazel sandbox and allows Jest or webpack (in watch
// mode) to be executed from directly within a pkg/ui/workspaces/... directory.
//
// See https://github.com/bazelbuild/rules_nodejs/issues/2028
func arrangeFilesForWatchers(d *dev, ossOnly bool) error {
	bazelBin, err := d.getBazelBin(d.cli.Context(), []string{})
	if err != nil {
		return err
	}

	dbConsoleSrc := filepath.Join(bazelBin, "pkg", "ui", "workspaces", "db-console")
	dbConsoleCclSrc := filepath.Join(dbConsoleSrc, "ccl")

	dstDirs, err := getUIDirs(d)
	if err != nil {
		return err
	}
	dbConsoleDst := dstDirs.dbConsole
	dbConsoleCclDst := filepath.Join(dbConsoleDst, "ccl")

	protoFiles := []string{
		filepath.Join("src", "js", "protos.js"),
		filepath.Join("src", "js", "protos.d.ts"),
	}

	// Recreate protobuf client files that were previously copied out of the sandbox
	for _, relPath := range protoFiles {
		ossDst := filepath.Join(dbConsoleDst, relPath)
		if err := d.os.CopyFile(filepath.Join(dbConsoleSrc, relPath), ossDst); err != nil {
			return err
		}

		if ossOnly {
			continue
		}

		cclDst := filepath.Join(dbConsoleCclDst, relPath)
		if err := d.os.CopyFile(filepath.Join(dbConsoleCclSrc, relPath), cclDst); err != nil {
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
					"//pkg/ui/workspaces/db-console/ccl/src/js:crdb-protobuf-client-ccl",
				}
				logCommand("bazel", args...)
				err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)

				if err != nil {
					log.Fatalf("failed to build UI test prerequisites: %v", err)
					return err
				}

				err = arrangeFilesForWatchers(d, false /* ossOnly */)
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

				argv := []string{
					"--dir",
					dirs.dbConsole,
					"exec",
					"jest",
					"--watch",
					"-w4",
				}
				env := append(os.Environ(), "BAZEL_TARGET=fake")
				logCommand("pnpm", args...)
				err = nbExec.CommandContextWithEnv(ctx, env, "pnpm", argv...)
				if err != nil {
					// nolint:errwrap
					return fmt.Errorf("unable to start db-console tests in watch mode: %+v", err)
				}

				argv = []string{
					"--dir",
					dirs.clusterUI,
					"exec",
					"jest",
					"--watch",
				}
				logCommand("pnpm", args...)
				env = append(os.Environ(), "BAZEL_TARGET=fake", "CI=1")
				err = nbExec.CommandContextWithEnv(ctx, env, "pnpm", argv...)
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
					isVerbose,
					false,                     // showLogs
					isWatch || isStreamOutput, // streamOutput
				)
				args := append([]string{
					"test",
					"//pkg/ui/workspaces/db-console:jest",
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

func makeUIE2eCmd(d *dev) *cobra.Command {
	const headedFlag = "headed"

	e2eTestCmd := &cobra.Command{
		Use:   "e2e -- [args passed to Cypress ...]",
		Short: "run e2e (Cypress) tests",
		Long: strings.TrimSpace(`
Run end-to-end tests with Cypress, spinning up a real local cluster and
launching test in a real browser. Extra flags are passed directly to the
'cypress' binary".
`),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			ctx := cmd.Context()

			isHeaded := mustGetFlagBool(cmd, headedFlag)
			var pnpmTarget string = "cy:run"
			if isHeaded {
				pnpmTarget = "cy:debug"
			}

			uiDirs, err := getUIDirs(d)
			if err != nil {
				return err
			}

			workspace, err := d.getWorkspace(ctx)
			if err != nil {
				return err
			}

			// Ensure e2e-tests dependencies are installed
			installArgv := []string{"--dir", uiDirs.e2eTests, "install"}
			logCommand("pnpm", installArgv...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "pnpm", installArgv...)
			if err != nil {
				return fmt.Errorf("unable to install NPM dependencies: %w", err)
			}

			// Build cockroach (relying on Bazel to short-circuit repeated builds)
			buildCockroachArgv := []string{
				"build",
				"//pkg/cmd/cockroach:cockroach",
			}
			logCommand("bazel", buildCockroachArgv...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "bazel", buildCockroachArgv...)
			if err != nil {
				return fmt.Errorf("unable to build cockroach with UI: %w", err)
			}

			// Ensure the native Cypress binary is installed.
			cyInstallArgv := []string{"--dir", uiDirs.e2eTests, "exec", "cypress", "install"}
			logCommand("pnpm", cyInstallArgv...)
			err = d.exec.CommandContextInheritingStdStreams(ctx, "pnpm", cyInstallArgv...)
			if err != nil {
				return fmt.Errorf("unable to install Cypress native package: %w", err)
			}

			// Run Cypress tests, passing any extra args through to 'cypress'
			startCrdbThenSh := filepath.Join(uiDirs.e2eTests, "build/start-crdb-then.sh")
			runCypressArgv := []string{
				"pnpm", "--dir", uiDirs.e2eTests, pnpmTarget,
			}
			runCypressArgv = append(runCypressArgv, cmd.Flags().Args()...)

			logCommand(startCrdbThenSh, runCypressArgv...)
			env := append(
				os.Environ(),
				fmt.Sprintf("COCKROACH=%s", filepath.Join(workspace, "cockroach")),
			)
			err = d.exec.CommandContextWithEnv(ctx, env, startCrdbThenSh, runCypressArgv...)
			if err != nil {
				return fmt.Errorf("error while running Cypress tests: %w", err)
			}

			return nil
		},
	}
	e2eTestCmd.Flags().Bool(headedFlag /* default */, false, "run tests in the interactive Cypress GUI")

	return e2eTestCmd
}

func makeMirrorDepsCmd(d *dev) *cobra.Command {
	mirrorDepsCmd := &cobra.Command{
		Use:   "mirror-deps",
		Short: "mirrors NPM dependencies to Google Cloud Storage",
		Long: strings.TrimSpace(`
Downloads NPM dependencies from public registries, uploads them to a Google
Cloud Storage bucket managed by Cockroach Labs.`),
		RunE: func(cmd *cobra.Command, commandLine []string) error {
			ctx := cmd.Context()

			mirrorArgv := []string{"run", "//pkg/cmd/mirror/npm:mirror_npm_dependencies"}
			logCommand("bazel", mirrorArgv...)
			if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", mirrorArgv...); err != nil {
				return fmt.Errorf("unable to mirror dependencies to GCS: %w", err)
			}

			return nil
		},
	}

	return mirrorDepsCmd
}
