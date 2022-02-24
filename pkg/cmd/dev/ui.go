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

			// `yarn` is required to be on a user's path, since it won't run via Bazel
			err = d.ensureBinaryInPath("yarn")
			if err != nil {
				return err
			}

			dirs, err := getUIDirs(d)
			if err != nil {
				log.Fatalf("unable to find cluster-ui and db-console directories: %v", err)
				return err
			}

			// Start the cluster-ui watch task
			nbExec := d.exec.AsNonBlocking()
			err = nbExec.CommandContextInheritingStdStreams(ctx, "yarn", "--silent", "--cwd", dirs.clusterUI, "build:watch")
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

			// Start the db-console web server + watcher
			err = nbExec.CommandContextInheritingStdStreams(ctx, "yarn", args...)
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
