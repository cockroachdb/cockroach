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

import "github.com/spf13/cobra"

func makeGoCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:     "go <arguments>",
		Short:   "Run `go` with the given arguments",
		Long:    "Run `go` with the given arguments",
		Example: "dev go mod tidy",
		Args:    cobra.MinimumNArgs(0),
		RunE:    runE,
	}
}

func (d *dev) gocmd(cmd *cobra.Command, commandLine []string) error {
	beforeDash, afterDash := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	args := []string{"run", "@go_sdk//:bin/go", "--ui_event_filters=-DEBUG,-info,-stdout,-stderr", "--noshow_progress", "--"}
	args = append(args, beforeDash...)
	args = append(args, afterDash...)
	return d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...)
}
