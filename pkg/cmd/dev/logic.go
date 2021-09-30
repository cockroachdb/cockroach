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
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	filesFlag    = "files"
	subtestsFlag = "subtests"
	configFlag   = "config"
)

func makeTestLogicCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	testLogicCmd := &cobra.Command{
		Use:   "testlogic {,base,ccl,opt}",
		Short: "Run logic tests.",
		Long:  "Run logic tests.",
		Example: `
	dev testlogic
	dev testlogic ccl
	dev testlogic --files=fk --subtests=20042 --config=local`,
		Args: cobra.MinimumNArgs(0),
		RunE: runE,
	}
	testLogicCmd.Flags().Duration(timeoutFlag, 0*time.Minute, "timeout for test")
	testLogicCmd.Flags().String(filesFlag, "", "run logic tests for files matching this regex")
	testLogicCmd.Flags().String(subtestsFlag, "", "run logic test subtests matching this regex")
	testLogicCmd.Flags().String(configFlag, "", "run logic tests under the specified config")
	return testLogicCmd
}

func (d *dev) testlogic(cmd *cobra.Command, commandLine []string) error {
	choices, additionalBazelArgs := splitArgsAtDash(cmd, commandLine)
	ctx := cmd.Context()
	files := mustGetFlagString(cmd, filesFlag)
	subtests := mustGetFlagString(cmd, subtestsFlag)
	config := mustGetFlagString(cmd, configFlag)
	timeout := mustGetFlagDuration(cmd, timeoutFlag)

	validChoices := []string{"base", "ccl", "opt"}
	if len(choices) == 0 {
		// Default to ALL
		choices = append(choices, validChoices...)
	}
	for _, choice := range choices {
		valid := false
		for _, candidate := range validChoices {
			if candidate == choice {
				valid = true
			}
		}
		if !valid {
			return fmt.Errorf("invalid choice for `dev testlogic`; got %s, must be one of %s", choice, strings.Join(validChoices, ","))
		}
	}

	for _, choice := range choices {
		args := []string{"test"}
		var target, selectorPrefix string
		switch choice {
		case "base":
			target = "//pkg/sql/logictest:logictest_test"
			selectorPrefix = "TestLogic"
		case "ccl":
			target = "//pkg/ccl/logictestccl:logictestccl_test"
			selectorPrefix = "TestCCLLogic"
		case "opt":
			target = "//pkg/sql/opt/exec/execbuilder:execbuilder_test"
			selectorPrefix = "TestExecBuild"
		}
		selector := fmt.Sprintf(
			"%s/%s/%s/%s",
			selectorPrefix,
			maybeAddBeginEndMarkers(config),
			maybeAddBeginEndMarkers(files),
			subtests)
		args = append(args, target)
		args = append(args, "--test_filter", selector)
		args = append(args, "--test_arg", "-test.v")
		if len(files) > 0 {
			args = append(args, "--test_arg", "-show-sql")
		}
		if len(config) > 0 {
			args = append(args, "--test_arg", "-config", "--test_arg", config)
		}
		if timeout > 0 {
			args = append(args, fmt.Sprintf("--test_timeout=%d", int(timeout.Seconds())))
		}
		args = append(args, additionalBazelArgs...)
		logCommand("bazel", args...)
		if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
			return err
		}
	}
	return nil
}

func maybeAddBeginEndMarkers(s string) string {
	if len(s) == 0 {
		return s
	}
	return "^" + s + "$"
}
