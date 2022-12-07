// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Check that GitHub PR descriptions and commit messages contain the
// expected epic and issue references.
package main

import (
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "docs-issue-generation",
	Short: "Generate a new set of release issues in the docs repo for a given commit.",
	Run: func(_ *cobra.Command, args []string) {
		params := defaultEnvParameters()
		docsIssueGeneration(params)
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func defaultEnvParameters() parameters {
	const (
		githubAPITokenEnv = "GITHUB_API_TOKEN"
		dryRunEnv         = "DRY_RUN_DOCS_ISSUE_GEN"
		startTimeEnv      = "DOCS_ISSUE_GEN_START_TIME"
		endTimeEnv        = "DOCS_ISSUE_GEN_END_TIME"
	)
	defaultStartTimeStr := timeutil.Now().Add(time.Hour * (-48)).Format(time.RFC3339)
	defaultEndTimeStr := timeutil.Now().Format(time.RFC3339)

	startTimeStr := maybeEnv(startTimeEnv, defaultStartTimeStr)
	endTimeStr := maybeEnv(endTimeEnv, defaultEndTimeStr)

	startTimeTime, _ := time.Parse(time.RFC3339, startTimeStr)
	endTimeTime, _ := time.Parse(time.RFC3339, endTimeStr)

	return parameters{
		Token:     maybeEnv(githubAPITokenEnv, ""),
		DryRun:    stringToBool(maybeEnv(dryRunEnv, "false")),
		StartTime: startTimeTime,
		EndTime:   endTimeTime,
	}
}

func maybeEnv(envKey, defaultValue string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return defaultValue
	}
	return v
}

func stringToBool(input string) bool {
	return input == "true"
}
