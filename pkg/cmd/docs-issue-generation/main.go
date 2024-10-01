// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Check that GitHub PR descriptions and commit messages contain the
// expected epic and issue references.

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "docs-issue-generation",
	Short: "Generate a new set of product change issues in the DOC project in Jira for a given timeframe.",
	RunE: func(_ *cobra.Command, args []string) error {
		params := defaultEnvParameters()
		return docsIssueGeneration(params)
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func defaultEnvParameters() queryParameters {
	const (
		dryRunEnv    = "DRY_RUN_DOCS_ISSUE_GEN"
		startTimeEnv = "DOCS_ISSUE_GEN_START_TIME"
		endTimeEnv   = "DOCS_ISSUE_GEN_END_TIME"
	)
	defaultStartTimeStr := timeutil.Now().Add(time.Hour * (-48)).Format(time.RFC3339)
	defaultEndTimeStr := timeutil.Now().Format(time.RFC3339)

	startTimeStr := maybeEnv(startTimeEnv, defaultStartTimeStr)
	endTimeStr := maybeEnv(endTimeEnv, defaultEndTimeStr)

	startTimeTime, _ := time.Parse(time.RFC3339, startTimeStr)
	endTimeTime, _ := time.Parse(time.RFC3339, endTimeStr)

	return queryParameters{
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
