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
		buildVcsNumberEnv = "BUILD_VCS_NUMBER"
	)

	return parameters{
		Token: maybeEnv(githubAPITokenEnv, ""),
		Sha:   maybeEnv(buildVcsNumberEnv, "4dd8da9609adb3acce6795cea93b67ccacfc0270"),
	}
}

func maybeEnv(envKey, defaultValue string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return defaultValue
	}
	return v
}
