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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type parameters struct {
	Token string // GitHub API token
	Sha   string
}

var rootCmd = &cobra.Command{
	Use:   "docs-issue-generation",
	Short: "Generate a new set of release issues in the docs repo for a given commit.",
	Run: func(_ *cobra.Command, args []string) {
		params := defaultEnvParameters()
		docsIssueGeneration(params)
	},
}

func main() {
	fmt.Println(defaultEnvParameters().Token)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func defaultEnvParameters() parameters {
	const (
		githubTokenEnv = "GITHUB_API_TOKEN"
		buildVcsNumber = "BUILD_VCS_NUMBER"
	)

	return parameters{
		Token: maybeEnv(githubTokenEnv, ""),
		Sha:   maybeEnv(buildVcsNumber, "964b5b4d70f058a612b7f1ea25e292740b199139"),
	}
}

func maybeEnv(envKey, defaultValue string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return defaultValue
	}
	return v
}
