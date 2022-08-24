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
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "lint-epic-issue-refs PR_NUMBER",
	Short: "Check the body and commit messages of the indicated PR for epic and issue refs",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		params := defaultEnvParameters()
		prNumber, err := parseArgs(args)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}

		params.PrNumber = prNumber
		err = lintPR(params)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func parseArgs(args []string) (int, error) {
	if len(args) < 1 {
		return 0, errors.New("no PR number specified")
	}
	if len(args) > 1 {
		return 0, errors.New("one argument is required: a PR number")
	}
	prNumber, err := strconv.Atoi(args[0])
	if err != nil {
		return 0, fmt.Errorf("invalid PR number: %w", err)
	}

	return prNumber, nil
}

type parameters struct {
	Token    string // GitHub API token
	Org      string
	Repo     string
	PrNumber int
}

func defaultEnvParameters() *parameters {
	const (
		githubTokenEnv = "GITHUB_TOKEN"
		githubOrgEnv   = "GITHUB_ORG"
		githubRepoEnv  = "GITHUB_REPO"
	)

	return &parameters{
		Token: maybeEnv(githubTokenEnv, ""),
		Org:   maybeEnv(githubOrgEnv, "cockroachdb"),
		Repo:  maybeEnv(githubRepoEnv, "cockroach"),
	}
}

func maybeEnv(envKey, defaultValue string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return defaultValue
	}
	return v
}
