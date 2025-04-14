// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/go-github/v61/github"
	"golang.org/x/oauth2"
)

type GitHubConfig struct {
	client *github.Client
	Owner  string
	Repo   string
	PrNum  int
}

const (
	CommentTag = "<!-- microbench-ci -->"
	PerfLabel  = "X-perf-check"
)

// NewGithubConfig creates a new GitHubConfig from the environment variables.
func NewGithubConfig() (*GitHubConfig, error) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return nil, errors.New("GITHUB_TOKEN is not set")
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	githubRepository := os.Getenv("GITHUB_REPO")
	if githubRepository == "" {
		return nil, errors.New("GITHUB_REPO is not set")
	}
	prNumber := os.Getenv("GITHUB_PR_NUMBER")
	if prNumber == "" {
		return nil, errors.New("GITHUB_PR_NUMBER is not set")
	}
	prNumberInt, err := strconv.Atoi(prNumber)
	if err != nil {
		return nil, errors.Newf("invalid GITHUB_PR_NUMBER format %s", prNumber)
	}
	repoInfo := strings.Split(githubRepository, "/")
	if len(repoInfo) != 2 {
		return nil, errors.Newf("invalid GitHub repository flag, %s", githubRepository)
	}
	owner := repoInfo[0]
	repo := repoInfo[1]

	return &GitHubConfig{
		client: client,
		Owner:  owner,
		Repo:   repo,
		PrNum:  prNumberInt,
	}, nil
}

// postComment posts a comment on a GitHub PR, with the given summary text, that
// has the CommentTag marker in it.
func (c *GitHubConfig) postComment(summaryText string) error {
	ctx := context.Background()
	commentBody := github.String(fmt.Sprintf("%s\n%s", summaryText, CommentTag))
	_, _, err := c.client.Issues.CreateComment(
		ctx, c.Owner, c.Repo, c.PrNum,
		&github.IssueComment{
			Body: commentBody,
		},
	)
	return err
}

// addLabel adds a label to a GitHub PR.
func (c *GitHubConfig) addLabel(label string) error {
	ctx := context.Background()
	_, _, err := c.client.Issues.AddLabelsToIssue(
		ctx, c.Owner, c.Repo, c.PrNum,
		[]string{label},
	)
	return err
}

// hasLabel checks if a GitHub PR has a label.
func (c *GitHubConfig) hasLabel(label string) (bool, error) {
	ctx := context.Background()
	labels, _, err := c.client.Issues.ListLabelsByIssue(
		ctx, c.Owner, c.Repo, c.PrNum, &github.ListOptions{PerPage: 100},
	)
	if err != nil {
		return false, err
	}
	for _, l := range labels {
		if *l.Name == label {
			return true, nil
		}
	}
	return false, nil
}
