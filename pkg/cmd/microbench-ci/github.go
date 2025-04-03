// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/go-github/v61/github"
	"golang.org/x/oauth2"
)

// CommentTag is used to identify an existing comment.
const CommentTag = "<!-- microbench-ci -->"

// post posts or updates a comment on a GitHub PR, with the given summary text,
// that has the CommentTag marker in it.
func post(summaryText, githubRepository string, prNumber int) error {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return errors.New("GITHUB_TOKEN is not set, this command is meant to be run in a GitHub Action")
	}

	repoInfo := strings.Split(githubRepository, "/")
	if len(repoInfo) != 2 {
		return errors.New("invalid GitHub repository flag")
	}
	owner := repoInfo[0]
	repo := repoInfo[1]

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	// Check for an existing comment
	comments, _, err := client.Issues.ListComments(ctx, owner, repo, prNumber, nil)
	if err != nil {
		return err
	}
	var existingComment *github.IssueComment
	for _, comment := range comments {
		if strings.Contains(comment.GetBody(), CommentTag) {
			existingComment = comment
			break
		}
	}

	commentBody := github.String(fmt.Sprintf("%s\n%s", summaryText, CommentTag))
	if existingComment != nil {
		// Update the existing comment
		existingComment.Body = commentBody
		_, _, err = client.Issues.EditComment(ctx, owner, repo, existingComment.GetID(), existingComment)
		return err
	}

	// Create a new comment
	_, _, err = client.Issues.CreateComment(ctx, owner, repo, prNumber, &github.IssueComment{
		Body: commentBody,
	})
	return err
}
