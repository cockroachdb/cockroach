// Copyright 2019 The Cockroach Authors.
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
	"context"
	"strings"

	"github.com/google/go-github/v42/github"
	"golang.org/x/oauth2"
)

const (
	githubPageSize = 100 // max is 100

	// openIssueMaxPages is the max number of pages we try to fetch for openIssues.
	// Setting to 3: if for some reason we had 300+ open blockers, what the team
	// really gets from that email is "there's a LOT of blockers to resolve".
	openIssueMaxPages = 3
)

type githubClient interface {
	openIssues(labels []string) ([]githubIssue, error)
}

// githubClientImpl implements the githubClient interface.
type githubClientImpl struct {
	client *github.Client
	ctx    context.Context
	owner  string
	repo   string
}

var _ githubClient = &githubClientImpl{}

type githubIssue struct {
	Number      int
	ProjectName string
}

// newGithubClient returns a githubClient, which is wrapper for a *github.Client
// for a specific repository.
// To generate personal access token, go to https://github.com/settings/tokens.
func newGithubClient(ctx context.Context, accessToken string) *githubClientImpl {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: accessToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	return &githubClientImpl{
		client: github.NewClient(tc),
		ctx:    ctx,
		owner:  "cockroachdb",
		repo:   "cockroach",
	}
}

func (c *githubClientImpl) openIssues(labels []string) ([]githubIssue, error) {
	var details []githubIssue
	for pageNum := 1; pageNum <= openIssueMaxPages; pageNum++ {
		issues, _, err := c.client.Issues.List(
			c.ctx,
			true, /* all: true searches `/issues/`, false searches `/user/issues/` */
			&github.IssueListOptions{
				Filter: "all",
				State:  "open",
				Labels: labels,
				ListOptions: github.ListOptions{
					PerPage: githubPageSize,
					Page:    pageNum,
				},
			},
		)
		if err != nil {
			return nil, err
		}
		for _, i := range issues {
			issueNum := *i.Number
			projectName, err := c.mostRecentProjectName(issueNum)
			if err != nil {
				return nil, err
			}

			details = append(details, githubIssue{
				Number:      issueNum,
				ProjectName: projectName,
			})
		}

		if len(issues) < githubPageSize {
			break
		}
	}

	return details, nil
}

func (c *githubClientImpl) branchExists(branchName string) (bool, error) {
	protected := true
	branches, _, err := c.client.Repositories.ListBranches(
		c.ctx, c.owner, c.repo,
		&github.BranchListOptions{Protected: &protected},
	)
	if err != nil {
		return false, err
	}
	for _, b := range branches {
		if *b.Name == branchName {
			return true, nil
		}
	}
	return false, nil
}

// mostRecentProjectName returns the most recently added project name, by iterating
// through issue.Project.Name values found in the issues' timeline.
// Returns nil if no project name found.
func (c *githubClientImpl) mostRecentProjectName(issueNum int) (string, error) {
	pageNum := 1
	removedProjects := make(map[string]bool)
	for {
		events, _, err := c.client.Issues.ListIssueTimeline(
			c.ctx, c.owner, c.repo, issueNum, &github.ListOptions{
				PerPage: githubPageSize,
				Page:    pageNum,
			})
		if err != nil {
			return "", err
		}
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].ProjectCard != nil {
				projectId := events[i].ProjectCard.ProjectID
				if projectId != nil {
					project, _, err := c.client.Projects.GetProject(c.ctx, *projectId)
					if err != nil {
						return "", err
					}
					// removed event means that the ProjectCard was removed from the issue.
					if strings.Contains(*events[i].Event, "removed") {
						removedProjects[*project.Name] = true
					} else if strings.Contains(*events[i].Event, "added") {
						if _, exists := removedProjects[*project.Name]; exists {
							delete(removedProjects, *project.Name)
						} else {
							return *project.Name, err
						}
					}
				}
			}
		}
		if len(events) < githubPageSize {
			break
		}
	}
	return "", nil
}
