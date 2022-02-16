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
	"fmt"
	"sort"

	"github.com/google/go-github/v42/github"
	"golang.org/x/oauth2"
)

const NoProjectName = "No Project"

type Issue struct {
	IssueNum    int
	IssueTitle  string
}

type ProjectBlocker struct {
	ProjectName string
	Issues []Issue
}

type ProjectBlockers []ProjectBlocker

func (p ProjectBlockers) Len() int {
	total := 0
	for _, blockers := range p {
		total += len(blockers.Issues)
	}
	return total
}

func (p ProjectBlockers) Less(i, j int) bool {
	if p[i].ProjectName == NoProjectName {
		return false
	}
	if p[j].ProjectName == NoProjectName {
		return true
	}
	return p[i].ProjectName < p[j].ProjectName
}

func (p ProjectBlockers) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func findOpenBlockers(
	client *githubClient,
	releaseSeries string,
) ([]ProjectBlocker, error) {
	// TODO(celia) account for master branch, i.e. what should `releaseSeries` value be here?
	releaseBranch := fmt.Sprintf("branch-release-%s", releaseSeries)
	issues, err := client.getOpenIssues([]string{
		"release-blocker",
		releaseBranch,
	})
	if err != nil {
		return nil, err
	}
	if len(issues) == 0 {
		return []ProjectBlocker{}, nil
	}
	blockersByProject := make(map[string][]Issue)
	for _, issue := range issues {
		projectName := "No Project"
		projectNamePtr, err := findIssueProjectName(client, *issue.Number)
		if err != nil {
			return nil, err
		}
		if projectNamePtr != nil {
			projectName = *projectNamePtr
		}
		blockersByProject[projectName] = append(
			blockersByProject[projectName],
			Issue{
				IssueNum:    *issue.Number,
				IssueTitle:  *issue.Title,
			},
		)
	}
	var blockers ProjectBlockers
	for projectName, issueList := range blockersByProject {
		blockers = append(blockers, ProjectBlocker{
			ProjectName: projectName,
			Issues: issueList,
		})
	}
	sort.Sort(blockers)
	return blockers, nil
}

// findIssueProjectName gets the issue project name by finding
// the most recent issue.Project.Name found in the issues' timeline.
// Returns nil if no project name found.
func findIssueProjectName(client *githubClient, issueNum int) (*string, error) {
	events, err := client.getTimelineEvents(issueNum)
	if err != nil {
		return nil, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].ProjectCard != nil {
			projectId := events[i].ProjectCard.ProjectID
			if projectId != nil {
				projectName, err := client.getProjectName(*projectId)
				if err != nil {
					return nil, err
				}
				return projectName, nil
			}
		}
	}
	return nil, nil
}

type githubClient struct {
	client *github.Client
	ctx    context.Context
	owner  string
	repo   string
}

// newGithubClient returns a githubClient, which is wrapper for a *github.Client
// for a specific repository.
// To generate personal access token, go to https://github.com/settings/tokens.
func newGithubClient(ctx context.Context, accessToken string) *githubClient {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: accessToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	return &githubClient{
		client: github.NewClient(tc),
		ctx:    ctx,
		owner:  "cockroachdb",
		repo:   "cockroach",
	}
}

func (c *githubClient) getOpenIssues(labels []string) ([]*github.Issue, error) {
	issues, _, err := c.client.Issues.List(
		c.ctx,
		true, /* all: true searches `/issues/`, false searches `/user/issues/` */
		&github.IssueListOptions{
			Filter: "all",
			State:  "open",
			Labels: labels,
		},
	)
	if err != nil {
		return nil, err
	}
	return issues, nil
}

func (c *githubClient) getTimelineEvents(issueNum int) ([]*github.Timeline, error) {
	events, _, err := c.client.Issues.ListIssueTimeline(
		c.ctx, c.owner, c.repo, issueNum, nil)
	return events, err
}

func (c *githubClient) getProjectName(projectId int64) (*string, error) {
	project, _, err := c.client.Projects.GetProject(c.ctx, projectId)
	if err != nil {
		return nil, err
	}
	return project.Name, nil
}
