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
  "github.com/google/go-github/v42/github"
  "golang.org/x/oauth2"
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
  var details []githubIssue
  for _, i := range issues {
    issueNum := *i.Number
    projectNamePtr, err := c.projectName(issueNum)
    if err != nil {
      return nil, err
    }
    projectName := ""
    if projectNamePtr != nil {
      projectName = *projectNamePtr
    }
    details = append(details, githubIssue{
      Number:      issueNum,
      ProjectName: projectName,
    })
  }
  return details, nil
}

// ProjectName gets the issue project name by finding
// the most recent issue.Project.Name found in the issues' timeline.
// Returns nil if no project name found.
func (c *githubClientImpl) projectName(issueNum int) (*string, error) {
  events, _, err := c.client.Issues.ListIssueTimeline(
    c.ctx, c.owner, c.repo, issueNum, nil)
  if err != nil {
    return nil, err
  }
  for i := len(events) - 1; i >= 0; i-- {
    if events[i].ProjectCard != nil {
      projectId := events[i].ProjectCard.ProjectID
      if projectId != nil {
        project, _, err := c.client.Projects.GetProject(c.ctx, *projectId)
        if err != nil {
          return nil, err
        }
        return project.Name, nil
      }
    }
  }
  return nil, nil
}
