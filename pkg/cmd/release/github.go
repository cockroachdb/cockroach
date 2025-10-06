// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/go-github/v42/github"
	"golang.org/x/oauth2"
)

const (
	githubPageSize = 100 // max is 100

	// openIssueMaxPages is the max number of pages we try to fetch for openIssues.
	// Setting to 3: if for some reason we had 300+ open blockers, what the team
	// really gets from that email is "there's a LOT of blockers to resolve".
	openIssueMaxPages = 3

	timelineEventMaxPages = 3
)

type githubClient interface {
	openIssues(labels []string) ([]githubIssue, error)
	issueEvents(issueNum int) ([]githubEvent, error)
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

type githubEvent struct {
	// The timestamp indicating when the event occurred.
	CreatedAt time.Time

	Event       string
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
		// TODO: This pagination pattern is a potential race condition: we may want to move to graphql api,
		// which has cursor-based pagination. But for now, this is probably good enough, as issues don't
		// change _that_ frequently.
		issues, _, err := c.client.Issues.ListByRepo(
			c.ctx,
			c.owner,
			c.repo,
			&github.IssueListByRepoOptions{
				State:  "open",
				Labels: labels,
				ListOptions: github.ListOptions{
					PerPage: githubPageSize,
					Page:    pageNum,
				},
			},
		)
		if err != nil {
			return nil, fmt.Errorf("error calling Issues.List: %w", err)
		}
		for _, i := range issues {
			issueNum := *i.Number
			projectName, err := mostRecentProjectName(c, issueNum)
			if err != nil {
				return nil, fmt.Errorf("error calling mostRecentProjectName: %w", err)
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
		return false, fmt.Errorf("error calling Repositories.ListBranches: %w", err)
	}
	for _, b := range branches {
		if *b.Name == branchName {
			return true, nil
		}
	}
	return false, nil
}

// issueEvents returns events in chronological order, e.g.
//
//	https://api.github.com/repos/cockroachdb/cockroach/issues/77157/timeline
func (c *githubClientImpl) issueEvents(issueNum int) ([]githubEvent, error) {
	var details []githubEvent
	// TODO: This pagination pattern is a potential race condition: we may want to move to graphql api,
	// which has cursor-based pagination. But for now, this is probably fine, as timeline events are
	// returned sequentially, so we don't expect past events (previous page values) to change.
	for pageNum := 1; pageNum <= timelineEventMaxPages; pageNum++ {
		events, _, err := c.client.Issues.ListIssueTimeline(
			c.ctx, c.owner, c.repo, issueNum, &github.ListOptions{
				PerPage: githubPageSize,
				Page:    pageNum,
			})
		if err != nil {
			return nil, fmt.Errorf("error calling Issues.ListIssueTimeline: %w", err)
		}
		for _, event := range events {
			if event == nil {
				continue
			}
			detail := githubEvent{
				Event: *event.Event,
			}
			if event.CreatedAt != nil {
				detail.CreatedAt = *event.CreatedAt
			}
			if event.ProjectCard != nil && event.ProjectCard.ProjectID != nil {
				project, _, err := c.client.Projects.GetProject(c.ctx, *event.ProjectCard.ProjectID)
				if err != nil {
					return nil, fmt.Errorf("error calling Projects.GetProject: %w", err)
				}
				if project.Name != nil {
					detail.ProjectName = *project.Name
				}
			}
			details = append(details, detail)
		}
		if len(events) < githubPageSize {
			break
		}
	}
	return details, nil
}
