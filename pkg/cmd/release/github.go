package main

import "context"

type Issue struct {
	IssueNum   int
	IssueTitle string
}

type ProjectBlocker struct {
	ProjectName string
	NumBlockers int
	Issues      []Issue
}

func findOpenBlockers(
	client *githubClient,
	releaseSeries string,
) ([]ProjectBlocker, error) {
	return []ProjectBlocker{
		{
			ProjectName: "My Project",
			NumBlockers: 2,
			Issues: []Issue{
				{
					IssueNum:   1234,
					IssueTitle: "This is a blocker!!!",
				},
				{
					IssueNum:   5678,
					IssueTitle: "This is a also a blocker",
				},
			},
		},
	}, nil
}

type githubClient struct {
	ctx   context.Context
	owner string
	repo  string
}

func newGithubClient(ctx context.Context, accessToken string) *githubClient {
	return &githubClient{
		ctx:   ctx,
		owner: "cockroachdb",
		repo:  "cockroach",
	}
}
