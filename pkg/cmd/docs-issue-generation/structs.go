// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "time"

// docsIssue contains details about each formatted commit to be committed to the docs repo.
type docsIssue struct {
	sourceCommitSha string
	title           string
	body            string
	labels          []string
}

// queryParameters stores the GitHub API token, a dry run flag to output the issues it would create, and the
// start and end times of the search.
type queryParameters struct {
	DryRun    bool
	StartTime time.Time
	EndTime   time.Time
}

type apiTokenParameters struct {
	GitHubToken string
	JiraToken   string
}

// pageInfo contains pagination information for querying the GraphQL API.
type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// gqlCockroachPRCommit contains details about commits within PRs in the cockroach repo.
type gqlCockroachPRCommit struct {
	Data struct {
		Repository struct {
			PullRequest struct {
				Commits struct {
					Edges []struct {
						Node struct {
							Commit struct {
								Oid             string `json:"oid"`
								MessageHeadline string `json:"messageHeadline"`
								MessageBody     string `json:"messageBody"`
							} `json:"commit"`
						} `json:"node"`
					} `json:"edges"`
					PageInfo pageInfo `json:"pageInfo"`
				} `json:"commits"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
}

// gqlCockroachPR contains details about PRs within the cockroach repo.
type gqlCockroachPR struct {
	Data struct {
		Search struct {
			Nodes []struct {
				Title       string `json:"title"`
				Number      int    `json:"number"`
				Body        string `json:"body"`
				BaseRefName string `json:"baseRefName"`
				Commits     struct {
					Edges []struct {
						Node struct {
							Commit struct {
								Oid             string `json:"oid"`
								MessageHeadline string `json:"messageHeadline"`
								MessageBody     string `json:"messageBody"`
							} `json:"commit"`
						} `json:"node"`
					} `json:"edges"`
					PageInfo pageInfo `json:"pageInfo"`
				} `json:"commits"`
			} `json:"nodes"`
			PageInfo pageInfo `json:"pageInfo"`
		} `json:"search"`
	} `json:"data"`
}

// gqlDocsIssue contains details about existing issues within the docs repo.
type gqlDocsIssue struct {
	Data struct {
		Search struct {
			Nodes []struct {
				Number int    `json:"number"`
				Body   string `json:"body"`
			} `json:"nodes"`
			PageInfo pageInfo `json:"pageInfo"`
		} `json:"search"`
	} `json:"data"`
}

type gqlSingleIssue struct {
	Data struct {
		Repository struct {
			Issue struct {
				Body string `json:"body"`
			} `json:"issue"`
		} `json:"repository"`
	} `json:"data"`
}

// gqlDocsRepoLabels contains details about the labels within the cockroach repo. In order to create issues using the
// GraphQL API, we need to use the label IDs and no
type gqlDocsRepoLabels struct {
	Data struct {
		Repository struct {
			ID     string `json:"id"`
			Labels struct {
				Edges []struct {
					Node struct {
						Name string `json:"name"`
						ID   string `json:"id"`
					} `json:"node"`
				} `json:"edges"`
				PageInfo pageInfo `json:"pageInfo"`
			} `json:"labels"`
		} `json:"repository"`
	} `json:"data"`
}

type gqlCreateIssueMutation struct {
	Data struct {
		CreateIssue struct {
			ClientMutationID interface{} `json:"clientMutationId"`
		} `json:"createIssue"`
	} `json:"data"`
}

type jiraIssueSearch struct {
	StartAt    int `json:"startAt"`
	MaxResults int `json:"maxResults"`
	Total      int `json:"total"`
	Issues     []struct {
		Key            string `json:"key"`
		RenderedFields struct {
			Description string `json:"description"`
		} `json:"renderedFields"`
	} `json:"issues"`
}

type cockroachPR struct {
	Title       string `json:"title"`
	Number      int    `json:"number"`
	Body        string `json:"body"`
	BaseRefName string `json:"baseRefName"`
	Commits     []cockroachCommit
}

type cockroachCommit struct {
	Sha             string `json:"oid"`
	MessageHeadline string `json:"messageHeadline"`
	MessageBody     string `json:"messageBody"`
}

type epicIssueRefInfo struct {
	epicRefs        map[string]int
	epicNone        bool
	issueCloseRefs  map[string]int
	issueInformRefs map[string]int
	isBugFix        bool
}
