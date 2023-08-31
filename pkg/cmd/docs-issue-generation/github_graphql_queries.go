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

import (
	"fmt"
	"time"
)

// searchDocsRepoLabels passes in a GitHub API token and returns the repo ID of the docs repo as well as a map
// of all the labels and their respective label IDs in GitHub.
func searchDocsRepoLabels() (string, map[string]string, error) {
	var labels = map[string]string{}
	var repoID string
	repoID, hasNextPage, nextCursor, err := searchDocsRepoLabelsSingle("", labels)
	if err != nil {
		fmt.Println(err)
		return "", nil, err
	}
	for hasNextPage {
		_, hasNextPage, nextCursor, err = searchDocsRepoLabelsSingle(nextCursor, labels)
		if err != nil {
			fmt.Println(err)
			return "", nil, err
		}
	}
	return repoID, labels, nil
}

// searchDocsRepoLabelsSingle runs once per page of 100 labels within the docs repo. It returns the repo ID,
// whether there is another page after the one that just ran, the next cursor value used to query the next
// page, and any error.
func searchDocsRepoLabelsSingle(cursor string, m map[string]string) (string, bool, string, error) {
	docsIssueGQLQuery := fmt.Sprintf(`query ($cursor: String) {
		repository(owner: "%s", name: "%s") {
			id
			labels(first: 100, after: $cursor) {
				edges {
					node {
						name
						id
					}
				}
				pageInfo {
					hasNextPage
					endCursor
				}
			}
		}
	}`, docsOrganization, docsRepo)
	var search gqlDocsRepoLabels
	queryVariables := map[string]interface{}{}
	if cursor != "" {
		queryVariables["cursor"] = cursor
	}
	err := queryGraphQL(docsIssueGQLQuery, queryVariables, &search)
	if err != nil {
		fmt.Println(err)
		return "", false, "", err
	}
	repoID := search.Data.Repository.ID
	for _, x := range search.Data.Repository.Labels.Edges {
		m[x.Node.Name] = x.Node.ID
	}
	pageInfo := search.Data.Repository.Labels.PageInfo
	return repoID, pageInfo.HasNextPage, pageInfo.EndCursor, nil
}

func searchCockroachPRs(startTime time.Time, endTime time.Time) ([]cockroachPR, error) {
	prCommitsToExclude, err := searchJiraDocsIssues(startTime)
	if err != nil {
		fmt.Println(err)
	}
	hasNextPage, nextCursor, prs, err := searchCockroachPRsSingle(startTime, endTime, "", prCommitsToExclude)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	result := prs
	for hasNextPage {
		hasNextPage, nextCursor, prs, err = searchCockroachPRsSingle(startTime, endTime, nextCursor, prCommitsToExclude)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		result = append(result, prs...)
	}
	return result, nil
}

func searchCockroachPRsSingle(
	startTime time.Time,
	endTime time.Time,
	cursor string,
	prCommitsToExclude map[int]map[string]string,
) (bool, string, []cockroachPR, error) {
	var search gqlCockroachPR
	var result []cockroachPR
	query := `query ($cursor: String, $ghSearchQuery: String!) {
	  search(first: 100, query: $ghSearchQuery, type: ISSUE, after: $cursor) {
	    nodes {
	      ... on PullRequest {
	        title
	        number
	        body
	        baseRefName
	        commits(first: 100) {
	          edges {
	            node {
	              commit {
	                oid
	                messageHeadline
	                messageBody
	              }
	            }
	          }
	          pageInfo {
	            hasNextPage
	            endCursor
	          }
	        }
	      }
	    }
	    pageInfo {
	      hasNextPage
	      endCursor
	    }
	  }
	}`
	queryVariables := map[string]interface{}{
		"ghSearchQuery": fmt.Sprintf(
			`repo:cockroachdb/cockroach is:pr is:merged merged:%s..%s`,
			startTime.Format(time.RFC3339),
			endTime.Format(time.RFC3339),
		),
	}
	if cursor != "" {
		queryVariables["cursor"] = cursor
	}
	err := queryGraphQL(query, queryVariables, &search)
	if err != nil {
		fmt.Println(err)
		return false, "", nil, err
	}
	for _, x := range search.Data.Search.Nodes {
		var commits []cockroachCommit
		for _, y := range x.Commits.Edges {
			matchingDocsIssue := prCommitsToExclude[x.Number][y.Node.Commit.Oid]
			if nonBugFixRNRE.MatchString(y.Node.Commit.MessageBody) && matchingDocsIssue == "" {
				commit := cockroachCommit{
					Sha:             y.Node.Commit.Oid,
					MessageHeadline: y.Node.Commit.MessageHeadline,
					MessageBody:     y.Node.Commit.MessageBody,
				}
				commits = append(commits, commit)
			}
		}
		// runs if there are more than 100 results
		if x.Commits.PageInfo.HasNextPage {
			additionalCommits, err := searchCockroachPRCommits(x.Number, x.Commits.PageInfo.EndCursor, prCommitsToExclude)
			if err != nil {
				return false, "", nil, err
			}
			commits = append(commits, additionalCommits...)
		}
		if len(commits) > 0 {
			pr := cockroachPR{
				Number:      x.Number,
				Title:       x.Title,
				Body:        x.Body,
				BaseRefName: x.BaseRefName,
				Commits:     commits,
			}
			result = append(result, pr)
		}
	}
	pageInfo := search.Data.Search.PageInfo
	return pageInfo.HasNextPage, pageInfo.EndCursor, result, nil
}

func searchCockroachPRCommits(
	pr int, cursor string, prCommitsToExclude map[int]map[string]string,
) ([]cockroachCommit, error) {
	hasNextPage, nextCursor, commits, err := searchCockroachPRCommitsSingle(pr, cursor, prCommitsToExclude)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	result := commits
	for hasNextPage {
		hasNextPage, nextCursor, commits, err = searchCockroachPRCommitsSingle(pr, nextCursor, prCommitsToExclude)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		result = append(result, commits...)
	}
	return result, nil
}

func searchCockroachPRCommitsSingle(
	prNumber int, cursor string, prCommitsToExclude map[int]map[string]string,
) (bool, string, []cockroachCommit, error) {
	var result []cockroachCommit
	var search gqlCockroachPRCommit
	query := `query ($cursor: String, $prNumber: Int!) {
	  repository(owner: "cockroachdb", name: "cockroach") {
	    pullRequest(number: $prNumber) {
	      commits(first: 100, after: $cursor) {
	        edges {
	          node {
	            commit {
	              oid
	              messageHeadline
	              messageBody
	            }
	          }
	        }
	      }
	    }
	  }
	}`
	queryVariables := map[string]interface{}{
		"prNumber": prNumber,
	}
	if cursor != "" {
		queryVariables["cursor"] = cursor
	}
	err := queryGraphQL(
		query,
		queryVariables,
		&search,
	)
	if err != nil {
		fmt.Println(err)
		return false, "", nil, err
	}
	for _, x := range search.Data.Repository.PullRequest.Commits.Edges {
		matchingDocsIssue := prCommitsToExclude[prNumber][x.Node.Commit.Oid]
		if nonBugFixRNRE.MatchString(x.Node.Commit.MessageHeadline) && matchingDocsIssue == "" {
			commit := cockroachCommit{
				Sha:             x.Node.Commit.Oid,
				MessageHeadline: x.Node.Commit.MessageHeadline,
				MessageBody:     x.Node.Commit.MessageHeadline,
			}
			result = append(result, commit)
		}
	}
	pageInfo := search.Data.Repository.PullRequest.Commits.PageInfo
	return pageInfo.HasNextPage, pageInfo.EndCursor, result, nil
}

// getJiraIssueFromGitHubIssue takes a GitHub issue and returns the appropriate Jira key from the issue body.
// getJiraIssueFromGitHubIssue is specified as a function closure to allow for testing
// of getJiraIssueFromGitHubIssue* methods.
var getJiraIssueFromGitHubIssue = func(org, repo string, issue int) (string, error) {
	query := `query ($org: String!, $repo: String!, $issue: Int!) {
		repository(owner: $org, name: $repo) {
			issue(number: $issue) {
				body
			}
		}
	}`
	var search gqlSingleIssue
	queryVariables := map[string]interface{}{
		"org":   org,
		"repo":  repo,
		"issue": issue,
	}
	err := queryGraphQL(query, queryVariables, &search)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	var jiraIssue string
	exalateRef := exalateJiraRefRE.FindString(search.Data.Repository.Issue.Body)
	if len(exalateRef) > 0 {
		jiraIssue = jiraIssuePartRE.FindString(exalateRef)
	}
	return jiraIssue, nil
}
