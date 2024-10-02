// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	exalateJiraRefPart = `Jira issue: ` + jiraIssuePart // e.g., Jira issue: CRDB-54321
	exalateJiraRefRE   = regexp.MustCompile(exalateJiraRefPart)
	nonBugFixRNRE      = regexp.MustCompile(`(?i)release note:? \(([^b]|b[^u]|bu[^g]|bug\S|bug [^f]|bug f[^i]|bug fi[^x]).*`)
)

func searchCockroachPRs(startTime time.Time, endTime time.Time) ([]cockroachPR, error) {
	prCommitsToExclude, err := searchJiraDocsIssues(startTime)
	if err != nil {
		return nil, err
	}
	hasNextPage, nextCursor, prs, err := searchCockroachPRsSingle(startTime, endTime, "", prCommitsToExclude)
	if err != nil {
		return nil, err
	}
	result := prs
	for hasNextPage {
		hasNextPage, nextCursor, prs, err = searchCockroachPRsSingle(startTime, endTime, nextCursor, prCommitsToExclude)
		if err != nil {
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
		return nil, err
	}
	result := commits
	for hasNextPage {
		hasNextPage, nextCursor, commits, err = searchCockroachPRCommitsSingle(pr, nextCursor, prCommitsToExclude)
		if err != nil {
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
        pageInfo {
          hasNextPage
          endCursor
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

var searchCockroachReleaseBranches = func() ([]string, error) {
	var result []string
	hasNextPage, nextCursor, branches, err := searchCockroachReleaseBranchesSingle("")
	if err != nil {
		return []string{}, err
	}
	result = append(result, branches...)
	for hasNextPage {
		hasNextPage, nextCursor, branches, err = searchCockroachReleaseBranchesSingle(nextCursor)
		if err != nil {
			return []string{}, err
		}
		result = append(result, branches...)
	}
	return result, nil
}

func searchCockroachReleaseBranchesSingle(cursor string) (bool, string, []string, error) {
	var search gqlRef
	var result []string
	query := `query ($cursor: String) {
  repository(owner: "cockroachdb", name: "cockroach") {
    refs(
      first: 100
      refPrefix: "refs/"
      query: "heads/release-"
      after: $cursor
      orderBy: {field: TAG_COMMIT_DATE, direction: DESC}
    ) {
      edges {
        node {
          name
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}`
	queryVariables := make(map[string]interface{})
	if cursor != "" {
		queryVariables["cursor"] = cursor
	}
	err := queryGraphQL(query, queryVariables, &search)
	if err != nil {
		return false, "", []string{}, err
	}
	for _, x := range search.Data.Repository.Refs.Edges {
		result = append(result, strings.Replace(x.Node.Name, "heads/", "", -1))
	}
	pageInfo := search.Data.Repository.Refs.PageInfo
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
		return "", err
	}
	var jiraIssue string
	exalateRef := exalateJiraRefRE.FindString(search.Data.Repository.Issue.Body)
	if len(exalateRef) > 0 {
		jiraIssue = jiraIssuePartRE.FindString(exalateRef)
	}
	return jiraIssue, nil
}
