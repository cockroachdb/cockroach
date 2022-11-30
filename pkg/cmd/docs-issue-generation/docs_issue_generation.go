// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// docsIssue contains details about each formatted commit to be committed to the docs repo.
type docsIssue struct {
	sourceCommitSha string
	title           string
	body            string
	labels          []string
}

// parameters stores the GitHub API token, a dry run flag to output the issues it would create, and the
// start and end times of the search.
type parameters struct {
	Token     string // GitHub API token
	DryRun    bool
	StartTime time.Time
	EndTime   time.Time
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

var (
	releaseNoteNoneRE      = regexp.MustCompile(`(?i)release note:? [nN]one`)
	allRNRE                = regexp.MustCompile(`(?i)release note:? \(.*`)
	nonBugFixRNRE          = regexp.MustCompile(`(?i)release note:? \(([^b]|b[^u]|bu[^g]|bug\S|bug [^f]|bug f[^i]|bug fi[^x]).*`)
	bugFixRNRE             = regexp.MustCompile(`(?i)release note:? \(bug fix\):.*`)
	releaseJustificationRE = regexp.MustCompile(`(?i)release justification:.*`)
	prNumberRE             = regexp.MustCompile(`Related PR: \[?https://github.com/cockroachdb/cockroach/pull/(\d+)\D`)
	commitShaRE            = regexp.MustCompile(`Commit: \[?https://github.com/cockroachdb/cockroach/commit/(\w+)\W`)
)

const (
	docsOrganization = "cockroachdb"
	docsRepo         = "docs"
)

// the heart of the script to fetch and manipulate all data and create the individual docs issues
func docsIssueGeneration(params parameters) {
	repoID, labelMap, err := searchDocsRepoLabels(params.Token)
	if err != nil {
		fmt.Println(err)
	}
	prs, err := searchCockroachPRs(params.StartTime, params.EndTime, params.Token)
	if err != nil {
		fmt.Println(err)
	}
	docsIssues := constructDocsIssues(prs)
	if params.DryRun {
		fmt.Printf("Start time: %#v\n", params.StartTime.Format(time.RFC3339))
		fmt.Printf("End time: %#v\n", params.EndTime.Format(time.RFC3339))
		if len(docsIssues) > 0 {
			fmt.Printf("Dry run is enabled. The following %d docs issue(s) would be created:\n", len(docsIssues))
			fmt.Println(docsIssues)
		} else {
			fmt.Println("No docs issues need to be created.")
		}
	} else {
		for _, di := range docsIssues {
			if params.DryRun {
				fmt.Printf("%#v\n", di)
			} else {
				err := di.createDocsIssues(params.Token, repoID, labelMap)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}

func searchDocsRepoLabels(token string) (string, map[string]string, error) {
	var labels = map[string]string{}
	var repoID string
	repoID, hasNextPage, nextCursor, err := searchDocsRepoLabelsSingle("", labels, token)
	if err != nil {
		fmt.Println(err)
		return "", nil, err
	}
	for hasNextPage {
		_, hasNextPage, nextCursor, err = searchDocsRepoLabelsSingle(nextCursor, labels, token)
		if err != nil {
			fmt.Println(err)
			return "", nil, err
		}
	}
	return repoID, labels, nil
}

func searchDocsRepoLabelsSingle(
	cursor string, m map[string]string, token string,
) (string, bool, string, error) {
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
	err := queryGraphQL(docsIssueGQLQuery, queryVariables, token, &search)
	if err != nil {
		fmt.Println(err)
		return "", false, "", err
	}
	repoID := search.Data.Repository.ID
	for _, x := range search.Data.Repository.Labels.Edges {
		m[x.Node.Name] = x.Node.ID
	}
	hasNextPage := search.Data.Repository.Labels.PageInfo.HasNextPage
	nextCursor := search.Data.Repository.Labels.PageInfo.EndCursor
	return repoID, hasNextPage, nextCursor, nil
}

func queryGraphQL(
	query string, queryVariables map[string]interface{}, token string, out interface{},
) error {
	const graphQLURL = "https://api.github.com/graphql"
	client := &http.Client{}
	bodyInt := map[string]interface{}{
		"query": query,
	}
	if queryVariables != nil {
		bodyInt["variables"] = queryVariables
	}
	requestBody, err := json.Marshal(bodyInt)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", graphQLURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+token)
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	bs, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	// unmarshal (convert) the byte slice into an interface
	var tmp interface{}
	err = json.Unmarshal(bs, &tmp)
	if err != nil {
		fmt.Println("Error: unable to unmarshal JSON into an empty interface")
		fmt.Println(string(bs[:]))
		return err
	}
	err = json.Unmarshal(bs, out)
	if err != nil {
		return err
	}
	return nil
}

func searchCockroachPRs(
	startTime time.Time, endTime time.Time, token string,
) ([]cockroachPR, error) {
	prCommitsToExclude, err := searchDocsIssues(startTime, token)
	if err != nil {
		fmt.Println(err)
	}
	hasNextPage, nextCursor, prs, err := searchCockroachPRsSingle(startTime, endTime, "", prCommitsToExclude, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	result := prs
	for hasNextPage {
		hasNextPage, nextCursor, prs, err = searchCockroachPRsSingle(startTime, endTime, nextCursor, prCommitsToExclude, token)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		result = append(result, prs...)
	}
	return result, nil
}

func searchDocsIssues(startTime time.Time, token string) (map[int]map[string]int, error) {
	var result = map[int]map[string]int{}
	hasNextPage, nextCursor, err := searchDocsIssuesSingle(startTime, "", result, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	for hasNextPage {
		hasNextPage, nextCursor, err = searchDocsIssuesSingle(startTime, nextCursor, result, token)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
	}
	return result, nil
}

func searchDocsIssuesSingle(
	startTime time.Time, cursor string, m map[int]map[string]int, token string,
) (bool, string, error) {
	query := `query ($cursor: String, $ghSearchQuery: String!) {
		search(first: 100, query: $ghSearchQuery, type: ISSUE, after: $cursor) {
			nodes {
				... on Issue {
					number
					body
				}
			}
			pageInfo {
				hasNextPage
				endCursor
			}
		}
	}`
	var search gqlDocsIssue
	today := timeutil.Now().Format(time.RFC3339)
	queryVariables := map[string]interface{}{
		"ghSearchQuery": fmt.Sprintf(`repo:%s/%s is:issue label:C-product-change created:%s..%s`, docsOrganization, docsRepo, startTime.Format(time.RFC3339), today),
	}
	if cursor != "" {
		queryVariables["cursor"] = cursor
	}
	err := queryGraphQL(query, queryVariables, token, &search)
	if err != nil {
		fmt.Println(err)
		return false, "", err
	}
	for _, x := range search.Data.Search.Nodes {
		prNumber, commitSha, err := parseDocsIssueBody(x.Body)
		if err != nil {
			fmt.Println(err)
		}
		if prNumber != 0 && commitSha != "" {
			_, ok := m[prNumber]
			if !ok {
				m[prNumber] = make(map[string]int)
			}
			m[prNumber][commitSha] = x.Number
		}
	}
	hasNextPage := search.Data.Search.PageInfo.HasNextPage
	nextCursor := search.Data.Search.PageInfo.EndCursor
	return hasNextPage, nextCursor, nil
}

func parseDocsIssueBody(body string) (int, string, error) {
	prMatches := prNumberRE.FindStringSubmatch(body)
	if len(prMatches) < 2 {
		return 0, "", fmt.Errorf("error: No PR number found in issue body")
	}
	prNumber, err := strconv.Atoi(prMatches[1])
	if err != nil {
		fmt.Println(err)
		return 0, "", err
	}
	commitShaMatches := commitShaRE.FindStringSubmatch(body)
	if len(commitShaMatches) < 2 {
		return 0, "", fmt.Errorf("error: No commit SHA found in issue body")
	}
	return prNumber, commitShaMatches[1], nil
}

func searchCockroachPRsSingle(
	startTime time.Time,
	endTime time.Time,
	cursor string,
	prCommitsToExclude map[int]map[string]int,
	token string,
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
	err := queryGraphQL(query, queryVariables, token, &search)
	if err != nil {
		fmt.Println(err)
		return false, "", nil, err
	}
	for _, x := range search.Data.Search.Nodes {
		var commits []cockroachCommit
		for _, y := range x.Commits.Edges {
			matchingDocsIssue := prCommitsToExclude[x.Number][y.Node.Commit.Oid]
			if nonBugFixRNRE.MatchString(y.Node.Commit.MessageBody) && matchingDocsIssue == 0 {
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
			additionalCommits, err := searchCockroachPRCommits(x.Number, x.Commits.PageInfo.EndCursor, prCommitsToExclude, token)
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
	hasNextPage := search.Data.Search.PageInfo.HasNextPage
	nextCursor := search.Data.Search.PageInfo.EndCursor
	return hasNextPage, nextCursor, result, nil
}

func searchCockroachPRCommits(
	pr int, cursor string, prCommitsToExclude map[int]map[string]int, token string,
) ([]cockroachCommit, error) {
	hasNextPage, nextCursor, commits, err := searchCockroachPRCommitsSingle(pr, cursor, prCommitsToExclude, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	result := commits
	for hasNextPage {
		hasNextPage, nextCursor, commits, err = searchCockroachPRCommitsSingle(pr, nextCursor, prCommitsToExclude, token)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		result = append(result, commits...)
	}
	return result, nil
}

func searchCockroachPRCommitsSingle(
	prNumber int, cursor string, prCommitsToExclude map[int]map[string]int, token string,
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
		token,
		&search,
	)
	if err != nil {
		fmt.Println(err)
		return false, "", nil, err
	}
	for _, x := range search.Data.Repository.PullRequest.Commits.Edges {
		matchingDocsIssue := prCommitsToExclude[prNumber][x.Node.Commit.Oid]
		if nonBugFixRNRE.MatchString(x.Node.Commit.MessageHeadline) && matchingDocsIssue == 0 {
			commit := cockroachCommit{
				Sha:             x.Node.Commit.Oid,
				MessageHeadline: x.Node.Commit.MessageHeadline,
				MessageBody:     x.Node.Commit.MessageHeadline,
			}
			result = append(result, commit)
		}
	}
	hasNextPage := search.Data.Repository.PullRequest.Commits.PageInfo.HasNextPage
	nextCursor := search.Data.Repository.PullRequest.Commits.PageInfo.EndCursor
	return hasNextPage, nextCursor, result, nil
}

// getIssues takes a list of commits from GitHub as well as the PR number associated with those commits and outputs a
// formatted list of docs issues with valid release notes
func constructDocsIssues(prs []cockroachPR) []docsIssue {
	var result []docsIssue
	for _, pr := range prs {
		for _, commit := range pr.Commits {
			rns := formatReleaseNotes(commit.MessageBody, pr.Number, commit.Sha)
			for i, rn := range rns {
				x := docsIssue{
					sourceCommitSha: commit.Sha,
					title:           formatTitle(commit.MessageHeadline, pr.Number, i+1, len(rns)),
					body:            rn,
					labels: []string{
						"C-product-change",
						pr.BaseRefName,
					},
				}
				result = append(result, x)

			}
		}
	}
	return result
}

func formatTitle(title string, prNumber int, index int, totalLength int) string {
	result := fmt.Sprintf("PR #%d - %s", prNumber, title)
	if totalLength > 1 {
		result += fmt.Sprintf(" (%d of %d)", index, totalLength)
	}
	return result
}

// formatReleaseNotes generates a list of docsIssue bodies for the docs repo based on a given CRDB sha
func formatReleaseNotes(message string, prNumber int, crdbSha string) []string {
	rnBodySlice := []string{}
	if releaseNoteNoneRE.MatchString(message) {
		return rnBodySlice
	}
	splitString := strings.Split(message, "\n")
	releaseNoteLines := []string{}
	var rnBody string
	for _, x := range splitString {
		validRn := allRNRE.MatchString(x)
		bugFixRn := bugFixRNRE.MatchString(x)
		releaseJustification := releaseJustificationRE.MatchString(x)
		if len(releaseNoteLines) > 0 && (validRn || releaseJustification) {
			rnBody = fmt.Sprintf(
				"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\n"+
					"Commit: https://github.com/cockroachdb/cockroach/commit/%s\n"+
					"\n---\n\n%s",
				strconv.Itoa(prNumber),
				crdbSha,
				strings.Join(releaseNoteLines, "\n"),
			)
			rnBodySlice = append(rnBodySlice, strings.TrimSuffix(rnBody, "\n"))
			rnBody = ""
			releaseNoteLines = []string{}
		}
		if (validRn && !bugFixRn) || (len(releaseNoteLines) > 0 && !bugFixRn && !releaseJustification) {
			releaseNoteLines = append(releaseNoteLines, x)
		}
	}
	if len(releaseNoteLines) > 0 { // commit whatever is left in the buffer to the rnBodySlice set
		rnBody = fmt.Sprintf(
			"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\n"+
				"Commit: https://github.com/cockroachdb/cockroach/commit/%s\n"+
				"\n---\n\n%s",
			strconv.Itoa(prNumber),
			crdbSha,
			strings.Join(releaseNoteLines, "\n"),
		)
		rnBodySlice = append(rnBodySlice, strings.TrimSuffix(rnBody, "\n"))
	}
	return rnBodySlice
}

// TODO: Redo this function

func (di docsIssue) createDocsIssues(
	token string, repoID string, labelMap map[string]string,
) error {
	var output gqlCreateIssueMutation
	mutation := `mutation ($repoId: ID!, $title: String!, $body: String!, $labelIds: [ID!]) {
		createIssue(
			input: {repositoryId: $repoId, title: $title, body: $body, labelIds: $labelIds}
		) {
			clientMutationId
		}
	}`
	var labelIds []string
	for _, x := range di.labels {
		labelIds = append(labelIds, labelMap[x])
	}
	mutationVariables := map[string]interface{}{
		"repoId": repoID,
		"title":  di.title,
		"body":   di.body,
	}
	if len(labelIds) > 0 {
		mutationVariables["labelIds"] = labelIds
	}
	err := queryGraphQL(mutation, mutationVariables, token, &output)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
