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

// Regex components for finding and validating issue and epic references in a string
var (
	ghIssuePart        = `(#\d+)`                                                   // e.g., #12345
	ghIssueRepoPart    = `([\w.-]+[/][\w.-]+#\d+)`                                  // e.g., cockroachdb/cockroach#12345
	ghURLPart          = `(https://github.com/[-a-z0-9]+/[-._a-z0-9/]+/issues/\d+)` // e.g., https://github.com/cockroachdb/cockroach/issues/12345
	jiraIssuePart      = `([[:alpha:]]+-\d+)`                                       // e.g., DOC-3456
	exalateJiraRefPart = `Jira issue: ` + jiraIssuePart                             // e.g., Jira issue: CRDB-54321
	jiraBaseUrlPart    = "https://cockroachlabs.atlassian.net/browse/"
	jiraURLPart        = jiraBaseUrlPart + jiraIssuePart // e.g., https://cockroachlabs.atlassian.net/browse/DOC-3456
	issueRefPart       = ghIssuePart + "|" + ghIssueRepoPart + "|" + ghURLPart + "|" + jiraIssuePart + "|" + jiraURLPart
	afterRefPart       = `[,.;]?(?:[ \t\n\r]+|$)`
)

// RegExes of each issue part
var (
	ghIssuePartRE     = regexp.MustCompile(ghIssuePart)
	ghIssueRepoPartRE = regexp.MustCompile(ghIssueRepoPart)
	ghURLPartRE       = regexp.MustCompile(ghURLPart)
	jiraIssuePartRE   = regexp.MustCompile(jiraIssuePart)
	jiraURLPartRE     = regexp.MustCompile(jiraURLPart)
)

// Fully composed regexs used to match strings.
var (
	fixIssueRefRE          = regexp.MustCompile(`(?im)(?i:close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+(?:(?:` + issueRefPart + `)` + afterRefPart + ")+")
	informIssueRefRE       = regexp.MustCompile(`(?im)(?:part of|see also|informs):?\s+(?:(?:` + issueRefPart + `)` + afterRefPart + ")+")
	epicRefRE              = regexp.MustCompile(`(?im)epic:?\s+(?:(?:` + jiraIssuePart + "|" + jiraURLPart + `)` + afterRefPart + ")+")
	epicNoneRE             = regexp.MustCompile(`(?im)epic:?\s+(?:(none)` + afterRefPart + ")+")
	githubJiraIssueRefRE   = regexp.MustCompile(issueRefPart)
	jiraIssueRefRE         = regexp.MustCompile(jiraIssuePart + "|" + jiraURLPart)
	releaseNoteNoneRE      = regexp.MustCompile(`(?i)release note:? [nN]one`)
	allRNRE                = regexp.MustCompile(`(?i)release note:? \(.*`)
	nonBugFixRNRE          = regexp.MustCompile(`(?i)release note:? \(([^b]|b[^u]|bu[^g]|bug\S|bug [^f]|bug f[^i]|bug fi[^x]).*`)
	bugFixRNRE             = regexp.MustCompile(`(?i)release note:? \(bug fix\):.*`)
	releaseJustificationRE = regexp.MustCompile(`(?i)release justification:.*`)
	prNumberRE             = regexp.MustCompile(`Related PR: \[?https://github.com/cockroachdb/cockroach/pull/(\d+)\D`)
	commitShaRE            = regexp.MustCompile(`Commit: \[?https://github.com/cockroachdb/cockroach/commit/(\w+)\W`)
	exalateJiraRefRE       = regexp.MustCompile(exalateJiraRefPart)
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
	docsIssues := constructDocsIssues(prs, params.Token)
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
			err := di.createDocsIssues(params.Token, repoID, labelMap)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

// searchDocsRepoLabels passes in a GitHub API token and returns the repo ID of the docs repo as well as a map
// of all the labels and their respective label IDs in GitHub.
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

// searchDocsRepoLabelsSingle runs once per page of 100 labels within the docs repo. It returns the repo ID,
// whether or not there is another page after the one that just ran, the next cursor value used to query the next
// page, and any error.
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
	pageInfo := search.Data.Repository.Labels.PageInfo
	return repoID, pageInfo.HasNextPage, pageInfo.EndCursor, nil
}

// queryGraphQL is the function that interfaces directly with the GitHub GraphQL API. Given a query, variables, and
// token, it will return a struct containing the requested data or an error if one exists.
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

// searchDocsIssues returns a map containing all the product change docs issues that have been created since the given
// start time. For reference, it's structured as map[crdb_pr_number]map[crdb_commit]docs_pr_number.
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

// searchDocsIssuesSingle searches one page of docs issues at a time. These docs issues will ultimately be excluded
// from the PRs through which we iterate to create new product change docs issues. This function returns a bool to
// check if there are more than 100 results, the cursor to query for the next page of results, and an error if
// one exists.
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
	pageInfo := search.Data.Search.PageInfo
	return pageInfo.HasNextPage, pageInfo.EndCursor, nil
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
	pageInfo := search.Data.Search.PageInfo
	return pageInfo.HasNextPage, pageInfo.EndCursor, result, nil
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
	pageInfo := search.Data.Repository.PullRequest.Commits.PageInfo
	return pageInfo.HasNextPage, pageInfo.EndCursor, result, nil
}

// extractStringsFromMessage takes in a commit message or PR body as well as two regular expressions. The first
// regular expression checks for a valid formatted epic or issue reference. If one is found, it searches that exact
// string for the individual issue references. The output is a map where the key is each epic or issue ref and the
// value is the count of references of that ref.
func extractStringsFromMessage(
	message string, firstMatch, secondMatch *regexp.Regexp,
) map[string]int {
	ids := map[string]int{}
	if allMatches := firstMatch.FindAllString(message, -1); len(allMatches) > 0 {
		for _, x := range allMatches {
			matches := secondMatch.FindAllString(x, -1)
			for _, match := range matches {
				ids[match]++
			}
		}
	}
	return ids
}

func extractFixIssueIDs(message string) map[string]int {
	return extractStringsFromMessage(message, fixIssueRefRE, githubJiraIssueRefRE)
}

func extractInformIssueIDs(message string) map[string]int {
	return extractStringsFromMessage(message, informIssueRefRE, githubJiraIssueRefRE)
}

func extractEpicIDs(message string) map[string]int {
	return extractStringsFromMessage(message, epicRefRE, jiraIssueRefRE)
}

func containsEpicNone(message string) bool {
	if allMatches := epicNoneRE.FindAllString(message, -1); len(allMatches) > 0 {
		return true
	}
	return false
}

func containsBugFix(message string) bool {
	if allMatches := bugFixRNRE.FindAllString(message, -1); len(allMatches) > 0 {
		return true
	}
	return false
}

// org/repo#issue: PROJECT-NUMBER

// getJiraIssueFromGitHubIssue takes a GitHub issue and returns the appropriate Jira key from the issue body.
// getJiraIssueFromGitHubIssue is specified as a function closure to allow for testing
// of getJiraIssueFromGitHubIssue* methods.
var getJiraIssueFromGitHubIssue = func(org, repo string, issue int, token string) (string, error) {
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
	err := queryGraphQL(query, queryVariables, token, &search)
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

func splitBySlashOrHash(r rune) bool {
	return r == '/' || r == '#'
}

func getJiraIssueFromRef(ref, token string) string {
	if jiraIssuePartRE.MatchString(ref) {
		return ref
	} else if jiraURLPartRE.MatchString(ref) {
		return strings.Replace(ref, "https://cockroachlabs.atlassian.net/browse/", "", 1)
	} else if ghIssueRepoPartRE.MatchString(ref) {
		split := strings.FieldsFunc(ref, splitBySlashOrHash)
		issueNumber, err := strconv.Atoi(split[2])
		if err != nil {
			fmt.Println(err)
			return ""
		}
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber, token)
		if err != nil {
			fmt.Println(err)
		}
		return issueRef
	} else if ghIssuePartRE.MatchString(ref) {
		issueNumber, err := strconv.Atoi(strings.Replace(ref, "#", "", 1))
		if err != nil {
			fmt.Println(err)
			return ""
		}
		issueRef, err := getJiraIssueFromGitHubIssue("cockroachdb", "cockroach", issueNumber, token)
		if err != nil {
			fmt.Println(err)
		}
		return issueRef
	} else if ghURLPartRE.MatchString(ref) {
		replace1 := strings.Replace(ref, "https://github.com/", "", 1)
		replace2 := strings.Replace(replace1, "/issues", "", 1)
		split := strings.FieldsFunc(replace2, splitBySlashOrHash)
		issueNumber, err := strconv.Atoi(split[2])
		if err != nil {
			fmt.Println(err)
			return ""
		}
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber, token)
		if err != nil {
			fmt.Println(err)
		}
		return issueRef
	} else {
		return "Malformed epic/issue ref (" + ref + ")"
	}
}

func extractIssueEpicRefs(prBody, commitBody, token string) string {
	refInfo := epicIssueRefInfo{
		epicRefs:        extractEpicIDs(commitBody + "\n" + prBody),
		epicNone:        containsEpicNone(commitBody + "\n" + prBody),
		issueCloseRefs:  extractFixIssueIDs(commitBody + "\n" + prBody),
		issueInformRefs: extractInformIssueIDs(commitBody + "\n" + prBody),
		isBugFix:        containsBugFix(commitBody + "\n" + prBody),
	}
	var builder strings.Builder
	if len(refInfo.epicRefs) > 0 {
		builder.WriteString("Epic:")
		for x := range refInfo.epicRefs {
			builder.WriteString(" " + getJiraIssueFromRef(x, token))
		}
		builder.WriteString("\n")
	}
	if len(refInfo.issueCloseRefs) > 0 {
		builder.WriteString("Fixes:")
		for x := range refInfo.issueCloseRefs {
			builder.WriteString(" " + getJiraIssueFromRef(x, token))
		}
		builder.WriteString("\n")
	}
	if len(refInfo.issueInformRefs) > 0 {
		builder.WriteString("Informs:")
		for x := range refInfo.issueInformRefs {
			builder.WriteString(" " + getJiraIssueFromRef(x, token))
		}
		builder.WriteString("\n")
	}
	if refInfo.epicNone && builder.Len() == 0 {
		builder.WriteString("Epic: none\n")
	}
	return builder.String()
}

// getIssues takes a list of commits from GitHub as well as the PR number associated with those commits and outputs a
// formatted list of docs issues with valid release notes
func constructDocsIssues(prs []cockroachPR, token string) []docsIssue {
	var result []docsIssue
	for _, pr := range prs {
		for _, commit := range pr.Commits {
			rns := formatReleaseNotes(commit.MessageBody, pr.Number, pr.Body, commit.Sha, token)
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
func formatReleaseNotes(
	commitMessage string, prNumber int, prBody, crdbSha, token string,
) []string {
	rnBodySlice := []string{}
	if releaseNoteNoneRE.MatchString(commitMessage) {
		return rnBodySlice
	}
	epicIssueRefs := extractIssueEpicRefs(prBody, commitMessage, token)
	splitString := strings.Split(commitMessage, "\n")
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
					"%s\n---\n\n%s",
				strconv.Itoa(prNumber),
				crdbSha,
				epicIssueRefs,
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
				"%s\n---\n\n%s",
			strconv.Itoa(prNumber),
			crdbSha,
			epicIssueRefs,
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
