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
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// commit contains details about each formatted commit
type commit struct {
	sha         string
	title       string
	releaseNote string
}

// pr contains details about the pull request
type pr struct {
	number      int
	mergeBranch string
	commits     []commit
}

// ghSearch contains a list of search items (in this case, PRs) from the GitHub API
type ghSearch struct { // this struct holds the search results of all PRs based on a given commit
	Items []ghSearchItem `json:"items"`
}

// ghSearchItem contains details about a specific PR, including its number, from the GitHub API
type ghSearchItem struct {
	Number      int            `json:"number"` // PR number
	PullRequest ghSearchItemPr `json:"pull_request"`
}

// ghSearchItemPr contains the time a PR was merged from the GitHub API
type ghSearchItemPr struct {
	MergedAt *time.Time `json:"merged_at"`
}

// this struct holds details about the PR from the GitHub API
type ghPull struct {
	Base struct {
		Ref string `json:"ref"` // this is the destination branch of the PR
	} `json:"base"`
}

// ghPullCommit holds the SHA and commit message for a particular commit, from the GitHub API
type ghPullCommit struct {
	Sha    string          `json:"sha"`
	Commit ghPullCommitMsg `json:"commit"`
}

// ghPullCommitMsg holds the commit message from the GitHub API
type ghPullCommitMsg struct {
	Message string `json:"message"` // the commit message
}

// parameters holds the GitHub access token and the SHA (BUILD_VCS_NUMBER).
type parameters struct {
	Token string // GitHub API token
	Sha   string
}

// the heart of the script to fetch and manipulate all data and create the individual docs issues
func docsIssueGeneration(params parameters) {
	var search ghSearch // a search for all PRs based on a given commit SHA
	var prs []pr
	if err := httpGet("https://api.github.com/search/issues?q=sha:"+params.Sha+"+repo:cockroachdb/cockroach+is:merged", params.Token, &search); err != nil {
		log.Fatal(err)
	}
	prs = prNums(search)     // populate slice of PRs of type pr
	for _, pr := range prs { // for each PR in the list, get the merge branch and the list of eligible commits
		var pull ghPull
		if err := httpGet("https://api.github.com/repos/cockroachdb/cockroach/pulls/"+strconv.Itoa(pr.number), params.Token, &pull); err != nil {
			log.Fatal(err)
		}
		pr.mergeBranch = pull.Base.Ref
		var commits []ghPullCommit
		if err := httpGet("https://api.github.com/repos/cockroachdb/cockroach/pulls/"+strconv.Itoa(pr.number)+"/commits?per_page:250", params.Token, &commits); err != nil {
			log.Fatal(err)
		}
		pr.commits = getCommits(commits, pr.number)
		pr.createDocsIssues(params.Token)
	}
}

func httpGet(url string, token string, out interface{}) error {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
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
	if err := json.Unmarshal(bs, out); err != nil { // unmarshal (convert) the byte slice into an interface
		return err
	}
	return nil
}

// prNums returns an array of PRS for the given GH search of PRs against a given commit.
func prNums(search ghSearch) []pr {
	var result []pr
	for _, x := range search.Items { // each PR returned from the search is iterated through, and a new pr object is created for each
		if x.PullRequest.MergedAt != nil {
			result = append(result, pr{number: x.Number}) // a new object of type pr containing the PR number is appended to the result slice
		}
	}
	return result // return a slice of prs
}

// getCommits takes a list of commits from GitHub as well as the PR associated with those commits and outputs a formatted list of commits with valid release notes on that PR
func getCommits(pullCommit []ghPullCommit, prNumber int) []commit {
	var result []commit
	for _, c := range pullCommit {
		message := c.Commit.Message
		rn := formatReleaseNote(message, prNumber, c.Sha) // return a slice of data that locates every instance of the phrase "release note (", case insensitive
		if rn != "" {                                     // checks to make sure a match was returned
			x := commit{ // declare a new commit object
				sha:         c.Sha,
				title:       formatTitle(message),
				releaseNote: rn,
			}
			result = append(result, x)
		}
	}
	return result
}

func formatReleaseNote(message string, prNumber int, sha string) string {
	re := regexp.MustCompile(`(?s)[rR]elease [nN]ote \(.*`)                                       // this regex checks to make sure there's a release note within the commit
	reNeg := regexp.MustCompile(`([rR]elease [nN]ote \(bug fix.*)|([rR]elease [nN]ote: [nN]one)`) // this regex is used to exclude bug fixes or releases without release notes
	rn := re.FindString(message)                                                                  // return the first instance of the phrase "release note (", case insensitive
	if len(rn) > 0 && !reNeg.MatchString(message) {                                               // checks to make sure the desired release note is not null and doesn't match the negating string.
		return fmt.Sprintf(
			"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\nCommit: https://github.com/cockroachdb/cockroach/commit/%s\n\n---\n\n%s",
			strconv.Itoa(prNumber),
			sha,
			rn,
		)
	}
	return ""
}

func formatTitle(message string) string {
	if i := strings.IndexRune(message, '\n'); i > 0 {
		return message[:i]
	}
	return message
}

func (p pr) createDocsIssues(token string) {
	postURL := "https://api.github.com/repos/cockroachdb/docs/issues"
	for _, commit := range p.commits {
		reqBody, err := json.Marshal(map[string]interface{}{
			"title":  commit.title,
			"labels": []string{"C-product-change", p.mergeBranch},
			"body":   commit.releaseNote,
		})
		if err != nil {
			log.Fatal(err)
		}
		client := &http.Client{}
		req, _ := http.NewRequest("POST", postURL, bytes.NewBuffer(reqBody))
		req.Header.Set("Authorization", "token "+token)
		_, err = client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
	}
}
