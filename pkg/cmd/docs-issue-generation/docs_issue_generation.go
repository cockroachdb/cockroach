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

// docsIssue contains details about each formatted commit to be committed to the docs repo
type docsIssue struct {
	crdbSha    string
	issueTitle string
	issueBody  string
}

// pr contains details about the cockroach pull request
type pr struct {
	number      int
	mergeBranch string
	docsIssues  []docsIssue
}

// ghSearch contains a list of search items (in this case, cockroach PRs) from the GitHub API.
// This struct holds the search results of all PRs based on a given commit.
type ghSearch struct {
	Items []ghSearchItem `json:"items"`
}

// ghSearchItem contains details about a specific cockroach PR,
// including its number, from the GitHub API.
type ghSearchItem struct {
	Number      int            `json:"number"` // PR number
	PullRequest ghSearchItemPr `json:"pull_request"`
}

// ghSearchItemPr contains the time a PR was merged from the GitHub API
type ghSearchItemPr struct {
	MergedAt *time.Time `json:"merged_at"`
}

// this struct holds the base branch for a cockroach PR from the GitHub API
type ghPull struct {
	Base struct {
		Ref string `json:"ref"` // this is the destination branch of the PR
	} `json:"base"`
}

// ghPullCommit holds the SHA and commit message for a particular commit, from the GitHub API
type ghPullCommit struct {
	CRDBSha       string          `json:"crdbSha"`
	CRDBCommitMsg ghPullCommitMsg `json:"commit"`
}

// ghPullCommitMsg holds the commit message in the cockroach PR from the GitHub API
type ghPullCommitMsg struct {
	Message string `json:"message"` // the commit message
}

// parameters holds the GitHub access token and the SHA (BUILD_VCS_NUMBER).
type parameters struct {
	Token string // GitHub API token
	Sha   string
}

// the heart of the script to fetch and manipulate all data and create the individual docs docsIssues
func docsIssueGeneration(params parameters) {
	var search ghSearch // a search for all PRs based on a given commit SHA
	var prs []pr
	if err := httpGet("https://api.github.com/search/docsIssues?q=crdbSha:"+params.Sha+"+repo:cockroachdb/cockroach+is:merged", params.Token, &search); err != nil {
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
		pr.docsIssues = getIssues(commits, pr.number)
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

// getIssues takes a list of commits from GitHub as well as the PR associated with those commits and outputs a formatted list of commits with valid release notes on that PR
func getIssues(pullCommit []ghPullCommit, prNumber int) []docsIssue {
	var result []docsIssue
	for _, c := range pullCommit {
		message := c.CRDBCommitMsg.Message
		rns := formatReleaseNotes(message, prNumber, c.CRDBSha) // return a slice of data that locates every instance of the phrase "release note (", case insensitive
		for i, rn := range rns {
			{ // checks to make sure a match was returned
				x := docsIssue{ // declare a new commit object
					crdbSha:    c.CRDBSha,
					issueTitle: formatTitle(message, prNumber, i+1, len(rns)),
					issueBody:  rn,
				}
				result = append(result, x)
			}
		}
	}
	return result
}

func formatReleaseNotes(message string, prNumber int, crdbSha string) []string {
	// formatReleaseNotes generates a list of docsIssue bodies for the docs repo
	// based on a given crdbSha
	rnBodySlice := []string{}
	noRN := regexp.MustCompile(`[rR]elease [nN]ote: [nN]one`)
	if !noRN.MatchString(message) {
		splitString := strings.Split(message, "\n")
		allRNRE := regexp.MustCompile(`[rR]elease [nN]ote \(.*`)
		bugFixRNRE := regexp.MustCompile(`([rR]elease [nN]ote \(bug fix\):.*)`)
		releaseJustificationRE := regexp.MustCompile(`[rR]elease [jJ]ustification:.*`)
		buffer := ""
		for _, x := range splitString {
			validRn := allRNRE.MatchString(x)
			bufferNotEmpty := buffer != ""
			bugFixRn := bugFixRNRE.MatchString(x)
			releaseJustification := releaseJustificationRE.MatchString(x)
			if bufferNotEmpty && (validRn || releaseJustification) {
				rnBodySlice = append(rnBodySlice, strings.TrimSuffix(fmt.Sprintf(
					"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\nCommit: https://github.com/cockroachdb/cockroach/commit/%s\n\n---\n\n%s",
					strconv.Itoa(prNumber),
					crdbSha,
					buffer,
				),
					"\n",
				),
				)
				buffer = ""
			}
			if (validRn && !bugFixRn) || (bufferNotEmpty && !bugFixRn && !releaseJustification) {
				buffer += x + "\n"
			}
		}
		if buffer != "" { // commit whatever is left in the buffer to the rnBodySlice set
			rnBodySlice = append(rnBodySlice, strings.TrimSuffix(fmt.Sprintf(
				"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\nCommit: https://github.com/cockroachdb/cockroach/commit/%s\n\n---\n\n%s",
				strconv.Itoa(prNumber),
				crdbSha,
				buffer,
			),
				"\n",
			),
			)
		}
	}
	return rnBodySlice
}

func formatTitle(message string, prNumber int, index int, totalLength int) string {
	var result string
	if i := strings.IndexRune(message, '\n'); i > 0 { //
		result = message[:i]
	} else {
		result = message
	}
	result = fmt.Sprintf("PR #%d - %s",
		prNumber,
		result,
	)
	if totalLength > 1 {
		result += fmt.Sprintf(
			" (%d of %d)",
			index,
			totalLength,
		)
	}
	return result
}

func (p pr) createDocsIssues(token string) {
	postURL := "https://api.github.com/repos/cockroachdb/docs/docsIssues"
	for _, issue := range p.docsIssues {
		reqBody, err := json.Marshal(map[string]interface{}{
			"issueTitle": issue.issueTitle,
			"labels":     []string{"C-product-change", p.mergeBranch},
			"body":       issue.issueBody,
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
