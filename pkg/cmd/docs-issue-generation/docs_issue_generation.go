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
type issue struct {
	sha         string
	title       string
	releaseNote string
}

// pr contains details about the pull request
type pr struct {
	number      int
	mergeBranch string
	issues      []issue
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
		pr.issues = getIssues(commits, pr.number)
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
func getIssues(pullCommit []ghPullCommit, prNumber int) []issue {
	var result []issue
	for _, c := range pullCommit {
		message := c.Commit.Message
		rns := formatReleaseNotes(message, prNumber, c.Sha) // return a slice of data that locates every instance of the phrase "release note (", case insensitive
		for i, rn := range rns {
			{ // checks to make sure a match was returned
				x := issue{ // declare a new commit object
					sha:         c.Sha,
					title:       formatTitle(message, prNumber, i+1, len(rns)),
					releaseNote: rn,
				}
				result = append(result, x)
			}
		}
	}
	return result
}

func formatReleaseNotes(message string, prNumber int, sha string) []string {
	result := []string{}                                      // each array in this string is a different release note
	noRN := regexp.MustCompile(`[rR]elease [nN]ote: [nN]one`) // this regex is used to exclude PRs without release notes
	if !noRN.MatchString(message) {
		splitString := strings.Split(message, "\n")                                    // split based on \n character
		allRNRE := regexp.MustCompile(`[rR]elease [nN]ote \(.*`)                       // this regex checks to make sure there's a release note within the commit
		bugFixRNRE := regexp.MustCompile(`([rR]elease [nN]ote \(bug fix\):.*)`)        // this regex is used to exclude bug fixes
		releaseJustificationRE := regexp.MustCompile(`[rR]elease [jJ]ustification:.*`) // this regex is used to exclude lines for a release justification
		buffer := ""                                                                   // the buffer collects the text for all valid release notes and then writes it to the result
		for _, x := range splitString {
			validRn := allRNRE.MatchString(x)
			bufferNotEmpty := buffer != "" // if the buffer contains text, it means that it is in the process of collecting a release note
			bugFixRn := bugFixRNRE.MatchString(x)
			releaseJustification := releaseJustificationRE.MatchString(x)
			// the above 4 variables are booleans to identify each line's purpose
			if bufferNotEmpty && (validRn || releaseJustification) { // if the buffer contains a release note and it finds a new release note or a release justification, we write and flush the buffer
				result = append(result, strings.TrimSuffix(fmt.Sprintf(
					"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\nCommit: https://github.com/cockroachdb/cockroach/commit/%s\n\n---\n\n%s",
					strconv.Itoa(prNumber),
					sha,
					buffer,
				),
					"\n",
				),
				) // add a new item to result slice with the formatted release text
				buffer = "" // flush the buffer
			}
			if (validRn && !bugFixRn) || (bufferNotEmpty && !bugFixRn && !releaseJustification) { // if the row is a valid, non bug fix RN or the buffer has a release note in progress and the line isn't a new bug fix RN or release justification, write the line to the buffer
				buffer += x + "\n"
			}
		}
		if buffer != "" { // commit whatever is left in the buffer to the result set
			result = append(result, strings.TrimSuffix(fmt.Sprintf(
				"Related PR: https://github.com/cockroachdb/cockroach/pull/%s\nCommit: https://github.com/cockroachdb/cockroach/commit/%s\n\n---\n\n%s",
				strconv.Itoa(prNumber),
				sha,
				buffer,
			),
				"\n",
			),
			)
		}
	}
	return result
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
	postURL := "https://api.github.com/repos/cockroachdb/docs/issues"
	for _, issue := range p.issues {
		reqBody, err := json.Marshal(map[string]interface{}{
			"title":  issue.title,
			"labels": []string{"C-product-change", p.mergeBranch},
			"body":   issue.releaseNote,
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
