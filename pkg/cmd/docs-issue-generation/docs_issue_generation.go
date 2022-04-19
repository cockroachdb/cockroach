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
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

type commit struct {
	sha         string
	title       string
	releaseNote string
}

type pr struct {
	number      int
	mergeBranch string
	commits     []commit
}

type ghSearch struct { // this struct holds the search results of all PRs based on a given commit
	Items []struct {
		Number      int      `json:"number"` // PR number
		PullRequest struct { // details about the pull request
			MergedAt *time.Time `json:"merged_at"` // the time the PR was merged
		} `json:"pull_request"`
	} `json:"items"`
}

type ghPull struct { // this struct holds details about the actual PR
	Base struct {
		Ref string `json:"ref"` // this is the destination branch of the PR
	} `json:"base"`
}

type ghPullCommit struct { // this struct holds the full list of commits and associated messages based on a given PR
	Sha    string `json:"sha"` // the SHA of the specific commit
	Commit struct {
		Message string `json:"message"` // the commit message
	} `json:"commit"`
}

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
		pr.mergeBranch = getMergeBranch(pull)
		var commit []ghPullCommit
		if err := httpGet("https://api.github.com/repos/cockroachdb/cockroach/pulls/"+strconv.Itoa(pr.number)+"/commits?per_page:250", params.Token, &commit); err != nil {
			log.Fatal(err)
		}
		pr.commits = getCommits(commit, pr.number)
		pr.createDocsIssues(params.Token)
	}
}

func httpGet(url string, token string, out interface{}) error {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil) // initialize a new HTTP request
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+token) // set the authentication token as a header in the request
	res, err := client.Do(req)                      // evaluate the request
	if err != nil {
		return err
	}
	bs, err := io.ReadAll(res.Body) // transcribe the response body into a byte slice
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bs, out); err != nil { // unmarshal (convert) the byte slice into an interface
		return err
	}
	return nil
}

func prNums(search ghSearch) []pr { // given a GH search of PRs against a given commit, return an array of PRs
	var result []pr
	for _, x := range search.Items { // each PR returned from the search is iterated through, and a new pr object is created for each
		if x.PullRequest.MergedAt != nil {
			result = append(result, pr{number: x.Number}) // a new object of type pr containing the PR number is appended to the result slice
		}
	}
	return result // return a slice of prs
}

func getMergeBranch(pull ghPull) string { // given a PR, return the base ref of that particular PR
	return pull.Base.Ref // once the data is unmarshaled, we search for the base ref of the PR in question
}

func getCommits(pullCommit []ghPullCommit, prNumber int) []commit { // given a specific PR, return a list of valid release commits on that PR
	var result []commit
	for _, c := range pullCommit {
		message := c.Commit.Message
		rn := formatReleaseNote(message, prNumber, c.Sha) // return a slice of data that locates every instance of the phrase "release note (", case insensitive
		if rn != "" {                                     // checks to make sure a match was returned
			commit := commit{ // declare a new commit object
				sha:         c.Sha,
				title:       formatTitle(message),
				releaseNote: rn,
			}
			result = append(result, commit)
		}
	}
	return result
}

func formatReleaseNote(message string, prNumber int, sha string) string {
	re := regexp.MustCompile("(?s)[rR]elease [nN]ote \\(.*")                                       // this regex checks to make sure there's a release note within the commit
	reNeg := regexp.MustCompile("([rR]elease [nN]ote \\(bug fix.*)|([rR]elease [nN]ote: [nN]one)") // this regex is used to exclude bug fixes or releases without release notes
	rnSlice := re.FindAllString(message, -1)                                                       // return a slice of data that locates every instance of the phrase "release note (", case insensitive
	if len(rnSlice) > 0 {                                                                          // checks to make sure a match was returned
		rn := rnSlice[0]                                // return the first instance of the desired release note
		if len(rn) > 0 && !reNeg.MatchString(message) { // checks to make sure the desired release note is not null and doesn't match the negating string.
			return "Related PR: https://github.com/cockroachdb/cockroach/pull/" + strconv.Itoa(prNumber) + "\nCommit: https://github.com/cockroachdb/cockroach/commit/" + sha + "\n\n---\n\n" + rn // add formatted release note
		}
	}
	return ""
}

func formatTitle(message string) string {
	reTitle := regexp.MustCompile("(?s)\n.*")
	return reTitle.ReplaceAllString(message, "")
}

func (p pr) createDocsIssues(token string) {
	postUrl := "https://api.github.com/repos/cockroachdb/docs/issues"
	for _, commit := range p.commits {
		reqBody, err := json.Marshal(map[string]interface{}{
			"title":  commit.title,
			"labels": []string{"C-product-change", p.mergeBranch},
			"body":   commit.releaseNote,
		})
		client := &http.Client{}
		req, _ := http.NewRequest("POST", postUrl, bytes.NewBuffer(reqBody))
		req.Header.Set("Authorization", "token "+token)
		_, err = client.Do(req)
		//res.Body.Close()
		if err != nil {
			log.Fatalf("%s blablah2", err)
		}
	}
}
