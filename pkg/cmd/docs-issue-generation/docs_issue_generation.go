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

type ghSearch struct {
	Items []struct {
		Number      int `json:"number"`
		PullRequest struct {
			MergedAt *time.Time `json:"merged_at"`
		} `json:"pull_request"`
	} `json:"items"`
}

type ghPull struct {
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

type ghPullCommit struct {
	Sha    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
	} `json:"commit"`
}

func docsIssueGeneration(params parameters) {
	bsSearch := httpGet("https://api.github.com/search/issues?q=sha:"+params.Sha+"+repo:cockroachdb/cockroach+is:merged", params.Token)
	prs := prNums(bsSearch)
	for _, pr := range prs {
		bsPull := httpGet("https://api.github.com/repos/cockroachdb/cockroach/pulls/"+strconv.Itoa(pr.number), params.Token)
		pr.mergeBranch = setMergeBranch(bsPull)
		bsCommit := httpGet("https://api.github.com/repos/cockroachdb/cockroach/pulls/"+strconv.Itoa(pr.number)+"/commits?per_page:250", params.Token)
		pr.commits = setCommits(bsCommit, pr.number)
		pr.createDocsIssues(params.Token)
	}
}

func httpGet(url string, token string) []byte {
	client := &http.Client{}
	req, err1 := http.NewRequest("GET", url, nil)
	if err1 != nil {
		log.Fatal(err1)
	}
	req.Header.Set("Authorization", "token "+token)
	res, err2 := client.Do(req)
	if err2 != nil {
		log.Fatal(err2)
	}
	bs, err3 := io.ReadAll(res.Body)
	if err3 != nil {
		log.Fatal(err3)
	}
	return bs
}

func prNums(bs []byte) []pr {
	search := ghSearch{}
	if err := json.Unmarshal(bs, &search); err != nil {
		log.Fatal(err)
	}
	var result []pr
	for _, x := range search.Items {
		if x.PullRequest.MergedAt != nil {
			result = append(result, pr{number: x.Number})
		}
	}
	return result
}

func setMergeBranch(bs []byte) string {
	pull := ghPull{}
	if err := json.Unmarshal(bs, &pull); err != nil {
		log.Fatal(err)
	}
	return pull.Base.Ref
}

func setCommits(bs []byte, p int) []commit {
	var pullCommit []ghPullCommit
	if err := json.Unmarshal(bs, &pullCommit); err != nil {
		log.Fatal(err)
	}
	var result []commit
	for _, c := range pullCommit {
		message := c.Commit.Message
		re := regexp.MustCompile("(?s)[rR]elease [nN]ote \\(.*")
		reNeg := regexp.MustCompile("([rR]elease [nN]ote \\(bug fix.*)|([rR]elease [nN]ote: [nN]one)")
		reTitle := regexp.MustCompile("(?s)\n.*")
		title := reTitle.ReplaceAllString(message, "")
		rnSlice := re.FindAllString(message, -1)
		if len(rnSlice) > 0 {
			rn := rnSlice[0]
			if len(rn) > 0 && !reNeg.MatchString(message) {
				commit := commit{
					sha:         c.Sha,
					title:       title,
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/" + strconv.Itoa(p) + "\nCommit: https://github.com/cockroachdb/cockroach/commit/" + c.Sha + "\n\n---\n\n" + rn,
				}
				result = append(result, commit)
			}
		}
	}
	return result
}

func (p pr) createDocsIssues(token string) {
	postUrl := "https://api.github.com/repos/nickvigilante/trialproject/issues"
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
