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
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Regex components for finding and validating issue and epic references in a string
var (
	ghIssuePart        = `(#\d+)`                                                   // e.g., #12345
	ghIssueRepoPart    = `([\w.-]+[/][\w.-]+#\d+)`                                  // e.g., cockroachdb/cockroach#12345
	ghURLPart          = `(https://github.com/[-a-z0-9]+/[-._a-z0-9/]+/issues/\d+)` // e.g., https://github.com/cockroachdb/cockroach/issues/12345
	jiraIssuePart      = `([[:alpha:]]+-\d+)`                                       // e.g., DOC-3456
	exalateJiraRefPart = `Jira issue: ` + jiraIssuePart                             // e.g., Jira issue: CRDB-54321
	jiraBrowseUrlPart  = crlJiraBaseUrl + "browse/"
	jiraURLPart        = jiraBrowseUrlPart + jiraIssuePart // e.g., https://cockroachlabs.atlassian.net/browse/DOC-3456
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
	prNumberHTMLRE         = regexp.MustCompile(`Related PR: <a href="https://github.com/cockroachdb/cockroach/pull/(\d+)\D`)
	commitShaRE            = regexp.MustCompile(`Commit: \[?https://github.com/cockroachdb/cockroach/commit/(\w+)\W`)
	commitShaHTMLRE        = regexp.MustCompile(`Commit: <a href="https://github.com/cockroachdb/cockroach/commit/(\w+)\W`)
	exalateJiraRefRE       = regexp.MustCompile(exalateJiraRefPart)
)

const (
	docsOrganization = "cockroachdb"
	docsRepo         = "docs"
	//jiraDocsUserId   = "712020:f8672db2-443f-4232-b01a-f97746f89805"
)

// the heart of the script to fetch and manipulate all data and create the individual docs issues
func docsIssueGeneration(params queryParameters) {
	prs, err := searchCockroachPRs(params.StartTime, params.EndTime)
	if err != nil {
		fmt.Println(err)
	}
	docsIssues := constructDocsIssues(prs)
	if params.DryRun {
		fmt.Printf("Start time: %#v\n", params.StartTime.Format(time.RFC3339))
		fmt.Printf("End time: %#v\n", params.EndTime.Format(time.RFC3339))
		//fmt.Printf("Repo ID: %s\n", repoID)
		fmt.Printf("Number of PRs found: %d\n", len(prs))
		if len(docsIssues) > 0 {
			fmt.Printf("Dry run is enabled. The following %d docs issue(s) would be created:\n", len(docsIssues))
			fmt.Println(docsIssues)
		} else {
			fmt.Println("No docs issues need to be created.")
		}
	} else {
		batchSize := 50
		for i := 0; i < len(docsIssues); i += batchSize {
			end := i + batchSize
			if end > len(docsIssues) {
				end = len(docsIssues)
			}
			batch := docsIssueBatch{
				IssueUpdates: docsIssues[i:end],
			}
			err := batch.createDocsIssuesInBulk()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func splitBySlashOrHash(r rune) bool {
	return r == '/' || r == '#'
}

func getJiraIssueFromRef(ref string) string {
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
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber)
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
		issueRef, err := getJiraIssueFromGitHubIssue("cockroachdb", "cockroach", issueNumber)
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
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber)
		if err != nil {
			fmt.Println(err)
		}
		return issueRef
	} else {
		return "Malformed epic/issue ref (" + ref + ")"
	}
}

// TODO: Redo this function
// figure out if you can do this against an array once and bundle it by groups of 50
