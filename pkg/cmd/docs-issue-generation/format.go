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
	"strconv"
	"strings"
)

// constructDocsIssues takes a list of commits from GitHub as well as the PR number associated with those commits and
// outputs a formatted list of docs issues with valid release notes
func constructDocsIssues(prs []cockroachPR) []docsIssue {
	jiraIssueMeta, err := getJiraIssueCreateMeta()
	if err != nil {
		fmt.Println(err)
	}
	const jiraDocsUserId = "712020:f8672db2-443f-4232-b01a-f97746f89805"
	var result []docsIssue
	for _, pr := range prs {
		for _, commit := range pr.Commits {
			rns := formatReleaseNotes(commit.MessageBody, pr.Number, pr.Body, commit.Sha)
			var epicRef string
			for k := range extractEpicIDs(commit.MessageBody + "\n" + pr.Body) {
				epicRef = k
				break
			}
			for i, rn := range rns {
				x := docsIssue{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{
							Id: jiraIssueMeta.Projects[0].Issuetypes[0].Fields.Issuetype.AllowedValues[0].Id,
						},
						Project: jiraFieldId{
							Id: jiraIssueMeta.Projects[0].Issuetypes[0].Fields.Project.AllowedValues[0].Id,
						},
						Summary: formatTitle(commit.MessageHeadline, pr.Number, i+1, len(rns)),
						Reporter: jiraFieldId{
							Id: jiraDocsUserId,
						},
						Description: rn,
						DocType: jiraFieldId{
							Id: jiraIssueMeta.Projects[0].Issuetypes[0].Fields.DocType.AllowedValues[0].Id,
						},
						ProductChangeCommitSHA: commit.Sha,
						ProductChangePrNumber:  strconv.Itoa(pr.Number),
					},
				}
				if epicRef != "" {
					x.Fields.EpicLink = epicRef
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
func formatReleaseNotes(commitMessage string, prNumber int, prBody, crdbSha string) []string {
	rnBodySlice := []string{}
	if releaseNoteNoneRE.MatchString(commitMessage) {
		return rnBodySlice
	}
	epicIssueRefs := extractIssueEpicRefs(prBody, commitMessage)
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
	if len(rnBodySlice) > 1 {
		relatedProductChanges := "Related product changes: " +
			"https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D" +
			"%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2F" +
			crdbSha + "%22%20ORDER%20BY%20created%20DESC\n\n---"
		for i, rn := range rnBodySlice {
			rnBodySlice[i] = strings.Replace(rn, "\n---", relatedProductChanges, -1)
		}
	}
	return rnBodySlice
}
