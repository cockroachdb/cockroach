// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	jiraDocsUserId = "712020:f8672db2-443f-4232-b01a-f97746f89805"
)

var (
	releaseNoteNoneRE      = regexp.MustCompile(`(?i)release note:? [nN]one`)
	allRNRE                = regexp.MustCompile(`(?i)release note:? \(.*`)
	bugFixRNRE             = regexp.MustCompile(`(?i)release note:? \(bug fix\):.*`)
	releaseJustificationRE = regexp.MustCompile(`(?i)release justification:.*`)
)

// constructDocsIssues takes a list of commits from GitHub as well as the PR number associated with those commits and
// outputs a formatted list of docs issues with valid release notes
func constructDocsIssues(prs []cockroachPR) ([]docsIssue, error) {
	jiraIssueMeta, err := getJiraIssueCreateMeta()
	if err != nil {
		return []docsIssue{}, err
	}
	fixVersionMap, err := generateFixVersionMap(jiraIssueMeta.Projects[0].Issuetypes[0].Fields.FixVersions.AllowedValues)
	if err != nil {
		return []docsIssue{}, err
	}
	docTypeId, err := extractProductChangeDocTypeId(jiraIssueMeta.Projects[0].Issuetypes[0].Fields.DocType.AllowedValues)
	if err != nil {
		return []docsIssue{}, err
	}
	var result []docsIssue
	for _, pr := range prs {
		for _, commit := range pr.Commits {
			rns, err := formatReleaseNotes(commit.MessageBody, pr.Number, pr.Body, commit.Sha)
			if err != nil {
				return []docsIssue{}, err
			}
			epicRefs := extractEpicIDs(commit.MessageBody + "\n" + pr.Body)
			var epicRef string
			for k := range epicRefs {
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
						EpicLink:    epicRef,
						DocType: jiraFieldId{
							Id: docTypeId,
						},
						ProductChangeCommitSHA: commit.Sha,
						ProductChangePrNumber:  strconv.Itoa(pr.Number),
					},
				}
				_, ok := fixVersionMap[pr.BaseRefName]
				if ok {
					x.Fields.FixVersions = []jiraFieldId{
						{
							Id: fixVersionMap[pr.BaseRefName],
						},
					}
				}
				result = append(result, x)
			}
		}
	}
	return result, nil
}

// generateFixVersionMap returns a mapping of release branches to the object ID in Jira of the matching fixVersions value
func generateFixVersionMap(
	fixVersions []jiraCreateIssueMetaFixVersionsAllowedValue,
) (map[string]string, error) {
	result := make(map[string]string)
	labelNameRe := regexp.MustCompile(`^\d{2}\.\d \(`)
	branchVersionRe := regexp.MustCompile(`release-(\d+\.\d+)`)
	labelToLabelId := make(map[string]string)
	for _, x := range fixVersions {
		if labelNameRe.MatchString(x.Name) {
			labelToLabelId[x.Name] = x.Id
		}
	}
	labelKeys := make([]string, 0, len(labelToLabelId))
	for key := range labelToLabelId {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)
	releaseBranches, err := searchCockroachReleaseBranches()
	if err != nil {
		return nil, err
	}
	result["master"] = labelToLabelId[labelKeys[len(labelKeys)-1]]
	for _, branch := range releaseBranches {
		branchMatches := branchVersionRe.FindStringSubmatch(branch)
		if len(branchMatches) >= 2 {
			for _, label := range labelKeys {
				if strings.Contains(label, branchMatches[1]) {
					result[branch] = labelToLabelId[label]
					//break
				}
			}
		}
	}
	return result, nil
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
	commitMessage string, prNumber int, prBody, crdbSha string,
) ([]adfRoot, error) {
	rnBodySlice := []adfRoot{}
	if releaseNoteNoneRE.MatchString(commitMessage) {
		return rnBodySlice, nil
	}
	epicIssueRefs, err := extractIssueEpicRefs(prBody, commitMessage)
	if err != nil {
		return []adfRoot{}, err
	}
	splitString := strings.Split(commitMessage, "\n")
	releaseNoteLines := []string{}
	var rnBody adfRoot
	for _, x := range splitString {
		validRn := allRNRE.MatchString(x)
		bugFixRn := bugFixRNRE.MatchString(x)
		releaseJustification := releaseJustificationRE.MatchString(x)
		if len(releaseNoteLines) > 0 && (validRn || releaseJustification) {
			formattedRNText := strings.TrimSuffix(strings.Join(releaseNoteLines, "\n"), "\n")
			rnBody = formatReleaseNotesSingle(prNumber, crdbSha, formattedRNText, epicIssueRefs)
			rnBodySlice = append(rnBodySlice, rnBody)
			releaseNoteLines = []string{}
		}
		if (validRn && !bugFixRn) || (len(releaseNoteLines) > 0 && !bugFixRn && !releaseJustification) {
			releaseNoteLines = append(releaseNoteLines, x)
		}
	}
	if len(releaseNoteLines) > 0 { // commit whatever is left in the buffer to the rnBodySlice set
		formattedRNText := strings.TrimSuffix(strings.Join(releaseNoteLines, "\n"), "\n")
		rnBody = formatReleaseNotesSingle(prNumber, crdbSha, formattedRNText, epicIssueRefs)
		rnBodySlice = append(rnBodySlice, rnBody)
	}
	if len(rnBodySlice) > 1 {
		relatedProductChanges := []adfNode{
			{
				Type: "hardBreak",
			},
			{
				Type: "text",
				Text: "Related product changes: ",
			},
			{
				Type: "text",
				Text: crlJiraBaseUrl + "issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D" +
					"%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2F" +
					crdbSha + "%22%20ORDER%20BY%20created%20DESC\n\n---",
				Marks: []adfMark{
					{
						Type: "link",
						Attrs: map[string]string{
							"href": crlJiraBaseUrl + "issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D" +
								"%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2F" +
								crdbSha + "%22%20ORDER%20BY%20created%20DESC\n\n---",
						},
					},
				},
			},
		}
		for _, rnBody := range rnBodySlice {
			rnBody.Content[0].Content = append(rnBody.Content[0].Content, relatedProductChanges...)
		}
	}
	return rnBodySlice, nil
}

func formatReleaseNotesSingle(
	prNumber int, crdbSha, releaseNoteBody string, epicIssueRefs []adfNode,
) adfRoot {
	result := adfRoot{
		Version: 1,
		Type:    "doc",
		Content: []adfNode{
			{
				Type: "paragraph",
				Content: []adfNode{
					{
						Type: "text",
						Text: "Related PR: ",
					},
					{
						Type: "text",
						Text: fmt.Sprintf("https://github.com/cockroachdb/cockroach/pull/%d", prNumber),
						Marks: []adfMark{
							{
								Type: "link",
								Attrs: map[string]string{
									"href": fmt.Sprintf("https://github.com/cockroachdb/cockroach/pull/%d", prNumber),
								},
							},
						},
					},
					{
						Type: "hardBreak",
					},
					{
						Type: "text",
						Text: "Commit: ",
					},
					{
						Type: "text",
						Text: fmt.Sprintf("https://github.com/cockroachdb/cockroach/commit/%s", crdbSha),
						Marks: []adfMark{
							{
								Type: "link",
								Attrs: map[string]string{
									"href": fmt.Sprintf("https://github.com/cockroachdb/cockroach/commit/%s", crdbSha),
								},
							},
						},
					},
				},
			},
			{
				Type: "paragraph",
				Content: []adfNode{
					{
						Type: "text",
						Text: "---",
					},
				},
			},
			{
				Type: "paragraph",
				Content: []adfNode{
					{
						Type: "text",
						Text: releaseNoteBody, // TODO: Find a way to convert the Markdown contents in this variable to ADF
					},
				},
			},
		},
	}
	if len(epicIssueRefs) > 0 {
		result.Content[0].Content = append(result.Content[0].Content, epicIssueRefs...)
	}
	return result
}
