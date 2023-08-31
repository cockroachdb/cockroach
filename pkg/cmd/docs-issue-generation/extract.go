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
)

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

func extractPRNumberCommitFromDocsIssueBody(body string) (int, string, error) {
	prMatches := prNumberHTMLRE.FindStringSubmatch(body)
	if len(prMatches) < 2 {
		return 0, "", fmt.Errorf("error: No PR number found in issue body")
	}
	prNumber, err := strconv.Atoi(prMatches[1])
	if err != nil {
		fmt.Println(err)
		return 0, "", err
	}
	commitShaMatches := commitShaHTMLRE.FindStringSubmatch(body)
	if len(commitShaMatches) < 2 {
		return 0, "", fmt.Errorf("error: No commit SHA found in issue body")
	}
	return prNumber, commitShaMatches[1], nil
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

func extractIssueEpicRefs(prBody, commitBody string) string {
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
			builder.WriteString(" " + getJiraIssueFromRef(x))
		}
		builder.WriteString("\n")
	}
	if len(refInfo.issueCloseRefs) > 0 {
		builder.WriteString("Fixes:")
		for x := range refInfo.issueCloseRefs {
			builder.WriteString(" " + getJiraIssueFromRef(x))
		}
		builder.WriteString("\n")
	}
	if len(refInfo.issueInformRefs) > 0 {
		builder.WriteString("Informs:")
		for x := range refInfo.issueInformRefs {
			builder.WriteString(" " + getJiraIssueFromRef(x))
		}
		builder.WriteString("\n")
	}
	if refInfo.epicNone && builder.Len() == 0 {
		builder.WriteString("Epic: none\n")
	}
	return builder.String()
}
