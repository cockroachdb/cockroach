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

// Regex components for finding and validating issue and epic references in a string
var (
	jiraBrowseUrlPart    = crlJiraBaseUrl + "browse/"
	ghIssuePart          = `(#\d+)`                                                   // e.g., #12345
	ghIssueRepoPart      = `([\w.-]+[/][\w.-]+#\d+)`                                  // e.g., cockroachdb/cockroach#12345
	ghURLPart            = `(https://github.com/[-a-z0-9]+/[-._a-z0-9/]+/issues/\d+)` // e.g., https://github.com/cockroachdb/cockroach/issues/12345
	jiraURLPart          = jiraBrowseUrlPart + jiraIssuePart                          // e.g., https://cockroachlabs.atlassian.net/browse/DOC-3456
	afterRefPart         = `[,.;]?(?:[ \t\n\r]+|$)`
	jiraIssuePart        = `([[:alpha:]]+-\d+)` // e.g., DOC-3456
	ghIssuePartRE        = regexp.MustCompile(ghIssuePart)
	ghIssueRepoPartRE    = regexp.MustCompile(ghIssueRepoPart)
	ghURLPartRE          = regexp.MustCompile(ghURLPart)
	jiraURLPartRE        = regexp.MustCompile(jiraURLPart)
	jiraIssuePartRE      = regexp.MustCompile(jiraIssuePart)
	issueRefPart         = ghIssuePart + "|" + ghIssueRepoPart + "|" + ghURLPart + "|" + jiraIssuePart + "|" + jiraURLPart
	fixIssueRefRE        = regexp.MustCompile(`(?im)(?i:close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+(?:(?:` + issueRefPart + `)` + afterRefPart + ")+")
	informIssueRefRE     = regexp.MustCompile(`(?im)(?:part of|see also|informs):?\s+(?:(?:` + issueRefPart + `)` + afterRefPart + ")+")
	epicRefRE            = regexp.MustCompile(`(?im)epic:?\s+(?:(?:` + jiraIssuePart + "|" + jiraURLPart + `)` + afterRefPart + ")+")
	epicNoneRE           = regexp.MustCompile(`(?im)epic:?\s+(?:(none)` + afterRefPart + ")+")
	githubJiraIssueRefRE = regexp.MustCompile(issueRefPart)
	jiraIssueRefRE       = regexp.MustCompile(jiraIssuePart + "|" + jiraURLPart)
	prNumberHTMLRE       = regexp.MustCompile(`Related PR: <a href="https://github.com/cockroachdb/cockroach/pull/(\d+)\D`)
	commitShaHTMLRE      = regexp.MustCompile(`Commit: <a href="https://github.com/cockroachdb/cockroach/commit/(\w+)\W`)
	invalidEpicRefs      = make(map[string]string)
)

// extractPRNumberCommitFromDocsIssueBody takes a Jira issue description and outputs a matching PR number, a commit SHA,
// and any error if applicable.
func extractPRNumberCommitFromDocsIssueBody(body string) (int, string, error) {
	prMatches := prNumberHTMLRE.FindStringSubmatch(body)
	if len(prMatches) < 2 {
		return 0, "", fmt.Errorf("error: No PR number found in issue body")
	}
	prNumber, err := strconv.Atoi(prMatches[1])
	if err != nil {
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
	result := extractStringsFromMessage(message, epicRefRE, jiraIssueRefRE)
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		count := result[key]
		var isEpic bool
		epicKey, ok := invalidEpicRefs[key]
		if ok {
			isEpic = epicKey == key
		} else {
			var err error
			isEpic, epicKey, err = getValidEpicRef(key)
			if err != nil {
				// if the supplied issueKey is bad or there's a problem with the Jira REST API, simply print out
				// the error message, but don't return it. Instead, remove the epic from the list, since we were
				// unable to validate whether it was an epic, and we strictly need a valid epic key.
				fmt.Printf("error: Unable to determine whether %s is a valid epic. Caused by:\n%s\n", key, err)
				delete(result, key)
				continue
			}
			if epicKey != "" {
				invalidEpicRefs[key] = epicKey
				invalidEpicRefs[epicKey] = epicKey
			}
		}
		if isEpic {
			continue
		} else if key != epicKey {
			if epicKey != "" {
				result[epicKey] = count
			}
			delete(result, key)
		}
	}
	return result
}

func containsEpicNone(message string) bool {
	return epicNoneRE.MatchString(message)
}

func extractIssueEpicRefs(prBody, commitBody string) ([]adfNode, error) {
	epicRefs := extractEpicIDs(commitBody + "\n" + prBody)
	refInfo := epicIssueRefInfo{
		epicRefs:        epicRefs,
		epicNone:        containsEpicNone(commitBody + "\n" + prBody),
		issueCloseRefs:  extractFixIssueIDs(commitBody + "\n" + prBody),
		issueInformRefs: extractInformIssueIDs(commitBody + "\n" + prBody),
	}
	var result []adfNode
	makeAdfNode := func(nodeType string, nodeText string, isLink bool) adfNode {
		result := adfNode{
			Type: nodeType,
			Text: nodeText,
		}
		if isLink {
			result.Marks = []adfMark{
				{
					Type: "link",
					Attrs: map[string]string{
						"href": jiraBrowseUrlPart + nodeText,
					},
				},
			}
		}
		return result
	}
	handleRefs := func(refs map[string]int, refPrefix string) error {
		if len(refs) > 0 {
			result = append(result, makeAdfNode("hardBreak", "", false))
			result = append(result, makeAdfNode("text", refPrefix, false))
			for x := range refs {
				ref, err := getJiraIssueFromRef(x)
				if err != nil {
					return err
				}
				result = append(result, makeAdfNode("text", " ", false))
				result = append(result, makeAdfNode("text", ref, true))
			}
		}
		return nil
	}
	err := handleRefs(refInfo.epicRefs, "Epic:")
	if err != nil {
		return []adfNode{}, err
	}
	err = handleRefs(refInfo.issueCloseRefs, "Fixes:")
	if err != nil {
		return []adfNode{}, err
	}
	err = handleRefs(refInfo.issueInformRefs, "Informs:")
	if err != nil {
		return []adfNode{}, err
	}
	if refInfo.epicNone && len(result) == 0 {
		result = append(result, makeAdfNode("hardBreak", "", false))
		result = append(result, makeAdfNode("text", "Epic: none", false))
	}
	return result, nil
}

func getJiraIssueFromRef(ref string) (string, error) {
	if jiraIssuePartRE.MatchString(ref) {
		return ref, nil
	} else if jiraURLPartRE.MatchString(ref) {
		return strings.Replace(ref, jiraBrowseUrlPart, "", 1), nil
	} else if ghIssueRepoPartRE.MatchString(ref) {
		split := strings.FieldsFunc(ref, splitBySlashOrHash)
		issueNumber, err := strconv.Atoi(split[2])
		if err != nil {
			return "", err
		}
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber)
		if err != nil {
			return "", err
		}
		return issueRef, nil
	} else if ghIssuePartRE.MatchString(ref) {
		issueNumber, err := strconv.Atoi(strings.Replace(ref, "#", "", 1))
		if err != nil {
			return "", err
		}
		issueRef, err := getJiraIssueFromGitHubIssue("cockroachdb", "cockroach", issueNumber)
		if err != nil {
			return "", err
		}
		return issueRef, nil
	} else if ghURLPartRE.MatchString(ref) {
		replace1 := strings.Replace(ref, "https://github.com/", "", 1)
		replace2 := strings.Replace(replace1, "/issues", "", 1)
		split := strings.FieldsFunc(replace2, splitBySlashOrHash)
		issueNumber, err := strconv.Atoi(split[2])
		if err != nil {
			return "", err
		}
		issueRef, err := getJiraIssueFromGitHubIssue(split[0], split[1], issueNumber)
		if err != nil {
			return "", err
		}
		return issueRef, nil
	} else {
		return "", fmt.Errorf("error: Malformed epic/issue ref (%s)", ref)
	}
}

func extractProductChangeDocTypeId(
	allowedValues []jiraCreateIssueMetaDocTypeAllowedValue,
) (string, error) {
	for _, v := range allowedValues {
		if v.Value == "Product Change" {
			return v.Id, nil
		}
	}
	return "", fmt.Errorf(
		"unable to locate a doc type of 'Product Change' in the %s project",
		jiraDocsProjectCode,
	)
}
