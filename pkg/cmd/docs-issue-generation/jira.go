// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"strconv"
	"time"
)

const (
	jiraDocsProjectCode  = "DOC"
	jiraEpicIssueTypeKey = 10000
)

// searchDocsIssues returns a map containing all the product change docs issues that have been created since the given
// start time. For reference, it's structured as map[crdb_pr_number]map[crdb_commit]docs_pr_number.
func searchJiraDocsIssues(startTime time.Time) (map[int]map[string]string, error) {
	var result = map[int]map[string]string{}
	startAt := 0
	pageSize := 100
	maxResults, total, err := searchJiraDocsIssuesSingle(startTime, pageSize, startAt, result)
	if err != nil {
		return nil, err
	}
	pageSize = maxResults // Jira REST API page sizes are subject to change at any time
	for total > startAt+pageSize && pageSize > 0 {
		startAt += pageSize
		_, _, err = searchJiraDocsIssuesSingle(startTime, pageSize, startAt, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// searchDocsIssuesSingle searches one page of docs issues at a time. These docs issues will ultimately be excluded
// from the PRs through which we iterate to create new product change docs issues. This function returns a bool to
// check if there are more than 100 results, the cursor to query for the next page of results, and an error if
// one exists.
func searchJiraDocsIssuesSingle(
	startTime time.Time, pageSize, startAt int, m map[int]map[string]string,
) (int, int, error) {
	apiEndpoint := "search"
	method := "POST"
	headers := map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
	}
	body := map[string]interface{}{
		"expand": []string{
			"renderedFields",
		},
		"fields": []string{
			"description",
		},
		"fieldsByKeys": false,
		"jql": fmt.Sprintf(
			`project = DOC and "Doc Type[Dropdown]" = "Product Change" and summary ~ "PR #" and createdDate >= "%s"`,
			startTime.Format("2006-01-02 15:04"),
		),
		"maxResults": pageSize,
		"startAt":    startAt,
	}
	var search jiraIssueSearch
	err := queryJiraRESTAPI(apiEndpoint, method, headers, body, &search)
	if err != nil {
		return 0, 0, err
	}
	for _, issue := range search.Issues {
		prNumber, commitSha, err := extractPRNumberCommitFromDocsIssueBody(issue.RenderedFields.Description)
		if err != nil {
			fmt.Printf("Error processing issue %s: %v", issue.Key, err)
			continue // Skip this issue and continue with the next one
		}
		if prNumber != 0 && commitSha != "" {
			_, ok := m[prNumber]
			if !ok {
				m[prNumber] = map[string]string{}
			}
			m[prNumber][commitSha] = issue.Key
		}
	}
	return search.MaxResults, search.Total, nil
}

// getJiraIssueCreateMeta gets details about the metadata of a project to help assist in creating issues of type "Docs"
// within that project.
var getJiraIssueCreateMeta = func() (jiraIssueCreateMeta, error) {
	apiEndpoint := fmt.Sprintf("issue/createmeta?projectKeys=%s&issuetypeNames=Docs&expand=projects.issuetypes.fields.parent", jiraDocsProjectCode)
	method := "GET"
	headers := map[string]string{
		"Accept": "application/json",
	}
	var search jiraIssueCreateMeta
	err := queryJiraRESTAPI(apiEndpoint, method, headers, nil, &search)
	if err != nil {
		return jiraIssueCreateMeta{}, err
	}
	return search, nil
}

// getValidEpicRef takes an issue key (PROJCODE-####) and outputs a bool if the issue key is an epic, a new epic key
// if the issue is part of another epic, and an error.
var getValidEpicRef = func(issueKey string) (bool, string, error) {
	apiEndpoint := fmt.Sprintf("issue/%s?fields=issuetype,customfield_10014", issueKey)
	method := "GET"
	headers := map[string]string{
		"Accept": "application/json",
	}
	var issueResp jiraIssue
	err := queryJiraRESTAPI(apiEndpoint, method, headers, nil, &issueResp)
	if err != nil {
		return false, "", err
	}
	var isEpic bool
	var epicKey string
	if issueResp.Fields.Issuetype.Id == strconv.Itoa(jiraEpicIssueTypeKey) {
		isEpic = true
		epicKey = issueKey
	} else {
		epicKey = issueResp.Fields.EpicLink
	}
	return isEpic, epicKey, nil
}

func (dib docsIssueBatch) createDocsIssuesInBulk() error {
	apiEndpoint := "issue/bulk"
	method := "POST"
	headers := map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
	}
	var res jiraBulkIssueCreateResponse
	err := queryJiraRESTAPI(apiEndpoint, method, headers, dib, &res)
	if err != nil {
		return err
	}
	if len(res.Errors) > 0 {
		return fmt.Errorf("error: Could not create issues: %+v", res.Errors)
	}
	return nil
}
