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
	"time"
)

// searchDocsIssues returns a map containing all the product change docs issues that have been created since the given
// start time. For reference, it's structured as map[crdb_pr_number]map[crdb_commit]docs_pr_number.
func searchJiraDocsIssues(startTime time.Time) (map[int]map[string]string, error) {
	var result = map[int]map[string]string{}
	startAt := 0
	pageSize := 100
	maxResults, total, err := searchJiraDocsIssuesSingle(startTime, startAt, pageSize, result)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	pageSize = maxResults // Jira REST API page sizes are subject to change at any time
	for total > startAt+pageSize {
		startAt += pageSize
		_, _, err = searchJiraDocsIssuesSingle(startTime, startAt, pageSize, result)
		if err != nil {
			fmt.Println(err)
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
		"jql":          fmt.Sprintf(`project = DOC and "Doc Type[Dropdown]" = "Product Change" and createdDate >= "%s"`, startTime.Format(time.DateTime)),
		"maxResults":   pageSize,
		"startAt":      startAt,
	}
	var search jiraIssueSearch
	err := queryJiraRESTAPI(apiEndpoint, method, headers, body, &search)
	if err != nil {
		fmt.Println(err)
		return 0, 0, err
	}
	for _, issue := range search.Issues {
		prNumber, commitSha, err := extractPRNumberCommitFromDocsIssueBody(issue.RenderedFields.Description)
		if err != nil {
			fmt.Println(err)
			return 0, 0, err
		}
		if prNumber != 0 && commitSha != "" {
			_, ok := m[prNumber]
			if !ok {
				m[prNumber] = make(map[string]string)
			}
			m[prNumber][commitSha] = issue.Key
		}
	}
	return search.MaxResults, search.Total, nil
}
