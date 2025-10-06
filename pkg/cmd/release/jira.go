// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/andygrunwald/go-jira"
)

const jiraBaseURL = "https://cockroachlabs.atlassian.net/"

// TODO(rail): use some "junk" project for the dry-run issues
const dryRunProject = "REL"

// Jira uses Wiki syntax, see https://jira.atlassian.com/secure/WikiRendererHelpAction.jspa?section=all
const trackingIssueTemplate = `
* Version: *{{ .Version }}*
* SHA: [{{ .SHA }}|https://github.com/cockroachlabs/release-staging/commit/{{ .SHA }}]
* Build ID / Tag: [{{ .Tag }}|https://github.com/cockroachlabs/release-staging/releases/tag/{{ .Tag }}]
* Deployment status: _fillmein_
* Publish Cockroach Release: _fillmein_
`

type jiraClient struct {
	client *jira.Client
}

type trackingIssueTemplateArgs struct {
	Version string
	SHA     string
	Tag     string
}

type jiraIssue struct {
	ID           string
	Key          string
	TypeName     string
	ProjectKey   string
	Summary      string
	Description  string
	CustomFields jira.CustomFields
}

// newJiraClient returns jira.Client for username and password (API token).
// To generate an API token, go to https://id.atlassian.com/manage-profile/security/api-tokens.
func newJiraClient(baseURL string, username string, password string) (*jiraClient, error) {
	tp := jira.BasicAuthTransport{
		Username: username,
		Password: password,
	}
	client, err := jira.NewClient(tp.Client(), baseURL)
	if err != nil {
		return nil, fmt.Errorf("cannot create Jira client: %w", err)
	}
	return &jiraClient{
		client: client,
	}, nil
}

// getIssueDetails stores a subset of details from jira.Issue into jiraIssue.
func (j *jiraClient) getIssueDetails(issueID string) (jiraIssue, error) {
	issue, resp, err := j.client.Issue.Get(issueID, nil)
	if err != nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return jiraIssue{}, fmt.Errorf("failed to get issue: %w. Response: %s", err, string(body))
	}
	customFields, resp, err := j.client.Issue.GetCustomFields(issueID)
	if err != nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return jiraIssue{}, fmt.Errorf("failed to get custom fields: %w. Response: %s", err, string(body))
	}
	return jiraIssue{
		ID:           issue.ID,
		Key:          issue.Key,
		TypeName:     issue.Fields.Type.Name,
		ProjectKey:   issue.Fields.Project.Name,
		Summary:      issue.Fields.Summary,
		Description:  issue.Fields.Description,
		CustomFields: customFields,
	}, nil
}

func newIssue(details *jiraIssue) *jira.Issue {
	var issue jira.Issue
	issue.Fields = &jira.IssueFields{}
	issue.Fields.Project = jira.Project{
		Key: details.ProjectKey,
	}
	issue.Fields.Type = jira.IssueType{
		Name: details.TypeName,
	}
	issue.Fields.Summary = details.Summary
	issue.Fields.Description = details.Description

	if details.CustomFields != nil {
		issue.Fields.Unknowns = make(map[string]interface{})
		for key, value := range details.CustomFields {
			issue.Fields.Unknowns[key] = map[string]string{"value": value}
		}
	}
	return &issue
}

func (d jiraIssue) url() string {
	return fmt.Sprintf("%s/browse/%s", strings.TrimSuffix(jiraBaseURL, "/"), d.Key)
}

// createJiraIssue creates a **real** JIRA issue.
func createJiraIssue(client *jiraClient, issue *jira.Issue) (jiraIssue, error) {
	newIssue, resp, err := client.client.Issue.Create(issue)
	if err != nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return jiraIssue{}, fmt.Errorf("failed to create issue: %w. Response: %s", err, string(body))
	}
	details, err := client.getIssueDetails(newIssue.ID)
	if err != nil {
		return jiraIssue{}, err
	}
	return details, nil
}

// createTrackingIssue creates a release tracking issue.
// See example ticket:
// - https://cockroachlabs.atlassian.net/browse/REL-3
// - https://cockroachlabs.atlassian.net/rest/api/2/issue/REL-3
func createTrackingIssue(client *jiraClient, release releaseInfo, dryRun bool) (jiraIssue, error) {
	templateArgs := trackingIssueTemplateArgs{
		Version: release.nextReleaseVersion,
		Tag:     release.buildInfo.Tag,
		SHA:     release.buildInfo.SHA,
	}
	description, err := templateToText(templateArgs, trackingIssueTemplate)
	if err != nil {
		return jiraIssue{}, fmt.Errorf("cannot parse tracking issue template: %w", err)
	}
	summary := fmt.Sprintf("Release: %s", release.nextReleaseVersion)
	projectKey := "REL"
	if dryRun {
		projectKey = dryRunProject
	}
	issue := newIssue(&jiraIssue{
		ProjectKey:  projectKey,
		TypeName:    "CRDB Release",
		Summary:     summary,
		Description: description,
	})
	return createJiraIssue(client, issue)
}
