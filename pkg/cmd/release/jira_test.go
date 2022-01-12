package release

import (
	"fmt"
	"testing"

	"github.com/andygrunwald/go-jira"
	"github.com/stretchr/testify/require"
)

const baseUrl = "https://cockroachlabs.atlassian.net/"

type issueDetails struct {
	Id          string
	Key         string
	TypeName    string
	ProjectKey  string
	Summary     string
	Description string
}

func TestGetIssue(t *testing.T) {
	// https://cockroachlabs.atlassian.net/rest/api/2/issue/RE-68
	testIssueId := "49841"
	details, err := getIssueDetails(testIssueId)
	require.NoError(t, err)
	fmt.Printf("issue: %+v\n", details)
}

func TestPostToJIRA(t *testing.T) {
	client, err := getClient()
	require.NoError(t, err)
	newIssue := createNewIssue(&issueDetails{
		ProjectKey:  "RE",
		TypeName:    "Bug",
		Summary:     "TEST: post via go-jira",
		Description: "@celia is testing JIRA post via github.com/andygrunwald/go-jira",
	})
	issue, _, err := client.Issue.Create(newIssue)
	require.NoError(t, err)

	// The only set values on issue object are ID, Self, Key.
	fmt.Printf("New Issue Key: %s\n", issue.Key)

	// We'll need to make another get all to fetch additional field values
	details, err := getIssueDetails(issue.ID)
	require.NoError(t, err)
	fmt.Printf("issue: %+v\n", details)

	fmt.Printf("%s: %+v\n", details.Key, details.Summary)
	fmt.Printf("Type: %s\n", details.TypeName)
	fmt.Printf("URL: https://cockroachlabs.atlassian.net/browse/%s\n", details.Key)
}

func createNewIssue(details *issueDetails) *jira.Issue {
	var newIssue jira.Issue
	newIssue.Fields = &jira.IssueFields{}
	newIssue.Fields.Project = jira.Project{
		Key: details.ProjectKey,
	}
	newIssue.Fields.Type = jira.IssueType{
		Name: details.TypeName,
	}
	newIssue.Fields.Summary = details.Summary
	newIssue.Fields.Description = details.Description
	return &newIssue
}

func getIssueDetails(issueId string)  (*issueDetails, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}
	issue, _, err := client.Issue.Get(issueId, nil)
	if err != nil {
		return nil, err
	}
	return &issueDetails{
		Id:          issue.ID,
		Key:         issue.Key,
		TypeName:    issue.Fields.Type.Name,
		ProjectKey:  issue.Fields.Project.Name,
		Summary:     issue.Fields.Summary,
		Description: issue.Fields.Description,
	}, nil
}

func getClient() (*jira.Client, error) {
	tp := jira.BasicAuthTransport{
		Username: testUsername,
		Password: testApiToken, // to generate, goto: https://id.atlassian.com/manage-profile/security/api-tokens
	}
	client, err := jira.NewClient(tp.Client(), baseUrl)
	if err != nil {
		return nil, err
	}
	return client, nil
}
