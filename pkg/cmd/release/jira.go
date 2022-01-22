/*
	Below is copy/pasted JIRA-relevant snippets from
	https://github.com/cockroachdb/cockroach/pull/74570.

	TODO(celia)
	- [x] use go-jira tool to post / get a test issue issue
	- [ ] use test code to fill in below snippets
	- [ ] create a JIRA bot shared account:
		- [ ] generate auth token
		- [ ] save in 1Password
	- Q: where do we store the JIRA auth info? and how it get passed through to this code?
*/
package release

import (
	"context"
	"fmt"

	"github.com/andygrunwald/go-jira"
)

const jiraUrl = "https://cockroachlabs.atlassian.net/"

const customFieldHasSlaKey = "customfield_10073"
const customFieldHasSlaValue = "Yes"

type metadata struct {
	Version   string `json:"version"`
	Tag       string `json:"tag"`
	Branch    string `json:"branch"`
	SHA       string `json:"sha"`
	Timestamp string `json:"timestamp"`
}

type jiraTicket struct {
	url    string
	ticket string
}

func postToJira(ctx context.Context, meta metadata) (jiraTicket, error) {
	// TODO: implement
	ticket := jiraTicket{
		url:    "https://....",
		ticket: "RE-555",
	}
	return ticket, nil
}

func createDeployToClusterIssue(version, buildId string) *jira.Issue {
	customFields := make(jira.CustomFields)
	customFields[customFieldHasSlaKey] = customFieldHasSlaValue
	return createNewIssue(&IssueDetails{
		ProjectKey:   "SREOPS",
		TypeName:     "Task",
		Summary:      deployToClusterSummary(version),
		Description:  deployToClusterDescription(buildId),
		CustomFields: customFields,
	})
}

// deployToClusterDescription creates instruction for the SREOPS ticket.
// TODO(celia) - we'll eventually want the ability to specify a qual partition & friendly ID
// (during the stability period, we may be qualifying multiple candidates at the same time;
// during which we'll want to explicitly specify which partition to use, so that we don't
// "overwrite" the qualification of one release candidate by pushing a second release candidate
// to the same cluster.
func deployToClusterDescription(buildId string) string {
	return fmt.Sprintf(`
Could you deploy the Docker image with the following tag to the release qualification CC cluster?

* Build ID: {{%s}}

Please follow [this playbook|https://github.com/cockroachlabs/production/wiki/Deploy-release-qualification-versions]

Thank you\!
`,
		buildId)

}

func deployToClusterSummary(version string) string {
	return fmt.Sprintf("Deploy %s to release qualification cluster", version)
}

type jiraClient struct {
	client *jira.Client
}

func newJiraClient(authUsername, authPassword string) (*jiraClient, error) {
	client, err := getClient(authUsername, authPassword)
	if err != nil {
		return nil, err
	}
	return &jiraClient{
		client: client,
	}, nil
}

// getClient returns jira.Client for username and password (API token).
// To generate an API token, go to https://id.atlassian.com/manage-profile/security/api-tokens.
func getClient(authUsername, authPassword string) (*jira.Client, error) {
	tp := jira.BasicAuthTransport{
		Username: authUsername,
		Password: authPassword,
	}
	client, err := jira.NewClient(tp.Client(), jiraUrl)
	if err != nil {
		return nil, err
	}
	return client, nil
}

type IssueDetails struct {
	Id           string
	Key          string
	TypeName     string
	ProjectKey   string
	Summary      string
	Description  string
	CustomFields jira.CustomFields
}

// GetIssueDetails stores a subset of details from jira.Issue into IssueDetails.
func (j *jiraClient) GetIssueDetails(issueId string) (*IssueDetails, error) {
	issue, _, err := j.client.Issue.Get(issueId, nil)
	if err != nil {
		return nil, err
	}
	customFields, _, err := j.client.Issue.GetCustomFields(issueId)
	if err != nil {
		return nil, err
	}
	return &IssueDetails{
		Id:           issue.ID,
		Key:          issue.Key,
		TypeName:     issue.Fields.Type.Name,
		ProjectKey:   issue.Fields.Project.Name,
		Summary:      issue.Fields.Summary,
		Description:  issue.Fields.Description,
		CustomFields: customFields,
	}, nil
}

func createNewIssue(details *IssueDetails) *jira.Issue {
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

	// set CustomFields
	issue.Fields.Unknowns = make(map[string]interface{})
	for key, value := range details.CustomFields {
		issue.Fields.Unknowns[key] = map[string]string{"value": value}
	}
	return &issue
}
