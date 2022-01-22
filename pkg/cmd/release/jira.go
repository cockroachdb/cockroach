package release

import (
	"fmt"

	"github.com/andygrunwald/go-jira"
)

const jiraUrl = "https://cockroachlabs.atlassian.net/"

// for ReleaseTrackingIssue
const customFieldShaUrl = "customfield_10210"
const customFieldTagUrl = "customfield_10211"
const customFieldBuildId = "customfield_10251"

// for DeployToClusterIssue
const customFieldHasSlaKey = "customfield_10073"

type metadata struct {
	Version   string `json:"version"`
	Tag       string `json:"tag"`
	Branch    string `json:"branch"`
	SHA       string `json:"sha"`
	Timestamp string `json:"timestamp"`
}

// createReleaseTrackingIssue creates a release tracking issue.
// See example ticket:
// - https://cockroachlabs.atlassian.net/browse/REL-3
// - https://cockroachlabs.atlassian.net/rest/api/2/issue/REL-3
func createReleaseTrackingIssue(meta metadata, setCustomFields bool) *jira.Issue {
	summary := fmt.Sprintf("Release: %s", meta.Version)
	shaUrl := fmt.Sprintf("https://github.com/cockroachlabs/release-staging/commit/%s", meta.SHA)
	tagUrl := fmt.Sprintf("https://github.com/cockroachlabs/release-staging/releases/tag/%s", meta.Tag)
	branchUrl := fmt.Sprintf("https://github.com/cockroachdb/cockroach/commits/%s", meta.Branch)
	description := fmt.Sprintf(`
* Version: %s
* Tag: [%s|%s]
* Branch: [%s|%s]
* SHA: [%s|%s]
* Timestamp: %s
`,
		meta.Version,
		meta.Tag, tagUrl,
		meta.Branch, branchUrl,
		meta.SHA, shaUrl,
		meta.Timestamp,
	)
	var customFields jira.CustomFields
	if setCustomFields {
		customFields := make(jira.CustomFields)
		customFields[customFieldShaUrl] = shaUrl
		customFields[customFieldTagUrl] = tagUrl
		customFields[customFieldBuildId] = meta.Tag // We probably don't need to set "Build ID", since this is the same as Tag
	}
	return createNewIssue(&IssueDetails{
		ProjectKey:   "REL",
		TypeName:     "Task",
		Summary:      summary,
		Description:  description,
		CustomFields: customFields,
	})
}

// createDeployToClusterIssue creates an SREOPS ticket to request release candidate
// qualification.
// See example ticket:
// - https://cockroachlabs.atlassian.net/browse/SREOPS-4037
// - https://cockroachlabs.atlassian.net/rest/api/2/issue/SREOPS-4037
// TODO(celia) - [Future "week 0" work] We'll eventually want the ability to specify
//  a qualification partition & friendly ID:
// During the stability period, release managers may be qualifying multiple candidates
// at the same time. If that's the case, release managers will want the ability to
// explicitly specify which partition to use, so that we don't "overwrite" the
// qualification of one release candidate by pushing a second release candidate
// to the same cluster. Tracked in: https://cockroachlabs.atlassian.net/browse/RE-83
func createDeployToClusterIssue(meta metadata) *jira.Issue {
	summary := fmt.Sprintf("Deploy %s to release qualification cluster", meta.Version)
	description := fmt.Sprintf(`
Could you deploy the Docker image with the following tag to the release qualification CC cluster?

* Build ID: {{%s}}

Please follow [this playbook|https://github.com/cockroachlabs/production/wiki/Deploy-release-qualification-versions]

Thank you\!
`, meta.Tag)
	customFields := make(jira.CustomFields)
	customFields[customFieldHasSlaKey] = "Yes"

	return createNewIssue(&IssueDetails{
		ProjectKey:   "SREOPS",
		TypeName:     "Task",
		Summary:      summary,
		Description:  description,
		CustomFields: customFields,
	})
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

	if details.CustomFields != nil {
		issue.Fields.Unknowns = make(map[string]interface{})
		for key, value := range details.CustomFields {
			issue.Fields.Unknowns[key] = map[string]string{"value": value}
		}
	}
	return &issue
}
