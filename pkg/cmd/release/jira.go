package main

import (
	"fmt"
	"strings"

	"github.com/andygrunwald/go-jira"
)

const jiraBaseUrl = "https://cockroachlabs.atlassian.net/"

// for ReleaseTrackingIssue
const customFieldShaUrl = "customfield_10210"
const customFieldTagUrl = "customfield_10211"
const customFieldBuildId = "customfield_10251"

// for DeployToClusterIssue
const customFieldHasSlaKey = "customfield_10073"

// Jira uses Wiki syntax, see https://jira.atlassian.com/secure/WikiRendererHelpAction.jspa?section=all
const trackingIssueTemplate = `
* Version: *{{ .Version }}*
* SHA: [{{ .SHA }}|https://github.com/cockroachlabs/release-staging/commit/{{ .SHA }}]
* Tag: [{{ .Tag }}|https://github.com/cockroachlabs/release-staging/releases/tag/{{ .Tag }}]
* SRE issue: [{{ .SREIssue }}]
* Deployment status: _fillmein_
* Publish Cockroach Release: _fillmein_

h2. [Release process checklist|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process]

* Assign the SRE issue [{{ .SREIssue }}] (use "/genie whoisoncall" in Slack). They will be notified by Jira.
* [5-8. Verify node crash reports|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/328859690/Release+Qualification#Verify-node-crash-reports-appear-in-sentry.io]

h2. Do not proceed below until the release date.

Release date: _fillmein_

* [9. Publish Cockroach Release|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-9.PublishTheRelease]
* Ack security@ and release-engineering-team@ on the generated AWS S3 bucket write alert to confirm these writes were part of a planned release
* [10. Check binaries|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-10.CheckBinaries]
* [12. Announce the release is cut to releases@|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-13.Announcethereleaseiscuttoreleases@]
* [13. Update version numbers|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-14.Updateversionnumbers]
* For production or stable releases in the latest [major release|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-Knowifthereleaseisonthelatestmajorreleaseseries] series only (in August 2020, this is the v20.1 series):
* Update [Brew Recipe|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-Brewrecipe]
* Update [Orchestrator configurations:CRDB|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-Orchestratorconfigurations:CRDB]
* Update [Orchestrator configurations:Helm Charts|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-Orchestratorconfigurations:HelmCharts]

For all production or stable releases:
* Create a ticket in the [Dev Inf tracker|https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/429097164/Submitting+Issues+Requests+to+the+Developer+Infrastructure+team] to update the Red Hat Container Image Repository
* *After docs are updated* [Announce version to registration cluster|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-AnnounceVersionToRegCluster]
* [Update version map in bin/roachtest (all stable releases) and regenerate test fixtures (only major release)|https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/73105625/Release+process#Releaseprocess-Updateversionmapinroachtestandregeneratetestfixtures]
* Update docs (handled by Docs team)
* External communications for release (handled by Marketing team)
`
const SREIssueTemplate = `
Could you deploy the Docker image with the following tag to the release qualification CC cluster?

* Version: {{ .Version }}
* Build ID: {{ .Tag }}

Please follow [this playbook|https://github.com/cockroachlabs/production/wiki/Deploy-release-qualification-versions]

Thank you\!
`

type trackingIssueTemplateArgs struct {
	Version  string
	SHA      string
	Tag      string
	SREIssue string
}

type SREIssueTemplateArgs struct {
	Version string
	Tag     string
}

// createReleaseTrackingIssue creates a release tracking issue.
// See example ticket:
// - https://cockroachlabs.atlassian.net/browse/REL-3
// - https://cockroachlabs.atlassian.net/rest/api/2/issue/REL-3
func createReleaseTrackingIssue(release releaseInfo, sreIssue IssueDetails) (*jira.Issue, error) {
	summary := fmt.Sprintf("Release: %s", release.nextReleaseVersion)
	templateArgs := trackingIssueTemplateArgs{
		Version:  release.nextReleaseVersion,
		Tag:      release.buildInfo.Tag,
		SHA:      release.buildInfo.SHA,
		SREIssue: sreIssue.Key,
	}
	description, err := templateToText(trackingIssueTemplate, templateArgs)
	if err != nil {
		return &jira.Issue{}, fmt.Errorf("cannot parse tracking issue template: %w", err)
	}
	return createNewIssue(&IssueDetails{
		ProjectKey: "REL",
		TypeName:   "Task",
		// TODO: switch to TypeName: "CRDB Release", which requires some fields to be set
		Summary:     summary,
		Description: description,
	}), nil
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
func createDeployToClusterIssue(release releaseInfo) (*jira.Issue, error) {
	summary := fmt.Sprintf("Deploy %s to release qualification cluster", release.nextReleaseVersion)
	description, err := templateToHTML(SREIssueTemplate, SREIssueTemplateArgs{Version: release.nextReleaseVersion,
		Tag: release.buildInfo.Tag})
	if err != nil {
		return &jira.Issue{}, fmt.Errorf("cannot parse SRE issue template: %w", err)
	}
	customFields := make(jira.CustomFields)
	customFields[customFieldHasSlaKey] = "Yes"

	return createNewIssue(&IssueDetails{
		ProjectKey:   "SREOPS",
		TypeName:     "Task",
		Summary:      summary,
		Description:  description,
		CustomFields: customFields,
	}), nil
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
	client, err := jira.NewClient(tp.Client(), jiraBaseUrl)
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
func (j *jiraClient) GetIssueDetails(issueId string) (IssueDetails, error) {
	issue, _, err := j.client.Issue.Get(issueId, nil)
	if err != nil {
		return IssueDetails{}, err
	}
	customFields, _, err := j.client.Issue.GetCustomFields(issueId)
	if err != nil {
		return IssueDetails{}, err
	}
	return IssueDetails{
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

// createRealJiraIssue creates a **real** JIRA issue:
// refer to the printed test output to get the url for the
// newly-created ticket.
func createJiraIssue(client *jiraClient, issue *jira.Issue) (IssueDetails, error) {
	newIssue, _, err := client.client.Issue.Create(issue)
	if err != nil {
		return IssueDetails{}, err
	}

	details, err := client.GetIssueDetails(newIssue.ID)
	if err != nil {
		return IssueDetails{}, err
	}
	return details, nil
}

func createTrackingIssue(
	username string, token string, release releaseInfo, sreIssue IssueDetails,
) (IssueDetails, error) {
	client, err := newJiraClient(username, token)
	if err != nil {
		return IssueDetails{}, err
	}
	issue, err := createReleaseTrackingIssue(release, sreIssue)
	if err != nil {
		return IssueDetails{}, fmt.Errorf("cannot create tracking issue details: %w", err)
	}

	// TODO: remove the following when ready
	// Before sending the post request, let's override
	// the `REL` project with our test `RE` project.
	issue.Fields.Project = jira.Project{
		Key: "RE",
	}

	return createJiraIssue(client, issue)
}

func createSRETicket(username string, token string, release releaseInfo) (IssueDetails, error) {
	client, err := newJiraClient(username, token)
	if err != nil {
		return IssueDetails{}, err
	}
	issue, err := createDeployToClusterIssue(release)
	if err != nil {
		return IssueDetails{}, fmt.Errorf("cannot create SRE issue")
	}
	return createJiraIssue(client, issue)
}

func jiraUrl(details IssueDetails) string {
	return fmt.Sprintf("%s/browse/%s", strings.TrimSuffix(jiraBaseUrl, "/"), details.Key)
}
