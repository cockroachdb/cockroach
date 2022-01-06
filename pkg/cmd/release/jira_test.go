package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/andygrunwald/go-jira"
	"github.com/stretchr/testify/require"
)

// TestGetIssueDetails fetches details for example tickets.
// To see all available data from jira.Issue for a `jiraIssueKey`, go to:
// - https://cockroachlabs.atlassian.net/rest/api/2/issue/{jiraIssueKey}
// To see custom field details [for all org custom fields], go to:
// - https://cockroachlabs.atlassian.net/rest/api/2/field
func TestGetIssueDetails(t *testing.T) {
	testIssues := []struct {
		id                   string
		expectedKey          string
		expectedSummary      string
		expectedCustomFields map[string]string
	}{
		{
			id:                   "49848",
			expectedKey:          "SREOPS-4037",
			expectedSummary:      "Deploy v21.2.4 to release qualification cluster",
			expectedCustomFields: map[string]string{customFieldHasSlaKey: "Yes"},
		},
		{
			id:              "49841",
			expectedKey:     "RE-68",
			expectedSummary: "[JIRA integration] Auto-create/update SREOPS and release tracking issue ",
		},
		{
			id:              "32815",
			expectedKey:     "REL-3",
			expectedSummary: "Test CRDB Release",
			expectedCustomFields: map[string]string{
				customFieldShaUrl:  "https://github.com/cockroachlabs/release-staging/commit/fdd672fed84323279f7070127f36cdbdc5c26b03",
				customFieldTagUrl:  "https://github.com/cockroachlabs/release-staging/releases/tag/v22.1.0-alpha.00000000-2771-gfdd672fed8",
				customFieldBuildId: "MyBuildId",
			},
		},
	}

	username, password := getAuthUsernameAndPassword()
	client, err := newJiraClient(username, password)
	require.NoError(t, err)
	for _, test := range testIssues {
		details, err := client.GetIssueDetails(test.id)
		require.NoError(t, err)
		require.Equal(t, test.expectedKey, details.Key)
		require.Equal(t, test.expectedSummary, details.Summary)
		for key, expectedValue := range test.expectedCustomFields {
			require.Equal(t, expectedValue, details.CustomFields[key])
		}
	}
}

// TestCreateReleaseTrackingIssue creates a new test tracking issue for the
// Release project: https://cockroachlabs.atlassian.net/jira/software/c/projects/REL/boards/62
// Note that we're not actively using this board yet, so feel free
// to use this board/project to test away!!
//
// 1/23(celia) - I don't have full permission to the project. See:
// - https://cockroachlabs.atlassian.net/browse/ATS-351
// This method should once (as well as posting the custom fields)
// once ATS-351 is done.
func TestCreateReleaseTrackingIssue_DOES_NOT_WORK_YET(t *testing.T) {
	username, password := getAuthUsernameAndPassword()
	client, err := newJiraClient(username, password)
	require.NoError(t, err)
	setCustomFields := true
	issue := createReleaseTrackingIssue(release{
		nextReleaseVersion: "v21.2.4",
		releaseSeries:      "21.2",
		nextReleaseMetadata: metadata{
			Tag:       "v21.2.3-144-g0c8df44947",
			SHA:       "fdd672fed84323279f7070127f36cdbdc5c26b03",
			Timestamp: time.Now().String(),
		},
	},
		setCustomFields,
	)
	createRealJiraIssue(t, client, issue)
}

// TestCreateTestREIssue_WORKS:
// This tests the summary/description of
// createReleaseTrackingIssue by posting it to
// the RE project, for which we have permission.
// Note that the RE project doesn't have all the
// custom fields the REL project does, so we
// shouldn't set those.
func TestCreateTestREIssue_WORKS(t *testing.T) {
	username, password := getAuthUsernameAndPassword()
	client, err := newJiraClient(username, password)
	require.NoError(t, err)

	// Attempting to set the custom fields
	// for the RE project will fail the post request,
	// since the RE project doesn't have these custom
	// fields, only the REL project.
	setCustomFields := false

	issue := createReleaseTrackingIssue(release{
		nextReleaseVersion: "v21.2.4",
		releaseSeries:      "21.2",
		nextReleaseMetadata: metadata{
			Tag:       "v21.2.3-144-g0c8df44947",
			SHA:       "fdd672fed84323279f7070127f36cdbdc5c26b03",
			Timestamp: time.Now().String(),
		},
	},
		setCustomFields,
	)

	// Before sending the post request, let's override
	// the `REL` project with our test `RE` project.
	issue.Fields.Project = jira.Project{
		Key: "RE",
	}

	createRealJiraIssue(t, client, issue)
}

// TestCreateDeployToClusterIssue creates a new JIRA issue:
// *******************************************************
//                      WARNING!!
// *******************************************************
// This creates a real SREOPS ticket, so remember to
// cancel/delete the SREOPS ticket afterwards! :)
// *******************************************************
func TestCreateDeployToClusterIssue(t *testing.T) {
	testVersion := "v21.2.4"
	testBuildId := "v21.2.3-144-g0c8df44947"

	username, password := getAuthUsernameAndPassword()
	client, err := newJiraClient(username, password)
	require.NoError(t, err)
	issue := createDeployToClusterIssue(release{
		nextReleaseVersion: testVersion,
		nextReleaseMetadata: metadata{
			Tag: testBuildId,
		},
	})
	createRealJiraIssue(t, client, issue)
}

// createRealJiraIssue creates a **real** JIRA issue:
// refer to the printed test output to get the url for the
// newly-created ticket.
// *******************************************************
//                      WARNING!!
// *******************************************************
// If this is creating a ticket for an actively used
// project, remember to cancel/delete the ticket
// afterwards! :)
// *******************************************************
func createRealJiraIssue(t *testing.T, client *jiraClient, issue *jira.Issue) {
	newIssue, resp, err := client.client.Issue.Create(issue)
	// saving resp in case we need to inspect (e.g. get a 400)
	require.NotEmpty(t, resp)
	require.NoError(t, err)

	// The only set values on a newIssue object are ID, Self, Key.
	fmt.Printf("New Issue Key: %s\n", newIssue.Key)

	// We'll need to make another get all to fetch additional field values
	details, err := client.GetIssueDetails(newIssue.ID)
	require.NoError(t, err)

	fmt.Printf("%s: %s\n", details.Key, details.Summary)
	fmt.Printf("Type: %s\n", details.TypeName)
	fmt.Printf("URL: https://cockroachlabs.atlassian.net/browse/%s\n", details.Key)
}

func getAuthUsernameAndPassword() (string, string) {
	username := os.Getenv("JIRA_AUTH_USERNAME")  // e.g. your-email@cockroachlabs.com
	password := os.Getenv("JIRA_AUTH_API_TOKEN") // to generate, goto: https://id.atlassian.com/manage-profile/security/api-tokens
	if username == "" || password == "" {
		panic("Both JIRA_AUTH_USERNAME and JIRA_AUTH_API_TOKEN must be set")
	}
	return username, password
}
