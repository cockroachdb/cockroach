package release

import (
	"fmt"
	"os"
	"testing"

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
			expectedCustomFields: map[string]string{customFieldHasSlaKey: customFieldHasSlaValue},
		},
		{
			id:              "49841",
			expectedKey:     "RE-68",
			expectedSummary: "[JIRA integration] Auto-create/update SREOPS and release tracking issue ",
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

// TestCreateDeployToClusterIssue creates a new JIRA issue:
// ** WARNING!! **: this creates a real SREOPS ticket, so
// remember to cancel/delete the SREOPS ticket afterwards! :)
func TestCreateDeployToClusterIssue(t *testing.T) {
	testVersion := "v21.2.4"
	testBuildId := "v21.2.3-144-g0c8df44947"

	username, password := getAuthUsernameAndPassword()
	client, err := newJiraClient(username, password)
	require.NoError(t, err)

	deployToClusterIssue := createDeployToClusterIssue(testVersion, testBuildId)

	newIssue, _, err := client.client.Issue.Create(deployToClusterIssue)
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
