package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// TaskParams holds the parameters for creating a Jira internal task.
type TaskParams struct {
	Title        string
	Description  string
	Parent       string // Epic/parent issue key (e.g., CRDB-12345)
	Sprint       string // Sprint ID for customfield_10020
	SprintName   string // Sprint name for display (not sent to API)
	Assignee     string // Jira account ID for assignee (resolved from email)
	AssigneeName string // Display name for output
}

// JiraClient is an interface for Jira API operations.
// This allows for easy mocking in tests.
type JiraClient interface {
	CreateInternalTask(params TaskParams) (string, error)
}

// UserInfo holds the resolved user information.
type UserInfo struct {
	AccountID   string
	DisplayName string
}

// resolveUser looks up a user by email via Jira API.
func (c *RealJiraClient) resolveUser(email string) (UserInfo, error) {
	body, err := c.doRequest("GET", jiraBaseURL+"/user/search?query="+email, nil)
	if err != nil {
		return UserInfo{}, fmt.Errorf("user search failed: %w", err)
	}

	var users []struct {
		AccountID    string `json:"accountId"`
		EmailAddress string `json:"emailAddress"`
		DisplayName  string `json:"displayName"`
	}
	if err := json.Unmarshal(body, &users); err != nil {
		return UserInfo{}, fmt.Errorf("failed to parse user search response: %w", err)
	}

	for _, u := range users {
		if strings.EqualFold(u.EmailAddress, email) {
			return UserInfo{AccountID: u.AccountID, DisplayName: u.DisplayName}, nil
		}
	}

	if len(users) == 0 {
		return UserInfo{}, fmt.Errorf("no user found for email %q", email)
	}
	return UserInfo{}, fmt.Errorf("no exact match for %q; found: %v", email, users)
}

// CreateInternalTask creates an Internal Task in Jira via the REST API.
// params.Assignee must already be a resolved account ID.
func (c *RealJiraClient) CreateInternalTask(params TaskParams) (string, error) {
	// Build the request body
	fields := map[string]any{
		"project": map[string]string{
			"key": c.Config.Project,
		},
		"summary": params.Title,
		"description": map[string]any{
			"type":    "doc",
			"version": 1,
			"content": []map[string]any{
				{
					"type": "paragraph",
					"content": []map[string]any{
						{
							"type": "text",
							"text": params.Description,
						},
					},
				},
			},
		},
		"issuetype": map[string]string{
			"name": "Internal Task",
		},
		// customfield_10089 is "Engineering team"
		"customfield_10089": map[string]string{
			"value": c.Config.EngTeam,
		},
	}

	// Add optional assignee (empty means "unassigned")
	if params.Assignee != "" {
		fields["assignee"] = map[string]string{
			"accountId": params.Assignee,
		}
	}

	// Add optional parent (epic)
	if params.Parent != "" {
		fields["parent"] = map[string]string{
			"key": params.Parent,
		}
	}

	// Add optional sprint
	if params.Sprint != "" {
		sprintID, err := strconv.Atoi(params.Sprint)
		if err != nil {
			return "", fmt.Errorf("internal error: sprint should be numeric at this point: %w", err)
		}
		fields["customfield_10020"] = sprintID
	}

	requestBody := map[string]any{
		"fields": fields,
	}

	body, err := c.doRequest("POST", jiraBaseURL+"/issue", requestBody)
	if err != nil {
		return "", err
	}

	var result struct {
		Key  string `json:"key"`
		Self string `json:"self"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Key, nil
}

// IssueFields represents the fields we care about when verifying an issue.
type IssueFields struct {
	Assignee *struct {
		AccountID string `json:"accountId"`
	} `json:"assignee"`
	Parent *struct {
		Key string `json:"key"`
	} `json:"parent"`
	Sprint []struct {
		ID int `json:"id"`
	} `json:"customfield_10020"`
	EngTeam *struct {
		Value string `json:"value"`
	} `json:"customfield_10089"`
}

// FetchIssue retrieves an issue from Jira to verify its fields.
func (c *RealJiraClient) FetchIssue(issueKey string) (*IssueFields, error) {
	body, err := c.doRequest("GET", jiraBaseURL+"/issue/"+issueKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch issue: %w", err)
	}

	var result struct {
		Fields IssueFields `json:"fields"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &result.Fields, nil
}

// VerifyIssue checks that the issue was created with the expected values.
// Returns a list of warnings for any mismatches.
func (c *RealJiraClient) VerifyIssue(issueKey string, params TaskParams) []string {
	fields, err := c.FetchIssue(issueKey)
	if err != nil {
		return []string{fmt.Sprintf("Could not verify issue: %v", err)}
	}

	var warnings []string

	// Check assignee (only if one was requested)
	if params.Assignee != "" {
		if fields.Assignee == nil {
			warnings = append(warnings, fmt.Sprintf("Assignee was not set (expected %s)", params.Assignee))
		} else if fields.Assignee.AccountID != params.Assignee {
			warnings = append(warnings, fmt.Sprintf("Assignee mismatch: got %s, expected %s",
				fields.Assignee.AccountID, params.Assignee))
		}
	}

	// Check parent
	if params.Parent != "" {
		if fields.Parent == nil {
			warnings = append(warnings, fmt.Sprintf("Parent was not set (expected %s)", params.Parent))
		} else if fields.Parent.Key != params.Parent {
			warnings = append(warnings, fmt.Sprintf("Parent mismatch: got %s, expected %s",
				fields.Parent.Key, params.Parent))
		}
	}

	// Check sprint
	if params.Sprint != "" {
		expectedSprintID, _ := strconv.Atoi(params.Sprint)
		if len(fields.Sprint) == 0 {
			warnings = append(warnings, fmt.Sprintf("Sprint was not set (expected %s)", params.Sprint))
		} else {
			found := false
			for _, s := range fields.Sprint {
				if s.ID == expectedSprintID {
					found = true
					break
				}
			}
			if !found {
				warnings = append(warnings, fmt.Sprintf("Sprint %s not found in issue sprints", params.Sprint))
			}
		}
	}

	// Check engineering team
	if fields.EngTeam == nil {
		warnings = append(warnings, fmt.Sprintf("Engineering team was not set (expected %s)", c.Config.EngTeam))
	} else if fields.EngTeam.Value != c.Config.EngTeam {
		warnings = append(warnings, fmt.Sprintf("Engineering team mismatch: got %s, expected %s",
			fields.EngTeam.Value, c.Config.EngTeam))
	}

	return warnings
}

// validateConfig validates the Jira configuration.
func validateConfig(config JiraConfig) error {
	if config.Token == "" {
		return fmt.Errorf("token is required (set --token or $JIRA_TOKEN)\n  Get one at: https://id.atlassian.com/manage-profile/security/api-tokens")
	}
	if config.Email == "" {
		return fmt.Errorf("email is required (set --email or $JIRA_EMAIL)")
	}
	return nil
}

// validateParams validates the task parameters.
func validateParams(params TaskParams) error {
	if params.Title == "" {
		return fmt.Errorf("title is required")
	}

	if params.Description == "" {
		return fmt.Errorf("description is required")
	}

	// Note: Assignee can be empty (meaning "unassigned").
	// The --assignee flag is required at the CLI level, but the special
	// value "unassigned" results in an empty Assignee here.

	return nil
}

// run is the main workhorse function that can be easily tested.
// It takes parsed parameters and a JiraClient interface, making it mockable.
// Returns the created issue key on success.
func run(config JiraConfig, params TaskParams, client JiraClient) (string, error) {
	// Validate config
	if err := validateConfig(config); err != nil {
		return "", err
	}

	// Validate parameters
	if err := validateParams(params); err != nil {
		return "", err
	}

	// Create the task
	issueKey, err := client.CreateInternalTask(params)
	if err != nil {
		return "", fmt.Errorf("failed to create Jira task: %w", err)
	}

	return issueKey, nil
}
