package main

import (
	"errors"
	"testing"
)

// MockJiraClient is a mock implementation of JiraClient for testing.
type MockJiraClient struct {
	CreateInternalTaskFunc func(params TaskParams) (string, error)
}

func (m *MockJiraClient) CreateInternalTask(params TaskParams) (string, error) {
	if m.CreateInternalTaskFunc != nil {
		return m.CreateInternalTaskFunc(params)
	}
	return "MOCK-123", nil
}

// validConfig returns a valid JiraConfig for testing.
func validConfig() JiraConfig {
	return JiraConfig{
		Token:   "test-token",
		Email:   "test@example.com",
		Project: "CRDB",
		EngTeam: "KV",
		BoardID: 400,
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  JiraConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid config",
			config:  validConfig(),
			wantErr: false,
		},
		{
			name: "missing token",
			config: JiraConfig{
				Email:   "test@example.com",
				Project: "CRDB",
			},
			wantErr: true,
			errMsg:  "token is required (set --token or $JIRA_TOKEN)\n  Get one at: https://id.atlassian.com/manage-profile/security/api-tokens",
		},
		{
			name: "missing email",
			config: JiraConfig{
				Token:   "test-token",
				Project: "CRDB",
			},
			wantErr: true,
			errMsg:  "email is required (set --email or $JIRA_EMAIL)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("validateConfig() expected error but got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("validateConfig() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateParams(t *testing.T) {
	tests := []struct {
		name    string
		params  TaskParams
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid params with all fields",
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Parent:      "CRDB-12345",
				Sprint:      "123",
				Assignee:    "user123",
			},
			wantErr: false,
		},
		{
			name: "valid params without optional fields",
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Assignee:    "user123",
			},
			wantErr: false,
		},
		{
			name: "missing title",
			params: TaskParams{
				Description: "Test description",
				Assignee:    "user123",
			},
			wantErr: true,
			errMsg:  "title is required",
		},
		{
			name: "missing description",
			params: TaskParams{
				Title:    "Test task",
				Assignee: "user123",
			},
			wantErr: true,
			errMsg:  "description is required",
		},
		{
			name: "empty assignee is valid (unassigned)",
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Assignee:    "", // Empty means "unassigned"
			},
			wantErr: false,
		},
		{
			name: "parent with any format is accepted",
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Parent:      "anything-goes",
				Assignee:    "user123",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParams(tt.params)
			if tt.wantErr {
				if err == nil {
					t.Errorf("validateParams() expected error but got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("validateParams() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateParams() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		config     JiraConfig
		params     TaskParams
		mockClient *MockJiraClient
		wantErr    bool
		errMsg     string
	}{
		{
			name:   "successful task creation",
			config: validConfig(),
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Parent:      "CRDB-12345",
				Assignee:    "user123",
			},
			mockClient: &MockJiraClient{
				CreateInternalTaskFunc: func(params TaskParams) (string, error) {
					return "CRDB-67890", nil
				},
			},
			wantErr: false,
		},
		{
			name:   "validation error - missing title",
			config: validConfig(),
			params: TaskParams{
				Description: "Test description",
				Assignee:    "user123",
			},
			mockClient: &MockJiraClient{},
			wantErr:    true,
			errMsg:     "title is required",
		},
		{
			name:   "config error - missing token",
			config: JiraConfig{Email: "test@example.com"},
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Assignee:    "user123",
			},
			mockClient: &MockJiraClient{},
			wantErr:    true,
			errMsg:     "token is required (set --token or $JIRA_TOKEN)\n  Get one at: https://id.atlassian.com/manage-profile/security/api-tokens",
		},
		{
			name:   "API error during task creation",
			config: validConfig(),
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Assignee:    "user123",
			},
			mockClient: &MockJiraClient{
				CreateInternalTaskFunc: func(params TaskParams) (string, error) {
					return "", errors.New("API connection failed")
				},
			},
			wantErr: true,
			errMsg:  "failed to create Jira task: API connection failed",
		},
		{
			name:   "valid params with sprint",
			config: validConfig(),
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Sprint:      "456",
				Assignee:    "user123",
			},
			mockClient: &MockJiraClient{
				CreateInternalTaskFunc: func(params TaskParams) (string, error) {
					if params.Sprint != "456" {
						t.Errorf("Expected sprint 456, got %s", params.Sprint)
					}
					return "CRDB-11111", nil
				},
			},
			wantErr: false,
		},
		{
			name:   "unassigned task (empty assignee)",
			config: validConfig(),
			params: TaskParams{
				Title:       "Test task",
				Description: "Test description",
				Assignee:    "", // Empty means "unassigned"
			},
			mockClient: &MockJiraClient{
				CreateInternalTaskFunc: func(params TaskParams) (string, error) {
					if params.Assignee != "" {
						t.Errorf("Expected empty assignee for unassigned, got %s", params.Assignee)
					}
					return "CRDB-22222", nil
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := run(tt.config, tt.params, tt.mockClient)
			if tt.wantErr {
				if err == nil {
					t.Errorf("run() expected error but got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("run() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("run() unexpected error = %v", err)
				}
			}
		})
	}
}
