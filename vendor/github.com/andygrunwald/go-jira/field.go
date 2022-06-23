package jira

import "context"

// FieldService handles fields for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-Field
type FieldService struct {
	client *Client
}

// Field represents a field of a Jira issue.
type Field struct {
	ID          string      `json:"id,omitempty" structs:"id,omitempty"`
	Key         string      `json:"key,omitempty" structs:"key,omitempty"`
	Name        string      `json:"name,omitempty" structs:"name,omitempty"`
	Custom      bool        `json:"custom,omitempty" structs:"custom,omitempty"`
	Navigable   bool        `json:"navigable,omitempty" structs:"navigable,omitempty"`
	Searchable  bool        `json:"searchable,omitempty" structs:"searchable,omitempty"`
	ClauseNames []string    `json:"clauseNames,omitempty" structs:"clauseNames,omitempty"`
	Schema      FieldSchema `json:"schema,omitempty" structs:"schema,omitempty"`
}

// FieldSchema represents a schema of a Jira field.
// Documentation: https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-issue-fields/#api-rest-api-2-field-get
type FieldSchema struct {
	Type     string `json:"type,omitempty" structs:"type,omitempty"`
	Items    string `json:"items,omitempty" structs:"items,omitempty"`
	Custom   string `json:"custom,omitempty" structs:"custom,omitempty"`
	System   string `json:"system,omitempty" structs:"system,omitempty"`
	CustomID int64  `json:"customId,omitempty" structs:"customId,omitempty"`
}

// GetListWithContext gets all fields from Jira
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-field-get
func (s *FieldService) GetListWithContext(ctx context.Context) ([]Field, *Response, error) {
	apiEndpoint := "rest/api/2/field"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	fieldList := []Field{}
	resp, err := s.client.Do(req, &fieldList)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return fieldList, resp, nil
}

// GetList wraps GetListWithContext using the background context.
func (s *FieldService) GetList() ([]Field, *Response, error) {
	return s.GetListWithContext(context.Background())
}
