package jira

import "context"

// PriorityService handles priorities for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-Priority
type PriorityService struct {
	client *Client
}

// Priority represents a priority of a Jira issue.
// Typical types are "Normal", "Moderate", "Urgent", ...
type Priority struct {
	Self        string `json:"self,omitempty" structs:"self,omitempty"`
	IconURL     string `json:"iconUrl,omitempty" structs:"iconUrl,omitempty"`
	Name        string `json:"name,omitempty" structs:"name,omitempty"`
	ID          string `json:"id,omitempty" structs:"id,omitempty"`
	StatusColor string `json:"statusColor,omitempty" structs:"statusColor,omitempty"`
	Description string `json:"description,omitempty" structs:"description,omitempty"`
}

// GetListWithContext gets all priorities from Jira
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-priority-get
func (s *PriorityService) GetListWithContext(ctx context.Context) ([]Priority, *Response, error) {
	apiEndpoint := "rest/api/2/priority"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	priorityList := []Priority{}
	resp, err := s.client.Do(req, &priorityList)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return priorityList, resp, nil
}

// GetList wraps GetListWithContext using the background context.
func (s *PriorityService) GetList() ([]Priority, *Response, error) {
	return s.GetListWithContext(context.Background())
}
