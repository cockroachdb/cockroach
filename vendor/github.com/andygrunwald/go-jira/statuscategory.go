package jira

import "context"

// StatusCategoryService handles status categories for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-Statuscategory
type StatusCategoryService struct {
	client *Client
}

// StatusCategory represents the category a status belongs to.
// Those categories can be user defined in every Jira instance.
type StatusCategory struct {
	Self      string `json:"self" structs:"self"`
	ID        int    `json:"id" structs:"id"`
	Name      string `json:"name" structs:"name"`
	Key       string `json:"key" structs:"key"`
	ColorName string `json:"colorName" structs:"colorName"`
}

// These constants are the keys of the default Jira status categories
const (
	StatusCategoryComplete   = "done"
	StatusCategoryInProgress = "indeterminate"
	StatusCategoryToDo       = "new"
	StatusCategoryUndefined  = "undefined"
)

// GetListWithContext gets all status categories from Jira
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-statuscategory-get
func (s *StatusCategoryService) GetListWithContext(ctx context.Context) ([]StatusCategory, *Response, error) {
	apiEndpoint := "rest/api/2/statuscategory"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	statusCategoryList := []StatusCategory{}
	resp, err := s.client.Do(req, &statusCategoryList)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return statusCategoryList, resp, nil
}

// GetList wraps GetListWithContext using the background context.
func (s *StatusCategoryService) GetList() ([]StatusCategory, *Response, error) {
	return s.GetListWithContext(context.Background())
}
