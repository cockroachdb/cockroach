package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// IssueLinkTypeService handles issue link types for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-group-Issue-link-types
type IssueLinkTypeService struct {
	client *Client
}

// GetListWithContext gets all of the issue link types from Jira.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-issueLinkType-get
func (s *IssueLinkTypeService) GetListWithContext(ctx context.Context) ([]IssueLinkType, *Response, error) {
	apiEndpoint := "rest/api/2/issueLinkType"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	linkTypeList := []IssueLinkType{}
	resp, err := s.client.Do(req, &linkTypeList)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return linkTypeList, resp, nil
}

// GetList wraps GetListWithContext using the background context.
func (s *IssueLinkTypeService) GetList() ([]IssueLinkType, *Response, error) {
	return s.GetListWithContext(context.Background())
}

// GetWithContext gets info of a specific issue link type from Jira.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-issueLinkType-issueLinkTypeId-get
func (s *IssueLinkTypeService) GetWithContext(ctx context.Context, ID string) (*IssueLinkType, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/api/2/issueLinkType/%s", ID)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	if err != nil {
		return nil, nil, err
	}

	linkType := new(IssueLinkType)
	resp, err := s.client.Do(req, linkType)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return linkType, resp, nil
}

// Get wraps GetWithContext using the background context.
func (s *IssueLinkTypeService) Get(ID string) (*IssueLinkType, *Response, error) {
	return s.GetWithContext(context.Background(), ID)
}

// CreateWithContext creates an issue link type in Jira.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-issueLinkType-post
func (s *IssueLinkTypeService) CreateWithContext(ctx context.Context, linkType *IssueLinkType) (*IssueLinkType, *Response, error) {
	apiEndpoint := "/rest/api/2/issueLinkType"
	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndpoint, linkType)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		return nil, resp, err
	}

	responseLinkType := new(IssueLinkType)
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e := fmt.Errorf("could not read the returned data")
		return nil, resp, NewJiraError(resp, e)
	}
	err = json.Unmarshal(data, responseLinkType)
	if err != nil {
		e := fmt.Errorf("could no unmarshal the data into struct")
		return nil, resp, NewJiraError(resp, e)
	}
	return linkType, resp, nil
}

// Create wraps CreateWithContext using the background context.
func (s *IssueLinkTypeService) Create(linkType *IssueLinkType) (*IssueLinkType, *Response, error) {
	return s.CreateWithContext(context.Background(), linkType)
}

// UpdateWithContext updates an issue link type.  The issue is found by key.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-issueLinkType-issueLinkTypeId-put
func (s *IssueLinkTypeService) UpdateWithContext(ctx context.Context, linkType *IssueLinkType) (*IssueLinkType, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/api/2/issueLinkType/%s", linkType.ID)
	req, err := s.client.NewRequestWithContext(ctx, "PUT", apiEndpoint, linkType)
	if err != nil {
		return nil, nil, err
	}
	resp, err := s.client.Do(req, nil)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	ret := *linkType
	return &ret, resp, nil
}

// Update wraps UpdateWithContext using the background context.
func (s *IssueLinkTypeService) Update(linkType *IssueLinkType) (*IssueLinkType, *Response, error) {
	return s.UpdateWithContext(context.Background(), linkType)
}

// DeleteWithContext deletes an issue link type based on provided ID.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-issueLinkType-issueLinkTypeId-delete
func (s *IssueLinkTypeService) DeleteWithContext(ctx context.Context, ID string) (*Response, error) {
	apiEndpoint := fmt.Sprintf("rest/api/2/issueLinkType/%s", ID)
	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req, nil)
	return resp, err
}

// Delete wraps DeleteWithContext using the background context.
func (s *IssueLinkTypeService) Delete(ID string) (*Response, error) {
	return s.DeleteWithContext(context.Background(), ID)
}
