package jira

import (
	"context"
	"fmt"
)

// PermissionSchemeService handles permissionschemes for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/#api-group-Permissionscheme
type PermissionSchemeService struct {
	client *Client
}
type PermissionSchemes struct {
	PermissionSchemes []PermissionScheme `json:"permissionSchemes" structs:"permissionSchemes"`
}

type Permission struct {
	ID     int    `json:"id" structs:"id"`
	Self   string `json:"expand" structs:"expand"`
	Holder Holder `json:"holder" structs:"holder"`
	Name   string `json:"permission" structs:"permission"`
}

type Holder struct {
	Type      string `json:"type" structs:"type"`
	Parameter string `json:"parameter" structs:"parameter"`
	Expand    string `json:"expand" structs:"expand"`
}

// GetListWithContext returns a list of all permission schemes
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/#api-api-3-permissionscheme-get
func (s *PermissionSchemeService) GetListWithContext(ctx context.Context) (*PermissionSchemes, *Response, error) {
	apiEndpoint := "/rest/api/3/permissionscheme"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	pss := new(PermissionSchemes)
	resp, err := s.client.Do(req, &pss)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return pss, resp, nil
}

// GetList wraps GetListWithContext using the background context.
func (s *PermissionSchemeService) GetList() (*PermissionSchemes, *Response, error) {
	return s.GetListWithContext(context.Background())
}

// GetWithContext returns a full representation of the permission scheme for the schemeID
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/#api-api-3-permissionscheme-schemeId-get
func (s *PermissionSchemeService) GetWithContext(ctx context.Context, schemeID int) (*PermissionScheme, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/3/permissionscheme/%d", schemeID)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	ps := new(PermissionScheme)
	resp, err := s.client.Do(req, ps)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}
	if ps.Self == "" {
		return nil, resp, fmt.Errorf("no permissionscheme with ID %d found", schemeID)
	}

	return ps, resp, nil
}

// Get wraps GetWithContext using the background context.
func (s *PermissionSchemeService) Get(schemeID int) (*PermissionScheme, *Response, error) {
	return s.GetWithContext(context.Background(), schemeID)
}
