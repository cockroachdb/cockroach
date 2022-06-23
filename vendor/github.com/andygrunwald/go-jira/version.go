package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// VersionService handles Versions for the Jira instance / API.
//
// Jira API docs: https://docs.atlassian.com/jira/REST/latest/#api/2/version
type VersionService struct {
	client *Client
}

// Version represents a single release version of a project
type Version struct {
	Self            string `json:"self,omitempty" structs:"self,omitempty"`
	ID              string `json:"id,omitempty" structs:"id,omitempty"`
	Name            string `json:"name,omitempty" structs:"name,omitempty"`
	Description     string `json:"description,omitempty" structs:"description,omitempty"`
	Archived        *bool  `json:"archived,omitempty" structs:"archived,omitempty"`
	Released        *bool  `json:"released,omitempty" structs:"released,omitempty"`
	ReleaseDate     string `json:"releaseDate,omitempty" structs:"releaseDate,omitempty"`
	UserReleaseDate string `json:"userReleaseDate,omitempty" structs:"userReleaseDate,omitempty"`
	ProjectID       int    `json:"projectId,omitempty" structs:"projectId,omitempty"` // Unlike other IDs, this is returned as a number
	StartDate       string `json:"startDate,omitempty" structs:"startDate,omitempty"`
}

// GetWithContext gets version info from Jira
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-version-id-get
func (s *VersionService) GetWithContext(ctx context.Context, versionID int) (*Version, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/version/%v", versionID)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	version := new(Version)
	resp, err := s.client.Do(req, version)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return version, resp, nil
}

// Get wraps GetWithContext using the background context.
func (s *VersionService) Get(versionID int) (*Version, *Response, error) {
	return s.GetWithContext(context.Background(), versionID)
}

// CreateWithContext creates a version in Jira.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-version-post
func (s *VersionService) CreateWithContext(ctx context.Context, version *Version) (*Version, *Response, error) {
	apiEndpoint := "/rest/api/2/version"
	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndpoint, version)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		return nil, resp, err
	}

	responseVersion := new(Version)
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e := fmt.Errorf("could not read the returned data")
		return nil, resp, NewJiraError(resp, e)
	}
	err = json.Unmarshal(data, responseVersion)
	if err != nil {
		e := fmt.Errorf("could not unmarshall the data into struct")
		return nil, resp, NewJiraError(resp, e)
	}
	return responseVersion, resp, nil
}

// Create wraps CreateWithContext using the background context.
func (s *VersionService) Create(version *Version) (*Version, *Response, error) {
	return s.CreateWithContext(context.Background(), version)
}

// UpdateWithContext updates a version from a JSON representation.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/#api-api-2-version-id-put
func (s *VersionService) UpdateWithContext(ctx context.Context, version *Version) (*Version, *Response, error) {
	apiEndpoint := fmt.Sprintf("rest/api/2/version/%v", version.ID)
	req, err := s.client.NewRequestWithContext(ctx, "PUT", apiEndpoint, version)
	if err != nil {
		return nil, nil, err
	}
	resp, err := s.client.Do(req, nil)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	// This is just to follow the rest of the API's convention of returning a version.
	// Returning the same pointer here is pointless, so we return a copy instead.
	ret := *version
	return &ret, resp, nil
}

// Update wraps UpdateWithContext using the background context.
func (s *VersionService) Update(version *Version) (*Version, *Response, error) {
	return s.UpdateWithContext(context.Background(), version)
}
