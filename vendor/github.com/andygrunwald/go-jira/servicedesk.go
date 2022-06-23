package jira

import (
	"context"
	"fmt"
)

// ServiceDeskService handles ServiceDesk for the Jira instance / API.
type ServiceDeskService struct {
	client *Client
}

// ServiceDeskOrganizationDTO is a DTO for ServiceDesk organizations
type ServiceDeskOrganizationDTO struct {
	OrganizationID int `json:"organizationId,omitempty" structs:"organizationId,omitempty"`
}

// GetOrganizationsWithContext returns a list of
// all organizations associated with a service desk.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-servicedesk-servicedeskid-organization-get
func (s *ServiceDeskService) GetOrganizationsWithContext(ctx context.Context, serviceDeskID int, start int, limit int, accountID string) (*PagedDTO, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/servicedesk/%d/organization?start=%d&limit=%d", serviceDeskID, start, limit)
	if accountID != "" {
		apiEndPoint += fmt.Sprintf("&accountId=%s", accountID)
	}

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	orgs := new(PagedDTO)
	resp, err := s.client.Do(req, &orgs)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return orgs, resp, nil
}

// GetOrganizations wraps GetOrganizationsWithContext using the background context.
func (s *ServiceDeskService) GetOrganizations(serviceDeskID int, start int, limit int, accountID string) (*PagedDTO, *Response, error) {
	return s.GetOrganizationsWithContext(context.Background(), serviceDeskID, start, limit, accountID)
}

// AddOrganizationWithContext adds an organization to
// a service desk. If the organization ID is already
// associated with the service desk, no change is made
// and the resource returns a 204 success code.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-servicedesk-servicedeskid-organization-post
func (s *ServiceDeskService) AddOrganizationWithContext(ctx context.Context, serviceDeskID int, organizationID int) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/servicedesk/%d/organization", serviceDeskID)

	organization := ServiceDeskOrganizationDTO{
		OrganizationID: organizationID,
	}

	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndPoint, organization)

	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return resp, jerr
	}

	return resp, nil
}

// AddOrganization wraps AddOrganizationWithContext using the background context.
func (s *ServiceDeskService) AddOrganization(serviceDeskID int, organizationID int) (*Response, error) {
	return s.AddOrganizationWithContext(context.Background(), serviceDeskID, organizationID)
}

// RemoveOrganizationWithContext removes an organization
// from a service desk. If the organization ID does not
// match an organization associated with the service desk,
// no change is made and the resource returns a 204 success code.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-servicedesk-servicedeskid-organization-delete
func (s *ServiceDeskService) RemoveOrganizationWithContext(ctx context.Context, serviceDeskID int, organizationID int) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/servicedesk/%d/organization", serviceDeskID)

	organization := ServiceDeskOrganizationDTO{
		OrganizationID: organizationID,
	}

	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndPoint, organization)

	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return resp, jerr
	}

	return resp, nil
}

// RemoveOrganization wraps RemoveOrganizationWithContext using the background context.
func (s *ServiceDeskService) RemoveOrganization(serviceDeskID int, organizationID int) (*Response, error) {
	return s.RemoveOrganizationWithContext(context.Background(), serviceDeskID, organizationID)
}
