package jira

import (
	"context"
	"fmt"
)

// OrganizationService handles Organizations for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/
type OrganizationService struct {
	client *Client
}

// OrganizationCreationDTO is DTO for creat organization API
type OrganizationCreationDTO struct {
	Name string `json:"name,omitempty" structs:"name,omitempty"`
}

// SelfLink Stores REST API URL to the organization.
type SelfLink struct {
	Self string `json:"self,omitempty" structs:"self,omitempty"`
}

// Organization contains Organization data
type Organization struct {
	ID    string    `json:"id,omitempty" structs:"id,omitempty"`
	Name  string    `json:"name,omitempty" structs:"name,omitempty"`
	Links *SelfLink `json:"_links,omitempty" structs:"_links,omitempty"`
}

// OrganizationUsersDTO contains organization user ids
type OrganizationUsersDTO struct {
	AccountIds []string `json:"accountIds,omitempty" structs:"accountIds,omitempty"`
}

// PagedDTO is response of a paged list
type PagedDTO struct {
	Size       int           `json:"size,omitempty" structs:"size,omitempty"`
	Start      int           `json:"start,omitempty" structs:"start,omitempty"`
	Limit      int           `limit:"size,omitempty" structs:"limit,omitempty"`
	IsLastPage bool          `json:"isLastPage,omitempty" structs:"isLastPage,omitempty"`
	Values     []interface{} `values:"isLastPage,omitempty" structs:"values,omitempty"`
	Expands    []string      `json:"_expands,omitempty" structs:"_expands,omitempty"`
}

// PropertyKey contains Property key details.
type PropertyKey struct {
	Self string `json:"self,omitempty" structs:"self,omitempty"`
	Key  string `json:"key,omitempty" structs:"key,omitempty"`
}

// PropertyKeys contains an array of PropertyKey
type PropertyKeys struct {
	Keys []PropertyKey `json:"keys,omitempty" structs:"keys,omitempty"`
}

// GetAllOrganizationsWithContext returns a list of organizations in
// the Jira Service Management instance.
// Use this method when you want to present a list
// of organizations or want to locate an organization
// by name.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-group-organization
func (s *OrganizationService) GetAllOrganizationsWithContext(ctx context.Context, start int, limit int, accountID string) (*PagedDTO, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization?start=%d&limit=%d", start, limit)
	if accountID != "" {
		apiEndPoint += fmt.Sprintf("&accountId=%s", accountID)
	}

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	v := new(PagedDTO)
	resp, err := s.client.Do(req, v)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return v, resp, nil
}

// GetAllOrganizations wraps GetAllOrganizationsWithContext using the background context.
func (s *OrganizationService) GetAllOrganizations(start int, limit int, accountID string) (*PagedDTO, *Response, error) {
	return s.GetAllOrganizationsWithContext(context.Background(), start, limit, accountID)
}

// CreateOrganizationWithContext creates an organization by
// passing the name of the organization.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-post
func (s *OrganizationService) CreateOrganizationWithContext(ctx context.Context, name string) (*Organization, *Response, error) {
	apiEndPoint := "rest/servicedeskapi/organization"

	organization := OrganizationCreationDTO{
		Name: name,
	}

	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndPoint, organization)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	o := new(Organization)
	resp, err := s.client.Do(req, &o)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return o, resp, nil
}

// CreateOrganization wraps CreateOrganizationWithContext using the background context.
func (s *OrganizationService) CreateOrganization(name string) (*Organization, *Response, error) {
	return s.CreateOrganizationWithContext(context.Background(), name)
}

// GetOrganizationWithContext returns details of an
// organization. Use this method to get organization
// details whenever your application component is
// passed an organization ID but needs to display
// other organization details.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-get
func (s *OrganizationService) GetOrganizationWithContext(ctx context.Context, organizationID int) (*Organization, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d", organizationID)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	o := new(Organization)
	resp, err := s.client.Do(req, &o)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return o, resp, nil
}

// GetOrganization wraps GetOrganizationWithContext using the background context.
func (s *OrganizationService) GetOrganization(organizationID int) (*Organization, *Response, error) {
	return s.GetOrganizationWithContext(context.Background(), organizationID)
}

// DeleteOrganizationWithContext deletes an organization. Note that
// the organization is deleted regardless
// of other associations it may have.
// For example, associations with service desks.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-delete
func (s *OrganizationService) DeleteOrganizationWithContext(ctx context.Context, organizationID int) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d", organizationID)

	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndPoint, nil)

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

// DeleteOrganization wraps DeleteOrganizationWithContext using the background context.
func (s *OrganizationService) DeleteOrganization(organizationID int) (*Response, error) {
	return s.DeleteOrganizationWithContext(context.Background(), organizationID)
}

// GetPropertiesKeysWithContext returns the keys of
// all properties for an organization. Use this resource
// when you need to find out what additional properties
// items have been added to an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-property-get
func (s *OrganizationService) GetPropertiesKeysWithContext(ctx context.Context, organizationID int) (*PropertyKeys, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/property", organizationID)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	pk := new(PropertyKeys)
	resp, err := s.client.Do(req, &pk)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return pk, resp, nil
}

// GetPropertiesKeys wraps GetPropertiesKeysWithContext using the background context.
func (s *OrganizationService) GetPropertiesKeys(organizationID int) (*PropertyKeys, *Response, error) {
	return s.GetPropertiesKeysWithContext(context.Background(), organizationID)
}

// GetPropertyWithContext returns the value of a property
// from an organization. Use this method to obtain the JSON
// content for an organization's property.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-property-propertykey-get
func (s *OrganizationService) GetPropertyWithContext(ctx context.Context, organizationID int, propertyKey string) (*EntityProperty, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/property/%s", organizationID, propertyKey)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	ep := new(EntityProperty)
	resp, err := s.client.Do(req, &ep)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return ep, resp, nil
}

// GetProperty wraps GetPropertyWithContext using the background context.
func (s *OrganizationService) GetProperty(organizationID int, propertyKey string) (*EntityProperty, *Response, error) {
	return s.GetPropertyWithContext(context.Background(), organizationID, propertyKey)
}

// SetPropertyWithContext sets the value of a
// property for an organization. Use this
// resource to store custom data against an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-property-propertykey-put
func (s *OrganizationService) SetPropertyWithContext(ctx context.Context, organizationID int, propertyKey string) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/property/%s", organizationID, propertyKey)

	req, err := s.client.NewRequestWithContext(ctx, "PUT", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

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

// SetProperty wraps SetPropertyWithContext using the background context.
func (s *OrganizationService) SetProperty(organizationID int, propertyKey string) (*Response, error) {
	return s.SetPropertyWithContext(context.Background(), organizationID, propertyKey)
}

// DeletePropertyWithContext removes a property from an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-property-propertykey-delete
func (s *OrganizationService) DeletePropertyWithContext(ctx context.Context, organizationID int, propertyKey string) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/property/%s", organizationID, propertyKey)

	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

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

// DeleteProperty wraps DeletePropertyWithContext using the background context.
func (s *OrganizationService) DeleteProperty(organizationID int, propertyKey string) (*Response, error) {
	return s.DeletePropertyWithContext(context.Background(), organizationID, propertyKey)
}

// GetUsersWithContext returns all the users
// associated with an organization. Use this
// method where you want to provide a list of
// users for an organization or determine if
// a user is associated with an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-user-get
func (s *OrganizationService) GetUsersWithContext(ctx context.Context, organizationID int, start int, limit int) (*PagedDTO, *Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/user?start=%d&limit=%d", organizationID, start, limit)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

	if err != nil {
		return nil, nil, err
	}

	users := new(PagedDTO)
	resp, err := s.client.Do(req, &users)
	if err != nil {
		jerr := NewJiraError(resp, err)
		return nil, resp, jerr
	}

	return users, resp, nil
}

// GetUsers wraps GetUsersWithContext using the background context.
func (s *OrganizationService) GetUsers(organizationID int, start int, limit int) (*PagedDTO, *Response, error) {
	return s.GetUsersWithContext(context.Background(), organizationID, start, limit)
}

// AddUsersWithContext adds users to an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-user-post
func (s *OrganizationService) AddUsersWithContext(ctx context.Context, organizationID int, users OrganizationUsersDTO) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/user", organizationID)

	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndPoint, users)

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

// AddUsers wraps AddUsersWithContext using the background context.
func (s *OrganizationService) AddUsers(organizationID int, users OrganizationUsersDTO) (*Response, error) {
	return s.AddUsersWithContext(context.Background(), organizationID, users)
}

// RemoveUsersWithContext removes users from an organization.
//
// https://developer.atlassian.com/cloud/jira/service-desk/rest/api-group-organization/#api-rest-servicedeskapi-organization-organizationid-user-delete
func (s *OrganizationService) RemoveUsersWithContext(ctx context.Context, organizationID int, users OrganizationUsersDTO) (*Response, error) {
	apiEndPoint := fmt.Sprintf("rest/servicedeskapi/organization/%d/user", organizationID)

	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndPoint, nil)
	req.Header.Set("Accept", "application/json")

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

// RemoveUsers wraps RemoveUsersWithContext using the background context.
func (s *OrganizationService) RemoveUsers(organizationID int, users OrganizationUsersDTO) (*Response, error) {
	return s.RemoveUsersWithContext(context.Background(), organizationID, users)
}
