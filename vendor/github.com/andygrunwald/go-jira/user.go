package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// UserService handles users for the Jira instance / API.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-group-Users
type UserService struct {
	client *Client
}

// User represents a Jira user.
type User struct {
	Self            string     `json:"self,omitempty" structs:"self,omitempty"`
	AccountID       string     `json:"accountId,omitempty" structs:"accountId,omitempty"`
	AccountType     string     `json:"accountType,omitempty" structs:"accountType,omitempty"`
	Name            string     `json:"name,omitempty" structs:"name,omitempty"`
	Key             string     `json:"key,omitempty" structs:"key,omitempty"`
	Password        string     `json:"-"`
	EmailAddress    string     `json:"emailAddress,omitempty" structs:"emailAddress,omitempty"`
	AvatarUrls      AvatarUrls `json:"avatarUrls,omitempty" structs:"avatarUrls,omitempty"`
	DisplayName     string     `json:"displayName,omitempty" structs:"displayName,omitempty"`
	Active          bool       `json:"active,omitempty" structs:"active,omitempty"`
	TimeZone        string     `json:"timeZone,omitempty" structs:"timeZone,omitempty"`
	Locale          string     `json:"locale,omitempty" structs:"locale,omitempty"`
	ApplicationKeys []string   `json:"applicationKeys,omitempty" structs:"applicationKeys,omitempty"`
}

// UserGroup represents the group list
type UserGroup struct {
	Self string `json:"self,omitempty" structs:"self,omitempty"`
	Name string `json:"name,omitempty" structs:"name,omitempty"`
}

type userSearchParam struct {
	name  string
	value string
}

type userSearch []userSearchParam

type userSearchF func(userSearch) userSearch

// GetWithContext gets user info from Jira using its Account Id
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-user-get
func (s *UserService) GetWithContext(ctx context.Context, accountId string) (*User, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/user?accountId=%s", accountId)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	user := new(User)
	resp, err := s.client.Do(req, user)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return user, resp, nil
}

// Get wraps GetWithContext using the background context.
func (s *UserService) Get(accountId string) (*User, *Response, error) {
	return s.GetWithContext(context.Background(), accountId)
}

// GetByAccountIDWithContext gets user info from Jira
// Searching by another parameter that is not accountId is deprecated,
// but this method is kept for backwards compatibility
// Jira API docs: https://docs.atlassian.com/jira/REST/cloud/#api/2/user-getUser
func (s *UserService) GetByAccountIDWithContext(ctx context.Context, accountID string) (*User, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/user?accountId=%s", accountID)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	user := new(User)
	resp, err := s.client.Do(req, user)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return user, resp, nil
}

// GetByAccountID wraps GetByAccountIDWithContext using the background context.
func (s *UserService) GetByAccountID(accountID string) (*User, *Response, error) {
	return s.GetByAccountIDWithContext(context.Background(), accountID)
}

// CreateWithContext creates an user in Jira.
//
// Jira API docs: https://docs.atlassian.com/jira/REST/cloud/#api/2/user-createUser
func (s *UserService) CreateWithContext(ctx context.Context, user *User) (*User, *Response, error) {
	apiEndpoint := "/rest/api/2/user"
	req, err := s.client.NewRequestWithContext(ctx, "POST", apiEndpoint, user)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		return nil, resp, err
	}

	responseUser := new(User)
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		e := fmt.Errorf("could not read the returned data")
		return nil, resp, NewJiraError(resp, e)
	}
	err = json.Unmarshal(data, responseUser)
	if err != nil {
		e := fmt.Errorf("could not unmarshall the data into struct")
		return nil, resp, NewJiraError(resp, e)
	}
	return responseUser, resp, nil
}

// Create wraps CreateWithContext using the background context.
func (s *UserService) Create(user *User) (*User, *Response, error) {
	return s.CreateWithContext(context.Background(), user)
}

// DeleteWithContext deletes an user from Jira.
// Returns http.StatusNoContent on success.
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-user-delete
func (s *UserService) DeleteWithContext(ctx context.Context, accountId string) (*Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/user?accountId=%s", accountId)
	req, err := s.client.NewRequestWithContext(ctx, "DELETE", apiEndpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		return resp, NewJiraError(resp, err)
	}
	return resp, nil
}

// Delete wraps DeleteWithContext using the background context.
func (s *UserService) Delete(accountId string) (*Response, error) {
	return s.DeleteWithContext(context.Background(), accountId)
}

// GetGroupsWithContext returns the groups which the user belongs to
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-user-groups-get
func (s *UserService) GetGroupsWithContext(ctx context.Context, accountId string) (*[]UserGroup, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/user/groups?accountId=%s", accountId)
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	userGroups := new([]UserGroup)
	resp, err := s.client.Do(req, userGroups)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return userGroups, resp, nil
}

// GetGroups wraps GetGroupsWithContext using the background context.
func (s *UserService) GetGroups(accountId string) (*[]UserGroup, *Response, error) {
	return s.GetGroupsWithContext(context.Background(), accountId)
}

// GetSelfWithContext information about the current logged-in user
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-myself-get
func (s *UserService) GetSelfWithContext(ctx context.Context) (*User, *Response, error) {
	const apiEndpoint = "rest/api/2/myself"
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	var user User
	resp, err := s.client.Do(req, &user)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return &user, resp, nil
}

// GetSelf wraps GetSelfWithContext using the background context.
func (s *UserService) GetSelf() (*User, *Response, error) {
	return s.GetSelfWithContext(context.Background())
}

// WithMaxResults sets the max results to return
func WithMaxResults(maxResults int) userSearchF {
	return func(s userSearch) userSearch {
		s = append(s, userSearchParam{name: "maxResults", value: fmt.Sprintf("%d", maxResults)})
		return s
	}
}

// WithStartAt set the start pager
func WithStartAt(startAt int) userSearchF {
	return func(s userSearch) userSearch {
		s = append(s, userSearchParam{name: "startAt", value: fmt.Sprintf("%d", startAt)})
		return s
	}
}

// WithActive sets the active users lookup
func WithActive(active bool) userSearchF {
	return func(s userSearch) userSearch {
		s = append(s, userSearchParam{name: "includeActive", value: fmt.Sprintf("%t", active)})
		return s
	}
}

// WithInactive sets the inactive users lookup
func WithInactive(inactive bool) userSearchF {
	return func(s userSearch) userSearch {
		s = append(s, userSearchParam{name: "includeInactive", value: fmt.Sprintf("%t", inactive)})
		return s
	}
}

// FindWithContext searches for user info from Jira:
// It can find users by email or display name using the query parameter
//
// Jira API docs: https://developer.atlassian.com/cloud/jira/platform/rest/v2/#api-rest-api-2-user-search-get
func (s *UserService) FindWithContext(ctx context.Context, property string, tweaks ...userSearchF) ([]User, *Response, error) {
	search := []userSearchParam{
		{
			name:  "query",
			value: property,
		},
	}
	for _, f := range tweaks {
		search = f(search)
	}

	var queryString = ""
	for _, param := range search {
		queryString += param.name + "=" + param.value + "&"
	}

	apiEndpoint := fmt.Sprintf("/rest/api/2/user/search?%s", queryString[:len(queryString)-1])
	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	users := []User{}
	resp, err := s.client.Do(req, &users)
	if err != nil {
		return nil, resp, NewJiraError(resp, err)
	}
	return users, resp, nil
}

// Find wraps FindWithContext using the background context.
func (s *UserService) Find(property string, tweaks ...userSearchF) ([]User, *Response, error) {
	return s.FindWithContext(context.Background(), property, tweaks...)
}
