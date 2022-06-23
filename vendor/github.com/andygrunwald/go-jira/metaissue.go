package jira

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-querystring/query"
	"github.com/trivago/tgo/tcontainer"
)

// CreateMetaInfo contains information about fields and their attributed to create a ticket.
type CreateMetaInfo struct {
	Expand   string         `json:"expand,omitempty"`
	Projects []*MetaProject `json:"projects,omitempty"`
}

// EditMetaInfo contains information about fields and their attributed to edit a ticket.
type EditMetaInfo struct {
	Fields tcontainer.MarshalMap `json:"fields,omitempty"`
}

// MetaProject is the meta information about a project returned from createmeta api
type MetaProject struct {
	Expand string `json:"expand,omitempty"`
	Self   string `json:"self,omitempty"`
	Id     string `json:"id,omitempty"`
	Key    string `json:"key,omitempty"`
	Name   string `json:"name,omitempty"`
	// omitted avatarUrls
	IssueTypes []*MetaIssueType `json:"issuetypes,omitempty"`
}

// MetaIssueType represents the different issue types a project has.
//
// Note: Fields is interface because this is an object which can
// have arbitraty keys related to customfields. It is not possible to
// expect these for a general way. This will be returning a map.
// Further processing must be done depending on what is required.
type MetaIssueType struct {
	Self        string                `json:"self,omitempty"`
	Id          string                `json:"id,omitempty"`
	Description string                `json:"description,omitempty"`
	IconUrl     string                `json:"iconurl,omitempty"`
	Name        string                `json:"name,omitempty"`
	Subtasks    bool                  `json:"subtask,omitempty"`
	Expand      string                `json:"expand,omitempty"`
	Fields      tcontainer.MarshalMap `json:"fields,omitempty"`
}

// GetCreateMetaWithContext makes the api call to get the meta information required to create a ticket
func (s *IssueService) GetCreateMetaWithContext(ctx context.Context, projectkeys string) (*CreateMetaInfo, *Response, error) {
	return s.GetCreateMetaWithOptionsWithContext(ctx, &GetQueryOptions{ProjectKeys: projectkeys, Expand: "projects.issuetypes.fields"})
}

// GetCreateMeta wraps GetCreateMetaWithContext using the background context.
func (s *IssueService) GetCreateMeta(projectkeys string) (*CreateMetaInfo, *Response, error) {
	return s.GetCreateMetaWithContext(context.Background(), projectkeys)
}

// GetCreateMetaWithOptionsWithContext makes the api call to get the meta information without requiring to have a projectKey
func (s *IssueService) GetCreateMetaWithOptionsWithContext(ctx context.Context, options *GetQueryOptions) (*CreateMetaInfo, *Response, error) {
	apiEndpoint := "rest/api/2/issue/createmeta"

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	if options != nil {
		q, err := query.Values(options)
		if err != nil {
			return nil, nil, err
		}
		req.URL.RawQuery = q.Encode()
	}

	meta := new(CreateMetaInfo)
	resp, err := s.client.Do(req, meta)

	if err != nil {
		return nil, resp, err
	}

	return meta, resp, nil
}

// GetCreateMetaWithOptions wraps GetCreateMetaWithOptionsWithContext using the background context.
func (s *IssueService) GetCreateMetaWithOptions(options *GetQueryOptions) (*CreateMetaInfo, *Response, error) {
	return s.GetCreateMetaWithOptionsWithContext(context.Background(), options)
}

// GetEditMetaWithContext makes the api call to get the edit meta information for an issue
func (s *IssueService) GetEditMetaWithContext(ctx context.Context, issue *Issue) (*EditMetaInfo, *Response, error) {
	apiEndpoint := fmt.Sprintf("/rest/api/2/issue/%s/editmeta", issue.Key)

	req, err := s.client.NewRequestWithContext(ctx, "GET", apiEndpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	meta := new(EditMetaInfo)
	resp, err := s.client.Do(req, meta)

	if err != nil {
		return nil, resp, err
	}

	return meta, resp, nil
}

// GetEditMeta wraps GetEditMetaWithContext using the background context.
func (s *IssueService) GetEditMeta(issue *Issue) (*EditMetaInfo, *Response, error) {
	return s.GetEditMetaWithContext(context.Background(), issue)
}

// GetProjectWithName returns a project with "name" from the meta information received. If not found, this returns nil.
// The comparison of the name is case insensitive.
func (m *CreateMetaInfo) GetProjectWithName(name string) *MetaProject {
	for _, m := range m.Projects {
		if strings.EqualFold(m.Name, name) {
			return m
		}
	}
	return nil
}

// GetProjectWithKey returns a project with "name" from the meta information received. If not found, this returns nil.
// The comparison of the name is case insensitive.
func (m *CreateMetaInfo) GetProjectWithKey(key string) *MetaProject {
	for _, m := range m.Projects {
		if strings.EqualFold(m.Key, key) {
			return m
		}
	}
	return nil
}

// GetIssueTypeWithName returns an IssueType with name from a given MetaProject. If not found, this returns nil.
// The comparison of the name is case insensitive
func (p *MetaProject) GetIssueTypeWithName(name string) *MetaIssueType {
	for _, m := range p.IssueTypes {
		if strings.EqualFold(m.Name, name) {
			return m
		}
	}
	return nil
}

// GetMandatoryFields returns a map of all the required fields from the MetaIssueTypes.
// if a field returned by the api was:
// "customfield_10806": {
//					"required": true,
//					"schema": {
//						"type": "any",
//						"custom": "com.pyxis.greenhopper.jira:gh-epic-link",
//						"customId": 10806
//					},
//					"name": "Epic Link",
//					"hasDefaultValue": false,
//					"operations": [
//						"set"
//					]
//				}
// the returned map would have "Epic Link" as the key and "customfield_10806" as value.
// This choice has been made so that the it is easier to generate the create api request later.
func (t *MetaIssueType) GetMandatoryFields() (map[string]string, error) {
	ret := make(map[string]string)
	for key := range t.Fields {
		required, err := t.Fields.Bool(key + "/required")
		if err != nil {
			return nil, err
		}
		if required {
			name, err := t.Fields.String(key + "/name")
			if err != nil {
				return nil, err
			}
			ret[name] = key
		}
	}
	return ret, nil
}

// GetAllFields returns a map of all the fields for an IssueType. This includes all required and not required.
// The key of the returned map is what you see in the form and the value is how it is representated in the jira schema.
func (t *MetaIssueType) GetAllFields() (map[string]string, error) {
	ret := make(map[string]string)
	for key := range t.Fields {

		name, err := t.Fields.String(key + "/name")
		if err != nil {
			return nil, err
		}
		ret[name] = key
	}
	return ret, nil
}

// CheckCompleteAndAvailable checks if the given fields satisfies the mandatory field required to create a issue for the given type
// And also if the given fields are available.
func (t *MetaIssueType) CheckCompleteAndAvailable(config map[string]string) (bool, error) {
	mandatory, err := t.GetMandatoryFields()
	if err != nil {
		return false, err
	}
	all, err := t.GetAllFields()
	if err != nil {
		return false, err
	}

	// check templateconfig against mandatory fields
	for key := range mandatory {
		if _, okay := config[key]; !okay {
			var requiredFields []string
			for name := range mandatory {
				requiredFields = append(requiredFields, name)
			}
			return false, fmt.Errorf("required field not found in provided jira.fields. Required are: %#v", requiredFields)
		}
	}

	// check templateConfig against all fields to verify they are available
	for key := range config {
		if _, okay := all[key]; !okay {
			var availableFields []string
			for name := range all {
				availableFields = append(availableFields, name)
			}
			return false, fmt.Errorf("fields in jira.fields are not available in jira. Available are: %#v", availableFields)
		}
	}

	return true, nil
}
