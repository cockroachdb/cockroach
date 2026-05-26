// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
)

const jiraBaseURL = "https://cockroachlabs.atlassian.net"

// jiraClient calls a small subset of the Jira Cloud REST v3 API: JQL search,
// issue fetch, field update, transition, and comment. It uses HTTP basic auth
// with the bot user's email and a token, both rotated through GCP Secret
// Manager.
type jiraClient struct {
	baseURL string
	email   string
	token   string
	http    *http.Client
}

// jiraHTTPTimeout caps the time a single Jira REST call (including JQL
// search, which can be slow on large result sets) may spend in flight.
// Without it a wedged Atlassian endpoint would hang the cron run until
// the GHA job timeout fires, with no failure-notify Slack post.
const jiraHTTPTimeout = 30 * time.Second

func newJiraClient(email, token string) *jiraClient {
	return &jiraClient{
		baseURL: jiraBaseURL,
		email:   email,
		token:   token,
		http:    httputil.NewClientWithTimeout(jiraHTTPTimeout).Client,
	}
}

// jiraIssue is a partial decoding of a Jira issue. Both Fields and RawFields
// are populated atomically by the custom UnmarshalJSON below: Fields holds
// typed accessors for the few entries (summary, subtasks) the bot reads on
// every ticket, and RawFields exposes the same payload as a name->RawMessage
// map so callers can pull arbitrary custom fields (customfield_10585 etc.)
// by ID without us having to model each one. The json:"-" on RawFields
// prevents json.Marshal from double-emitting the "fields" object — it does
// not exclude RawFields from unmarshaling, which UnmarshalJSON drives directly.
type jiraIssue struct {
	Key       string                     `json:"key"`
	Fields    jiraIssueFields            `json:"fields"`
	RawFields map[string]json.RawMessage `json:"-"`
}

type jiraIssueFields struct {
	Summary  string        `json:"summary"`
	Subtasks []jiraSubtask `json:"subtasks"`
}

type jiraSubtask struct {
	Key    string `json:"key"`
	Fields struct {
		Summary string `json:"summary"`
		Status  struct {
			Name string `json:"name"`
		} `json:"status"`
	} `json:"fields"`
}

// UnmarshalJSON captures both the typed fields (summary, subtasks) and the
// raw map of all fields (so callers can pull arbitrary custom fields).
func (i *jiraIssue) UnmarshalJSON(data []byte) error {
	var aux struct {
		Key    string                     `json:"key"`
		Fields map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return errors.Wrap(err, "decoding Jira issue envelope")
	}
	i.Key = aux.Key
	i.RawFields = aux.Fields
	if raw, ok := aux.Fields["summary"]; ok {
		if err := json.Unmarshal(raw, &i.Fields.Summary); err != nil {
			return errors.Wrap(err, "decoding summary")
		}
	}
	if raw, ok := aux.Fields["subtasks"]; ok {
		if err := json.Unmarshal(raw, &i.Fields.Subtasks); err != nil {
			return errors.Wrap(err, "decoding subtasks")
		}
	}
	return nil
}

// stringField returns the value of a string custom field, or "" if the field
// is absent or null.
func (i *jiraIssue) stringField(name string) (string, error) {
	raw, ok := i.RawFields[name]
	if !ok {
		return "", nil
	}
	if string(raw) == "null" {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.Wrapf(err, "field %s is not a string", name)
	}
	return s, nil
}

// statusName returns the issue's current workflow status name (e.g.
// "Baking", "Done"), reading from RawFields["status"]. Jira represents
// status as {"name":"…","id":"…"} — distinct from the {"value":"…"}
// shape optionField handles — so a dedicated accessor is needed.
// Returns "" when the field is absent or unparseable; callers use the
// result to gate idempotent state transitions and treat "" as "unknown,
// proceed with the transition" so a fresh ticket is never blocked.
func (i *jiraIssue) statusName() string {
	raw, ok := i.RawFields["status"]
	if !ok {
		return ""
	}
	var s struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s.Name
}

// optionField returns the .value of a single-select custom field (Jira
// represents these as objects with id/value/self), or "" if absent.
func (i *jiraIssue) optionField(name string) (string, error) {
	raw, ok := i.RawFields[name]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	var obj struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", errors.Wrapf(err, "field %s is not an option object", name)
	}
	return obj.Value, nil
}

// SearchJQL executes a JQL query and returns matching issues. It pages through
// the results using Jira's nextPageToken (the v3 /search/jql endpoint).
func (c *jiraClient) SearchJQL(jql string, fields []string) ([]jiraIssue, error) {
	var all []jiraIssue
	pageToken := ""
	for {
		body := map[string]interface{}{
			"jql":          jql,
			"fields":       fields,
			"fieldsByKeys": false,
		}
		if pageToken != "" {
			body["nextPageToken"] = pageToken
		}
		var resp struct {
			Issues        []jiraIssue `json:"issues"`
			NextPageToken string      `json:"nextPageToken"`
			IsLast        bool        `json:"isLast"`
		}
		if err := c.do("POST", "/rest/api/3/search/jql", body, &resp); err != nil {
			return nil, errors.Wrap(err, "jira JQL search")
		}
		all = append(all, resp.Issues...)
		if resp.IsLast || resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return all, nil
}

// GetIssue fetches a single issue by key.
func (c *jiraClient) GetIssue(key string) (*jiraIssue, error) {
	var issue jiraIssue
	path := "/rest/api/3/issue/" + url.PathEscape(key)
	if err := c.do("GET", path, nil, &issue); err != nil {
		return nil, errors.Wrapf(err, "fetching Jira issue %s", key)
	}
	return &issue, nil
}

// UpdateFields sets the given fields on an issue (PUT /issue/{key}).
func (c *jiraClient) UpdateFields(key string, fields map[string]interface{}) error {
	body := map[string]interface{}{"fields": fields}
	path := "/rest/api/3/issue/" + url.PathEscape(key)
	return c.do("PUT", path, body, nil)
}

// Transition advances the issue's workflow state using a transition ID.
func (c *jiraClient) Transition(key string, transitionID string) error {
	body := map[string]interface{}{
		"transition": map[string]string{"id": transitionID},
	}
	path := "/rest/api/3/issue/" + url.PathEscape(key) + "/transitions"
	return c.do("POST", path, body, nil)
}

// AddComment posts a comment whose body is the supplied Atlassian Document
// Format doc (built via the adf* helpers in jira_adf.go). Callers own the
// formatting; this method only handles the API envelope and POST.
func (c *jiraClient) AddComment(key string, doc map[string]interface{}) error {
	body := map[string]interface{}{"body": doc}
	path := "/rest/api/3/issue/" + url.PathEscape(key) + "/comment"
	return c.do("POST", path, body, nil)
}

// do issues an authenticated request and decodes the JSON response into out
// (when non-nil). 2xx is treated as success; any other status returns an error
// that includes the response body.
func (c *jiraClient) do(method, path string, reqBody, out interface{}) error {
	var body io.Reader
	if reqBody != nil {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(reqBody); err != nil {
			return errors.Wrap(err, "encoding request body")
		}
		body = buf
	}
	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return errors.Wrap(err, "building request")
	}
	req.SetBasicAuth(c.email, c.token)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.Wrapf(err, "%s %s", method, path)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.Newf("%s %s: %s: %s", method, path, resp.Status, sanitizeBody(respBytes))
	}
	if out == nil || len(respBytes) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBytes, out); err != nil {
		return errors.Wrapf(err, "decoding response\nbody: %s", sanitizeBody(respBytes))
	}
	return nil
}

// maxErrorBodyBytes caps the amount of upstream response we keep in error
// messages. Long HTML error pages or stack traces from a misbehaving proxy
// otherwise blow up Slack messages and GHA logs.
const maxErrorBodyBytes = 512

// sanitizeBody truncates b to maxErrorBodyBytes and removes any line that
// looks like an Authorization header — defense-in-depth in case an upstream
// or intermediary ever echoes the request headers back in an error body.
func sanitizeBody(b []byte) string {
	s := string(b)
	if len(s) > maxErrorBodyBytes {
		s = s[:maxErrorBodyBytes] + "…(truncated)"
	}
	// Drop any Authorization: / Bearer ... fragments that might appear in
	// echoed-back request dumps.
	for _, needle := range []string{"Authorization:", "authorization:", "Bearer "} {
		if i := strings.Index(s, needle); i >= 0 {
			s = s[:i] + "[redacted]"
			break
		}
	}
	return s
}
