package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	jiraBaseURL  = "https://cockroachlabs.atlassian.net/rest/api/3"
	jiraAgileURL = "https://cockroachlabs.atlassian.net/rest/agile/1.0"
)

// JiraConfig holds configuration for connecting to Jira.
type JiraConfig struct {
	Token   string
	Email   string
	Project string
	EngTeam string
	BoardID int
}

// RealJiraClient implements JiraClient with actual Jira API calls.
type RealJiraClient struct {
	Config JiraConfig
}

// doRequest performs an HTTP request with authentication.
// If body is non-nil, it's marshaled to JSON. Returns the response body.
func (c *RealJiraClient) doRequest(method, url string, body any) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	auth := base64.StdEncoding.EncodeToString([]byte(c.Config.Email + ":" + c.Config.Token))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Jira API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
