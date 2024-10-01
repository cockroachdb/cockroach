// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	graphQLURL        = "https://api.github.com/graphql"
	crlJiraBaseUrl    = "https://cockroachlabs.atlassian.net/"
	jiraRESTURLPart   = "rest/api/3/"
	jiraDocsUserEmail = "cockroach-jira-docs@cockroachlabs.com"
)

const (
	httpSourceGitHub httpReqSource = iota
	httpSourceJira
)

var (
	tokenParams      = tokenParameters()
	githubAuthHeader = fmt.Sprintf("Bearer %s", tokenParams.GitHubToken)
)

func tokenParameters() apiTokenParameters {
	const (
		githubApiTokenEnv = "GITHUB_API_TOKEN"
		jiraApiTokenEnv   = "JIRA_API_TOKEN"
	)
	return apiTokenParameters{
		GitHubToken: maybeEnv(githubApiTokenEnv, ""),
		JiraToken:   maybeEnv(jiraApiTokenEnv, ""),
	}
}

// queryGraphQL is the function that interfaces directly with the GitHub GraphQL API. Given a query, variables, and
// token, it will return a struct containing the requested data or an error if one exists.
func queryGraphQL(query string, queryVariables map[string]interface{}, out interface{}) error {
	body := map[string]interface{}{
		"query": query,
	}
	if queryVariables != nil {
		body["variables"] = queryVariables
	}
	err := httpRequest(graphQLURL, "POST", httpSourceGitHub, nil, body, &out)
	if err != nil {
		return err
	}
	return nil
}

func queryJiraRESTAPI(
	apiEndpoint, method string, headers map[string]string, body interface{}, out interface{},
) error {
	url := crlJiraBaseUrl + jiraRESTURLPart + apiEndpoint
	err := httpRequest(url, method, httpSourceJira, headers, body, &out)
	if err != nil {
		return err
	}
	return nil
}

func httpRequest(
	url, method string,
	source httpReqSource,
	headers map[string]string,
	body interface{},
	out interface{},
) error {
	var requestBody bytes.Buffer
	encoder := json.NewEncoder(&requestBody)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(method, url, &requestBody)
	if err != nil {
		return err
	}
	if source == httpSourceJira {
		req.SetBasicAuth(jiraDocsUserEmail, tokenParams.JiraToken)
	} else if source == httpSourceGitHub {
		req.Header.Set("Authorization", githubAuthHeader)
	} else {
		return fmt.Errorf("error: Unexpected httpReqSource %d", source)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	var respErr error
	if res.StatusCode != http.StatusOK && res.StatusCode != 201 {
		respErr = fmt.Errorf("request failed with status: %s", res.Status)
	}
	bs, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	// unmarshal (convert) the byte slice into an interface
	var tmp interface{}
	err = json.Unmarshal(bs, &tmp)
	if err != nil {
		return fmt.Errorf("%w\nByte slice: %s", err, string(bs[:]))
	}
	if respErr != nil {
		return fmt.Errorf("%w\n%+v", respErr, tmp)
	}
	err = json.Unmarshal(bs, out)
	if err != nil {
		return fmt.Errorf("could not umarshall interface:\n%w\nActual response from server: %+v", err, tmp)
	}
	return nil
}
