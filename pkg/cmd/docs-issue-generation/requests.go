// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	err := httpRequest(graphQLURL, "POST", "GitHub", nil, body, &out)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func queryJiraRESTAPI(
	apiEndpoint, method string,
	headers map[string]string,
	body map[string]interface{},
	out interface{},
) error {
	url := crlJiraBaseUrl + jiraRESTURLPart + apiEndpoint
	err := httpRequest(url, method, "Jira", headers, body, &out)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func httpRequest(
	url, method, source string, headers map[string]string, body interface{}, out interface{},
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
	if source == "Jira" {
		req.SetBasicAuth(jiraDocsUserEmail, tokenParams.JiraToken)
	} else if source == "GitHub" {
		req.Header.Set("Authorization", githubAuthHeader)
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
	if res.StatusCode != http.StatusOK && res.StatusCode != 201 {
		err = fmt.Errorf("error: Request failed with status: %s", res.Status)
		fmt.Println(err) // deliberately not returning an error here to print out the tmp interface below
	}
	bs, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	// unmarshal (convert) the byte slice into an interface
	var tmp interface{}
	err = json.Unmarshal(bs, &tmp)
	if err != nil {
		fmt.Printf("error: Unable to unmarshal JSON into an empty interface: %s", err)
		fmt.Printf("Byte slice: %s", string(bs[:]))
		return err
	}
	err = json.Unmarshal(bs, out)
	if err != nil {
		fmt.Printf("%#v\n", tmp)
		return err
	}
	return nil
}
