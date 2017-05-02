// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"golang.org/x/oauth2"

	"github.com/google/go-github/github"
	"github.com/pkg/errors"
	"github.com/tebeka/go2xunit/lib"
)

const githubAPITokenEnv = "GITHUB_API_TOKEN"
const teamcityVCSNumberEnv = "BUILD_VCS_NUMBER"
const teamcityBuildIDEnv = "TC_BUILD_ID"
const teamcityServerURLEnv = "TC_SERVER_URL"
const githubUser = "cockroachdb"
const githubRepo = "cockroach"

const pkgEnv = "PKG"
const tagsEnv = "TAGS"
const goFlagsEnv = "GOFLAGS"
const cockroachPkgPrefix = "github.com/cockroachdb/cockroach/pkg/"

var issueLabels = []string{"Robot", "test-failure"}

// Based on the following observed API response:
//
// 422 Validation Failed [{Resource:Issue Field:body Code:custom Message:body is too long (maximum is 65536 characters)}]
const githubIssueBodyMaximumLength = 1<<16 - 1

func main() {
	token, ok := os.LookupEnv(githubAPITokenEnv)
	if !ok {
		log.Fatalf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}

	ctx := context.Background()

	client := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)))

	if err := runGH(ctx, os.Stdin, client.Issues.Create, client.Search.Issues, client.Issues.CreateComment); err != nil {
		log.Fatal(err)
	}
}

func trimIssueRequestBody(message string, usedCharacters int) string {
	maxLength := githubIssueBodyMaximumLength - usedCharacters

	for len(message) > maxLength {
		if idx := strings.IndexByte(message, '\n'); idx != -1 {
			message = message[idx+1:]
		} else {
			message = message[len(message)-maxLength:]
		}
	}
	return message
}

func runGH(
	ctx context.Context,
	input io.Reader,
	createIssue func(ctx context.Context, owner string, repo string, issue *github.IssueRequest) (*github.Issue, *github.Response, error),
	searchIssues func(ctx context.Context, query string, opt *github.SearchOptions) (*github.IssuesSearchResult, *github.Response, error),
	createComment func(ctx context.Context, owner string, repo string, number int, comment *github.IssueComment) (*github.IssueComment, *github.Response, error),
) error {
	sha, ok := os.LookupEnv(teamcityVCSNumberEnv)
	if !ok {
		return errors.Errorf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}

	buildID, ok := os.LookupEnv(teamcityBuildIDEnv)
	if !ok {
		log.Fatalf("teamcity build ID environment variable %s is not set", teamcityBuildIDEnv)
	}

	serverURL, ok := os.LookupEnv(teamcityServerURLEnv)
	if !ok {
		log.Fatalf("teamcity server URL environment variable %s is not set", teamcityServerURLEnv)
	}

	options := url.Values{}
	options.Add("buildId", buildID)
	options.Add("tab", "buildLog")

	u, err := url.Parse(serverURL)
	if err != nil {
		log.Fatal(err)
	}
	u.Scheme = "https"
	u.Path = "viewLog.html"
	u.RawQuery = options.Encode()

	var inputBuf bytes.Buffer
	input = io.TeeReader(input, &inputBuf)

	var parameters []string
	for _, parameter := range []string{
		tagsEnv,
		goFlagsEnv,
	} {
		if val, ok := os.LookupEnv(parameter); ok {
			parameters = append(parameters, parameter+"="+val)
		}
	}
	parametersStr := "```\n" + strings.Join(parameters, "\n") + "\n```"
	const bodyTemplate = `SHA: https://github.com/cockroachdb/cockroach/commits/%s

Parameters:
%s

Stress build found a failed test: %s`

	newIssueRequest := func(packageName, testName, message string) *github.IssueRequest {
		title := fmt.Sprintf("%s: %s failed under stress",
			strings.TrimPrefix(packageName, cockroachPkgPrefix), testName)
		body := fmt.Sprintf(bodyTemplate, sha, parametersStr, u.String()) + "\n\n```\n%s\n```"
		// We insert a raw "%s" above so we can figure out the length of the
		// body so far, without the actual error text. We need this length so we
		// can calculate the maximum amount of error text we can include in the
		// issue without exceeding GitHub's limit. We replace that %s in the
		// following Sprintf.
		body = fmt.Sprintf(body, trimIssueRequestBody(message, len(body)))

		return &github.IssueRequest{
			Title:  &title,
			Body:   &body,
			Labels: &issueLabels,
		}
	}

	newIssueComment := func(packageName, testname string) *github.IssueComment {
		body := fmt.Sprintf(bodyTemplate, sha, parametersStr, u.String())
		return &github.IssueComment{Body: &body}
	}

	suites, err := lib.ParseGotest(input, "")
	if err != nil {
		return errors.Wrap(err, "failed to parse `go test` output")
	}
	posted := false
	for _, suite := range suites {
		packageName := suite.Name
		if packageName == "" {
			var ok bool
			packageName, ok = os.LookupEnv(pkgEnv)
			if !ok {
				log.Fatalf("package name environment variable %s is not set", pkgEnv)
			}
		}
		for _, test := range suite.Tests {
			switch test.Status {
			case lib.Failed:
				issueRequest := newIssueRequest(packageName, test.Name, test.Message)
				searchQuery := fmt.Sprintf(`"%s" user:%s repo:%s is:open`, *issueRequest.Title, githubUser, githubRepo)
				for _, label := range issueLabels {
					searchQuery = searchQuery + fmt.Sprintf(` label:"%s"`, label)
				}

				var foundIssue *int

				result, _, err := searchIssues(ctx, searchQuery, &github.SearchOptions{
					ListOptions: github.ListOptions{
						PerPage: 1,
					},
				})
				if err != nil {
					return errors.Wrapf(err, "failed to search GitHub with query %s", github.Stringify(searchQuery))
				}
				if *result.Total > 0 {
					foundIssue = result.Issues[0].Number
				}

				if foundIssue == nil {
					if _, _, err := createIssue(ctx, githubUser, githubRepo, issueRequest); err != nil {
						return errors.Wrapf(err, "failed to create GitHub issue %s", github.Stringify(issueRequest))
					}
				} else {
					comment := newIssueComment(packageName, test.Name)
					if _, _, err := createComment(ctx, githubUser, githubRepo, *foundIssue, comment); err != nil {
						return errors.Wrapf(err, "failed to update issue #%d with %s", *foundIssue, github.Stringify(comment))
					}
				}
				posted = true
			}
		}
	}

	const unknown = "(unknown)"

	if !posted {
		packageName, ok := os.LookupEnv(pkgEnv)
		if !ok {
			packageName = unknown
		}
		issueRequest := newIssueRequest(packageName, unknown, inputBuf.String())
		if _, _, err := createIssue(ctx, githubUser, githubRepo, issueRequest); err != nil {
			return errors.Wrapf(err, "failed to create GitHub issue %s", github.Stringify(issueRequest))
		}
	}

	return nil
}
