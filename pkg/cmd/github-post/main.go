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

const pkgEnv = "PKG"

// Based on the following observed API response:
//
// 422 Validation Failed [{Resource:Issue Field:body Code:custom Message:body is too long (maximum is 65536 characters)}]
//
// Subtract some length just to be safe.
const githubIssueBodyMaximumLength = 1<<16 - 1<<10

func main() {
	token, ok := os.LookupEnv(githubAPITokenEnv)
	if !ok {
		log.Fatalf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}

	client := github.NewClient(oauth2.NewClient(oauth2.NoContext, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)))

	if err := runGH(os.Stdin, client.Issues.Create); err != nil {
		log.Fatal(err)
	}
}

func runGH(
	input io.Reader,
	createIssue func(owner string, repo string, issue *github.IssueRequest) (*github.Issue, *github.Response, error),
) error {
	sha, ok := os.LookupEnv(teamcityVCSNumberEnv)
	if !ok {
		return errors.Errorf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}

	host, ok := os.LookupEnv(teamcityServerURLEnv)
	if !ok {
		log.Fatalf("teamcity server URL environment variable %s is not set", teamcityServerURLEnv)
	}

	buildID, ok := os.LookupEnv(teamcityBuildIDEnv)
	if !ok {
		log.Fatalf("teamcity build ID environment variable %s is not set", teamcityBuildIDEnv)
	}

	options := url.Values{}
	options.Add("buildId", buildID)

	u := (&url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     "viewLog.html",
		RawQuery: options.Encode(),
	}).String()

	suites, err := lib.ParseGotest(input, "")
	if err != nil {
		return errors.Wrap(err, "failed to parse `go test` output")
	}
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
			message := test.Message
			for len(message) > githubIssueBodyMaximumLength {
				if idx := strings.IndexByte(message, '\n'); idx != -1 {
					message = message[idx+1:]
				} else {
					message = message[len(message)-githubIssueBodyMaximumLength:]
				}
			}
			switch test.Status {
			case lib.Failed:
				title := fmt.Sprintf("%s: %s failed under stress", packageName, test.Name)
				body := fmt.Sprintf(`SHA: https://github.com/cockroachdb/cockroach/commits/%s

Stress build found a failed test: %s

%s`, sha, u, "```\n"+message+"\n```")

				issueRequest := &github.IssueRequest{
					Title: &title,
					Body:  &body,
					Labels: &[]string{
						"Robot",
						"test-failure",
					},
				}
				if _, _, err := createIssue("cockroachdb", "cockroach", issueRequest); err != nil {
					return errors.Wrapf(err, "failed to create GitHub issue %s", github.Stringify(issueRequest))
				}
			}
		}
	}

	return nil
}
