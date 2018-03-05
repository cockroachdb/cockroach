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

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"golang.org/x/oauth2"

	"github.com/google/go-github/github"
	version "github.com/hashicorp/go-version"
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

	getLatestTag := func() (string, error) {
		cmd := exec.Command("git", "describe", "--abbrev=0", "--tags")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	}

	if err := runGH(
		ctx,
		os.Stdin,
		client.Issues.Create,
		client.Search.Issues,
		client.Issues.CreateComment,
		client.Repositories.ListCommits,
		client.Issues.ListMilestones,
		getLatestTag,
	); err != nil {
		log.Fatal(err)
	}
}

var stacktraceRE = regexp.MustCompile(`(?m:^goroutine\s\d+)`)

func trimIssueRequestBody(message string, usedCharacters int) string {
	maxLength := githubIssueBodyMaximumLength - usedCharacters

	if m := stacktraceRE.FindStringIndex(message); m != nil {
		// We want the top stack traces plus a few lines before.
		{
			startIdx := m[0]
			for i := 0; i < 100; i++ {
				if idx := strings.LastIndexByte(message[:startIdx], '\n'); idx != -1 {
					startIdx = idx
				}
			}
			message = message[startIdx:]
		}
		for len(message) > maxLength {
			if idx := strings.LastIndexByte(message, '\n'); idx != -1 {
				message = message[:idx]
			} else {
				message = message[:maxLength]
			}
		}
	} else {
		// We want the FAIL line.
		for len(message) > maxLength {
			if idx := strings.IndexByte(message, '\n'); idx != -1 {
				message = message[idx+1:]
			} else {
				message = message[len(message)-maxLength:]
			}
		}
	}

	return message
}

// If the assignee would be the key in this map, assign to the value instead.
// Helpful to avoid pinging former employees.
var oldFriendsMap = map[string]string{
	"tamird": "tschottdorf",
}

func getAssignee(
	ctx context.Context,
	packageName, testName string,
	listCommits func(ctx context.Context, owner string, repo string, opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error),
) (string, error) {
	// Search the source code for the email address of the last committer to touch
	// the first line of the source code that contains testName. Then, ask GitHub
	// for the GitHub username of the user with that email address by searching
	// commits in cockroachdb/cockroach for commits authored by the address.
	subtests := strings.Split(testName, "/")
	testName = subtests[0]
	packageName = strings.TrimPrefix(packageName, "github.com/cockroachdb/cockroach/")
	cmd := exec.Command(`/bin/bash`, `-c`, fmt.Sprintf(`git grep -n "func %s" $(git rev-parse --show-toplevel)/%s/*_test.go`, testName, packageName))
	// This command returns output such as:
	// ../ccl/storageccl/export_test.go:31:func TestExportCmd(t *testing.T) {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.Errorf("couldn't find test %s in %s: %s %s", testName, packageName, err, string(out))
	}
	re := regexp.MustCompile(`(.*):(.*):`)
	// The first 2 :-delimited fields are the filename and line number.
	matches := re.FindSubmatch(out)
	if matches == nil {
		return "", errors.Errorf("couldn't find filename/line number for test %s in %s: %s", testName, packageName, string(out))
	}
	filename := matches[1]
	linenum := matches[2]

	// Now run git blame.
	cmd = exec.Command(`/bin/bash`, `-c`, fmt.Sprintf(`git blame --porcelain -L%s,+1 %s | grep author-mail`, linenum, filename))
	// This command returns output such as:
	// author-mail <jordan@cockroachlabs.com>
	out, err = cmd.CombinedOutput()
	if err != nil {
		return "", errors.Errorf("couldn't find author of test %s in %s: %s %s", testName, packageName, err, string(out))
	}
	re = regexp.MustCompile("author-mail <(.*)>")
	matches = re.FindSubmatch(out)
	if matches == nil {
		return "", errors.Errorf("couldn't find author email of test %s in %s: %s", testName, packageName, string(out))
	}
	email := string(matches[1])

	commits, _, err := listCommits(ctx, githubUser, githubRepo, &github.CommitsListOptions{
		Author: email,
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if err != nil {
		return "", err
	}
	if len(commits) == 0 {
		return "", errors.Errorf("couldn't find GitHub commits for user email %s", email)
	}

	assignee := *commits[0].Author.Login

	if newAssignee, ok := oldFriendsMap[assignee]; ok {
		assignee = newAssignee
	}
	return assignee, nil
}

func getProbableMilestone(
	ctx context.Context,
	getLatestTag func() (string, error),
	listMilestones func(ctx context.Context, owner string, repo string, opt *github.MilestoneListOptions) ([]*github.Milestone, *github.Response, error),
) *int {
	tag, err := getLatestTag()
	if err != nil {
		log.Printf("unable to get latest tag: %s", err)
		log.Printf("issues will be posted without milestone")
		return nil
	}

	v, err := version.NewVersion(tag)
	if err != nil {
		log.Printf("unable to parse version from tag: %s", err)
		log.Printf("issues will be posted without milestone")
		return nil
	}
	if len(v.Segments()) < 2 {
		log.Printf("version %s has less than two components; issues will be posted without milestone", tag)
		return nil
	}
	vstring := fmt.Sprintf("%d.%d", v.Segments()[0], v.Segments()[1])

	milestones, _, err := listMilestones(ctx, githubUser, githubRepo, &github.MilestoneListOptions{
		State: "open",
	})
	if err != nil {
		log.Printf("unable to list milestones: %s", err)
		log.Printf("issues will be posted without milestone")
		return nil
	}

	for _, m := range milestones {
		if m.GetTitle() == vstring {
			return m.Number
		}
	}
	return nil
}

func runGH(
	ctx context.Context,
	input io.Reader,
	createIssue func(ctx context.Context, owner string, repo string, issue *github.IssueRequest) (*github.Issue, *github.Response, error),
	searchIssues func(ctx context.Context, query string, opt *github.SearchOptions) (*github.IssuesSearchResult, *github.Response, error),
	createComment func(ctx context.Context, owner string, repo string, number int, comment *github.IssueComment) (*github.IssueComment, *github.Response, error),
	listCommits func(ctx context.Context, owner string, repo string, opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error),
	listMilestones func(ctx context.Context, owner string, repo string, opt *github.MilestoneListOptions) ([]*github.Milestone, *github.Response, error),
	getLatestTag func() (string, error),
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

	milestone := getProbableMilestone(ctx, getLatestTag, listMilestones)

	newIssueRequest := func(packageName, testName, message, assignee string) *github.IssueRequest {
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
			Title:     &title,
			Body:      &body,
			Labels:    &issueLabels,
			Assignee:  &assignee,
			Milestone: milestone,
		}
	}

	newIssueComment := func(packageName, testName string) *github.IssueComment {
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
				assignee, err := getAssignee(ctx, packageName, test.Name, listCommits)
				if err != nil {
					// if we *can't* assign anyone, sigh, feel free to hard-code me.
					// -- tschottdorf, 11/3/2017
					assignee = "tschottdorf"
					test.Message += fmt.Sprintf("\n\nFailed to find issue assignee: \n%s", err)
				}
				issueRequest := newIssueRequest(packageName, test.Name, test.Message, assignee)
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
		issueRequest := newIssueRequest(packageName, unknown, inputBuf.String(), "")
		if _, _, err := createIssue(ctx, githubUser, githubRepo, issueRequest); err != nil {
			return errors.Wrapf(err, "failed to create GitHub issue %s", github.Stringify(issueRequest))
		}
	}

	return nil
}
