// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/google/go-github/github"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

const (
	githubAPITokenEnv    = "GITHUB_API_TOKEN"
	teamcityVCSNumberEnv = "BUILD_VCS_NUMBER"
	teamcityBuildIDEnv   = "TC_BUILD_ID"
	teamcityServerURLEnv = "TC_SERVER_URL"
	tagsEnv              = "TAGS"
	goFlagsEnv           = "GOFLAGS"
	githubUser           = "cockroachdb"
	githubRepo           = "cockroach"
	// CockroachPkgPrefix is the crdb package prefix.
	CockroachPkgPrefix = "github.com/cockroachdb/cockroach/pkg/"
)

var (
	issueLabels  = []string{"O-robot", "C-test-failure"}
	stacktraceRE = regexp.MustCompile(`(?m:^goroutine\s\d+)`)
)

// Based on the following observed API response the maximum here is 1<<16-1
// (but we stay way below that as nobody likes to scroll for pages and pages).
//
// 422 Validation Failed [{Resource:Issue Field:body Code:custom Message:body
// is too long (maximum is 65536 characters)}]
const githubIssueBodyMaximumLength = 5000

// trimIssueRequestBody trims message such that the total size of an issue body
// is less than githubIssueBodyMaximumLength. usedCharacters specifies the
// number of characters that have already been used for the issue body (see
// newIssueRequest below). message is usually the test failure message and
// possibly includes stacktraces for all of the goroutines (which is what makes
// the message very large).
//
// TODO(peter): Rather than trimming the message like this, perhaps it can be
// added as an attachment or some other expandable comment.
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
// An "" value means that issues that would have gone to the key are left
// unassigned.
var oldFriendsMap = map[string]string{
	"a-robinson":   "andreimatei",
	"benesch":      "nvanbenschoten",
	"georgeutsin":  "yuzefovich",
	"tamird":       "tbg",
	"vivekmenezes": "",
}

func getAssignee(
	ctx context.Context,
	authorEmail string,
	listCommits func(ctx context.Context, owner string, repo string,
		opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error),
) (string, error) {
	if authorEmail == "" {
		return "", nil
	}
	commits, _, err := listCommits(ctx, githubUser, githubRepo, &github.CommitsListOptions{
		Author: authorEmail,
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if err != nil {
		return "", err
	}
	if len(commits) == 0 {
		return "", errors.Errorf("couldn't find GitHub commits for user email %s", authorEmail)
	}

	if commits[0].Author == nil {
		return "", nil
	}
	assignee := *commits[0].Author.Login

	if newAssignee, ok := oldFriendsMap[assignee]; ok {
		if newAssignee == "" {
			return "", fmt.Errorf("old friend %s is friendless", assignee)
		}
		assignee = newAssignee
	}
	return assignee, nil
}

func getLatestTag() (string, error) {
	cmd := exec.Command("git", "describe", "--abbrev=0", "--tags", "--match=v[0-9]*")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func getProbableMilestone(
	ctx context.Context,
	getLatestTag func() (string, error),
	listMilestones func(ctx context.Context, owner string, repo string,
		opt *github.MilestoneListOptions) ([]*github.Milestone, *github.Response, error),
) *int {
	tag, err := getLatestTag()
	if err != nil {
		log.Printf("unable to get latest tag: %s", err)
		log.Printf("issues will be posted without milestone")
		return nil
	}

	v, err := version.Parse(tag)
	if err != nil {
		log.Printf("unable to parse version from tag: %s", err)
		log.Printf("issues will be posted without milestone")
		return nil
	}
	vstring := fmt.Sprintf("%d.%d", v.Major(), v.Minor())

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

type poster struct {
	sha       string
	buildID   string
	serverURL string
	milestone *int

	createIssue func(ctx context.Context, owner string, repo string,
		issue *github.IssueRequest) (*github.Issue, *github.Response, error)
	searchIssues func(ctx context.Context, query string,
		opt *github.SearchOptions) (*github.IssuesSearchResult, *github.Response, error)
	createComment func(ctx context.Context, owner string, repo string, number int,
		comment *github.IssueComment) (*github.IssueComment, *github.Response, error)
	listCommits func(ctx context.Context, owner string, repo string,
		opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error)
	listMilestones func(ctx context.Context, owner string, repo string,
		opt *github.MilestoneListOptions) ([]*github.Milestone, *github.Response, error)
	getLatestTag func() (string, error)
}

func newPoster() *poster {
	token, ok := os.LookupEnv(githubAPITokenEnv)
	if !ok {
		log.Fatalf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}

	ctx := context.Background()
	client := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)))

	return &poster{
		createIssue:    client.Issues.Create,
		searchIssues:   client.Search.Issues,
		createComment:  client.Issues.CreateComment,
		listCommits:    client.Repositories.ListCommits,
		listMilestones: client.Issues.ListMilestones,
		getLatestTag:   getLatestTag,
	}
}

func (p *poster) init() {
	var ok bool
	if p.sha, ok = os.LookupEnv(teamcityVCSNumberEnv); !ok {
		log.Fatalf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}
	if p.buildID, ok = os.LookupEnv(teamcityBuildIDEnv); !ok {
		log.Fatalf("teamcity build ID environment variable %s is not set", teamcityBuildIDEnv)
	}
	if p.serverURL, ok = os.LookupEnv(teamcityServerURLEnv); !ok {
		log.Fatalf("teamcity server URL environment variable %s is not set", teamcityServerURLEnv)
	}
	p.milestone = getProbableMilestone(context.Background(), p.getLatestTag, p.listMilestones)
}

// DefaultStressFailureTitle provides the default title for stress failure
// issues.
func DefaultStressFailureTitle(packageName, testName string) string {
	trimmedPkgName := strings.TrimPrefix(packageName, CockroachPkgPrefix)
	return fmt.Sprintf("%s: %s failed under stress", trimmedPkgName, testName)
}

func (p *poster) post(ctx context.Context, req PostRequest) error {
	const bodyTemplate = `SHA: https://github.com/cockroachdb/cockroach/commits/%[1]s

Parameters:%[2]s

To repro, try:

` + "```" + `
# Don't forget to check out a clean suitable branch and experiment with the
# stress invocation until the desired results present themselves. For example,
# using stress instead of stressrace and passing the '-p' stressflag which
# controls concurrency.
./scripts/gceworker.sh start && ./scripts/gceworker.sh mosh
cd ~/go/src/github.com/cockroachdb/cockroach && \
stdbuf -oL -eL \
make stressrace TESTS=%[5]s PKG=%[4]s TESTTIMEOUT=5m STRESSFLAGS='-maxtime 20m -timeout 10m' 2>&1 | tee /tmp/stress.log
` + "```" + `

Failed test: %[3]s`
	const messageTemplate = "\n\n```\n%s\n```"

	body := func(packageName, testName, message string) string {
		// If the test has artifacts, link straight to them.
		// Otherwise, link to the build log in TeamCity.
		var testURL *url.URL
		if req.Artifacts != "" {
			testURL = p.teamcityArtifactsURL(req.Artifacts)
		} else {
			testURL = p.teamcityBuildLogURL()
		}
		body := fmt.Sprintf(bodyTemplate, p.sha, p.parameters(), testURL, packageName, testName) + messageTemplate
		// We insert a raw "%s" above so we can figure out the length of the
		// body so far, without the actual error text. We need this length so we
		// can calculate the maximum amount of error text we can include in the
		// issue without exceeding GitHub's limit. We replace that %s in the
		// following Sprintf.
		return fmt.Sprintf(body, trimIssueRequestBody(message, len(body)))
	}

	newIssueRequest := func(packageName, testName, message, assignee string) *github.IssueRequest {
		b := body(packageName, testName, message)

		labels := append(issueLabels, req.ExtraLabels...)
		return &github.IssueRequest{
			Title:     &req.Title,
			Body:      &b,
			Labels:    &labels,
			Assignee:  &assignee,
			Milestone: p.milestone,
		}
	}

	newIssueComment := func(packageName, testName, message string) *github.IssueComment {
		b := body(packageName, testName, message)
		return &github.IssueComment{Body: &b}
	}

	assignee, err := getAssignee(ctx, req.AuthorEmail, p.listCommits)
	if err != nil {
		req.Message += fmt.Sprintf("\n\nFailed to find issue assignee: \n%s", err)
	}

	issueRequest := newIssueRequest(req.PackageName, req.TestName, req.Message, assignee)
	searchQuery := fmt.Sprintf(`"%s" user:%s repo:%s is:open`,
		*issueRequest.Title, githubUser, githubRepo)
	for _, label := range issueLabels {
		searchQuery = searchQuery + fmt.Sprintf(` label:"%s"`, label)
	}

	var foundIssue *int
	result, _, err := p.searchIssues(ctx, searchQuery, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to search GitHub with query %s",
			github.Stringify(searchQuery))
	}
	if *result.Total > 0 {
		foundIssue = result.Issues[0].Number
	}

	if foundIssue == nil {
		if _, _, err := p.createIssue(ctx, githubUser, githubRepo, issueRequest); err != nil {
			return errors.Wrapf(err, "failed to create GitHub issue %s",
				github.Stringify(issueRequest))
		}
	} else {
		comment := newIssueComment(req.PackageName, req.TestName, req.Message)
		if _, _, err := p.createComment(
			ctx, githubUser, githubRepo, *foundIssue, comment); err != nil {
			return errors.Wrapf(err, "failed to update issue #%d with %s",
				*foundIssue, github.Stringify(comment))
		}
	}

	return nil
}

func (p *poster) teamcityURL(tab, fragment string) *url.URL {
	options := url.Values{}
	options.Add("buildId", p.buildID)
	options.Add("tab", tab)

	u, err := url.Parse(p.serverURL)
	if err != nil {
		log.Fatal(err)
	}
	u.Scheme = "https"
	u.Path = "viewLog.html"
	u.RawQuery = options.Encode()
	u.Fragment = fragment
	return u
}

func (p *poster) teamcityBuildLogURL() *url.URL {
	return p.teamcityURL("buildLog", "")
}

func (p *poster) teamcityArtifactsURL(artifacts string) *url.URL {
	return p.teamcityURL("artifacts", artifacts)
}

func (p *poster) parameters() string {
	var parameters []string
	for _, parameter := range []string{
		tagsEnv,
		goFlagsEnv,
	} {
		if val, ok := os.LookupEnv(parameter); ok {
			parameters = append(parameters, parameter+"="+val)
		}
	}
	if len(parameters) == 0 {
		return ""
	}
	return "\n```\n" + strings.Join(parameters, "\n") + "\n```"
}

func isInvalidAssignee(err error) bool {
	e, ok := errors.Cause(err).(*github.ErrorResponse)
	if !ok {
		return false
	}
	if e.Response.StatusCode != 422 {
		return false
	}
	for _, t := range e.Errors {
		if t.Resource == "Issue" &&
			t.Field == "assignee" &&
			t.Code == "invalid" {
			return true
		}
	}
	return false
}

var defaultP struct {
	sync.Once
	*poster
}

// A PostRequest contains the information needed to create an issue about a
// test failure.
type PostRequest struct {
	// The title of the issue to create. A search for this title will be carried
	// out and on match, a comment will be posted into the existing issue
	// instead.
	Title,
	// The name of the package the test failure relates to.
	PackageName,
	// The name of the failing test.
	TestName,
	// The test output, ideally shrunk to contain only relevant details.
	Message,
	// A link to the test artifacts. If empty, defaults to a link constructed
	// from the TeamCity env vars (if available).
	Artifacts,
	// The email of the author, used to determine who to assign the issue to.
	AuthorEmail string
	// Additional labels that will be added to the issue. The labels must exist.
	ExtraLabels []string
}

// Post either creates a new issue for a failed test, or posts a comment to an
// existing open issue.
func Post(ctx context.Context, req PostRequest) error {
	defaultP.Do(func() {
		defaultP.poster = newPoster()
		defaultP.init()
	})
	err := defaultP.post(ctx, req)
	if !isInvalidAssignee(err) {
		return err
	}
	req.AuthorEmail = "tobias.schottdorf@gmail.com"
	return defaultP.post(ctx, req)
}

// CanPost returns true if the github API token environment variable is set.
func CanPost() bool {
	_, ok := os.LookupEnv(githubAPITokenEnv)
	return ok
}
