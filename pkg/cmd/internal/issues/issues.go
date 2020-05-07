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
	"strings"
	"sync"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

const (
	githubAPITokenEnv      = "GITHUB_API_TOKEN"
	teamcityVCSNumberEnv   = "BUILD_VCS_NUMBER"
	teamcityBuildIDEnv     = "TC_BUILD_ID"
	teamcityServerURLEnv   = "TC_SERVER_URL"
	teamcityBuildBranchEnv = "TC_BUILD_BRANCH"
	tagsEnv                = "TAGS"
	goFlagsEnv             = "GOFLAGS"
	// CockroachPkgPrefix is the crdb package prefix.
	CockroachPkgPrefix = "github.com/cockroachdb/cockroach/pkg/"
	// Based on the following observed API response the maximum here is 1<<16-1.
	// We shouldn't usually get near that limit but if we do, better to post a
	// clipped issue.
	//
	// 422 Validation Failed [{Resource:Issue Field:body Code:custom Message:body
	// is too long (maximum is 65536 characters)}]
	githubIssueBodyMaximumLength = 60000
)

var (
	githubUser = "cockroachdb" // changeable for testing
	githubRepo = "cockroach"   // changeable for testing
)

func enforceMaxLength(s string) string {
	if len(s) > githubIssueBodyMaximumLength {
		return s[:githubIssueBodyMaximumLength]
	}
	return s
}

// UnitTestFailureTitle is a title template suitable for posting issues about
// vanilla Go test failures.
const UnitTestFailureTitle = `{{ shortpkg .PackageName }}: {{.TestName}} failed`

// UnitTestFailureBody is a body template suitable for posting issues about vanilla Go
// test failures.
const UnitTestFailureBody = `[({{shortpkg .PackageName}}).{{.TestName}} failed]({{.URL}}) on [{{.Branch}}@{{.Commit}}]({{commiturl .Commit}}):

{{ if (.CondensedMessage.FatalOrPanic 50).Error }}{{with $fop := .CondensedMessage.FatalOrPanic 50 -}}
Fatal error:
{{threeticks}}
{{ .Error }}{{threeticks}}

Stack:
{{threeticks}}
{{ $fop.FirstStack }}
{{threeticks}}

<details><summary>Log preceding fatal error</summary><p>

{{threeticks}}
{{ $fop.LastLines }}
{{threeticks}}

</p></details>{{end}}{{ else -}}
{{threeticks}}
{{ .CondensedMessage.Digest 50 }}
{{ threeticks }}{{end}}

<details><summary>More</summary><p>
{{if .Parameters -}}
Parameters:
{{range .Parameters }}
- {{ . }}{{end}}{{end}}

{{if .ArtifactsURL }}Artifacts: [{{.Artifacts}}]({{ .ArtifactsURL }})
{{else -}}
{{threeticks}}
make stressrace TESTS={{.TestName}} PKG=./pkg/{{shortpkg .PackageName}} TESTTIMEOUT=5m STRESSFLAGS='-timeout 5m' 2>&1
{{threeticks}}

{{end -}}

{{ if .RelatedIssues }}Related:{{end}}{{range .RelatedIssues}}
- #{{ .Number}} {{ .Title }} {{ range .Labels }} [{{ .Name }}]({{ .URL }}){{- end}}
{{end}}
[See this test on roachdash](https://roachdash.crdb.dev/?filter={{urlquery "status:open t:.*" .TestName ".*" }}&sort=title&restgroup=false&display=lastcommented+project)
<sub>powered by [pkg/cmd/internal/issues](https://github.com/cockroachdb/cockroach/tree/master/pkg/cmd/internal/issues)</sub></p></details>
`

var (
	// Set of labels attached to created issues.
	issueLabels = []string{"O-robot", "C-test-failure"}
	// Label we expect when checking existing issues. Sometimes users open
	// issues about flakes and don't assign all the labels. We want to at
	// least require the test-failure label to avoid pathological situations
	// in which a test name is so generic that it matches lots of random issues.
	// Note that we'll only post a comment into an existing label if the labels
	// match 100%, but we also cross-link issues whose labels differ. But we
	// require that they all have searchLabel as a baseline.
	searchLabel = issueLabels[1]
)

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
	branch    string
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
	// TODO(tbg): make these not fatals. It's better to post an incomplete issue than to add
	// more weird failures to the CI pipeline.
	if p.sha, ok = os.LookupEnv(teamcityVCSNumberEnv); !ok {
		log.Fatalf("VCS number environment variable %s is not set", teamcityVCSNumberEnv)
	}
	if p.buildID, ok = os.LookupEnv(teamcityBuildIDEnv); !ok {
		log.Fatalf("teamcity build ID environment variable %s is not set", teamcityBuildIDEnv)
	}
	if p.serverURL, ok = os.LookupEnv(teamcityServerURLEnv); !ok {
		log.Fatalf("teamcity server URL environment variable %s is not set", teamcityServerURLEnv)
	}
	if p.branch, ok = os.LookupEnv(teamcityBuildBranchEnv); !ok {
		p.branch = "unknown"
	}
	p.milestone = getProbableMilestone(context.Background(), p.getLatestTag, p.listMilestones)
}

// TemplateData holds the data available in (PostRequest).(Body|Title)Template,
// respectively. On top of the below, there are also a few functions, for which
// UnitTestFailureBody can serve as a reference.
type TemplateData struct {
	PostRequest
	Parameters       []string
	CondensedMessage CondensedMessage
	Commit           string
	Branch           string
	ArtifactsURL     string
	URL              string
	Assignee         interface{} // lazy
	RelatedIssues    []github.Issue
}

type lazy struct {
	work func() interface{}

	s    interface{}
	once sync.Once
}

func (l *lazy) String() string {
	l.once.Do(func() {
		l.s = l.work()
	})
	return fmt.Sprint(l.s)
}

func (p *poster) templateData(
	ctx context.Context, req PostRequest, relatedIssues []github.Issue,
) TemplateData {
	return TemplateData{
		PostRequest:      req,
		Parameters:       p.parameters(),
		CondensedMessage: CondensedMessage(req.Message),
		Branch:           p.branch,
		Commit:           p.sha,
		ArtifactsURL: func() string {
			if req.Artifacts != "" {
				return p.teamcityArtifactsURL(req.Artifacts).String()
			}
			return ""
		}(),
		URL: p.teamcityBuildLogURL().String(),
		Assignee: &lazy{work: func() interface{} {
			// NB: the laziness here isn't motivated by anything in particular,
			// so rip it out if it ever causes problems.
			handle, err := getAssignee(ctx, req.AuthorEmail, p.listCommits)
			if err != nil {
				return ""
			}
			return handle
		}},
		RelatedIssues: relatedIssues,
	}
}

func (p *poster) execTemplate(ctx context.Context, tpl string, data TemplateData) (string, error) {
	tlp, err := template.New("").Funcs(template.FuncMap{
		"threeticks": func() string { return "```" },
		"commiturl": func(sha string) string {
			return fmt.Sprintf("https://github.com/cockroachdb/cockroach/commits/%s", sha)
		},
		"shortpkg": func(fullpkg string) string {
			return strings.TrimPrefix(fullpkg, CockroachPkgPrefix)
		},
	}).Parse(tpl)
	if err != nil {
		return "", err
	}

	var buf strings.Builder
	if err := tlp.Execute(&buf, data); err != nil {
		return "", err
	}
	return enforceMaxLength(buf.String()), nil
}

func (p *poster) post(ctx context.Context, req PostRequest) error {
	assignee, err := getAssignee(ctx, req.AuthorEmail, p.listCommits)
	if err != nil {
		req.Message += fmt.Sprintf("\n\nFailed to find issue assignee: \n%s", err)
	}

	title, err := p.execTemplate(ctx, req.TitleTemplate, p.templateData(ctx, req, nil))
	if err != nil {
		return err
	}

	// We carry out two searches below, one attempting to find an issue that we
	// adopt (i.e. add a comment to) and one finding "related issues", i.e. those
	// that would match if it weren't for their branch label.
	qBase := fmt.Sprintf(
		`repo:%q user:%q is:issue is:open in:title label:%q sort:created-desc %q`,
		githubRepo, githubUser, searchLabel, title)

	releaseLabel := fmt.Sprintf("branch-%s", p.branch)
	qExisting := qBase + " label:" + releaseLabel
	qRelated := qBase + " -label:" + releaseLabel

	rExisting, _, err := p.searchIssues(ctx, qExisting, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to search GitHub for %s", qExisting)
	}

	rRelated, _, err := p.searchIssues(ctx, qRelated, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 10,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to search GitHub for %s", qRelated)
	}

	var foundIssue *int
	if len(rExisting.Issues) > 0 {
		// We found an existing issue to post a comment into.
		foundIssue = rExisting.Issues[0].Number
	}

	body, err := p.execTemplate(ctx, req.BodyTemplate, p.templateData(ctx, req, rRelated.Issues))
	if err != nil {
		return err
	}

	createLabels := append(issueLabels, releaseLabel)
	createLabels = append(createLabels, req.ExtraLabels...)
	if foundIssue == nil {
		issueRequest := github.IssueRequest{
			Title:     &title,
			Body:      &body,
			Labels:    &createLabels,
			Assignee:  &assignee,
			Milestone: p.milestone,
		}
		if _, _, err := p.createIssue(ctx, githubUser, githubRepo, &issueRequest); err != nil {
			return errors.Wrapf(err, "failed to create GitHub issue %s",
				github.Stringify(issueRequest))
		}
	} else {
		comment := github.IssueComment{Body: &body}
		if _, _, err := p.createComment(
			ctx, githubUser, githubRepo, *foundIssue, &comment); err != nil {
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

func (p *poster) parameters() []string {
	var parameters []string
	for _, parameter := range []string{
		tagsEnv,
		goFlagsEnv,
	} {
		if val, ok := os.LookupEnv(parameter); ok {
			parameters = append(parameters, parameter+"="+val)
		}
	}
	return parameters
}

func isInvalidAssignee(err error) bool {
	var e *github.ErrorResponse
	if !errors.As(err, &e) {
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
	// The title of the issue. See UnitTestFailureTitleTemplate for an example.
	TitleTemplate,
	// The body of the issue. See UnitTestFailureBodyTemplate for an example.
	BodyTemplate,
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
	// Additional labels that will be added to the issue. They will be created
	// as necessary (as a side effect of creating an issue with them). An
	// existing issue may be adopted even if it does not have these labels.
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

// CanPost returns true if the github API token environment variable is set to
// a nontrivial value.
func CanPost() bool {
	s, ok := os.LookupEnv(githubAPITokenEnv)
	return ok && len(s) > 0
}
