// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package issues

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

const (
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

func enforceMaxLength(s string) string {
	if len(s) > githubIssueBodyMaximumLength {
		return s[:githubIssueBodyMaximumLength]
	}
	return s
}

// context augments context.Context with a logger.
type postCtx struct {
	context.Context
	strings.Builder
}

func (ctx *postCtx) Printf(format string, args ...interface{}) {
	if n := len(format); n > 0 && format[n-1] != '\n' {
		format += "\n"
	}
	fmt.Fprintf(&ctx.Builder, format, args...)
}

func (p *poster) getProbableMilestone(ctx *postCtx) *int {
	gbv := p.GetBinaryVersion
	if gbv == nil {
		gbv = build.BinaryVersion
	}
	bv := gbv()
	v, err := version.Parse(bv)
	if err != nil {
		ctx.Printf("unable to parse version from binary version to determine milestone: %s", err)
		return nil
	}
	vstring := fmt.Sprintf("%d.%d", v.Major(), v.Minor())

	milestones, _, err := p.listMilestones(ctx, p.Org, p.Repo, &github.MilestoneListOptions{
		State: "open",
	})
	if err != nil {
		ctx.Printf("unable to list milestones for %s/%s: %v", p.Org, p.Repo, err)
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
	*Options

	l Logger

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
	createProjectCard func(ctx context.Context, columnID int64,
		opt *github.ProjectCardOptions) (*github.ProjectCard, *github.Response, error)
}

func newPoster(l Logger, client *github.Client, opts *Options) *poster {
	return &poster{
		Options:           opts,
		l:                 l,
		createIssue:       client.Issues.Create,
		searchIssues:      client.Search.Issues,
		createComment:     client.Issues.CreateComment,
		listCommits:       client.Repositories.ListCommits,
		listMilestones:    client.Issues.ListMilestones,
		createProjectCard: client.Projects.CreateProjectCard,
	}
}

// parameters returns the parameters to be displayed in the failure
// report. It adds the default parameters (currently, TAGS and
// GOFLAGS) to the list of parameters passed by the caller.
func (p *poster) parameters(extraParams map[string]string) map[string]string {
	ps := map[string]string{}
	for name, value := range extraParams {
		ps[name] = value
	}

	tcOpts := p.TeamCityOptions
	if tcOpts != nil {
		if tcOpts.Tags != "" {
			ps["TAGS"] = tcOpts.Tags
		}
		if tcOpts.Goflags != "" {
			ps["GOFLAGS"] = tcOpts.Goflags
		}
	}

	return ps
}

// TeamCityOptions configures TeamCity-specific Options.
type TeamCityOptions struct {
	BuildTypeID string
	BuildID     string
	ServerURL   string
	Tags        string
	Goflags     string
}

// EngFlowOptions configures EngFlow-specific Options.
type EngFlowOptions struct {
	ServerURL           string
	InvocationID        string
	Label               string
	Shard, Run, Attempt int
}

// Options configures the issue poster.
type Options struct {
	Token            string // GitHub API token
	Org              string
	Repo             string
	SHA              string
	Branch           string
	GetBinaryVersion func() string
	// One of the following sub-structs is expected to be populated. Post()
	// will fail if one is not.
	TeamCityOptions *TeamCityOptions
	EngFlowOptions  *EngFlowOptions
}

// DefaultOptionsFromEnv initializes the Options from the environment variables,
// falling back to placeholders if the environment is not or only partially
// populated. Note these default options are TeamCity-specific.
func DefaultOptionsFromEnv() *Options {
	// NB: these are hidden here as "proof" that nobody uses them directly
	// outside of this method.
	const (
		githubOrgEnv           = "GITHUB_ORG"
		githubRepoEnv          = "GITHUB_REPO"
		githubAPITokenEnv      = "GITHUB_API_TOKEN"
		teamcityVCSNumberEnv   = "BUILD_VCS_NUMBER"
		teamcityBuildTypeIDEnv = "TC_BUILDTYPE_ID"
		teamcityBuildIDEnv     = "TC_BUILD_ID"
		teamcityServerURLEnv   = "TC_SERVER_URL"
		teamcityBuildBranchEnv = "TC_BUILD_BRANCH"
		tagsEnv                = "TAGS"
		goFlagsEnv             = "GOFLAGS"
	)

	return &Options{
		Token: maybeEnv(githubAPITokenEnv, ""),
		Org:   maybeEnv(githubOrgEnv, "cockroachdb"),
		Repo:  maybeEnv(githubRepoEnv, "cockroach"),
		// The default value is the very first commit in the repository.
		// This was chosen simply because it exists and while surprising,
		// at least it'll be obvious that something went wrong (as an
		// issue will be posted pointing at that SHA).
		SHA:              maybeEnv(teamcityVCSNumberEnv, "8548987813ff9e1b8a9878023d3abfc6911c16db"),
		Branch:           maybeEnv(teamcityBuildBranchEnv, "branch-not-found-in-env"),
		GetBinaryVersion: build.BinaryVersion,
		TeamCityOptions: &TeamCityOptions{
			BuildTypeID: maybeEnv(teamcityBuildTypeIDEnv, "BUILDTYPE_ID-not-found-in-env"),
			BuildID:     maybeEnv(teamcityBuildIDEnv, "NOTFOUNDINENV"),
			ServerURL:   maybeEnv(teamcityServerURLEnv, "https://server-url-not-found-in-env.com"),
			Tags:        maybeEnv(tagsEnv, ""),
			Goflags:     maybeEnv(goFlagsEnv, ""),
		},
	}
}

func maybeEnv(envKey, defaultValue string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return defaultValue
	}
	return v
}

// CanPost returns true if the github API token environment variable is set to
// a nontrivial value.
func (o *Options) CanPost() bool {
	return o.Token != ""
}

// IsReleaseBranch returns true for branches that we want to treat as
// "release" branches, including master and provisional branches.
func (o *Options) IsReleaseBranch() bool {
	return o.Branch == "master" || strings.HasPrefix(o.Branch, "release-") || strings.HasPrefix(o.Branch, "provisional_")
}

// TemplateData is the input on which an IssueFormatter operates. It has
// everything known about the test failure in a predigested form.
type TemplateData struct {
	PostRequest
	// This is foo/bar instead of github.com/cockroachdb/cockroach/pkg/foo/bar.
	PackageNameShort string
	// Parameters includes relevant test or build parameters, such as
	// build tags or cluster configuration
	Parameters map[string]string
	// The message, garnished with helpers that allow extracting the useful
	// bots.
	CondensedMessage CondensedMessage
	// The commit SHA.
	Commit string
	// Link to the commit on GitHub.
	CommitURL string
	// The branch.
	Branch string
	// An URL that goes straight to the artifacts for this test.
	// Set only if PostRequest.Artifacts was provided.
	ArtifactsURL string
	// URL is the link to the failing build.
	URL string
	// SideEyeSnapshotURL is the URL for accessing a Side-Eye snapshot associated
	// with this test failure. Empty if no such snapshot exists.
	SideEyeSnapshotURL string
	// SideEyeSnapshotMsg is a message to prepend to the link to the SideEye
	// snapshot. Empty if SideEyeSnapshotURL is empty.
	SideEyeSnapshotMsg string
	// Issues that match this one, except they're on other branches.
	RelatedIssues []github.Issue
	// InternalLog contains information about non-critical issues encountered
	// while forming the issue.
	InternalLog string
}

func (p *poster) templateData(
	ctx context.Context, req PostRequest, relatedIssues []github.Issue,
) TemplateData {
	var artifactsURL string
	if req.Artifacts != "" {
		artifactsURL = p.teamcityArtifactsURL(req.Artifacts).String()
	}
	return TemplateData{
		PostRequest:        req,
		PackageNameShort:   strings.TrimPrefix(req.PackageName, CockroachPkgPrefix),
		Parameters:         p.parameters(req.ExtraParams),
		CondensedMessage:   CondensedMessage(req.Message),
		Commit:             p.SHA,
		CommitURL:          fmt.Sprintf("https://github.com/%s/%s/commits/%s", p.Org, p.Repo, p.SHA),
		Branch:             p.Branch,
		ArtifactsURL:       artifactsURL,
		URL:                p.buildURL().String(),
		SideEyeSnapshotURL: req.SideEyeSnapshotURL,
		SideEyeSnapshotMsg: req.SideEyeSnapshotMsg,
		RelatedIssues:      relatedIssues,
	}
}

func releaseLabel(branch string) string {
	return fmt.Sprintf("branch-%s", branch)
}

func buildIssueQueries(
	repo string, org string, branch string, title string, req PostRequest,
) (existingIssueQuery string, relatedIssuesQuery string) {
	base := fmt.Sprintf(
		`repo:%q user:%q is:issue is:open in:title sort:created-desc %q`,
		repo, org, title)

	labelsQuery := func(mustHave, mustNotHave []string) string {
		var b bytes.Buffer
		for _, l := range mustHave {
			fmt.Fprintf(&b, " label:%s", l)
		}
		for _, l := range mustNotHave {
			fmt.Fprintf(&b, " -label:%s", l)
		}
		return b.String()
	}

	// Build a set of labels.
	labels := make(map[string]bool)
	for _, l := range req.labels() {
		labels[l] = true
	}

	// Build the sets of labels that must be present on the existing issue, and
	// which must NOT be present on the existing issue.
	mustHave := []string{RobotLabel}
	var mustNotHave []string
	for _, l := range req.AdoptIssueLabelMatchSet {
		if labels[l] {
			mustHave = append(mustHave, l)
		} else {
			mustNotHave = append(mustNotHave, l)
		}
	}

	existingIssueQuery = base + labelsQuery(
		append(mustHave, releaseLabel(branch)),
		append(mustNotHave, noReuseLabel),
	)
	// The related issues query selects for branches.
	relatedIssuesQuery = base + labelsQuery(
		mustHave,
		append(mustNotHave, releaseLabel(branch)),
	)
	return existingIssueQuery, relatedIssuesQuery
}

type TestFailureType string

const (
	TestFailureNewIssue     = TestFailureType("new_issue")
	TestFailureIssueComment = TestFailureType("comment")
)

// TestFailureIssue encapsulates data about an issue created or
// changed in order to report a test failure.
type TestFailureIssue struct {
	Type TestFailureType
	ID   int
}

func (tfi TestFailureIssue) String() string {
	switch tfi.Type {
	case TestFailureNewIssue:
		return fmt.Sprintf("created new GitHub issue #%d", tfi.ID)
	case TestFailureIssueComment:
		return fmt.Sprintf("commented on existing GitHub issue #%d", tfi.ID)
	default:
		return fmt.Sprintf("[unrecognized test failure type %q, ID=%d]", tfi.Type, tfi.ID)
	}
}

func (p *poster) post(
	origCtx context.Context, formatter IssueFormatter, req PostRequest,
) (*TestFailureIssue, error) {
	ctx := &postCtx{Context: origCtx}
	data := p.templateData(
		ctx,
		req,
		nil, // relatedIssues
	)

	// We just want the title this time around, as we're going to use
	// it to figure out if an issue already exists.
	title := formatter.Title(data)

	// We carry out two searches below, one attempting to find an issue that we
	// adopt (i.e. add a comment to) and one finding "related issues", i.e. those
	// that would match if it weren't for their branch label.
	qExisting, qRelated := buildIssueQueries(p.Repo, p.Org, p.Branch, title, req)

	rExisting, _, err := p.searchIssues(ctx, qExisting, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 10,
		},
	})
	if err != nil {
		// Tough luck, keep going even if that means we're going to add a duplicate
		// issue.
		p.l.Printf("error trying to find existing GitHub issues: %v", err)
		rExisting = &github.IssuesSearchResult{}
	}

	rRelated, _, err := p.searchIssues(ctx, qRelated, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 10,
		},
	})
	if err != nil {
		// This is no reason to throw the towel, keep going.
		p.l.Printf("error trying to find related GitHub issues: %v", err)
		rRelated = &github.IssuesSearchResult{}
	}

	existingIssues := filterByPrefixTitleMatch(rExisting, title)
	var foundIssue *int
	if len(existingIssues) > 0 {
		// We found an existing issue to post a comment into.
		foundIssue = existingIssues[0].Number
		p.l.Printf("found existing GitHub issue: #%d", *foundIssue)
		// We are not going to create an issue, so don't show
		// MentionOnCreate to the formatter.Body call below.
		data.MentionOnCreate = nil
	}

	data.RelatedIssues = filterByPrefixTitleMatch(rRelated, title)
	data.InternalLog = ctx.Builder.String()
	r := &Renderer{}
	if err := formatter.Body(r, data); err != nil {
		// Failure is not an option.
		_ = err
		fmt.Fprintln(&r.buf, "\nFailed to render body: "+err.Error())
	}

	body := enforceMaxLength(r.buf.String())

	createLabels := []string{RobotLabel}
	createLabels = append(createLabels, req.labels()...)
	createLabels = append(createLabels, releaseLabel(p.Branch))
	var result TestFailureIssue
	if foundIssue == nil {
		issueRequest := github.IssueRequest{
			Title:     &title,
			Body:      github.String(body),
			Labels:    &createLabels,
			Milestone: p.getProbableMilestone(ctx),
		}
		issue, _, err := p.createIssue(ctx, p.Org, p.Repo, &issueRequest)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create GitHub issue %s",
				github.Stringify(issueRequest))
		}

		result.Type = TestFailureNewIssue
		result.ID = *issue.Number
		p.l.Printf("%s", result)
		if req.ProjectColumnID != 0 {
			_, _, err := p.createProjectCard(ctx, int64(req.ProjectColumnID), &github.ProjectCardOptions{
				ContentID:   *issue.ID,
				ContentType: "Issue",
			})
			if err != nil {
				// Tough luck, keep going.
				//
				// TODO(tbg): retrieve the project column ID before posting, so that if
				// it can't be found we can mention that in the issue we'll file anyway.
				p.l.Printf("could not create GitHub project card: %v", err)
			}
		}
	} else {
		comment := github.IssueComment{Body: github.String(body)}
		if _, _, err := p.createComment(
			ctx, p.Org, p.Repo, *foundIssue, &comment); err != nil {
			return nil, errors.Wrapf(err, "failed to update issue #%d with %s",
				*foundIssue, github.Stringify(comment))
		} else {
			result.Type = TestFailureIssueComment
			result.ID = *foundIssue
			p.l.Printf("%s", result)
		}
	}

	return &result, nil
}

func (p *poster) teamcityURL(tab, fragment string) *url.URL {
	if p.TeamCityOptions == nil {
		return nil
	}
	opts := p.TeamCityOptions
	options := url.Values{}
	options.Add("buildTab", tab)

	u, err := url.Parse(opts.ServerURL)
	if err != nil {
		log.Fatal(err)
	}
	u.Scheme = "https"
	u.Path = fmt.Sprintf("buildConfiguration/%s/%s", opts.BuildTypeID, opts.BuildID)
	u.RawQuery = options.Encode()
	u.Fragment = fragment
	return u
}

func (p *poster) buildURL() *url.URL {
	if p.Options.TeamCityOptions != nil {
		u := p.teamcityURL("log", "")
		return u
	} else if p.Options.EngFlowOptions != nil {
		opts := p.Options.EngFlowOptions
		u, err := url.Parse(opts.ServerURL)
		if err != nil {
			log.Fatal(err)
		}
		base64Target := base64.StdEncoding.EncodeToString([]byte(opts.Label))
		u.Path = fmt.Sprintf("invocations/default/%s", opts.InvocationID)
		u.RawQuery = fmt.Sprintf("testReportRun=%d&testReportShard=%d&testReportAttempt=%d", opts.Run, opts.Shard, opts.Attempt)
		u.Fragment = fmt.Sprintf("targets-%s", base64Target)
		return u
	}
	return nil
}

func (p *poster) teamcityArtifactsURL(artifacts string) *url.URL {
	return p.teamcityURL("artifacts", artifacts)
}

const (
	RobotLabel          = "O-robot"
	TestFailureLabel    = "C-test-failure"
	ReleaseBlockerLabel = "release-blocker"
	noReuseLabel        = "X-noreuse"
)

// DefaultLabels is the default value for PostRequest.Labels.
var DefaultLabels = []string{TestFailureLabel, ReleaseBlockerLabel}

// A PostRequest contains the information needed to create an issue about a
// test failure.
type PostRequest struct {
	// The name of the package the test failure relates to.
	PackageName string
	// The name of the failing test.
	TestName string
	// Labels that will be set for the issue, in addition to `O-robot` which is
	// always added. Labels that don't exist will be created as necessary.
	// If nil, the DefaultLabels value is used.
	Labels []string
	// AdoptIssueLabelMatchSet is the set of labels whose presence or absence must
	// match in order to adopt an issue.
	//
	// For each label l in this set: if l is part of Labels, then any adopted
	// issue must also have this label set; it l is not part of Labels, than any
	// adopted issue must also NOT have this label set.
	AdoptIssueLabelMatchSet []string
	// TopLevelNotes are messages that are printed prominently at the top of the
	// issue description.
	TopLevelNotes []string
	// The test output.
	Message string
	// ExtraParams contains the parameters to be included in a failure
	// report, other than the defaults (git branch, test flags).
	ExtraParams map[string]string
	// A path to the test artifacts relative to the artifacts root. If nonempty,
	// allows the poster formatter to construct a direct URL to this directory.
	Artifacts string
	// SideEyeSnapshotURL is the URL for accessing a Side-Eye snapshot associated
	// with this test failure. Empty if no such snapshot exists.
	SideEyeSnapshotURL string
	// SideEyeSnapshotMsg is a message to prepend to the link to the SideEye
	// snapshot. Empty if SideEyeSnapshotURL is empty.
	SideEyeSnapshotMsg string
	// MentionOnCreate is a slice of GitHub handles (@foo, @cockroachdb/some-team, etc)
	// that should be mentioned in the message when creating a new issue. These are
	// *not* mentioned when posting to an existing issue.
	MentionOnCreate []string
	// A help section of the issue, for example with links to documentation or
	// instructions on how to reproduce the issue.
	HelpCommand func(*Renderer)

	// ProjectColumnID is the id of the GitHub project column to add the issue to,
	// or 0 if none.
	ProjectColumnID int
}

func (r PostRequest) labels() []string {
	if r.Labels == nil {
		return DefaultLabels
	}
	return r.Labels
}

// Logger is an interface that allows callers to plug their own log
// implementation when they post GitHub issues. It avoids us having to
// link against heavy dependencies in certain cases (such as in
// `bazci`) while still allowing other callers (such as `roachtest`)
// to use other logger implementations.
type Logger interface {
	Printf(format string, args ...interface{})
}

// Post either creates a new issue for a failed test, or posts a comment to an
// existing open issue. GITHUB_API_TOKEN must be set to a valid GitHub token
// that has permissions to search and create issues and comments or an error
// will be returned.
func Post(
	ctx context.Context, l Logger, formatter IssueFormatter, req PostRequest, opts *Options,
) (*TestFailureIssue, error) {
	if !opts.CanPost() {
		return nil, errors.Newf("GITHUB_API_TOKEN env variable is not set; cannot post issue")
	}

	client := github.NewClient(oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: opts.Token},
	)))
	return newPoster(l, client, opts).post(ctx, formatter, req)
}

// ReproductionCommandFromString returns a value for the
// PostRequest.HelpCommand field that is a command to run. It is
// formatted as a bash code block.
func ReproductionCommandFromString(repro string) func(*Renderer) {
	if repro == "" {
		return func(*Renderer) {}
	}
	return func(r *Renderer) {
		r.Escaped("To reproduce, try:\n")
		r.CodeBlock("bash", repro)
	}
}

// HelpCommandAsLink returns a value for the PostRequest.HelpCommand field
// that prints a link to documentation to refer to.
func HelpCommandAsLink(title, href string) func(r *Renderer) {
	return func(r *Renderer) {
		// Bit of weird formatting here but apparently markdown links don't work inside
		// of a line that also has a <p> tag. Putting it on its own line makes it work.
		r.Escaped("\n\nSee: ")
		r.A(title, href)
		r.Escaped("\n\n")
	}
}

// filterByPrefixTitleMatch filters the search result passed and removes any
// issues where the title does not match the expected title, optionally followed
// by whitespace. This is done because the GitHub API does not support searching
// by exact title; as a consequence, without this function, there is a chance we
// would group together test failures for two similarly named tests. That is
// confusing and undesirable behavior.
func filterByPrefixTitleMatch(
	result *github.IssuesSearchResult, expectedTitle string,
) []github.Issue {
	expectedTitleRegex := regexp.MustCompile(`^` + regexp.QuoteMeta(expectedTitle) + `(\s+|$)`)
	var issues []github.Issue
	for _, issue := range result.Issues {
		if title := issue.Title; title != nil && expectedTitleRegex.MatchString(*title) {
			issues = append(issues, issue)
		}
	}

	return issues
}
