// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file implements the replay loop for the dlq-replay CLI tool: it
// walks the failed/ prefix of a DLQ bucket, claims entries via GCS
// preconditions, and re-fires them through issues.Post once GitHub has
// recovered. The CLI entry point lives in
// pkg/cmd/roachtest/dlq-replay/main.go.

package dlq

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
)

// ReplayResult tallies what happened during a Replay.
type ReplayResult struct {
	Posted  int // entry was claimed and successfully replayed
	Skipped int // entry could not be claimed (already taken by another runner)
	Failed  int // entry was claimed but its replay attempt failed; left in failed/
}

// ReplayOptions control the replay loop.
type ReplayOptions struct {
	// Bucket is the GCS bucket containing failed/, processing/, and
	// processed/ prefixes.
	Bucket *storage.BucketHandle

	// BranchFilter restricts processing to entries under
	// failed/<BranchFilter>/. Empty means all branches.
	BranchFilter string

	// PostFunc is the function used to re-fire issue posts. Defaults to
	// issues.Post; tests inject a fake.
	PostFunc PostFunc

	// DryRun skips the actual issues.Post call and releases the claim
	// without moving the entry to processed/. Useful for verifying that
	// entries deserialize and would be replayable without actually
	// posting to GitHub.
	DryRun bool

	// Logger receives a one-line status update per entry plus a final
	// summary. Defaults to a no-op logger.
	Logger Logger
}

// Replay lists the failed/ prefix of opts.Bucket and processes each
// entry. It returns once every entry has been visited (or skipped).
// Errors from individual entries do not abort the loop — they are
// reflected in ReplayResult.Failed.
func Replay(ctx context.Context, opts ReplayOptions) (ReplayResult, error) {
	if opts.Bucket == nil {
		return ReplayResult{}, errors.New("ReplayOptions.Bucket is nil")
	}
	if opts.PostFunc == nil {
		opts.PostFunc = issues.Post
	}
	if opts.Logger == nil {
		opts.Logger = noopLogger{}
	}

	prefix := "failed/"
	if opts.BranchFilter != "" {
		prefix = fmt.Sprintf("failed/%s/", opts.BranchFilter)
	}

	var result ReplayResult
	it := opts.Bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if stderrors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return result, errors.Wrap(err, "listing DLQ bucket")
		}
		// Skip "directory" markers GCS sometimes lists.
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		switch processEntry(ctx, opts, attrs.Name) {
		case outcomePosted:
			result.Posted++
		case outcomeSkipped:
			result.Skipped++
		case outcomeFailed:
			result.Failed++
		}
	}
	opts.Logger.Printf("done: %d ok, %d skipped, %d failed",
		result.Posted, result.Skipped, result.Failed)
	return result, nil
}

type outcome int

const (
	outcomePosted outcome = iota
	outcomeSkipped
	outcomeFailed
)

// processEntry handles the full lifecycle of one failed/ object: claim
// via GCS precondition, deserialize, replay, finalize.
func processEntry(ctx context.Context, opts ReplayOptions, failedName string) outcome {
	processingName := strings.Replace(failedName, "failed/", "processing/", 1)
	processedName := strings.Replace(failedName, "failed/", "processed/", 1)

	failedObj := opts.Bucket.Object(failedName)
	processingObj := opts.Bucket.Object(processingName)

	// Claim by copying with a DoesNotExist precondition. If another
	// runner already moved this to processing/, the copy fails with
	// 412 Precondition Failed and we skip.
	if _, err := processingObj.If(storage.Conditions{DoesNotExist: true}).
		CopierFrom(failedObj).Run(ctx); err != nil {
		opts.Logger.Printf("- %s skipped (already claimed: %v)", failedName, err)
		return outcomeSkipped
	}

	entry, err := readEntry(ctx, processingObj)
	if err != nil {
		opts.Logger.Printf("x %s read failed: %v", failedName, err)
		_ = processingObj.Delete(ctx) // release claim
		return outcomeFailed
	}

	req, postOpts := reconstruct(entry)
	if opts.DryRun {
		preview, err := previewURL(entry, req)
		if err != nil {
			opts.Logger.Printf("- %s [dry-run] previewing failed: %v", failedName, err)
		} else {
			opts.Logger.Printf("- %s [dry-run] would post to %s/%s as %q\n    preview: %s",
				failedName, entry.Org, entry.Repo, entry.TestName, preview)
		}
		_ = processingObj.Delete(ctx) // release claim, leave failed/ in place
		return outcomePosted
	}
	if _, err := opts.PostFunc(ctx, opts.Logger, issues.UnitTestFormatter, req, postOpts); err != nil {
		opts.Logger.Printf("x %s post failed: %v", failedName, err)
		_ = processingObj.Delete(ctx) // release claim, leave failed/ in place
		return outcomeFailed
	}

	// Success: move to processed/, delete failed/ and processing/.
	processedObj := opts.Bucket.Object(processedName)
	if _, err := processedObj.CopierFrom(processingObj).Run(ctx); err != nil {
		// Don't release the claim; the entry posted successfully and
		// re-running would create a duplicate. Leave processing/ around
		// so a human can investigate.
		opts.Logger.Printf("ok %s posted, but failed to archive to processed/: %v",
			failedName, err)
		return outcomePosted
	}
	if err := failedObj.Delete(ctx); err != nil {
		opts.Logger.Printf("ok %s posted, but failed to delete failed/ object: %v",
			failedName, err)
	}
	if err := processingObj.Delete(ctx); err != nil {
		opts.Logger.Printf("ok %s posted, but failed to delete processing/ object: %v",
			failedName, err)
	}
	opts.Logger.Printf("ok %s posted to %s/%s", failedName, entry.Org, entry.Repo)
	return outcomePosted
}

// readEntry reads and deserializes a JSON DLQ entry from the given object.
func readEntry(ctx context.Context, obj *storage.ObjectHandle) (*Entry, error) {
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "opening object")
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading object")
	}
	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, errors.Wrap(err, "unmarshaling DLQ entry")
	}
	return &entry, nil
}

// reconstruct rebuilds the issues.PostRequest and issues.Options from a
// stored entry. The HelpCommand closure re-emits the pre-rendered help
// text via Renderer.Raw so the replayed issue body matches what the
// original would have looked like.
//
// The token is intentionally not stored in entries — we read it from
// the GITHUB_API_TOKEN env var at replay time via
// issues.DefaultOptionsFromEnv.
func reconstruct(entry *Entry) (issues.PostRequest, *issues.Options) {
	req := issues.PostRequest{
		PackageName:             entry.PackageName,
		TestName:                entry.TestName,
		Labels:                  entry.Labels,
		AdoptIssueLabelMatchSet: entry.AdoptIssueLabelMatchSet,
		TopLevelNotes:           entry.TopLevelNotes,
		Message:                 entry.Message,
		ExtraParams:             entry.ExtraParams,
		Artifacts:               entry.Artifacts,
		MentionOnCreate:         entry.MentionOnCreate,
		HelpCommand:             helpCommandFromString(entry.RenderedHelpCommand),
	}
	binVer := entry.BinaryVersion
	opts := &issues.Options{
		Token:            issues.DefaultOptionsFromEnv().Token,
		Org:              entry.Org,
		Repo:             entry.Repo,
		SHA:              entry.SHA,
		Branch:           entry.Branch,
		GetBinaryVersion: func() string { return binVer },
	}
	if entry.TeamCityBuildTypeID != "" || entry.TeamCityBuildID != "" {
		opts.TeamCityOptions = &issues.TeamCityOptions{
			BuildTypeID: entry.TeamCityBuildTypeID,
			BuildID:     entry.TeamCityBuildID,
			ServerURL:   entry.TeamCityServerURL,
			Tags:        entry.TeamCityTags,
			Goflags:     entry.TeamCityGoflags,
		}
	}
	return req, opts
}

// previewURL renders the issue body and returns a "create new issue"
// link with the title and body pre-populated, so a dry-run operator can
// preview what the replay would post. Mirrors what issues.Post would
// produce, but targets the entry's stored org/repo instead of opening
// the GitHub API.
//
// Populates the same TemplateData fields that issues.Post's poster
// does — including URL/CommitURL/ArtifactsURL — so the rendered body
// has the build, commit, and artifacts links populated. Without these,
// the body shows empty markdown links like [failed]() and [<sha>]().
func previewURL(entry *Entry, req issues.PostRequest) (string, error) {
	org, repo := entry.Org, entry.Repo
	if org == "" {
		org = "cockroachdb"
	}
	if repo == "" {
		repo = "cockroach"
	}

	var commitURL string
	if entry.SHA != "" {
		commitURL = fmt.Sprintf("https://github.com/%s/%s/commits/%s", org, repo, entry.SHA)
	}
	buildURL := teamcityBuildURL(entry, "log", "")
	artifactsURL := ""
	if req.Artifacts != "" {
		artifactsURL = teamcityBuildURL(entry, "artifacts", req.Artifacts)
	}

	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		Branch:           entry.Branch,
		Commit:           entry.SHA,
		CommitURL:        commitURL,
		URL:              buildURL,
		ArtifactsURL:     artifactsURL,
		PackageNameShort: strings.TrimPrefix(req.PackageName, issues.CockroachPkgPrefix),
	}
	formatter := issues.UnitTestFormatter
	r := &issues.Renderer{}
	if err := formatter.Body(r, data); err != nil {
		return "", errors.Wrap(err, "rendering body")
	}

	// Append labels to the body so the operator can see what would be
	// applied. The "create new issue" form URL we generate cannot
	// auto-apply labels on the GitHub side; only the actual
	// issues.Post call (the non-dry-run replay path) sets them via
	// the API. Mirrors formatPostRequest in pkg/cmd/roachtest/github.go.
	body := r.String()
	if len(req.Labels) > 0 {
		var b strings.Builder
		b.WriteString(body)
		b.WriteString("\n------\nLabels:\n")
		for _, label := range req.Labels {
			fmt.Fprintf(&b, "- <code>%s</code>\n", label)
		}
		body = b.String()
	}

	u, err := url.Parse(fmt.Sprintf("https://github.com/%s/%s/issues/new", org, repo))
	if err != nil {
		return "", errors.Wrap(err, "building URL")
	}
	q := u.Query()
	q.Add("title", formatter.Title(data))
	q.Add("body", body)
	// Without a template parameter, GitHub redirects to the template
	// selection page instead of the new-issue form.
	q.Add("template", "none")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// teamcityBuildURL mirrors poster.teamcityURL in the issues package
// (which is unexported). Returns "" when TeamCity options are unset.
func teamcityBuildURL(entry *Entry, tab, fragment string) string {
	if entry.TeamCityServerURL == "" || entry.TeamCityBuildTypeID == "" || entry.TeamCityBuildID == "" {
		return ""
	}
	u, err := url.Parse(entry.TeamCityServerURL)
	if err != nil {
		return ""
	}
	u.Scheme = "https"
	u.Path = fmt.Sprintf("buildConfiguration/%s/%s", entry.TeamCityBuildTypeID, entry.TeamCityBuildID)
	q := url.Values{}
	q.Add("buildTab", tab)
	u.RawQuery = q.Encode()
	u.Fragment = fragment
	return u.String()
}

// helpCommandFromString returns a closure that re-emits the given
// pre-rendered HelpCommand output. Returns nil for an empty string so
// the formatter omits the Help section entirely.
func helpCommandFromString(rendered string) func(*issues.Renderer) {
	if rendered == "" {
		return nil
	}
	return func(r *issues.Renderer) {
		r.Raw(rendered)
	}
}

// noopLogger satisfies Logger when the caller doesn't supply one.
type noopLogger struct{}

func (noopLogger) Printf(string, ...interface{}) {}
