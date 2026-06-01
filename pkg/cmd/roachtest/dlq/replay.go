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
	"net/http"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/googleapi"
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
	// Bucket is set from the dlq-replay --bucket CLI flag. It is the GCS bucket
	// containing failed/, processing/, and processed/ prefixes.
	Bucket *storage.BucketHandle

	// BranchFilter is set from the dlq-replay --branch CLI flag. It restricts
	// processing to entries under failed/<BranchFilter>/. Empty means all
	// branches.
	BranchFilter string

	// PostFunc is the function used to re-fire issue posts. Defaults to
	// issues.Post; tests inject a fake.
	PostFunc PostFunc

	// SkipGitHubPost is set by the dlq-replay --skip-github-post CLI flag. It
	// skips the actual issues.Post call and releases the claim without moving
	// the entry to processed/. Useful for verifying that entries deserialize
	// and would be replayable without mutating GitHub.
	SkipGitHubPost bool

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

type replayStore interface {
	claim(ctx context.Context, failedName, processingName string) error
	processedExists(ctx context.Context, processedName string) (bool, error)
	read(ctx context.Context, name string) (*Entry, error)
	copy(ctx context.Context, srcName, dstName string) error
	delete(ctx context.Context, name string) error
}

type gcsReplayStore struct {
	bucket *storage.BucketHandle
}

func (s gcsReplayStore) claim(ctx context.Context, failedName, processingName string) error {
	_, err := s.bucket.Object(processingName).
		If(storage.Conditions{DoesNotExist: true}).
		CopierFrom(s.bucket.Object(failedName)).
		Run(ctx)
	return err
}

func (s gcsReplayStore) processedExists(ctx context.Context, processedName string) (bool, error) {
	if _, err := s.bucket.Object(processedName).Attrs(ctx); err == nil {
		return true, nil
	} else if stderrors.Is(err, storage.ErrObjectNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func (s gcsReplayStore) read(ctx context.Context, name string) (*Entry, error) {
	return readEntry(ctx, s.bucket.Object(name))
}

func (s gcsReplayStore) copy(ctx context.Context, srcName, dstName string) error {
	_, err := s.bucket.Object(dstName).CopierFrom(s.bucket.Object(srcName)).Run(ctx)
	return err
}

func (s gcsReplayStore) delete(ctx context.Context, name string) error {
	return s.bucket.Object(name).Delete(ctx)
}

// processEntry handles the replay state machine for one failed/ entry.
//
// The original failed/ object stays in place until GitHub accepts the replay
// and the entry is archived to processed/. To claim work, replay first copies
// failed/X to processing/X with a DoesNotExist precondition. That gives each
// entry a single active runner: a second runner attempting the same claim gets
// a GCS 412 and skips the entry.
//
// After claiming, replay checks for processed/X. If it already exists, a prior
// run posted and archived the entry but failed to delete failed/X. In that
// case, replay treats failed/X as stale cleanup and does not post to GitHub
// again.
//
// GCS does not provide transactions, and GitHub posts are external side
// effects. Once the GitHub post succeeds, failures in archiving or stale
// failed/ cleanup leave processing/X in place so later runs skip the entry
// pending manual inspection instead of risking a duplicate post.
//
// The post-success finalization cases are:
//  1. Copying processing/X to processed/X fails. failed/X and processing/X
//     both remain, so processing/X must stay in place to prevent a later
//     duplicate replay from failed/X.
//  2. processed/X was written, but deleting failed/X fails. failed/X still
//     exists, so processing/X stays in place as an extra duplicate-post guard.
//  3. processed/X was written and failed/X was deleted, but deleting
//     processing/X fails. This leaves stale processing/X cleanup work, but no
//     duplicate-post risk because replay only lists failed/.
//
// Manual reconciliation for a stuck processing/X entry is:
//   - If the expected GitHub issue/comment exists, make sure processed/X exists,
//     delete failed/X if it still exists, then delete processing/X.
//   - If the expected GitHub issue/comment does not exist, make sure failed/X
//     exists, then delete processing/X so a future replay can retry it.
func processEntry(ctx context.Context, opts ReplayOptions, failedName string) outcome {
	return processEntryWithStore(ctx, opts, failedName, gcsReplayStore{bucket: opts.Bucket})
}

func processEntryWithStore(
	ctx context.Context, opts ReplayOptions, failedName string, store replayStore,
) outcome {
	processingName := strings.Replace(failedName, "failed/", "processing/", 1)
	processedName := strings.Replace(failedName, "failed/", "processed/", 1)

	// Claim by copying with a DoesNotExist precondition. If another
	// runner already moved this to processing/, the copy fails with
	// 412 Precondition Failed and we skip.
	if err := store.claim(ctx, failedName, processingName); err != nil {
		var gerr *googleapi.Error
		if stderrors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed {
			opts.Logger.Printf("- %s skipped (already claimed)", failedName)
			return outcomeSkipped
		}
		opts.Logger.Printf("x %s claim failed: %v", failedName, err)
		return outcomeFailed
	}

	// If a prior successful replay archived this entry but failed to
	// delete failed/, avoid posting it again. Treat failed/ as stale and
	// clean it up before releasing this claim.
	if exists, err := store.processedExists(ctx, processedName); err != nil {
		opts.Logger.Printf("x %s processed/ check failed: %v", failedName, err)
		if err := store.delete(ctx, processingName); err != nil {
			opts.Logger.Printf("x %s failed to release processing/ claim after processed/ check failed: %v",
				failedName, err)
		}
		return outcomeFailed
	} else if exists {
		if err := store.delete(ctx, failedName); err != nil {
			opts.Logger.Printf("x %s already processed, but failed to delete failed/ object: %v",
				failedName, err)
			return outcomeFailed
		}
		if err := store.delete(ctx, processingName); err != nil {
			opts.Logger.Printf("x %s already processed, but failed to delete processing/ object: %v",
				failedName, err)
			return outcomeFailed
		}
		opts.Logger.Printf("- %s skipped (already processed)", failedName)
		return outcomeSkipped
	}

	// The current runner owns processing/X and there is no processed/X marker,
	// so the entry will be deserialized and replayed.
	entry, err := store.read(ctx, processingName)
	if err != nil {
		opts.Logger.Printf("x %s read failed: %v", failedName, err)
		_ = store.delete(ctx, processingName) // release claim
		return outcomeFailed
	}

	req, postOpts := reconstruct(entry)
	if opts.SkipGitHubPost {
		preview, err := previewURL(entry, req)
		if err != nil {
			opts.Logger.Printf("- %s [skip-github-post] previewing failed: %v", failedName, err)
		} else {
			opts.Logger.Printf("- %s [skip-github-post] would post to %s/%s as %q\n    preview: %s",
				failedName, entry.Org, entry.Repo, entry.TestName, preview)
		}
		_ = store.delete(ctx, processingName) // release claim, leave failed/ in place
		return outcomePosted
	}
	if _, err := opts.PostFunc(ctx, opts.Logger, issues.UnitTestFormatter, req, postOpts); err != nil {
		opts.Logger.Printf("x %s post failed: %v", failedName, err)
		_ = store.delete(ctx, processingName) // release claim, leave failed/ in place
		return outcomeFailed
	}

	// GitHub accepted the replay; attempt to archive it to processed/ and
	// clean up failed/ and processing/.
	if err := store.copy(ctx, processingName, processedName); err != nil {
		// Don't release the claim; the entry posted successfully and
		// re-running would create a duplicate. Leave processing/ around
		// so a human can investigate.
		opts.Logger.Printf("x %s posted, but failed to archive to processed/: %v",
			failedName, err)
		return outcomeFailed
	}
	if err := store.delete(ctx, failedName); err != nil {
		// Don't release the claim; failed/ still exists and could be
		// replayed again if processing/ were removed.
		opts.Logger.Printf("x %s posted and archived, but failed to delete failed/ object: %v",
			failedName, err)
		return outcomeFailed
	}
	if err := store.delete(ctx, processingName); err != nil {
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
// link with the title and body pre-populated, so an operator can
// preview what the replay would post. Mirrors roachtest's existing
// dry-run formatter, but targets the entry's stored org/repo instead
// of opening the GitHub API.
func previewURL(entry *Entry, req issues.PostRequest) (string, error) {
	org, repo := entry.Org, entry.Repo
	if org == "" {
		org = "cockroachdb"
	}
	if repo == "" {
		repo = "cockroach"
	}

	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		Branch:           "test_branch",
		Commit:           "test_SHA",
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
	// issues.Post call sets them via
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
