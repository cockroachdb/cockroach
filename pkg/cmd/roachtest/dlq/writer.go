// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// PostFunc is the signature of issues.Post and the issuePoster field on
// roachtest's *githubIssues struct. Defining it here lets the DLQ wrapper
// produce a value that can be assigned back to that field.
type PostFunc = func(
	ctx context.Context, l issues.Logger, formatter issues.IssueFormatter,
	req issues.PostRequest, opts *issues.Options,
) (*issues.TestFailureIssue, error)

// Logger is the minimal logging surface the DLQ writer needs. It matches
// the methods we use on roachtest's *logger.Logger; defining a local
// interface keeps this package free of the roachprod logger dependency.
type Logger interface {
	Printf(format string, args ...interface{})
}

// persistFunc writes a single DLQ entry to durable storage. Extracted
// as a function type so tests can swap in an in-memory implementation
// without standing up a real GCS client.
type persistFunc func(ctx context.Context, entry *Entry) error

// WrapIssuePoster returns a PostFunc that delegates to inner and, on
// error, persists the failed request to a GCS bucket so it can be
// replayed later. The returned PostFunc always returns the original
// inner result; DLQ write failures are logged but not propagated, since
// callers already handle the underlying post failure.
//
// The returned PostFunc is safe for concurrent use.
func WrapIssuePoster(
	ctx context.Context, l Logger, bucket string, inner PostFunc,
) (PostFunc, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "creating GCS client")
	}
	b := client.Bucket(bucket)
	persist := func(ctx context.Context, entry *Entry) error {
		return writeEntry(ctx, b, entry)
	}
	return wrapWithPersist(l, inner, persist), nil
}

// wrapWithPersist composes inner with a DLQ persist step. Internal
// helper used by WrapIssuePoster and exercised directly by tests.
func wrapWithPersist(l Logger, inner PostFunc, persist persistFunc) PostFunc {
	return func(
		ctx context.Context, ll issues.Logger, fmtr issues.IssueFormatter,
		req issues.PostRequest, opts *issues.Options,
	) (*issues.TestFailureIssue, error) {
		result, err := inner(ctx, ll, fmtr, req, opts)
		if err == nil {
			return result, nil
		}
		entry := buildEntry(req, opts, err)
		if dlqErr := persist(ctx, entry); dlqErr != nil {
			l.Printf("DLQ: failed to persist failed post for %s: %v", req.TestName, dlqErr)
		} else {
			l.Printf("DLQ: persisted failed post for %s", req.TestName)
		}
		return result, err
	}
}

// buildEntry serializes the contents of a PostRequest and Options into
// an Entry, pre-rendering HelpCommand into a string so it survives JSON
// round-tripping.
func buildEntry(req issues.PostRequest, opts *issues.Options, postErr error) *Entry {
	entry := &Entry{
		FailedAt:                timeutil.Now(),
		FailureError:            postErr.Error(),
		PackageName:             req.PackageName,
		TestName:                req.TestName,
		Labels:                  req.Labels,
		AdoptIssueLabelMatchSet: req.AdoptIssueLabelMatchSet,
		TopLevelNotes:           req.TopLevelNotes,
		Message:                 req.Message,
		ExtraParams:             req.ExtraParams,
		Artifacts:               req.Artifacts,
		MentionOnCreate:         req.MentionOnCreate,
		RenderedHelpCommand:     renderHelpCommand(req.HelpCommand),
	}
	if opts != nil {
		entry.Org = opts.Org
		entry.Repo = opts.Repo
		entry.SHA = opts.SHA
		entry.Branch = opts.Branch
		if opts.GetBinaryVersion != nil {
			entry.BinaryVersion = opts.GetBinaryVersion()
		}
		if opts.TeamCityOptions != nil {
			entry.TeamCityBuildTypeID = opts.TeamCityOptions.BuildTypeID
			entry.TeamCityBuildID = opts.TeamCityOptions.BuildID
			entry.TeamCityServerURL = opts.TeamCityOptions.ServerURL
			entry.TeamCityTags = opts.TeamCityOptions.Tags
			entry.TeamCityGoflags = opts.TeamCityOptions.Goflags
		}
	}
	return entry
}

// renderHelpCommand runs the closure against a fresh Renderer and
// returns the resulting markdown/HTML string. Returns "" when the
// closure is nil.
func renderHelpCommand(fn func(*issues.Renderer)) string {
	if fn == nil {
		return ""
	}
	var r issues.Renderer
	fn(&r)
	return r.String()
}

// writeEntry serializes the entry as JSON and uploads it to the bucket
// at ObjectKey(entry).
func writeEntry(ctx context.Context, bucket *storage.BucketHandle, entry *Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return errors.Wrap(err, "marshaling DLQ entry")
	}
	w := bucket.Object(ObjectKey(entry)).NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return errors.Wrap(err, "writing DLQ entry to GCS")
	}
	return errors.Wrap(w.Close(), "closing GCS writer")
}
