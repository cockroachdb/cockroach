// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/dlq"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DLQWriter persists failed GitHub issue post requests for later replay.
type DLQWriter interface {
	Add(ctx context.Context, entry *dlq.DLQEntry) error
}

// gcsDLQWriter writes DLQ entries to a GCS bucket.
type gcsDLQWriter struct {
	bucket *storage.BucketHandle
}

func newGCSDLQWriter(ctx context.Context, bucket string) (*gcsDLQWriter, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "creating GCS client")
	}
	return &gcsDLQWriter{bucket: client.Bucket(bucket)}, nil
}

func (w *gcsDLQWriter) Add(ctx context.Context, entry *dlq.DLQEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return errors.Wrap(err, "marshaling DLQ entry")
	}
	key := dlq.ObjectKey(entry)
	obj := w.bucket.Object(key)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return errors.Wrap(err, "writing DLQ entry to GCS")
	}
	return errors.Wrap(writer.Close(), "closing GCS writer")
}

// dlqGithubIssues wraps a *githubIssues and writes failed posts to a
// DLQ writer. It implements GithubPoster.
type dlqGithubIssues struct {
	inner  *githubIssues
	writer DLQWriter
}

func (d *dlqGithubIssues) MaybePost(
	t *testImpl,
	issueInfo *githubIssueInfo,
	l *logger.Logger,
	message string,
	params map[string]string,
) (*issues.TestFailureIssue, error) {
	result, err := d.inner.MaybePost(t, issueInfo, l, message, params)
	if err != nil {
		entry := d.buildDLQEntry(t, issueInfo, l, message, params, err)
		if dlqErr := d.writer.Add(context.Background(), entry); dlqErr != nil {
			l.Printf("failed to write DLQ entry: %v", dlqErr)
		} else {
			l.Printf("wrote failed GitHub issue post to DLQ for test %s", t.Name())
		}
	}
	return result, err
}

func (d *dlqGithubIssues) buildDLQEntry(
	t *testImpl,
	issueInfo *githubIssueInfo,
	l *logger.Logger,
	message string,
	params map[string]string,
	postErr error,
) *dlq.DLQEntry {
	// Try to build PostRequest to get computed fields (labels, mentions, etc.).
	req, reqErr := d.inner.createPostRequest(
		t.Name(), t.start, t.end, t.spec, t.failures(), message,
		roachtestutil.UsingRuntimeAssertions(t), t.goCoverEnabled,
		params, issueInfo,
	)
	opts := issues.DefaultOptionsFromEnv()

	clusterName := ""
	if issueInfo.cluster != nil {
		clusterName = issueInfo.cluster.name
	}

	entry := &dlq.DLQEntry{
		FailedAt:        timeutil.Now(),
		FailureError:    postErr.Error(),
		HelpTestName:    t.Name(),
		HelpClusterName: clusterName,
		HelpCloud:       roachtestflags.Cloud.String(),
		HelpStart:       t.start,
		HelpEnd:         t.end,
		HelpRunID:       runID,
		Org:             opts.Org,
		Repo:            opts.Repo,
		SHA:             opts.SHA,
		Branch:          opts.Branch,
		BinaryVersion:   opts.GetBinaryVersion(),
	}

	if opts.TeamCityOptions != nil {
		entry.TeamCityBuildTypeID = opts.TeamCityOptions.BuildTypeID
		entry.TeamCityBuildID = opts.TeamCityOptions.BuildID
		entry.TeamCityServerURL = opts.TeamCityOptions.ServerURL
		entry.TeamCityTags = opts.TeamCityOptions.Tags
		entry.TeamCityGoflags = opts.TeamCityOptions.Goflags
	}

	if reqErr == nil {
		entry.PackageName = req.PackageName
		entry.TestName = req.TestName
		entry.Labels = req.Labels
		entry.AdoptIssueLabelMatchSet = req.AdoptIssueLabelMatchSet
		entry.TopLevelNotes = req.TopLevelNotes
		entry.Message = req.Message
		entry.ExtraParams = req.ExtraParams
		entry.Artifacts = req.Artifacts
		entry.MentionOnCreate = req.MentionOnCreate
	} else {
		// Fallback: use raw inputs when createPostRequest fails.
		l.Printf("DLQ: createPostRequest failed (%v), using raw inputs", reqErr)
		entry.PackageName = "roachtest"
		entry.TestName = t.Name()
		entry.Message = message
		entry.ExtraParams = params
		entry.Artifacts = fmt.Sprintf("/%s", t.Name())
	}
	return entry
}
