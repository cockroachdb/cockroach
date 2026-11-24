// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger is a Logger that records every Printf call. Used to
// assert that the wrapper logs the right thing without pulling in the
// roachprod logger dependency.
type captureLogger struct {
	lines []string
}

func (c *captureLogger) Printf(format string, args ...interface{}) {
	c.lines = append(c.lines, format)
}

func TestEntryRoundTrip(t *testing.T) {
	entry := &Entry{
		FailedAt:                time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
		FailureError:            "Post https://api.github.com: 502 Bad Gateway",
		PackageName:             "roachtest",
		TestName:                "kv/splits",
		Labels:                  []string{"O-roachtest", "C-test-failure", "release-blocker"},
		AdoptIssueLabelMatchSet: []string{"X-infra-flake"},
		TopLevelNotes:           []string{"runtime assertions enabled"},
		Message:                 "test failed",
		ExtraParams:             map[string]string{"arch": "amd64"},
		Artifacts:               "/kv/splits",
		MentionOnCreate:         []string{"@cockroachdb/test-eng"},
		RenderedHelpCommand:     "\n\nSee: [README](https://example.com)\n\n",
		Org:                     "cockroachdb",
		Repo:                    "cockroach",
		SHA:                     "abc123",
		Branch:                  "master",
		BinaryVersion:           "v26.2.0-alpha",
		TeamCityBuildTypeID:     "Cockroach_Nightlies_RoachtestNightly",
		TeamCityBuildID:         "12345",
		TeamCityServerURL:       "https://teamcity.example.com",
	}

	data, err := json.Marshal(entry)
	require.NoError(t, err)

	var got Entry
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, *entry, got)
}

func TestObjectKey(t *testing.T) {
	entry := &Entry{
		FailedAt: time.Date(2026, 3, 27, 12, 0, 0, 123456789, time.UTC),
		TestName: "kv/splits",
		Branch:   "master",
	}
	require.Equal(t,
		"failed/master/20260327/kv_splits-1774612800123456789.json",
		ObjectKey(entry),
	)
}

func TestRenderHelpCommandRoundTrip(t *testing.T) {
	// Original HelpCommand: a few links and an escaped tagline. Pre-render
	// it, then re-emit via Renderer.Raw and verify the output is identical
	// — this is the mechanism the replay tool relies on.
	original := func(r *issues.Renderer) {
		issues.HelpCommandAsLink("README", "https://example.com/readme")(r)
		issues.HelpCommandAsLink("Grafana", "https://example.com/grafana")(r)
		r.Escaped("\n_some <html> escaped_ note\n")
	}

	// Render once to capture the canonical output.
	var canonical issues.Renderer
	original(&canonical)
	want := canonical.String()

	// Pre-render path: store, then re-emit via Raw at "replay" time.
	stored := renderHelpCommand(original)
	var replayed issues.Renderer
	wrapAsClosure(stored)(&replayed)
	require.Equal(t, want, replayed.String())
}

// wrapAsClosure mirrors what the replay tool does: turn a stored,
// pre-rendered HelpCommand string back into a closure compatible with
// issues.PostRequest.HelpCommand.
func wrapAsClosure(rendered string) func(*issues.Renderer) {
	return func(r *issues.Renderer) {
		if rendered != "" {
			r.Raw(rendered)
		}
	}
}

func TestBuildEntry(t *testing.T) {
	postErr := errors.New("502 Bad Gateway")
	req := issues.PostRequest{
		PackageName:     "roachtest",
		TestName:        "kv/splits",
		Labels:          []string{"O-roachtest"},
		Message:         "boom",
		ExtraParams:     map[string]string{"arch": "amd64"},
		Artifacts:       "/kv/splits",
		MentionOnCreate: []string{"@x"},
		HelpCommand: func(r *issues.Renderer) {
			r.Escaped("hello")
		},
	}
	opts := &issues.Options{
		Org:              "cockroachdb",
		Repo:             "cockroach",
		SHA:              "deadbeef",
		Branch:           "master",
		GetBinaryVersion: func() string { return "v26.2.0" },
		TeamCityOptions: &issues.TeamCityOptions{
			BuildTypeID: "TC_BUILD_TYPE",
			BuildID:     "999",
			ServerURL:   "https://tc.example.com",
		},
	}

	got := buildEntry(req, opts, postErr)

	assert.Equal(t, "502 Bad Gateway", got.FailureError)
	assert.Equal(t, "roachtest", got.PackageName)
	assert.Equal(t, "kv/splits", got.TestName)
	assert.Equal(t, []string{"O-roachtest"}, got.Labels)
	assert.Equal(t, "hello", got.RenderedHelpCommand)
	assert.Equal(t, "cockroachdb", got.Org)
	assert.Equal(t, "v26.2.0", got.BinaryVersion)
	assert.Equal(t, "TC_BUILD_TYPE", got.TeamCityBuildTypeID)
}

func TestWrapIssuePosterPassesThrough(t *testing.T) {
	// On success, the wrapper should return the inner result unmodified,
	// not log anything, and never call persist.
	want := &issues.TestFailureIssue{ID: 42}
	inner := func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
		_ issues.PostRequest, _ *issues.Options,
	) (*issues.TestFailureIssue, error) {
		return want, nil
	}

	var persisted []*Entry
	persist := func(_ context.Context, e *Entry) error {
		persisted = append(persisted, e)
		return nil
	}
	logger := &captureLogger{}
	wrapped := wrapWithPersist(logger, inner, persist)

	got, err := wrapped(context.Background(), nil, nil, issues.PostRequest{}, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Empty(t, persisted, "should not persist on success")
	require.Empty(t, logger.lines, "should not log on success")
}

func TestWrapIssuePosterPersistsOnError(t *testing.T) {
	innerErr := errors.New("502 Bad Gateway")
	inner := func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
		_ issues.PostRequest, _ *issues.Options,
	) (*issues.TestFailureIssue, error) {
		return nil, innerErr
	}

	var persisted []*Entry
	persist := func(_ context.Context, e *Entry) error {
		persisted = append(persisted, e)
		return nil
	}
	logger := &captureLogger{}
	wrapped := wrapWithPersist(logger, inner, persist)

	req := issues.PostRequest{TestName: "kv/splits"}
	opts := &issues.Options{Branch: "master"}
	_, err := wrapped(context.Background(), nil, nil, req, opts)

	require.ErrorIs(t, err, innerErr)
	require.Len(t, persisted, 1)
	assert.Equal(t, "kv/splits", persisted[0].TestName)
	assert.Equal(t, "502 Bad Gateway", persisted[0].FailureError)
	assert.Equal(t, "master", persisted[0].Branch)
	require.Len(t, logger.lines, 1)
	assert.Equal(t, "DLQ: persisted failed post for %s", logger.lines[0])
}

func TestWrapIssuePosterPropagatesInnerErrorEvenIfPersistFails(t *testing.T) {
	// We never want a DLQ persistence failure to mask the original post
	// error — callers track that in r.numGithubPostErrs and use it for
	// alerting.
	innerErr := errors.New("502 Bad Gateway")
	inner := func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
		_ issues.PostRequest, _ *issues.Options,
	) (*issues.TestFailureIssue, error) {
		return nil, innerErr
	}
	persistErr := errors.New("GCS unavailable")
	persist := func(_ context.Context, _ *Entry) error { return persistErr }
	logger := &captureLogger{}
	wrapped := wrapWithPersist(logger, inner, persist)

	_, err := wrapped(context.Background(), nil, nil,
		issues.PostRequest{TestName: "x"}, &issues.Options{})

	require.ErrorIs(t, err, innerErr)
	require.NotErrorIs(t, err, persistErr)
	require.Len(t, logger.lines, 1)
	assert.Equal(t, "DLQ: failed to persist failed post for %s: %v", logger.lines[0])
}
