// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/dlq"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDLQWriter records calls to Add for test assertions.
type mockDLQWriter struct {
	entries []*dlq.DLQEntry
	err     error // error to return from Add, if any
}

func (m *mockDLQWriter) Add(_ context.Context, entry *dlq.DLQEntry) error {
	m.entries = append(m.entries, entry)
	return m.err
}

func TestDLQEntryRoundTrip(t *testing.T) {
	entry := &dlq.DLQEntry{
		FailedAt:                time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
		FailureError:            "Post https://api.github.com: context deadline exceeded",
		PackageName:             "roachtest",
		TestName:                "kv/splits",
		Labels:                  []string{"O-roachtest", "C-test-failure", "release-blocker"},
		AdoptIssueLabelMatchSet: []string{"X-infra-flake", "B-coverage-enabled"},
		TopLevelNotes:           []string{"note1"},
		Message:                 "test failed with error xyz",
		ExtraParams:             map[string]string{"arch": "amd64"},
		Artifacts:               "/kv/splits",
		MentionOnCreate:         []string{"@cockroachdb/test-eng"},
		HelpTestName:            "kv/splits",
		HelpClusterName:         "test-cluster-1234",
		HelpCloud:               "gce",
		HelpStart:               time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC),
		HelpEnd:                 time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
		HelpRunID:               "run-abc-123",
		Org:                     "cockroachdb",
		Repo:                    "cockroach",
		SHA:                     "abc123",
		Branch:                  "master",
		BinaryVersion:           "v24.1.0",
		TeamCityBuildTypeID:     "Cockroach_Nightlies_RoachtestNightly",
		TeamCityBuildID:         "12345",
		TeamCityServerURL:       "https://teamcity.example.com",
	}

	data, err := json.Marshal(entry)
	require.NoError(t, err)

	var roundTripped dlq.DLQEntry
	require.NoError(t, json.Unmarshal(data, &roundTripped))
	require.Equal(t, *entry, roundTripped)
}

func TestDLQObjectKey(t *testing.T) {
	entry := &dlq.DLQEntry{
		FailedAt: time.Date(2026, 3, 27, 12, 0, 0, 123456789, time.UTC),
		TestName: "kv/splits",
		Branch:   "master",
	}
	key := dlq.ObjectKey(entry)
	require.Equal(t, "failed/master/20260327/kv_splits-1774612800123456789.json", key)
}

func TestDLQGithubIssues(t *testing.T) {
	reg := makeTestRegistry()
	clusterSpec := reg.MakeClusterSpec(1)

	makeTestImpl := func() *testImpl {
		ti := &testImpl{
			spec: &registry.TestSpec{
				Name:            "test/foo",
				Owner:           registry.OwnerTestEng,
				Cluster:         clusterSpec,
				CockroachBinary: registry.StandardCockroach,
				Run:             func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			},
			start:       time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC),
			end:         time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
			cockroach:   "cockroach",
			cockroachEA: "cockroach-ea",
		}
		ti.ReplaceL(nilLogger())
		return ti
	}

	l := nilLogger()

	issueInfo := &githubIssueInfo{}

	t.Run("writes to DLQ on error", func(t *testing.T) {
		postErr := errors.New("github API unavailable")
		mock := &mockDLQWriter{}

		inner := &githubIssues{
			disable: false,
			issuePoster: func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
				_ issues.PostRequest, _ *issues.Options,
			) (*issues.TestFailureIssue, error) {
				return nil, postErr
			},
			teamLoader: validTeamsFn,
		}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()

		t.Setenv("GITHUB_API_TOKEN", "fake-token")
		t.Setenv("TC_BUILD_BRANCH", "master")
		defaultOpts = issues.DefaultOptionsFromEnv()

		_, retErr := wrapper.MaybePost(ti, issueInfo, l, "test failed", nil)

		require.Error(t, retErr)
		assert.Equal(t, postErr, retErr) // original error returned
		require.Len(t, mock.entries, 1)

		entry := mock.entries[0]
		assert.Equal(t, "roachtest", entry.PackageName)
		assert.Equal(t, "test/foo", entry.TestName)
		assert.Equal(t, "github API unavailable", entry.FailureError)
		assert.Contains(t, entry.Labels, "O-roachtest")
	})

	t.Run("skips DLQ on success", func(t *testing.T) {
		mock := &mockDLQWriter{}
		inner := &githubIssues{
			disable: false,
			issuePoster: func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
				_ issues.PostRequest, _ *issues.Options,
			) (*issues.TestFailureIssue, error) {
				return &issues.TestFailureIssue{}, nil
			},
			teamLoader: validTeamsFn,
		}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()

		t.Setenv("GITHUB_API_TOKEN", "fake-token")
		t.Setenv("TC_BUILD_BRANCH", "master")
		defaultOpts = issues.DefaultOptionsFromEnv()

		_, retErr := wrapper.MaybePost(ti, issueInfo, l, "test failed", nil)

		require.NoError(t, retErr)
		require.Empty(t, mock.entries)
	})

	t.Run("skips DLQ when posting is skipped", func(t *testing.T) {
		mock := &mockDLQWriter{}
		inner := &githubIssues{disable: true}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()

		_, retErr := wrapper.MaybePost(ti, issueInfo, l, "test failed", nil)

		require.NoError(t, retErr)
		require.Empty(t, mock.entries)
	})

	t.Run("returns original error when DLQ write fails", func(t *testing.T) {
		postErr := errors.New("github API unavailable")
		mock := &mockDLQWriter{err: errors.New("GCS unavailable")}

		inner := &githubIssues{
			disable: false,
			issuePoster: func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
				_ issues.PostRequest, _ *issues.Options,
			) (*issues.TestFailureIssue, error) {
				return nil, postErr
			},
			teamLoader: validTeamsFn,
		}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()

		t.Setenv("GITHUB_API_TOKEN", "fake-token")
		t.Setenv("TC_BUILD_BRANCH", "master")
		defaultOpts = issues.DefaultOptionsFromEnv()

		_, retErr := wrapper.MaybePost(ti, issueInfo, l, "test failed", nil)

		// Original error is returned even when DLQ write fails.
		require.Error(t, retErr)
		assert.Equal(t, postErr, retErr)
		// DLQ write was attempted.
		require.Len(t, mock.entries, 1)
	})

	t.Run("falls back to raw inputs when createPostRequest fails", func(t *testing.T) {
		postErr := errors.New("github API unavailable")
		mock := &mockDLQWriter{}

		inner := &githubIssues{
			disable: false,
			issuePoster: func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
				_ issues.PostRequest, _ *issues.Options,
			) (*issues.TestFailureIssue, error) {
				return nil, postErr
			},
			teamLoader: invalidTeamsFn, // causes createPostRequest to fail
		}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()

		t.Setenv("GITHUB_API_TOKEN", "fake-token")
		t.Setenv("TC_BUILD_BRANCH", "master")
		defaultOpts = issues.DefaultOptionsFromEnv()

		_, retErr := wrapper.MaybePost(ti, issueInfo, l, "test output msg", nil)

		require.Error(t, retErr)
		require.Len(t, mock.entries, 1)

		entry := mock.entries[0]
		// Fallback values used.
		assert.Equal(t, "roachtest", entry.PackageName)
		assert.Equal(t, "test/foo", entry.TestName)
		assert.Equal(t, "test output msg", entry.Message)
		assert.Equal(t, "/test/foo", entry.Artifacts)
	})

	t.Run("captures HelpCommand inputs", func(t *testing.T) {
		postErr := errors.New("github API unavailable")
		mock := &mockDLQWriter{}

		inner := &githubIssues{
			disable: false,
			issuePoster: func(_ context.Context, _ issues.Logger, _ issues.IssueFormatter,
				_ issues.PostRequest, _ *issues.Options,
			) (*issues.TestFailureIssue, error) {
				return nil, postErr
			},
			teamLoader: validTeamsFn,
		}

		wrapper := &dlqGithubIssues{inner: inner, writer: mock}
		ti := makeTestImpl()
		clusterInfo := &githubIssueInfo{
			cluster: &clusterImpl{name: "my-cluster"},
		}

		t.Setenv("GITHUB_API_TOKEN", "fake-token")
		t.Setenv("TC_BUILD_BRANCH", "master")
		defaultOpts = issues.DefaultOptionsFromEnv()
		oldCloud := roachtestflags.Cloud
		roachtestflags.Cloud = spec.GCE
		defer func() { roachtestflags.Cloud = oldCloud }()

		_, _ = wrapper.MaybePost(ti, clusterInfo, l, "test failed", nil)

		require.Len(t, mock.entries, 1)
		entry := mock.entries[0]
		assert.Equal(t, "test/foo", entry.HelpTestName)
		assert.Equal(t, "my-cluster", entry.HelpClusterName)
		assert.Equal(t, "gce", entry.HelpCloud)
		assert.Equal(t, ti.start, entry.HelpStart)
		assert.Equal(t, ti.end, entry.HelpEnd)
	})
}
