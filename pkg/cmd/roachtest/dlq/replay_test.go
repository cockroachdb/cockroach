// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestReconstruct(t *testing.T) {
	entry := &Entry{
		FailedAt:                time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC),
		PackageName:             "roachtest",
		TestName:                "kv/splits",
		Labels:                  []string{"O-roachtest", "C-test-failure"},
		AdoptIssueLabelMatchSet: []string{"X-infra-flake"},
		TopLevelNotes:           []string{"runtime assertions enabled"},
		Message:                 "boom",
		ExtraParams:             map[string]string{"arch": "amd64"},
		Artifacts:               "/kv/splits",
		MentionOnCreate:         []string{"@cockroachdb/test-eng"},
		RenderedHelpCommand:     "\n\nSee: [README](https://example.com)\n\n",
		Org:                     "cockroachdb",
		Repo:                    "cockroach",
		SHA:                     "abc123",
		Branch:                  "master",
		BinaryVersion:           "v26.2.0",
		TeamCityBuildTypeID:     "TC_BUILD_TYPE",
		TeamCityBuildID:         "999",
		TeamCityServerURL:       "https://tc.example.com",
	}

	req, opts := reconstruct(entry)

	assert.Equal(t, "kv/splits", req.TestName)
	assert.Equal(t, []string{"O-roachtest", "C-test-failure"}, req.Labels)
	assert.Equal(t, "boom", req.Message)
	assert.Equal(t, map[string]string{"arch": "amd64"}, req.ExtraParams)

	require.NotNil(t, req.HelpCommand)
	var r issues.Renderer
	req.HelpCommand(&r)
	assert.Equal(t, entry.RenderedHelpCommand, r.String())

	assert.Equal(t, "cockroachdb", opts.Org)
	assert.Equal(t, "cockroach", opts.Repo)
	assert.Equal(t, "abc123", opts.SHA)
	assert.Equal(t, "master", opts.Branch)
	require.NotNil(t, opts.GetBinaryVersion)
	assert.Equal(t, "v26.2.0", opts.GetBinaryVersion())

	require.NotNil(t, opts.TeamCityOptions)
	assert.Equal(t, "TC_BUILD_TYPE", opts.TeamCityOptions.BuildTypeID)
	assert.Equal(t, "999", opts.TeamCityOptions.BuildID)
}

func TestReconstructNoTeamCity(t *testing.T) {
	// Without TeamCity fields, opts.TeamCityOptions stays nil so the
	// formatter falls back to its default (no TC link section).
	entry := &Entry{
		TestName: "x",
		Org:      "cockroachdb",
		Repo:     "cockroach",
		Branch:   "master",
	}
	_, opts := reconstruct(entry)
	require.Nil(t, opts.TeamCityOptions)
}

func TestProcessEntryClaimAlreadyClaimed(t *testing.T) {
	store := &fakeReplayStore{
		claimErr: &googleapi.Error{Code: http.StatusPreconditionFailed},
	}
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeSkipped, got)
	require.Zero(t, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryClaimFailure(t *testing.T) {
	store := &fakeReplayStore{claimErr: errors.New("permission denied")}
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeFailed, got)
	require.Zero(t, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryAlreadyProcessedCleansUpStaleFailed(t *testing.T) {
	store := &fakeReplayStore{processedExistsValue: true}
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeSkipped, got)
	require.Zero(t, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"delete failed/master/20260526/kv_splits-1.json",
		"delete processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryProcessedCheckFailureReleasesClaim(t *testing.T) {
	store := &fakeReplayStore{processedErr: errors.New("attrs failed")}
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeFailed, got)
	require.Zero(t, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"delete processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryPostFailureReleasesClaim(t *testing.T) {
	store := newFakeReplayStore()
	postCalls := 0

	got := processEntryWithStore(
		context.Background(),
		replayOptionsForTest(errors.New("github unavailable"), &postCalls),
		testFailedName,
		store,
	)

	require.Equal(t, outcomeFailed, got)
	require.Equal(t, 1, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"read processing/master/20260526/kv_splits-1.json",
		"delete processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryPostSuccessArchiveFailureKeepsClaim(t *testing.T) {
	store := newFakeReplayStore()
	store.copyErr = errors.New("copy failed")
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeFailed, got)
	require.Equal(t, 1, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"read processing/master/20260526/kv_splits-1.json",
		"copy processing/master/20260526/kv_splits-1.json -> processed/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryPostSuccessDeleteFailedFailureKeepsClaim(t *testing.T) {
	store := newFakeReplayStore()
	store.deleteErrs[testFailedName] = errors.New("delete failed")
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomeFailed, got)
	require.Equal(t, 1, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"read processing/master/20260526/kv_splits-1.json",
		"copy processing/master/20260526/kv_splits-1.json -> processed/master/20260526/kv_splits-1.json",
		"delete failed/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntryPostSuccessDeleteProcessingFailureIsPosted(t *testing.T) {
	store := newFakeReplayStore()
	store.deleteErrs[testProcessingName] = errors.New("delete processing")
	postCalls := 0

	got := processEntryWithStore(
		context.Background(), replayOptionsForTest(nil, &postCalls), testFailedName, store,
	)

	require.Equal(t, outcomePosted, got)
	require.Equal(t, 1, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"read processing/master/20260526/kv_splits-1.json",
		"copy processing/master/20260526/kv_splits-1.json -> processed/master/20260526/kv_splits-1.json",
		"delete failed/master/20260526/kv_splits-1.json",
		"delete processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestProcessEntrySkipGitHubPostReleasesClaim(t *testing.T) {
	store := newFakeReplayStore()
	postCalls := 0
	opts := replayOptionsForTest(nil, &postCalls)
	opts.SkipGitHubPost = true

	got := processEntryWithStore(
		context.Background(),
		opts,
		testFailedName,
		store,
	)

	require.Equal(t, outcomePosted, got)
	require.Zero(t, postCalls)
	require.Equal(t, []string{
		"claim failed/master/20260526/kv_splits-1.json -> processing/master/20260526/kv_splits-1.json",
		"processed-exists processed/master/20260526/kv_splits-1.json",
		"read processing/master/20260526/kv_splits-1.json",
		"delete processing/master/20260526/kv_splits-1.json",
	}, store.ops)
}

func TestHelpCommandFromStringNilWhenEmpty(t *testing.T) {
	// An empty stored HelpCommand should result in a nil closure so the
	// formatter omits the entire Help section. (PostRequest.HelpCommand
	// being nil is checked by formatter_unit.go to skip the section.)
	require.Nil(t, helpCommandFromString(""))
}

func TestPreviewURL(t *testing.T) {
	entry := &Entry{
		Org:    "test-owner",
		Repo:   "test-repo",
		Branch: "master",
		SHA:    "abc",
	}
	req := issues.PostRequest{
		PackageName: "roachtest",
		TestName:    "kv/splits",
		Message:     "boom",
	}
	got, err := previewURL(entry, req)
	require.NoError(t, err)
	// URL targets the entry's repo and not hardcoded cockroachdb/cockroach.
	assert.Contains(t, got, "https://github.com/test-owner/test-repo/issues/new")
	assert.Contains(t, got, "title=")
	assert.Contains(t, got, "body=")
	assert.Contains(t, got, "template=none")
}

func TestPreviewURLUsesDryRunPlaceholders(t *testing.T) {
	entry := &Entry{
		Org:                 "x",
		Repo:                "y",
		Branch:              "master",
		SHA:                 "abc123",
		TeamCityServerURL:   "https://tc.example.com",
		TeamCityBuildTypeID: "BUILD_TYPE",
		TeamCityBuildID:     "999",
	}
	req := issues.PostRequest{
		PackageName: "roachtest",
		TestName:    "kv/splits",
		Artifacts:   "/kv/splits",
	}
	got, err := previewURL(entry, req)
	require.NoError(t, err)

	u, err := url.Parse(got)
	require.NoError(t, err)
	body := u.Query().Get("body")
	assert.Contains(t, body, "roachtest.kv/splits [failed]() on test_branch @ [test_SHA]():")
	assert.NotContains(t, body, "tc.example.com")
	assert.NotContains(t, body, "abc123")
	assert.NotContains(t, body, "%5Bfailed%5D", "body query value should decode to markdown")
}

func TestPreviewURLAppendsLabelsToBody(t *testing.T) {
	// Mirrors formatPostRequest in pkg/cmd/roachtest/github.go: the
	// "create new issue" form URL doesn't auto-apply labels, so we
	// append them to the body so the operator can see them.
	entry := &Entry{Org: "x", Repo: "y", Branch: "master"}
	req := issues.PostRequest{
		PackageName: "roachtest",
		TestName:    "kv/splits",
		Labels:      []string{"O-roachtest", "C-test-failure"},
	}
	got, err := previewURL(entry, req)
	require.NoError(t, err)
	// Both labels appear in the body section (URL-encoded).
	assert.Contains(t, got, "Labels%3A")
	assert.Contains(t, got, "%3Ccode%3EO-roachtest%3C%2Fcode%3E")
	assert.Contains(t, got, "%3Ccode%3EC-test-failure%3C%2Fcode%3E")
}

func TestPreviewURLFallsBackToCockroachdbWhenOrgMissing(t *testing.T) {
	// Entries persisted before org/repo were captured (defensive: the
	// schema requires Org but a malformed entry shouldn't crash).
	entry := &Entry{Branch: "master"}
	got, err := previewURL(entry, issues.PostRequest{TestName: "x"})
	require.NoError(t, err)
	assert.Contains(t, got, "https://github.com/cockroachdb/cockroach/issues/new")
}

func TestHelpCommandFromStringPreservesRawOutput(t *testing.T) {
	stored := "<p>arbitrary <a href=\"x\">html</a></p>"
	fn := helpCommandFromString(stored)
	require.NotNil(t, fn)
	var r issues.Renderer
	fn(&r)
	// Renderer.Raw must not re-escape — the stored string is already
	// rendered HTML/markdown.
	assert.Equal(t, stored, r.String())
}

const (
	testFailedName     = "failed/master/20260526/kv_splits-1.json"
	testProcessingName = "processing/master/20260526/kv_splits-1.json"
)

type fakeReplayStore struct {
	entry                *Entry
	claimErr             error
	processedExistsValue bool
	processedErr         error
	readErr              error
	copyErr              error
	deleteErrs           map[string]error
	ops                  []string
}

func newFakeReplayStore() *fakeReplayStore {
	return &fakeReplayStore{
		entry: &Entry{
			PackageName: "roachtest",
			TestName:    "kv/splits",
			Org:         "cockroachdb",
			Repo:        "cockroach",
			Branch:      "master",
		},
		deleteErrs: map[string]error{},
	}
}

func (f *fakeReplayStore) claim(_ context.Context, failedName, processingName string) error {
	f.ops = append(f.ops, "claim "+failedName+" -> "+processingName)
	return f.claimErr
}

func (f *fakeReplayStore) processedExists(_ context.Context, processedName string) (bool, error) {
	f.ops = append(f.ops, "processed-exists "+processedName)
	return f.processedExistsValue, f.processedErr
}

func (f *fakeReplayStore) read(_ context.Context, name string) (*Entry, error) {
	f.ops = append(f.ops, "read "+name)
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.entry, nil
}

func (f *fakeReplayStore) copy(_ context.Context, srcName, dstName string) error {
	f.ops = append(f.ops, "copy "+srcName+" -> "+dstName)
	return f.copyErr
}

func (f *fakeReplayStore) delete(_ context.Context, name string) error {
	f.ops = append(f.ops, "delete "+name)
	return f.deleteErrs[name]
}

func replayOptionsForTest(postErr error, postCalls *int) ReplayOptions {
	return ReplayOptions{
		Logger: noopLogger{},
		PostFunc: func(
			_ context.Context,
			_ issues.Logger,
			_ issues.IssueFormatter,
			_ issues.PostRequest,
			_ *issues.Options,
		) (*issues.TestFailureIssue, error) {
			*postCalls++
			if postErr != nil {
				return nil, postErr
			}
			return &issues.TestFailureIssue{}, nil
		},
	}
}
