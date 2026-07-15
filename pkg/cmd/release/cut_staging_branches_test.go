// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/version"
	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func mustParseVersion(t *testing.T, s string) version.Version {
	t.Helper()
	v, err := version.Parse(s)
	require.NoError(t, err)
	return v
}

func TestParseReleaseVersion(t *testing.T) {
	tests := []struct {
		name            string
		summary         string
		expectedVersion string
		expectedErr     string
	}{
		{name: "patch release", summary: "Release: v25.3.1", expectedVersion: "v25.3.1"},
		{name: "trailing text", summary: "Release: v25.3.1 some other text", expectedVersion: "v25.3.1"},
		{name: "alpha pre-release", summary: "Release: v25.3.1-alpha.1", expectedVersion: "v25.3.1-alpha.1"},
		{name: "hotfix marker ignored", summary: "Release: v25.2.2 (hot fix)", expectedVersion: "v25.2.2"},
		{name: "missing prefix", summary: "v25.3.1", expectedErr: "summary does not start with"},
		{name: "garbage version", summary: "Release: vfoo.bar", expectedErr: "parsing version"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := parseReleaseVersion(tc.summary)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedVersion, v.String())
		})
	}
}

func TestDeriveBranchNames(t *testing.T) {
	tests := []struct {
		name              string
		version           string
		hotfix            bool
		expectedBase      string
		expectedBaseIsTag bool
		expectedStaging   string
	}{
		{name: "patch release", version: "v25.4.3", expectedBase: "release-25.4", expectedStaging: "release-25.4.3-rc"},
		{name: "alpha pre-release", version: "v25.3.1-alpha.1", expectedBase: "release-25.3", expectedStaging: "release-25.3.1-rc"},
		{name: "first rc of major release", version: "v26.3.0-rc.1", expectedBase: "release-26.3", expectedStaging: "release-26.3.0-rc"},
		{name: "hotfix branches from previous patch tag", version: "v25.2.2", hotfix: true,
			expectedBase: "v25.2.1", expectedBaseIsTag: true, expectedStaging: "staging-v25.2.2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := deriveBranchNames(mustParseVersion(t, tc.version), tc.hotfix)
			require.Equal(t, tc.expectedBase, b.base)
			require.Equal(t, tc.expectedBaseIsTag, b.baseIsTag)
			require.Equal(t, tc.expectedStaging, b.staging)
		})
	}
}

func TestIsFirstMajorRC(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		expected bool
	}{
		{name: "first rc of major release", version: "v26.3.0-rc.1", expected: true},
		{name: "second rc", version: "v26.3.0-rc.2", expected: false},
		{name: "alpha", version: "v26.3.0-alpha.1", expected: false},
		{name: "beta", version: "v26.3.0-beta.1", expected: false},
		{name: "final major release", version: "v26.3.0", expected: false},
		{name: "first rc of a patch release", version: "v26.3.1-rc.1", expected: false},
		{name: "cloudonly sub-phase of first rc", version: "v26.3.0-rc.1-cloudonly.2", expected: false},
		{name: "adhoc build of first rc", version: "v26.3.0-rc.1-5-gabcdef1234", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, isFirstMajorRC(mustParseVersion(t, tc.version)))
		})
	}
}

func TestIsHotfix(t *testing.T) {
	tests := []struct {
		name     string
		summary  string
		expected bool
	}{
		{name: "normal patch", summary: "Release: v25.2.20", expected: false},
		{name: "hot fix marker", summary: "Release: v25.2.2 (hot fix)", expected: true},
		{name: "unspaced hotfix marker", summary: "Release: v25.2.2 (hotfix)", expected: true},
		{name: "uppercase marker", summary: "Release: v25.2.2 (HOT FIX)", expected: true},
		{name: "pre-release", summary: "Release: v25.3.1-alpha.1", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, isHotfix(tc.summary))
		})
	}
}

func TestIsCutDateReady(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		cutDate       string
		expectedReady bool
		expectedErr   string
	}{
		{name: "yesterday is ready", cutDate: "2026-04-19", expectedReady: true},
		{name: "today is ready", cutDate: "2026-04-20", expectedReady: true},
		{name: "tomorrow not ready", cutDate: "2026-04-21", expectedReady: false},
		{name: "empty not ready", cutDate: "", expectedReady: false},
		{name: "garbage rejected", cutDate: "not-a-date", expectedErr: "parsing cut date"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, err := isCutDateReady(tc.cutDate, today)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedReady, ready)
		})
	}
}

func TestReleaseTypeFor(t *testing.T) {
	tests := []struct {
		name         string
		version      string
		jiraOverride string
		expected     string
	}{
		{name: "scheduled patch", version: "v25.4.3", expected: scheduledType},
		{name: "pre-release derived", version: "v25.3.1-alpha.1", expected: preReleaseType},
		{name: "first rc of major release", version: "v26.3.0-rc.1", expected: preReleaseType},
		{name: "patch zero is major", version: "v25.4.0", expected: majorReleaseType},
		{name: "override beats heuristic", version: "v25.4.3", jiraOverride: "Custom", expected: "Custom"},
		{name: "override on pre-release", version: "v25.3.1-alpha.1", jiraOverride: "Pre-Release", expected: "Pre-Release"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := releaseTypeFor(mustParseVersion(t, tc.version), tc.jiraOverride)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestIsPreRelease(t *testing.T) {
	tests := []struct {
		name        string
		releaseType string
		expected    bool
	}{
		{name: "exact match", releaseType: "Pre-Release", expected: true},
		{name: "lowercase", releaseType: "pre-release", expected: true},
		{name: "no hyphen", releaseType: "PreRelease", expected: true},
		{name: "scheduled", releaseType: "Scheduled", expected: false},
		{name: "major", releaseType: "Major Release", expected: false},
		{name: "empty", releaseType: "", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, isPreRelease(tc.releaseType))
		})
	}
}

func TestFormatJiraDate(t *testing.T) {
	tests := []struct {
		name     string
		jiraDate string
		layout   string
		expected string
	}{
		{name: "pretty weekday", jiraDate: "2026-04-22", layout: "Monday, 01/02", expected: "Wednesday, 04/22"},
		{name: "month/day", jiraDate: "2026-04-22", layout: "01/02", expected: "04/22"},
		{name: "empty input", jiraDate: "", layout: "01/02", expected: ""},
		{name: "garbage passes through", jiraDate: "not-a-date", layout: "01/02", expected: "not-a-date"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, formatJiraDate(tc.jiraDate, tc.layout))
		})
	}
}

func TestBuildReleaseDetails(t *testing.T) {
	// Build a Jira issue with the custom fields the function reads.
	rawFields, err := json.Marshal(map[string]interface{}{
		"summary":           "Release: v25.4.3",
		cfPickSHADate:       "2026-04-22",
		cfCloudReleaseNotes: "2026-04-23",
		cfPublishBinary:     "2026-04-24",
	})
	require.NoError(t, err)
	var issue jiraIssue
	require.NoError(t, json.Unmarshal([]byte(`{"key":"REL-1234","fields":`+string(rawFields)+`}`), &issue))

	v := mustParseVersion(t, "v25.4.3")
	b := deriveBranchNames(v, false)
	now := time.Date(2026, 4, 20, 7, 0, 0, 0, time.UTC)
	d, err := buildReleaseDetails(v, &issue, b, "abc1234", now, "cockroachdb/cockroach")
	require.NoError(t, err)

	require.Equal(t, "v25.4.3", d.Release)
	require.Equal(t, "v25.4.3", d.FinalRelease)
	require.Equal(t, "25.4", d.Series)
	require.Equal(t, "release-25.4.3-rc", d.StagingBranch)
	require.Equal(t, "release-25.4", d.BaseBranch)
	require.Equal(t, "abc1234", d.CutAtSHA)
	require.Equal(t, "https://github.com/cockroachdb/cockroach/commit/abc1234", d.CutAtCommitURL)
	require.Equal(t, "Wednesday, 04/22", d.PickSHADate)
	require.Equal(t, "Thursday, 04/23", d.CloudReleaseNotesDate)
	require.Equal(t, "Friday, 04/24", d.PublishBinaryDate)
	require.Equal(t, "04/22: pick SHA", d.ReleaseStatus)
	require.Equal(t, "backport-25.4.3-rc", d.BackportLabel)
	require.Equal(t, "Wednesday, 04/22: release-25.4.3-rc will be frozen", d.BackportLabelDescription)
	require.Equal(t, scheduledType, d.ReleaseType)
}

// TestBuildReleaseDetailsFirstRC pins the details derived for the first RC of a
// major release (vX.Y.0-rc.1): FinalRelease drops the -rc.1 suffix, the
// backport label targets the release-X.Y.0-rc branch, and the release type is
// pre-release so the RC/final-.0 Slack and Jira templates fire.
func TestBuildReleaseDetailsFirstRC(t *testing.T) {
	rawFields, err := json.Marshal(map[string]interface{}{
		"summary":           "Release: v26.3.0-rc.1",
		cfPickSHADate:       "2026-04-22",
		cfCloudReleaseNotes: "2026-04-23",
		cfPublishBinary:     "2026-04-24",
	})
	require.NoError(t, err)
	var issue jiraIssue
	require.NoError(t, json.Unmarshal([]byte(`{"key":"REL-1234","fields":`+string(rawFields)+`}`), &issue))

	v := mustParseVersion(t, "v26.3.0-rc.1")
	b := deriveBranchNames(v, false)
	now := time.Date(2026, 4, 20, 7, 0, 0, 0, time.UTC)
	d, err := buildReleaseDetails(v, &issue, b, "abc1234", now, "cockroachdb/cockroach")
	require.NoError(t, err)

	require.Equal(t, "v26.3.0-rc.1", d.Release)
	require.Equal(t, "v26.3.0", d.FinalRelease)
	require.Equal(t, "26.3", d.Series)
	require.Equal(t, "release-26.3.0-rc", d.StagingBranch)
	require.Equal(t, "release-26.3", d.BaseBranch)
	require.Equal(t, "backport-26.3.0-rc", d.BackportLabel)
	require.Equal(t, preReleaseType, d.ReleaseType)
}

func TestBuildSlackMessage(t *testing.T) {
	// CutAtCommitURL uses a placeholder owner/repo so we can prove the template
	// substitutes the field rather than a hardcoded cockroachdb/cockroach URL.
	d := releaseDetails{
		Release:               "v25.4.3",
		FinalRelease:          "v25.4.3",
		Series:                "25.4",
		StagingBranch:         "release-25.4.3-rc",
		BaseBranch:            "release-25.4",
		CutAtSHA:              "abc1234",
		CutAtCommitURL:        "https://github.com/example-org/example-repo/commit/abc1234",
		BranchCutDateTime:     "2026-04-20 07:00 UTC",
		PickSHADate:           "Wednesday, 04/22",
		CloudReleaseNotesDate: "Thursday, 04/23",
		PublishBinaryDate:     "Friday, 04/24",
		ReleaseType:           scheduledType,
	}

	t.Run("scheduled template", func(t *testing.T) {
		msg := buildSlackMessage(d, false)
		require.Contains(t, msg, "`v25.4.3` staging branch `release-25.4.3-rc` has been cut")
		require.Contains(t, msg, "*Pick SHA (Branch Freeze) Date:* `Wednesday, 04/22`")
		require.Contains(t, msg, "<https://github.com/example-org/example-repo/commit/abc1234|abc1234>")
		require.False(t, strings.HasPrefix(msg, "[DRY RUN] "))
	})

	t.Run("dry-run prefix", func(t *testing.T) {
		msg := buildSlackMessage(d, true)
		require.True(t, strings.HasPrefix(msg, "[DRY RUN] "))
	})

	t.Run("pre-release template", func(t *testing.T) {
		pd := d
		pd.Release = "v25.3.1-alpha.1"
		pd.FinalRelease = "v25.3.1"
		pd.Series = "25.3"
		pd.StagingBranch = "release-25.3.1-rc"
		pd.ReleaseType = preReleaseType
		msg := buildSlackMessage(pd, false)
		require.Contains(t, msg, "Staging branch `release-25.3.1-rc` has now been cut and frozen for the RC and final .0")
		require.Contains(t, msg, "Database Backport Policy")
		require.Contains(t, msg, "If your backport needs to go into `v25.3.1`")
	})
}

func TestFindCutSubtask(t *testing.T) {
	mkIssue := func(subtasks ...jiraSubtask) *jiraIssue {
		return &jiraIssue{Fields: jiraIssueFields{Subtasks: subtasks}}
	}
	mkSubtask := func(key, summary, status string) jiraSubtask {
		st := jiraSubtask{Key: key}
		st.Fields.Summary = summary
		st.Fields.Status.Name = status
		return st
	}

	tests := []struct {
		name        string
		issue       *jiraIssue
		expectedKey string
	}{
		{
			name: "matches case insensitively",
			issue: mkIssue(
				mkSubtask("REL-2", "Cut Staging Branch", "To Do"),
			),
			expectedKey: "REL-2",
		},
		{
			name: "skips already-done subtask",
			issue: mkIssue(
				mkSubtask("REL-2", "Cut Staging Branch", "Done"),
			),
			expectedKey: "",
		},
		{
			name: "no match returns empty",
			issue: mkIssue(
				mkSubtask("REL-2", "Some unrelated work", "To Do"),
			),
			expectedKey: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedKey, findCutSubtask(tc.issue))
		})
	}
}

func TestIsProductionRepo(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected bool
	}{
		{name: "empty is non-prod", envValue: "", expected: false},
		{name: "exact true is prod", envValue: "true", expected: true},
		{name: "uppercase TRUE is non-prod", envValue: "TRUE", expected: false},
		{name: "anything else is non-prod", envValue: "1", expected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envIsProductionRepo, tc.envValue)
			require.Equal(t, tc.expected, isProductionRepo())
		})
	}
}

// newGitHubClientForTest returns a *githubClient whose underlying go-github
// HTTP client is redirected at the supplied test server. The caller's handler
// owns rewriting paths and producing JSON responses; this helper only handles
// the BaseURL plumbing (go-github requires a trailing slash).
func newGitHubClientForTest(t *testing.T, srv *httptest.Server) *githubClient {
	t.Helper()
	gh := newGitHubClient("test-token", "owner", "repo")
	u, err := url.Parse(srv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u
	return gh
}

// mkRefHandler returns a handler that serves /repos/{owner}/{repo}/git/ref/heads/{branch}
// from a name->SHA map; missing branches respond 404 so callers can exercise
// errBranchNotFound without bespoke handler code per case. Note that the
// upstream GitHub REST endpoint for fetching a single ref uses the singular
// "ref" — go-github v61 calls `git/ref/heads/...`.
func mkRefHandler(refs map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/git/ref/heads/"
		i := strings.Index(r.URL.Path, prefix)
		if i < 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		branch := r.URL.Path[i+len(prefix):]
		sha, ok := refs[branch]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ref":"refs/heads/%s","object":{"sha":%q}}`, branch, sha)
	}
}

// mkJiraIssue builds a *jiraIssue from a summary and a fields map. The fields
// map is merged with the summary at the JSON level so the custom UnmarshalJSON
// populates RawFields correctly.
func mkJiraIssue(t *testing.T, key, summary string, fields map[string]interface{}) *jiraIssue {
	t.Helper()
	all := make(map[string]interface{}, len(fields)+1)
	for k, v := range fields {
		all[k] = v
	}
	all["summary"] = summary
	raw, err := json.Marshal(all)
	require.NoError(t, err)
	var issue jiraIssue
	require.NoError(t, json.Unmarshal(
		[]byte(`{"key":`+strconv.Quote(key)+`,"fields":`+string(raw)+`}`), &issue))
	return &issue
}

func TestCutRunnerValidate(t *testing.T) {
	bn := branchNames{base: "release-25.4", staging: "release-25.4.3-rc"}
	const baseSHA = "basesha000"
	const stagingSHA = "stagingsha111"

	tests := []struct {
		name               string
		jiraStaging        string            // value of cfStagingBranch on the issue
		ghRefs             map[string]string // branches that exist on GitHub
		dryRun             bool
		expectedSHA        string
		expectedAlreadyCut bool
		expectedErr        string
	}{
		{
			name:        "happy path: nothing exists yet, returns base SHA",
			ghRefs:      map[string]string{"release-25.4": baseSHA},
			expectedSHA: baseSHA,
		},
		{
			name:               "idempotent recovery: branch on GitHub and Jira matches",
			jiraStaging:        "release-25.4.3-rc",
			ghRefs:             map[string]string{"release-25.4.3-rc": stagingSHA, "release-25.4": baseSHA},
			expectedSHA:        stagingSHA,
			expectedAlreadyCut: true,
		},
		{
			name:        "branch on GitHub but Jira empty surfaces conflict",
			ghRefs:      map[string]string{"release-25.4.3-rc": stagingSHA},
			expectedErr: "already exists on GitHub but cfStagingBranch in Jira is",
		},
		{
			name:        "branch on GitHub but Jira holds a different branch",
			jiraStaging: "release-25.4.4-rc",
			ghRefs:      map[string]string{"release-25.4.3-rc": stagingSHA},
			expectedErr: "already exists on GitHub but cfStagingBranch in Jira is",
		},
		{
			name:        "Jira already names a staging branch but it isn't on GitHub",
			jiraStaging: "release-25.4.3-rc",
			ghRefs:      map[string]string{"release-25.4": baseSHA},
			expectedErr: "but no such branch exists on GitHub",
		},
		{
			name:        "dry-run lets stale Jira pass through",
			jiraStaging: "release-25.4.3-rc",
			ghRefs:      map[string]string{"release-25.4": baseSHA},
			dryRun:      true,
			expectedSHA: baseSHA,
		},
		{
			name:        "missing base branch surfaces user-readable error",
			ghRefs:      map[string]string{},
			expectedErr: "base branch release-25.4 does not exist on GitHub",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(mkRefHandler(tc.ghRefs))
			defer srv.Close()
			r := &cutRunner{
				gh:     newGitHubClientForTest(t, srv),
				dryRun: tc.dryRun,
			}
			fields := map[string]interface{}{}
			if tc.jiraStaging != "" {
				fields[cfStagingBranch] = tc.jiraStaging
			}
			issue := mkJiraIssue(t, "REL-1", "Release: v25.4.3", fields)
			sha, alreadyCut, err := r.validate(context.Background(), issue, bn)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedSHA, sha)
			require.Equal(t, tc.expectedAlreadyCut, alreadyCut)
		})
	}
}

// TestCutRunnerValidateHotfix covers the hotfix branch of validate, where the
// base ref is a release tag resolved through the Commits API rather than a
// branch tip. mkRefHandler only serves heads refs, so this test stands a
// dedicated server that also answers the commits/{ref} SHA lookup.
func TestCutRunnerValidateHotfix(t *testing.T) {
	bn := branchNames{base: "v25.2.1", baseIsTag: true, staging: "staging-v25.2.2"}
	const tagSHA = "tagsha000deadbeef"

	tests := []struct {
		name        string
		tagExists   bool
		expectedSHA string
		expectedErr string
	}{
		{name: "branches from previous patch tag", tagExists: true, expectedSHA: tagSHA},
		{name: "missing base tag surfaces user-readable error", tagExists: false,
			expectedErr: "base tag v25.2.1 does not exist on GitHub"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				// Staging branch never exists yet for a fresh hotfix cut.
				case strings.Contains(r.URL.Path, "/git/ref/heads/"):
					w.WriteHeader(http.StatusNotFound)
				// GetCommitSHA1 returns the SHA as the raw response body.
				case strings.HasSuffix(r.URL.Path, "/commits/refs/tags/v25.2.1"):
					if !tc.tagExists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_, _ = w.Write([]byte(tagSHA))
				default:
					t.Errorf("unexpected GitHub call: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer srv.Close()
			r := &cutRunner{gh: newGitHubClientForTest(t, srv)}
			issue := mkJiraIssue(t, "REL-1", "Release: v25.2.2 (hot fix)", nil)
			sha, alreadyCut, err := r.validate(context.Background(), issue, bn)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.False(t, alreadyCut)
			require.Equal(t, tc.expectedSHA, sha)
		})
	}
}

func TestJiraIssueStatusName(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{
			name:     "open ticket",
			body:     `{"key":"REL-1","fields":{"summary":"x","status":{"name":"Open","id":"1"}}}`,
			expected: "Open",
		},
		{
			name:     "already baking",
			body:     `{"key":"REL-1","fields":{"summary":"x","status":{"name":"Baking","id":"2"}}}`,
			expected: "Baking",
		},
		{
			name:     "missing status field",
			body:     `{"key":"REL-1","fields":{"summary":"x"}}`,
			expected: "",
		},
		{
			name:     "wrong-shape status returns empty rather than panicking",
			body:     `{"key":"REL-1","fields":{"summary":"x","status":"unexpected-string"}}`,
			expected: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var issue jiraIssue
			require.NoError(t, json.Unmarshal([]byte(tc.body), &issue))
			require.Equal(t, tc.expected, issue.statusName())
		})
	}
}

// newSlackClientForTest builds a slackClient that talks to the supplied
// httptest server. slack-go's APIURL option requires a trailing slash.
func newSlackClientForTest(baseURL string) *slackClient {
	return &slackClient{
		api: slack.New("test-token", slack.OptionAPIURL(baseURL+"/")),
	}
}

// TestCutRunnerProcessCandidateSkipsBakingTransition exercises the gate that
// guards Transition(transitionBaking) on full.statusName() so a re-run after
// a partial failure (where the ticket is already in Baking) doesn't error
// out at Jira's "transition not valid for current state". The Jira mock
// fails the test if it sees a /transitions POST against the main ticket;
// the rest of the recovery flow is allowed through.
func TestCutRunnerProcessCandidateSkipsBakingTransition(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	bn := branchNames{base: "release-25.4", staging: "release-25.4.3-rc"}

	// Candidate matches the JQL search shape: cut date is today (ready) and
	// summary parses. The recovery gate also requires cfStagingBranch to
	// already be set on the ticket so validate returns branchAlreadyCut.
	candidate := *mkJiraIssue(t, "REL-1", "Release: v25.4.3", map[string]interface{}{
		cfCutBranchDate: "2026-04-20",
		cfStagingBranch: bn.staging,
	})

	// Full issue (returned by Jira GetIssue) reports the ticket is already
	// Baking and the cut subtask is Done. Subtasks include a Done
	// "Cut Staging Branch" so findCutSubtask returns "" and the subtask
	// transition is skipped naturally.
	doneSubtask := jiraSubtask{Key: "REL-2"}
	doneSubtask.Fields.Summary = cutStagingSubtaskMatch
	doneSubtask.Fields.Status.Name = "Done"
	subtaskRaw, err := json.Marshal([]jiraSubtask{doneSubtask})
	require.NoError(t, err)
	fullFields, err := json.Marshal(map[string]interface{}{
		"summary":           "Release: v25.4.3",
		cfCutBranchDate:     "2026-04-20",
		cfStagingBranch:     bn.staging,
		cfPickSHADate:       "2026-04-22",
		cfCloudReleaseNotes: "2026-04-23",
		cfPublishBinary:     "2026-04-24",
		"status":            map[string]interface{}{"name": bakingStatusName, "id": "151"},
		"subtasks":          json.RawMessage(subtaskRaw),
	})
	require.NoError(t, err)
	fullJSON := `{"key":"REL-1","fields":` + string(fullFields) + `}`

	const stagingSHA = "stagingsha000"

	var labelCreated, fieldsUpdated atomic.Bool
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/git/ref/heads/"+bn.staging):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"ref":"refs/heads/%s","object":{"sha":%q}}`, bn.staging, stagingSHA)
		case strings.HasSuffix(r.URL.Path, "/labels"):
			labelCreated.Store(true)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected GitHub call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ghSrv.Close()

	jiraSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rest/api/3/issue/REL-1"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fullJSON))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/rest/api/3/issue/REL-1"):
			fieldsUpdated.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/REL-1/transitions"):
			t.Errorf("Transition(transitionBaking) must be skipped when ticket is already Baking")
			w.WriteHeader(http.StatusBadRequest)
		case strings.HasSuffix(r.URL.Path, "/REL-1/comment"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected jira call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer jiraSrv.Close()

	// Slack PostMessage is unconditional in processCandidate. Stub via the
	// slack-go API URL so the call exits cleanly without our test having to
	// reach into the slackClient internals.
	slackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C1","ts":"1.2","permalink":"https://slack/x"}`))
	}))
	defer slackSrv.Close()

	gh := newGitHubClient("test", "owner", "repo")
	u, err := url.Parse(ghSrv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u

	jc := newJiraClient("bot@example.com", "test")
	jc.baseURL = jiraSrv.URL

	r := &cutRunner{
		jira:    jc,
		gh:      gh,
		slack:   newSlackClientForTest(slackSrv.URL),
		today:   today,
		repo:    "owner/repo",
		channel: "test-channel",
	}
	require.NoError(t, r.processCandidate(context.Background(), candidate))
	require.True(t, fieldsUpdated.Load(), "UpdateFields must run on the recovery path")
	require.True(t, labelCreated.Load(), "CreateLabel must run on the recovery path")
}

// TestCutRunnerProcessCandidateCutsFirstRC exercises the fresh-cut path for
// the first RC of a major release (vX.Y.0-rc.1): the one patch-zero version
// that does cut a staging branch. It verifies the release-X.Y.0-rc branch is
// created at the tip of release-X.Y and the matching backport label is created.
func TestCutRunnerProcessCandidateCutsFirstRC(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	bn := branchNames{base: "release-26.3", staging: "release-26.3.0-rc"}
	const baseSHA = "basesha000"

	candidate := *mkJiraIssue(t, "REL-1", "Release: v26.3.0-rc.1", map[string]interface{}{
		cfCutBranchDate: "2026-04-20",
	})

	todoSubtask := jiraSubtask{Key: "REL-2"}
	todoSubtask.Fields.Summary = cutStagingSubtaskMatch
	todoSubtask.Fields.Status.Name = "To Do"
	subtaskRaw, err := json.Marshal([]jiraSubtask{todoSubtask})
	require.NoError(t, err)
	fullFields, err := json.Marshal(map[string]interface{}{
		"summary":       "Release: v26.3.0-rc.1",
		cfCutBranchDate: "2026-04-20",
		cfPickSHADate:   "2026-04-22",
		"status":        map[string]interface{}{"name": "Open", "id": "1"},
		"subtasks":      json.RawMessage(subtaskRaw),
	})
	require.NoError(t, err)
	fullJSON := `{"key":"REL-1","fields":` + string(fullFields) + `}`

	var createdRef, createdSHA, createdLabel string
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		// The staging branch doesn't exist yet; the base branch does. Exact
		// suffix matches keep "release-26.3" from also matching the staging
		// branch path.
		case strings.HasSuffix(r.URL.Path, "/git/ref/heads/"+bn.staging):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/git/ref/heads/"+bn.base):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"ref":"refs/heads/%s","object":{"sha":%q}}`, bn.base, baseSHA)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git/refs"):
			var req struct {
				Ref string `json:"ref"`
				SHA string `json:"sha"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			createdRef, createdSHA = req.Ref, req.SHA
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"ref":%q,"object":{"sha":%q}}`, req.Ref, req.SHA)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/labels"):
			var req struct {
				Name string `json:"name"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			createdLabel = req.Name
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected GitHub call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ghSrv.Close()

	var transitionedToBaking atomic.Bool
	jiraSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rest/api/3/issue/REL-1"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fullJSON))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/rest/api/3/issue/REL-1"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/REL-1/transitions"):
			transitionedToBaking.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/REL-2/transitions"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/REL-1/comment"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected jira call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer jiraSrv.Close()

	slackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C1","ts":"1.2","permalink":"https://slack/x"}`))
	}))
	defer slackSrv.Close()

	gh := newGitHubClient("test", "owner", "repo")
	u, err := url.Parse(ghSrv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u

	jc := newJiraClient("bot@example.com", "test")
	jc.baseURL = jiraSrv.URL

	r := &cutRunner{
		jira:    jc,
		gh:      gh,
		slack:   newSlackClientForTest(slackSrv.URL),
		today:   today,
		repo:    "owner/repo",
		channel: "test-channel",
	}
	require.NoError(t, r.processCandidate(context.Background(), candidate))
	require.Equal(t, "refs/heads/"+bn.staging, createdRef)
	require.Equal(t, baseSHA, createdSHA)
	require.Equal(t, "backport-26.3.0-rc", createdLabel)
	require.True(t, transitionedToBaking.Load())
}

// TestCutRunnerProcessCandidateEarlyReturns covers the early-exit paths in
// processCandidate that return nil without touching GitHub or Jira: an
// unparseable summary, a patch-zero release, a missing cut-branch date, and
// a future cut date. A cutRunner with no jira/gh clients is sufficient
// because none of the side-effecting paths are reached; if the early returns
// regress, the test will panic on the nil clients.
func TestCutRunnerProcessCandidateEarlyReturns(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		summary   string
		cutDate   string // value of cfCutBranchDate on the candidate
		todayFlag string // override r.today via the testIssueKey gate, "" = use today
	}{
		{
			name:    "unparseable summary skipped",
			summary: "Release: notice this is not parseable",
		},
		{
			name:    "patch-zero release skipped",
			summary: "Release: v25.4.0",
		},
		// The patch-zero pre-release cases set a ready cut date so a
		// regression in the patch-zero gate would fall through to the
		// side-effecting path and panic on the nil clients, rather than
		// being masked by the missing-cut-date skip.
		{
			name:    "alpha of major release skipped",
			summary: "Release: v25.4.0-alpha.1",
			cutDate: "2026-04-20",
		},
		{
			name:    "beta of major release skipped",
			summary: "Release: v25.4.0-beta.1",
			cutDate: "2026-04-20",
		},
		{
			name:    "second rc of major release skipped",
			summary: "Release: v25.4.0-rc.2",
			cutDate: "2026-04-20",
		},
		{
			name:    "missing cut date skipped",
			summary: "Release: v25.4.3",
		},
		{
			name:    "future cut date skipped",
			summary: "Release: v25.4.3",
			cutDate: "2026-04-25",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &cutRunner{today: today}
			fields := map[string]interface{}{}
			if tc.cutDate != "" {
				fields[cfCutBranchDate] = tc.cutDate
			}
			c := mkJiraIssue(t, "REL-1", tc.summary, fields)
			require.NoError(t, r.processCandidate(context.Background(), *c))
		})
	}
}

// TestBuildCutSummary locks down the Markdown rendered for the job summary in
// the cut, resumed, and dry-run cases, plus the failure block.
func TestBuildCutSummary(t *testing.T) {
	const (
		title = "Release: v25.2.20"
		sha   = "stagingsha0001beef99"
	)

	t.Run("fresh cut", func(t *testing.T) {
		s := buildCutSummary("REL-1", title, "release-25.2.20-rc", sha, "owner/repo", false, false)
		require.Contains(t, s, "✅")
		// Heading links "<KEY> - <title>" to the Jira issue.
		require.Contains(t, s, "[REL-1 - Release: v25.2.20](https://cockroachlabs.atlassian.net/browse/REL-1)")
		// Body line drops the "Release vX:" prefix the old format carried.
		require.Contains(t, s, "staging branch `release-25.2.20-rc` at")
		require.NotContains(t, s, "Release `v25.2.20`:")
		require.Contains(t, s, "stagingsha000") // shortened to 12 chars
		require.Contains(t, s, "https://github.com/owner/repo/commit/"+sha)
		require.NotContains(t, s, "dry run")
		require.NotContains(t, s, "resumed")
	})

	t.Run("resumed", func(t *testing.T) {
		s := buildCutSummary("REL-1", title, "release-25.2.20-rc", sha, "owner/repo", true, false)
		require.Contains(t, s, "resumed")
	})

	t.Run("dry run", func(t *testing.T) {
		s := buildCutSummary("REL-1", title, "release-25.2.20-rc", sha, "owner/repo", false, true)
		require.Contains(t, s, "dry run")
	})
}

func TestBuildCutFailureSummary(t *testing.T) {
	s := buildCutFailureSummary("REL-1", errors.New("creating staging branch: boom"))
	require.Contains(t, s, "❌")
	require.Contains(t, s, "REL-1")
	require.Contains(t, s, "branch cut failed")
	require.Contains(t, s, "creating staging branch: boom")
}

// TestCutRunnerProcessCandidateWritesSummary guards the success-block wiring in
// processCandidate: the recovery (resumed) path must append a ✅ block to the
// configured summary file. It reuses the already-Baking recovery setup so no
// branch is created — only the resume path runs.
func TestCutRunnerProcessCandidateWritesSummary(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	bn := branchNames{base: "release-25.4", staging: "release-25.4.3-rc"}

	candidate := *mkJiraIssue(t, "REL-1", "Release: v25.4.3", map[string]interface{}{
		cfCutBranchDate: "2026-04-20",
		cfStagingBranch: bn.staging,
	})

	doneSubtask := jiraSubtask{Key: "REL-2"}
	doneSubtask.Fields.Summary = cutStagingSubtaskMatch
	doneSubtask.Fields.Status.Name = "Done"
	subtaskRaw, err := json.Marshal([]jiraSubtask{doneSubtask})
	require.NoError(t, err)
	fullFields, err := json.Marshal(map[string]interface{}{
		"summary":           "Release: v25.4.3",
		cfCutBranchDate:     "2026-04-20",
		cfStagingBranch:     bn.staging,
		cfPickSHADate:       "2026-04-22",
		cfCloudReleaseNotes: "2026-04-23",
		cfPublishBinary:     "2026-04-24",
		"status":            map[string]interface{}{"name": bakingStatusName, "id": "151"},
		"subtasks":          json.RawMessage(subtaskRaw),
	})
	require.NoError(t, err)
	fullJSON := `{"key":"REL-1","fields":` + string(fullFields) + `}`

	const stagingSHA = "stagingsha000"
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/git/ref/heads/"+bn.staging):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"ref":"refs/heads/%s","object":{"sha":%q}}`, bn.staging, stagingSHA)
		case strings.HasSuffix(r.URL.Path, "/labels"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected GitHub call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ghSrv.Close()

	jiraSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rest/api/3/issue/REL-1"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fullJSON))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/rest/api/3/issue/REL-1"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/REL-1/comment"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected jira call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer jiraSrv.Close()

	slackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C1","ts":"1.2","permalink":"https://slack/x"}`))
	}))
	defer slackSrv.Close()

	gh := newGitHubClient("test", "owner", "repo")
	u, err := url.Parse(ghSrv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u

	jc := newJiraClient("bot@example.com", "test")
	jc.baseURL = jiraSrv.URL

	summaryFile := filepath.Join(t.TempDir(), "summary.md")
	r := &cutRunner{
		jira:          jc,
		gh:            gh,
		slack:         newSlackClientForTest(slackSrv.URL),
		today:         today,
		repo:          "owner/repo",
		channel:       "test-channel",
		summaryWriter: summaryWriter{path: summaryFile},
	}
	require.NoError(t, r.processCandidate(context.Background(), candidate))

	data, err := os.ReadFile(summaryFile)
	require.NoError(t, err)
	require.Contains(t, string(data), "✅")
	require.Contains(t, string(data), "REL-1")
	require.Contains(t, string(data), "resumed")
}

// TestCutRunnerRunWritesFailureSummary guards the failure-block wiring in
// run(): a candidate whose processing fails must produce a ❌ block in the
// summary file. The failure is induced by a GitHub error during validate (the
// branch-existence check), which run()'s per-candidate error handler turns
// into a failure block.
func TestCutRunnerRunWritesFailureSummary(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	fullFields, err := json.Marshal(map[string]interface{}{
		"summary":       "Release: v25.4.3",
		cfCutBranchDate: "2026-04-20",
	})
	require.NoError(t, err)
	fullJSON := `{"key":"REL-1","fields":` + string(fullFields) + `}`

	// GitHub fails every call: BranchExists (the first GH call in validate)
	// surfaces the error, so processCandidate returns before any mutation.
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ghSrv.Close()

	jiraSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rest/api/3/issue/REL-1") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fullJSON))
			return
		}
		t.Errorf("unexpected jira call: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer jiraSrv.Close()

	slackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C1","ts":"1.2","permalink":"https://slack/x"}`))
	}))
	defer slackSrv.Close()

	gh := newGitHubClient("test", "owner", "repo")
	u, err := url.Parse(ghSrv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u

	jc := newJiraClient("bot@example.com", "test")
	jc.baseURL = jiraSrv.URL

	summaryFile := filepath.Join(t.TempDir(), "summary.md")
	r := &cutRunner{
		jira:          jc,
		gh:            gh,
		slack:         newSlackClientForTest(slackSrv.URL),
		today:         today,
		repo:          "owner/repo",
		channel:       "test-channel",
		opsChannel:    "ops-channel",
		testIssueKey:  "REL-1", // bypass JQL search; candidates() fetches REL-1
		summaryWriter: summaryWriter{path: summaryFile},
	}
	require.Error(t, r.run(context.Background()))

	data, err := os.ReadFile(summaryFile)
	require.NoError(t, err)
	require.Contains(t, string(data), "❌")
	require.Contains(t, string(data), "REL-1")
	require.Contains(t, string(data), "branch cut failed")
	// The actual error must reach the block, not just the header — otherwise a
	// future change that drops error detail would go undetected.
	require.Contains(t, string(data), "validating ticket and repo state")
}

func TestBuildJiraComment(t *testing.T) {
	d := releaseDetails{
		Release:               "v25.4.3",
		FinalRelease:          "v25.4.3",
		Series:                "25.4",
		StagingBranch:         "release-25.4.3-rc",
		BaseBranch:            "release-25.4",
		CutAtSHA:              "deadbeef",
		CutAtCommitURL:        "https://github.com/example-org/example-repo/commit/deadbeef",
		BranchCutDateTime:     "2026-04-20 07:00 UTC",
		PickSHADate:           "Wednesday, 04/22",
		CloudReleaseNotesDate: "Thursday, 04/23",
		PublishBinaryDate:     "Friday, 04/24",
		ReleaseType:           scheduledType,
	}
	const slackLink = "https://example.slack.com/archives/C123/p456"

	t.Run("scheduled with slack link", func(t *testing.T) {
		doc := buildJiraComment(d, slackLink)
		require.Equal(t, "doc", doc["type"])
		require.Equal(t, 1, doc["version"])
		content, ok := doc["content"].([]interface{})
		require.True(t, ok)
		// Slack-link paragraph + intro paragraph + dates bullet list +
		// "Staging branch X:" paragraph + cut/SHA bullet list = 5 blocks.
		require.Len(t, content, 5)

		encoded, err := json.Marshal(doc)
		require.NoError(t, err)
		s := string(encoded)
		require.Contains(t, s, slackLink)
		require.Contains(t, s, "release-25.4.3-rc")
		require.Contains(t, s, "deadbeef")
		require.Contains(t, s, "Pick SHA (Branch Freeze) Date:")
		require.Contains(t, s, "Wednesday, 04/22")
	})

	t.Run("pre-release uses backport-policy template", func(t *testing.T) {
		pd := d
		pd.Release = "v25.3.1-alpha.1"
		pd.FinalRelease = "v25.3.1"
		pd.Series = "25.3"
		pd.StagingBranch = "release-25.3.1-rc"
		pd.ReleaseType = preReleaseType
		doc := buildJiraComment(pd, "")
		encoded, err := json.Marshal(doc)
		require.NoError(t, err)
		s := string(encoded)
		require.Contains(t, s, backportPolicyURL)
		require.Contains(t, s, "release-25.3.1-rc")
		// Pre-release template doesn't render the date bullets, so
		// "Pick SHA (Branch Freeze) Date:" must be absent.
		require.NotContains(t, s, "Pick SHA (Branch Freeze) Date:")
	})

	t.Run("empty slack link omits leading paragraph", func(t *testing.T) {
		doc := buildJiraComment(d, "")
		content, ok := doc["content"].([]interface{})
		require.True(t, ok)
		// Same blocks as the scheduled-with-slack case, minus the Slack-link
		// paragraph: intro + dates + "Staging branch:" para + cut bullets = 4.
		require.Len(t, content, 4)
	})
}

func TestSplitRepo(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedOwner string
		expectedRepo  string
		expectedErr   string
	}{
		{name: "valid", input: "cockroachdb/cockroach", expectedOwner: "cockroachdb", expectedRepo: "cockroach"},
		{name: "missing slash", input: "cockroach", expectedErr: "expected owner/name"},
		{name: "empty owner", input: "/cockroach", expectedErr: "expected owner/name"},
		{name: "empty repo", input: "cockroachdb/", expectedErr: "expected owner/name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			owner, repo, err := splitRepo(tc.input)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOwner, owner)
			require.Equal(t, tc.expectedRepo, repo)
		})
	}
}
