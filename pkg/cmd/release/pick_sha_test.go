// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPickSHAJQL(t *testing.T) {
	// Production JQL keeps `parent is not empty` to skip template tickets.
	require.Equal(t,
		`issueType="CRDB Release" and parent is not empty and `+
			`status not in (done, cancelled, "Post-Publish Tasks") and `+
			`"Staging Branch[Short text]" is not empty`,
		pickSHAJQL)

	// Dry-run JQL drops the parent filter so rehearsals can target template
	// tickets.
	require.NotContains(t, pickSHAJQLDryRun, "parent is not empty")
	require.Contains(t, pickSHAJQLDryRun, `"Staging Branch[Short text]" is not empty`)
}

func TestBuildPickSHADetails(t *testing.T) {
	rawFields, err := json.Marshal(map[string]interface{}{
		"summary":           "Release: v25.4.3",
		cfStagingBranch:     "release-25.4.3-rc",
		cfCloudReleaseNotes: "2026-04-23",
		cfPublishBinary:     "2026-04-24",
	})
	require.NoError(t, err)
	var issue jiraIssue
	require.NoError(t, json.Unmarshal(
		[]byte(`{"key":"REL-1234","fields":`+string(rawFields)+`}`), &issue))

	d, err := buildPickSHADetails(
		&issue, "release-25.4.3-rc", "deadbeef", "example-org/example-repo", "release-build-and-sign.yml")
	require.NoError(t, err)
	require.Equal(t, "release-25.4.3-rc", d.StagingBranch)
	require.Equal(t, "deadbeef", d.PickedSHA)
	require.Equal(t, "https://github.com/example-org/example-repo/commit/deadbeef", d.PickedCommitURL)
	require.Equal(t,
		"https://github.com/example-org/example-repo/actions/workflows/release-build-and-sign.yml",
		d.BuildRunListURL)
	require.Equal(t, "Thursday, 04/23", d.CloudReleaseNotesDate)
	require.Equal(t, "Friday, 04/24", d.PublishBinaryDate)
}

func TestBuildPickSHADetailsTBD(t *testing.T) {
	// Empty date fields collapse to "TBD" so the rendered message doesn't
	// show a bare backtick pair.
	var issue jiraIssue
	require.NoError(t, json.Unmarshal([]byte(`{"key":"REL-1","fields":{}}`), &issue))
	d, err := buildPickSHADetails(&issue, "release-25.4.3-rc", "abc", "o/r", "w.yml")
	require.NoError(t, err)
	require.Equal(t, "TBD", d.CloudReleaseNotesDate)
	require.Equal(t, "TBD", d.PublishBinaryDate)
}

func TestBuildPickSHASlackMessage(t *testing.T) {
	d := pickSHADetails{
		StagingBranch:         "release-25.4.3-rc",
		PickedSHA:             "deadbeef",
		PickedCommitURL:       "https://github.com/example-org/example-repo/commit/deadbeef",
		BuildRunListURL:       "https://github.com/example-org/example-repo/actions/workflows/release-build-and-sign.yml",
		CloudReleaseNotesDate: "Thursday, 04/23",
		PublishBinaryDate:     "Friday, 04/24",
	}

	t.Run("regular message", func(t *testing.T) {
		msg := buildPickSHASlackMessage(d, false)
		require.Contains(t, msg, "SHA picked for `release-25.4.3-rc`")
		require.Contains(t, msg, "<https://github.com/example-org/example-repo/commit/deadbeef|deadbeef>")
		require.Contains(t, msg,
			"<https://github.com/example-org/example-repo/actions/workflows/release-build-and-sign.yml|view in Actions>")
		require.Contains(t, msg, "*Cloud Release Notes Date:* `Thursday, 04/23`")
		require.Contains(t, msg, "*Publish Binaries Date:* `Friday, 04/24`")
		require.False(t, strings.HasPrefix(msg, "[DRY RUN] "))
	})

	t.Run("dry-run prefix", func(t *testing.T) {
		require.True(t, strings.HasPrefix(buildPickSHASlackMessage(d, true), "[DRY RUN] "))
	})
}

func TestBuildPickSHAJiraComment(t *testing.T) {
	d := pickSHADetails{
		StagingBranch:         "release-25.4.3-rc",
		PickedSHA:             "deadbeef",
		PickedCommitURL:       "https://github.com/example-org/example-repo/commit/deadbeef",
		BuildRunListURL:       "https://github.com/example-org/example-repo/actions/workflows/release-build-and-sign.yml",
		CloudReleaseNotesDate: "Thursday, 04/23",
		PublishBinaryDate:     "Friday, 04/24",
	}
	const slackLink = "https://example.slack.com/archives/C123/p456"

	doc := buildPickSHAJiraComment(d, slackLink)
	require.Equal(t, "doc", doc["type"])
	require.Equal(t, 1, doc["version"])
	content, ok := doc["content"].([]interface{})
	require.True(t, ok)

	// Expect: Slack-link paragraph, intro paragraph, bullet list.
	require.Len(t, content, 3)
	require.Equal(t, "paragraph", content[0].(map[string]interface{})["type"])
	require.Equal(t, "paragraph", content[1].(map[string]interface{})["type"])
	require.Equal(t, "bulletList", content[2].(map[string]interface{})["type"])

	// Encode and inspect for the high-signal substrings rather than walking
	// every node — the structural assertions above are the contract; the
	// JSON check is a smoke test that data flows through the marks.
	encoded, err := json.Marshal(doc)
	require.NoError(t, err)
	asString := string(encoded)
	require.Contains(t, asString, slackLink)
	require.Contains(t, asString, "release-25.4.3-rc")
	require.Contains(t, asString, "deadbeef")
	require.Contains(t, asString, d.PickedCommitURL)
	require.Contains(t, asString, d.BuildRunListURL)
	require.Contains(t, asString, "Cloud Release Notes Date:")
	require.Contains(t, asString, "Thursday, 04/23")
}

func TestBuildPickSHAJiraCommentNoSlackLink(t *testing.T) {
	d := pickSHADetails{
		StagingBranch:         "release-25.4.3-rc",
		PickedSHA:             "abc",
		PickedCommitURL:       "https://example.test/commit/abc",
		BuildRunListURL:       "https://example.test/actions",
		CloudReleaseNotesDate: "TBD",
		PublishBinaryDate:     "TBD",
	}
	doc := buildPickSHAJiraComment(d, "")
	content, ok := doc["content"].([]interface{})
	require.True(t, ok)
	// No Slack-link paragraph: just intro paragraph + bullet list.
	require.Len(t, content, 2)
	require.Equal(t, "paragraph", content[0].(map[string]interface{})["type"])
	require.Equal(t, "bulletList", content[1].(map[string]interface{})["type"])
}

func TestFindSubtask(t *testing.T) {
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
		name         string
		issue        *jiraIssue
		needle       string
		expectedKey  string
		expectedDone bool
	}{
		{
			name:        "open subtask returns key and not-done",
			issue:       mkIssue(mkSubtask("REL-2", "Pick SHA", "To Do")),
			needle:      "Pick SHA",
			expectedKey: "REL-2",
		},
		{
			name:         "done subtask returns key and done",
			issue:        mkIssue(mkSubtask("REL-2", "Pick SHA", "Done")),
			needle:       "Pick SHA",
			expectedKey:  "REL-2",
			expectedDone: true,
		},
		{
			name:   "missing subtask returns empty and not-done",
			issue:  mkIssue(mkSubtask("REL-2", "Some unrelated work", "To Do")),
			needle: "Pick SHA",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, done := findSubtask(tc.issue, tc.needle)
			require.Equal(t, tc.expectedKey, key)
			require.Equal(t, tc.expectedDone, done)
		})
	}
}

func TestBuildReleaseNotesPayload(t *testing.T) {
	mkIssue := func(summary string, fields map[string]interface{}, subtasks ...jiraSubtask) *jiraIssue {
		fields["summary"] = summary
		raw, err := json.Marshal(fields)
		require.NoError(t, err)
		var issue jiraIssue
		require.NoError(t, json.Unmarshal(
			[]byte(`{"key":"REL-1","fields":`+string(raw)+`}`), &issue))
		issue.Fields.Subtasks = subtasks
		return &issue
	}
	docsSubtask := jiraSubtask{Key: "DOC-42"}
	docsSubtask.Fields.Summary = docsSubtaskMatch
	docsSubtask.Fields.Status.Name = "To Do"

	tests := []struct {
		name             string
		issue            *jiraIssue
		sha              string
		expected         releaseNotesPayload
		expectErrContain string
	}{
		{
			name: "scheduled patch with cloud notes date and docs subtask",
			issue: mkIssue("Release: v25.4.3", map[string]interface{}{
				cfCloudReleaseNotes: "2026-04-23",
				cfPublishBinary:     "2026-04-24",
			}, docsSubtask),
			sha: "deadbeef",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.3",
				ReleaseDate:    "2026-04-23",
				ReleaseSHA:     "deadbeef",
				Cloud:          true,
				DocsTicket:     "DOC-42",
			},
		},
		{
			name: "major release patch zero",
			issue: mkIssue("Release: v25.4.0", map[string]interface{}{
				cfCloudReleaseNotes: "2026-04-23",
			}),
			sha: "abc",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.0",
				ReleaseDate:    "2026-04-23",
				ReleaseSHA:     "abc",
				Cloud:          true,
				DocsTicket:     "",
			},
		},
		{
			name: "pre-release suppresses cloud flag",
			issue: mkIssue("Release: v25.4.0-alpha.1", map[string]interface{}{
				cfPublishBinary: "2026-04-24",
			}),
			sha: "abc",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.0-alpha.1",
				ReleaseDate:    "2026-04-24",
				ReleaseSHA:     "abc",
				Cloud:          false,
			},
		},
		{
			name: "release date falls back to publish-binary date",
			issue: mkIssue("Release: v25.4.3", map[string]interface{}{
				cfPublishBinary: "2026-04-24",
			}),
			sha: "abc",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.3",
				ReleaseDate:    "2026-04-24",
				ReleaseSHA:     "abc",
				Cloud:          true,
			},
		},
		{
			name:  "both date fields empty yields empty release date",
			issue: mkIssue("Release: v25.4.3", map[string]interface{}{}),
			sha:   "abc",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.3",
				ReleaseDate:    "",
				ReleaseSHA:     "abc",
				Cloud:          true,
			},
		},
		{
			name: "jira override forces release type",
			issue: mkIssue("Release: v25.4.3", map[string]interface{}{
				cfReleaseTypeOverride: map[string]interface{}{"value": preReleaseType},
				cfCloudReleaseNotes:   "2026-04-23",
			}),
			sha: "abc",
			expected: releaseNotesPayload{
				CurrentRelease: "v25.4.3",
				ReleaseDate:    "2026-04-23",
				ReleaseSHA:     "abc",
				Cloud:          false,
			},
		},
		{
			name:             "unparseable summary returns error",
			issue:            mkIssue("not a release ticket", map[string]interface{}{}),
			sha:              "abc",
			expectErrContain: "parsing release version",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildReleaseNotesPayload(tc.issue, tc.sha)
			if tc.expectErrContain != "" {
				require.ErrorContains(t, err, tc.expectErrContain)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestBuildReleaseNotesWarning(t *testing.T) {
	payload := releaseNotesPayload{
		CurrentRelease: "v25.4.3",
		ReleaseDate:    "2026-04-23",
		ReleaseSHA:     "deadbeef",
		Cloud:          true,
		DocsTicket:     "DOC-42",
	}

	t.Run("with payload renders headline, jira link, payload, and forward line", func(t *testing.T) {
		msg := buildReleaseNotesWarning("REL-1234", payload, fmt.Errorf("500 Internal Server Error"))
		require.Contains(t, msg, ":x: *release-notes-api failed for v25.4.3*")
		require.Contains(t, msg,
			"*Release*: <https://cockroachlabs.atlassian.net/browse/REL-1234|REL-1234>")
		require.Contains(t, msg, "*Error*: 500 Internal Server Error")
		require.Contains(t, msg, "*Payload*: ```")
		require.Contains(t, msg, `"current_release": "v25.4.3"`)
		require.Contains(t, msg, `"docs_ticket": "DOC-42"`)
		require.Contains(t, msg, "#docs-infrastructure-team")
	})

	t.Run("zero payload omits payload block and falls back to ticket key headline", func(t *testing.T) {
		msg := buildReleaseNotesWarning("REL-1234", releaseNotesPayload{}, fmt.Errorf("bad jira data"))
		require.Contains(t, msg, ":x: *release-notes-api failed for REL-1234*")
		require.Contains(t, msg, "*Error*: bad jira data")
		require.NotContains(t, msg, "*Payload*:")
		require.Contains(t, msg, "#docs-infrastructure-team")
	})
}

func TestPostReleaseNotes(t *testing.T) {
	payload := releaseNotesPayload{
		CurrentRelease: "v25.4.3",
		ReleaseDate:    "2026-04-23",
		ReleaseSHA:     "deadbeef",
		Cloud:          true,
		DocsTicket:     "DOC-42",
	}

	t.Run("posts payload with X-API-Key header on success", func(t *testing.T) {
		var gotMethod, gotKey, gotContentType string
		var gotBody []byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotKey = r.Header.Get("X-API-Key")
			gotContentType = r.Header.Get("Content-Type")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		require.NoError(t, postReleaseNotes(srv.URL, "secret-key", payload))
		require.Equal(t, http.MethodPost, gotMethod)
		require.Equal(t, "secret-key", gotKey)
		require.Equal(t, "application/json", gotContentType)

		// The Go function wraps the payload in {"ReleaseNotesPayload": ...}
		// so the docs API consumer keeps its existing field name.
		var wrapped struct {
			ReleaseNotesPayload releaseNotesPayload `json:"ReleaseNotesPayload"`
		}
		require.NoError(t, json.Unmarshal(gotBody, &wrapped))
		require.Equal(t, payload, wrapped.ReleaseNotesPayload)
	})

	t.Run("non-2xx returns error including status and body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
		}))
		defer srv.Close()

		err := postReleaseNotes(srv.URL, "k", payload)
		require.ErrorContains(t, err, "500")
		require.ErrorContains(t, err, "boom")
	})
}

// TestPickSHARunnerProcessCandidateIdempotent exercises the gate at the top
// of processCandidate: when the Pick SHA subtask is already Done, the runner
// must skip the entire candidate — no GitHub call, no Jira mutation. The test
// fails the GitHub mock loudly (handler errors the test) so a regression that
// re-dispatches build-and-sign is caught.
func TestPickSHARunnerProcessCandidateIdempotent(t *testing.T) {
	today := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	// Candidate has the bare fields the runner reads up to the gate:
	// summary parses as v25.4.3 and pick-date is today (so the readiness
	// check passes).
	candidate := mkPickSHACandidate(t, "2026-04-20")

	// Full issue (returned by Jira GetIssue) must include a Done Pick SHA
	// subtask so findSubtask reports done=true.
	doneSubtask := jiraSubtask{Key: "REL-2"}
	doneSubtask.Fields.Summary = "Pick SHA"
	doneSubtask.Fields.Status.Name = "Done"

	var ghCalls atomic.Int64
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ghCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ghSrv.Close()

	jiraSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only GetIssue is allowed past the gate; everything else (UpdateFields,
		// Transition, AddComment) means a regression.
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/rest/api/3/issue/REL-1") {
			t.Errorf("unexpected jira call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		issue := mkPickSHAFullIssueJSON(t, "release-25.4.3-rc", []jiraSubtask{doneSubtask})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(issue))
	}))
	defer jiraSrv.Close()

	r := newPickSHARunnerForTest(t, ghSrv, jiraSrv, today)
	require.NoError(t, r.processCandidate(context.Background(), candidate))
	require.Equal(t, int64(0), ghCalls.Load(),
		"GitHub must not be called when Pick SHA subtask is already Done")
}

// mkPickSHACandidate builds a candidate jiraIssue suitable for the JQL search
// result: summary parses, pickDate is set, and no further fields are needed
// because processCandidate refetches the full issue via Jira.
func mkPickSHACandidate(t *testing.T, pickDate string) jiraIssue {
	t.Helper()
	rawFields, err := json.Marshal(map[string]interface{}{
		"summary":     "Release: v25.4.3",
		cfPickSHADate: pickDate,
	})
	require.NoError(t, err)
	var issue jiraIssue
	require.NoError(t, json.Unmarshal(
		[]byte(`{"key":"REL-1","fields":`+string(rawFields)+`}`), &issue))
	return issue
}

// mkPickSHAFullIssueJSON builds the JSON the Jira GetIssue mock returns for
// REL-1 with a given staging branch and subtask set.
func mkPickSHAFullIssueJSON(t *testing.T, staging string, subtasks []jiraSubtask) string {
	t.Helper()
	subtaskRaw, err := json.Marshal(subtasks)
	require.NoError(t, err)
	rawFields, err := json.Marshal(map[string]interface{}{
		"summary":       "Release: v25.4.3",
		cfStagingBranch: staging,
		cfPickSHADate:   "2026-04-20",
		cfPublishBinary: "2026-04-24",
		"subtasks":      json.RawMessage(subtaskRaw),
	})
	require.NoError(t, err)
	return `{"key":"REL-1","fields":` + string(rawFields) + `}`
}

func newPickSHARunnerForTest(
	t *testing.T, ghSrv, jiraSrv *httptest.Server, today time.Time,
) *pickSHARunner {
	t.Helper()
	gh := newGitHubClient("test-token", "owner", "repo")
	u, err := url.Parse(ghSrv.URL + "/")
	require.NoError(t, err)
	gh.client.BaseURL = u

	jira := newJiraClient("bot@example.com", "test-token")
	jira.baseURL = jiraSrv.URL

	return &pickSHARunner{
		jira:          jira,
		gh:            gh,
		dryRun:        false,
		today:         today,
		buildWorkflow: defaultBuildWorkflow,
	}
}

func TestFindOpenSubtask(t *testing.T) {
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
		needle      string
		expectedKey string
	}{
		{
			name:        "matches case insensitively",
			issue:       mkIssue(mkSubtask("REL-2", "Pick SHA for v25.4.3", "To Do")),
			needle:      "pick sha",
			expectedKey: "REL-2",
		},
		{
			name:        "skips already-done subtask",
			issue:       mkIssue(mkSubtask("REL-2", "Pick SHA", "Done")),
			needle:      "Pick SHA",
			expectedKey: "",
		},
		{
			name:        "no match returns empty",
			issue:       mkIssue(mkSubtask("REL-2", "Some unrelated work", "To Do")),
			needle:      "Pick SHA",
			expectedKey: "",
		},
		{
			name: "first matching open subtask wins",
			issue: mkIssue(
				mkSubtask("REL-1", "Cut Staging Branch", "To Do"),
				mkSubtask("REL-2", "Pick SHA", "To Do"),
			),
			needle:      "Pick SHA",
			expectedKey: "REL-2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedKey, findOpenSubtask(tc.issue, tc.needle))
		})
	}
}
