// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestJiraCommentPermalink(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		commentID string
		expected  string
	}{
		{
			name:      "typical",
			key:       "REL-4910",
			commentID: "363988",
			expected:  "https://cockroachlabs.atlassian.net/browse/REL-4910?focusedCommentId=363988",
		},
		{
			name:      "comment id with special chars is encoded",
			key:       "REL-1",
			commentID: "abc def&x=1",
			expected:  "https://cockroachlabs.atlassian.net/browse/REL-1?focusedCommentId=abc+def%26x%3D1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, jiraCommentPermalink(tc.key, tc.commentID))
		})
	}
}

func TestBuildBlessedSlackMessage(t *testing.T) {
	got := buildBlessedSlackMessage("v24.3.33", "REL-4910", "363988")
	want := "<https://cockroachlabs.atlassian.net/browse/REL-4910?focusedCommentId=363988|REL-4910>\n" +
		"`v24.3.33` binaries have been blessed:\n" +
		"• Working on Homebrew versions.\n" +
		"• Ready for docs for download/SH.\n" +
		"• Ready for SRE to enable on CockroachCloud."
	require.Equal(t, want, got)
}

func TestBuildPublishedSlackMessage(t *testing.T) {
	got := buildPublishedSlackMessage("v26.2.0")
	want := "`v26.2.0` binaries have been published:\n" +
		"• Ready for Security scans and SBOM\n" +
		"• Ready for Initial Images and VCoO artifacts"
	require.Equal(t, want, got)
}

// TestBuildBlessedJiraComment asserts the ADF JSON we send to Jira.
// JSONEq lets the test ignore key ordering while still pinning every
// node — so a future change to the comment shape requires updating the
// expected JSON, not silently drifting.
func TestBuildBlessedJiraComment(t *testing.T) {
	doc := buildBlessedJiraComment("v24.3.33")
	got, err := json.Marshal(doc)
	require.NoError(t, err)
	want := `{
		"type": "doc",
		"version": 1,
		"content": [
			{
				"type": "paragraph",
				"content": [
					{"type":"text","text":"v24.3.33","marks":[{"type":"code"}]},
					{"type":"text","text":" binaries have been blessed:","marks":[{"type":"strong"}]}
				]
			},
			{
				"type": "bulletList",
				"content": [
					{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"Working on Homebrew versions."}]}]},
					{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"Ready for docs for download/SH."}]}]},
					{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"Ready for SRE to enable on CockroachCloud."}]}]}
				]
			}
		]
	}`
	require.JSONEq(t, want, string(got))
}

func TestReadVersionFile(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expected    string
		expectedErr string
	}{
		{
			name:     "simple line with v prefix",
			content:  "v24.3.33\n",
			expected: "v24.3.33",
		},
		{
			name:     "prepends v when missing",
			content:  "24.3.33\n",
			expected: "v24.3.33",
		},
		{
			name:     "skips comments and blank lines",
			content:  "# header\n\n# another\nv26.3.0-alpha.00000000\n",
			expected: "v26.3.0-alpha.00000000",
		},
		{
			name:     "trims surrounding whitespace",
			content:  "   v25.4.3   \n",
			expected: "v25.4.3",
		},
		{
			name:        "empty file is an error",
			content:     "",
			expectedErr: "no version line",
		},
		{
			name:        "only-comments file is an error",
			content:     "# nothing here\n# really\n",
			expectedErr: "no version line",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "version.txt")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0644))
			got, err := readVersionFile(path)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}

	t.Run("missing file surfaces the os error", func(t *testing.T) {
		_, err := readVersionFile(filepath.Join(t.TempDir(), "does-not-exist.txt"))
		require.Error(t, err)
	})
}

// fakeJira is a minimal jiraSearchCommenter for resolveTicketKey and run
// tests. It records the JQL it was asked to run, returns canned issues,
// and tracks AddComment calls. Behavior is per-test (set fields, then
// invoke the runner); no goroutines so concurrent access isn't a concern.
type fakeJira struct {
	searchResult []jiraIssue
	searchErr    error
	lastJQL      string

	addCommentID  string
	addCommentErr error
	addCommentKey string
	addCommentDoc map[string]interface{}
}

func (f *fakeJira) SearchJQL(jql string, _ []string) ([]jiraIssue, error) {
	f.lastJQL = jql
	return f.searchResult, f.searchErr
}

func (f *fakeJira) AddComment(key string, doc map[string]interface{}) (string, error) {
	f.addCommentKey = key
	f.addCommentDoc = doc
	return f.addCommentID, f.addCommentErr
}

// fakeSlack records every PostMessage call. The runner makes more than
// one call per run (blessed + IBM-OEM), so a single-slot recorder would
// hide ordering bugs. postErrs returns one error per call (nil = success);
// out-of-range index returns nil so callers only need to populate the
// failing slot.
type fakeSlack struct {
	postErrs []error
	posts    []slackPost
}

type slackPost struct{ channel, body string }

func (f *fakeSlack) PostMessage(channel, text string) (string, error) {
	f.posts = append(f.posts, slackPost{channel: channel, body: text})
	if len(f.posts) <= len(f.postErrs) {
		if err := f.postErrs[len(f.posts)-1]; err != nil {
			return "", err
		}
	}
	return "https://example.slack/permalink", nil
}

func TestResolveTicketKey(t *testing.T) {
	tests := []struct {
		name         string
		jiraIssue    string // operator override
		searchResult []jiraIssue
		searchErr    error
		expectedKey  string
		expectedJQL  string // empty = no JQL expected
		expectedErr  string
	}{
		{
			name:         "single match returns the key",
			searchResult: []jiraIssue{{Key: "REL-4910"}},
			expectedKey:  "REL-4910",
			expectedJQL:  `issueType="CRDB Release" and cf[10590]="deadbeef"`,
		},
		{
			name:        "zero matches errors with --jira-issue hint",
			expectedJQL: `issueType="CRDB Release" and cf[10590]="deadbeef"`,
			expectedErr: "no CRDB Release ticket found with cfBuildSHA=deadbeef; pass --jira-issue",
		},
		{
			name: "multiple matches errors and lists keys",
			searchResult: []jiraIssue{
				{Key: "REL-1"},
				{Key: "REL-2"},
			},
			expectedJQL: `issueType="CRDB Release" and cf[10590]="deadbeef"`,
			expectedErr: "multiple CRDB Release tickets match cfBuildSHA=deadbeef: [REL-1 REL-2]; pass --jira-issue",
		},
		{
			name:        "API error wraps the cause",
			searchErr:   errors.New("boom"),
			expectedJQL: `issueType="CRDB Release" and cf[10590]="deadbeef"`,
			expectedErr: "looking up release ticket by cfBuildSHA: boom",
		},
		{
			name:        "--jira-issue override skips the JQL entirely",
			jiraIssue:   "REL-9999",
			expectedKey: "REL-9999",
			// expectedJQL stays empty: SearchJQL must not be called.
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jira := &fakeJira{
				searchResult: tc.searchResult,
				searchErr:    tc.searchErr,
			}
			r := &notifyPublishRunner{
				jira:      jira,
				sha:       "deadbeef",
				jiraIssue: tc.jiraIssue,
			}
			got, err := r.resolveTicketKey()
			require.Equal(t, tc.expectedJQL, jira.lastJQL)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedKey, got)
		})
	}
}

// TestNotifyPublishRunnerRun exercises the end-to-end happy path:
// resolveTicketKey -> AddComment -> blessed Slack post -> IBM-OEM Slack
// post. Asserts the two Slack posts happen in the right order, on the
// right channels, with the right bodies (blessed contains the
// commentID-permalink; published does not link to Jira).
func TestNotifyPublishRunnerRun(t *testing.T) {
	jira := &fakeJira{
		searchResult: []jiraIssue{{Key: "REL-4910"}},
		addCommentID: "363988",
	}
	slack := &fakeSlack{}
	r := &notifyPublishRunner{
		jira:       jira,
		slack:      slack,
		sha:        "deadbeef",
		version:    "v24.3.33",
		channel:    "#db-release-status",
		ibmChannel: "#proj-ibm-oem-releases",
	}
	require.NoError(t, r.run())

	require.Equal(t, "REL-4910", jira.addCommentKey)
	require.NotNil(t, jira.addCommentDoc, "AddComment must receive an ADF doc")

	require.Len(t, slack.posts, 2, "expected blessed + IBM-OEM posts")

	require.Equal(t, "#db-release-status", slack.posts[0].channel)
	require.Contains(t, slack.posts[0].body,
		"<https://cockroachlabs.atlassian.net/browse/REL-4910?focusedCommentId=363988|REL-4910>")
	require.Contains(t, slack.posts[0].body, "`v24.3.33` binaries have been blessed:")

	require.Equal(t, "#proj-ibm-oem-releases", slack.posts[1].channel)
	require.Contains(t, slack.posts[1].body, "`v24.3.33` binaries have been published:")
	require.NotContains(t, slack.posts[1].body, "atlassian.net",
		"IBM-OEM message must not include a Jira link")
}

func TestNotifyPublishRunnerRun_AssertsOnEmptyCommentID(t *testing.T) {
	jira := &fakeJira{
		searchResult: []jiraIssue{{Key: "REL-4910"}},
		addCommentID: "", // simulate Jira API contract drift
	}
	r := &notifyPublishRunner{
		jira:       jira,
		slack:      &fakeSlack{},
		sha:        "deadbeef",
		version:    "v24.3.33",
		channel:    "#db-release-status",
		ibmChannel: "#proj-ibm-oem-releases",
	}
	err := r.run()
	require.ErrorContains(t, err, "Jira returned empty comment ID")
}

// TestNotifyPublishRunnerRun_IBMPostFailureSurfaces guards the asymmetric
// recovery story: when the blessed message has already been posted but
// the IBM-OEM post fails, the job must fail loudly so the operator sees
// it in the existing #release-ops notify summary. The blessed Slack
// message is still considered delivered; only the IBM post needs to be
// reposted manually.
func TestNotifyPublishRunnerRun_IBMPostFailureSurfaces(t *testing.T) {
	jira := &fakeJira{
		searchResult: []jiraIssue{{Key: "REL-4910"}},
		addCommentID: "363988",
	}
	slack := &fakeSlack{
		// First call (blessed) succeeds, second call (IBM) fails.
		postErrs: []error{nil, errors.New("slack 500")},
	}
	r := &notifyPublishRunner{
		jira:       jira,
		slack:      slack,
		sha:        "deadbeef",
		version:    "v24.3.33",
		channel:    "#db-release-status",
		ibmChannel: "#proj-ibm-oem-releases",
	}
	err := r.run()
	require.ErrorContains(t, err, "posting IBM-OEM published message to #proj-ibm-oem-releases")
	require.Len(t, slack.posts, 2,
		"both Slack posts must be attempted before failing")
}

func TestNotifyPublishRunnerRun_DryRunDoesNotCallJiraOrSlack(t *testing.T) {
	jira := &fakeJira{
		searchResult:  []jiraIssue{{Key: "REL-4910"}},
		addCommentErr: errors.New("AddComment must not be called in dry-run"),
	}
	slack := &fakeSlack{
		postErrs: []error{
			errors.New("blessed PostMessage must not be called in dry-run"),
			errors.New("IBM PostMessage must not be called in dry-run"),
		},
	}
	r := &notifyPublishRunner{
		jira:       jira,
		slack:      slack,
		sha:        "deadbeef",
		version:    "v24.3.33",
		channel:    "#db-release-status",
		ibmChannel: "#proj-ibm-oem-releases",
		dryRun:     true,
	}
	require.NoError(t, r.run())
	require.Empty(t, jira.addCommentKey, "AddComment must not be called in dry-run")
	require.Empty(t, slack.posts, "PostMessage must not be called in dry-run")
}

// TestAddCommentReturnsID exercises the modified jiraClient.AddComment
// against a stubbed Jira REST endpoint that mirrors the
// /rest/api/3/issue/{key}/comment response shape. Catches Jira-API
// contract regressions (renamed/removed .id field) without hitting real
// Jira.
func TestAddCommentReturnsID(t *testing.T) {
	const wantID = "1234567"
	const wantKey = "REL-42"

	var seenPath, seenMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		seenPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%q,"self":"https://cockroachlabs.atlassian.net/rest/api/3/issue/%s/comment/%s"}`,
			wantID, wantKey, wantID)
	}))
	defer srv.Close()

	c := newJiraClient("test@example.com", "test-token")
	c.baseURL = srv.URL

	gotID, err := c.AddComment(wantKey, map[string]interface{}{"hello": "world"})
	require.NoError(t, err)
	require.Equal(t, wantID, gotID)
	require.Equal(t, http.MethodPost, seenMethod)
	require.True(t, strings.HasSuffix(seenPath, "/rest/api/3/issue/REL-42/comment"),
		"unexpected request path %s", seenPath)
}
