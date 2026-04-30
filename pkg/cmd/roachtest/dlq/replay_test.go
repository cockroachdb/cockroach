// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestHelpCommandFromStringNilWhenEmpty(t *testing.T) {
	// An empty stored HelpCommand should result in a nil closure so the
	// formatter omits the entire Help section. (PostRequest.HelpCommand
	// being nil is checked by formatter_unit.go to skip the section.)
	require.Nil(t, helpCommandFromString(""))
}

func TestPreviewURL(t *testing.T) {
	entry := &Entry{
		Org:    "williamchoe3",
		Repo:   "cockroach",
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
	// URL targets the entry's repo (so dry-run for a fork-bound entry
	// links to the fork) and not hardcoded cockroachdb/cockroach.
	assert.Contains(t, got, "https://github.com/williamchoe3/cockroach/issues/new")
	assert.Contains(t, got, "title=")
	assert.Contains(t, got, "body=")
	assert.Contains(t, got, "template=none")
}

func TestPreviewURLPopulatesBuildLinksFromTeamCity(t *testing.T) {
	// Without TC fields populated, the rendered body shows empty markdown
	// links like [failed]() and [<sha>](). This test makes sure we
	// reconstruct the build/artifacts/commit URLs in the preview.
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
	// The preview URL embeds the rendered body — check that the build,
	// commit, and artifacts URLs appear (URL-encoded).
	assert.Contains(t, got, "tc.example.com", "build/artifacts URL should appear in body")
	assert.Contains(t, got, "BUILD_TYPE")
	assert.Contains(t, got, "github.com%2Fx%2Fy%2Fcommits%2Fabc123",
		"commit URL should appear in body")
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

func TestTeamCityBuildURLEmptyWhenUnset(t *testing.T) {
	require.Empty(t, teamcityBuildURL(&Entry{}, "log", ""))
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
