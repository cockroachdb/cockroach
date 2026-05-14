// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
)

// releaseNotesAPIURL is the docs team's release-notes-automation endpoint.
// Posting here kicks off the cloud release-notes draft workflow on their
// side. There is no separate non-prod endpoint, so dry runs skip the call
// entirely (see pickSHARunner.processCandidate).
const releaseNotesAPIURL = "https://us-central1-release-notes-automation-prod.cloudfunctions.net/release-notes-api/generate-release-epic"

// envReleaseNotesAPIKey is the env var that holds the X-API-Key value for
// the release-notes-automation endpoint. The pick-sha workflow fetches it
// from Secret Manager (gha-releases-release-notes-api-key); pick-sha
// requires it at startup.
const envReleaseNotesAPIKey = "RELEASE_NOTES_API_KEY"

// docsSubtaskMatch is the (case-insensitive) substring used to find the
// docs subtask whose key is forwarded to the release-notes API as
// docs_ticket. Subtask summaries are created by the same Jira automation
// that owns the release ticket template.
const docsSubtaskMatch = "[Docs] Generate release notes"

// releaseNotesPayload is the per-release information the docs API needs to
// open the cloud release-notes draft. Field names and JSON shape mirror the
// Superblocks payload this replaces; do not rename without coordinating
// with the docs team.
type releaseNotesPayload struct {
	CurrentRelease string `json:"current_release"` // e.g. v25.4.0
	ReleaseDate    string `json:"release_date"`    // YYYY-MM-DD
	ReleaseSHA     string `json:"release_sha"`
	Cloud          bool   `json:"cloud"`
	DocsTicket     string `json:"docs_ticket"`
}

// postReleaseNotes POSTs payload to url with the X-API-Key header. Returns
// nil on a 2xx response, otherwise an error containing the response status
// and (truncated) body. Caller is expected to treat failures as a warning,
// not a fatal error: the rest of pick-sha has already succeeded and the
// docs team can re-trigger manually. URL is injected (rather than reading
// the constant directly) so tests can point at an httptest server.
func postReleaseNotes(url, apiKey string, p releaseNotesPayload) error {
	body, err := json.Marshal(map[string]interface{}{
		"ReleaseNotesPayload": p,
	})
	if err != nil {
		return errors.Wrap(err, "marshalling payload")
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "building request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	client := httputil.NewClientWithTimeout(30 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "posting to release-notes API")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return errors.Newf("release-notes API returned %s: %s", resp.Status, respBody)
	}
	return nil
}
