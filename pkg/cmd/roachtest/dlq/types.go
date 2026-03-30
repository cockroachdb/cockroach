// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"fmt"
	"strings"
	"time"
)

// DLQEntry is the JSON-serializable record persisted to GCS when a GitHub
// issue post fails. It captures all the data needed to reconstruct the
// issues.PostRequest and issues.Options for replay.
//
// This struct's schema must remain compatible across all active release
// branches, since entries written by one branch may be replayed by a
// replay function built from another. Only add new fields; do not remove
// or change the semantics of existing ones.
type DLQEntry struct {
	FailedAt     time.Time `json:"failed_at"`
	FailureError string    `json:"failure_error"`

	// PostRequest fields (all serializable fields from issues.PostRequest).
	PackageName             string            `json:"package_name"`
	TestName                string            `json:"test_name"`
	Labels                  []string          `json:"labels"`
	AdoptIssueLabelMatchSet []string          `json:"adopt_issue_label_match_set"`
	TopLevelNotes           []string          `json:"top_level_notes"`
	Message                 string            `json:"message"`
	ExtraParams             map[string]string `json:"extra_params"`
	Artifacts               string            `json:"artifacts"`
	MentionOnCreate         []string          `json:"mention_on_create"`

	// HelpCommand reconstruction inputs. These allow regenerating the
	// HelpCommand function at replay time.
	HelpTestName    string    `json:"help_test_name"`
	HelpClusterName string    `json:"help_cluster_name"`
	HelpCloud       string    `json:"help_cloud"`
	HelpStart       time.Time `json:"help_start"`
	HelpEnd         time.Time `json:"help_end"`
	HelpRunID       string    `json:"help_run_id"`

	// Options fields (token excluded — read from env at replay time).
	Org           string `json:"org"`
	Repo          string `json:"repo"`
	SHA           string `json:"sha"`
	Branch        string `json:"branch"`
	BinaryVersion string `json:"binary_version"`

	// TeamCity CI options (populated when the original run was on TeamCity).
	TeamCityBuildTypeID string `json:"tc_build_type_id,omitempty"`
	TeamCityBuildID     string `json:"tc_build_id,omitempty"`
	TeamCityServerURL   string `json:"tc_server_url,omitempty"`
	TeamCityTags        string `json:"tc_tags,omitempty"`
	TeamCityGoflags     string `json:"tc_goflags,omitempty"`
}

// ObjectKey returns the GCS object key for this entry.
// Format: failed/{branch}/{YYYYMMDD}/{testName}-{unix_nanos}.json
func ObjectKey(entry *DLQEntry) string {
	sanitized := strings.ReplaceAll(entry.TestName, "/", "_")
	return fmt.Sprintf("failed/%s/%s/%s-%d.json",
		entry.Branch,
		entry.FailedAt.Format("20060102"),
		sanitized,
		entry.FailedAt.UnixNano(),
	)
}
