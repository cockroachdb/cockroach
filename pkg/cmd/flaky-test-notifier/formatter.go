// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
)

// flakyTestFormatter is an IssueFormatter for flaky test notifications.
// It generates issues that summarize flaky test occurrences across multiple
// builds.
type flakyTestFormatter struct {
	failures []TestFailure
}

// Title implements the IssueFormatter interface.
func (f *flakyTestFormatter) Title(data issues.TemplateData) string {
	// NB: The title is the same as the normal test failure GH issue title.
	// This is so the issue poster will make the post as a comment on the
	// existing GH issue, if one exists (it probably should).
	return fmt.Sprintf("%s: %s failed", data.PackageNameShort, data.TestName)
}

// Body implements the IssueFormatter interface.
func (f *flakyTestFormatter) Body(r *issues.Renderer, data issues.TemplateData) error {
	r.Escaped(fmt.Sprintf("%s.%s is flaky on %s. It is one of the top %d flakiest tests in the last %d days.\n\n",
		data.PackageNameShort, data.TestName, data.Branch, maxAlerts, *lookbackDays))

	r.Escaped("This test has failed across the following builds. Please check the links to see each failure.\n\n")

	for _, failure := range f.failures {
		r.Escaped(fmt.Sprintf("- **%s** (%.1f%% failure rate, %d total runs): ",
			failure.Build(), failure.FailureRate()*100, failure.TotalRuns()))
		links := failure.Links()
		for i, link := range links {
			if i > 0 {
				r.Escaped(", ")
			}
			r.A(fmt.Sprintf("%d", i+1), link.String())
		}
		r.Escaped("\n")
	}
	r.Escaped("\n")

	if len(data.RelatedIssues) > 0 {
		r.Collapsed("Same failure on other branches", func() {
			for _, iss := range data.RelatedIssues {
				r.Escaped(fmt.Sprintf("\n- #%d %s", iss.GetNumber(), iss.GetTitle()))
			}
			r.Escaped("\n")
		})
	}

	if len(data.MentionOnCreate) > 0 {
		r.Escaped("/cc")
		for _, handle := range data.MentionOnCreate {
			r.Escaped(" ")
			r.Escaped(handle)
		}
		r.Escaped("\n")
	}
	return nil
}
