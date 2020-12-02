// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package unimplemented

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/errors"
)

func TestUnimplemented(t *testing.T) {
	testData := []struct {
		err        error
		expMsg     string
		expFeature string
		expHint    string
		expIssue   int
	}{
		{New("woo", "waa"), "unimplemented: waa", "woo", "", 0},
		{Newf("woo", "hello %s", "world"), "unimplemented: hello world", "woo", "", 0},
		{NewWithDepthf(1, "woo", "hello %s", "world"), "unimplemented: hello world", "woo", "", 0},
		{NewWithIssue(123, "waa"), "unimplemented: waa", "", "", 123},
		{NewWithIssuef(123, "hello %s", "world"), "unimplemented: hello world", "", "", 123},
		{NewWithIssueDetail(123, "waa", "woo"), "unimplemented: woo", "waa", "", 123},
		{NewWithIssueDetailf(123, "waa", "hello %s", "world"), "unimplemented: hello world", "waa", "", 123},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d: %v", i, test.err), func(t *testing.T) {
			if test.err.Error() != test.expMsg {
				t.Errorf("expected %q, got %q", test.expMsg, test.err.Error())
			}

			hints := errors.GetAllHints(test.err)
			found := 0
			for _, hint := range hints {
				if test.expHint != "" && hint == test.expHint {
					found |= 1
				}
				if test.expIssue != 0 {
					ref := fmt.Sprintf("%s\nSee: %s",
						errors.UnimplementedErrorHint, build.MakeIssueURL(test.expIssue))
					if hint == ref {
						found |= 2
					}
				}
				if strings.HasPrefix(hint, errors.UnimplementedErrorHint) {
					found |= 4
				}
			}
			if test.expHint != "" && found&1 == 0 {
				t.Errorf("expected hint %q, not found\n%+v", test.expHint, hints)
			}
			if test.expIssue != 0 && found&2 == 0 {
				t.Errorf("expected issue ref url to %d in link, not found\n%+v", test.expIssue, hints)
			}
			if found&4 == 0 {
				t.Errorf("expected standard hint introduction %q, not found\n%+v",
					errors.UnimplementedErrorHint, hints)
			}

			links := errors.GetAllIssueLinks(test.err)
			if len(links) != 1 {
				t.Errorf("expected 1 issue link, got %+v", links)
			} else {
				if links[0].Detail != test.expFeature {
					t.Errorf("expected link detail %q, got %q", test.expFeature, links[0].Detail)
				}

				if test.expIssue != 0 {
					url := build.MakeIssueURL(test.expIssue)
					if links[0].IssueURL != url {
						t.Errorf("expected link url %q, got %q", url, links[0].IssueURL)
					}
				}
			}

			keys := errors.GetTelemetryKeys(test.err)
			if len(keys) != 1 {
				t.Errorf("expected 1 telemetry key, got %+v", keys)
			} else {
				expKey := test.expFeature
				if test.expIssue > 0 {
					if expKey != "" {
						expKey = fmt.Sprintf("#%d.%s", test.expIssue, expKey)
					} else {
						expKey = fmt.Sprintf("#%d", test.expIssue)
					}
				}
				if keys[0] != expKey {
					t.Errorf("expected key %q, got %q", expKey, keys[0])
				}
			}

			if t.Failed() {
				t.Logf("while inspecting error: %+v", test.err)
			}
		})
	}
}
