// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListFailures(t *testing.T) {
	type issue struct {
		testName   string
		title      string
		message    string
		author     string
		expRepro   string
		mention    []string
		hasProject bool
	}
	// Each test case expects a number of issues.
	testCases := []struct {
		pkgEnv    string
		fileName  string
		expPkg    string
		expIssues []issue
		formatter formatter
	}{
		{
			pkgEnv:   "",
			fileName: "implicit-pkg.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/util/stop",
			expIssues: []issue{{
				testName:   "TestStopperWithCancelConcurrent",
				title:      "util/stop: TestStopperWithCancelConcurrent failed",
				message:    "this is just a testing issue",
				author:     "nvanbenschoten@gmail.com",
				mention:    []string{"@cockroachdb/server"},
				hasProject: true,
			}},
			formatter: defaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			fileName: "stress-failure.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			expIssues: []issue{{
				testName:   "TestReplicateQueueRebalance",
				title:      "kv/kvserver: TestReplicateQueueRebalance failed",
				message:    "replicate_queue_test.go:88: condition failed to evaluate within 45s: not balanced: [10 1 10 1 8]",
				author:     "petermattis@gmail.com",
				mention:    []string{"@cockroachdb/kv"},
				hasProject: true,
			}},
			formatter: defaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			fileName: "stress-fatal.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			expIssues: []issue{{
				testName:   "TestGossipHandlesReplacedNode",
				title:      "kv/kvserver: TestGossipHandlesReplacedNode failed",
				message:    "F180711 20:13:15.826193 83 storage/replica.go:1877  [n?,s1,r1/1:/M{in-ax}] on-disk and in-memory state diverged:",
				author:     "alexdwanerobinson@gmail.com",
				mention:    []string{"@cockroachdb/kv"},
				hasProject: true,
			}},
			formatter: defaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/storage",
			fileName: "stress-unknown.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/storage",
			expIssues: []issue{{
				testName: "(unknown)",
				title:    "storage: package failed",
				message:  "make: *** [bin/.submodules-initialized] Error 1",
				author:   "",
			}},
			formatter: defaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/util/json",
			fileName: "stress-subtests.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/util/json",
			expIssues: []issue{{
				testName: "TestPretty",
				title:    "util/json: TestPretty failed",
				message: `=== RUN   TestPretty/["hello",_["world"]]
    --- FAIL: TestPretty/["hello",_["world"]] (0.00s)
    	json_test.go:1656: injected failure`,
				author:  "justin@cockroachlabs.com",
				mention: []string{"@cockroachdb/unowned"},
			}},
			formatter: defaultFormatter,
		},
		{
			// A test run where there's a timeout, and the timed out test was the
			// longest running test, so the issue assumes it's the culprit.
			// To spice things up, the test run has another test failure too.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord",
			fileName: "timeout-culprit-found.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord",
			expIssues: []issue{
				{
					testName:   "TestTxnCoordSenderPipelining",
					title:      "kv/kvclient/kvcoord: TestTxnCoordSenderPipelining failed",
					message:    `injected failure`,
					author:     "nikhil.benesch@gmail.com",
					mention:    []string{"@cockroachdb/kv"},
					hasProject: true,
				},
				{
					testName: "TestAbortReadOnlyTransaction",
					title:    "kv/kvclient/kvcoord: TestAbortReadOnlyTransaction timed out",
					message: `Slow failing tests:
TestAbortReadOnlyTransaction - 3.99s
TestTxnCoordSenderPipelining - 1.00s

Slow passing tests:
TestAnchorKey - 1.01s
`,
					author:     "andrei@cockroachlabs.com",
					mention:    []string{"@cockroachdb/kv"},
					hasProject: true,
				},
			},
			formatter: defaultFormatter,
		},
		{
			// A test run where there's a timeout, but the test that happened to be
			// running when the timeout hit has not been running for very long, and so
			// the issue just names the package.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "timeout-culprit-not-found.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package timed out",
					message: `Slow failing tests:
TestXXX/sub3 - 0.50s

Slow passing tests:
TestXXA - 1.00s
`,
					author: "",
				},
			},
			formatter: defaultFormatter,
		},
		{
			// Like the above, except this time the output comes from a stress run,
			// not from the test binary directly.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-timeout-culprit-not-found.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package timed out",
					message: `Slow failing tests:
TestXXX/sub1 - 0.49s

Slow passing tests:
TestXXB - 1.01s
TestXXA - 1.00s
`,
					author: "",
				},
			},
			formatter: defaultFormatter,
		},
		{
			// A stress timeout where the test running when the timeout is hit is the
			// longest.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-timeout-culprit-found.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "TestXXX/sub2",
					title:    "kv: TestXXX/sub2 timed out",
					message: `Slow failing tests:
TestXXX/sub2 - 2.99s

Slow passing tests:
TestXXB - 1.01s
TestXXA - 1.00s
`,
					author: "",
				},
			},
			formatter: defaultFormatter,
		},
		{
			// A panic in a test.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-panic.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "TestXXX",
					title:    "kv: TestXXX failed",
					message:  `panic: induced panic`,
					author:   "",
				},
			},
			formatter: defaultFormatter,
		},
		{
			// A panic outside of a test (in this case, in a package init function).
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-init-panic.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package failed",
					message:  `panic: induced panic`,
					author:   "",
				},
			},
			formatter: defaultFormatter,
		},
		{
			// The Pebble metamorphic issue formatter.
			pkgEnv:   "internal/metamorphic",
			fileName: "pebble-metamorphic-panic.json",
			expPkg:   "internal/metamorphic",
			expIssues: []issue{
				{
					testName: "TestMeta",
					title:    "internal/metamorphic: TestMeta failed",
					message:  "panic: induced panic",
					expRepro: "go test -mod=vendor -tags 'invariants' -exec 'stress -p 1' -timeout 0 -test.v -run TestMeta$ ./internal/metamorphic -seed 1600209371838097000",
				},
			},
			formatter: formatPebbleMetamorphicIssue,
		},
	}
	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			if err := os.Setenv("PKG", c.pkgEnv); err != nil {
				t.Fatal(err)
			}

			file, err := os.Open(filepath.Join("testdata", c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			curIssue := 0

			f := func(ctx context.Context, f failure) error {
				if t.Failed() {
					return nil
				}
				_, req := c.formatter(ctx, f)

				if curIssue >= len(c.expIssues) {
					t.Errorf("unexpected issue filed. title: %s", f.title)
				}
				if exp := c.expPkg; exp != f.packageName {
					t.Errorf("expected package %s, but got %s", exp, f.packageName)
				}
				if exp := c.expIssues[curIssue].testName; exp != f.testName {
					t.Errorf("expected test name %s, but got %s", exp, f.testName)
				}
				if exp := c.expIssues[curIssue].author; exp != "" && exp != req.AuthorEmail {
					t.Errorf("expected author %s, but got %s", exp, req.AuthorEmail)
				}
				if exp := c.expIssues[curIssue].title; exp != f.title {
					t.Errorf("expected title %s, but got %s", exp, f.title)
				}
				if exp := c.expIssues[curIssue].message; !strings.Contains(f.testMessage, exp) {
					t.Errorf("expected message containing %s, but got:\n%s", exp, f.testMessage)
				}
				if exp := c.expIssues[curIssue].expRepro; exp != "" && exp != req.ReproductionCommand {
					t.Errorf("expected reproduction %q, but got:\n%q", exp, req.ReproductionCommand)
				}
				assert.Equal(t, c.expIssues[curIssue].mention, req.Mention)
				assert.Equal(t, c.expIssues[curIssue].hasProject, req.ProjectColumnID != 0)
				// On next invocation, we'll check the next expected issue.
				curIssue++
				return nil
			}
			if err := listFailures(context.Background(), file, f); err != nil {
				t.Fatal(err)
			}
			if curIssue != len(c.expIssues) {
				t.Fatalf("expected %d issues, got: %d", len(c.expIssues), curIssue)
			}
		})
	}
}
