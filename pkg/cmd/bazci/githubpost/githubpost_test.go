// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package githubpost

import (
	"context"
	"encoding/xml"
	"os"
	"strings"
	"testing"

	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type issue struct {
	testName   string
	title      string
	message    string
	expRepro   string
	mention    []string
	labels     []string
	hasProject bool
}

func init() {
	fileLineForTesting = map[packageAndTest]fileAndLine{
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/util/stop",
			testName:    "TestStopperWithCancelConcurrent",
		}: {
			filename: "pkg/util/stop/stopper_test.go",
			linenum:  "375",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			testName:    "TestReplicateQueueRebalance",
		}: {
			filename: "pkg/kv/kvserver/replicate_queue_test.go",
			linenum:  "56",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			testName:    "TestGossipHandlesReplacedNode",
		}: {
			filename: "pkg/kv/kvserver/gossip_test.go",
			linenum:  "142",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/util/json",
			testName:    "TestPretty",
		}: {
			filename: "pkg/util/json/json_test.go",
			linenum:  "2234",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/util/json",
			testName:    "TestJSONErrors",
		}: {
			filename: "pkg/util/json/json_test.go",
			linenum:  "249",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord",
			testName:    "TestTxnCoordSenderPipelining",
		}: {
			filename: "pkg/kv/kvclient/kvcoord/txn_coord_sender_test.go",
			linenum:  "2429",
		},
		{
			packageName: "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord",
			testName:    "TestAbortReadOnlyTransaction",
		}: {
			filename: "pkg/kv/kvclient/kvcoord/txn_coord_sender_test.go",
			linenum:  "1990",
		},
	}
}

func TestListFailuresFromJSON(t *testing.T) {
	// Each test case expects a number of issues.
	testCases := []struct {
		pkgEnv    string
		extraEnv  map[string]string
		fileName  string
		expPkg    string
		expIssues []issue
		formatter Formatter
	}{
		{
			pkgEnv:   "",
			fileName: "implicit-pkg.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/util/stop",
			expIssues: []issue{{
				testName:   "TestStopperWithCancelConcurrent",
				title:      "util/stop: TestStopperWithCancelConcurrent failed",
				message:    "this is just a testing issue",
				mention:    []string{"@cockroachdb/kv"},
				labels:     []string{"C-test-failure", "release-blocker", "T-kv"},
				hasProject: true,
			}},
			formatter: DefaultFormatter,
		},
		{
			// A clone of the above but configured to skip the test failure label.
			pkgEnv:   "",
			extraEnv: map[string]string{"SKIP_LABEL_TEST_FAILURE": "1"},
			fileName: "implicit-pkg.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/util/stop",
			expIssues: []issue{{
				testName:   "TestStopperWithCancelConcurrent",
				title:      "util/stop: TestStopperWithCancelConcurrent failed",
				message:    "this is just a testing issue",
				mention:    []string{"@cockroachdb/kv"},
				labels:     []string{"T-kv"},
				hasProject: true,
			}},
			formatter: DefaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			fileName: "stress-failure.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			expIssues: []issue{{
				testName:   "TestReplicateQueueRebalance",
				title:      "kv/kvserver: TestReplicateQueueRebalance failed",
				message:    "replicate_queue_test.go:88: condition failed to evaluate within 45s: not balanced: [10 1 10 1 8]",
				mention:    []string{"@cockroachdb/kv"},
				labels:     []string{"C-test-failure", "release-blocker", "T-kv"},
				hasProject: true,
			}},
			formatter: DefaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			fileName: "stress-fatal.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv/kvserver",
			expIssues: []issue{{
				testName:   "TestGossipHandlesReplacedNode",
				title:      "kv/kvserver: TestGossipHandlesReplacedNode failed",
				message:    "F180711 20:13:15.826193 83 storage/replica.go:1877  [n?,s1,r1/1:/M{in-ax}] on-disk and in-memory state diverged:",
				mention:    []string{"@cockroachdb/kv"},
				labels:     []string{"C-test-failure", "release-blocker", "T-kv"},
				hasProject: true,
			}},
			formatter: DefaultFormatter,
		},
		{
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/storage",
			fileName: "stress-unknown.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/storage",
			expIssues: []issue{{
				testName:   "(unknown)",
				title:      "storage: package failed",
				message:    "make: *** [bin/.submodules-initialized] Error 1",
				mention:    []string{"@cockroachdb/test-eng"},
				labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
				hasProject: true,
			}},
			formatter: DefaultFormatter,
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
				mention: []string{"@cockroachdb/unowned"},
				labels:  []string{"C-test-failure", "release-blocker"},
			}},
			formatter: DefaultFormatter,
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
					mention:    []string{"@cockroachdb/kv"},
					labels:     []string{"C-test-failure", "release-blocker", "T-kv"},
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
					mention:    []string{"@cockroachdb/kv"},
					labels:     []string{"C-test-failure", "release-blocker", "T-kv"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
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
					testName: "TestXXX/sub3",
					title:    "kv: TestXXX/sub3 timed out",
					message: `Slow failing tests:
TestXXX/sub3 - 0.50s

Slow passing tests:
TestXXA - 1.00s
`,
					mention:    []string{"@cockroachdb/test-eng"},
					labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
		},
		{
			// Like the above, except this time the output comes from a stress run,
			// not from the test binary directly.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-timeout-culprit-not-found.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName: "TestXXX/sub1",
					title:    "kv: TestXXX/sub1 timed out",
					message: `Slow failing tests:
TestXXX/sub1 - 0.49s

Slow passing tests:
TestXXB - 1.01s
TestXXA - 1.00s
`,
					mention:    []string{"@cockroachdb/test-eng"},
					labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
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
					mention:    []string{"@cockroachdb/test-eng"},
					labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
		},
		{
			// A panic in a test.
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-panic.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName:   "TestXXX",
					title:      "kv: TestXXX failed",
					message:    `panic: induced panic`,
					mention:    []string{"@cockroachdb/test-eng"},
					labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
		},
		{
			// A panic outside of a test (in this case, in a package init function).
			pkgEnv:   "github.com/cockroachdb/cockroach/pkg/kv",
			fileName: "stress-init-panic.json",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/kv",
			expIssues: []issue{
				{
					testName:   "(unknown)",
					title:      "kv: package failed",
					message:    `panic: induced panic`,
					mention:    []string{"@cockroachdb/test-eng"},
					labels:     []string{"C-test-failure", "release-blocker", "T-testeng"},
					hasProject: true,
				},
			},
			formatter: DefaultFormatter,
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
					expRepro: `go test -tags 'invariants' -exec 'stress -p 1' -timeout 0 -test.v -run TestMeta$ ./internal/metamorphic -seed 1600209371838097000 -ops "uniform:5000-10000"`,
					labels:   []string{"metamorphic-failure", "C-test-failure", "release-blocker"},
				},
			},
			formatter: formatPebbleMetamorphicIssue,
		},
	}
	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			if c.extraEnv == nil {
				c.extraEnv = make(map[string]string)
			}

			c.extraEnv["PKG"] = c.pkgEnv

			for k, v := range c.extraEnv {
				t.Setenv(k, v)
			}

			file, err := os.Open(datapathutils.TestDataPath(t, c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			curIssue := 0

			f := func(ctx context.Context, f Failure) error {
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
				if exp := c.expIssues[curIssue].title; exp != f.title {
					t.Errorf("expected title %s, but got %s", exp, f.title)
				}
				if exp := c.expIssues[curIssue].message; !strings.Contains(f.testMessage, exp) {
					t.Errorf("expected message containing %s, but got:\n%s", exp, f.testMessage)
				}
				// NB: all test cases here emit a repro, but we only check it when the expectation
				// is set.
				if c.expIssues[curIssue].expRepro != "" {
					var actRepro, expRepro string
					{
						r := &issues.Renderer{}
						req.HelpCommand(r)
						actRepro = r.String()
					}
					{
						r := &issues.Renderer{}
						issues.ReproductionCommandFromString(c.expIssues[curIssue].expRepro)(r)
						expRepro = r.String()
					}
					require.Equal(t, expRepro, actRepro)
				}
				assert.Equal(t, c.expIssues[curIssue].mention, req.MentionOnCreate)
				assert.Equal(t, c.expIssues[curIssue].hasProject, req.ProjectColumnID != 0)
				assert.Equal(t, c.expIssues[curIssue].labels, req.Labels)
				// On next invocation, we'll check the next expected issue.
				curIssue++
				return nil
			}
			if err := listFailuresFromJSON(context.Background(), file, f); err != nil {
				t.Fatal(err)
			}
			if curIssue != len(c.expIssues) {
				t.Fatalf("expected %d issues, got: %d", len(c.expIssues), curIssue)
			}
		})
	}
}

func TestListFailuresFromTestXML(t *testing.T) {
	testCases := []struct {
		fileName  string
		expPkg    string
		expIssues []issue
		formatter Formatter
	}{
		{
			fileName: "basic.xml",
			expPkg:   "github.com/cockroachdb/cockroach/pkg/util/json",
			expIssues: []issue{{
				testName: "TestJSONErrors",
				title:    "util/json: TestJSONErrors failed",
				message: `=== RUN   TestJSONErrors
--- FAIL: TestJSONErrors (0.00s)
=== RUN   TestJSONErrors/frues
    json_test.go:278: expected error message to be 'trailing characters after JSON document', but was 'unable to decode JSON: invalid character 'r' in literal false (expecting 'a')'
    --- FAIL: TestJSONErrors/frues (0.00s)`,
				mention: []string{"@cockroachdb/unowned"},
			}},
			formatter: DefaultFormatter,
		},
	}

	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			content, err := os.ReadFile(datapathutils.TestDataPath(t, c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			var testXml bazelutil.TestSuites
			if err := xml.Unmarshal(content, &testXml); err != nil {
				t.Fatal(err)
			}
			curIssue := 0

			f := func(ctx context.Context, f Failure) error {
				if t.Failed() {
					return nil
				}
				if curIssue >= len(c.expIssues) {
					t.Errorf("unexpected issue filed. title: %s", f.title)
				}
				if exp := c.expPkg; exp != f.packageName {
					t.Errorf("expected package %s, but got %s", exp, f.packageName)
				}
				if exp := c.expIssues[curIssue].testName; exp != f.testName {
					t.Errorf("expected test name %s, but got %s", exp, f.testName)
				}
				if exp := c.expIssues[curIssue].title; exp != f.title {
					t.Errorf("expected title %s, but got %s", exp, f.title)
				}
				if exp := c.expIssues[curIssue].message; !strings.Contains(f.testMessage, exp) {
					t.Errorf("expected message containing %s, but got:\n%s", exp, f.testMessage)
				}
				curIssue++
				return nil
			}
			if err := listFailuresFromTestXML(context.Background(), testXml, f); err != nil {
				t.Fatal(err)
			}
			if curIssue != len(c.expIssues) {
				t.Fatalf("expected %d issues, got: %d", len(c.expIssues), curIssue)
			}
		})
	}
}

func TestPostGeneralFailure(t *testing.T) {
	testCases := []struct {
		fileName  string
		expIssues []issue
		formatter Formatter
	}{
		{
			fileName: "failed-build-output.txt",
			expIssues: []issue{{
				title:   "unexpected build failure",
				mention: []string{"@cockroachdb/unowned"},
				labels:  []string{"C-test-failure", "release-blocker", "T-testeng"},
			}},
			formatter: DefaultFormatter,
		},
	}

	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			b, err := os.ReadFile(datapathutils.TestDataPath(t, c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			curIssue := 0
			for _, issue := range c.expIssues {
				issue.message = string(b)
			}

			f := func(ctx context.Context, f Failure) error {
				if t.Failed() {
					return nil
				}
				if curIssue >= len(c.expIssues) {
					t.Errorf("unexpected issue filed. title: %s", f.title)
				}
				if exp := c.expIssues[curIssue].title; exp != f.title {
					t.Errorf("expected title %s, but got %s", exp, f.title)
				}
				if exp := c.expIssues[curIssue].message; !strings.Contains(f.testMessage, exp) {
					t.Errorf("expected message containing %s, but got:\n%s", exp, f.testMessage)
				}
				curIssue++
				return nil
			}
			postGeneralFailureImpl(string(b), f)
			if curIssue != len(c.expIssues) {
				t.Fatalf("expected %d issues, got: %d", len(c.expIssues), curIssue)
			}
		})
	}
}
