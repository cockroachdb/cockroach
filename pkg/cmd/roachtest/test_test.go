// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

const OwnerUnitTest Owner = `unowned`

const defaultParallelism = 10

func mkReg(t *testing.T) testRegistryImpl {
	t.Helper()
	r, err := makeTestRegistry(spec.GCE, "", "", false /* preferSSD */)
	require.NoError(t, err)
	return r
}

func TestMatchOrSkip(t *testing.T) {
	testCases := []struct {
		filter       []string
		name         string
		tags         []string
		expected     bool
		expectedSkip string
	}{
		{nil, "foo", nil, true, ""},
		{nil, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"tag:b"}, "foo", nil, true, "[tag:b] does not match [default]"},
		{[]string{"tag:default"}, "foo", nil, true, ""},
		{[]string{"tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
		{[]string{"f"}, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"f"}, "bar", []string{"bar"}, false, ""},
		{[]string{"f", "tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"f", "tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f := newFilter(c.filter)
			spec := &TestSpec{Name: c.name, Owner: OwnerUnitTest, Tags: c.tags}
			if value := spec.matchOrSkip(f); c.expected != value {
				t.Fatalf("expected %t, but found %t", c.expected, value)
			} else if value && c.expectedSkip != spec.Skip {
				t.Fatalf("expected %s, but found %s", c.expectedSkip, spec.Skip)
			}
		})
	}
}

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: ioutil.Discard,
		Stderr: ioutil.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func TestRunnerRun(t *testing.T) {
	ctx := context.Background()
	r := mkReg(t)
	r.Add(TestSpec{
		Name:    "pass",
		Owner:   OwnerUnitTest,
		Run:     func(ctx context.Context, t test.Test, c cluster.Cluster) {},
		Cluster: r.MakeClusterSpec(0),
	})
	r.Add(TestSpec{
		Name:  "fail",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Fatal("failed")
		},
		Cluster: r.MakeClusterSpec(0),
	})

	testCases := []struct {
		filters []string
		expErr  string
	}{
		{nil, "some tests failed"},
		{[]string{"pass"}, ""},
		{[]string{"fail"}, "some tests failed"},
		{[]string{"pass|fail"}, "some tests failed"},
		{[]string{"pass", "fail"}, "some tests failed"},
		{[]string{"notests"}, "no test"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			tests := testsToRun(ctx, r, newFilter(c.filters))
			cr := newClusterRegistry()
			runner := newTestRunner(cr, r.buildVersion)

			lopt := loggingOpt{
				l:            nilLogger(),
				tee:          logger.NoTee,
				stdout:       ioutil.Discard,
				stderr:       ioutil.Discard,
				artifactsDir: "",
			}
			copt := clustersOpt{
				typ:                       roachprodCluster,
				user:                      "test_user",
				cpuQuota:                  1000,
				keepClustersOnTestFailure: false,
			}
			err := runner.Run(ctx, tests, 1, /* count */
				defaultParallelism, copt, testOpts{}, lopt)

			if !testutils.IsError(err, c.expErr) {
				t.Fatalf("expected err: %q, but found %v. Filters: %s", c.expErr, err, c.filters)
			}
		})
	}
}

type syncedBuffer struct {
	mu  syncutil.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunnerTestTimeout(t *testing.T) {
	ctx := context.Background()

	cr := newClusterRegistry()
	runner := newTestRunner(cr, version.Version{})

	var buf syncedBuffer
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       &buf,
		stderr:       &buf,
		artifactsDir: "",
	}
	copt := clustersOpt{
		typ:                       roachprodCluster,
		user:                      "test_user",
		cpuQuota:                  1000,
		keepClustersOnTestFailure: false,
	}
	test := TestSpec{
		Name:    `timeout`,
		Owner:   OwnerUnitTest,
		Timeout: 10 * time.Millisecond,
		Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			<-ctx.Done()
		},
	}
	err := runner.Run(ctx, []TestSpec{test}, 1, /* count */
		defaultParallelism, copt, testOpts{}, lopt)
	if !testutils.IsError(err, "some tests failed") {
		t.Fatalf("expected error \"some tests failed\", got: %v", err)
	}

	out := buf.String()
	timeoutRE := regexp.MustCompile(`(?m)^.*test timed out \(.*\)$`)
	if !timeoutRE.MatchString(out) {
		t.Fatalf("unable to find \"timed out\" message:\n%s", out)
	}
}

func TestRegistryPrepareSpec(t *testing.T) {
	dummyRun := func(context.Context, test.Test, cluster.Cluster) {}

	var listTests = func(t *TestSpec) []string {
		return []string{t.Name}
	}

	testCases := []struct {
		spec          TestSpec
		expectedErr   string
		expectedTests []string
	}{
		{
			TestSpec{
				Name:    "a",
				Owner:   OwnerUnitTest,
				Run:     dummyRun,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			"",
			[]string{"a"},
		},
		{
			TestSpec{
				Name:       "a",
				Owner:      OwnerUnitTest,
				MinVersion: "v2.1.0",
				Run:        dummyRun,
				Cluster:    spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			"",
			[]string{"a"},
		},
		{
			TestSpec{
				Name:       "a",
				Owner:      OwnerUnitTest,
				MinVersion: "foo",
				Run:        dummyRun,
				Cluster:    spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			"a: unable to parse min-version: invalid version string 'foo'",
			nil,
		},
		{
			TestSpec{
				Name:       "illegal *[]",
				Owner:      OwnerUnitTest,
				MinVersion: "foo",
				Run:        dummyRun,
				Cluster:    spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			`illegal \*\[\]: Name must match this regexp: `,
			nil,
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r, err := makeTestRegistry(spec.GCE, "", "", false /* preferSSD */)
			if err != nil {
				t.Fatal(err)
			}
			err = r.prepareSpec(&c.spec)
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %q, but found %q", c.expectedErr, err.Error())
			}
			if c.expectedErr == "" {
				tests := listTests(&c.spec)
				sort.Strings(tests)
				if diff := pretty.Diff(c.expectedTests, tests); len(diff) != 0 {
					t.Fatalf("unexpected tests:\n%s", strings.Join(diff, "\n"))
				}
			}
		})
	}
}

func TestRegistryMinVersion(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		buildVersion string
		expectedA    bool
		expectedB    bool
		expErr       string
	}{
		{"v1.1.0", false, false, "no test matched filters"},
		{"v2.0.0", true, false, ""},
		{"v2.1.0", true, true, ""},
	}
	for _, c := range testCases {
		t.Run(c.buildVersion, func(t *testing.T) {
			var runA, runB bool
			r := mkReg(t)
			r.Add(TestSpec{
				Name:       "a",
				Owner:      OwnerUnitTest,
				MinVersion: "v2.0.0",
				Cluster:    spec.MakeClusterSpec(spec.GCE, "", 0),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runA = true
				},
			})
			r.Add(TestSpec{
				Name:       "b",
				Owner:      OwnerUnitTest,
				MinVersion: "v2.1.0",
				Cluster:    spec.MakeClusterSpec(spec.GCE, "", 0),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runB = true
				},
			})
			if err := r.setBuildVersion(c.buildVersion); err != nil {
				t.Fatal(err)
			}
			tests := testsToRun(ctx, r, newFilter(nil))

			var buf syncedBuffer
			lopt := loggingOpt{
				l:            nilLogger(),
				tee:          logger.NoTee,
				stdout:       &buf,
				stderr:       &buf,
				artifactsDir: "",
			}
			copt := clustersOpt{
				typ:                       roachprodCluster,
				user:                      "test_user",
				cpuQuota:                  1000,
				keepClustersOnTestFailure: false,
			}
			cr := newClusterRegistry()
			runner := newTestRunner(cr, r.buildVersion)
			err := runner.Run(ctx, tests, 1, /* count */
				defaultParallelism, copt, testOpts{}, lopt)
			if !testutils.IsError(err, c.expErr) {
				t.Fatalf("expected err: %q, got: %v", c.expErr, err)
			}

			if c.expectedA != runA || c.expectedB != runB {
				t.Fatalf("expected %t,%t, but got %t,%t\n%s",
					c.expectedA, c.expectedB, runA, runB, buf.String())
			}
		})
	}
}

func runExitCodeTest(t *testing.T, injectedError error) error {
	ctx := context.Background()
	t.Helper()
	cr := newClusterRegistry()
	runner := newTestRunner(cr, version.Version{})
	r := mkReg(t)
	r.Add(TestSpec{
		Name:    "boom",
		Owner:   OwnerUnitTest,
		Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if injectedError != nil {
				t.Fatal(injectedError)
			}
		},
	})
	tests := testsToRun(ctx, r, newFilter(nil))
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       ioutil.Discard,
		stderr:       ioutil.Discard,
		artifactsDir: "",
	}
	return runner.Run(ctx, tests, 1, 1, clustersOpt{}, testOpts{}, lopt)
}

func TestExitCode(t *testing.T) {
	require.NoError(t, runExitCodeTest(t, nil /* test passes */))
	err := runExitCodeTest(t, errors.New("boom"))
	require.True(t, errors.Is(err, errTestsFailed))
}
