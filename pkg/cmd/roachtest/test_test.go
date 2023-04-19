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
	"io"
	"math/rand"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

const OwnerUnitTest registry.Owner = `unowned`

const defaultParallelism = 10

func mkReg(t *testing.T) testRegistryImpl {
	t.Helper()
	return makeTestRegistry(spec.GCE, "", "", false /* preferSSD */)
}

func TestMatchOrSkip(t *testing.T) {
	testCases := []struct {
		filter   []string
		name     string
		tags     map[string]struct{}
		expected registry.MatchType
	}{
		{nil, "foo", nil, registry.Matched},
		{nil, "foo", registry.Tags("bar"), registry.Matched},
		{[]string{"tag:bar"}, "foo", registry.Tags("bar"), registry.Matched},
		// Partial tag match is not supported
		{[]string{"tag:b"}, "foo", registry.Tags("bar"), registry.FailedTags},
		{[]string{"tag:b"}, "foo", nil, registry.FailedTags},
		{[]string{"tag:f"}, "foo", registry.Tags("bar"), registry.FailedTags},
		// Specifying no tag filters matches all tags.
		{[]string{"f"}, "foo", registry.Tags("bar"), registry.Matched},
		{[]string{"f"}, "bar", registry.Tags("bar"), registry.FailedFilter},
		{[]string{"f", "tag:bar"}, "foo", registry.Tags("bar"), registry.Matched},
		{[]string{"f", "tag:b"}, "foo", registry.Tags("bar"), registry.FailedTags},
		{[]string{"f", "tag:f"}, "foo", registry.Tags("bar"), registry.FailedTags},
		// Match tests that have both tags 'abc' and 'bar'
		{[]string{"f", "tag:abc,bar"}, "foo", registry.Tags("abc", "bar"), registry.Matched},
		{[]string{"f", "tag:abc,bar"}, "foo", registry.Tags("abc"), registry.FailedTags},
		// Match tests that have tag 'abc' but not 'bar'
		{[]string{"f", "tag:abc,!bar"}, "foo", registry.Tags("abc"), registry.Matched},
		{[]string{"f", "tag:abc,!bar"}, "foo", registry.Tags("abc", "bar"), registry.FailedTags},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f := registry.NewTestFilter(c.filter, false)
			spec := &registry.TestSpec{Name: c.name, Owner: OwnerUnitTest, Tags: c.tags}
			if value := spec.Match(f); c.expected != value {
				t.Fatalf("expected %v, but found %v", c.expected, value)
			}
		})
	}
}

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func alwaysFailingClusterAllocator(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, error) {
	return nil, errors.New("cluster creation failed")
}

func alwaysSucceedingClusterAllocator(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, error) {
	res := clusterImpl{l: nilLogger(), expiration: (&spec.ClusterSpec{}).Expiration(), r: &clusterRegistry{}}
	return &res, nil
}

func TestRunnerRun(t *testing.T) {
	ctx := context.Background()

	r := mkReg(t)
	r.Add(registry.TestSpec{
		Name:    "pass",
		Owner:   OwnerUnitTest,
		Run:     func(ctx context.Context, t test.Test, c cluster.Cluster) {},
		Cluster: r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:  "fail",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Fatal("failed")
		},
		Cluster: r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:  "errors",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Errorf("first %s", "error")
			t.Errorf("second error")
		},
		Cluster: r.MakeClusterSpec(0),
	})
	r.Add(registry.TestSpec{
		Name:  "panic",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			sl := []int{0}
			// We need to throw the RoachVet linter off our scent since it's pretty
			// good at figuring out static out of bound indexing.
			idx := rand.Intn(2) + 1 // definitely out of bounds
			t.L().Printf("boom %d", sl[idx])
		},
		Cluster: r.MakeClusterSpec(0),
	})

	testCases := []struct {
		filters []string
		expErr  string
		expOut  string
	}{
		{filters: nil, expErr: "some tests failed"},
		{filters: []string{"pass"}},
		{filters: []string{"fail"}, expErr: "some tests failed"},
		{filters: []string{"pass|fail"}, expErr: "some tests failed"},
		{filters: []string{"pass", "fail"}, expErr: "some tests failed"},
		{filters: []string{"notests"}, expErr: "no test"},
		{filters: []string{"errors"}, expErr: "some tests failed", expOut: "second error"},
		{filters: []string{"panic"}, expErr: "some tests failed", expOut: "index out of range"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			runConfigs := []bool{true, false}
			var allocator clusterAllocatorFn
			// Run each test with and without cluster allocation error.
			for _, injectClusterErr := range runConfigs {
				rt := setupRunnerTest(t, r, c.filters)

				if injectClusterErr {
					allocator = alwaysFailingClusterAllocator
				} else {
					allocator = alwaysSucceedingClusterAllocator
				}
				err := rt.runner.Run(ctx, rt.tests, 1, /* count */
					defaultParallelism, rt.copt, testOpts{}, rt.lopt, allocator)
				// N.B. when len(rt.tests) == 0, no tests will be executed, hence injectClusterErr is (implicitly) disabled.
				assertTestCompletion(t, rt.tests, c.filters, rt.runner.getCompletedTests(), err, c.expErr,
					injectClusterErr && len(rt.tests) > 0)

				out := rt.stdout.String() + "\n" + rt.stderr.String()
				if !injectClusterErr {
					// In case of cluster error, the test's Run is never executed, so we skip this check.
					if exp := c.expOut; exp != "" && !strings.Contains(out, exp) {
						t.Fatalf("'%s' not found in output:\n%s", exp, out)
					}
				}
				t.Log(out)
			}
		})
	}
}

func TestRunnerEncryptionAtRest(t *testing.T) {
	// Verify that if a test opts into EncryptionMetamorphic, it will
	// (eventually) get a cluster that has encryption at rest enabled.
	{
		prevProb := encryptionProbability
		encryptionProbability = 0.5 // --metamorphic-encrypt-probability=0.5
		defer func() {
			encryptionProbability = prevProb
		}()
	}
	r := mkReg(t)
	var sawEncrypted int32 // atomic
	r.Add(registry.TestSpec{
		Name:              "enc-random",
		Owner:             OwnerUnitTest,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			encAtRest := c.(*clusterImpl).encAtRest
			t.L().Printf("encryption-at-rest=%t", encAtRest)
			if encAtRest {
				atomic.StoreInt32(&sawEncrypted, 1)
			}
		},
		Cluster: r.MakeClusterSpec(0),
	})

	rt := setupRunnerTest(t, r, nil)

	for i := 0; i < 10000; i++ {
		require.NoError(t, rt.runner.Run(
			context.Background(), rt.tests, 1 /* count */, 1, /* parallelism */
			rt.copt, testOpts{}, rt.lopt, alwaysSucceedingClusterAllocator,
		))
		if atomic.LoadInt32(&sawEncrypted) == 0 {
			// NB: since it's a 50% chance, the probability of *not* hitting
			// this branch over 10k runs is 1 - (0.5)^10000 which is essentially 1.
			continue
		}
		t.Logf("done after %d iterations", i+1)
		return
	}
	t.Fatalf("encryption at rest never randomly enabled")
}

type runnerTest struct {
	stdout, stderr *syncedBuffer // captures runner.Run
	lopt           loggingOpt
	copt           clustersOpt
	tests          []registry.TestSpec
	runner         *testRunner
}

func setupRunnerTest(t *testing.T, r testRegistryImpl, testFilters []string) *runnerTest {
	ctx := context.Background()

	tests := testsToRun(ctx, r, registry.NewTestFilter(testFilters, false))
	cr := newClusterRegistry()

	stopper := stop.NewStopper()
	t.Cleanup(func() { stopper.Stop(ctx) })
	runner := newUnitTestRunner(cr, stopper)

	var stdout syncedBuffer
	var stderr syncedBuffer
	lopt := loggingOpt{
		l: func() *logger.Logger {
			l, err := logger.RootLogger(filepath.Join(t.TempDir(), "test.log"), logger.NoTee)
			if err != nil {
				panic(err)
			}
			return l
		}(),
		tee:          logger.NoTee,
		stdout:       &stdout,
		stderr:       &stderr,
		artifactsDir: "",
	}
	copt := clustersOpt{
		typ:       roachprodCluster,
		user:      "test_user",
		cpuQuota:  1000,
		debugMode: NoDebug,
	}
	return &runnerTest{
		stdout: &stdout,
		stderr: &stderr,
		lopt:   lopt,
		copt:   copt,
		tests:  tests,
		runner: runner,
	}
}

// verifies that actual test completion conditions match the expected
func assertTestCompletion(
	t *testing.T,
	tests []registry.TestSpec,
	filters []string,
	completed []completedTestInfo,
	actualErr error,
	expectedErr string,
	injectedClusterErr bool,
) {
	t.Helper()
	require.Equal(t, len(tests), len(completed))

	for _, info := range completed {
		if !injectedClusterErr {
			if info.test == "pass" {
				require.True(t, info.pass)
			} else if info.test == "fail" {
				require.True(t, !info.pass)
			}
		} else {
			// In case of cluster error, test is expected to always fail.
			require.True(t, !info.pass)
		}
	}
	if injectedClusterErr {
		// In case of cluster error, the expected error is always of this form.
		expectedErr = "some clusters could not be created"
	}
	if !testutils.IsError(actualErr, expectedErr) {
		t.Fatalf("expected err: %q, but found %v. Filters: %s", expectedErr, actualErr, filters)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       &buf,
		stderr:       &buf,
		artifactsDir: "",
	}
	copt := clustersOpt{
		typ:       roachprodCluster,
		user:      "test_user",
		cpuQuota:  1000,
		debugMode: NoDebug,
	}
	test := registry.TestSpec{
		Name:    `timeout`,
		Owner:   OwnerUnitTest,
		Timeout: 10 * time.Millisecond,
		Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			<-ctx.Done()
		},
	}
	err := runner.Run(ctx, []registry.TestSpec{test}, 1, /* count */
		defaultParallelism, copt, testOpts{}, lopt, alwaysSucceedingClusterAllocator)
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

	var listTests = func(t *registry.TestSpec) []string {
		return []string{t.Name}
	}

	testCases := []struct {
		spec          registry.TestSpec
		expectedErr   string
		expectedTests []string
	}{
		{
			registry.TestSpec{
				Name:    "a",
				Owner:   OwnerUnitTest,
				Run:     dummyRun,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			"",
			[]string{"a"},
		},
		{
			registry.TestSpec{
				Name:    "illegal *[]",
				Owner:   OwnerUnitTest,
				Run:     dummyRun,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
			},
			`illegal \*\[\]: Name must match this regexp: `,
			nil,
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := makeTestRegistry(spec.GCE, "", "", false /* preferSSD */)
			err := r.prepareSpec(&c.spec)
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

func runExitCodeTest(t *testing.T, injectedError error) error {
	ctx := context.Background()
	t.Helper()
	cr := newClusterRegistry()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	runner := newUnitTestRunner(cr, stopper)
	r := mkReg(t)
	r.Add(registry.TestSpec{
		Name:    "boom",
		Owner:   OwnerUnitTest,
		Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if injectedError != nil {
				t.Fatal(injectedError)
			}
		},
	})
	tests := testsToRun(ctx, r, registry.NewTestFilter(nil, false))
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       io.Discard,
		stderr:       io.Discard,
		artifactsDir: "",
	}
	return runner.Run(ctx, tests, 1, 1, clustersOpt{}, testOpts{}, lopt, alwaysSucceedingClusterAllocator)
}

func TestExitCode(t *testing.T) {
	require.NoError(t, runExitCodeTest(t, nil /* test passes */))
	err := runExitCodeTest(t, errors.New("boom"))
	require.True(t, errors.Is(err, errTestsFailed))
}

func TestSetup(t *testing.T) {
	ctx := context.Background()
	t.Helper()
	cr := newClusterRegistry()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	runner := newUnitTestRunner(cr, stopper)

	testCases := []struct {
		spec                 registry.TestSpec
		expectPreSetup       bool
		expectPostSetup      bool
		expectRun            bool
		expectedStatus       string
		injectClusterFailure bool
	}{
		{
			registry.TestSpec{
				Name:    "NoSetupSuccess",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				Run:     func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, true, "pass", false,
		},
		{
			registry.TestSpec{
				Name:    "NoSetupFailure",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					t.Fatal("boom")
				},
			}, false, false, true, "fail", false,
		},
		{
			registry.TestSpec{
				Name:    "NoSetupSkip",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					t.Skip("skip")
				},
			}, false, false, true, "skip", false,
		},
		{
			registry.TestSpec{
				Name:    "PreSetupSuccess",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, true, false, true, "pass", false,
		},
		{
			registry.TestSpec{
				Name:    "PreSetupFail",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					t.Fatal("boom")
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "fail", false,
		},
		{
			registry.TestSpec{
				Name:    "PreSetupSkip",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					if t.Spec() != nil {
						t.Skip("skip")
					}
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "skip", false,
		},
		{
			registry.TestSpec{
				Name:    "PostSetupSuccess",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, true, true, "pass", false,
		},
		{
			registry.TestSpec{
				Name:    "PostSetupFail",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					t.Fatalf("boom")
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "fail", false,
		},
		{
			registry.TestSpec{
				Name:    "PostSetupSkip",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					if c.Spec().Cloud != spec.GCE {
						t.Skip("skip")
					}

					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "skip", false,
		},
		{
			registry.TestSpec{
				Name:    "PrePostSetupSuccess",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					return nil
				},
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, true, true, true, "pass", false,
		},
		{
			registry.TestSpec{
				Name:    "PrePostSetupFail",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					return errors.New("boom")
				},
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "fail", false,
		},
		{
			registry.TestSpec{
				Name:    "PrePostSetupSkip",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					t.Skip("skip")
					return nil
				},
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					return errors.New("boom")
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, false, false, false, "skip", false,
		},
		{
			registry.TestSpec{
				Name:    "PrePostSetupClusterFail",
				Owner:   OwnerUnitTest,
				Cluster: spec.MakeClusterSpec(spec.GCE, "", 0),
				PreSetup: func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
					return nil
				},
				PostSetup: func(ctx context.Context, t test.Test, c cluster.Cluster) error {
					return nil
				},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			}, true, false, false, "fail", true,
		},
	}

	for _, c := range testCases {
		require.True(t, c.spec.PreSetup != nil || c.spec.PostSetup != nil || c.spec.Run != nil, "test case %s: must define at least one of PreSetup, PostSetup, or Run", c.spec.Name)
		require.True(t, c.expectedStatus == "pass" || c.expectedStatus == "fail" || c.expectedStatus == "skip",
			"test case %s: must expect at least one of test outcomes: 'fail', 'pass', 'skip'", c.spec.Name)

		var preSetupErr error
		var preSetupCalled bool

		if c.expectPreSetup {
			preSetup := c.spec.PreSetup
			require.NotNil(t, preSetup, "test case %s: must define PreSetup", c.spec.Name)

			c.spec.PreSetup = func(ctx context.Context, t test.Test, spec *registry.TestSpec) error {
				preSetupErr = preSetup(ctx, t, spec)
				preSetupCalled = true

				return preSetupErr
			}
		}
		var postSetupErr error
		var postSetupCalled bool

		if c.expectPostSetup {
			postSetup := c.spec.PostSetup
			require.NotNil(t, postSetup, "test case %s: must define PostSetup", c.spec.Name)

			c.spec.PostSetup = func(ctx context.Context, t test.Test, cluster cluster.Cluster) error {
				postSetupErr = postSetup(ctx, t, cluster)
				postSetupCalled = true

				return postSetupErr
			}
		}
		var res *test.Test
		var runCalled bool

		if c.expectRun {
			run := c.spec.Run
			require.NotNil(t, run, "test case %s: must define Run", c.spec.Name)

			c.spec.Run = func(ctx context.Context, t test.Test, cluster cluster.Cluster) {
				res = &t
				run(ctx, t, cluster)
				runCalled = true
			}
		}
		r := mkReg(t)
		r.Add(c.spec)
		tests := testsToRun(ctx, r, registry.NewTestFilter(nil, false))

		lopt := loggingOpt{
			l:            nilLogger(),
			tee:          logger.NoTee,
			stdout:       io.Discard,
			stderr:       io.Discard,
			artifactsDir: "",
		}
		var err error
		if c.injectClusterFailure {
			err = runner.Run(ctx, tests, 1, 1, clustersOpt{}, testOpts{}, lopt, alwaysFailingClusterAllocator)
		} else {
			err = runner.Run(ctx, tests, 1, 1, clustersOpt{}, testOpts{}, lopt, alwaysSucceedingClusterAllocator)
		}

		if c.expectedStatus != "fail" {
			require.NoError(t, err, "test case %s: expected Run to return without error", c.spec.Name)
		} else {
			if c.injectClusterFailure {
				require.ErrorIs(t, err, errSomeClusterProvisioningFailed, "test case %s: expected Run to return the error", c.spec.Name)
			} else {
				require.ErrorIs(t, err, errTestsFailed, "test case %s: expected Run to return the error", c.spec.Name)
			}
		}

		if c.expectPreSetup {
			require.True(t, preSetupCalled, "test case %s: expected PreSetup to return", c.spec.Name)
			require.NoError(t, preSetupErr, "test case %s: expected PreSetup to return without an error", c.spec.Name)
		}
		if c.expectPostSetup {
			require.True(t, postSetupCalled, "test case %s: expected PostSetup to return", c.spec.Name)
			require.NoError(t, postSetupErr, "test case %s: expected PostSetup to return without an error", c.spec.Name)
		}
		if c.expectRun {
			require.NotNil(t, res, "test case %s: expected Run to be executed", c.spec.Name)
			// Verify the test outcome after executing 'Run'.
			if c.expectedStatus == "fail" {
				require.True(t, (*res).Failed(), "test case %s: expected test to fail", c.spec.Name)
			} else if c.expectedStatus == "skip" {
				require.True(t, (*res).Skipped(), "test case %s: expected test to be skipped", c.spec.Name)
			} else if c.expectedStatus == "pass" {
				require.True(t, runCalled, "test case %s: expected Run to return", c.spec.Name)
				require.False(t, (*res).Failed(), "test case %s: expected test to succeed instead of failed", c.spec.Name)
				require.False(t, (*res).Skipped(), "test case %s: expected test to succeed instead of skipped", c.spec.Name)
			}
		}
	}
}
