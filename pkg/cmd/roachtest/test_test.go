// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	return makeTestRegistry()
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

func TestRunnerRun(t *testing.T) {
	ctx := context.Background()

	r := mkReg(t)
	r.Add(registry.TestSpec{
		Name:             "pass",
		Owner:            OwnerUnitTest,
		Run:              func(ctx context.Context, t test.Test, c cluster.Cluster) {},
		Cluster:          r.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
	})
	r.Add(registry.TestSpec{
		Name:  "fail",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Fatal("failed")
		},
		Cluster:          r.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
	})
	r.Add(registry.TestSpec{
		Name:  "errors",
		Owner: OwnerUnitTest,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Errorf("first %s", "error")
			t.Errorf("second error")
		},
		Cluster:          r.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
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
		Cluster:          r.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
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
			rt := setupRunnerTest(t, r, c.filters)

			const count = 1
			err := rt.runner.Run(ctx, rt.tests, count, defaultParallelism, rt.copt, testOpts{}, rt.lopt)

			assertTestCompletion(t, rt.tests, c.filters, rt.runner.getCompletedTests(), err, c.expErr)

			// N.B. skip the case of no matching tests
			if len(rt.tests) > 0 {
				// run _with_ cluster allocator error injection
				copt := rt.copt
				copt.preAllocateClusterFn = func(ctx context.Context, t registry.TestSpec, arch vm.CPUArch) error {
					return errors.New("cluster creation failed")
				}
				err = rt.runner.Run(ctx, rt.tests, count, defaultParallelism, copt, testOpts{}, rt.lopt)

				assertTestCompletion(t,
					rt.tests, c.filters, rt.runner.getCompletedTests(),
					err, "some clusters could not be created",
				)
			}
			out := rt.stdout.String() + "\n" + rt.stderr.String()
			if exp := c.expOut; exp != "" && !strings.Contains(out, exp) {
				t.Fatalf("'%s' not found in output:\n%s", exp, out)
			}
			t.Log(out)
		})
	}
}

func TestRunnerEncryptionAtRest(t *testing.T) {
	// Verify that if a test opts into EncryptionMetamorphic, it will
	// (eventually) get a cluster that has encryption at rest enabled.
	{
		prevProb := roachtestflags.EncryptionProbability
		roachtestflags.EncryptionProbability = 0.5 // --metamorphic-encrypt-probability=0.5
		defer func() {
			roachtestflags.EncryptionProbability = prevProb
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
		Cluster:          r.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
	})

	rt := setupRunnerTest(t, r, nil)

	for i := 0; i < 10000; i++ {
		require.NoError(t, rt.runner.Run(
			context.Background(), rt.tests, 1 /* count */, 1, /* parallelism */
			rt.copt, testOpts{}, rt.lopt,
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

	tf, err := registry.NewTestFilter(testFilters)
	require.NoError(t, err)

	tests, _ := testsToRun(r, tf, false, 1.0, true)
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
) {
	t.Helper()

	if !testutils.IsError(actualErr, expectedErr) {
		t.Fatalf("expected err: %q, but found %v. Filters: %s", expectedErr, actualErr, filters)
	}

	require.Equal(t, len(tests), len(completed), "len(completed) invalid")

	for i, info := range completed {
		if info.test == "pass" {
			require.Truef(t, info.pass, "expected test %s to pass", tests[i].Name)
		} else if info.test == "fail" {
			require.Falsef(t, info.pass, "expected test %s to fail", tests[i].Name)
		}
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
		Name:             `timeout`,
		Owner:            OwnerUnitTest,
		Timeout:          10 * time.Millisecond,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			<-ctx.Done()
		},
	}
	err := runner.Run(ctx, []registry.TestSpec{test}, 1, /* count */
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
				Name:             "a",
				Owner:            OwnerUnitTest,
				Run:              dummyRun,
				Cluster:          spec.MakeClusterSpec(0),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Nightly),
			},
			"",
			[]string{"a"},
		},
		{
			registry.TestSpec{
				Name:             "illegal *[]",
				Owner:            OwnerUnitTest,
				Run:              dummyRun,
				Cluster:          spec.MakeClusterSpec(0),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Nightly),
			},
			`illegal \*\[\]: Name must match this regexp: `,
			nil,
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			r := makeTestRegistry()
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
		Name:             "boom",
		Owner:            OwnerUnitTest,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if injectedError != nil {
				t.Fatal(injectedError)
			}
		},
	})
	tf, err := registry.NewTestFilter(nil)
	require.NoError(t, err)

	tests, _ := testsToRun(r, tf, false, 1.0, true)
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       io.Discard,
		stderr:       io.Discard,
		artifactsDir: "",
	}
	return runner.Run(ctx, tests, 1, 1, clustersOpt{}, testOpts{}, lopt)
}

func TestExitCode(t *testing.T) {
	require.NoError(t, runExitCodeTest(t, nil /* test passes */))
	err := runExitCodeTest(t, errors.New("boom"))
	require.True(t, errors.Is(err, errTestsFailed))
}

func TestNewCluster(t *testing.T) {
	ctx := context.Background()
	factory := &clusterFactory{sem: make(chan struct{}, 1)}
	cfg := clusterConfig{spec: spec.MakeClusterSpec(1)}
	setStatus := func(string) {}

	defer func() {
		create = roachprod.Create
	}()

	var createCallsCounter int

	testCases := []struct {
		name                string
		createMock          func(ctx context.Context, l *logger.Logger, username string, numNodes int, createVMOpts vm.CreateOpts, providerOptsContainer vm.ProviderOptionsContainer) (retErr error)
		expectedCreateCalls int
	}{
		{
			"Malformed Cluster Name Error",
			func(ctx context.Context, l *logger.Logger, username string, numNodes int, createVMOpts vm.CreateOpts, providerOptsContainer vm.ProviderOptionsContainer) (retErr error) {
				createCallsCounter++
				return &roachprod.MalformedClusterNameError{}
			},
			1, /* expectedCreateCalls */
		},
		{
			"Cluster Already Exists Error",
			func(ctx context.Context, l *logger.Logger, username string, numNodes int, createVMOpts vm.CreateOpts, providerOptsContainer vm.ProviderOptionsContainer) (retErr error) {
				createCallsCounter++
				return &roachprod.ClusterAlreadyExistsError{}
			},
			1, /* expectedCreateCalls */
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			createCallsCounter = 0
			create = c.createMock
			_, _, err := factory.newCluster(ctx, cfg, setStatus, true)
			require.Error(t, err)
			require.Equal(t, c.expectedCreateCalls, createCallsCounter)
		})
	}
}
