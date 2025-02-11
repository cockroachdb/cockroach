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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
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

func defaultClusterOpt() clustersOpt {
	return clustersOpt{
		typ:       roachprodCluster,
		user:      "test_user",
		cpuQuota:  1000,
		debugMode: NoDebug,
	}
}

func defaultLoggingOpt(buf *syncedBuffer) loggingOpt {
	return loggingOpt{
		l:            nilLogger(),
		tee:          logger.NoTee,
		stdout:       buf,
		stderr:       buf,
		artifactsDir: "",
	}
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
	copt := defaultClusterOpt()
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
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)
	numTasks := 3
	tasksWaitGroup := sync.WaitGroup{}
	tasksWaitGroup.Add(numTasks)
	test := registry.TestSpec{
		Name:             `timeout`,
		Owner:            OwnerUnitTest,
		Timeout:          10 * time.Millisecond,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			for i := 0; i < numTasks; i++ {
				t.Go(func(taskCtx context.Context, l *logger.Logger) error {
					defer func() {
						tasksWaitGroup.Done()
					}()
					<-taskCtx.Done()
					return nil
				})
			}
			<-ctx.Done()
		},
	}
	err := runner.Run(ctx, []registry.TestSpec{test}, 1, /* count */
		defaultParallelism, copt, testOpts{}, lopt)
	if !testutils.IsError(err, "some tests failed") {
		t.Fatalf("expected error \"some tests failed\", got: %v", err)
	}

	out := buf.String()
	timeoutRE := regexp.MustCompile(`(?m)^.*test timed out \(.*\)`)
	if !timeoutRE.MatchString(out) {
		t.Fatalf("unable to find \"timed out\" message:\n%s", out)
	}

	// Ensure tasks are also canceled.
	tasksWaitGroup.Wait()
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
	var buf syncedBuffer
	lopt := defaultLoggingOpt(&buf)
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
		createMock          func(ctx context.Context, l *logger.Logger, username string, opts ...*cloud.ClusterCreateOpts) (retErr error)
		expectedCreateCalls int
	}{
		{
			"Malformed Cluster Name Error",
			func(ctx context.Context, l *logger.Logger, username string, opts ...*cloud.ClusterCreateOpts) (retErr error) {
				createCallsCounter++
				return &roachprod.MalformedClusterNameError{}
			},
			1, /* expectedCreateCalls */
		},
		{
			"Cluster Already Exists Error",
			func(ctx context.Context, l *logger.Logger, username string, opts ...*cloud.ClusterCreateOpts) (retErr error) {
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

// Regression test for: https://github.com/cockroachdb/cockroach/issues/129997
// Tests that workload nodes are assigned the same default zone as the main CRDB cluster.
func TestGCESameDefaultZone(t *testing.T) {
	ctx := context.Background()
	factory := &clusterFactory{sem: make(chan struct{}, 1)}
	cfg := clusterConfig{spec: spec.MakeClusterSpec(2, spec.WorkloadNode())}
	setStatus := func(string) {}

	defer func() {
		create = roachprod.Create
	}()

	create = func(ctx context.Context, l *logger.Logger, username string, opts ...*cloud.ClusterCreateOpts) (retErr error) {
		// Since we specified no zone for this cluster, roachtest should assign a default one for us.
		// Check that it assigns the same default zone to both the CRDB cluster and the workload node.
		require.Equal(t, len(opts), 2)
		crdbZones := opts[0].ProviderOptsContainer[gce.ProviderName].(*gce.ProviderOpts).Zones
		workloadZones := opts[1].ProviderOptsContainer[gce.ProviderName].(*gce.ProviderOpts).Zones
		require.Equal(t, crdbZones, workloadZones)
		// A bit of a workaround, we don't have a mock for registerCluster at this time which will panic if hit.
		// Instead, just return an error to return early since we already tested the code paths we care about.
		return &roachprod.ClusterAlreadyExistsError{}
	}

	testCases := []struct {
		name       string
		geo        bool
		createMock func(ctx context.Context, l *logger.Logger, username string, opts ...*cloud.ClusterCreateOpts) (retErr error)
	}{
		{
			name: "Separate GCE create calls for same cluster default to same zone",
			geo:  false,
		},
		{
			name: "Separate GCE create calls for same geo cluster default to same zones",
			geo:  true,
		},
	}

	for _, c := range testCases {
		cfg.spec.Geo = c.geo
		t.Run(c.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				_, _, _ = factory.newCluster(ctx, cfg, setStatus, true)
			}
		})
	}
}

func TestTransientErrorFallback(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)

	// Test that if a test fails with a transient error handled by the `require` package,
	// the test runner will correctly still identify it as a flake and the run will have
	// no failed tests.
	t.Run("Require API", func(t *testing.T) {
		mockTest := registry.TestSpec{
			Name:             `ssh flake`,
			Owner:            OwnerUnitTest,
			Cluster:          spec.MakeClusterSpec(0),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			CockroachBinary:  registry.StandardCockroach,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				require.NoError(t, rperrors.NewSSHError(errors.New("oops")))
			},
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		require.NoError(t, err)
	})

	// Now test that if the transient error is not handled by the `require` package,
	// but similarly lost due to casting to a string, the test runner *won't* mark
	// it as a flake and we will have a failed test.
	t.Run("Require API Not Used", func(t *testing.T) {
		mockTest := registry.TestSpec{
			Name:             `ssh flake`,
			Owner:            OwnerUnitTest,
			Cluster:          spec.MakeClusterSpec(0),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			CockroachBinary:  registry.StandardCockroach,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				err := errors.Newf("%s", rperrors.NewSSHError(errors.New("oops")))
				t.Fatal(err)
			},
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		if !testutils.IsError(err, "some tests failed") {
			t.Fatalf("expected error \"some tests failed\", got: %v", err)
		}
	})
}

func TestRunnerTasks(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)

	mockTest := registry.TestSpec{
		Name:             `mock test`,
		Owner:            OwnerUnitTest,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Go(func(taskCtx context.Context, l *logger.Logger) error {
				return errors.New("task error")
			}, task.Name("task"))
			<-ctx.Done()
		},
	}

	// If a task fails, the test runner should return an error.
	t.Run("Task Error", func(t *testing.T) {
		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Go(func(taskCtx context.Context, l *logger.Logger) error {
				return errors.New("task error")
			}, task.Name("task"))
			<-ctx.Done()
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		if !testutils.IsError(err, "some tests failed") {
			t.Fatalf("expected error \"some tests failed\", got: %v", err)
		}
	})

	// If a task panics, the test runner should return an error.
	t.Run("Task Panic", func(t *testing.T) {
		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Go(func(taskCtx context.Context, l *logger.Logger) error {
				panic("task panic")
			}, task.Name("task"))
			<-ctx.Done()
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		if !testutils.IsError(err, "some tests failed") {
			t.Fatalf("expected error \"some tests failed\", got: %v", err)
		}
	})

	// Test task termination if a test fails.
	t.Run("Terminate Failure", func(t *testing.T) {
		var tasksDone atomic.Uint32
		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Go(func(taskCtx context.Context, l *logger.Logger) error {
				defer func() {
					tasksDone.Add(1)
				}()
				<-taskCtx.Done()
				return nil
			}, task.Name("task"))
			t.Fatalf("test failed")
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		if !testutils.IsError(err, "some tests failed") {
			t.Fatalf("expected error \"some tests failed\", got: %v", err)
		}
		require.Equal(t, uint32(1), tasksDone.Load())
	})

	// Test task termination if a test fails.
	t.Run("Terminate Success", func(t *testing.T) {
		var tasksDone atomic.Uint32
		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Go(func(taskCtx context.Context, l *logger.Logger) error {
				defer func() {
					tasksDone.Add(1)
				}()
				<-taskCtx.Done()
				return nil
			}, task.Name("task"))
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		require.NoError(t, err)
		require.Equal(t, uint32(1), tasksDone.Load())
	})
}

func TestVMPreemptionPolling(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)

	mockTest := registry.TestSpec{
		Name:             `preemption`,
		Owner:            OwnerUnitTest,
		Cluster:          spec.MakeClusterSpec(0, spec.UseSpotVMs()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  registry.StandardCockroach,
		Timeout:          10 * time.Second,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			<-ctx.Done()
		},
	}

	setPollPreemptionInterval := func(interval time.Duration) {
		pollPreemptionInterval.Lock()
		defer pollPreemptionInterval.Unlock()
		pollPreemptionInterval.interval = interval
	}

	getPreemptedVMsHook = func(c cluster.Cluster, ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
		preemptedVMs := []vm.PreemptedVM{{
			Name:        "test_node",
			PreemptedAt: time.Now(),
		}}
		return preemptedVMs, nil
	}

	defer func() {
		getPreemptedVMsHook = func(c cluster.Cluster, ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
			return c.GetPreemptedVMs(ctx, l)
		}
		setPollPreemptionInterval(5 * time.Minute)
	}()

	// Test that if a VM is preempted, the VM preemption monitor will catch
	// it and cancel the test before it times out.
	t.Run("polling cancels test", func(t *testing.T) {
		setPollPreemptionInterval(50 * time.Millisecond)

		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		// The preemption monitor should mark a VM as preempted and the test should
		// be treated as a flake instead of a failed test.
		require.NoError(t, err)
	})

	// Test that if a VM is preempted but the polling doesn't catch it because the
	// test finished first, the post failure checks will check again and mark it as a flake.
	t.Run("polling doesn't catch preemption", func(t *testing.T) {
		// Set the interval very high so we don't poll for preemptions.
		setPollPreemptionInterval(1 * time.Hour)

		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Error("Should be ignored")
		}
		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)
		// The post test failure check should mark a VM as preempted and the test should
		// be treated as a flake instead of a failed test.
		require.NoError(t, err)
	})

	// Test that if VM preemption polling finds a preempted VM but the post test failure
	// check doesn't, the test is still marked as a flake.
	t.Run("post test check doesn't catch preemption", func(t *testing.T) {
		setPollPreemptionInterval(10 * time.Millisecond)
		testPreemptedCh := make(chan struct{})
		getPreemptedVMsHook = func(c cluster.Cluster, ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
			preemptedVMs := []vm.PreemptedVM{{
				Name:        "test_node",
				PreemptedAt: time.Now(),
			}}
			close(testPreemptedCh)
			return preemptedVMs, nil
		}

		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			defer func() {
				getPreemptedVMsHook = func(c cluster.Cluster, ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
					return nil, nil
				}
			}()
			// Make sure the preemption polling is called and the test context is cancelled
			// before unblocking. Under stress, the test may time out before the preemption
			// check is called otherwise.
			<-testPreemptedCh
			<-ctx.Done()
		}

		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)

		require.NoError(t, err)
	})

	// Test that if the test hangs until timeout, a VM preemption will still be caught.
	t.Run("test hangs and still catches preemption", func(t *testing.T) {
		// We don't want the polling to cancel the test early.
		setPollPreemptionInterval(10 * time.Minute)
		getPreemptedVMsHook = func(c cluster.Cluster, ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
			preemptedVMs := []vm.PreemptedVM{{
				Name:        "test_node",
				PreemptedAt: time.Now(),
			}}
			return preemptedVMs, nil
		}

		mockTest.Timeout = 10 * time.Millisecond
		// We expect the following to occur:
		//	1. The test blocks on the context, which is only cancelled when the test runner
		//		 returns after test completion. This effectively blocks the test forever.
		//  2. The test times out and the test runner marks it as failed.
		//  3. Normally, this would result in a failed test and runner.Run returning an error.
		//     However, because we injected a preemption, the test runner marks it as a flake
		//     instead and returns no errors.
		mockTest.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			<-ctx.Done()
		}

		err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
			defaultParallelism, copt, testOpts{}, lopt)

		require.NoError(t, err)
	})
}

// TestRunnerFailureAfterTimeout checks that a test has a failure added
// after the test has timed out works as expected.
//
// Specifically, this is a regression test that replacing the test logger
// for post test artifacts collection or assertion checks is atomic and
// doesn't race with the logger potentially still being used by the test.
func TestRunnerFailureAfterTimeout(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)
	test := registry.TestSpec{
		Name:  `timeout`,
		Owner: OwnerUnitTest,
		// Set the timeout very low so we can observe the timeout
		// and error racing.
		Timeout:          1 * time.Nanosecond,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Error("test failed")
		},
	}
	err := runner.Run(ctx, []registry.TestSpec{test}, 1, /* count */
		defaultParallelism, copt, testOpts{}, lopt)
	require.Error(t, err)
}
