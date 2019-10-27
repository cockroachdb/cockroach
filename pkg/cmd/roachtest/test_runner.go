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
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/logtags"
	"github.com/petermattis/goid"
	"github.com/pkg/errors"
)

// testRunner runs tests.
type testRunner struct {
	// buildVersion is the version of the Cockroach binary that tests will run against.
	buildVersion version.Version

	config struct {
		// skipClusterValidationOnAttach skips validation on existing clusters that
		// the registry uses for running tests.
		skipClusterValidationOnAttach bool
		// skipClusterStopOnAttach skips stopping existing clusters that
		// the registry uses for running tests. It implies skipClusterWipeOnAttach.
		skipClusterStopOnAttach bool
		skipClusterWipeOnAttach bool
	}

	status struct {
		syncutil.Mutex
		running map[*test]struct{}
		pass    map[*test]struct{}
		fail    map[*test]struct{}
		skip    map[*test]struct{}
	}

	// cr keeps track of all live clusters.
	cr *clusterRegistry

	workersMu struct {
		syncutil.Mutex
		// workers is a map from worker name to information about each worker currently running tests.
		workers map[string]*workerStatus
	}

	// work maintains the remaining tests to run.
	work *workPool

	completedTestsMu struct {
		syncutil.Mutex
		// completed maintains information on all completed test runs.
		completed []completedTestInfo
	}
}

// newTestRunner constructs a testRunner.
//
// cr: The cluster registry with which all clusters will be registered. The
//   caller provides this as the caller needs to be able to shut clusters down
//   on Ctrl+C.
// buildVersion: The version of the Cockroach binary against which tests will run.
func newTestRunner(cr *clusterRegistry, buildVersion version.Version) *testRunner {
	r := &testRunner{
		cr:           cr,
		buildVersion: buildVersion,
	}
	r.config.skipClusterWipeOnAttach = !clusterWipe
	r.workersMu.workers = make(map[string]*workerStatus)
	return r
}

// clustersOpt groups options for the clusters to be used by the tests.
type clustersOpt struct {
	// The type of cluster to use. If localCluster, then no other fields can be
	// set.
	typ clusterType

	// If set, all the tests will run against this roachprod cluster.
	clusterName string

	// If set, all the clusters will use this ID as part of their name. When
	// roachtests is invoked by TeamCity, this will be the build id.
	clusterID string
	// The name of the user running the tests. This will be part of cluster names.
	user string

	// cpuQuota specifies how many CPUs can be used concurrently by the roachprod
	// clusters. While there's no quota available for creating a new cluster, the
	// test runner will wait for other tests to finish and their cluster to be
	// destroyed (or reused). Note that this limit is global, not per zone.
	cpuQuota int
	// If set, clusters will not be wiped or destroyed when a test using the
	// respective cluster fails. These cluster will linger around and they'll
	// continue counting towards the cpuQuota.
	keepClustersOnTestFailure bool
}

func (c clustersOpt) validate() error {
	if c.typ == localCluster {
		if c.clusterName != "" {
			return errors.New("clusterName cannot be set when typ=localCluster")
		}
		if c.clusterID != "" {
			return errors.New("clusterID cannot be set when typ=localCluster")
		}
	}
	return nil
}

// Run runs tests.
//
// Args:
// tests: The tests to run.
// count: How many times to run each test selected by filter.
// parallelism: How many workers to use for running tests. Tests are run
//   locally (although generally they run against remote roachprod clusters).
//   parallelism bounds the maximum number of tests that run concurrently. Note
//   that the concurrency is also affected by cpuQuota.
// clusterOpt: Options for the clusters to use by tests.
// lopt: Options for logging.
func (r *testRunner) Run(
	ctx context.Context,
	tests []testSpec,
	count int,
	parallelism int,
	clustersOpt clustersOpt,
	artifactsDir string,
	lopt loggingOpt,
) error {
	// Validate options.
	if len(tests) == 0 {
		return fmt.Errorf("no test matched filters")
	}
	if err := clustersOpt.validate(); err != nil {
		return err
	}
	if parallelism != 1 {
		if clustersOpt.clusterName != "" {
			return fmt.Errorf("--cluster incompatible with --parallelism. Use --parallelism=1")
		}
		if clustersOpt.typ == localCluster {
			return fmt.Errorf("--local incompatible with --parallelism. Use --parallelism=1")
		}
	}

	if name := clustersOpt.clusterName; name != "" {
		// Since we were given a cluster, check that all tests we have to run have compatible specs.
		// We should also check against the spec of the cluster, but we don't
		// currently have a way of doing that; we're relying on the fact that attaching to the cluster
		// will fail if the cluster is incompatible.
		spec := tests[0].Cluster
		spec.Lifetime = 0
		for i := 1; i < len(tests); i++ {
			spec2 := tests[i].Cluster
			spec2.Lifetime = 0
			if spec != spec2 {
				return errors.Errorf("cluster specified but found tests "+
					"with incompatible specs: %s (%s) - %s (%s)",
					tests[0].Name, spec, tests[i].Name, spec2,
				)
			}
		}
	}

	var numConcurrentClusterCreations int
	if cloud == "aws" {
		// AWS has ridiculous API calls limits, so we're going to create one cluster
		// at a time. Internally, roachprod has throttling for the calls required to
		// create a single cluster.
		numConcurrentClusterCreations = 1
	} else {
		numConcurrentClusterCreations = 1000
	}
	clusterFactory := newClusterFactory(
		clustersOpt.user, clustersOpt.clusterID, lopt.artifactsDir, r.cr, numConcurrentClusterCreations)

	// allocateCluster will be used by workers to create new clusters (or to attach
	// to an existing one).
	allocateCluster := func(
		ctx context.Context,
		t testSpec,
		alloc *quotapool.IntAlloc,
		artifactsDir string,
		wStatus *workerStatus,
	) (*cluster, error) {
		wStatus.SetStatus("creating cluster")
		defer wStatus.SetStatus("")

		lopt.l.PrintfCtx(ctx, "Creating new cluster for test %s: %s", t.Name, t.Cluster)

		existingClusterName := clustersOpt.clusterName
		if existingClusterName != "" {
			// Logs for attaching to a cluster go to a dedicated log file.
			logPath := filepath.Join(artifactsDir, runnerLogsDir, "cluster-create", existingClusterName+".log")
			clusterL, err := rootLogger(logPath, lopt.tee)
			defer clusterL.close()
			if err != nil {
				return nil, err
			}
			opt := attachOpt{
				skipValidation: r.config.skipClusterValidationOnAttach,
				skipStop:       r.config.skipClusterStopOnAttach,
				skipWipe:       r.config.skipClusterWipeOnAttach,
			}
			return attachToExistingCluster(ctx, existingClusterName, clusterL, t.Cluster, opt, r.cr)
		}

		cfg := clusterConfig{
			spec:         t.Cluster,
			artifactsDir: artifactsDir,
			localCluster: clustersOpt.typ == localCluster,
			alloc:        alloc,
		}
		return clusterFactory.newCluster(ctx, cfg, wStatus.SetStatus, lopt.tee)
	}

	// Seed the default rand source so that different runs get different cluster
	// IDs.
	rand.Seed(timeutil.Now().UnixNano())

	n := len(tests)
	if n*count < parallelism {
		// Don't spin up more workers than necessary.
		parallelism = n * count
	}

	r.status.running = make(map[*test]struct{})
	r.status.pass = make(map[*test]struct{})
	r.status.fail = make(map[*test]struct{})
	r.status.skip = make(map[*test]struct{})

	r.work = newWorkPool(tests, count)
	stopper := stop.NewStopper()
	errs := &workerErrors{}

	qp := quotapool.NewIntPool("cloud cpu", uint64(clustersOpt.cpuQuota))
	l := lopt.l

	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		stopper.RunWorker(ctx, func(ctx context.Context) {
			defer wg.Done()

			if err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, r.work, qp,
				stopper.ShouldQuiesce(),
				clustersOpt.keepClustersOnTestFailure,
				lopt.artifactsDir, lopt.runnerLogPath, lopt.tee, lopt.stdout,
				allocateCluster,
				l,
			); err != nil {
				// A worker returned an error. Let's shut down.
				msg := fmt.Sprintf("Worker %d returned with error. Quiescing. Error: %s", i, err)
				shout(ctx, l, lopt.stdout, msg)
				errs.AddErr(err)
				// Quiesce the stopper. This will cause all workers to not pick up more
				// tests after finishing the currently running one.
				stopper.Quiesce(ctx)
				// Interrupt everybody waiting for resources.
				if qp != nil {
					qp.Close(msg)
				}
			}
		})
	}

	// Wait for all the workers to finish.
	wg.Wait()
	r.cr.destroyAllClusters(ctx, l)

	if errs.Err() != nil {
		shout(ctx, l, lopt.stdout, "FAIL (err: %s)", errs.Err())
		return errs.Err()
	}
	passFailLine := r.generateReport()
	shout(ctx, l, lopt.stdout, passFailLine)
	if len(r.status.fail) > 0 {
		return fmt.Errorf("some tests failed")
	}
	return nil
}

type clusterAllocatorFn func(
	ctx context.Context,
	t testSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*cluster, error)

// runWorker runs tests in a loop until work is exhausted.
//
// Errors are returned in exceptional circumstances, like when a cluster failed
// to be created or when a test timed out and failed to react to its
// cancellation. Upon return, an attempt is performed to destroy the cluster used
// by this worker. If an error is returned, we might have "leaked" cpu quota
// because the cluster destruction might have failed but we've still released
// the quota. Also, we might have "leaked" a test goroutine (in the test
// nonresponsive to timeout case) which might still be running and doing
// arbitrary things to the cluster it was using.
//
// Args:
// name: The worker's name, to be used as a prefix for log messages.
// artifactsRootDir: The artifacts dir. Each test's logs are going to be under a
//   run_<n> dir. If empty, test log files will not be created.
// testRunnerLogPath: The path to the test runner's log. It will be copied to
//  	failing tests' artifacts dir if running under TeamCity.
// stdout: The Writer to use for messages that need to go to stdout (e.g. the
// 	 "=== RUN" and "--- FAIL" lines).
// teeOpt: The teeing option for future test loggers.
// l: The logger to use for more verbose messages.
func (r *testRunner) runWorker(
	ctx context.Context,
	name string,
	work *workPool,
	qp *quotapool.IntPool,
	interrupt <-chan struct{},
	debug bool,
	artifactsRootDir string,
	testRunnerLogPath string,
	teeOpt teeOptType,
	stdout io.Writer,
	allocateCluster clusterAllocatorFn,
	l *logger,
) error {
	ctx = logtags.AddTag(ctx, name, nil /* value */)
	wStatus := r.addWorker(ctx, name)
	defer func() {
		r.removeWorker(ctx, name)
	}()

	var c *cluster // The cluster currently being used.
	var err error
	// When this method returns we'll destroy the cluster we had at the time.
	// Note that, if debug was set, c has been set to nil.
	defer func() {
		wStatus.SetTest(nil /* test */, testToRunRes{noWork: true})
		wStatus.SetStatus("worker done")
		wStatus.SetCluster(nil)

		if c == nil {
			return
		}
		doDestroy := ctx.Err() == nil
		if doDestroy {
			l.PrintfCtx(ctx, "Worker exiting; destroying cluster.")
			c.Destroy(context.Background(), closeLogger, l)
		} else {
			l.PrintfCtx(ctx, "Worker exiting with canceled ctx. Not destroying cluster.")
		}
	}()

	// Loop until there's no more work in the pool, we get interrupted, or an
	// error occurs.
	for {
		select {
		case <-interrupt:
			l.ErrorfCtx(ctx, "worker detected interruption")
			return errors.Errorf("interrupted")
		default:
			if ctx.Err() != nil {
				// The context has been canceled. No need to continue.
				return errors.Wrap(ctx.Err(), "worker ctx done")
			}
		}

		if c != nil {
			if _, ok := c.spec.ReusePolicy.(reusePolicyNone); ok {
				wStatus.SetStatus("destroying cluster")
				// We use a context that can't be canceled for the Destroy().
				c.Destroy(ctx, closeLogger, l)
				c = nil
			}
		}
		var testToRun testToRunRes
		wStatus.SetTest(nil /* test */, testToRunRes{})
		wStatus.SetStatus("getting work")
		testToRun, c, err = r.getWork(
			ctx, work, qp, c, interrupt, l,
			getWorkCallbacks{
				createCluster: func(ctx context.Context, ttr testToRunRes) (*cluster, error) {
					wStatus.SetTest(nil /* test */, ttr)
					return allocateCluster(ctx, ttr.spec, ttr.alloc, artifactsRootDir, wStatus)
				},
				onDestroy: func() {
					wStatus.SetCluster(nil)
				},
			})
		if err != nil || testToRun.noWork {
			return err
		}
		c.status("running test")

		// Prepare the test's logger.
		logPath := ""
		var artifactsDir string
		if artifactsRootDir != "" {
			artifactsSuffix := "run_" + strconv.Itoa(testToRun.runNum)
			artifactsDir = filepath.Join(
				artifactsRootDir, teamCityNameEscape(testToRun.spec.Name), artifactsSuffix)
			logPath = filepath.Join(artifactsDir, "test.log")
		}
		testL, err := rootLogger(logPath, teeOpt)
		if err != nil {
			return err
		}
		t := &test{
			spec:         &testToRun.spec,
			buildVersion: r.buildVersion,
			artifactsDir: artifactsDir,
			l:            testL,
		}
		// Tell the cluster that, from now on, it will be run "on behalf of this
		// test".
		c.setTest(t)
		wStatus.SetCluster(c)
		wStatus.SetTest(t, testToRun)
		wStatus.SetStatus("running test")

		// Now run the test.
		l.PrintfCtx(ctx, "starting test: %s:%d", testToRun.spec.Name, testToRun.runNum)
		var success bool
		success, err = r.runTest(ctx, t, testToRun.runNum, c, testRunnerLogPath, stdout, testL)
		if err != nil {
			shout(ctx, l, stdout, "test returned error: %s: %s", t.Name(), err)
			// Mark the test as failed if it isn't already.
			if !t.Failed() {
				t.printAndFail(0 /* skip */, err)
			}
		} else {
			msg := "test passed"
			if !success {
				msg = fmt.Sprintf("test failed: %s (run %d)", t.Name(), testToRun.runNum)
			}
			l.PrintfCtx(ctx, msg)
		}
		testL.close()
		if err != nil || t.Failed() {
			failureMsg := fmt.Sprintf("%s (%d) - ", testToRun.spec.Name, testToRun.runNum)
			if err != nil {
				failureMsg += err.Error()
			} else {
				failureMsg += t.FailureMsg()
			}

			if debug {
				// Save the cluster for future debugging.
				c.Save(ctx, failureMsg, l)

				// Continue with a fresh cluster.
				c = nil
			} else {
				// On any test failure or error, we destroy the cluster. We could be
				// more selective, but this sounds safer.
				l.PrintfCtx(ctx, "destroying cluster %s because: %s", c, failureMsg)
				c.Destroy(ctx, closeLogger, l)
				c = nil
			}

			if err != nil {
				return err
			}
		} else {
			// Upon success fetch the perf artifacts from the remote hosts.
			getPerfArtifacts(ctx, l, c, t)
		}
	}
}

// getPerfArtifacts retrieves the perf artifacts for the test.
// If there's an error, oh well, don't do anything rash like fail a test
// which already passed.
func getPerfArtifacts(ctx context.Context, l *logger, c *cluster, t *test) {
	g := ctxgroup.WithContext(ctx)
	fetchNode := func(node int) func(context.Context) error {
		return func(ctx context.Context) error {
			// RunWithBuffer to toss the output.
			if _, err := c.RunWithBuffer(ctx, l, c.Node(node), "ls", perfArtifactsDir); err != nil {
				// perfArtifactsDir doesn't exist, nothing to fetch.
				return nil
			}
			dst := fmt.Sprintf("%s/%d.%s", t.artifactsDir, node, perfArtifactsDir)
			return c.Get(ctx, l, perfArtifactsDir, dst, c.Node(node))
		}
	}
	for _, i := range c.All() {
		g.GoCtx(fetchNode(i))
	}
	if err := g.Wait(); err != nil {
		l.PrintfCtx(ctx, "failed to get perf artifacts: %v", err)
	}
}

// An error is returned if the test is still running (on another goroutine) when
// this returns. This happens when the test doesn't respond to cancellation.
// Returns true if the test is considered to have passed, false otherwise.
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy
//    the cluster.
// testRunnerLogPath: The path to the test runner's log. It will be copied to
//  	the test's artifacts dir if the test fails and we're running under
//  	TeamCity.
func (r *testRunner) runTest(
	ctx context.Context,
	t *test,
	runNum int,
	c *cluster,
	testRunnerLogPath string,
	stdout io.Writer,
	l *logger,
) (bool, error) {
	if t.spec.Skip != "" {
		return false, fmt.Errorf("can't run skipped test: %s: %s", t.Name(), t.spec.Skip)
	}

	if teamCity {
		shout(ctx, l, stdout, "##teamcity[testStarted name='%s' flowId='%s']", t.Name(), t.Name())
	} else {
		shout(ctx, l, stdout, "=== RUN   %s", t.Name())
	}

	r.status.Lock()
	r.status.running[t] = struct{}{}
	r.status.Unlock()

	t.runner = callerName()
	t.runnerID = goid.Get()

	defer func() {
		t.end = timeutil.Now()

		if err := recover(); err != nil {
			t.mu.Lock()
			t.mu.failed = true
			t.mu.output = append(t.mu.output, t.decorate(0 /* skip */, fmt.Sprint(err))...)
			t.mu.Unlock()
		}

		t.mu.Lock()
		t.mu.done = true
		t.mu.Unlock()

		durationStr := fmt.Sprintf("%.2fs", t.duration().Seconds())
		if t.Failed() {
			t.mu.Lock()
			output := fmt.Sprintf("test artifacts and logs in: %s\n", t.ArtifactsDir()) + string(t.mu.output)
			failLoc := t.mu.failLoc
			t.mu.Unlock()

			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
					t.Name(), teamCityEscape(output), t.Name(),
				)

				// Copy a snapshot of the testrunner's log to the test's artifacts dir
				// so that we collect it below.
				cp := exec.Command("cp", testRunnerLogPath, t.artifactsDir)
				if err := cp.Run(); err != nil {
					l.ErrorfCtx(ctx, "failed to copy test runner's logs to test artifacts: %s", err)
				}
			}

			shout(ctx, l, stdout, "--- FAIL: %s %s\n%s", t.Name(), durationStr, output)
			// NB: check NodeCount > 0 to avoid posting issues from this pkg's unit tests.
			if issues.CanPost() && t.spec.Run != nil && t.spec.Cluster.NodeCount > 0 {
				authorEmail := getAuthorEmail(failLoc.file, failLoc.line)
				branch := "<unknown branch>"
				if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
					branch = b
				}
				msg := fmt.Sprintf("The test failed on branch=%s, cloud=%s:\n%s",
					branch, cloud, output)
				artifacts := fmt.Sprintf("/%s", t.Name())

				if err := issues.Post(
					context.Background(),
					fmt.Sprintf("roachtest: %s failed", t.Name()),
					"roachtest", t.Name(), msg, artifacts, authorEmail, []string{"O-roachtest"},
				); err != nil {
					shout(ctx, l, stdout, "failed to post issue: %s", err)
				}
			}
		} else {
			shout(ctx, l, stdout, "--- PASS: %s %s", t.Name(), durationStr)
			// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
			// TeamCity regards the test as successful.
		}

		if teamCity {
			shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s']", t.Name(), t.Name())

			artifactsGlobPath := filepath.Join(t.ArtifactsDir(), "**")
			escapedTestName := teamCityNameEscape(t.Name())
			artifactsSpec := fmt.Sprintf("%s => %s", artifactsGlobPath, escapedTestName)
			shout(ctx, l, stdout, "##teamcity[publishArtifacts '%s']", artifactsSpec)
		}

		r.recordTestFinish(completedTestInfo{
			test:    t.Name(),
			run:     runNum,
			start:   t.start,
			end:     t.end,
			pass:    !t.Failed(),
			failure: t.FailureMsg(),
		})
		r.status.Lock()
		delete(r.status.running, t)
		// Only include tests with a Run function in the summary output.
		if t.spec.Run != nil {
			if t.Failed() {
				r.status.fail[t] = struct{}{}
			} else if t.spec.Skip == "" {
				r.status.pass[t] = struct{}{}
			} else {
				r.status.skip[t] = struct{}{}
			}
		}
		r.status.Unlock()
	}()

	t.start = timeutil.Now()

	timeout := 10 * time.Hour
	if t.spec.Timeout != 0 {
		timeout = t.spec.Timeout
	}
	// Make sure the cluster has enough life left for the test plus enough headroom
	// after the test finishes so that the next test can be selected. If it
	// doesn't, extend it.
	minExp := timeutil.Now().Add(timeout + time.Hour)
	if c.expiration.Before(minExp) {
		extend := minExp.Sub(c.expiration)
		l.PrintfCtx(ctx, "cluster needs to survive until %s, but has expiration: %s. Extending.",
			minExp, c.expiration)
		if err := c.Extend(ctx, extend, l); err != nil {
			t.printfAndFail(0 /* skip */, "failed to extend cluster: %s", err)
			return false, nil
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.mu.Lock()
	// t.Fatal() will cancel this context.
	t.mu.cancel = cancel
	t.mu.Unlock()

	// We run the actual test in a different goroutine because it might call
	// t.Fatal() which kills the goroutine, and also because we want to enforce a
	// timeout.
	success := false
	done := make(chan struct{})
	go func() {
		defer close(done) // closed only after we've grabbed the debug info below

		// This is the call to actually run the test.
		t.spec.Run(runCtx, t, c)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		// We hit a timeout. We're going to mark the test as failed (which will also
		// cancel its context). Then we'll wait up to 5 minutes in the hope
		// that the test reacts either to the ctx cancelation or to the fact that it
		// was marked as failed. If that happens, great - we return normally and so
		// the cluster can be reused. It the test does not react to anything, then
		// we return an error, which will cause the caller to stop everything and
		// destroy this cluster (as well as all the others). The cluster
		// cannot be reused since we have a runaway test goroutine that's presumably
		// going to continue using the cluster.
		t.printfAndFail(0 /* skip */, "test timed out (%s)", timeout)
		select {
		case <-done:
			if success {
				panic("expected success=false after a timeout")
			}
		case <-time.After(5 * time.Minute):
			msg := "test timed out and afterwards failed to respond to cancelation"
			t.l.PrintfCtx(ctx, msg)
			r.collectClusterLogs(ctx, c, t.l)
			// We return an error here because the test goroutine is still running, so
			// we want to alert the caller of this unusual situation.
			return false, fmt.Errorf(msg)
		}
	}

	// Detect replica divergence (i.e. ranges in which replicas have arrived
	// at the same log position with different states).
	c.FailOnReplicaDivergence(ctx, t)
	// Detect dead nodes in an inner defer. Note that this will call
	// t.printfAndFail() when appropriate, which will cause the code below to
	// enter the t.Failed() branch.
	c.FailOnDeadNodes(ctx, t)

	if t.Failed() {
		r.collectClusterLogs(ctx, c, t.l)
		return false, nil
	}
	return true, nil
}

func (r *testRunner) collectClusterLogs(ctx context.Context, c *cluster, l *logger) {
	// NB: fetch the logs even when we have a debug zip because
	// debug zip can't ever get the logs for down nodes.
	// We only save artifacts for failed tests in CI, so this
	// duplication is acceptable.
	// NB: fetch the logs *first* in case one of the other steps
	// below has problems. For example, `debug zip` is known to
	// hang sometimes at the time of writing, see:
	// https://github.com/cockroachdb/cockroach/issues/39620
	if err := c.FetchLogs(ctx); err != nil {
		l.Printf("failed to download logs: %s", err)
	}
	l.PrintfCtx(ctx, "collecting cluster logs")
	if err := c.FetchDebugZip(ctx); err != nil {
		l.Printf("failed to download logs: %s", err)
	}
	if err := c.FetchDmesg(ctx); err != nil {
		l.Printf("failed to fetch dmesg: %s", err)
	}
	if err := c.FetchJournalctl(ctx); err != nil {
		l.Printf("failed to fetch journalctl: %s", err)
	}
	if err := c.FetchCores(ctx); err != nil {
		l.Printf("failed to fetch cores: %s", err)
	}
	if err := c.CopyRoachprodState(ctx); err != nil {
		l.Printf("failed to copy roachprod state: %s", err)
	}
}

func callerName() string {
	// Make room for the skip PC.
	var pc [2]uintptr
	n := runtime.Callers(2, pc[:]) // runtime.Callers + callerName
	if n == 0 {
		panic("zero callers found")
	}
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	return frame.Function
}

// generateReport produces the final pass/fail line and produces a slack report
// if configured.
func (r *testRunner) generateReport() string {
	r.status.Lock()
	defer r.status.Unlock()
	postSlackReport(r.status.pass, r.status.fail, r.status.skip)

	fails := len(r.status.fail)
	var msg string
	if fails > 0 {
		msg = fmt.Sprintf("FAIL (%d fails)\n", fails)
	} else {
		msg = "PASS"
	}
	return msg
}

type getWorkCallbacks struct {
	createCluster func(context.Context, testToRunRes) (*cluster, error)
	onDestroy     func()
}

// getWork selects the next test to run and creates a suitable cluster for it if
// need be. If a new cluster needs to be created, the method blocks until there
// are enough resources available to run it.
// getWork takes in a cluster; if not nil, tests that can reuse it are
// preferred. If a test that can reuse it is not found (or if there's no more
// work), the cluster is destroyed (and so its resources are released).
//
// If the cluster is to be reused, getWork() wipes it.
func (r *testRunner) getWork(
	ctx context.Context,
	work *workPool,
	qp *quotapool.IntPool,
	c *cluster,
	interrupt <-chan struct{},
	l *logger,
	callbacks getWorkCallbacks,
) (testToRunRes, *cluster, error) {

	select {
	case <-interrupt:
		return testToRunRes{}, nil, fmt.Errorf("interrupted")
	default:
	}

	testToRun, err := work.getTestToRun(ctx, c, qp, r.cr, callbacks.onDestroy, l)
	if err != nil {
		return testToRunRes{}, nil, err
	}
	if !testToRun.noWork {
		l.PrintfCtx(ctx, "Selected test: %s run: %d.", testToRun.spec.Name, testToRun.runNum)
	}
	// Are we done?
	if testToRun.noWork {
		return testToRun, nil, nil
	}

	// Create a cluster, if we no longer have one.
	if testToRun.canReuseCluster {
		l.PrintfCtx(ctx, "Using existing cluster: %s. Wiping", c.name)
		if err := c.WipeE(ctx, l); err != nil {
			return testToRunRes{}, nil, err
		}
		if err := c.RunL(ctx, l, c.All(), "rm -rf "+perfArtifactsDir); err != nil {
			return testToRunRes{}, nil, errors.Wrapf(err, "failed to remove perf artifacts dir")
		}
		// Overwrite the spec of the cluster with the one coming from the test. In
		// particular, this overwrites the reuse policy to reflect what the test
		// intends to do with it.
		c.spec = testToRun.spec.Cluster
	} else {
		var err error
		c, err = callbacks.createCluster(ctx, testToRun)
		if err != nil {
			return testToRunRes{}, nil, err
		}
	}
	return testToRun, c, nil
}

// addWorker updates the bookkeeping for one more worker.
func (r *testRunner) addWorker(ctx context.Context, name string) *workerStatus {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()
	w := &workerStatus{name: name}
	if _, ok := r.workersMu.workers[name]; ok {
		log.Fatalf(ctx, "worker %q already exists", name)
	}
	r.workersMu.workers[name] = w
	return w
}

// removeWorker deletes the bookkepping for a worker that has finished running.
func (r *testRunner) removeWorker(ctx context.Context, name string) {
	r.workersMu.Lock()
	delete(r.workersMu.workers, name)
	r.workersMu.Unlock()
}

// runHTTPServer starts a server running in the background.
//
// httpPort: The port on which to serve the web interface. Pass 0 for allocating
// 	 a port automatically (which will be printed to stdout).
func (r *testRunner) runHTTPServer(httpPort int, stdout io.Writer) error {
	http.HandleFunc("/", r.serveHTTP)
	// Run an http server in the background.
	// We handle the case where httpPort is 0, which means we automatically
	// allocate a port.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", httpPort))
	if err != nil {
		return err
	}
	httpPort = listener.Addr().(*net.TCPAddr).Port
	go func() {
		if err := http.Serve(listener, nil /* handler */); err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(stdout, "HTTP server listening on all network interfaces, port %d.\n", httpPort)
	return nil
}

// serveHTTP is the handler for the test runner's web server.
func (r *testRunner) serveHTTP(wr http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(wr, "<html><body>")
	fmt.Fprintf(wr, "<a href='debug/pprof'>pprof</a>")
	fmt.Fprintf(wr, "<p>")
	// Print the workers report.
	fmt.Fprintf(wr, "<h2>Workers:</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Worker</th>
	<th>Worker Status</th>
	<th>Test</th>
	<th>Cluster</th>
	<th>Cluster reused</th>
	<th>Test Status</th>
	</tr>`)
	r.workersMu.Lock()
	workers := make([]*workerStatus, len(r.workersMu.workers))
	i := 0
	for _, w := range r.workersMu.workers {
		workers[i] = w
		i++
	}
	r.workersMu.Unlock()
	sort.Slice(workers, func(i int, j int) bool {
		l := workers[i]
		r := workers[j]
		return strings.Compare(l.name, r.name) < 0
	})
	for _, w := range workers {
		var testName string
		ttr := w.TestToRun()
		clusterReused := ""
		if ttr.noWork {
			testName = "done"
		} else if ttr.spec.Name == "" {
			testName = "N/A"
		} else {
			testName = fmt.Sprintf("%s (run %d)", ttr.spec.Name, ttr.runNum)
			if ttr.canReuseCluster {
				clusterReused = "yes"
			} else {
				clusterReused = "no"
			}
		}
		var clusterName string
		if w.Cluster() != nil {
			clusterName = w.Cluster().name
		}
		t := w.Test()
		testStatus := "N/A"
		if t != nil {
			testStatus = t.GetStatus()
		}
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
			w.name, w.Status(), testName, clusterName, clusterReused, testStatus)
	}
	fmt.Fprintf(wr, "</table>")

	// Print the finished tests report.
	fmt.Fprintf(wr, "<p>")
	fmt.Fprintf(wr, "<h2>Finished tests:</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Test</th>
	<th>Status</th>
	<th>Duration</th>
	</tr>`)
	for _, t := range r.getCompletedTests() {
		name := fmt.Sprintf("%s (run %d)", t.test, t.run)
		status := "PASS"
		if !t.pass {
			status = "FAIL " + t.failure
		}
		duration := fmt.Sprintf("%s (%s - %s)", t.end.Sub(t.start), t.start, t.end)
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><tr/>", name, status, duration)
	}
	fmt.Fprintf(wr, "</table>")

	// Print the saved clusters report.
	fmt.Fprintf(wr, "<p>")
	fmt.Fprintf(wr, "<h2>Clusters left alive for further debugging "+
		"(if --debug was specified):</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Cluster</th>
	<th>Test</th>
	</tr>`)
	for _, c := range r.cr.savedClusters() {
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><tr/>", c.name, c.savedMsg)
	}
	fmt.Fprintf(wr, "</table>")

	fmt.Fprintf(wr, "<p>")
	fmt.Fprintf(wr, "<h2>Tests left:</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Test</th>
	<th>Runs</th>
	</tr>`)
	for _, t := range r.work.workRemaining() {
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%d</td><tr/>", t.spec.Name, t.count)
	}

	fmt.Fprintf(wr, "</body></html>")
}

// recordTestFinish updated bookkeeping when a test finishes.
func (r *testRunner) recordTestFinish(info completedTestInfo) {
	r.completedTestsMu.Lock()
	r.completedTestsMu.completed = append(r.completedTestsMu.completed, info)
	r.completedTestsMu.Unlock()
}

// getCompletedTests returns info on all tests that finished running.
func (r *testRunner) getCompletedTests() []completedTestInfo {
	r.completedTestsMu.Lock()
	defer r.completedTestsMu.Unlock()
	res := make([]completedTestInfo, len(r.completedTestsMu.completed))
	copy(res, r.completedTestsMu.completed)
	return res
}

// completedTestInfo represents information on a completed test run.
type completedTestInfo struct {
	test    string
	run     int
	start   time.Time
	end     time.Time
	pass    bool
	failure string
}

// PredecessorVersion returns a recent predecessor of the build version (i.e.
// the build tag of the main binary). For example, if the running binary is from
// the master branch prior to releasing 19.2.0, this will return a recent
// (ideally though not necessarily the latest) 19.1 patch release.
func PredecessorVersion(buildVersion version.Version) (string, error) {
	if buildVersion == (version.Version{}) {
		return "", errors.Errorf("buildVersion not set")
	}

	buildVersionMajorMinor := fmt.Sprintf("%d.%d", buildVersion.Major(), buildVersion.Minor())

	verMap := map[string]string{
		"19.2": "19.1.5",
		"19.1": "2.1.8",
		"2.2":  "2.1.8",
		"2.1":  "2.0.7",
	}
	v, ok := verMap[buildVersionMajorMinor]
	if !ok {
		return "", errors.Errorf("prev version not set for version: %s", buildVersionMajorMinor)
	}
	return v, nil
}

type workerErrors struct {
	mu struct {
		syncutil.Mutex
		errs []error
	}
}

func (we *workerErrors) AddErr(err error) {
	we.mu.Lock()
	defer we.mu.Unlock()
	we.mu.errs = append(we.mu.errs, err)
}

func (we *workerErrors) Err() error {
	we.mu.Lock()
	defer we.mu.Unlock()
	if len(we.mu.errs) == 0 {
		return nil
	}
	// TODO(andrei): Maybe we should do something other than return the first
	// error...
	return we.mu.errs[0]
}
