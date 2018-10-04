// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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

	completedTestsMu struct {
		syncutil.Mutex
		// completed maintains information on all completed test runs.
		completed []completedTestInfo
	}
}

// newTestRunner constructs a testRunner.
//
// buildVersion: The version of the Cockroach binary against which tests will run.
// cr: The cluster registry with which all clusters will be registered. The
//   caller provides this as the caller needs to be able to shut clusters down
//   on Ctrl+C.
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
		// No other field can be set.
		if c != (clustersOpt{typ: localCluster}) {
			return fmt.Errorf("local cluster incompatible with spec: %+v", c)
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
// artifactsDir: A directory where all the test artifacts (in particular, log
//   files) will be placed.
// user: The user running these tests. If clusters are created, the user will be
//   a prefix to their names.
// lopt: Options for logging.
func (r *testRunner) Run(
	ctx context.Context,
	tests []testSpec,
	count int,
	parallelism int,
	clustersOpt clustersOpt,
	artifactsDir string,
	user string,
	lopt loggingOpt,
) error {
	// Validate options.
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
		numConcurrentClusterCreations, user, clustersOpt.clusterID, artifactsDir, r.cr)

	// allocateCluster will be used by workers to create new clusters (or to attach
	// to an existing one).
	allocateCluster := func(
		ctx context.Context,
		t testSpec,
		alloc quotaAlloc,
		artifactsDir string,
		wStatus *workerStatus,
	) (*cluster, error) {
		wStatus.SetStatus("creating cluster")
		defer wStatus.SetStatus("")

		lopt.l.PrintfCtx(ctx, "Creating new cluster for test: %s", t.Name)

		existingClusterName := clustersOpt.clusterName
		if existingClusterName != "" {
			// Logs for attaching to a cluster go to a dedicated log file.
			logPath := filepath.Join(artifactsDir, "cluster-create", existingClusterName+".log")
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
	if n == 0 {
		return fmt.Errorf("no test matched filters")
	}
	if n*count < parallelism {
		// Don't spin up more workers than necessary.
		parallelism = n * count
	}

	r.status.running = make(map[*test]struct{})
	r.status.pass = make(map[*test]struct{})
	r.status.fail = make(map[*test]struct{})
	r.status.skip = make(map[*test]struct{})

	work := newWorkPool(tests, count)
	stopper := stop.NewStopper()
	errs := &workerErrors{}

	qp := quotapool.NewIntPool("cloud cpu", int64(clustersOpt.cpuQuota))
	l := lopt.l

	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		stopper.RunWorker(ctx, func(ctx context.Context) {
			defer wg.Done()

			if err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, work, qp,
				stopper.ShouldQuiesce(),
				clustersOpt.keepClustersOnTestFailure,
				lopt.artifactsDir, user, lopt.tee, lopt.stdout,
				allocateCluster,
				l,
			); err != nil {
				// A worker returned an error. Let's shut down.
				shout(ctx, l, lopt.stdout, "Worker %d returned with error. Quiescing. Error: %s", i, err)
				errs.AddErr(err)
				// Quiesce the stopper. This will cause all workers to not pick up more
				// tests after finishing the currently running one.
				stopper.Quiesce(ctx)
				// Interrupt everybody waiting for resources.
				if qp != nil {
					qp.Close()
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
	// !!! this can produce an empty line; figure out how
	report, success, err := r.generateReport()
	if err != nil {
		log.Fatal(ctx, err)
	}
	shout(ctx, l, lopt.stdout, report)
	if success {
		return nil
	}
	return fmt.Errorf("some tests failed")
}

type clusterAllocatorFn func(
	ctx context.Context,
	t testSpec,
	alloc quotaAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*cluster, error)

// runWorker runs tests in a loop until work is exhausted.
//
// Errors are returned in exceptional circumstances, generally when a cluster
// failed to be created. Upon return, an attempt is performed to destroy the cluster used by
// this worker.
//
// Args:
// name: The worker's name, to be used as a prefix for log messages.
// artifactsDir: The artifacts dir. Each test's logs are going to be under a
//   run_<n> dir. If empty, test log files will not be created.
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
	artifactsDir string,
	user string,
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

	var c *cluster     // The cluster currently being used.
	var cs clusterSpec // The spec of the current cluster.
	var tag string     // The tag of the current cluster.
	// Set if the current cluster needs to be destroyed after the current test.
	var needDestroy bool

	// When this method returns we'll destroy the cluster we had at the time.
	// Note that, if debug was set, c has been set to nil.
	defer func() {
		if c == nil {
			return
		}
		l.PrintfCtx(ctx, "Worker exiting; destroying cluster.")
		// We use a context that can't be canceled for the Destroy().
		c.Destroy(context.Background(), closeLogger, l)
	}()

	// Loop until there's no more work in the pool, we get interrupted, or an
	// error occurs.
	for {
		select {
		case <-interrupt:
			return errors.Errorf("interrupted")
		default:
		}

		if needDestroy && c != nil {
			wStatus.SetStatus("destroying cluster")
			// We use a context that can't be canceled for the Destroy().
			c.Destroy(context.Background(), closeLogger, l)
			c = nil
			cs = clusterSpec{}
			tag = ""
			needDestroy = false
		} else if c != nil {
			wStatus.SetStatus("wiping cluster")
			// We wipe clusters before reusing.
			if err := c.WipeE(ctx, l); err != nil {
				return errors.Wrapf(err, "failed to wipe cluster for reuse")
			}
		}

		oldCluster := c
		var testToRun testToRunRes
		var err error
		wStatus.SetStatus("getting work")
		// !!! make sure the case when there's not enough resources available to run
		// some tests (in particular because we've saved a bunch of clusters) is
		// handled fine.
		testToRun, c, cs, err = r.getWork(
			ctx, work, qp, c, cs, tag, interrupt, l,
			func(ctx context.Context, t testSpec, alloc quotaAlloc) (*cluster, error) {
				return allocateCluster(ctx, t, alloc, artifactsDir, wStatus)
			})
		if err != nil || testToRun.noWork {
			return err
		}
		wStatus.SetCluster(c)
		wStatus.SetTest(testToRun)
		wStatus.SetStatus("running test")
		c.status("running test")

		// If the cluster changed, reset some state.
		if oldCluster != nil && oldCluster != c {
			needDestroy = false
			tag = ""
		}
		if _, ok := testToRun.spec.Cluster.ReusePolicy.(reusePolicyNone); ok {
			needDestroy = true
		} else if p, ok := testToRun.spec.Cluster.ReusePolicy.(reusePolicyTagged); ok {
			// Our cluster is now tagged.
			tag = p.tag
		}

		// Prepare the test's logger.
		logPath := ""
		if artifactsDir != "" {
			artifactsSuffix := "run_" + strconv.Itoa(testToRun.runNum)
			artifactsDir = filepath.Join(
				artifactsDir, teamCityNameEscape(testToRun.spec.Name), artifactsSuffix)
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

		// Now run the test.
		l.PrintfCtx(ctx, "starting test: %s:%d", testToRun.spec.Name, testToRun.runNum)
		var success bool
		success, err = r.runTest(ctx, t, testToRun.runNum, c, artifactsDir, stdout, testL)
		testL.close()
		if err != nil {
			shout(ctx, l, stdout, "test returned error: %s: %s", t.Name(), err)
			// Mark the test as failed if it isn't already.
			if !t.Failed() {
				t.printAndFail(0 /* skip */, err)
			}
		} else {
			msg := "test passed"
			if !success {
				msg = fmt.Sprintf("test failed: %s:%d", t.Name(), testToRun.runNum)
			}
			l.PrintfCtx(ctx, msg)
		}
		// If a test failed and debug was set, we bail.
		if (err != nil || t.Failed()) && debug {
			failureMsg := fmt.Sprintf("%s (%d) - ", testToRun.spec.Name, testToRun.runNum)
			if err != nil {
				failureMsg += err.Error()
			} else {
				failureMsg += t.FailureMsg()
			}
			// Save the cluster for future debugging.
			c.Save(failureMsg)
			// Continue with a fresh cluster.
			c = nil
			if err != nil {
				return err
			}
		}
	}
}

// An error is returned in exceptional situations. The cluster cannot be reused
// if an error is returned.
// Returns true if the test is considered to have passed, false otherwise.
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy
//    the cluster.
func (r *testRunner) runTest(
	ctx context.Context,
	t *test,
	runNum int,
	c *cluster,
	artifactsDir string,
	stdout io.Writer,
	l *logger,
) (bool, error) {
	if t.spec.Skip != "" {
		return false, fmt.Errorf("Can't run skipped test: %s: %s", t.Name(), t.spec.Skip)
	}

	if teamCity {
		shout(ctx, l, stdout, "##teamcity[testStarted name='%s' flowId='%s']", t.Name(), t.Name())
	} else {
		shout(ctx, l, stdout, "=== RUN   %s", t.Name())
	}

	r.status.Lock()
	r.status.running[t] = struct{}{}
	r.status.Unlock()

	callerName := func() string {
		// Make room for the skip PC.
		var pc [2]uintptr
		n := runtime.Callers(2, pc[:]) // skip + runtime.Callers + callerName
		if n == 0 {
			panic("zero callers found")
		}
		frames := runtime.CallersFrames(pc[:n])
		frame, _ := frames.Next()
		return frame.Function
	}

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

		dstr := fmt.Sprintf("%.2fs", t.duration().Seconds())

		if t.Failed() {
			t.mu.Lock()
			output := t.mu.output
			failLoc := t.mu.failLoc
			t.mu.Unlock()

			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
					t.Name(), teamCityEscape(string(output)), t.Name(),
				)
			}

			shout(ctx, l, stdout, "--- FAIL: %s %s\n%s", t.Name(), dstr, output)
			if issues.CanPost() && t.spec.Run != nil {
				authorEmail := getAuthorEmail(failLoc.file, failLoc.line)
				branch := "<unknown branch>"
				if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
					branch = b
				}
				msg := fmt.Sprintf("The test failed on branch=%s, cloud=%s:\n%s",
					branch, cloud, output)

				if err := issues.Post(
					context.Background(),
					fmt.Sprintf("roachtest: %s failed", t.Name()),
					"roachtest", t.Name(), msg, authorEmail, []string{"O-roachtest"},
				); err != nil {
					shout(ctx, l, stdout, "failed to post issue: %s", err)
				}
			}
		} else {
			shout(ctx, l, stdout, "--- PASS: %s %s", t.Name(), dstr)
			// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
			// TeamCity regards the test as successful.
		}

		if teamCity {
			shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s']", t.Name(), t.Name())

			escapedTestName := teamCityNameEscape(t.Name())
			artifactsGlobPath := filepath.Join(artifactsDir, escapedTestName, "**")
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

	defer func() {
		if err := c.FetchLogs(ctx); err != nil {
			c.l.PrintfCtx(ctx, "failed to download logs: %s\n", err)
		}
	}()

	timeout := 12 * time.Hour
	if t.spec.Timeout != 0 {
		timeout = t.spec.Timeout
	}
	// Make sure the cluster has enough life left for the test plus enough headroom
	// after the test finishes so that the next test can be selected. If it
	// doesn't, extend it.
	// !!! there was a report that when cluster expiration is reached, the test
	// seems to fail in a messy way (in `registry.runTest`). it prints "test timed
	// out" then "test unresponsive after time out".
	minExp := timeutil.Now().Add(timeout + time.Hour)
	if c.expiration.Before(minExp) {
		if err := c.Extend(ctx, timeout+time.Hour, l); err != nil {
			t.printfAndFail(0 /* skip */, "failed to extend cluster")
			return false, err
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

		defer func() {
			// Detect replica divergence (i.e. ranges in which replicas have arrived
			// at the same log position with different states).
			c.FailOnReplicaDivergence(ctx, t)
			// Detect dead nodes in an inner defer. Note that this will call t.Fatal
			// when appropriate, which will cause the closure above to enter the
			// t.Failed() branch.
			c.FailOnDeadNodes(ctx, t)

			if t.Failed() {
				if err := c.FetchDebugZip(ctx); err != nil {
					t.l.Printf("failed to download logs: %s", err)
				}
				if err := c.FetchDmesg(ctx); err != nil {
					c.l.Printf("failed to fetch dmesg: %s", err)
				}
				if err := c.FetchJournalctl(ctx); err != nil {
					c.l.Printf("failed to fetch journalctl: %s", err)
				}
				if err := c.FetchCores(ctx); err != nil {
					c.l.Printf("failed to fetch cores: %s", err)
				}
				if err := c.CopyRoachprodState(ctx); err != nil {
					c.l.Printf("failed to copy roachprod state: %s", err)
				}
			} else {
				success = true
			}
		}()

		// This is the call to actually run the test.
		t.spec.Run(runCtx, t, c)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		// We hit a timeout. We're going to mark the test as failed (which will also
		// cancel its context).  Otherwise, we'll wait another 5 minutes in the hope
		// that the test reacts either to the ctx cancelation or to the fact that it
		// was marked as failed. If that happens, great - we return normally and so
		// the cluster can be reused. It the test does not react to anything, then
		// we return an error, which will cause the caller to stop everything and
		// destroy this cluster (as well as all the others). The cluster
		// cannot be reused since we have a runaway test goroutine that's presumably
		// going to continue using the cluster case.

		msg := fmt.Sprintf("test timed out (%s)", timeout)
		t.printfAndFail(0 /* skip */, "%s\n", msg)
	}
	return success, nil
}

// generateReport writes the final pass/fail line, produces a slack report if
// configured, and return true if the run is to considered successful or false
// otherwise.
func (r *testRunner) generateReport() (string, bool, error) {
	r.status.Lock()
	defer r.status.Unlock()
	postSlackReport(r.status.pass, r.status.fail, r.status.skip)

	fails := len(r.status.fail)
	var b strings.Builder
	var msg string
	if fails > 0 {
		msg = fmt.Sprintf("FAIL (%d fails)\n", fails)
	}
	if _, err := b.WriteString(msg); err != nil {
		return "", false, err
	}
	return b.String(), fails == 0, nil
}

// getWork selects the next test to run and creates a suitable cluster for it if
// need be. If a new cluster needs to be created, the method blocks until there
// are enough resources available to run it.
// getWork takes in a cluster; if not nil, tests that can reuse it are
// preferred. If a test that can reuse it is not found (or if there's no more
// work), the cluster is destroyed (and so its resources are released).
//
// Args:
// createCluster: a cluster factory.
func (r *testRunner) getWork(
	ctx context.Context,
	work *workPool,
	qp *quotapool.IntPool,
	c *cluster,
	cs clusterSpec,
	tag string,
	interrupt <-chan struct{},
	l *logger,
	createCluster func(context.Context, testSpec, quotaAlloc) (*cluster, error),
) (testToRunRes, *cluster, clusterSpec, error) {

	select {
	case <-interrupt:
		return testToRunRes{}, nil, clusterSpec{}, fmt.Errorf("interrupted")
	default:
	}

	testToRun, err := work.getTestToRun(ctx, c, cs, qp, r.cr, l)
	if err != nil {
		return testToRunRes{}, nil, clusterSpec{}, err
	}
	if !testToRun.noWork {
		l.PrintfCtx(ctx, "Selected test: %s run: %d\n", testToRun.spec.Name, testToRun.runNum)
	}
	// Are we done?
	if testToRun.noWork {
		return testToRun, nil, clusterSpec{}, nil
	}

	// Create a cluster, if we no longer have one.
	if testToRun.canReuseCluster {
		l.PrintfCtx(ctx, "Using existing cluster: %s\n", c.name)
	} else {
		var err error
		c, err = createCluster(ctx, testToRun.spec, testToRun.alloc)
		if err != nil {
			return testToRunRes{}, nil, clusterSpec{}, err
		}
	}
	return testToRun, c, testToRun.spec.Cluster, nil
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
	<th>Status</th>
	<th>Test</th>
	<th>Cluster</th>
	<th>Cluster reused</th>
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
		t := w.Test()
		if t.noWork {
			testName = "no work"
		} else {
			testName = fmt.Sprintf("%s (run %d)", t.spec.Name, t.runNum)
		}
		var clusterName string
		if w.Cluster() != nil {
			clusterName = w.Cluster().name
		}
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%t</td></tr>\n",
			w.name, w.Status(), testName, clusterName, t.canReuseCluster)
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
	for c, msg := range r.cr.savedClusters() {
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><tr/>", c.name, msg)
	}
	fmt.Fprintf(wr, "</table>")

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
		"19.2": "19.1.0-rc.4",
		"19.1": "2.1.6",
		"2.2":  "2.1.6",
		"2.1":  "2.0.7",
	}
	v, ok := verMap[buildVersionMajorMinor]
	if !ok {
		return "", errors.Errorf("prev version not set for version: %s", buildVersionMajorMinor)
	}
	return v, nil
}
