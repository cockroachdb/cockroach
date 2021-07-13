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
	"archive/zip"
	"context"
	"fmt"
	"html"
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/petermattis/goid"
)

var errTestsFailed = fmt.Errorf("some tests failed")

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
		running map[*testImpl]struct{}
		pass    map[*testImpl]struct{}
		fail    map[*testImpl]struct{}
		skip    map[*testImpl]struct{}
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

type testOpts struct {
	versionsBinaryOverride map[string]string
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
	tests []registry.TestSpec,
	count int,
	parallelism int,
	clustersOpt clustersOpt,
	topt testOpts,
	lopt loggingOpt,
) error {
	// Validate options.
	if len(tests) == 0 {
		return fmt.Errorf("no test matched filters")
	}

	hasDevLicense := envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "") != ""
	for _, t := range tests {
		if t.RequiresLicense && !hasDevLicense {
			return fmt.Errorf("test %q requires an enterprise license, set COCKROACH_DEV_LICENSE", t.Name)
		}
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
		t registry.TestSpec,
		alloc *quotapool.IntAlloc,
		artifactsDir string,
		wStatus *workerStatus,
	) (*clusterImpl, error) {
		wStatus.SetStatus("creating cluster")
		defer wStatus.SetStatus("")

		lopt.l.PrintfCtx(ctx, "Creating new cluster for test %s: %s", t.Name, t.Cluster)

		existingClusterName := clustersOpt.clusterName
		if existingClusterName != "" {
			// Logs for attaching to a cluster go to a dedicated log file.
			logPath := filepath.Join(artifactsDir, runnerLogsDir, "cluster-create", existingClusterName+".log")
			clusterL, err := logger.RootLogger(logPath, lopt.tee)
			if err != nil {
				return nil, err
			}
			defer clusterL.Close()
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

	r.status.running = make(map[*testImpl]struct{})
	r.status.pass = make(map[*testImpl]struct{})
	r.status.fail = make(map[*testImpl]struct{})
	r.status.skip = make(map[*testImpl]struct{})

	r.work = newWorkPool(tests, count)
	stopper := stop.NewStopper()
	errs := &workerErrors{}

	qp := quotapool.NewIntPool("cloud cpu", uint64(clustersOpt.cpuQuota))
	l := lopt.l

	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		if err := stopper.RunAsyncTask(ctx, "worker", func(ctx context.Context) {
			defer wg.Done()

			if err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, r.work, qp,
				stopper.ShouldQuiesce(),
				clustersOpt.keepClustersOnTestFailure,
				lopt.artifactsDir, lopt.runnerLogPath, lopt.tee, lopt.stdout,
				allocateCluster,
				topt,
				l,
			); err != nil {
				// A worker returned an error. Let's shut down.
				msg := fmt.Sprintf("Worker %d returned with error. Quiescing. Error: %v", i, err)
				shout(ctx, l, lopt.stdout, msg)
				errs.AddErr(err)
				// Stop the stopper. This will cause all workers to not pick up more
				// tests after finishing the currently running one. We add one to the
				// WaitGroup so that wg.Wait() will also wait for the stopper.
				wg.Add(1)
				go func() {
					defer wg.Done()
					stopper.Stop(ctx)
				}()
				// Interrupt everybody waiting for resources.
				if qp != nil {
					qp.Close(msg)
				}
			}
		}); err != nil {
			wg.Done()
		}
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
		return errTestsFailed
	}
	return nil
}

type clusterAllocatorFn func(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, error)

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
	teeOpt logger.TeeOptType,
	stdout io.Writer,
	allocateCluster clusterAllocatorFn,
	topt testOpts,
	l *logger.Logger,
) error {
	ctx = logtags.AddTag(ctx, name, nil /* value */)
	wStatus := r.addWorker(ctx, name)
	defer func() {
		r.removeWorker(ctx, name)
	}()

	var c *clusterImpl // The cluster currently being used.
	var err error
	// When this method returns we'll destroy the cluster we had at the time.
	// Note that, if debug was set, c has been set to nil.
	defer func() {
		wStatus.SetTest(nil /* test */, testToRunRes{noWork: true})
		wStatus.SetStatus("worker done")
		wStatus.SetCluster(nil)

		if c == nil {
			l.PrintfCtx(ctx, "Worker exiting; no cluster to destroy.")
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
			if _, ok := c.spec.ReusePolicy.(spec.ReusePolicyNone); ok {
				wStatus.SetStatus("destroying cluster")
				// We use a context that can't be canceled for the Destroy().
				c.Destroy(context.Background(), closeLogger, l)
				c = nil
			}
		}
		var testToRun testToRunRes
		wStatus.SetTest(nil /* test */, testToRunRes{})
		wStatus.SetStatus("getting work")
		testToRun, c, err = r.getWork(
			ctx, work, qp, c, interrupt, l,
			getWorkCallbacks{
				createCluster: func(ctx context.Context, ttr testToRunRes) (*clusterImpl, error) {
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
		var artifactsSpec string
		if artifactsRootDir != "" {
			escapedTestName := teamCityNameEscape(testToRun.spec.Name)
			runSuffix := "run_" + strconv.Itoa(testToRun.runNum)

			base := filepath.Join(artifactsRootDir, escapedTestName)

			artifactsDir = filepath.Join(base, runSuffix)
			logPath = filepath.Join(artifactsDir, "test.log")

			// Map artifacts/TestFoo/** => TestFoo/**, i.e. collect the artifacts
			// for this test exactly as they are laid out on disk (when the time
			// comes).
			artifactsSpec = fmt.Sprintf("%s/** => %s", base, escapedTestName)
		}
		testL, err := logger.RootLogger(logPath, teeOpt)
		if err != nil {
			return err
		}
		t := &testImpl{
			spec:                   &testToRun.spec,
			cockroach:              cockroach,
			deprecatedWorkload:     workload,
			buildVersion:           r.buildVersion,
			artifactsDir:           artifactsDir,
			artifactsSpec:          artifactsSpec,
			l:                      testL,
			versionsBinaryOverride: topt.versionsBinaryOverride,
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
		testL.Close()
		if err != nil || t.Failed() {
			failureMsg := fmt.Sprintf("%s (%d) - ", testToRun.spec.Name, testToRun.runNum)
			if err != nil {
				failureMsg += fmt.Sprintf("%+v", err)
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
				c.Destroy(context.Background(), closeLogger, l)
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
func getPerfArtifacts(ctx context.Context, l *logger.Logger, c *clusterImpl, t test.Test) {
	g := ctxgroup.WithContext(ctx)
	fetchNode := func(node int) func(context.Context) error {
		return func(ctx context.Context) error {
			testCmd := `'PERF_ARTIFACTS="` + perfArtifactsDir + `"
if [[ -d "${PERF_ARTIFACTS}" ]]; then
    echo true
elif [[ -e "${PERF_ARTIFACTS}" ]]; then
    ls -la "${PERF_ARTIFACTS}"
    exit 1
else
    echo false
fi'`
			out, err := c.RunWithBuffer(ctx, l, c.Node(node), "bash", "-c", testCmd)
			if err != nil {
				return errors.Wrapf(err, "failed to check for perf artifacts: %v", string(out))
			}
			switch out := strings.TrimSpace(string(out)); out {
			case "true":
				dst := fmt.Sprintf("%s/%d.%s", t.ArtifactsDir(), node, perfArtifactsDir)
				return c.Get(ctx, l, perfArtifactsDir, dst, c.Node(node))
			case "false":
				l.PrintfCtx(ctx, "no perf artifacts exist on node %v", c.Node(node))
				return nil
			default:
				return errors.Errorf("unexpected output when checking for perf artifacts: %s", out)
			}
		}
	}
	for _, i := range c.All() {
		g.GoCtx(fetchNode(i))
	}
	if err := g.Wait(); err != nil {
		l.PrintfCtx(ctx, "failed to get perf artifacts: %v", err)
	}
}

func allStacks() []byte {
	// Collect up to 5mb worth of stacks.
	b := make([]byte, 5*(1<<20))
	return b[:runtime.Stack(b, true /* all */)]
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
//
// TODO(test-eng): the `bool` should go away, instead `t.Failed()` can be consulted
// by the caller to determine whether the test passed.
func (r *testRunner) runTest(
	ctx context.Context,
	t *testImpl,
	runNum int,
	c *clusterImpl,
	testRunnerLogPath string,
	stdout io.Writer,
	l *logger.Logger,
) (bool, error) {
	if t.Spec().(*registry.TestSpec).Skip != "" {
		return false, fmt.Errorf("can't run skipped test: %s: %s", t.Name(), t.Spec().(*registry.TestSpec).Skip)
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

		// We only have to record panics if the panic'd value is not the sentinel
		// produced by t.Fatal*().
		if err := recover(); err != nil && err != errTestFatal {
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
			t.mu.Unlock()

			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
					t.Name(), teamCityEscape(output), t.Name(),
				)

				// Copy a snapshot of the testrunner's log to the test's artifacts dir
				// so that we collect it below.
				cp := exec.Command("cp", testRunnerLogPath, t.ArtifactsDir())
				if err := cp.Run(); err != nil {
					l.ErrorfCtx(ctx, "failed to copy test runner's logs to test artifacts: %s", err)
				}
			}

			shout(ctx, l, stdout, "--- FAIL: %s (%s)\n%s", t.Name(), durationStr, output)
			r.maybePostGithubIssue(ctx, l, t, stdout, output)
		} else {
			shout(ctx, l, stdout, "--- PASS: %s (%s)", t.Name(), durationStr)
			// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
			// TeamCity regards the test as successful.
		}

		if teamCity {
			shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s']", t.Name(), t.Name())

			// Zip the artifacts. This improves the TeamCity UX where we can navigate
			// through zip files just fine, but we can't download subtrees of the
			// artifacts storage. By zipping we get this capability as we can just
			// download the zip file for the failing test instead.
			if err := zipArtifacts(t.ArtifactsDir()); err != nil {
				l.Printf("unable to zip artifacts: %s", err)
			}

			if t.artifactsSpec != "" {
				// Tell TeamCity to collect this test's artifacts now. The TC job
				// also collects the artifacts directory wholesale at the end, but
				// here we make sure that the artifacts for any test that has already
				// finished are available in the UI even before the job as a whole
				// has completed. We're using the exact same destination to avoid
				// duplication of any of the artifacts.
				shout(ctx, l, stdout, "##teamcity[publishArtifacts '%s']", t.artifactsSpec)
			}
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
		if t.Spec().(*registry.TestSpec).Run != nil {
			if t.Failed() {
				r.status.fail[t] = struct{}{}
			} else if t.Spec().(*registry.TestSpec).Skip == "" {
				r.status.pass[t] = struct{}{}
			} else {
				r.status.skip[t] = struct{}{}
			}
		}
		r.status.Unlock()
	}()

	t.start = timeutil.Now()

	timeout := 10 * time.Hour
	if d := t.Spec().(*registry.TestSpec).Timeout; d != 0 {
		timeout = d
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
		defer func() {
			// We only have to record panics if the panic'd value is not the sentinel
			// produced by t.Fatal*().
			if r := recover(); r != nil && r != errTestFatal {
				// TODO(andreimatei): prevent the cluster from being reused.
				t.Fatalf("test panicked: %v", r)
			}
		}()

		t.Spec().(*registry.TestSpec).Run(runCtx, t, c)
	}()

	teardownL, err := c.l.ChildLogger("teardown", logger.QuietStderr, logger.QuietStdout)
	if err != nil {
		return false, err
	}
	select {
	case <-done:
		s := "success"
		if t.Failed() {
			s = "failure"
		}
		t.L().Printf("tearing down after %s; see teardown.log", s)
		l, c.l = teardownL, teardownL
		t.ReplaceL(teardownL)
	case <-time.After(timeout):
		t.L().Printf("tearing down after timeout; see teardown.log")
		l, c.l = teardownL, teardownL
		t.ReplaceL(teardownL)
		// Timeouts are often opaque. Improve our changes by dumping the stack
		// so that at least we can piece together what the test is trying to
		// do at this very moment.
		// In large roachtest runs, this will dump everyone else's stack as well,
		// but this is just hard to avoid. Moving to a worker model in which
		// each roachtest is spawned off as a separate process would address
		// this at the expense of ease of communication between the runner and
		// the test.
		//
		// Do this before we cancel the context, which might (hopefully) unblock
		// the test. We want to see where it got stuck.
		const stacksFile = "__stacks"
		if cl, err := t.L().ChildLogger(stacksFile, logger.QuietStderr, logger.QuietStdout); err == nil {
			cl.PrintfCtx(ctx, "all stacks:\n\n%s\n", allStacks())
			t.L().PrintfCtx(ctx, "dumped stacks to %s", stacksFile)
		}
		// Now kill anything going on in this cluster while collecting stacks
		// to the logs, to get the server side of the hang.
		//
		// TODO(tbg): send --sig=3 followed by a hard kill after we've fixed
		// https://github.com/cockroachdb/cockroach/issues/45875.
		// Signal 11 will dump stacks, but it might be confusing to folks
		// who debug from the artifacts only.
		//
		// Don't use surrounding context, which are likely already canceled.
		if nodes := c.All(); len(nodes) > 0 { // avoid tests
			innerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = c.StopE(innerCtx, c.All(), option.StopArgs("--sig=11"))
			cancel()
		}

		// Mark the test as failed (which will also cancel its context). Then
		// we'll wait up to 5 minutes in the hope that the test reacts either to
		// the ctx cancelation or to the fact that it was marked as failed (and
		// all processes nuked out from under it).
		// If that happens, great - we return normally and so the cluster can be
		// reused. It the test does not react to anything, then we return an
		// error, which will cause the caller to stop everything and destroy
		// this cluster (as well as all the others). The cluster cannot be
		// reused since we have a runaway test goroutine that's presumably going
		// to continue using the cluster.
		t.printfAndFail(0 /* skip */, "test timed out (%s)", timeout)
		select {
		case <-done:
			if success {
				panic("expected success=false after a timeout")
			}
		case <-time.After(5 * time.Minute):
			// We really shouldn't get here unless the test code somehow managed
			// to deadlock without blocking on anything remote - since we killed
			// everything.
			const msg = "test timed out and afterwards failed to respond to cancellation"
			t.L().PrintfCtx(ctx, msg)

			const stacksFile = "__stacks_after_cancellation"
			if cl, err := t.L().ChildLogger(stacksFile, logger.QuietStderr, logger.QuietStdout); err == nil {
				cl.PrintfCtx(ctx, "all stacks:\n\n%s\n", allStacks())
				t.L().PrintfCtx(ctx, "dumped stacks (after cancellation) to %s", stacksFile)
			}
			r.collectClusterLogs(ctx, c, t)
			// We already marked the test as failing, so don't return an error here.
			// Doing so would shut down the entire roachtest run.
			return false, nil
		}
	}

	// Detect dead nodes in an inner defer. Note that this will call
	// t.printfAndFail() when appropriate, which will cause the code below to
	// enter the t.Failed() branch.
	c.FailOnDeadNodes(ctx, t)

	if !t.Failed() {
		// Detect replica divergence (i.e. ranges in which replicas have arrived
		// at the same log position with different states).
		//
		// We avoid trying to do this when t.Failed() (and in particular when there
		// are dead nodes) because for reasons @tbg does not understand this gets
		// stuck occasionally, which really ruins the roachtest run. The method
		// below already uses a ctx timeout and SQL statement_timeout, but it does
		// not seem to be enough.
		//
		// TODO(testinfra): figure out why this can still get stuck despite the
		// above.
		c.FailOnReplicaDivergence(ctx, t)
	}

	if t.Failed() {
		r.collectClusterLogs(ctx, c, t)
		return false, nil
	}
	return true, nil
}

func (r *testRunner) shouldPostGithubIssue(t test.Test) bool {
	// NB: check NodeCount > 0 to avoid posting issues from this pkg's unit tests.
	opts := issues.DefaultOptionsFromEnv()
	return opts.CanPost() && opts.IsReleaseBranch() && t.Spec().(*registry.TestSpec).Run != nil && t.Spec().(*registry.TestSpec).Cluster.NodeCount > 0
}

func (r *testRunner) maybePostGithubIssue(
	ctx context.Context, l *logger.Logger, t test.Test, stdout io.Writer, output string,
) {
	if !r.shouldPostGithubIssue(t) {
		return
	}

	teams, err := team.DefaultLoadTeams()
	if err != nil {
		t.Fatalf("could not load teams: %v", err)
	}

	var mention []string
	var projColID int
	if sl, ok := teams.GetAliasesForPurpose(ownerToAlias(t.Spec().(*registry.TestSpec).Owner), team.PurposeRoachtest); ok {
		for _, alias := range sl {
			mention = append(mention, "@"+string(alias))
		}
		projColID = teams[sl[0]].TriageColumnID
	}

	branch := "<unknown branch>"
	if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
		branch = b
	}
	msg := fmt.Sprintf("The test failed on branch=%s, cloud=%s:\n%s",
		branch, t.Spec().(*registry.TestSpec).Cluster.Cloud, output)
	artifacts := fmt.Sprintf("/%s", t.Name())

	// Issues posted from roachtest are identifiable as such and
	// they are also release blockers (this label may be removed
	// by a human upon closer investigation).
	labels := []string{"O-roachtest"}
	if !t.Spec().(*registry.TestSpec).NonReleaseBlocker {
		labels = append(labels, "release-blocker")
	}

	req := issues.PostRequest{
		AuthorEmail:     "", // intentionally unset - we add to the board and cc the team
		Mention:         mention,
		ProjectColumnID: projColID,
		PackageName:     "roachtest",
		TestName:        t.Name(),
		Message:         msg,
		Artifacts:       artifacts,
		ExtraLabels:     labels,
		ReproductionCommand: fmt.Sprintf(
			`# From https://go.crdb.dev/p/roachstress, perhaps edited lightly.
caffeinate ./roachstress.sh %s
`, t.Name()),
	}
	if err := issues.Post(
		context.Background(),
		issues.UnitTestFormatter,
		req,
	); err != nil {
		shout(ctx, l, stdout, "failed to post issue: %s", err)
	}
}

func (r *testRunner) collectClusterLogs(ctx context.Context, c *clusterImpl, t test.Test) {
	// NB: fetch the logs even when we have a debug zip because
	// debug zip can't ever get the logs for down nodes.
	// We only save artifacts for failed tests in CI, so this
	// duplication is acceptable.
	// NB: fetch the logs *first* in case one of the other steps
	// below has problems. For example, `debug zip` is known to
	// hang sometimes at the time of writing, see:
	// https://github.com/cockroachdb/cockroach/issues/39620
	t.L().PrintfCtx(ctx, "collecting cluster logs")
	// Do this before collecting logs to make sure the file gets
	// downloaded below.
	if err := saveDiskUsageToLogsDir(ctx, c); err != nil {
		t.L().Printf("failed to fetch disk uage summary: %s", err)
	}
	if err := c.FetchLogs(ctx, t); err != nil {
		t.L().Printf("failed to download logs: %s", err)
	}
	if err := c.FetchDmesg(ctx, t); err != nil {
		t.L().Printf("failed to fetch dmesg: %s", err)
	}
	if err := c.FetchJournalctl(ctx, t); err != nil {
		t.L().Printf("failed to fetch journalctl: %s", err)
	}
	if err := c.FetchCores(ctx, t); err != nil {
		t.L().Printf("failed to fetch cores: %s", err)
	}
	if err := c.CopyRoachprodState(ctx); err != nil {
		t.L().Printf("failed to copy roachprod state: %s", err)
	}
	if err := c.FetchTimeseriesData(ctx, t); err != nil {
		t.L().Printf("failed to fetch timeseries data: %s", err)
	}
	if err := c.FetchDebugZip(ctx, t); err != nil {
		t.L().Printf("failed to collect zip: %s", err)
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
	createCluster func(context.Context, testToRunRes) (*clusterImpl, error)
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
	c *clusterImpl,
	interrupt <-chan struct{},
	l *logger.Logger,
	callbacks getWorkCallbacks,
) (testToRunRes, *clusterImpl, error) {

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
		var clusterName, clusterAdminUIAddr string
		if w.Cluster() != nil {
			clusterName = w.Cluster().name
			adminUIAddrs, err := w.Cluster().ExternalAdminUIAddr(req.Context(), w.Cluster().Node(1))
			if err == nil {
				clusterAdminUIAddr = adminUIAddrs[0]
			}
		}
		t := w.Test()
		testStatus := "N/A"
		if t != nil {
			testStatus = t.GetStatus()
		}
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><td><a href='//%s'>%s</a></td><td>%s</td><td>%s</td></tr>\n",
			w.name, w.Status(), testName, clusterAdminUIAddr, clusterName, clusterReused, testStatus)
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
			status = "FAIL " + strings.ReplaceAll(html.EscapeString(t.failure), "\n", "<br>")
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

func zipArtifacts(path string) error {
	f, err := os.Create(filepath.Join(path, "artifacts.zip"))
	if err != nil {
		return err
	}
	defer f.Close()
	z := zip.NewWriter(f)
	rel := func(targetpath string) string {
		relpath, err := filepath.Rel(path, targetpath)
		if err != nil {
			return targetpath
		}
		return relpath
	}

	walk := func(visitor func(string, os.FileInfo) error) error {
		return filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, ".zip") {
				// Skip any top-level zip files, which notably includes itself
				// and, if present, the debug.zip.
				return nil
			}
			return visitor(path, info)
		})
	}

	// Zip all of the files.
	if err := walk(func(path string, info os.FileInfo) error {
		if info.IsDir() {
			return nil
		}
		w, err := z.Create(rel(path))
		if err != nil {
			return err
		}
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()
		if _, err := io.Copy(w, r); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := z.Close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	// Now that the zip file is there, remove all of the files that went into it.
	// Note that 'walk' skips the debug.zip and our newly written zip file.
	root := path
	return walk(func(path string, info os.FileInfo) error {
		if path == root {
			return nil
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		return nil
	})
}
