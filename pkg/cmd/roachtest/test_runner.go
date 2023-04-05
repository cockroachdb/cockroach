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
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/petermattis/goid"
)

var (
	errTestsFailed = fmt.Errorf("some tests failed")

	// reference error used by main.go at the end of a run of tests
	errSomeClusterProvisioningFailed = fmt.Errorf("some clusters could not be created")

	// reference error used when cluster creation fails for a test
	errClusterProvisioningFailed = fmt.Errorf("cluster could not be created")

	prometheusNameSpace = "roachtest"
	// prometheusScrapeInterval should be consistent with the scrape interval defined in
	// https://grafana.testeng.crdb.io/prometheus/config
	prometheusScrapeInterval = time.Second * 15
)

// testRunner runs tests.
type testRunner struct {
	stopper *stop.Stopper

	config struct {
		// skipClusterValidationOnAttach skips validation on existing clusters that
		// the registry uses for running tests.
		skipClusterValidationOnAttach bool
		// skipClusterStopOnAttach skips stopping existing clusters that
		// the registry uses for running tests. It implies skipClusterWipeOnAttach.
		skipClusterStopOnAttach bool
		skipClusterWipeOnAttach bool
		// disableIssue disables posting GitHub issues for test failures.
		disableIssue bool
		// overrideShutdownPromScrapeInterval overrides the default time a test runner waits to
		// shut down, normally used to ensure a remote prometheus server has scraped the roachtest
		// endpoint.
		overrideShutdownPromScrapeInterval time.Duration
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

	// Counts cluster creation errors across all workers.
	numClusterErrs int32
}

// newTestRunner constructs a testRunner.
//
// cr: The cluster registry with which all clusters will be registered. The
//
//	caller provides this as the caller needs to be able to shut clusters down
//	on Ctrl+C.
func newTestRunner(cr *clusterRegistry, stopper *stop.Stopper) *testRunner {
	r := &testRunner{
		stopper: stopper,
		cr:      cr,
	}
	r.config.skipClusterWipeOnAttach = !clusterWipe
	r.config.disableIssue = disableIssue
	r.workersMu.workers = make(map[string]*workerStatus)
	return r
}

func newUnitTestRunner(cr *clusterRegistry, stopper *stop.Stopper) *testRunner {
	r := newTestRunner(cr, stopper)
	// To speed up unit tests, reduce test runner shutdown time.
	r.config.overrideShutdownPromScrapeInterval = time.Millisecond
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

	// Controls whether the cluster is cleaned up at the end of the test.
	debugMode  debugMode
	enableFIPS bool
}

type debugMode int

const (
	// NoDebug does not enable any debug behaviour. Clusters will
	// be destroyed regardless of the test result.
	NoDebug debugMode = iota
	// DebugKeepOnFailure does not wipe or destroy a cluster when
	// a test using the respective cluster fails. These clusters
	// will linger around and they'll continue counting towards
	// the cpuQuota.
	DebugKeepOnFailure
	// DebugKeepAlways never wipes or destroys a cluster.
	DebugKeepAlways
)

func (p debugMode) IsDebug() bool {
	return p == DebugKeepOnFailure || p == DebugKeepAlways
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
	skipInit               bool
}

// Run runs tests.
//
// Args:
// tests: The tests to run.
// count: How many times to run each test selected by filter.
// parallelism: How many workers to use for running tests. Tests are run
//
//	locally (although generally they run against remote roachprod clusters).
//	parallelism bounds the maximum number of tests that run concurrently. Note
//	that the concurrency is also affected by cpuQuota.
//
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
	clusterAllocator clusterAllocatorFn,
) error {
	// Validate options.
	if len(tests) == 0 {
		return fmt.Errorf("no test matched filters")
	}

	hasDevLicense := config.CockroachDevLicense != ""
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
	if clusterAllocator == nil {
		clusterAllocator = defaultClusterAllocator(r, clustersOpt, lopt)
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
	errs := &workerErrors{}

	qp := quotapool.NewIntPool("cloud cpu", uint64(clustersOpt.cpuQuota))
	l := lopt.l
	var wg sync.WaitGroup

	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		if err := r.stopper.RunAsyncTask(ctx, "worker", func(ctx context.Context) {
			defer wg.Done()

			err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, r.work, qp,
				r.stopper.ShouldQuiesce(),
				clustersOpt.debugMode,
				lopt.artifactsDir, lopt.literalArtifactsDir, lopt.tee, lopt.stdout,
				clusterAllocator,
				topt,
				l,
			)

			if err != nil {
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
					r.stopper.Stop(ctx)
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
	shutdownStart := timeutil.Now()
	r.cr.destroyAllClusters(ctx, l)

	if errs.Err() != nil {
		shout(ctx, l, lopt.stdout, "FAIL (err: %s)", errs.Err())
		return errs.Err()
	}
	passFailLine := r.generateReport()
	shout(ctx, l, lopt.stdout, passFailLine)

	if r.numClusterErrs > 0 {
		shout(ctx, l, lopt.stdout, "%d clusters could not be created", r.numClusterErrs)
		return errSomeClusterProvisioningFailed
	}

	if len(r.status.fail) > 0 {
		return errTestsFailed
	}
	// To ensure all prometheus metrics have been scraped, ensure shutdown takes
	// at least one scrapeInterval, unless the roachtest fails or gets cancelled.
	requiredShutDownTime := prometheusScrapeInterval
	if r.config.overrideShutdownPromScrapeInterval > 0 {
		requiredShutDownTime = r.config.overrideShutdownPromScrapeInterval
	}
	if shutdownSleep := requiredShutDownTime - timeutil.Since(shutdownStart); shutdownSleep > 0 {
		select {
		case <-r.stopper.ShouldQuiesce():
		case <-time.After(shutdownSleep):
		}
	}
	return nil
}

// N.B. currently this value is hardcoded per cloud provider.
func numConcurrentClusterCreations() int {
	var res int
	if cloud == "aws" {
		// AWS has ridiculous API calls limits, so we're going to create one cluster
		// at a time. Internally, roachprod has throttling for the calls required to
		// create a single cluster.
		res = 1
	} else {
		res = 1000
	}
	return res
}

// defaultClusterAllocator is used by workers to create new clusters (or to attach
// to an existing one).
//
// N.B. the resulting clusterAllocatorFn reuses the same clusterFactory to allocate clusters.
func defaultClusterAllocator(
	r *testRunner, clustersOpt clustersOpt, lopt loggingOpt,
) clusterAllocatorFn {
	clusterFactory := newClusterFactory(
		clustersOpt.user, clustersOpt.clusterID, lopt.artifactsDir, r.cr, numConcurrentClusterCreations())

	allocateCluster := func(
		ctx context.Context,
		t registry.TestSpec,
		alloc *quotapool.IntAlloc,
		artifactsDir string,
		wStatus *workerStatus,
	) (*clusterImpl, *vm.CreateOpts, error) {
		wStatus.SetStatus("creating cluster")
		defer wStatus.SetStatus("")

		existingClusterName := clustersOpt.clusterName
		if existingClusterName != "" {
			// Logs for attaching to a cluster go to a dedicated log file.
			logPath := filepath.Join(artifactsDir, runnerLogsDir, "cluster-create", existingClusterName+".log")
			clusterL, err := logger.RootLogger(logPath, lopt.tee)
			if err != nil {
				return nil, nil, err
			}
			defer clusterL.Close()
			opt := attachOpt{
				skipValidation: r.config.skipClusterValidationOnAttach,
				skipStop:       r.config.skipClusterStopOnAttach,
				skipWipe:       r.config.skipClusterWipeOnAttach,
			}
			lopt.l.PrintfCtx(ctx, "Attaching to existing cluster %s for test %s", existingClusterName, t.Name)
			c, err := attachToExistingCluster(ctx, existingClusterName, clusterL, t.Cluster, opt, r.cr)
			if err == nil {
				return c, nil, nil
			}
			if !errors.Is(err, errClusterNotFound) {
				return nil, nil, err
			}
			// Fall through to create new cluster with name override.
			lopt.l.PrintfCtx(
				ctx, "Creating new cluster with custom name %q for test %s: %s",
				clustersOpt.clusterName, t.Name, t.Cluster,
			)
		} else {
			lopt.l.PrintfCtx(ctx, "Creating new cluster for test %s: %s", t.Name, t.Cluster)
		}

		cfg := clusterConfig{
			nameOverride: clustersOpt.clusterName, // only set if we hit errClusterFound above
			spec:         t.Cluster,
			artifactsDir: artifactsDir,
			username:     clustersOpt.user,
			localCluster: clustersOpt.typ == localCluster,
			alloc:        alloc,
			enableFIPS:   clustersOpt.enableFIPS,
		}
		return clusterFactory.newCluster(ctx, cfg, wStatus.SetStatus, lopt.tee)
	}
	return allocateCluster
}

type clusterAllocatorFn func(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, *vm.CreateOpts, error)

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
// If a cluster cannot be provisioned (owing to an infrastructure issue), the corresponding
// test is skipped; the provisioning error is posted to github; the count of cluster provisioning
// errors is incremented.
//
// runWorker returns either error (other than cluster provisioning) or the count of cluster provisioning errors.
//
// Args:
// name: The worker's name, to be used as a prefix for log messages.
// artifactsRootDir: The artifacts dir. Each test's logs are going to be under a
//
//	run_<n> dir. If empty, test log files will not be created.
//
// literalArtifactsDir: The literal on-agent path where artifacts are stored.
//
//	Only used for teamcity[publishArtifacts] messages.
//
// stdout: The Writer to use for messages that need to go to stdout (e.g. the
//
//	"=== RUN" and "--- FAIL" lines).
//
// teeOpt: The teeing option for future test loggers.
// l: The logger to use for more verbose messages.
func (r *testRunner) runWorker(
	ctx context.Context,
	name string,
	work *workPool,
	qp *quotapool.IntPool,
	interrupt <-chan struct{},
	debugMode debugMode,
	artifactsRootDir string,
	literalArtifactsDir string,
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

	prng, _ := randutil.NewPseudoRand()

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
		var err error

		wStatus.SetTest(nil /* test */, testToRunRes{})
		wStatus.SetStatus("getting work")
		testToRun, err = r.getWork(
			ctx, work, qp, c, interrupt, l,
			getWorkCallbacks{
				onDestroy: func() {
					wStatus.SetCluster(nil)
				},
			})
		if err != nil {
			// Problem selecting a test, bail out.
			return err
		}
		if testToRun.noWork {
			shout(ctx, l, stdout, "no work remaining; runWorker is bailing out...")
			return nil
		}
		// Attempt to reuse existing cluster.
		if c != nil && testToRun.canReuseCluster {
			err = func() error {
				l.PrintfCtx(ctx, "Using existing cluster: %s. Wiping", c.name)
				if err := c.WipeE(ctx, l); err != nil {
					return err
				}
				if err := c.RunE(ctx, c.All(), "rm -rf "+perfArtifactsDir); err != nil {
					return errors.Wrapf(err, "failed to remove perf artifacts dir")
				}
				if c.localCertsDir != "" {
					if err := os.RemoveAll(c.localCertsDir); err != nil {
						return errors.Wrapf(err,
							"failed to remove local certs in %s", c.localCertsDir)
					}
					c.localCertsDir = ""
				}
				// Overwrite the spec of the cluster with the one coming from the test. In
				// particular, this overwrites the reuse policy to reflect what the test
				// intends to do with it.
				c.spec = testToRun.spec.Cluster
				return nil
			}()
			if err != nil {
				shout(ctx, l, stdout, "Unable to reuse cluster: %s due to: %s. Will attempt to create a fresh one",
					c.Name(), err)
				// N.B. we do not count reuse attempt error toward clusterCreateErr.
				// Let's attempt to create a fresh one.
				testToRun.canReuseCluster = false
			}
		}

		// Verify that required native libraries are available.
		if err = VerifyLibraries(testToRun.spec.NativeLibs); err != nil {
			shout(ctx, l, stdout, "Library verification failed: %s", err)
			return err
		}

		var clusterCreateErr error
		var vmCreateOpts *vm.CreateOpts

		if !testToRun.canReuseCluster {
			// Create a new cluster if can't reuse or reuse attempt failed.
			// N.B. non-reusable cluster would have been destroyed above.
			wStatus.SetTest(nil /* test */, testToRun)
			wStatus.SetStatus("creating cluster")
			c, vmCreateOpts, clusterCreateErr = allocateCluster(ctx, testToRun.spec, testToRun.alloc, artifactsRootDir, wStatus)
			if clusterCreateErr != nil {
				clusterCreateErr = errors.Mark(clusterCreateErr, errClusterProvisioningFailed)
				atomic.AddInt32(&r.numClusterErrs, 1)
				shout(ctx, l, stdout, "Unable to create (or reuse) cluster for test %s due to: %s.",
					testToRun.spec.Name, clusterCreateErr)
			}
		}
		// Prepare the test's logger. Always set this up with real files, using a
		// temp dir if necessary. This simplifies testing.
		if artifactsRootDir == "" {
			artifactsRootDir, _ = os.MkdirTemp("", "roachtest-logger")
		}

		escapedTestName := teamCityNameEscape(testToRun.spec.Name)
		runSuffix := "run_" + strconv.Itoa(testToRun.runNum)

		artifactsDir := filepath.Join(filepath.Join(artifactsRootDir, escapedTestName), runSuffix)
		logPath := filepath.Join(artifactsDir, "test.log")

		// Map artifacts/TestFoo/run_?/** => TestFoo/run_?/**, i.e. collect the artifacts
		// for this test exactly as they are laid out on disk (when the time
		// comes).
		artifactsSpec := fmt.Sprintf("%s/%s/** => %s/%s", filepath.Join(literalArtifactsDir, escapedTestName), runSuffix, escapedTestName, runSuffix)

		testL, err := logger.RootLogger(logPath, teeOpt)
		if err != nil {
			return err
		}
		binaryVersion, err := version.Parse(build.BinaryVersion())
		if err != nil {
			return err
		}
		t := &testImpl{
			spec:                   &testToRun.spec,
			cockroach:              cockroach,
			cockroachShort:         cockroachShort,
			deprecatedWorkload:     workload,
			buildVersion:           binaryVersion,
			artifactsDir:           artifactsDir,
			artifactsSpec:          artifactsSpec,
			l:                      testL,
			versionsBinaryOverride: topt.versionsBinaryOverride,
			skipInit:               topt.skipInit,
			debug:                  debugMode.IsDebug(),
		}
		// Now run the test.
		l.PrintfCtx(ctx, "starting test: %s:%d", testToRun.spec.Name, testToRun.runNum)

		github := newGithubIssues(r.config.disableIssue, c, vmCreateOpts)

		if clusterCreateErr != nil {
			// N.B. cluster creation must have failed...
			// We don't want to prematurely abort the test suite since it's likely a transient issue.
			// Instead, let's report an infrastructure issue, mark the test as failed and continue with the next test.

			// Generate failure reason and mark the test failed to preclude fetching (cluster) artifacts.
			t.Error(clusterCreateErr)
			// N.B. issue title is of the form "roachtest: ${t.spec.Name} failed" (see UnitTestFormatter).
			if err := github.MaybePost(t, l, t.failureMsg()); err != nil {
				shout(ctx, l, stdout, "failed to post issue: %s", err)
			}
		} else {
			c.setTest(t)
			if c.spec.NodeCount > 0 { // skip during tests
				err = c.PutDefaultCockroach(ctx, l, t.Cockroach())
			}
			if err == nil {
				err = c.PutLibraries(ctx, "./lib", t.spec.NativeLibs)
			}

			if err == nil {
				// Tell the cluster that, from now on, it will be run "on behalf of this
				// test".
				c.status("running test")

				switch t.Spec().(*registry.TestSpec).EncryptionSupport {
				case registry.EncryptionAlwaysEnabled:
					c.encAtRest = true
				case registry.EncryptionAlwaysDisabled:
					c.encAtRest = false
				case registry.EncryptionMetamorphic:
					// when tests opted-in to metamorphic testing, encryption will
					// be enabled according to the probability passed to
					// --metamorphic-encryption-probability
					c.encAtRest = prng.Float64() < encryptionProbability
				}

				wStatus.SetCluster(c)
				wStatus.SetTest(t, testToRun)
				wStatus.SetStatus("running test")

				err = r.runTest(ctx, t, testToRun.runNum, testToRun.runCount, c, stdout, testL, github)
			}
		}

		if err != nil {
			shout(ctx, l, stdout, "test returned error: %s: %s", t.Name(), err)
			// Mark the test as failed if it isn't already.
			if !t.Failed() {
				t.Error(err)
			}
		} else {
			msg := "test passed: %s (run %d)"
			if t.Failed() {
				msg = "test failed: %s (run %d)"
			}
			msg = fmt.Sprintf(msg, t.Name(), testToRun.runNum)
			l.PrintfCtx(ctx, msg)
		}
		testL.Close()
		if err != nil || t.Failed() {
			failureMsg := fmt.Sprintf("%s (%d) - ", testToRun.spec.Name, testToRun.runNum)
			if err != nil {
				failureMsg += fmt.Sprintf("%+v", err)
			} else {
				failureMsg += t.failureMsg()
			}
			if c != nil {
				switch debugMode {
				case DebugKeepAlways, DebugKeepOnFailure:
					// Save the cluster for future debugging.
					c.Save(ctx, failureMsg, l)

					// Continue with a fresh cluster.
					c = nil
				case NoDebug:
					// On any test failure or error, we destroy the cluster. We could be
					// more selective, but this sounds safer.
					l.PrintfCtx(ctx, "destroying cluster %s because: %s", c, failureMsg)
					c.Destroy(context.Background(), closeLogger, l)
					c = nil
				}
			}
			if err != nil {
				// N.B. bail out iff runTest exits exceptionally.
				return err
			}
		} else {
			// Upon success fetch the perf artifacts from the remote hosts.
			getPerfArtifacts(ctx, l, c, t)
			if debugMode == DebugKeepAlways {
				c.Save(ctx, "cluster saved since --debug-always set", l)
				c = nil
			}
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
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), "bash", "-c", testCmd)
			if err != nil {
				return errors.Wrapf(err, "failed to check for perf artifacts")
			}
			out := strings.TrimSpace(result.Stdout)
			switch out {
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
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy
//
//	the cluster.
func (r *testRunner) runTest(
	ctx context.Context,
	t *testImpl,
	runNum int,
	runCount int,
	c *clusterImpl,
	stdout io.Writer,
	l *logger.Logger,
	github *githubIssues,
) error {

	runID := t.Name()
	if runCount > 1 {
		runID += fmt.Sprintf("#%d", runNum)
	}
	if !teamCity {
		shout(ctx, l, stdout, "=== RUN   %s", runID)
	}

	r.status.Lock()
	r.status.running[t] = struct{}{}
	r.status.Unlock()

	t.runner = callerName()
	t.runnerID = goid.Get()

	s := t.Spec().(*registry.TestSpec)
	defer func() {
		t.end = timeutil.Now()

		// We only have to record panics if the panic'd value is not the sentinel
		// produced by t.Fatal*(). We may see calls to t.Fatal from this goroutine
		// during the post-flight checks; the test itself runs on a different
		// goroutine and has similar code to terminate errTestFatal.
		if err := recover(); err != nil && err != errTestFatal {
			t.Error(err)
		}

		t.mu.Lock()
		t.mu.done = true
		t.mu.Unlock()

		if s.Skip != "" {
			// When skipping a test, we should not report ##teamcity[testStarted...] or ##teamcity[testFinished...]
			// service messages else the test will be reported as having run twice.
			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testIgnored name='%s' message='%s' duration='%d']\n",
					s.Name, teamCityEscape(s.Skip), t.duration().Milliseconds())
			}
			shout(ctx, l, stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "N/A", s.Skip)
		} else {
			// Delaying the ##teamcity[testStarted...] service message until the test is finished allows us to branch
			// separately for skipped tests. The duration of the test is passed to ##teamcity[testFinished...] for
			// accurate reporting in the TC UI.
			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testStarted name='%s' flowId='%s']", t.Name(), runID)
			}

			durationStr := fmt.Sprintf("%.2fs", t.duration().Seconds())
			if t.Failed() {
				output := fmt.Sprintf("test artifacts and logs in: %s\n%s", t.ArtifactsDir(), t.failureMsg())

				if teamCity {
					// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
					// TeamCity regards the test as successful.
					shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
						s.Name, teamCityEscape(output), runID)
				}

				shout(ctx, l, stdout, "--- FAIL: %s (%s)\n%s", runID, durationStr, output)

				if err := github.MaybePost(t, l, output); err != nil {
					shout(ctx, l, stdout, "failed to post issue: %s", err)
				}
			} else {
				shout(ctx, l, stdout, "--- PASS: %s (%s)", runID, durationStr)
			}

			shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s' duration='%d']",
				t.Name(), runID, t.duration().Milliseconds())
		}

		if teamCity {

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
			failure: t.failureMsg(),
		})
		r.status.Lock()
		delete(r.status.running, t)
		// Only include tests with a Run function in the summary output.
		if s.Run != nil {
			if t.Failed() {
				r.status.fail[t] = struct{}{}
			} else if s.Skip != "" {
				r.status.skip[t] = struct{}{}
			} else {
				r.status.pass[t] = struct{}{}
			}
		}
		r.status.Unlock()
	}()

	t.start = timeutil.Now()

	timeout := 10 * time.Hour
	if d := s.Timeout; d != 0 {
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
			return errors.Wrapf(err, "failed to extend cluster: %s", c.name)
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
	testReturnedCh := make(chan struct{})
	go func() {
		defer close(testReturnedCh) // closed only after we've grabbed the debug info below

		defer func() {
			// We only have to record panics if the panic'd value is not the sentinel
			// produced by t.Fatal*().
			if r := recover(); r != nil && r != errTestFatal {
				// NB: we're careful to avoid t.Fatalf here, which re-panics.
				// Note that the error will be logged to a file, and the stack will
				// contain the source of the panic.
				t.Errorf("test panicked: %v", r)
			}
		}()

		// This is the call to actually run the test.
		s.Run(runCtx, t, c)
	}()

	var timedOut bool

	select {
	case <-testReturnedCh:
		s := "success"
		if t.Failed() {
			s = "failure"
		}
		t.L().Printf("tearing down after %s; see teardown.log", s)
	case <-time.After(timeout):
		// NB: We're adding the timeout failure intentionally without cancelling the context
		// to capture as much state as possible during artifact collection.
		t.addFailure(0, "test timed out (%s)", timeout)
		timedOut = true
	}

	// From now on, all logging goes to teardown.log to give a clear
	// separation between operations originating from the test vs the
	// harness.
	teardownL, err := c.l.ChildLogger("teardown", logger.QuietStderr, logger.QuietStdout)
	if err != nil {
		return err
	}
	l, c.l = teardownL, teardownL
	t.ReplaceL(teardownL)

	return r.teardownTest(ctx, t, c, timedOut)
}

func (r *testRunner) teardownTest(
	ctx context.Context, t *testImpl, c *clusterImpl, timedOut bool,
) error {

	// We still have to collect artifacts and run post-flight checks, and any of
	// these might hang. So they go into a goroutine and the main goroutine
	// abandons them after a timeout. We intentionally don't wait for the
	// goroutines to return, as this too may hang if something doesn't respond to
	// ctx cancellation.

	artifactsCollectedCh := make(chan struct{})
	_ = r.stopper.RunAsyncTask(ctx, "collect-artifacts", func(ctx context.Context) {
		// TODO(tbg): make `t` and `logger` resilient to use-after-Close to avoid
		// crashes here in cases where the goroutine leaks but later gets unstuck
		// and tries to log something.
		defer close(artifactsCollectedCh)
		if timedOut {
			// Timeouts are often opaque. Improve our changes by dumping the stack
			// so that at least we can piece together what the test is trying to
			// do at this very moment.
			//
			// We're careful here to not fail the test, i.e. we don't call t.Error
			// here. We want to preserve as much state as possible in the artifacts,
			// and calling t.{Error,Fatal}{,f} cancels the test's main context.
			//
			// We make sure to fail the test later when handling the timedOut variable.
			const stacksFile = "__stacks"
			if cl, err := t.L().ChildLogger(stacksFile, logger.QuietStderr, logger.QuietStdout); err == nil {
				sl := allStacks()
				if c.Spec().NodeCount == 0 {
					sl = []byte("<elided during unit test>") // keep test outputs clutter-free
				}
				cl.PrintfCtx(ctx, "all stacks:\n\n%s\n", sl)
				t.L().PrintfCtx(ctx, "dumped stacks to %s", stacksFile)
			}

			// Send SIGQUIT to ask all processes to dump stacks if requested (without shutting down).
			// We need to do this before collectClusterArtifacts below, which will download the logs.
			// Note that the debug.zip will hopefully also contain stacks, but we're just making sure
			// there's something even if the debug.zip doesn't go through.
			args := option.DefaultStopOpts()
			if c.Spec().GatherCores {
				// Need to use ABRT to get cores.
				args.RoachprodOpts.Sig = 6
			} else {
				args.RoachprodOpts.Sig = 3
			}
			err := c.StopE(ctx, t.L(), args, c.All())
			t.L().PrintfCtx(ctx, "asked CRDB nodes to dump stacks; check their main (DEV) logs: %v", err)
			// It takes a little moment for the stacks to get flushed to the logs.
			// Against a real cluster they'll typically be there by the time we fetch
			// logs but on local clusters this may not be true; either way better to
			// not take any chances.
			if c.Spec().NodeCount > 0 { // unit tests
				time.Sleep(3 * time.Second)
			}
		}

		// Detect dead nodes. This will call t.Error() when appropriate. Note that
		// we do this even if t.Failed() since a down node is often the reason for
		// the failure, and it's helpful to have the listing in the teardown logs
		// as well (it is typically already in the main logs if the test used a
		// monitor).
		c.assertNoDeadNode(ctx, t)

		// We avoid trying to do this when t.Failed() (and in particular when there
		// are dead nodes) because for reasons @tbg does not understand this gets
		// stuck occasionally, which really ruins the roachtest run. The method
		// below already uses a ctx timeout and SQL statement_timeout, but it does
		// not seem to be enough.
		//
		// TODO(testinfra): figure out why this can still get stuck despite the
		// above.
		db, node := c.ConnectToLiveNode(ctx, t)
		if db != nil {
			defer db.Close()
			t.L().Printf("running validation checks on node %d (<10m)", node)
			if t.spec.SkipPostValidations&registry.PostValidationInvalidDescriptors == 0 {
				c.FailOnInvalidDescriptors(ctx, db, t)
			}
			// Detect replica divergence (i.e. ranges in which replicas have arrived
			// at the same log position with different states).
			if t.spec.SkipPostValidations&registry.PostValidationReplicaDivergence == 0 {
				c.FailOnReplicaDivergence(ctx, db, t)
			}
		} else {
			t.L().Printf("no live node found, skipping validation checks")
		}

		if timedOut || t.Failed() {
			r.collectClusterArtifacts(ctx, c, t.L())
		}
	})

	const artifactsCollectionTimeout = time.Hour
	select {
	case <-artifactsCollectedCh:
	case <-time.After(artifactsCollectionTimeout):
		// Leak the artifacts collection goroutine. Note that the test may not be
		// marked as failing here. We intentionally do not trigger it to fail here,
		// but we could entertain doing so once we have a mechanism that can route
		// such post-test problems to the test-eng team.
		t.L().Printf("giving up on artifacts collection after %s", artifactsCollectionTimeout)
	}

	if timedOut {
		// Shut down the cluster. We only do this on timeout to help the test terminate;
		// for regular failures, if the --debug flag is used, we want the cluster to stay
		// around so someone can poke at it.
		_ = c.StopE(ctx, t.L(), option.DefaultStopOpts(), c.All())

		// We previously added a timeout failure without cancellation, so we cancel here.
		if t.mu.cancel != nil {
			t.mu.cancel()
		}
		t.L().Printf("test timed out; check __stacks.log and CRDB logs for goroutine dumps")
	}
	return nil
}

func (r *testRunner) collectClusterArtifacts(
	ctx context.Context, c *clusterImpl, l *logger.Logger,
) {
	// NB: fetch the logs even when we have a debug zip because
	// debug zip can't ever get the logs for down nodes.
	// We only save artifacts for failed tests in CI, so this
	// duplication is acceptable.
	// NB: fetch the logs *first* in case one of the other steps
	// below has problems.
	l.PrintfCtx(ctx, "collecting cluster logs")
	// Do this before collecting logs to make sure the file gets
	// downloaded below.
	if err := saveDiskUsageToLogsDir(ctx, c); err != nil {
		l.Printf("failed to fetch disk uage summary: %s", err)
	}
	if err := c.FetchLogs(ctx, l); err != nil {
		l.Printf("failed to download logs: %s", err)
	}
	if err := c.FetchDmesg(ctx, l); err != nil {
		l.Printf("failed to fetch dmesg: %s", err)
	}
	if err := c.FetchJournalctl(ctx, l); err != nil {
		l.Printf("failed to fetch journalctl: %s", err)
	}
	if err := c.FetchCores(ctx, l); err != nil {
		l.Printf("failed to fetch cores: %s", err)
	}
	if err := c.CopyRoachprodState(ctx); err != nil {
		l.Printf("failed to copy roachprod state: %s", err)
	}
	if err := c.FetchTimeseriesData(ctx, l); err != nil {
		l.Printf("failed to fetch timeseries data: %s", err)
	}
	if err := c.FetchDebugZip(ctx, l); err != nil {
		l.Printf("failed to collect zip: %s", err)
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
	onDestroy func()
}

// getWork selects the next test to run and creates a suitable cluster for it if
// need be. If a new cluster needs to be created, the method blocks until there
// are enough resources available to run it.
// getWork takes in a cluster; if not nil, tests that can reuse it are
// preferred. If a test that can reuse it is not found (or if there's no more
// work), the cluster is destroyed (and so its resources are released).
func (r *testRunner) getWork(
	ctx context.Context,
	work *workPool,
	qp *quotapool.IntPool,
	c *clusterImpl,
	interrupt <-chan struct{},
	l *logger.Logger,
	callbacks getWorkCallbacks,
) (testToRunRes, error) {

	select {
	case <-interrupt:
		return testToRunRes{}, fmt.Errorf("interrupted")
	default:
	}

	testToRun, err := work.getTestToRun(ctx, c, qp, r.cr, callbacks.onDestroy, l)
	if err != nil {
		return testToRunRes{}, err
	}
	if !testToRun.noWork {
		l.PrintfCtx(ctx, "Selected test: %s run: %d.", testToRun.spec.Name, testToRun.runNum)
	} else {
		// We're done--there are no remaining tests.
		return testToRun, nil
	}
	return testToRun, nil
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

// removeWorker deletes the bookkeeping for a worker that has finished running.
func (r *testRunner) removeWorker(ctx context.Context, name string) {
	r.workersMu.Lock()
	delete(r.workersMu.workers, name)
	r.workersMu.Unlock()
}

// runHTTPServer starts a server running in the background.
//
// httpPort: The port on which to serve the web interface. Pass 0 for allocating
// bindTo: The host/ip on which to bind. Leave empty to bind on all local ips
//
//	a port automatically (which will be printed to stdout).
func (r *testRunner) runHTTPServer(httpPort int, stdout io.Writer, bindTo string) error {
	http.HandleFunc("/", r.serveHTTP)
	// Run an http server in the background.
	// We handle the case where httpPort is 0, which means we automatically
	// allocate a port.
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindTo, httpPort))
	if err != nil {
		return err
	}
	httpPort = listener.Addr().(*net.TCPAddr).Port
	go func() {
		if err := http.Serve(listener, nil /* handler */); err != nil {
			panic(err)
		}
	}()
	bindToDesc := "all network interfaces"
	if bindTo != "" {
		bindToDesc = bindTo
	}
	fmt.Fprintf(stdout, "HTTP server listening on %s, port %d.\n", bindToDesc, httpPort)
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
			adminUIAddrs, err := w.Cluster().ExternalAdminUIAddr(req.Context(), w.Cluster().l, w.Cluster().Node(1))
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
			dir, _ := filepath.Split(rel(path))
			isTopLevel := dir == ""
			if !info.IsDir() && isTopLevel && strings.HasSuffix(path, ".zip") {
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
