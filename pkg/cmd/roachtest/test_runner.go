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
	gosql "database/sql"
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
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

	prometheusNameSpace = "roachtest"
	// prometheusScrapeInterval should be consistent with the scrape interval defined in
	// https://grafana.testeng.crdb.io/prometheus/config
	prometheusScrapeInterval = time.Second * 15

	// errClusterProvisioningFailed wraps the error given in an error
	// that is properly sent to Test Eng and marked as an infra flake.
	errClusterProvisioningFailed = func(err error) error {
		return registry.ErrorWithOwner(
			registry.OwnerTestEng, err,
			registry.WithTitleOverride("cluster_creation"),
			registry.InfraFlake,
		)
	}

	// vmPreemptionError is the error that indicates that a test failed
	// *and* VMs were preempted. These errors are directed to Test Eng
	// instead of owning teams.
	vmPreemptionError = func(preemptedVMs string) error {
		return registry.ErrorWithOwner(
			registry.OwnerTestEng, fmt.Errorf("preempted VMs: %s", preemptedVMs),
			registry.WithTitleOverride("vm_preemption"),
			registry.InfraFlake,
		)
	}

	// vmHostError is the error that indicates that a test failed
	// a result of VM host error. These errors are directed to Test Eng
	// instead of owning teams.
	vmHostError = func(hostErrorVMs string) error {
		return registry.ErrorWithOwner(
			registry.OwnerTestEng, fmt.Errorf("hostError VMs: %s", hostErrorVMs),
			registry.WithTitleOverride("vm_host_error"),
			registry.InfraFlake,
		)
	}

	prng, _ = randutil.NewLockedPseudoRand()

	runID string
)

// VmLabelTestName is the label used to identify the test name in the VM metadata
const VmLabelTestName string = "test_name"

// VmLabelTestRunID is the label used to identify the test run id in the VM metadata
const VmLabelTestRunID string = "test_run_id"

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
	r.config.skipClusterWipeOnAttach = !roachtestflags.ClusterWipe
	r.config.disableIssue = roachtestflags.DisableIssue
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
	debugMode debugMode

	// preAllocateClusterFn is a function called right before allocating a
	// cluster. It allows the caller to e.g. inject errors for testing.
	preAllocateClusterFn func(
		ctx context.Context,
		t registry.TestSpec,
		arch vm.CPUArch,
	) error
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
	goCoverEnabled         bool
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

	clusterFactory := newClusterFactory(
		clustersOpt.user, clustersOpt.clusterID, lopt.artifactsDir,
		r.cr, numConcurrentClusterCreations(),
	)

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
	runID = generateRunID(clustersOpt)
	shout(ctx, l, lopt.stdout, "%s: %s", VmLabelTestRunID, runID)
	var wg sync.WaitGroup

	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		if err := r.stopper.RunAsyncTask(ctx, "worker", func(ctx context.Context) {
			defer wg.Done()

			err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, r.work, qp,
				r.stopper.ShouldQuiesce(),
				clusterFactory,
				clustersOpt,
				lopt,
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
	if roachtestflags.Cloud == "aws" {
		// AWS has ridiculous API calls limits, so we're going to create one cluster
		// at a time. Internally, roachprod has throttling for the calls required to
		// create a single cluster.
		res = 1
	} else {
		res = 1000
	}
	return res
}

// This will be added as a label to all cluster nodes when the
// cluster is registered. `clusterOpt.clusterID` is conveniently
// set to the TC Build ID when running on TeamCity.
func generateRunID(cOpts clustersOpt) string {
	if cOpts.clusterID == "" {
		return fmt.Sprintf("%s-%d", cOpts.user, timeutil.Now().Unix())
	}
	return fmt.Sprintf("%s-%s", cOpts.user, cOpts.clusterID)
}

func (r *testRunner) allocateCluster(
	ctx context.Context,
	clusterFactory *clusterFactory,
	clustersOpt clustersOpt,
	lopt loggingOpt,
	t registry.TestSpec,
	arch vm.CPUArch,
	wStatus *workerStatus,
) (*clusterImpl, *vm.CreateOpts, error) {
	wStatus.SetStatus(fmt.Sprintf("creating cluster (arch=%q)", arch))
	defer wStatus.SetStatus("")

	if clustersOpt.preAllocateClusterFn != nil {
		if err := clustersOpt.preAllocateClusterFn(ctx, t, arch); err != nil {
			return nil, nil, err
		}
	}

	existingClusterName := clustersOpt.clusterName
	if existingClusterName != "" {
		// Logs for attaching to a cluster go to a dedicated log file.
		logPath := filepath.Join(lopt.artifactsDir, runnerLogsDir, "cluster-create", existingClusterName+".log")
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
		// TODO(srosenberg): we need to think about validation here. Attaching to an incompatible cluster, e.g.,
		// using arm64 AMI with amd64 binary, would result in obscure errors. The test runner ensures compatibility
		// during cluster reuse, whereas attachment via CLI (e.g., via roachprod) does not.
		lopt.l.PrintfCtx(ctx, "Attaching to existing cluster %s for test %s", existingClusterName, t.Name)
		c, err := attachToExistingCluster(ctx, existingClusterName, clusterL, t.Cluster, opt, r.cr)
		if err == nil {
			// Pretend pre-existing's cluster architecture matches the desired one; see the above TODO wrt validation.
			c.arch = arch
			return c, nil, nil
		}
		if !errors.Is(err, errClusterNotFound) {
			return nil, nil, err
		}
		// Fall through to create new cluster with name override.
		lopt.l.PrintfCtx(
			ctx, "Creating new cluster with custom name %q for test %s: %s (arch=%q)",
			clustersOpt.clusterName, t.Name, t.Cluster, arch,
		)
	} else {
		lopt.l.PrintfCtx(ctx, "Creating new cluster for test %s: %s (arch=%q)", t.Name, t.Cluster, arch)
	}

	cfg := clusterConfig{
		nameOverride: clustersOpt.clusterName, // only set if we hit errClusterFound above
		spec:         t.Cluster,
		artifactsDir: lopt.artifactsDir,
		username:     clustersOpt.user,
		localCluster: clustersOpt.typ == localCluster,
		arch:         arch,
	}
	return clusterFactory.newCluster(ctx, cfg, wStatus.SetStatus, lopt.tee)
}

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
// The worker's name will be used as a prefix for log messages.
//
// Each test's logs are going to be under a <test-name>/run_<n> dir inside
// lotp.artifactsDir. If empty, test log files will not be created.
func (r *testRunner) runWorker(
	ctx context.Context,
	name string,
	work *workPool,
	qp *quotapool.IntPool,
	interrupt <-chan struct{},
	clusterFactory *clusterFactory,
	clustersOpt clustersOpt,
	lopt loggingOpt,
	topt testOpts,
	l *logger.Logger,
) error {
	stdout := lopt.stdout

	ctx = logtags.AddTag(ctx, name, nil /* value */)
	wStatus := r.addWorker(ctx, name)
	defer func() {
		r.removeWorker(ctx, name)
	}()

	var c *clusterImpl // The cluster currently being used.
	// When this method returns we'll destroy the cluster we had at the time.
	// Note that, if debug was set, c has been set to nil.
	defer func() {
		// TODO (miral): Consider removing the test_run_id label here, as
		// currently, is only removed when a cluster is unregistered, via c.Destroy()
		// but not when the cluster is preserved via a debug mode.
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

	var alloc *quotapool.IntAlloc
	defer func() {
		// Release any quota, in case we exit from the loop from an error path.
		if alloc != nil {
			if alloc.Acquired() > 0 {
				l.PrintfCtx(ctx, "Releasing quota for %s CPUs", alloc.String())
			}
			qp.Release(alloc)
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

		wStatus.SetTest(nil /* test */, testToRunRes{})

		testToRun := testToRunRes{noWork: true}
		if c != nil {
			// Try to reuse cluster.
			testToRun = work.selectTestForCluster(ctx, c.spec, r.cr)
			if !testToRun.noWork {
				// We found a test to run on this cluster. Wipe the cluster.
				if err := c.WipeForReuse(ctx, l, testToRun.spec.Cluster); err != nil {
					// We do not count reuse attempt error toward clusterCreateErr. If
					// either the Wipe or Extend failed, then destroy the cluster and attempt
					// to create a fresh cluster for the selected test.
					shout(ctx, l, stdout, "Unable to reuse cluster: %s due to: %s. Will attempt to create a fresh one",
						c.Name(), err)
					// We don't release the quota allocation - the new cluster will be
					// identical.
					testToRun.canReuseCluster = false
					// We use a context that can't be canceled for the Destroy().
					c.Destroy(context.Background(), closeLogger, l)
					wStatus.SetCluster(nil)
					c = nil
				}
			}
		}

		// We could not find a test that can reuse the cluster. Destroy the cluster
		// and search for a new test.
		if testToRun.noWork {
			if c != nil {
				wStatus.SetStatus("destroying cluster")
				// We failed to find a test that can take advantage of this cluster. So
				// we're going to release it, which will deallocate its resources.
				l.PrintfCtx(ctx, "No tests that can reuse cluster %s found. Destroying.", c)
				// We use a context that can't be canceled for the Destroy().
				c.Destroy(context.Background(), closeLogger, l)
				wStatus.SetCluster(nil)
				c = nil
			}

			// At this point, any previous cluster was destroyed; release any
			// associated quota allocation.
			if alloc != nil {
				if alloc.Acquired() > 0 {
					l.PrintfCtx(ctx, "Releasing quota for %s CPUs", alloc.String())
				}
				qp.Release(alloc)
				alloc = nil
			}

			var err error
			testToRun, alloc, err = work.selectTest(ctx, qp, l)
			if err != nil {
				return err
			}
			if testToRun.noWork {
				shout(ctx, l, stdout, "No work remaining; runWorker is bailing out...")
				return nil
			}
		}

		// From this point onward, c != nil iff we are reusing the cluster.

		var arch vm.CPUArch
		if c != nil && !c.IsLocal() {
			// We are reusing a non-local cluster. We have already determined that its
			// architecture is acceptable for the test (from the fact that the
			// previous cluster spec had the same arch).
			//
			// Note that we treat local clusters differently because (in the case of
			// Apple M1/M2) it can run multiple architectures.
			// TODO(radu): this is not true of Intel and/or linux hosts, we should
			// somehow determine the capabilities at runtime.
			arch = c.arch
		} else {
			arch = archForTest(ctx, l, testToRun.spec)
			if c != nil {
				// Switch architecture of local cluster (see above).
				c.arch = arch
			}
		}

		//  TODO(babusrithar): remove this once we see enough data in
		//  nightly runs. This is a temp logic to test spot VMs.
		if roachtestflags.Cloud == spec.GCE &&
			testToRun.spec.Benchmark &&
			!testToRun.spec.Suites.Contains(registry.Weekly) &&
			rand.Float64() <= 0.5 {
			l.PrintfCtx(ctx, "using spot VMs to run test %s", testToRun.spec.Name)
			testToRun.spec.Cluster.UseSpotVMs = true
		}

		if roachtestflags.UseSpotVM {
			testToRun.spec.Cluster.UseSpotVMs = true
		}

		// Verify that required native libraries are available.
		//
		// TODO(radu): the arch is not guaranteed and another arch can be selected
		// (in RoachprodOpts). All the code below using arch is incorrect in this
		// case.
		if err := VerifyLibraries(testToRun.spec.NativeLibs, arch); err != nil {
			shout(ctx, l, stdout, "Library verification failed: %s", err)
			return err
		}

		var clusterCreateErr error
		var vmCreateOpts *vm.CreateOpts

		if c == nil {
			// Create a new cluster if can't reuse or reuse attempt failed.
			// N.B. non-reusable cluster would have been destroyed above.
			wStatus.SetTest(nil /* test */, testToRun)
			c, vmCreateOpts, clusterCreateErr = r.allocateCluster(
				ctx, clusterFactory, clustersOpt, lopt,
				testToRun.spec, arch, wStatus)

			if clusterCreateErr != nil {
				atomic.AddInt32(&r.numClusterErrs, 1)
				shout(ctx, l, stdout, "Unable to create (or reuse) cluster for test %s due to: %s.",
					testToRun.spec.Name, clusterCreateErr)
			} else {
				if c.arch != arch {
					// N.B. this can happen if requested machine type is not feasible/available.
					l.PrintfCtx(ctx, "WARN: cluster arch for test differs %s: %s (cluster arch=%q, specified arch=%q)",
						testToRun.spec.Name, c.Name(), c.arch, arch)
					arch = c.arch
				}
				l.PrintfCtx(ctx, "Created new cluster for test %s: %s (arch=%q)", testToRun.spec.Name, c.Name(), arch)
			}
		}
		// Prepare the test's logger. Always set this up with real files, using a
		// temp dir if necessary. This simplifies testing.
		artifactsRootDir := lopt.artifactsDir
		if artifactsRootDir == "" {
			artifactsRootDir, _ = os.MkdirTemp("", "roachtest-logger")
		}
		testName := testToRun.spec.Name
		// N.B. c may be nil owing to clusterCreateErr
		if c != nil && c.arch != vm.ArchAMD64 {
			// N.B. For non-default cpu architecture, encode it in the test name. This helps to differentiate test
			// artifacts/results by cpu architecture.
			testName = fmt.Sprintf("%s/cpu_arch=%s", testName, c.arch)
		}
		escapedTestName := teamCityNameEscape(testName)
		runSuffix := "run_" + strconv.Itoa(testToRun.runNum)

		testArtifactsDir := filepath.Join(filepath.Join(artifactsRootDir, escapedTestName), runSuffix)
		logPath := filepath.Join(testArtifactsDir, "test.log")

		// Map artifacts/TestFoo/run_?/** => TestFoo/run_?/**, i.e. collect the artifacts
		// for this test exactly as they are laid out on disk (when the time
		// comes).
		artifactsSpec := fmt.Sprintf("%s/%s/** => %s/%s", filepath.Join(lopt.literalArtifactsDir, escapedTestName), runSuffix, escapedTestName, runSuffix)

		testL, err := logger.RootLogger(logPath, lopt.tee)
		if err != nil {
			return err
		}
		binaryVersion, err := version.Parse(build.BinaryVersion())
		if err != nil {
			return err
		}
		t := &testImpl{
			spec:                   &testToRun.spec,
			cockroach:              cockroach[arch],
			cockroachEA:            cockroachEA[arch],
			deprecatedWorkload:     workload[arch],
			buildVersion:           binaryVersion,
			artifactsDir:           testArtifactsDir,
			artifactsSpec:          artifactsSpec,
			l:                      testL,
			versionsBinaryOverride: topt.versionsBinaryOverride,
			skipInit:               topt.skipInit,
			debug:                  clustersOpt.debugMode.IsDebug(),
			goCoverEnabled:         topt.goCoverEnabled,
		}
		github := newGithubIssues(r.config.disableIssue, c, vmCreateOpts)

		// handleClusterCreationFailure can be called when the `err` given
		// occurred for reasons related to creating or setting up a
		// cluster for a test.
		handleClusterCreationFailure := func(err error) {
			t.Error(errClusterProvisioningFailed(err))

			if _, err := github.MaybePost(t, l, t.failureMsg()); err != nil {
				shout(ctx, l, stdout, "failed to post issue: %s", err)
			}
		}

		if clusterCreateErr != nil {
			// N.B. cluster creation failed. We mark the test as failed and
			// continue with the next test.
			handleClusterCreationFailure(clusterCreateErr)
		} else {
			// Now run the test.
			l.PrintfCtx(ctx, "Starting test: %s:%d on cluster=%s (arch=%q)", testToRun.spec.Name, testToRun.runNum, c.Name(), arch)

			c.setTest(t)

			var setupErr error
			if c.spec.NodeCount > 0 { // skip during tests
				setupErr = c.PutCockroach(ctx, l, t)
			}
			if setupErr == nil {
				setupErr = c.PutLibraries(ctx, "./lib", t.spec.NativeLibs)
			}

			if setupErr != nil {
				// If there was an error setting up the cluster (uploading
				// initial files), we treat the error just like a cluster
				// creation failure: the error is reported as an
				// infrastructure flake, and we continue with the next test.
				handleClusterCreationFailure(setupErr)
			} else {
				// Tell the cluster that, from now on, it will be run "on behalf of this
				// test".
				c.status("running test")

				testSpec := t.Spec().(*registry.TestSpec)
				switch testSpec.EncryptionSupport {
				case registry.EncryptionAlwaysEnabled:
					c.encAtRest = true
				case registry.EncryptionAlwaysDisabled:
					c.encAtRest = false
				case registry.EncryptionMetamorphic:
					// when tests opted-in to metamorphic testing, encryption will
					// be enabled according to the probability passed to
					// --metamorphic-encryption-probability
					c.encAtRest = prng.Float64() < roachtestflags.EncryptionProbability
				}

				// Set initial cluster settings for this test.
				c.clusterSettings = map[string]string{}
				c.virtualClusterSettings = map[string]string{}

				switch testSpec.Leases {
				case registry.DefaultLeases:
				case registry.EpochLeases:
					c.clusterSettings["kv.expiration_leases_only.enabled"] = "false"
				case registry.ExpirationLeases:
					c.clusterSettings["kv.expiration_leases_only.enabled"] = "true"
				case registry.MetamorphicLeases:
					enabled := prng.Float64() < 0.5
					c.status(fmt.Sprintf("metamorphically setting kv.expiration_leases_only.enabled = %t",
						enabled))
					c.clusterSettings["kv.expiration_leases_only.enabled"] = fmt.Sprintf("%t", enabled)
				default:
					t.Fatalf("unknown lease type %s", testSpec.Leases)
				}

				c.goCoverDir = t.GoCoverArtifactsDir()

				wStatus.SetCluster(c)
				wStatus.SetTest(t, testToRun)
				wStatus.SetStatus("running test")

				r.runTest(ctx, t, testToRun.runNum, testToRun.runCount, c, stdout, testL, github)
			}
		}

		msg := "test passed: %s (run %d)"
		if t.Failed() {
			msg = "test failed: %s (run %d)"
		}
		msg = fmt.Sprintf(msg, t.Name(), testToRun.runNum)
		l.PrintfCtx(ctx, msg)

		testL.Close()
		if t.Failed() {
			failureMsg := fmt.Sprintf("%s (%d) - %s", testToRun.spec.Name, testToRun.runNum, t.failureMsg())
			if c != nil {
				switch clustersOpt.debugMode {
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
		} else {
			// Upon success fetch the perf artifacts from the remote hosts.
			if t.spec.Benchmark {
				getPerfArtifacts(ctx, c, t)
			}
			if clustersOpt.debugMode == DebugKeepAlways {
				alloc.Freeze()
				alloc = nil
				c.Save(ctx, "cluster saved since --debug-always set", l)
				c = nil
			}
		}
	}
}

// getArtifacts retrieves artifacts (like perf or go cover) produced by a
// successful test.
//
// Any errors are logged but otherwise don't cause a test failure.
func getArtifacts(
	ctx context.Context,
	c *clusterImpl,
	t test.Test,
	srcDirOnNode string,
	dstDirFn func(nodeIdx int) string,
) {
	fetchNode := func(ctx context.Context, node int) error {
		testCmd := `'ARTIFACTS_DIR="` + srcDirOnNode + `"
if [[ -d "${ARTIFACTS_DIR}" ]]; then
    echo true
elif [[ -e "${ARTIFACTS_DIR}" ]]; then
    ls -la "${ARTIFACTS_DIR}"
    exit 1
else
    echo false
fi'`
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(node)), "bash", "-c", testCmd)
		if err != nil {
			return errors.Wrapf(err, "failed to check for artifacts in %q", srcDirOnNode)
		}
		out := strings.TrimSpace(result.Stdout)
		switch out {
		case "true":
			return c.Get(ctx, t.L(), srcDirOnNode, dstDirFn(node), c.Node(node))
		case "false":
			t.L().PrintfCtx(ctx, "no artifacts exist in %q on node %v", srcDirOnNode, c.Node(node))
			return nil
		default:
			return errors.Errorf("unexpected output when checking for artifacts in %q: %s", srcDirOnNode, out)
		}
	}
	g := ctxgroup.WithContext(ctx)
	for _, i := range c.All() {
		node := i
		g.GoCtx(func(ctx context.Context) error {
			return fetchNode(ctx, node)
		})
	}
	if err := g.Wait(); err != nil {
		t.L().PrintfCtx(ctx, "failed to get artifacts from %q: %v", srcDirOnNode, err)
	}
}

// getPerfArtifacts retrieves the perf artifacts for the test.
func getPerfArtifacts(ctx context.Context, c *clusterImpl, t test.Test) {
	dstDirFn := func(nodeIdx int) string {
		return fmt.Sprintf("%s/%d.%s", t.ArtifactsDir(), nodeIdx, perfArtifactsDir)
	}
	getArtifacts(ctx, c, t, t.PerfArtifactsDir(), dstDirFn)
}

// getGoCoverArtifacts retrieves the go coverage artifacts for the test.
func getGoCoverArtifacts(ctx context.Context, c *clusterImpl, t test.Test) {
	dstDirFn := func(nodeIdx int) string {
		return fmt.Sprintf("%s/%d.%s", t.ArtifactsDir(), nodeIdx, goCoverArtifactsDir)
	}
	getArtifacts(ctx, c, t, t.GoCoverArtifactsDir(), dstDirFn)
}

// An error is returned if the test is still running (on another goroutine) when
// this returns. This happens when the test doesn't respond to cancellation.
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy  the cluster.
func (r *testRunner) runTest(
	ctx context.Context,
	t *testImpl,
	runNum int,
	runCount int,
	c *clusterImpl,
	stdout io.Writer,
	l *logger.Logger,
	github *githubIssues,
) {
	testRunID := t.Name()
	if runCount > 1 {
		testRunID += fmt.Sprintf("#%d", runNum)
	}

	r.status.Lock()
	r.status.running[t] = struct{}{}
	r.status.Unlock()

	t.runner = callerName()
	t.runnerID = goid.Get()

	s := t.Spec().(*registry.TestSpec)

	grafanaAvailable := roachtestflags.Cloud == spec.GCE
	if err := c.addLabels(map[string]string{VmLabelTestName: testRunID}); err != nil {
		shout(ctx, l, stdout, "failed to add label to cluster [%s] - %s", c.Name(), err)
		grafanaAvailable = false
	}

	if grafanaAvailable {
		// Add the runID, testRunID, and cluster name to grafanaTags. These are the three
		// template variables grafana uses to filter tests by.
		c.grafanaTags = []string{vm.SanitizeLabel(runID), vm.SanitizeLabel(testRunID), vm.SanitizeLabel(c.Name())}
	}

	defer func() {
		t.end = timeutil.Now()
		if err := c.removeLabels([]string{VmLabelTestName}); err != nil {
			shout(ctx, l, stdout, "failed to remove label from cluster [%s] - %s", c.Name(), err)
		}

		if grafanaAvailable {
			// Links to the dashboard overview for this test where a user can then navigate
			// to a preferred dashboard. Add 2 minutes to show complete metrics in grafana.
			l.Printf("metrics: https://go.crdb.dev/roachtest-grafana/%s/%s/%d/%d", vm.SanitizeLabel(runID),
				vm.SanitizeLabel(testRunID), t.start.UnixMilli(), t.end.Add(2*time.Minute).UnixMilli())
		}
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
			if roachtestflags.TeamCity {
				shout(ctx, l, stdout, "##teamcity[testIgnored name='%s' message='%s' duration='%d']\n",
					s.Name, TeamCityEscape(s.Skip), t.duration().Milliseconds())
			}
			shout(ctx, l, stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "N/A", s.Skip)
		} else {
			// Delaying the ##teamcity[testStarted...] service message until the test is finished allows us to branch
			// separately for skipped tests. The duration of the test is passed to ##teamcity[testFinished...] for
			// accurate reporting in the TC UI.
			if roachtestflags.TeamCity {
				shout(ctx, l, stdout, "##teamcity[testStarted name='%s' flowId='%s']", t.Name(), testRunID)
			}

			durationStr := fmt.Sprintf("%.2fs", t.duration().Seconds())
			if t.Failed() {
				failureMsg := t.failureMsg()
				preemptedVMNames := getPreemptedVMNames(ctx, c, l)
				if preemptedVMNames != "" {
					failureMsg = fmt.Sprintf("VMs preempted during the test run: %s\n\n**Other Failures:**\n%s", preemptedVMNames, failureMsg)
					// Reset failures in the test so that the VM preemption
					// error is the one that is taken into account when
					// reporting the failure. Note any other failures that
					// happened during the test will be present in the
					// `failureMsg` used when reporting the issue. In addition,
					// `failure_N.log` files should also already exist at this
					// point.
					t.resetFailures()
					t.Error(vmPreemptionError(preemptedVMNames))
				}
				hostErrorVMNames := getHostErrorVMNames(ctx, c, l)
				if hostErrorVMNames != "" {
					failureMsg = fmt.Sprintf("VMs received host error during the test run: %s\n\n**Other Failures:**\n%s", hostErrorVMNames, failureMsg)
					// Reset failures in the test so that the VM host error
					// is the one that is taken into account when
					// reporting the failure. Note any other failures that
					// happened during the test will be present in the
					// `failureMsg` used when reporting the issue. In addition,
					// `failure_N.log` files should also already exist at this
					// point.
					t.resetFailures()
					t.Error(vmHostError(hostErrorVMNames))
				}

				output := fmt.Sprintf("%s\ntest artifacts and logs in: %s", failureMsg, t.ArtifactsDir())

				issue, err := github.MaybePost(t, l, output)
				if err != nil {
					shout(ctx, l, stdout, "failed to post issue: %s", err)
				}

				// If an issue was created (or comment added) on GitHub,
				// include that information in the output so that it can be
				// easily inspected on the TeamCity overview page.
				if issue != nil {
					output += "\n" + issue.String()
				}
				if roachtestflags.TeamCity {
					// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
					// TeamCity regards the test as successful.
					shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
						s.Name, TeamCityEscape(output), testRunID)
				}

				shout(ctx, l, stdout, "--- FAIL: %s (%s)\n%s", testRunID, durationStr, output)

				if roachtestflags.GitHubActions {
					outputLines := strings.Split(strings.TrimSpace(output), "\n")
					for _, line := range outputLines {
						shout(ctx, l, stdout, "::error title=%s failed::%s", s.Name, line)
					}
				}
			} else {
				shout(ctx, l, stdout, "--- PASS: %s (%s)", testRunID, durationStr)
			}

			if roachtestflags.TeamCity {
				shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s' duration='%d']",
					t.Name(), testRunID, t.duration().Milliseconds())
			}
		}

		if roachtestflags.TeamCity {
			// Zip the artifacts. This improves the TeamCity UX where we can navigate
			// through zip files just fine, but we can't download subtrees of the
			// artifacts storage. By zipping we get this capability as we can just
			// download the zip file for the failing test instead.
			if err := zipArtifacts(t); err != nil {
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

		if roachtestflags.GitHubActions && roachtestflags.Parallelism == 1 {
			shout(ctx, l, stdout, "::endgroup::")
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

	// NB: Nesting won't work properly if we're running multiple tests
	// concurrently. Therefore, we only group log lines if parallelism is 1
	// (which is true for local roachtests that we run in GitHub Actions).
	if roachtestflags.GitHubActions && roachtestflags.Parallelism == 1 {
		shout(ctx, l, stdout, "::group::%s", s.Name)
	}

	t.start = timeutil.Now()

	// Extend the lifetime of the cluster if needed.
	if err := c.MaybeExtendCluster(ctx, l, t.spec); err != nil {
		t.Error(errClusterProvisioningFailed(err))
		return
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
	timeout := testTimeout(t.spec)

	if grafanaAvailable {
		// Shout this to the log and stdout to make it available to anyone watching the test via CI or locally.
		// At this point, we don't have an end time, so default to a 30 minute window from the start time.
		shout(ctx, l, stdout, "=== RUN   %s  [metrics: https://go.crdb.dev/roachtest-grafana/%s/%s/%d/%d]",
			testRunID, vm.SanitizeLabel(runID), vm.SanitizeLabel(testRunID), t.start.UnixMilli(), t.start.Add(30*time.Minute).UnixMilli())
	} else {
		shout(ctx, l, stdout, "=== RUN   %s", testRunID)
	}
	select {
	case <-testReturnedCh:
		s := "successfully"
		if t.Failed() {
			s = "with failure(s)"
		}
		t.L().Printf("test completed %s", s)
	case <-time.After(timeout):
		// NB: We're adding the timeout failure intentionally without cancelling the context
		// to capture as much state as possible during artifact collection.
		t.addFailure(0, "test timed out (%s)", timeout)
		// We suppress other failures from being surfaced to the top as the timeout is always going
		// to be the main error and subsequent errors (i.e. context cancelled) add noise.
		t.suppressFailures()
		timedOut = true
	}

	// Replacing the logger is best effort.
	replaceLogger := func(name string) {
		logger, err := c.l.ChildLogger(name, logger.QuietStderr, logger.QuietStdout)
		if err != nil {
			l.Printf("unable to create logger %s: %s", name, err)
			return
		}
		c.l = logger
		t.ReplaceL(logger)
	}

	if !t.Failed() {
		// Awkward file name to keep it close to test.log.
		l.Printf("running post test assertions (test-post-assertions.log)")
		replaceLogger("test-post-assertions")

		// We still want to run the post-test assertions even if the test timed out as it
		// might provide useful information about the health of the nodes. Any assertion failures
		// will will be recorded against, and eventually fail, the test.
		if err := r.postTestAssertions(ctx, t, c, 10*time.Minute); err != nil {
			l.Printf("error during post test assertions: %v; see test-post-assertions.log for details", err)
		}
	} else {
		l.Printf("skipping post test assertions as test failed")
	}

	l.Printf("running test teardown (test-teardown.log)")
	// From now on, all logging goes to test-teardown.log to give a clear separation between
	// operations originating from the test vs the harness. The only error that can originate here
	// is from artifact collection, which is best effort and for which we do not fail the test.
	replaceLogger("test-teardown")
	if err := r.teardownTest(ctx, t, c, timedOut); err != nil {
		l.Printf("error during test teardown: %v; see test-teardown.log for details", err)
	}
}

// getPreemptedVMNames returns a comma separated list of preempted VM
// names, or an empty string if no VM was preempted or an error was found.
func getPreemptedVMNames(ctx context.Context, c *clusterImpl, l *logger.Logger) string {
	preemptedVMs, err := c.GetPreemptedVMs(ctx, l)
	if err != nil {
		l.Printf("failed to check preempted VMs:\n%+v", err)
		return ""
	}

	var preemptedVMNames []string
	for _, item := range preemptedVMs {
		// Expected format: projects/{project}/zones/{zone}/instances/{name}
		parts := strings.Split(item.Name, "/")

		// If the instance name is in the expected format, only include
		// the VM name and the zone, to make it easier to for a human
		// reading the output.
		if len(parts) == 6 {
			instanceName := parts[5]
			zone := parts[3]
			preemptedVMNames = append(preemptedVMNames, fmt.Sprintf("%s (%s)", instanceName, zone))
		} else {
			preemptedVMNames = append(preemptedVMNames, item.Name)
		}
	}

	return strings.Join(preemptedVMNames, ", ")
}

// getHostErrorVMNames returns a comma separated list of host error VM
// names, or an empty string if no VM had a host error.
func getHostErrorVMNames(ctx context.Context, c *clusterImpl, l *logger.Logger) string {
	hostErrorVMs, err := c.GetPreemptedVMs(ctx, l)
	if err != nil {
		l.Printf("failed to check preempted VMs:\n%+v", err)
		return ""
	}

	var hostErrorVMNames []string
	for _, item := range hostErrorVMs {
		// Expected format: projects/{project}/zones/{zone}/instances/{name}
		parts := strings.Split(item.Name, "/")

		// If the instance name is in the expected format, only include
		// the VM name and the zone, to make it easier to for a human
		// reading the output.
		if len(parts) == 6 {
			instanceName := parts[5]
			zone := parts[3]
			hostErrorVMNames = append(hostErrorVMNames, fmt.Sprintf("%s (%s)", instanceName, zone))
		} else {
			hostErrorVMNames = append(hostErrorVMNames, item.Name)
		}
	}

	return strings.Join(hostErrorVMNames, ", ")
}

// The assertions here are executed after each test, and may result in a test failure. Test authors
// may opt out of these assertions by setting the relevant `SkipPostValidations` flag in the test spec.
// An error caused by a timeout will not result in a failure.
func (r *testRunner) postTestAssertions(
	ctx context.Context, t *testImpl, c *clusterImpl, timeout time.Duration,
) error {
	assertionFailed := false
	postAssertionErr := func(err error) {
		assertionFailed = true
		t.Error(fmt.Errorf(
			"failed during post test assertions (see test-post-assertions.log): %w", err,
		))
	}

	postAssertCh := make(chan struct{})
	_ = r.stopper.RunAsyncTask(ctx, "test-post-assertions", func(ctx context.Context) {
		defer close(postAssertCh)
		// When a dead node is detected, the subsequent post validation queries are likely
		// to hang (reason unclear), and eventually timeout according to the statement_timeout.
		// If this occurs frequently enough, we can look at skipping post validations on a node
		// failure (or even on any test failure).
		if err := c.assertNoDeadNode(ctx, t); err != nil {
			// Some tests expect dead nodes, so they may opt out of this check.
			if t.spec.SkipPostValidations&registry.PostValidationNoDeadNodes == 0 {
				postAssertionErr(err)
			} else {
				t.L().Printf("dead node(s) detected but expected")
			}
		}

		// We collect all the admin health endpoints in parallel,
		// and select the first one that succeeds to run the validation queries
		statuses, err := c.HealthStatus(ctx, t.L(), c.All())
		if err != nil {
			postAssertionErr(errors.WithDetail(err, "Unable to check health status"))
		}

		var db *gosql.DB
		var validationNode int
		for _, s := range statuses {
			if s.Err != nil {
				t.L().Printf("n%d:/health?ready=1 error=%s", s.Node, s.Err)
				continue
			}

			if s.Status != http.StatusOK {
				t.L().Printf("n%d:/health?ready=1 status=%d body=%s", s.Node, s.Status, s.Body)
				continue
			}

			if db == nil {
				db = c.Conn(ctx, t.L(), s.Node)
				validationNode = s.Node
			}
			t.L().Printf("n%d:/health?ready=1 status=200 ok", s.Node)
		}

		// We avoid trying to do this when t.Failed() (and in particular when there
		// are dead nodes) because for reasons @tbg does not understand this gets
		// stuck occasionally, which really ruins the roachtest run. The method
		// below already uses a ctx timeout and SQL statement_timeout, but it does
		// not seem to be enough.
		//
		// TODO(testinfra): figure out why this can still get stuck despite the
		// above.
		if db != nil {
			defer db.Close()
			t.L().Printf("running validation checks on node %d (<10m)", validationNode)
			// If this validation fails due to a timeout, it is very likely that
			// the replica divergence check below will also fail.
			if t.spec.SkipPostValidations&registry.PostValidationInvalidDescriptors == 0 {
				if err := roachtestutil.CheckInvalidDescriptors(ctx, db); err != nil {
					postAssertionErr(errors.WithDetail(err, "invalid descriptors check failed"))
				}
			}
			// Detect replica divergence (i.e. ranges in which replicas have arrived
			// at the same log position with different states).
			if t.spec.SkipPostValidations&registry.PostValidationReplicaDivergence == 0 {
				if err := c.assertConsistentReplicas(ctx, db, t); err != nil {
					postAssertionErr(errors.WithDetail(err, "consistency check failed"))
				}
			}
		} else {
			t.L().Printf("no live node found, skipping validation checks")
		}
	})

	select {
	case <-postAssertCh:
	case <-time.After(timeout):
		return errors.Errorf("post test assertions timed out after %s", timeout)
	}

	if assertionFailed {
		return errors.New("post test assertion(s) failed")
	}
	return nil
}

// teardownTest is best effort and should not fail a test.
// Errors during artifact collection will be propagated up.
func (r *testRunner) teardownTest(
	ctx context.Context, t *testImpl, c *clusterImpl, timedOut bool,
) error {
	if timedOut || t.Failed() {
		err := r.collectArtifacts(ctx, t, c, timedOut, time.Hour)
		if err != nil {
			t.L().Printf("error collecting artifacts: %v", err)
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
		return err
	}

	// Test was successful. If we are collecting code coverage, copy the files now.
	if t.goCoverEnabled {
		t.L().Printf("Stopping all nodes to obtain go cover artifacts")
		if err := c.StopE(ctx, t.L(), option.DefaultStopOpts(), c.All()); err != nil {
			t.L().PrintfCtx(ctx, "error stopping cluster: %v", err)
		}

		t.L().Printf("Retrieving go cover artifacts")
		getGoCoverArtifacts(ctx, c, t)
	}

	return nil
}

func (r *testRunner) collectArtifacts(
	ctx context.Context, t *testImpl, c *clusterImpl, timedOut bool, timeout time.Duration,
) error {
	// Collecting artifacts may hang so we run it in a goroutine which is abandoned
	// after a timeout.
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
				sl := allstacks.Get()
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

		// NB: fetch the logs even when we have a debug zip because
		// debug zip can't ever get the logs for down nodes.
		// We only save artifacts for failed tests in CI, so this
		// duplication is acceptable.
		// NB: fetch the logs *first* in case one of the other steps
		// below has problems.
		t.L().PrintfCtx(ctx, "collecting cluster logs")
		// Do this before collecting logs to make sure the file gets
		// downloaded below.
		if err := saveDiskUsageToLogsDir(ctx, c); err != nil {
			t.L().Printf("failed to fetch disk uage summary: %s", err)
		}
		if err := c.FetchLogs(ctx, t.L()); err != nil {
			t.L().Printf("failed to download logs: %s", err)
		}
		if err := c.FetchDmesg(ctx, t.L()); err != nil {
			t.L().Printf("failed to fetch dmesg: %s", err)
		}
		if err := c.FetchJournalctl(ctx, t.L()); err != nil {
			t.L().Printf("failed to fetch journalctl: %s", err)
		}
		if err := c.FetchCores(ctx, t.L()); err != nil {
			t.L().Printf("failed to fetch cores: %s", err)
		}
		if err := c.CopyRoachprodState(ctx); err != nil {
			t.L().Printf("failed to copy roachprod state: %s", err)
		}
		if err := c.FetchPebbleCheckpoints(ctx, t.L()); err != nil {
			t.L().Printf("failed to fetch Pebble checkpoints: %s", err)
		}
		if err := c.FetchTimeseriesData(ctx, t.L()); err != nil {
			t.L().Printf("failed to fetch timeseries data: %s", err)
		}
		if err := c.FetchDebugZip(ctx, t.L(), "debug.zip"); err != nil {
			t.L().Printf("failed to collect zip: %s", err)
		}
	})

	select {
	case <-artifactsCollectedCh:
	case <-time.After(timeout):
		// Leak the artifacts collection goroutine. Note that the test may not be
		// marked as failing here. We intentionally do not trigger it to fail here,
		// but we could entertain doing so once we have a mechanism that can route
		// such post-test problems to the test-eng team.
		return errors.Errorf("artifact collection timed out after %s", timeout)
	}
	return nil
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
	fmt.Fprintf(stdout, "HTTP server listening on port %d on %s: http://%s:%d/\n", httpPort, bindToDesc, bindTo, httpPort)
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

// zipArtifacts moves everything inside the artifacts dir except any zip files
// (like debug.zip) into an artifacts.zip file.
//
// If Go coverage artifacts are present, they are moved inside a separate
// gocover.zip file.
func zipArtifacts(t *testImpl) error {
	if t.goCoverEnabled {
		// First, look for any go coverage artifacts.
		if goCoverList, err := filterDirEntries(t.ArtifactsDir(), func(entry os.DirEntry) bool {
			return entry.IsDir() && strings.HasSuffix(entry.Name(), "."+goCoverArtifactsDir)
		}); err != nil {
			return err
		} else if len(goCoverList) > 0 {
			// Found artifacts; move them to an archive. Note that this archive will be
			// filtered out below.
			if err := moveToZipArchive("gocover.zip", t.ArtifactsDir(), goCoverList...); err != nil {
				return err
			}
		}
	}

	list, err := filterDirEntries(t.ArtifactsDir(), func(entry os.DirEntry) bool {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".zip") {
			// Skip any zip files.
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return moveToZipArchive("artifacts.zip", t.ArtifactsDir(), list...)
}

// testTimeout returns the timeout of a test. The default is set
// to 3 hours but tests may specify their own timeouts.
func testTimeout(spec *registry.TestSpec) time.Duration {
	timeout := 3 * time.Hour
	if d := spec.Timeout; d != 0 {
		timeout = d
	}
	return timeout
}
