// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

type (
	// backgroundEvent is the struct sent by background steps when they
	// finish (successfully or not).
	backgroundEvent struct {
		Name            string
		Err             error
		TriggeredByTest bool
	}

	backgroundRunner struct {
		group     ctxgroup.Group
		ctx       context.Context
		events    chan backgroundEvent
		logger    *logger.Logger
		stopFuncs []StopFunc
	}

	serviceFailureDetails struct {
		descriptor     *ServiceDescriptor
		binaryVersions []roachpb.Version
		// Cluster versions before and after the failure occurred. Before
		// each step is executed, the test runner will cache each node's
		// view of the cluster version; after a failure occurs, we'll try
		// to read the cluster version from every node again. This context
		// is added to the failure message displayed to the user with the
		// intention of highlighting whether the cluster version changed
		// during the failure, which is useful for test failures that
		// happen while the upgrade is finalizing.
		clusterVersionsBefore []roachpb.Version
		clusterVersionsAfter  []roachpb.Version
	}

	testFailureDetails struct {
		seed          int64
		testContext   *Context
		systemService *serviceFailureDetails
		tenantService *serviceFailureDetails
	}

	// crdbMonitor is a thin wrapper around the roachtest monitor API
	// (cluster.NewMonitor) that produces error events through a channel
	// whenever an unexpected node death happens. It also allows us to
	// provide an API for test authors to inform the framework that a
	// node death is expected if the test performs its own restarts or
	// chaos events.
	crdbMonitor struct {
		once      sync.Once
		crdbNodes option.NodeListOption
		monitor   cluster.Monitor
		errCh     chan error
	}

	serviceRuntime struct {
		descriptor      *ServiceDescriptor
		binaryVersions  *atomic.Value
		clusterVersions *atomic.Value

		connCache struct {
			mu    syncutil.Mutex
			cache map[int]*gosql.DB
		}
	}

	testRunner struct {
		ctx           context.Context
		cancel        context.CancelFunc
		plan          *TestPlan
		tag           string
		cluster       cluster.Cluster
		systemService *serviceRuntime
		tenantService *serviceRuntime
		logger        *logger.Logger

		background *backgroundRunner
		monitor    *crdbMonitor

		// ranUserHooks keeps track of whether the runner has run any
		// user-provided hooks so far.
		ranUserHooks *atomic.Bool
	}
)

var (
	// everything that is not an alphanum or a few special characters
	invalidChars = regexp.MustCompile(`[^a-zA-Z0-9 \-_\.]`)

	// internalQueryTimeout is the maximum amount of time we will wait
	// for an internal query (i.e., performed by the framework) to
	// complete. These queries are typically associated with gathering
	// upgrade state data to be displayed during execution.
	internalQueryTimeout = 30 * time.Second
)

func newServiceRuntime(desc *ServiceDescriptor) *serviceRuntime {
	var binaryVersions atomic.Value
	var clusterVersions atomic.Value

	return &serviceRuntime{
		descriptor:      desc,
		binaryVersions:  &binaryVersions,
		clusterVersions: &clusterVersions,
	}
}

func newTestRunner(
	ctx context.Context,
	cancel context.CancelFunc,
	plan *TestPlan,
	tag string,
	l *logger.Logger,
	c cluster.Cluster,
) *testRunner {
	allCRDBNodes := make(map[int]struct{})
	var systemService *serviceRuntime
	var tenantService *serviceRuntime

	for _, s := range plan.services {
		for _, n := range s.Nodes {
			allCRDBNodes[n] = struct{}{}
		}

		if s.Name == install.SystemInterfaceName {
			systemService = newServiceRuntime(s)
		} else {
			tenantService = newServiceRuntime(s)
		}
	}

	var ranUserHooks atomic.Bool
	return &testRunner{
		ctx:           ctx,
		cancel:        cancel,
		plan:          plan,
		tag:           tag,
		logger:        l,
		systemService: systemService,
		tenantService: tenantService,
		cluster:       c,
		background:    newBackgroundRunner(ctx, l),
		monitor:       newCRDBMonitor(ctx, c, maps.Keys(allCRDBNodes)),
		ranUserHooks:  &ranUserHooks,
	}
}

// run implements the test running logic, which boils down to running
// each step in sequence.
func (tr *testRunner) run() (retErr error) {
	stepsErr := make(chan error)
	defer func() { tr.teardown(stepsErr, retErr != nil) }()
	defer func() {
		if retErr != nil {
			// If the test failed, and we haven't run any user hooks up to this point,
			// redirect the failure to Test Eng, as this indicates a setup problem
			// that should be investigated separately.
			if !tr.ranUserHooks.Load() {
				retErr = registry.ErrorWithOwner(registry.OwnerTestEng, retErr)
			}

			// If this test run had a tag assigned, wrap the error with that
			// tag to make it more immediately clear which run failed.
			if tr.tag != "" {
				retErr = errors.Wrapf(retErr, "%s", tr.tag)
			}
		}
	}()

	go func() {
		defer close(stepsErr)
		for _, step := range tr.plan.Steps() {
			if err := tr.runStep(tr.ctx, step); err != nil {
				stepsErr <- err
				return
			}
		}
	}()

	for {
		select {
		case err := <-stepsErr:
			return err
		case event := <-tr.background.CompletedEvents():
			if event.Err == nil {
				tr.logger.Printf("background step finished: %s", event.Name)
				continue
			} else if event.TriggeredByTest {
				tr.logger.Printf("background step canceled by test: %s", event.Name)
				continue
			}

			return fmt.Errorf("background step `%s` returned error: %w", event.Name, event.Err)

		case err := <-tr.monitor.Err():
			return tr.testFailure(tr.ctx, err, tr.logger, nil)
		}
	}
}

// runStep contains the logic of running a single test step, called
// recursively in the case of sequentialRunStep and concurrentRunStep.
func (tr *testRunner) runStep(ctx context.Context, step testStep) error {
	if ss, ok := step.(*singleStep); ok {
		if ss.ID > tr.plan.startSystemID {
			if err := tr.refreshServiceData(ctx, tr.systemService); err != nil {
				return errors.Wrapf(err, "preparing to run step %d", ss.ID)
			}
		}

		if ss.ID > tr.plan.startTenantID && tr.tenantService != nil {
			if err := tr.refreshServiceData(ctx, tr.tenantService); err != nil {
				return errors.Wrapf(err, "preparing to run step %d", ss.ID)
			}
		}
	}

	switch s := step.(type) {
	case sequentialRunStep:
		for _, ss := range s.steps {
			if err := tr.runStep(ctx, ss); err != nil {
				return err
			}
		}
		return nil

	case concurrentRunStep:
		group := ctxgroup.WithContext(tr.ctx)
		for _, cs := range s.delayedSteps {
			cs := cs
			group.GoCtx(func(concurrentCtx context.Context) error {
				return tr.runStep(concurrentCtx, cs)
			})
		}
		return group.Wait()

	case delayedStep:
		time.Sleep(s.delay)
		return tr.runStep(ctx, s.step)

	default:
		ss := s.(*singleStep)
		stepLogger, err := tr.loggerFor(ss)
		if err != nil {
			return err
		}

		if stopChan := ss.impl.Background(); stopChan != nil {
			tr.startBackgroundStep(ss, stepLogger, stopChan)
			return nil
		}

		if _, isUserHook := ss.impl.(runHookStep); isUserHook {
			tr.ranUserHooks.Store(true)
		}

		return tr.runSingleStep(ctx, ss, stepLogger)
	}
}

// runSingleStep takes care of the logic of running a `singleStep`,
// including logging start and finish times, wrapping the error (if
// any) with useful information, and renaming the log file to indicate
// failure. This logic is the same whether running a step in the
// background or not.
func (tr *testRunner) runSingleStep(ctx context.Context, ss *singleStep, l *logger.Logger) error {
	tr.logStep("STARTING", ss, l)
	tr.logVersions(l, ss.context)
	start := timeutil.Now()
	defer func() {
		prefix := fmt.Sprintf("FINISHED [%s]", timeutil.Since(start))
		tr.logStep(prefix, ss, l)
	}()

	if err := panicAsError(l, func() error {
		return ss.impl.Run(ctx, l, ss.rng, tr.newHelper(ctx, l, ss.context))
	}); err != nil {
		if isContextCanceled(ctx) {
			l.Printf("step terminated (context canceled)")
			// Avoid creating a `stepError` (which involves querying binary
			// and cluster versions) when the context was canceled as the
			// main error (that caused the context to be canceled) will
			// already include relevant information. This context
			// cancelation could also be happening because the test author
			// is explicitly stopping a background step, so running those
			// queries would be wasteful.
			return err
		}
		return tr.stepError(ctx, err, ss, l)
	}

	return nil
}

func (tr *testRunner) startBackgroundStep(ss *singleStep, l *logger.Logger, stopChan shouldStop) {
	stop := tr.background.Start(ss.impl.Description(), func(ctx context.Context) error {
		return tr.runSingleStep(ctx, ss, l)
	})

	// We start a goroutine to listen for user-requests to stop the
	// background function.
	go func() {
		select {
		case <-stopChan:
			// Test has requested the background function to stop.
			stop()
		case <-tr.ctx.Done():
			// Parent context is done (test has finished).
			return
		}
	}()
}

// stepError generates a `testFailure` error by augmenting the error
// passed with extra information. Specifically, the error message will
// include the ID of the step that failed, the random seed used, the
// binary version on each node when the error occurred, and the
// cluster version before and after the step (in case the failure
// happened *while* the cluster version was updating).
func (tr *testRunner) stepError(
	ctx context.Context, err error, step *singleStep, l *logger.Logger,
) error {
	stepErr := errors.Wrapf(
		err,
		"mixed-version test failure while running step %d (%s)",
		step.ID, step.impl.Description(),
	)

	return tr.testFailure(ctx, stepErr, l, &step.context)
}

// testFailure generates a `testFailure` for failures that happened
// due to the given error. It logs the error to the logger passed,
// and renames the underlying file to include the "FAILED" prefix to
// help in debugging.
func (tr *testRunner) testFailure(
	ctx context.Context, err error, l *logger.Logger, testContext *Context,
) error {
	detailsForService := func(service *serviceRuntime) *serviceFailureDetails {
		return &serviceFailureDetails{
			descriptor:            service.descriptor,
			binaryVersions:        loadAtomicVersions(service.binaryVersions),
			clusterVersionsBefore: loadAtomicVersions(service.clusterVersions),
			clusterVersionsAfter:  loadAtomicVersions(service.clusterVersions),
		}
	}

	var systemDetails *serviceFailureDetails
	var tenantDetails *serviceFailureDetails
	for _, service := range tr.allServices() {
		if service.descriptor.Name == install.SystemInterfaceName {
			systemDetails = detailsForService(service)
		} else {
			tenantDetails = detailsForService(service)
		}
	}

	currentClusterVersions := func(service *serviceRuntime) []roachpb.Version {
		if tr.connCacheInitialized(service) {
			if err := tr.refreshClusterVersions(ctx, service); err == nil {
				return loadAtomicVersions(service.clusterVersions)
			} else {
				tr.logger.Printf(
					"failed to fetch cluster versions for service %s after failure: %s",
					service.descriptor.Name, err,
				)
			}
		}

		return loadAtomicVersions(service.clusterVersions)
	}

	systemDetails.clusterVersionsAfter = currentClusterVersions(tr.systemService)
	if tenantDetails != nil {
		tenantDetails.clusterVersionsAfter = currentClusterVersions(tr.tenantService)
	}

	tf := &testFailureDetails{
		seed:          tr.plan.seed,
		testContext:   testContext,
		systemService: systemDetails,
		tenantService: tenantDetails,
	}

	// failureErr wraps the original error, adding mixed-version state
	// information as error details.
	failureErr := errors.WithDetailf(err, "%s", tf.Format())

	// Print the test failure on the step's logger for convenience, and
	// to reduce cross referencing of logs.
	l.Printf("%+v", failureErr)

	if err := renameFailedLogger(l); err != nil {
		tr.logger.Printf("could not rename failed step logger: %v", err)
	}

	return failureErr
}

// teardown groups together all tasks that happen once a test finishes.
func (tr *testRunner) teardown(stepsChan chan error, testFailed bool) {
	if testFailed {
		tr.logger.Printf("mixed-version test FAILED")
	} else {
		tr.logger.Printf("mixed-version test PASSED")
	}

	tr.cancel()

	// Stop background functions explicitly so that the corresponding
	// termination is marked `TriggeredByTest` (not necessary for
	// correctness, just for clarity).
	tr.logger.Printf("stopping background functions")
	tr.background.Terminate()

	tr.logger.Printf("stopping node monitor")
	if err := tr.monitor.Stop(); err != nil {
		tr.logger.Printf("monitor returned error: %v", err)
	}

	// If the test failed, we wait for any currently running steps to
	// return before passing control back to the roachtest
	// framework. This achieves a test.log that does not contain any
	// test step output once roachtest started to collect failure
	// artifacts, which would be confusing.
	if testFailed {
		tr.logger.Printf("waiting for all steps to finish after context cancelation")
		waitForChannel(stepsChan, "test steps", tr.logger)
	}

	tr.logger.Printf("closing database connections")
	tr.closeConnections()
}

func (tr *testRunner) logStep(prefix string, step *singleStep, l *logger.Logger) {
	dashes := strings.Repeat("-", 10)
	l.Printf("%[1]s %s (%d): %s %[1]s", dashes, prefix, step.ID, step.impl.Description())
}

// logVersions writes the current cached versions of the binary and
// cluster versions on each node. The cached versions should exist for
// all steps but the first one (when we start the cluster itself).
func (tr *testRunner) logVersions(l *logger.Logger, testContext Context) {
	binaryVersions := loadAtomicVersions(tr.systemService.binaryVersions)
	systemClusterVersions := loadAtomicVersions(tr.systemService.clusterVersions)
	var tenantClusterVersions []roachpb.Version
	if tr.tenantService != nil {
		tenantClusterVersions = loadAtomicVersions(tr.tenantService.clusterVersions)
	}

	releasedVersions := make([]*clusterupgrade.Version, 0, len(testContext.System.Descriptor.Nodes))
	for _, node := range testContext.System.Descriptor.Nodes {
		nv, err := testContext.NodeVersion(node)
		handleInternalError(err)
		releasedVersions = append(releasedVersions, nv)
	}

	if binaryVersions == nil || systemClusterVersions == nil {
		return
	}

	tw := newTableWriter(testContext.System.Descriptor.Nodes)
	tw.AddRow("released versions", toString(releasedVersions)...)
	tw.AddRow("logical binary versions", toString(binaryVersions)...)

	tw.AddRow("cluster versions (system)", toString(systemClusterVersions)...)
	if len(tenantClusterVersions) > 0 {
		tw.AddRow(
			fmt.Sprintf("cluster versions (%s)", tr.tenantService.descriptor.Name),
			toString(tenantClusterVersions)...,
		)
	}

	l.Printf("current cluster configuration:\n%s", tw.String())
}

// loggerFor creates a logger instance to be used by a test step. Logs
// will be available under `mixed-version-test/{ID}.log`, making it
// easy to go from the IDs displayed in the test plan to the
// corresponding output of that step.
func (tr *testRunner) loggerFor(step *singleStep) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(step.impl.Description()), "")
	name = fmt.Sprintf("%d_%s", step.ID, name)
	prefix := filepath.Join(tr.tag, logPrefix, name)

	return prefixedLoggerWithFilename(tr.logger, prefix, filepath.Join(logPrefix, name))
}

// refreshBinaryVersions updates the `binaryVersions` field for every
// service with the binary version running on each node of the
// cluster. We use the `atomic` package here as this function may be
// called by two steps that are running concurrently.
func (tr *testRunner) refreshBinaryVersions(ctx context.Context, service *serviceRuntime) error {
	newBinaryVersions := make([]roachpb.Version, len(tr.systemService.descriptor.Nodes))
	connectionCtx, cancel := context.WithTimeout(ctx, internalQueryTimeout)
	defer cancel()

	group := ctxgroup.WithContext(connectionCtx)
	for j, node := range service.descriptor.Nodes {
		j, node := j, node
		group.GoCtx(func(ctx context.Context) error {
			bv, err := clusterupgrade.BinaryVersion(ctx, tr.conn(node, service.descriptor.Name))
			if err != nil {
				return fmt.Errorf("failed to get binary version for node %d: %w", node, err)
			}

			newBinaryVersions[j] = bv
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	service.binaryVersions.Store(newBinaryVersions)
	return nil
}

// refreshClusterVersions updates the internal `clusterVersions` field
// with the current view of the cluster version in each of the nodes
// of the cluster.
func (tr *testRunner) refreshClusterVersions(ctx context.Context, service *serviceRuntime) error {
	newClusterVersions := make([]roachpb.Version, len(service.descriptor.Nodes))
	connectionCtx, cancel := context.WithTimeout(ctx, internalQueryTimeout)
	defer cancel()

	group := ctxgroup.WithContext(connectionCtx)
	for j, node := range service.descriptor.Nodes {
		j, node := j, node
		group.GoCtx(func(ctx context.Context) error {
			cv, err := clusterupgrade.ClusterVersion(ctx, tr.conn(node, service.descriptor.Name))
			if err != nil {
				return fmt.Errorf("failed to get cluster version for node %d: %w", node, err)
			}

			newClusterVersions[j] = cv
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	service.clusterVersions.Store(newClusterVersions)
	return nil
}

func (tr *testRunner) refreshServiceData(ctx context.Context, service *serviceRuntime) error {
	// Update the runner's view of the cluster's binary and cluster
	// versions for given service before every non-initialization
	// `singleStep` is executed.
	if err := tr.maybeInitConnections(service); err != nil {
		return err
	}

	if service == tr.systemService {
		if err := tr.refreshBinaryVersions(ctx, service); err != nil {
			return err
		}
	}

	if err := tr.refreshClusterVersions(ctx, service); err != nil {
		return err
	}

	tr.monitor.Init()
	return nil
}

// maybeInitConnections initializes connections if the connection
// cache is empty. When the function returns, either the `connCache`
// field is populated with a connection for every crdb node, or the
// field is left untouched, and an error is returned.
func (tr *testRunner) maybeInitConnections(service *serviceRuntime) error {
	service.connCache.mu.Lock()
	defer service.connCache.mu.Unlock()

	if service.connCache.cache != nil {
		return nil
	}

	cc := map[int]*gosql.DB{}
	for _, node := range service.descriptor.Nodes {
		conn, err := tr.cluster.ConnE(
			tr.ctx, tr.logger, node, option.VirtualClusterName(service.descriptor.Name),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to node %d: %w", node, err)
		}

		cc[node] = conn
	}

	service.connCache.cache = cc
	return nil
}

func (tr *testRunner) connCacheInitialized(service *serviceRuntime) bool {
	service.connCache.mu.Lock()
	defer service.connCache.mu.Unlock()

	return service.connCache.cache != nil
}

func (tr *testRunner) newHelper(
	ctx context.Context, l *logger.Logger, testContext Context,
) *Helper {
	newService := func(sc *ServiceContext, cv *atomic.Value) *Service {
		if sc == nil {
			return nil
		}

		connFunc := func(node int) *gosql.DB {
			return tr.conn(node, sc.Descriptor.Name)
		}

		return &Service{
			ServiceContext: sc,

			ctx:             ctx,
			connFunc:        connFunc,
			stepLogger:      l,
			clusterVersions: cv,
		}
	}

	var tenantCV *atomic.Value
	if tr.tenantService != nil {
		tenantCV = tr.tenantService.clusterVersions
	}

	return &Helper{
		System: newService(testContext.System, tr.systemService.clusterVersions),
		Tenant: newService(testContext.Tenant, tenantCV),

		testContext: testContext,
		ctx:         ctx,
		runner:      tr,
		stepLogger:  l,
	}
}

// conn returns a database connection to the given node. Assumes the
// connection cache has been previously initialized.
func (tr *testRunner) conn(node int, virtualClusterName string) *gosql.DB {
	var service *serviceRuntime
	if virtualClusterName == install.SystemInterfaceName {
		service = tr.systemService
	} else if tr.tenantService != nil && virtualClusterName == tr.tenantService.descriptor.Name {
		service = tr.tenantService
	} else {
		panic(fmt.Errorf("internal error: unknown virtual cluster %q", virtualClusterName))
	}

	service.connCache.mu.Lock()
	defer service.connCache.mu.Unlock()
	return service.connCache.cache[node]
}

func (tr *testRunner) closeConnections() {
	for _, service := range tr.allServices() {
		service.connCache.mu.Lock()
		defer service.connCache.mu.Unlock()

		for _, db := range service.connCache.cache {
			if db != nil {
				_ = db.Close()
			}
		}
	}
}

func (tr *testRunner) allServices() []*serviceRuntime {
	services := []*serviceRuntime{tr.systemService}
	if tr.tenantService != nil {
		services = append(services, tr.tenantService)
	}

	return services
}

func newCRDBMonitor(
	ctx context.Context, c cluster.Cluster, crdbNodes option.NodeListOption,
) *crdbMonitor {
	return &crdbMonitor{
		crdbNodes: crdbNodes,
		monitor:   c.NewMonitor(ctx, crdbNodes),
		errCh:     make(chan error),
	}
}

// Init must be called once the cluster is initialized and the
// cockroach process is running on the nodes. Init is idempotent.
func (cm *crdbMonitor) Init() {
	cm.once.Do(func() {
		go func() {
			if err := cm.monitor.WaitForNodeDeath(); err != nil {
				cm.errCh <- err
			}
		}()
	})
}

// Err returns a channel that will receive errors whenever an
// unexpected node death is observed.
func (cm *crdbMonitor) Err() chan error {
	return cm.errCh
}

func (cm *crdbMonitor) ExpectDeaths(n int) {
	cm.monitor.ExpectDeaths(int32(n))
}

func (cm *crdbMonitor) Stop() error {
	if cm.monitor == nil { // test-only
		return nil
	}

	return cm.monitor.WaitE()
}

func newBackgroundRunner(ctx context.Context, l *logger.Logger) *backgroundRunner {
	g := ctxgroup.WithContext(ctx)
	return &backgroundRunner{
		group:  g,
		ctx:    ctx,
		logger: l,
		events: make(chan backgroundEvent),
	}
}

// Start will run the function `fn` in a goroutine. Any errors
// returned by that function are observable by reading from the
// channel returned by the `Events()` function. Returns a function
// that can be called to stop the background function (canceling the
// context passed to it).
func (br *backgroundRunner) Start(name string, fn func(context.Context) error) context.CancelFunc {
	bgCtx, cancel := context.WithCancel(br.ctx)
	var expectedContextCancelation bool
	br.group.Go(func() error {
		err := fn(bgCtx)
		event := backgroundEvent{
			Name:            name,
			Err:             err,
			TriggeredByTest: err != nil && isContextCanceled(bgCtx) && expectedContextCancelation,
		}

		select {
		case br.events <- event:
			// exit goroutine
		case <-br.ctx.Done():
			// Test already finished, exit goroutine.
			return nil
		}

		return err
	})

	stopBgFunc := func() {
		expectedContextCancelation = true
		cancel()
	}
	// Collect all stopFuncs so that we can explicitly stop all
	// background functions when the test finishes.
	br.stopFuncs = append(br.stopFuncs, stopBgFunc)
	return stopBgFunc
}

// Terminate will call the stop functions for every background function
// started during the test. This includes background functions created
// during test runtime (using `helper.Background()`), as well as
// background steps declared in the test setup (using
// `BackgroundFunc`, `Workload`, et al). Returns when all background
// functions have returned.
func (br *backgroundRunner) Terminate() {
	for _, stop := range br.stopFuncs {
		stop()
	}

	doneCh := make(chan error)
	go func() {
		defer close(doneCh)
		_ = br.group.Wait()
	}()

	waitForChannel(doneCh, "background functions", br.logger)
}

func (br *backgroundRunner) CompletedEvents() <-chan backgroundEvent {
	return br.events
}

func (tfd *testFailureDetails) Format() string {
	lines := []string{
		"test failed:",
		fmt.Sprintf("test random seed: %d\n", tfd.seed),
	}

	tw := newTableWriter(tfd.systemService.descriptor.Nodes)
	if tfd.testContext != nil {
		releasedVersions := make([]*clusterupgrade.Version, 0, len(tfd.testContext.System.Descriptor.Nodes))
		for _, node := range tfd.testContext.System.Descriptor.Nodes {
			nv, err := tfd.testContext.NodeVersion(node)
			handleInternalError(err)
			releasedVersions = append(releasedVersions, nv)
		}
		tw.AddRow("released versions", toString(releasedVersions)...)
	}

	tw.AddRow("logical binary versions", toString(tfd.systemService.binaryVersions)...)
	for _, service := range []*serviceFailureDetails{tfd.systemService, tfd.tenantService} {
		if service == nil {
			continue
		}

		tw.AddRow(
			fmt.Sprintf("cluster versions before failure (%s)", service.descriptor.Name),
			toString(service.clusterVersionsBefore)...,
		)

		if cv := service.clusterVersionsAfter; cv != nil {
			tw.AddRow(
				fmt.Sprintf("cluster versions after failure (%s)", service.descriptor.Name),
				toString(cv)...,
			)
		}
	}

	lines = append(lines, tw.String())
	return strings.Join(lines, "\n")
}

// tableWriter is a thin wrapper around the `tabwriter` package used
// by the test runner to display logical and released binary versions
// in a tabular format.
type tableWriter struct {
	buffer *bytes.Buffer
	w      *tabwriter.Writer
}

// newTableWriter creates a tableWriter to display tabular data for
// the nodes passed as parameter.
func newTableWriter(nodes option.NodeListOption) *tableWriter {
	var buffer bytes.Buffer
	const (
		minWidth = 3
		tabWidth = 4
		padding  = 5
		padchar  = ' '
		flags    = 0
	)

	tw := tabwriter.NewWriter(&buffer, minWidth, tabWidth, padding, padchar, flags)
	writer := &tableWriter{buffer: &buffer, w: tw}

	var nodeValues []string
	for _, n := range nodes {
		nodeValues = append(nodeValues, fmt.Sprintf("n%d", n))
	}

	writer.AddRow("", nodeValues...)
	return writer
}

// AddRow adds a row to the table with the given title and values.
func (tw *tableWriter) AddRow(title string, values ...string) {
	cells := append([]string{title}, values...)
	fmt.Fprintf(tw.w, "%s", strings.Join(cells, "\t"))
	fmt.Fprintf(tw.w, "\n")
}

func (tw *tableWriter) String() string {
	_ = tw.w.Flush()
	return tw.buffer.String()
}

func renameFailedLogger(l *logger.Logger) error {
	if l.File == nil { // test-only
		return nil
	}

	currentFileName := l.File.Name()
	newLogName := filepath.Join(
		filepath.Dir(currentFileName),
		"FAILED_"+filepath.Base(currentFileName),
	)
	return os.Rename(currentFileName, newLogName)
}

func loadAtomicVersions(v *atomic.Value) []roachpb.Version {
	if v == nil || v.Load() == nil {
		return nil
	}

	return v.Load().([]roachpb.Version)
}

// panicAsError ensures that any panics that might happen while the function
// passed runs are captured and returned as regular errors. A stack trace is
// included in the logs when that happens to facilitate debugging.
func panicAsError(l *logger.Logger, f func() error) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			l.Printf("panic stack trace:\n%s", string(debug.Stack()))
			retErr = fmt.Errorf("panic (stack trace above): %v", r)
		}
	}()
	return f()
}

// waitForChannel waits for the given channel `ch` to close; returns
// when that happens. If the channel does not close within 5 minutes,
// the function logs a message and returns.
//
// The main use-case for this function is waiting for user-provided
// hooks to return after the context passed to them is canceled. We
// want to allow some time for them to finish, but we also don't want
// to block indefinitely if a function inadvertently ignores context
// cancelation.
func waitForChannel(ch chan error, desc string, l *logger.Logger) {
	maxWait := 5 * time.Minute
	select {
	case <-ch:
		// return
	case <-time.After(maxWait):
		l.Printf("waited for %s for %s to finish, giving up", maxWait, desc)
	}
}

func toString[T fmt.Stringer](xs []T) []string {
	var result []string
	for _, x := range xs {
		result = append(result, x.String())
	}

	return result
}

// isContextCanceled returns a boolean indicating whether the context
// passed is canceled.
func isContextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
