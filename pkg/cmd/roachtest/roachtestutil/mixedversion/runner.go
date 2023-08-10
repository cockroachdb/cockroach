// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type (
	// Helper is the struct passed to user-functions providing helper
	// functions that mixed-version tests can use.
	Helper struct {
		ctx         context.Context
		testContext *Context
		// bgCount keeps track of the number of background tasks started
		// with `helper.Background()`. The counter is used to generate
		// unique log file names.
		bgCount    int64
		runner     *testRunner
		stepLogger *logger.Logger
	}

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

	testFailure struct {
		summarized     bool
		description    string
		seed           int64
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

	testRunner struct {
		ctx       context.Context
		cancel    context.CancelFunc
		plan      *TestPlan
		cluster   cluster.Cluster
		crdbNodes option.NodeListOption
		seed      int64
		logger    *logger.Logger

		binaryVersions  atomic.Value
		clusterVersions atomic.Value

		background *backgroundRunner
		monitor    *crdbMonitor

		connCache struct {
			mu    syncutil.Mutex
			cache []*gosql.DB
		}
	}
)

var (
	// everything that is not an alphanum or a few special characters
	invalidChars = regexp.MustCompile(`[^a-zA-Z0-9 \-_\.]`)
)

func newTestRunner(
	ctx context.Context,
	cancel context.CancelFunc,
	plan *TestPlan,
	l *logger.Logger,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
	randomSeed int64,
) *testRunner {
	return &testRunner{
		ctx:        ctx,
		cancel:     cancel,
		plan:       plan,
		logger:     l,
		cluster:    c,
		crdbNodes:  crdbNodes,
		background: newBackgroundRunner(ctx, l),
		monitor:    newCRDBMonitor(ctx, c, crdbNodes),
		seed:       randomSeed,
	}
}

// run implements the test running logic, which boils down to running
// each step in sequence.
func (tr *testRunner) run() (retErr error) {
	stepsErr := make(chan error)
	defer func() { tr.teardown(stepsErr, retErr != nil) }()

	go func() {
		defer close(stepsErr)
		for _, step := range tr.plan.steps {
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
			return tr.testFailure(err.Error(), tr.logger)
		}
	}
}

// runStep contains the logic of running a single test step, called
// recursively in the case of sequentialRunStep and concurrentRunStep.
func (tr *testRunner) runStep(ctx context.Context, step testStep) error {
	if ss, ok := step.(singleStep); ok {
		if ss.ID() > tr.plan.startClusterID {
			// update the runner's view of the cluster's binary and cluster
			// versions before every non-initialization `singleStep` is
			// executed
			if err := tr.maybeInitConnections(); err != nil {
				return err
			}
			if err := tr.refreshBinaryVersions(); err != nil {
				return err
			}
			if err := tr.refreshClusterVersions(); err != nil {
				return err
			}
			tr.monitor.Init()
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
		ss := s.(singleStep)
		stepLogger, err := tr.loggerFor(ss)
		if err != nil {
			return err
		}

		if stopChan := ss.Background(); stopChan != nil {
			tr.startBackgroundStep(ss, stepLogger, stopChan)
			return nil
		}

		return tr.runSingleStep(ctx, ss, stepLogger)
	}
}

// runSingleStep takes care of the logic of running a `singleStep`,
// including logging start and finish times, wrapping the error (if
// any) with useful information, and renaming the log file to indicate
// failure. This logic is the same whether running a step in the
// background or not.
func (tr *testRunner) runSingleStep(ctx context.Context, ss singleStep, l *logger.Logger) error {
	tr.logStep("STARTING", ss, l)
	tr.logVersions(l)
	start := timeutil.Now()
	defer func() {
		prefix := fmt.Sprintf("FINISHED [%s]", timeutil.Since(start))
		tr.logStep(prefix, ss, l)
	}()

	if err := panicAsError(l, func() error {
		return ss.Run(ctx, l, tr.cluster, tr.newHelper(ctx, l))
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
		return tr.stepError(err, ss, l)
	}

	return nil
}

func (tr *testRunner) startBackgroundStep(ss singleStep, l *logger.Logger, stopChan shouldStop) {
	stop := tr.background.Start(ss.Description(), func(ctx context.Context) error {
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
func (tr *testRunner) stepError(err error, step singleStep, l *logger.Logger) error {
	desc := fmt.Sprintf("mixed-version test failure while running step %d (%s): %s",
		step.ID(), step.Description(), err,
	)

	return tr.testFailure(desc, l)
}

// testFailure generates a `testFailure` with the given
// description. It logs the error to the logger passed, and renames
// the underlying file to include the "FAILED" prefix to help in
// debugging.
func (tr *testRunner) testFailure(desc string, l *logger.Logger) error {
	clusterVersionsBefore := tr.clusterVersions
	var clusterVersionsAfter atomic.Value
	if tr.connCacheInitialized() {
		if err := tr.refreshClusterVersions(); err != nil {
			tr.logger.Printf("failed to fetch cluster versions after failure: %s", err)
		} else {
			clusterVersionsAfter = tr.clusterVersions
		}
	}

	tf := &testFailure{
		description:           desc,
		seed:                  tr.seed,
		binaryVersions:        loadAtomicVersions(tr.binaryVersions),
		clusterVersionsBefore: loadAtomicVersions(clusterVersionsBefore),
		clusterVersionsAfter:  loadAtomicVersions(clusterVersionsAfter),
	}

	// Print the test failure on the step's logger for convenience, and
	// to reduce cross referencing of logs.
	l.Printf("%v", tf)

	if err := renameFailedLogger(l); err != nil {
		tr.logger.Printf("could not rename failed step logger: %v", err)
	}

	return tf
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

func (tr *testRunner) logStep(prefix string, step singleStep, l *logger.Logger) {
	dashes := strings.Repeat("-", 10)
	l.Printf("%[1]s %s (%d): %s %[1]s", dashes, prefix, step.ID(), step.Description())
}

// logVersions writes the current cached versions of the binary and
// cluster versions on each node. The cached versions should exist for
// all steps but the first one (when we start the cluster itself).
func (tr *testRunner) logVersions(l *logger.Logger) {
	binaryVersions := loadAtomicVersions(tr.binaryVersions)
	clusterVersions := loadAtomicVersions(tr.clusterVersions)

	if binaryVersions == nil || clusterVersions == nil {
		return
	}

	l.Printf("binary versions: %s", formatVersions(binaryVersions))
	l.Printf("cluster versions: %s", formatVersions(clusterVersions))
}

// loggerFor creates a logger instance to be used by a test step. Logs
// will be available under `mixed-version-test/{ID}.log`, making it
// easy to go from the IDs displayed in the test plan to the
// corresponding output of that step.
func (tr *testRunner) loggerFor(step singleStep) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(step.Description()), "")
	name = fmt.Sprintf("%d_%s", step.ID(), name)

	prefix := path.Join(logPrefix, name)
	return prefixedLogger(tr.logger, prefix)
}

// refreshBinaryVersions updates the internal `binaryVersions` field
// with the binary version running on each node of the cluster. We use
// the `atomic` package here as this function may be called by two
// steps that are running concurrently.
func (tr *testRunner) refreshBinaryVersions() error {
	newBinaryVersions := make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		bv, err := clusterupgrade.BinaryVersion(tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get binary version for node %d: %w", node, err)
		}
		newBinaryVersions = append(newBinaryVersions, bv)
	}

	tr.binaryVersions.Store(newBinaryVersions)
	return nil
}

// refreshClusterVersions updates the internal `clusterVersions` field
// with the current view of the cluster version in each of the nodes
// of the cluster.
func (tr *testRunner) refreshClusterVersions() error {
	newClusterVersions := make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		cv, err := clusterupgrade.ClusterVersion(tr.ctx, tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get cluster version for node %d: %w", node, err)
		}
		newClusterVersions = append(newClusterVersions, cv)
	}

	tr.clusterVersions.Store(newClusterVersions)
	return nil
}

// maybeInitConnections initialize connections if the connection cache
// is empty. When the function returns, either the `connCache` field
// is populated with a connection for every crdb node, or the field is
// left untouched, and an error is returned.
func (tr *testRunner) maybeInitConnections() error {
	tr.connCache.mu.Lock()
	defer tr.connCache.mu.Unlock()

	if tr.connCache.cache != nil {
		return nil
	}

	cc := make([]*gosql.DB, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		conn, err := tr.cluster.ConnE(tr.ctx, tr.logger, node)
		if err != nil {
			return fmt.Errorf("failed to connect to node %d: %w", node, err)
		}

		cc[node-1] = conn
	}

	tr.connCache.cache = cc
	return nil
}

func (tr *testRunner) connCacheInitialized() bool {
	tr.connCache.mu.Lock()
	defer tr.connCache.mu.Unlock()

	return tr.connCache.cache != nil
}

func (tr *testRunner) newHelper(ctx context.Context, l *logger.Logger) *Helper {
	return &Helper{
		ctx:        ctx,
		runner:     tr,
		stepLogger: l,
	}
}

// conn returns a database connection to the given node. Assumes the
// connection cache has been previously initialized.
func (tr *testRunner) conn(node int) *gosql.DB {
	tr.connCache.mu.Lock()
	defer tr.connCache.mu.Unlock()
	return tr.connCache.cache[node-1]
}

func (tr *testRunner) closeConnections() {
	tr.connCache.mu.Lock()
	defer tr.connCache.mu.Unlock()

	for _, db := range tr.connCache.cache {
		if db != nil {
			_ = db.Close()
		}
	}
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

func (tf *testFailure) Error() string {
	if tf.summarized {
		return tf.description
	}

	tf.summarized = true
	debugInfo := func(label, value string) string {
		return fmt.Sprintf("%-40s%s", label+":", value)
	}
	seedInfo := debugInfo("test random seed", strconv.FormatInt(tf.seed, 10))
	binaryVersions := debugInfo("binary versions", formatVersions(tf.binaryVersions))
	clusterVersionsBefore := debugInfo(
		"cluster versions before failure",
		formatVersions(tf.clusterVersionsBefore),
	)
	var clusterVersionsAfter string
	if cv := tf.clusterVersionsAfter; cv != nil {
		clusterVersionsBefore += "\n"
		clusterVersionsAfter = debugInfo("cluster versions after failure", formatVersions(cv))
	}

	return fmt.Sprintf(
		"%s\n%s\n%s\n%s%s",
		tf.description, seedInfo, binaryVersions, clusterVersionsBefore, clusterVersionsAfter,
	)
}

func renameFailedLogger(l *logger.Logger) error {
	currentFileName := l.File.Name()
	newLogName := path.Join(
		filepath.Dir(currentFileName),
		"FAILED_"+filepath.Base(currentFileName),
	)
	return os.Rename(currentFileName, newLogName)
}

func loadAtomicVersions(v atomic.Value) []roachpb.Version {
	if v.Load() == nil {
		return nil
	}

	return v.Load().([]roachpb.Version)
}

// panicAsError ensures that the any panics that might happen while
// the function passed runs are captured and returned as regular
// errors. A stack trace is included in the logs when that happens to
// facilitate debugging.
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

func formatVersions(versions []roachpb.Version) string {
	var pairs []string
	for idx, version := range versions {
		pairs = append(pairs, fmt.Sprintf("%d: %s", idx+1, version))
	}

	return fmt.Sprintf("[%s]", strings.Join(pairs, ", "))
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
