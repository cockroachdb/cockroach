// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

const baseSleepTime = 15

// deferredCleanup holds everything needed to run a cleanup that was
// deferred by a DeferCleanup operation. It is enqueued with a future
// executeAt time and processed by the cleanup processor goroutine.
type deferredCleanup struct {
	executeAt time.Time
	op        *operationImpl
	c         *dynamicClusterImpl
	cleanup   registry.OperationCleanup
	emitter   *operationEventEmitter
	opSpec    *registry.OperationSpec
}

type opsRunner struct {
	clusterName string
	nodeCount   int

	workloadClusterName string
	workloadNodes       int

	seed    int64
	logger  *logger.Logger
	metrics *operationMetrics

	datadogEventClient *datadogV1.EventsApi
	datadogTags        []string

	waitBeforeNextExecution time.Duration
	runForever              bool

	// cleanupMu protects cleanupQueue.
	cleanupMu syncutil.Mutex
	// cleanupQueue holds deferred cleanups sorted by executeAt (ascending).
	cleanupQueue []deferredCleanup
	// cleanupDone is closed when the cleanup processor goroutine exits.
	cleanupDone chan struct{}

	status struct {
		syncutil.Mutex
		// locks out worker from selecting operation to run
		lockOperationSelection bool
		// condition variable used to wait for running operation to finish
		// before running an operation which cannotRunConcurrently
		operationRunCompleted sync.Cond
		running               map[string]struct{}
		lastRun               map[string]time.Time
		// pendingCleanup tracks operations with deferred cleanup waiting
		// to execute. Prevents re-selection until cleanup completes.
		pendingCleanup map[string]struct{}
	}
}

// runOperations spins `parallelism` workers to run operations.
func runOperations(register func(registry.Registry), filter, skip, clusterName string) error {
	r := makeTestRegistry()
	register(&r)
	opSpecs, err := opsToRun(r, filter, skip)
	if err != nil {
		return err
	}

	if err := roachprod.LoadClusters(); err != nil {
		return err
	}
	cluster, err := getCachedCluster(clusterName)
	if err != nil {
		return err
	}

	_, seed := randutil.NewTestRand()
	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		return err
	}

	// Split operations into normal and long-running lists.
	var normalOps, longRunningOps []registry.OperationSpec
	for _, op := range opSpecs {
		if op.LongRunning {
			longRunningOps = append(longRunningOps, op)
		} else {
			normalOps = append(normalOps, op)
		}
	}

	metrics := newOperationMetrics(r.PromFactory())
	or := opsRunner{
		clusterName:             clusterName,
		nodeCount:               cluster.VMs.Len(),
		seed:                    seed,
		logger:                  l,
		metrics:                 metrics,
		datadogEventClient:      datadogV1.NewEventsApi(datadog.NewAPIClient(datadog.NewConfiguration())),
		datadogTags:             getDatadogTags(),
		waitBeforeNextExecution: roachtestflags.WaitBeforeNextExecution,
		runForever:              roachtestflags.RunForever,
	}
	or.status.running = make(map[string]struct{})
	or.status.lastRun = make(map[string]time.Time)
	or.status.pendingCleanup = make(map[string]struct{})
	or.status.operationRunCompleted.L = &or.status
	or.cleanupDone = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		<-or.cleanupDone
	}()

	ctx = newDatadogContext(ctx)
	CtrlC(ctx, l, cancel, nil)
	go or.runCleanupProcessor(ctx)

	go func() {
		if err := http.ListenAndServe(
			fmt.Sprintf(":%d", roachtestflags.PromPort),
			promhttp.HandlerFor(r.promRegistry, promhttp.HandlerOpts{}),
		); err != nil {
			l.Errorf("error serving prometheus: %v", err)
		}
	}()

	if roachtestflags.WorkloadCluster != "" {
		workloadCluster, err := getCachedCluster(roachtestflags.WorkloadCluster)
		if err != nil {
			return err
		}
		or.workloadClusterName = workloadCluster.Name
		or.workloadNodes = workloadCluster.VMs.Len()
	}

	var wg errgroup.Group
	runForever := roachtestflags.RunForever
	parallelism := min(roachtestflags.OperationParallelism, roachtestflags.MaxOperationParallelism)
	if len(normalOps) > 0 {
		for i := 1; i <= parallelism; i++ {
			label := fmt.Sprintf("worker-%d", i)
			wg.Go(func() error {
				or.runWorker(ctx, label, runForever, normalOps)
				return nil
			})
		}
	}
	if len(longRunningOps) > 0 {
		wg.Go(func() error {
			or.runWorker(ctx, "long-running", runForever, longRunningOps)
			return nil
		})
	}
	wgErr := wg.Wait()
	// All workers have exited. Process any cleanups that were enqueued
	// after the processor's last poll or drain, ensuring nothing is lost.
	or.processCleanups(or.dequeueAll())
	return wgErr
}

// workerStateTracker tracks the current metric state for a single worker,
// clearing the previous gauge value before setting the new one.
type workerStateTracker struct {
	label    string
	metrics  *operationMetrics
	curOp    string
	curState string
}

func newWorkerStateTracker(label string, metrics *operationMetrics) *workerStateTracker {
	w := &workerStateTracker{label: label, metrics: metrics}
	w.setIdle()
	return w
}

// transition clears the current gauge and sets the new (op, state) pair.
func (w *workerStateTracker) transition(op, state string) {
	if w.curOp != "" && w.curState != "" {
		w.metrics.workerCurrentOperation.
			WithLabelValues(w.label, w.curOp, w.curState).Set(0)
	}
	w.curOp, w.curState = op, state
	if op != "" && state != "" {
		w.metrics.workerCurrentOperation.
			WithLabelValues(w.label, op, state).Set(1)
	}
}

func (w *workerStateTracker) setIdle() {
	w.transition(workerOperationIdle, workerStateIdle)
}

func (w *workerStateTracker) setExecuting(opName string) {
	w.transition(opName, workerStateExecuting)
}

func (w *workerStateTracker) clear() { w.transition("", "") }

// runWorker manages the loop for one operation runner worker. It
// selects operations from the given ops slice, executes them, and
// repeats until the context is cancelled or runForever is false.
// Both normal and long-running workers use this method — the only
// difference is the ops slice they receive.
func (r *opsRunner) runWorker(
	ctx context.Context, workerLabel string, runForever bool, ops []registry.OperationSpec,
) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(workerLabel))
	rng := rand.New(rand.NewSource(r.seed + int64(h.Sum64())))
	ws := newWorkerStateTracker(workerLabel, r.metrics)
	consecutiveMisses := 0

	for {
		if ctx.Err() != nil {
			ws.clear()
			return
		}

		opSpec := r.selectOperationToRun(ctx, rng, ws, ops)
		if opSpec == nil {
			consecutiveMisses++
			sleepDuration := time.Duration(baseSleepTime+rng.Intn(baseSleepTime)) * time.Second
			if consecutiveMisses == 1 || consecutiveMisses%10 == 0 {
				r.logger.Printf("[%s] couldn't find candidate operation to run (x%d), sleeping for %s", workerLabel, consecutiveMisses, sleepDuration)
			}
			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return
			}
			continue
		}
		consecutiveMisses = 0

		ws.setExecuting(opSpec.NamePrefix())
		_, deferred := r.runOperation(ctx, opSpec, rng, workerLabel)
		r.completeOperation(opSpec, deferred)
		ws.setIdle()

		if !runForever {
			ws.clear()
			return
		}
		sleepDuration := time.Duration(1+rng.Intn(2)) * time.Minute
		r.logger.Printf("[%s] going idle for %s", workerLabel, sleepDuration)
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return
		}
	}
}

// completeOperation updates the status maps after an operation finishes.
// It removes the operation from running, records lastRun time, optionally
// marks deferred cleanup, and broadcasts to unblock waiting workers.
func (r *opsRunner) completeOperation(opSpec *registry.OperationSpec, deferred bool) {
	r.status.Lock()
	defer r.status.Unlock()
	defer r.status.operationRunCompleted.Broadcast()
	if r.status.lockOperationSelection &&
		opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
		r.status.lockOperationSelection = false
	}
	key := opSpec.DedupKey()
	delete(r.status.running, key)
	r.status.lastRun[key] = timeutil.Now()
	if deferred {
		r.status.pendingCleanup[key] = struct{}{}
	}
}

// selectOperationToRun picks one operation from ops to run, that hasn't
// been run for at least its cadence interval (or the global default).
func (r *opsRunner) selectOperationToRun(
	ctx context.Context, rng *rand.Rand, ws *workerStateTracker, ops []registry.OperationSpec,
) *registry.OperationSpec {
	if ctx.Err() != nil {
		return nil
	}

	// Randomly select a candidate operation to run.
	opSpec := ops[rng.Intn(len(ops))]

	r.status.Lock()
	defer r.status.Unlock()

	// An exclusive operation is currently running — skip selection.
	if r.status.lockOperationSelection {
		return nil
	}

	if _, ok := r.status.running[opSpec.DedupKey()]; ok {
		return nil
	}
	if _, ok := r.status.pendingCleanup[opSpec.DedupKey()]; ok {
		return nil
	}

	// Per-operation Cadence overrides the global waitBeforeNextExecution.
	if lastRun, ok := r.status.lastRun[opSpec.DedupKey()]; ok {
		cadence := r.waitBeforeNextExecution
		if opSpec.Cadence > 0 {
			cadence = opSpec.Cadence
		}
		if timeutil.Now().Before(lastRun.Add(cadence)) {
			return nil
		}
	}

	if opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
		r.status.lockOperationSelection = true
		for len(r.status.running) > 0 {
			ws.transition(opSpec.NamePrefix(), workerStateWaitingLock)
			r.logger.Printf("[%s] operation: %s waiting for other operation to complete", ws.label, opSpec.Name)
			r.status.operationRunCompleted.Wait()
			ws.setIdle()
		}
	}

	r.status.running[opSpec.DedupKey()] = struct{}{}
	return &opSpec
}

// executeCleanup runs the cleanup phase with its own panic recovery so that
// a panic during cleanup (e.g. from o.Fatal) does not crash the worker.
func (r *opsRunner) executeCleanup(
	op *operationImpl,
	c *dynamicClusterImpl,
	cleanup registry.OperationCleanup,
	emitter *operationEventEmitter,
	opSpec *registry.OperationSpec,
) {
	if cleanup == nil {
		if !op.Failed() {
			op.Status("operation ran successfully")
			emitter.EmitCleanupCompleted(cleanupResultSkipped, nil)
		}
		return
	}

	op.Status("running cleanup")

	// Clear run-phase failures so that op.Failed() after cleanup only
	// reflects whether cleanup itself failed. The run-phase failure has
	// already been logged and emitted.
	func() {
		op.mu.Lock()
		defer op.mu.Unlock()
		op.mu.failures = nil
	}()

	// Inner defer: guarantees EmitCleanupCompleted fires even if cleanup
	// panics, and recovers the panic so the worker survives.
	defer func() {
		cleanupResult := cleanupResultSuccess
		if rc := recover(); rc != nil {
			cleanupResult = cleanupResultFailed
			stack := debugutil.Stack()
			r.logger.Printf("recovered from cleanup panic: %v\n%s", rc, stack)
		}
		var cleanupFailures []error
		if op.Failed() {
			cleanupResult = cleanupResultFailed
			cleanupFailures = op.Failures()
			op.Status("operation cleanup failed")
		}
		emitter.EmitCleanupCompleted(cleanupResult, cleanupFailures)
	}()

	cleanupCtx, cancel := context.WithTimeout(context.Background(), opSpec.Timeout)
	defer cancel()
	cleanup.Cleanup(cleanupCtx, op, c)
}

// enqueueCleanup adds a deferred cleanup to the queue, maintaining
// ascending executeAt order.
func (r *opsRunner) enqueueCleanup(dc deferredCleanup) {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	i := sort.Search(len(r.cleanupQueue), func(i int) bool {
		return r.cleanupQueue[i].executeAt.After(dc.executeAt)
	})
	r.cleanupQueue = slices.Insert(r.cleanupQueue, i, dc)
	r.metrics.pendingCleanups.WithLabelValues(dc.opSpec.NamePrefix()).Inc()
}

// dequeueReady removes and returns all cleanups whose executeAt <= now.
func (r *opsRunner) dequeueReady() []deferredCleanup {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	now := timeutil.Now()
	i := sort.Search(len(r.cleanupQueue), func(i int) bool {
		return r.cleanupQueue[i].executeAt.After(now)
	})
	if i == 0 {
		return nil
	}
	ready := make([]deferredCleanup, i)
	copy(ready, r.cleanupQueue[:i])
	r.cleanupQueue = r.cleanupQueue[i:]
	return ready
}

// dequeueAll removes and returns all queued cleanups regardless of
// executeAt time. Used during shutdown to drain the queue.
func (r *opsRunner) dequeueAll() []deferredCleanup {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	all := r.cleanupQueue
	r.cleanupQueue = nil
	return all
}

// runCleanupProcessor polls the cleanup queue and executes ready
// cleanups. On context cancellation (shutdown), it drains all remaining
// cleanups immediately.
func (r *opsRunner) runCleanupProcessor(ctx context.Context) {
	defer close(r.cleanupDone)

	const pollInterval = 30 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.processCleanups(r.dequeueReady())
		case <-ctx.Done():
			r.processCleanups(r.dequeueAll())
			return
		}
	}
}

// processCleanups executes a batch of deferred cleanups, updating
// metrics and concurrency tracking after each one.
func (r *opsRunner) processCleanups(cleanups []deferredCleanup) {
	for _, dc := range cleanups {
		opName := dc.opSpec.NamePrefix()
		r.logger.Printf(
			"executing deferred cleanup for %s (scheduled at %s)",
			opName, dc.executeAt.Format(time.RFC3339),
		)
		r.executeCleanup(dc.op, dc.c, dc.cleanup, dc.emitter, dc.opSpec)
		r.metrics.pendingCleanups.WithLabelValues(opName).Dec()

		func() {
			r.status.Lock()
			defer r.status.Unlock()
			delete(r.status.pendingCleanup, dc.opSpec.DedupKey())
			r.status.operationRunCompleted.Broadcast()
		}()
	}
}

// runOperation runs a single operation passed in as opSpec parameter
// within a single operation worker. When DeferCleanup is set on the
// spec and the operation succeeds, cleanup is enqueued for later
// execution and cleanupDeferred is returned as true. The caller must
// add the operation to pendingCleanup under the status lock.
func (r *opsRunner) runOperation(
	ctx context.Context, opSpec *registry.OperationSpec, rng *rand.Rand, workerLabel string,
) (error, bool) {
	// operationRunID is used for datadog event aggregation and logging.
	operationRunID := rng.Uint64()
	opName := opSpec.NamePrefix()
	owner := string(opSpec.Owner)

	r.metrics.activeOps.WithLabelValues(opName, workerLabel).Inc()
	defer r.metrics.activeOps.WithLabelValues(opName, workerLabel).Dec()

	emitter := newOperationEventEmitter(
		ctx, r.datadogEventClient, opSpec, r.clusterName, operationRunID,
		owner, workerLabel, r.datadogTags,
	)

	config := struct {
		ClusterSettings install.ClusterSettings
		StartOpts       option.StartOpts
		ClusterSpec     spec.ClusterSpec
	}{
		ClusterSettings: install.MakeClusterSettings(),
		StartOpts:       option.NewStartOpts(option.NoBackupSchedule),
		ClusterSpec:     spec.ClusterSpec{NodeCount: r.nodeCount},
	}

	if roachtestflags.ConfigPath != "" {
		r.logger.Printf("Loading operation configuration from: %s", roachtestflags.ConfigPath)
		configFileData, err := os.ReadFile(roachtestflags.ConfigPath)
		if err != nil {
			return errors.Wrap(err, "failed to read config"), false
		}
		if err = yaml.UnmarshalStrict(configFileData, &config); err != nil {
			return errors.Wrapf(err, "failed to unmarshal config: %s", roachtestflags.ConfigPath), false
		}
	}

	op := &operationImpl{
		clusterSettings: config.ClusterSettings,
		startOpts:       config.StartOpts,
		l:               r.logger,
		spec:            opSpec,
		workerLabel:     workerLabel,
	}

	cSpec := spec.ClusterSpec{NodeCount: r.nodeCount}
	c := &dynamicClusterImpl{
		&clusterImpl{
			name:       r.clusterName,
			cloud:      roachtestflags.Cloud,
			spec:       cSpec,
			f:          op,
			l:          r.logger,
			expiration: cSpec.Expiration(),
			destroyState: destroyState{
				owned: false,
			},
			localCertsDir: roachtestflags.CertsDir,
		},
	}

	if r.workloadClusterName != "" {
		op.workLoadCluster = &clusterImpl{
			name: r.workloadClusterName,
			spec: spec.ClusterSpec{NodeCount: r.workloadNodes},
			l:    r.logger,
			f:    op,
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	op.mu.cancel = cancel

	var cleanup registry.OperationCleanup
	var pendingCleanupIncremented bool
	// cleanupDeferred is set to true when the cleanup is enqueued for
	// later execution. When true, the defer skips running cleanup and
	// decrementing pendingCleanups (the processor handles both).
	var cleanupDeferred bool

	defer func() {
		// Handle panic if it occurred during operation execution.
		if rc := recover(); rc != nil {
			stack := debugutil.Stack()
			// Include recorded failures (from o.Fatal) before the panic+stack entry
			// so the actual error message surfaces in Datadog events.
			failures := append(op.Failures(), fmt.Errorf("panic: %v\n\n%s", rc, stack))
			emitter.EmitCompleted(resultPanicked, failures)
			r.logger.Printf("recovered from panic: %v\n%s", rc, stack)
		}
		if cleanupDeferred {
			return
		}
		if pendingCleanupIncremented {
			r.metrics.pendingCleanups.WithLabelValues(opName).Dec()
		}
		r.executeCleanup(op, c, cleanup, emitter, opSpec)
	}()

	// Dependency check phase.
	op.Status(fmt.Sprintf("checking if operation %s dependencies are met", opSpec.Name))
	if !roachtestflags.SkipDependencyCheck {
		ok, err := operations.CheckDependencies(ctx, c, r.logger, opSpec)
		if err != nil {
			emitter.EmitDepCheckFailed(depCheckError, []error{err})
			op.Errorf("error checking dependencies: %s", err)
			return errors.Wrap(err, "checking dependencies"), false
		}
		if !ok {
			emitter.EmitDepCheckFailed(depCheckFailed, nil)
			// Record a failure so the defer does not emit a spurious
			// "ran successfully" / cleanupResultSkipped event.
			op.Errorf("operation dependencies not met")
			op.Status("operation dependencies not met. Use --skip-dependency-check to skip this check.")
			return nil, false
		}
	}

	// Run phase.
	emitter.EmitStarted()
	op.Status(fmt.Sprintf("running operation %s with run id %d", op.spec.Name, operationRunID))
	runStart := timeutil.Now()
	func() {
		ctx, cancel := context.WithTimeout(ctx, opSpec.Timeout)
		defer cancel()

		cleanup = opSpec.Run(ctx, op, c)
	}()
	r.metrics.lastRunDuration.WithLabelValues(opName).
		Set(timeutil.Since(runStart).Seconds())

	opFailed := op.Failed()
	if opFailed {
		op.Status("operation failed")
		failures := op.Failures()
		emitter.EmitCompleted(resultFailed, failures)
		// Don't return early — defer will run cleanup if a cleanup handler
		// was returned by the operation.
		return failures[0], false
	}
	emitter.EmitCompleted(resultSuccess, nil)

	if cleanup == nil {
		// No cleanup needed; the defer will emit cleanupResultSkipped.
		return nil, false
	}

	// Wait before cleanup if operation succeeded.
	waitBeforeCleanup := roachtestflags.WaitBeforeCleanup
	if opSpec.WaitBeforeCleanup != 0 {
		waitBeforeCleanup = opSpec.WaitBeforeCleanup
	}

	// If DeferCleanup is set and we're running in forever mode,
	// enqueue cleanup for later execution and free the worker.
	// In single-run mode, deferring has no benefit since the
	// process exits after the operation completes.
	if opSpec.DeferCleanup && r.runForever {
		op.Status(fmt.Sprintf(
			"operation ran successfully; cleanup deferred for %s", waitBeforeCleanup,
		))
		r.enqueueCleanup(deferredCleanup{
			executeAt: timeutil.Now().Add(waitBeforeCleanup),
			op:        op,
			c:         c,
			cleanup:   cleanup,
			emitter:   emitter,
			opSpec:    opSpec,
		})
		cleanupDeferred = true
		return nil, true
	}

	r.metrics.pendingCleanups.WithLabelValues(opName).Inc()
	pendingCleanupIncremented = true
	op.Status(fmt.Sprintf("operation ran successfully; waiting %s before cleanup", waitBeforeCleanup))
	select {
	case <-time.After(waitBeforeCleanup):
	case <-ctx.Done():
		op.Status("context canceled during wait; proceeding to cleanup immediately")
	}

	// Function ends, defer runs and executes cleanup.
	return nil, false
}
