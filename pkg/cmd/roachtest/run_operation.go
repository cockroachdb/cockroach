// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
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
	opsToRun    []registry.OperationSpec

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

	metrics := newOperationMetrics(r.PromFactory())
	or := opsRunner{
		clusterName:             clusterName,
		nodeCount:               cluster.VMs.Len(),
		opsToRun:                opSpecs,
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
	for i := 1; i <= parallelism; i++ {
		idx := i
		wg.Go(func() error {
			or.runWorker(ctx, idx, runForever)
			return nil
		})
	}
	wgErr := wg.Wait()
	// All workers have exited. Process any cleanups that were enqueued
	// after the processor's last poll or drain, ensuring nothing is lost.
	or.processCleanups(or.dequeueAll())
	return wgErr
}

// setWorkerState updates the workerCurrentOperation gauge for a worker,
// clearing the previous state and setting the new one.
func (r *opsRunner) setWorkerState(workerLabel, prevOp, prevState, newOp, newState string) {
	if prevOp != "" && prevState != "" {
		r.metrics.workerCurrentOperation.
			WithLabelValues(workerLabel, prevOp, prevState).Set(0)
	}
	if newOp != "" && newState != "" {
		r.metrics.workerCurrentOperation.
			WithLabelValues(workerLabel, newOp, newState).Set(1)
	}
}

// runWorker manages the infinite loop for one operation runner worker.
func (r *opsRunner) runWorker(ctx context.Context, workerIdx int, runForever bool) {
	rng := rand.New(rand.NewSource(r.seed + int64(workerIdx)))
	workerLabel := strconv.Itoa(workerIdx)

	// Start idle.
	r.setWorkerState(workerLabel, "", "", workerOperationIdle, workerStateIdle)

	for {
		if err := ctx.Err(); err != nil {
			r.setWorkerState(workerLabel, workerOperationIdle, workerStateIdle, "", "")
			return
		}

		opSpec := r.selectOperationToRun(ctx, rng, workerIdx)
		if opSpec == nil {
			// Already idle, stay idle.
			sleepDuration := time.Duration(baseSleepTime+rng.Intn(baseSleepTime)) * time.Second
			r.logger.Printf("[%d] couldn't find candidate operation to run, sleeping for %s", workerIdx, sleepDuration)
			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return
			}
			continue
		}

		r.setWorkerState(workerLabel, workerOperationIdle, workerStateIdle, opSpec.NamePrefix(), workerStateExecuting)
		_, deferred := r.runOperation(ctx, opSpec, rng, workerIdx)
		func() {
			r.status.Lock()
			defer r.status.Unlock()
			defer r.status.operationRunCompleted.Broadcast()
			if r.status.lockOperationSelection && opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
				r.status.lockOperationSelection = false
			}
			delete(r.status.running, opSpec.NamePrefix())
			r.status.lastRun[opSpec.NamePrefix()] = timeutil.Now()
			if deferred {
				r.status.pendingCleanup[opSpec.NamePrefix()] = struct{}{}
			}
		}()

		r.setWorkerState(workerLabel, opSpec.NamePrefix(), workerStateExecuting, workerOperationIdle, workerStateIdle)

		if !runForever {
			r.setWorkerState(workerLabel, workerOperationIdle, workerStateIdle, "", "")
			return
		}
		sleepDuration := time.Duration(1+rng.Intn(2)) * time.Minute
		r.logger.Printf("[%d] going idle for %s", workerIdx, sleepDuration)
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return
		}
	}
}

// selectOperationToRun picks one operation to run, that hasn't been run for at least waitBeforeNextExecution time.
func (r *opsRunner) selectOperationToRun(
	ctx context.Context, rng *rand.Rand, workerID int,
) *registry.OperationSpec {

	if err := ctx.Err(); err != nil {
		return nil
	}

	// randomly select a candidate operation to run
	opSpec := r.opsToRun[rng.Intn(len(r.opsToRun))]

	r.status.Lock()
	defer r.status.Unlock()

	// operation which cannotRunConcurrently with other operation is currently
	// running by another worker — blocking operation selection for now.
	if r.status.lockOperationSelection {
		return nil
	}

	// operation is already running, choose another one
	if _, ok := r.status.running[opSpec.NamePrefix()]; ok {
		return nil
	}

	// Operation has a deferred cleanup pending; choose another one.
	if _, ok := r.status.pendingCleanup[opSpec.NamePrefix()]; ok {
		return nil
	}

	// If the time since the last run of the operation has not exceeded its
	// cadence, choose another operation.
	if lastRun, ok := r.status.lastRun[opSpec.NamePrefix()]; ok {
		nextRunTime := lastRun.Add(r.waitBeforeNextExecution)
		if timeutil.Now().Before(nextRunTime) {
			return nil
		}
	}

	if opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
		r.status.lockOperationSelection = true
		// selected operation cannot run concurrently with other operations —
		// wait for other running operations and pending cleanups to finish.
		workerLabel := strconv.Itoa(workerID)
		for {
			if len(r.status.running) == 0 && len(r.status.pendingCleanup) == 0 {
				break
			}
			r.setWorkerState(workerLabel, workerOperationIdle, workerStateIdle, opSpec.NamePrefix(), workerStateWaitingLock)
			r.logger.Printf("[%d] operation: %s waiting for other operation to complete", workerID, opSpec.Name)
			r.status.operationRunCompleted.Wait()
			r.setWorkerState(workerLabel, opSpec.NamePrefix(), workerStateWaitingLock, workerOperationIdle, workerStateIdle)
		}
	}

	r.status.running[opSpec.NamePrefix()] = struct{}{}
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
			delete(r.status.pendingCleanup, opName)
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
	ctx context.Context, opSpec *registry.OperationSpec, rng *rand.Rand, workerIdx int,
) (error, bool) {
	// operationRunID is used for datadog event aggregation and logging.
	operationRunID := rng.Uint64()
	opName := opSpec.NamePrefix()
	owner := string(opSpec.Owner)
	workerLabel := strconv.Itoa(workerIdx)

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
		workerId:        workerIdx,
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
