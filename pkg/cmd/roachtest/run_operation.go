// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

const baseSleepTime = 15

type opsRunner struct {
	clusterName string
	nodeCount   int
	opsToRun    []registry.OperationSpec

	workloadClusterName string
	workloadNodes       int

	seed   int64
	logger *logger.Logger

	datadogEventClient *datadogV1.EventsApi
	datadogTags        []string

	waitBeforeNextExecution time.Duration

	status struct {
		syncutil.Mutex
		// locks out worker from selecting operation to run
		lockOperationSelection bool
		// condition variable used to wait for running operation to finish
		// before running an operation which cannotRunConcurrently
		operationRunCompleted sync.Cond
		running               map[string]struct{}
		lastRun               map[string]time.Time
	}
}

// runOperations spins `parallelism` workers to run operations.
func runOperations(register func(registry.Registry), filter, clusterName string) error {
	r := makeTestRegistry()
	register(&r)
	opSpecs, err := opsToRun(r, filter)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = newDatadogContext(ctx)
	CtrlC(ctx, l, cancel, nil)

	or := opsRunner{
		clusterName:             clusterName,
		nodeCount:               cluster.VMs.Len(),
		opsToRun:                opSpecs,
		seed:                    seed,
		logger:                  l,
		datadogEventClient:      datadogV1.NewEventsApi(datadog.NewAPIClient(datadog.NewConfiguration())),
		datadogTags:             getDatadogTags(),
		waitBeforeNextExecution: roachtestflags.WaitBeforeNextExecution,
	}
	or.status.running = make(map[string]struct{})
	or.status.lastRun = make(map[string]time.Time)
	or.status.operationRunCompleted.L = &or.status

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
	return wg.Wait()
}

// runWorker manages the infinite loop for one operation runner worker.
func (r *opsRunner) runWorker(ctx context.Context, workerIdx int, runForever bool) {
	rng := rand.New(rand.NewSource(r.seed + int64(workerIdx)))
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		opSpec := r.selectOperationToRun(ctx, rng, workerIdx)
		if opSpec == nil {
			sleepDuration := time.Duration(baseSleepTime+rng.Intn(baseSleepTime)) * time.Second
			r.logger.Printf("[%d] couldn't find candidate operation to run, sleeping for %s", workerIdx, sleepDuration)
			time.Sleep(sleepDuration)
			continue
		}

		_ = r.runOperation(ctx, opSpec, rng, workerIdx)
		func() {
			r.status.Lock()
			defer r.status.Unlock()
			defer r.status.operationRunCompleted.Broadcast()
			if r.status.lockOperationSelection && opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
				r.status.lockOperationSelection = false
			}
			delete(r.status.running, opSpec.NamePrefix())
			r.status.lastRun[opSpec.NamePrefix()] = timeutil.Now()
		}()

		if !runForever {
			return
		}
		// give sometime to worker before picking next operation to run
		sleepDuration := time.Duration(5+rng.Intn(5)) * time.Minute
		r.logger.Printf("[%d] going idle for %s", workerIdx, sleepDuration)
		time.Sleep(sleepDuration)
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

	// operation which cannotRunConcurrently with other operation is currently running by another worker.
	// blocking operation selection for now.
	if r.status.lockOperationSelection {
		return nil
	}

	// operation is already running choose another one
	if _, ok := r.status.running[opSpec.NamePrefix()]; ok {
		return nil
	}

	// If the time since the last run of the operation has not exceeded its cadence,
	// choose another operation
	if lastRun, ok := r.status.lastRun[opSpec.NamePrefix()]; ok {
		nextRunTime := lastRun.Add(r.waitBeforeNextExecution)
		if timeutil.Now().Before(nextRunTime) {
			return nil
		}
	}

	if opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
		r.status.lockOperationSelection = true
		// selected operation cannot run concurrently with other operations
		// wait for other running operation completion
		for {
			if len(r.status.running) == 0 {
				break
			}
			r.logger.Printf("[%d] operation: %s waiting for other operation to complete", workerID, opSpec.Name)
			r.status.operationRunCompleted.Wait()
		}
	}

	r.status.running[opSpec.NamePrefix()] = struct{}{}
	return &opSpec
}

// runOperation runs a single operation passed in as opSpec parameter within a single operation worker.
func (r *opsRunner) runOperation(
	ctx context.Context, opSpec *registry.OperationSpec, rng *rand.Rand, workerIdx int,
) error {
	// operationRunID is used for datadog event aggregation and logging.
	operationRunID := rng.Uint64()

	defer func() {
		if rc := recover(); rc != nil {
			maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpError, operationRunID, r.datadogTags)
			r.logger.Printf("recovered from panic: %v", rc)
			debug.PrintStack()
		}
	}()

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
			return errors.Wrap(err, "failed to read config")
		}
		if err = yaml.UnmarshalStrict(configFileData, &config); err != nil {
			return errors.Wrapf(err, "failed to unmarshal config: %s", roachtestflags.ConfigPath)
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
	op.Status(fmt.Sprintf("checking if operation %s dependencies are met", opSpec.Name))

	if roachtestflags.SkipDependencyCheck {
		op.Status("skipping dependency check")
	} else if ok, err := operations.CheckDependencies(ctx, c, r.logger, opSpec); !ok || err != nil {
		if err != nil {
			op.Fatalf("error checking dependencies: %s", err)
		}
		op.Status("operation dependencies not met. Use --skip-dependency-check to skip this check.")
		return nil
	}

	maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpStarted, operationRunID, r.datadogTags)
	op.Status(fmt.Sprintf("running operation %s with run id %d", op.spec.Name, operationRunID))
	var cleanup registry.OperationCleanup
	func() {
		ctx, cancel := context.WithTimeout(ctx, opSpec.Timeout)
		defer cancel()

		cleanup = opSpec.Run(ctx, op, c)
	}()
	if op.Failed() {
		op.Status("operation failed")
		maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpError, operationRunID, r.datadogTags)
		return op.mu.failures[0]
	}

	maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpRan, operationRunID, r.datadogTags)
	if cleanup == nil {
		op.Status("operation ran successfully")
		maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpFinishedCleanup, operationRunID, r.datadogTags)
		return nil
	}

	op.Status(fmt.Sprintf("operation ran successfully; waiting %s before cleanup", roachtestflags.WaitBeforeCleanup))
	<-time.After(roachtestflags.WaitBeforeCleanup)

	op.Status("running cleanup")
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), opSpec.Timeout)
		defer cancel()

		cleanup.Cleanup(ctx, op, c)
	}()

	if op.Failed() {
		op.Status("operation cleanup failed")
		maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpError, operationRunID, r.datadogTags)
		return op.mu.failures[0]
	}
	maybeEmitDatadogEvent(ctx, r.datadogEventClient, opSpec, r.clusterName, eventOpFinishedCleanup, operationRunID, r.datadogTags)
	return nil
}
