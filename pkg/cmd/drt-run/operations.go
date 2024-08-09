// Copyright 2024 The Cockroach Authors.
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
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func ctrlC(ctx context.Context, cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		// Signal runners to stop.
		cancel()
	}()
}

// opsRegistry is a registry.Registry implementation which registers
// operations for use in drt-run only.
type opsRegistry struct {
	ops []registry.OperationSpec
}

// MakeClusterSpec implements the registry.Registry interface.
func (o *opsRegistry) MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec {
	return spec.MakeClusterSpec(nodeCount, opts...)
}

// Add implements the registry.Registry interface.
func (o *opsRegistry) Add(spec registry.TestSpec) {
	panic("unimplemented")
}

// AddOperation implements the registry.Registry interface.
func (o *opsRegistry) AddOperation(spec registry.OperationSpec) {
	o.ops = append(o.ops, spec)
}

// PromFactory implements the registry.Registry interface.
func (o *opsRegistry) PromFactory() promauto.Factory {
	panic("unimplemented")
}

var _ registry.Registry = &opsRegistry{}

// opsRunner holds all state for all `parallel` operation workers
// being run at the moment.
type opsRunner struct {
	specs       [][]registry.OperationSpec
	config      config
	l           *logger.Logger
	registry    registry.Registry
	parallelism int
	seed        int64
	eventLogger *eventLogger

	mu struct {
		syncutil.Mutex

		completed         sync.Cond
		lastRun           map[string]time.Time
		runningOperations []string
	}
}

// pickOperation picks one operation to run, that hasn't been run for at least `cadence`
// time (specified in the YAML config).
func (r *opsRunner) pickOperation(ctx context.Context, rng *rand.Rand) *registry.OperationSpec {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		setIdx := rng.Intn(len(r.specs))
		opSpecsSet := r.specs[setIdx]
		opSpec := &opSpecsSet[rng.Intn(len(opSpecsSet))]
		r.mu.Lock()
		lastRun := r.mu.lastRun[opSpec.Name]
		eligibleForNextRun := lastRun.Add(r.config.Operations.Sets[setIdx].Cadence)

		// TODO(bilal): Use opSpec.CanRunConcurrently to lock out other operations
		// here.

		if time.Now().Compare(eligibleForNextRun) >= 0 {
			// Ratchet lastRun forward.
			r.mu.lastRun[opSpec.Name] = time.Now()
			r.mu.Unlock()
			return opSpec
		}
		r.mu.completed.Wait()
		r.mu.Unlock()
	}
}

// getRunningOperations returns a list of all operations currently running.
// The slice returned is r.parallelism long, and each element is either nil (if
// a worker is idle), or contains the name of a running operation.
func (r *opsRunner) getRunningOperations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.mu.runningOperations...)
}

// runOperation runs a single operation within a single operation worker. It spins
// up a subprocesss into `roachtest run-operation`.
func (r *opsRunner) runOperation(
	ctx context.Context, opSpec *registry.OperationSpec, rng *rand.Rand, workerIdx int,
) {
	var cancel context.CancelFunc
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel = context.WithTimeout(ctx, opSpec.Timeout)
	defer cancel()

	// Have the effect of the operation last 5 minutes + a random amount of time between 0s and 24h.
	cleanupDuration := 5*time.Minute + (time.Duration(rng.Intn(int((24 * time.Hour).Seconds()))) * time.Second)

	args := []string{"run-operation", r.config.ClusterName, opSpec.Name,
		fmt.Sprintf("--wait-before-cleanup=%s", cleanupDuration.String()),
		fmt.Sprintf("--cockroach-binary=%s", r.config.CockroachBinary),
		fmt.Sprintf("--cloud=%s", r.config.Cloud),
	}
	if r.config.CertsDir != "" {
		args = append(args, fmt.Sprintf("--certs-dir=%s", r.config.CertsDir))
	}

	// Set the operation being run by this worker. Used for observability
	// such as in the http endpoints.
	r.mu.Lock()
	r.mu.runningOperations[workerIdx] = opSpec.Name
	r.mu.Unlock()

	r.eventLogger.logOperationEvent(Event{
		Type:       EventStart,
		SourceName: opSpec.Name,
	})

	cmd := exec.CommandContext(ctx, r.config.RoachtestBinary, args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		r.eventLogger.logOperationEvent(Event{
			Type:       EventError,
			SourceName: opSpec.Name,
			Details:    fmt.Sprintf("error when getting stdout pipe: %s", err),
		})
		return
	}
	stderrPipe, err2 := cmd.StderrPipe()
	if err2 != nil {
		r.eventLogger.logOperationEvent(Event{
			Type:       EventError,
			SourceName: opSpec.Name,
			Details:    fmt.Sprintf("error when getting stderr pipe: %s", err),
		})
		return
	}
	cmd.Start()
	wg.Add(2)
	// Spin up goroutines to read stdout and stderr, and pipe them
	// into the event logger.
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			r.eventLogger.logOperationEvent(Event{
				Type:       EventOutput,
				SourceName: opSpec.Name,
				Details:    scanner.Text(),
			})
		}
	}()
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			r.eventLogger.logOperationEvent(Event{
				Type:       EventOutput,
				SourceName: opSpec.Name,
				Details:    scanner.Text(),
			})
		}
	}()
	if err := cmd.Wait(); err != nil {
		r.eventLogger.logOperationEvent(Event{
			Type:       EventError,
			SourceName: opSpec.Name,
			Details:    err.Error(),
		})
		return
	}
	r.eventLogger.logOperationEvent(Event{
		Type:       EventFinish,
		SourceName: opSpec.Name,
	})

	r.mu.Lock()
	r.mu.runningOperations[workerIdx] = ""
	r.mu.completed.Broadcast()
	r.mu.Unlock()
}

// runWorker manages the infinite loop for one operation runner worker.
func (r *opsRunner) runWorker(ctx context.Context, workerIdx int) {
	rng := rand.New(rand.NewSource(r.seed + int64(workerIdx)))

	for {
		// Exit if the context is cancelled.
		if err := ctx.Err(); err != nil {
			return
		}

		opSpec := r.pickOperation(ctx, rng)
		r.runOperation(ctx, opSpec, rng, workerIdx)
	}
}

// Run spins up `parallelism` workers to run operations.
func (r *opsRunner) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctrlC(ctx, cancel)

	var wg sync.WaitGroup
	wg.Add(r.parallelism)

	// Spin up `parallelism` workers.
	for i := 0; i < r.parallelism; i++ {
		i := i
		go func() {
			defer wg.Done()
			r.runWorker(ctx, i)
		}()
	}

	wg.Wait()
}

func filterOps(ops []registry.OperationSpec, filter string) ([]registry.OperationSpec, error) {
	regex, err := regexp.Compile(filter)
	if err != nil {
		return nil, err
	}
	var filteredOps []registry.OperationSpec
	for _, opSpec := range ops {
		if regex.MatchString(opSpec.Name) {
			filteredOps = append(filteredOps, opSpec)
		}
	}
	if len(filteredOps) == 0 {
		return nil, errors.New("no matching operations to run")
	}
	return filteredOps, nil
}

func makeOpsRunner(parallelism int, config config, eventLogger *eventLogger) (*opsRunner, error) {
	r := &opsRegistry{}
	_, seed := randutil.NewTestRand()

	operations.RegisterOperations(r)
	specs := make([][]registry.OperationSpec, len(config.Operations.Sets))

	for i := range specs {
		var err error
		specs[i], err = filterOps(r.ops, config.Operations.Sets[i].Filter)
		if err != nil {
			return nil, err
		}
	}
	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		return nil, err
	}

	runner := &opsRunner{
		specs:       specs,
		config:      config,
		registry:    r,
		l:           l,
		eventLogger: eventLogger,
		seed:        seed,
		parallelism: parallelism,
	}
	runner.mu.runningOperations = make([]string, parallelism)
	runner.mu.lastRun = make(map[string]time.Time)
	runner.mu.completed.L = &runner.mu

	return runner, nil
}
