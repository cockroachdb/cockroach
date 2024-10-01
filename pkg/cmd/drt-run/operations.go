// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func ctrlC(ctx context.Context, cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		select {
		case <-sig:
		case <-ctx.Done():
			return
		}
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

		lockOutOperations bool
		completed         sync.Cond
		lastRun           map[string]time.Time
		runningOperations []string
	}
}

// pickOperation picks one operation to run, that hasn't been run for at least `cadence`
// time (specified in the YAML config).
func (r *opsRunner) pickOperation(
	ctx context.Context, rng *rand.Rand, workerIdx int,
) *registry.OperationSpec {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		setIdx := rng.Intn(len(r.specs))
		opSpecsSet := r.specs[setIdx]
		opSpec := &opSpecsSet[rng.Intn(len(opSpecsSet))]
		shouldContinue := func() bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.mu.lockOutOperations {
				r.mu.completed.Wait()
				return true
			}

			lastRun := r.mu.lastRun[opSpec.Name]
			eligibleForNextRun := lastRun.Add(r.config.Operations.Sets[setIdx].Cadence)

			if timeutil.Now().Compare(eligibleForNextRun) < 0 {
				// Find another operation to run.
				r.mu.completed.Wait()
				return true
			}
			// Ratchet lastRun forward.
			r.mu.lastRun[opSpec.Name] = timeutil.Now()
			r.mu.runningOperations[workerIdx] = opSpec.Name

			// See what level of isolation this operation requires
			// from other operations.
			switch opSpec.CanRunConcurrently {
			case registry.OperationCanRunConcurrently:
				// Nothing to do.
			case registry.OperationCannotRunConcurrently:
				r.mu.lockOutOperations = true
				fallthrough
			case registry.OperationCannotRunConcurrentlyWithItself:
				for otherOpsRunning := true; otherOpsRunning; {
					otherOpsRunning = false
					for i := range r.mu.runningOperations {
						if i == workerIdx {
							continue
						}
						if r.mu.runningOperations[i] != "" &&
							(opSpec.CanRunConcurrently != registry.OperationCannotRunConcurrentlyWithItself || r.mu.runningOperations[i] == opSpec.Name) {
							otherOpsRunning = true
							break
						}
					}
					if otherOpsRunning {
						r.mu.completed.Wait()
					}
				}
			}
			return false
		}()
		if shouldContinue {
			continue
		}
		return opSpec
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
		fmt.Sprintf("--cloud=%s", r.config.Cloud),
	}
	if r.config.CertsDir != "" {
		args = append(args, fmt.Sprintf("--certs-dir=%s", r.config.CertsDir))
	}

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
	_ = cmd.Start()
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
	defer r.mu.Unlock()
	r.mu.runningOperations[workerIdx] = ""
	if opSpec.CanRunConcurrently == registry.OperationCannotRunConcurrently {
		r.mu.lockOutOperations = false
	}
	r.mu.completed.Broadcast()
}

// runWorker manages the infinite loop for one operation runner worker.
func (r *opsRunner) runWorker(ctx context.Context, workerIdx int) {
	rng := rand.New(rand.NewSource(r.seed + int64(workerIdx)))

	for {
		// Exit if the context is cancelled.
		if err := ctx.Err(); err != nil {
			return
		}

		opSpec := r.pickOperation(ctx, rng, workerIdx)
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
