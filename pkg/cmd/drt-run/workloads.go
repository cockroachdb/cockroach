// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type workloadRunner struct {
	config config
	eventL *eventLogger
	// NB: errors written to the error channel are considered fatal.
	// Errors that can be retried are written to the event logger.
	errChan chan error
}

// runWorkloadStep runs one step of the workload as defined in workloadConfig.
func (w *workloadRunner) runWorkloadStep(
	ctx context.Context, workload workloadConfig, workloadIdx int, step workloadStep,
) {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	args := []string{step.Command, workload.Kind}
	// TODO(bilal): Can we pass a no-op logger to roachprod.PgURL ?
	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		w.errChan <- errors.Wrap(err, "error when getting root logger")
		return
	}
	pgUrl, err := roachprod.PgURL(ctx, l, w.config.ClusterName, w.config.CertsDir, roachprod.PGURLOptions{
		Secure:   w.config.CertsDir != "",
		External: true,
	})
	if err != nil {
		// We treat pgurl errors as fatal.
		w.errChan <- errors.Wrapf(err, "error when getting PgURL for workload %d", workloadIdx)
		return
	}
	for i, url := range pgUrl {
		pgUrl[i] = strings.Trim(url, "'")
	}
	args = append(args, pgUrl...)
	args = append(args, step.Args...)

	fmt.Printf("running command %s %s\n", w.config.WorkloadBinary, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, w.config.WorkloadBinary, args...)

	// Set up pipes for reading stdout/err from the workload
	// process, and writing them to the event logger.
	out, err := cmd.StdoutPipe()
	if err != nil {
		w.errChan <- err
		return
	}
	errPipe, err2 := cmd.StderrPipe()
	if err2 != nil {
		w.errChan <- err2
		return
	}
	stdoutScanner := bufio.NewScanner(out)
	stderrScanner := bufio.NewScanner(errPipe)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for stderrScanner.Scan() {
			w.eventL.logWorkloadEvent(Event{
				Type:       EventOutput,
				SourceName: workload.Name,
				Details:    stderrScanner.Text(),
			}, workloadIdx)
		}
	}()
	go func() {
		defer wg.Done()
		for stdoutScanner.Scan() {
			w.eventL.logWorkloadEvent(Event{
				Type:       EventOutput,
				SourceName: workload.Name,
				Details:    stdoutScanner.Text(),
			}, workloadIdx)
		}
	}()

	_ = cmd.Start()
	if err := cmd.Wait(); err != nil {
		w.errChan <- err
		return
	}
}

func (w *workloadRunner) runWorker(ctx context.Context, workloadIdx int, workload workloadConfig) {
	ctx = logtags.WithTags(ctx, logtags.SingleTagBuffer("workload", workload.Name))

	// NB: We run all steps in an infinite loop. The only way to
	// exit from this loop is through a context cancellation.
	for {
		for _, step := range workload.Steps {
			select {
			case <-ctx.Done():
				return
			default:
			}
			w.runWorkloadStep(ctx, workload, workloadIdx, step)
		}
	}
}

func (w *workloadRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctrlC(ctx, cancel)
	var poolWG sync.WaitGroup
	for i := range w.config.Workloads {
		i := i
		poolWG.Add(1)
		go func() {
			defer poolWG.Done()
			w.runWorker(ctx, i, w.config.Workloads[i])
		}()
	}
	var retErr error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-w.errChan:
		w.eventL.logWorkloadEvent(Event{
			Type:       EventError,
			SourceName: "workload runner",
			Details:    err.Error(),
		}, 0)
		w.eventL.logWorkloadEvent(Event{
			Type:       EventError,
			SourceName: "workload runner",
			Details:    "closing all worklaods due to fatal error",
		}, 0)
		retErr = err
		cancel()
	}
	poolWG.Wait()
	return retErr
}

func makeWorkloadRunner(config config, eventL *eventLogger) *workloadRunner {
	concurrency := len(config.Workloads)
	runner := &workloadRunner{
		config:  config,
		eventL:  eventL,
		errChan: make(chan error, concurrency),
	}
	return runner
}
