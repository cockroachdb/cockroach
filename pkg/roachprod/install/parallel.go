// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/errors"
)

type ParallelResult struct {
	// Index is the order position in which the node was passed to Parallel.
	// This is useful in maintaining the order of results.
	Index int
	*RunResultDetails
}

type ParallelOptions struct {
	concurrency int
	display     string
	retryOpts   *RunRetryOpts
	// waitOnFail will cause the Parallel function to wait for all nodes to
	// finish when encountering a command error on any node. The default
	// behaviour is to exit immediately on the first error, in which case the
	// slice of ParallelResults will only contain the one error result.
	waitOnFail bool
}

type ParallelOption func(result *ParallelOptions)

func WithConcurrency(concurrency int) ParallelOption {
	return func(result *ParallelOptions) {
		result.concurrency = concurrency
	}
}

func WithRetryOpts(retryOpts *RunRetryOpts) ParallelOption {
	return func(result *ParallelOptions) {
		result.retryOpts = retryOpts
	}
}

func WithWaitOnFail() ParallelOption {
	return func(result *ParallelOptions) {
		result.waitOnFail = true
	}
}

func WithDisplay(display string) ParallelOption {
	return func(result *ParallelOptions) {
		result.display = display
	}
}

// ParallelE runs the given function in parallel on the specified nodes.
//
// By default, this will fail fast if a command error occurs on any node, and return
// a slice containing all results up to that point, along with a boolean indicating
// that at least one error occurred. If `WithWaitOnFail()` is passed in, then the function
// will wait for all invocations to complete before returning.
//
// ParallelE only returns an error for roachprod itself, not any command errors run
// on the cluster.
//
// ParallelE runs at most `concurrency` (or  `config.MaxConcurrency` if it is lower) in parallel.
// If `concurrency` is 0, then it defaults to `len(nodes)`.
//
// The function returns pointers to *RunResultDetails as we may enrich
// the result with retry information (attempt number, wrapper error).
//
// RunRetryOpts controls the retry behavior in the case that
// the function fails, but returns a nil error. A non-nil error returned by the
// function denotes a roachprod error and will not be retried regardless of the
// retry options.
// NB: Result order is the same as input node order
func ParallelE[N any](
	ctx context.Context,
	l *logger.Logger,
	nodes []N,
	fn func(ctx context.Context, node N) (*RunResultDetails, error),
	opts ...ParallelOption,
) ([]*RunResultDetails, bool, error) {
	options := ParallelOptions{retryOpts: DefaultSSHRetryOpts}
	for _, opt := range opts {
		opt(&options)
	}

	count := len(nodes)
	if options.concurrency == 0 || options.concurrency > count {
		options.concurrency = count
	}
	if config.MaxConcurrency > 0 && options.concurrency > config.MaxConcurrency {
		options.concurrency = config.MaxConcurrency
	}

	completed := make(chan ParallelResult, count)
	errorChannel := make(chan error)

	var wg sync.WaitGroup
	wg.Add(count)

	groupCtx, groupCancel := context.WithCancel(ctx)
	defer groupCancel()
	var index int
	startNext := func() {
		// If we needed to react to a context cancellation here we would need to
		// nest this goroutine in another one and select on the groupCtx. However,
		// since anything intensive here is a command over ssh, and we are threading
		// the context through, a cancellation will be handled by the command itself.
		go func(i int) {
			defer wg.Done()
			// This is rarely expected to return an error, but we fail fast in case.
			// Command errors, which are far more common, will be contained within the result.
			res, err := runWithMaybeRetry(
				groupCtx, l, options.retryOpts, func(ctx context.Context) (*RunResultDetails, error) { return fn(ctx, nodes[i]) },
			)
			if err != nil {
				errorChannel <- err
				return
			}
			// The index is captured here so that we can maintain the order of results.
			completed <- ParallelResult{Index: i, RunResultDetails: res}
		}(index)
		index++
	}

	for index < options.concurrency {
		startNext()
	}

	go func() {
		defer close(completed)
		defer close(errorChannel)
		wg.Wait()
	}()

	var writer ui.Writer
	out := l.Stdout
	if options.display == "" {
		out = io.Discard
	}

	var ticker *time.Ticker
	if !config.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
		fmt.Fprintf(out, "%s", options.display)
		if l.File != nil {
			fmt.Fprintf(out, "\n")
		}
	}
	defer ticker.Stop()

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	var hasError bool
	n := 0
	results := make([]*RunResultDetails, count)
	for done := false; !done; {
		select {
		case <-ticker.C:
			if config.Quiet && l.File == nil {
				fmt.Fprintf(out, ".")
			}
		case r, ok := <-completed:
			if ok {
				results[r.Index] = r.RunResultDetails
				n++
				if r.Err != nil { // Command error
					hasError = true
					if !options.waitOnFail {
						groupCancel()
						return results, true, nil
					}
				}
				if index < count {
					startNext()
				}
			}
			done = !ok
		case err, ok := <-errorChannel: // Roachprod error
			if ok {
				groupCancel()
				return nil, false, err
			}
		}

		if !config.Quiet && l.File == nil {
			fmt.Fprint(&writer, options.display)
			fmt.Fprintf(&writer, " %d/%d", n, count)
			if !done {
				fmt.Fprintf(&writer, " %s", spinner[spinnerIdx%len(spinner)])
			}
			fmt.Fprintf(&writer, "\n")
			_ = writer.Flush(out)
			spinnerIdx++
		}
	}

	if config.Quiet && l.File == nil {
		fmt.Fprintf(out, "\n")
	}

	return results, hasError, nil
}

// Parallel runs a user-defined function across the nodes in the
// cluster. If any of the commands fail, Parallel will log each failure
// and return an error.
//
// A user may also pass in a RunRetryOpts to control how the function is retried
// in the case of a failure.
//
// See ParallelE for more information.
func Parallel[N any](
	ctx context.Context,
	l *logger.Logger,
	nodes []N,
	fn func(ctx context.Context, node N) (*RunResultDetails, error),
	opts ...ParallelOption,
) error {
	results, hasError, err := ParallelE[N](ctx, l, nodes, fn, opts...)
	// `err` is an unexpected roachprod error, which we return immediately.
	if err != nil {
		return err
	}

	// `hasError` is true if any of the commands returned an error.
	if hasError {
		for _, r := range results {
			// Since this function is potentially returning a single error despite
			// having run on multiple nodes, we combine all the errors into a single
			// error.
			if r != nil && r.Err != nil {
				err = errors.CombineErrors(err, r.Err)
				l.Errorf("%d: %+v: %s", r.Node, r.Err, r.CombinedOut)
			}
		}
		return errors.Wrap(err, "one or more parallel execution failure(s)")
	}
	return nil
}
