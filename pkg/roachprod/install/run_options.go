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

import "github.com/cockroachdb/cockroach/pkg/util/retry"

type RunOptions struct {
	// RetryOptions are the retry options
	RetryOptions *retry.Options
	// ShouldRetryFn is only applicable when RetryOptions is not nil
	// and specifies a function to be called in the case that a retry
	// is about to be performed. A user can provide a function which, for
	// example, inspects the previous result's output, and decides not to
	// retry any further, by returning false.
	ShouldRetryFn func(*RunResultDetails) bool
	// FailSlow will cause the Parallel function to wait for all nodes to
	// finish when encountering a command error on any node. The default
	// behaviour is to exit immediately on the first error, in which case the
	// slice of ParallelResults will only contain the one error result.
	// Named as such to make clear that this is not enabled by default.
	FailSlow bool
	// These are private to roachprod
	Concurrency int
	Display     string
}

type RunOption func(runOpts *RunOptions)

// WithRetryOpts specifies retry behaviour
func WithRetryOpts(retryOpts retry.Options) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.RetryOptions = &retryOpts
	}
}

// WithRetryDisabled disables retries for a command,
// and is a friendly equivalent to `WithRetryOpts(nil)`
func WithRetryDisabled() RunOption {
	return func(runOpts *RunOptions) {
		runOpts.RetryOptions = nil
	}
}

// WithRetryFn is only applicable when retryOpts is not nil
func WithRetryFn(fn func(*RunResultDetails) bool) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.ShouldRetryFn = fn
	}
}

func WithFailSlow(failSlow bool) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.FailSlow = failSlow
	}
}

func WithConcurrency(concurrency int) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.Concurrency = concurrency
	}
}

func WithDisplay(display string) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.Display = display
	}
}
