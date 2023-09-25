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
	RetryOptions  *retry.Options
	ShouldRetryFn func(*RunResultDetails) bool
	// FailFast will cause the Parallel function to wait for all nodes to
	// finish when encountering a command error on any node. The default
	// behaviour is to exit immediately on the first error, in which case the
	// slice of ParallelResults will only contain the one error result.
	FailFast bool
	// These are private to roachprod
	Concurrency int
	Display     string
}

type RunOption func(runOpts *RunOptions)

func WithRetryOpts(retryOpts *retry.Options) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.RetryOptions = retryOpts
	}
}
func WithRetryFn(fn func(*RunResultDetails) bool) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.ShouldRetryFn = fn
	}
}

func WithFailFast(failFast bool) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.FailFast = failFast
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
