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
	*RetryOpts
	// WaitOnFail will cause the Parallel function to wait for all nodes to
	// finish when encountering a command error on any node. The default
	// behaviour is to exit immediately on the first error, in which case the
	// slice of ParallelResults will only contain the one error result.
	WaitOnFail bool
	// These are private to roachprod
	Concurrency int
	Display     string
}

type RunOption func(runOpts *RunOptions)

func WithRetryOpts(retryOpts *RetryOpts) RunOption {
	return func(runOpts *RunOptions) {
		runOpts.RetryOpts = retryOpts
	}
}

func WithWaitOnFail() RunOption {
	return func(runOpts *RunOptions) {
		runOpts.WaitOnFail = true
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

type RetryOpts struct {
	retry.Options
	ShouldRetryFn func(*RunResultDetails) bool
}

func NewRetryOpts(retryOpts retry.Options, shouldRetryFn func(*RunResultDetails) bool) *RetryOpts {
	return &RetryOpts{
		Options:       retryOpts,
		ShouldRetryFn: shouldRetryFn,
	}
}
