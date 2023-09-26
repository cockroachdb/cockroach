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
	// RetryOptions controls the retry behaviour when encountering an error.
	RetryOptions *retry.Options
	// ShouldRetryFn can be specified by a caller if they would like to be notified of an error that may
	// be retried. A retry will only be performed if it returns true.
	ShouldRetryFn func(*RunResultDetails) bool
	// FailFast will cause the Parallel function to return immediately when encountering
	// an error on any node, otherwise it will wait to collect all results.
	FailFast bool
	// Concurrency controls across how many nodes a given command runs at any time
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
