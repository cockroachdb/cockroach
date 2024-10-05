// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// FailOption will decide if the operation will wait for all nodes to finish
	// when encountering a command error, or fail immediately. The default
	// behaviour depends on the context in which RunOptions are used. It is
	// recommended to check the documentation of the function you are using to see
	// what the default behaviour is.
	FailOption FailOption

	// These are private to roachprod
	Nodes       Nodes
	Concurrency int
	Display     string
}

type FailOption int8

const (
	// FailDefault will use the default behaviour of the function you are using.
	FailDefault FailOption = iota
	// FailFast will exit immediately on the first error, in which case the slice
	// of ParallelResults will only contain the one error result.
	FailFast
	// FailSlow will wait for all nodes to finish when encountering a command
	// error on any node.
	FailSlow
)

func OnNodes(nodes Nodes) RunOptions {
	return RunOptions{
		Nodes: nodes,
	}
}

func (r RunOptions) WithRetryOpts(retryOpts retry.Options) RunOptions {
	r.RetryOptions = &retryOpts
	return r
}

func (r RunOptions) WithRetryDisabled() RunOptions {
	r.RetryOptions = nil
	return r
}

func (r RunOptions) WithRetryFn(fn func(*RunResultDetails) bool) RunOptions {
	r.ShouldRetryFn = fn
	return r
}

func (r RunOptions) WithFailSlow() RunOptions {
	r.FailOption = FailSlow
	return r
}

func (r RunOptions) WithFailFast() RunOptions {
	r.FailOption = FailFast
	return r
}

func (r RunOptions) WithConcurrency(concurrency int) RunOptions {
	r.Concurrency = concurrency
	return r
}

func (r RunOptions) WithDisplay(display string) RunOptions {
	r.Display = display
	return r
}
