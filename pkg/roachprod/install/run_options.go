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
	// ExpanderConfig configures the behaviour of the roachprod expander
	// during a run.
	ExpanderConfig ExpanderConfig
	// LogExpandedCmd will log the expanded command if it differs from the original.
	LogExpandedCmd bool

	// These are private to roachprod
	Nodes       Nodes
	Concurrency int
	Display     string
}

type FailOption int8

const (
	// FailDefault will use the default behaviour of the function it's being used
	// with. For instance, note that `RunWithDetails` will use FailSlow, while
	// `Run` and `Parallel` will use FailFast when FailDefault is specified in the
	// RunOptions.
	FailDefault FailOption = iota
	// FailFast will exit immediately on the first error, in which case the slice
	// of ParallelResults will only contain the one error result.
	FailFast
	// FailSlow will wait for all nodes to finish when encountering a command
	// error on any node.
	FailSlow
)

// AlwaysTrue is a should retry predicate function that always returns true to
// indicate that the operation is always retryable no matter what the previous
// result was.
var AlwaysTrue = func(res *RunResultDetails) bool { return true }

func DefaultRunOptions() RunOptions {
	return RunOptions{
		RetryOptions:  DefaultRetryOpt,
		ShouldRetryFn: DefaultShouldRetryFn,
		FailOption:    FailDefault,
	}
}

func WithNodes(nodes Nodes) RunOptions {
	r := DefaultRunOptions()
	r.Nodes = nodes
	return r
}

func (r RunOptions) WithRetryOpts(retryOpts retry.Options) RunOptions {
	r.RetryOptions = &retryOpts
	return r
}

func (r RunOptions) WithRetryDisabled() RunOptions {
	r.RetryOptions = nil
	return r
}

func (r RunOptions) WithShouldRetryFn(fn func(*RunResultDetails) bool) RunOptions {
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

func (r RunOptions) WithExpanderConfig(cfg ExpanderConfig) RunOptions {
	r.ExpanderConfig = cfg
	return r
}

func (r RunOptions) WithLogExpandedCommand() RunOptions {
	r.LogExpandedCmd = true
	return r
}
