// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// retryHook is a function that is called when a specific retry event occurs.
// The result and error from the last attempt are provided as arguments. If
// an error is returned, the retry loop will terminate with that error.
type retryHook[R any] func(*DynamicRetryState, R, error) error

type RetryHooks[R any] struct {
	OnAttempt   retryHook[R]
	OnContinue  retryHook[R]
	OnFastFail  retryHook[R]
	OnReset     retryHook[R]
	OnNewPolicy retryHook[R]
}

// DynamicRetry is a retry mechanism that allows for dynamic switching between
// retry policies based on the outcome of each retry attempt. It supports
// multiple named retry.Options, and a user-defined strategy that determines the
// next action after each attempt. The supported actions are:
//   - Continue retrying with the current policy.
//   - Fast fail with a specific error.
//   - Reset the current retry policy.
//   - Transition to a new policy.
//
// Additionally, hooks can be registered to execute custom logic between
// strategy transitions.
//
// NB: Dynamic retry is not thread-safe and should be used by a single goroutine
// at a time.
type DynamicRetry[Result any] struct {
	DynamicRetryState
	sv            *settings.Values
	policies      map[string]Options
	retryStrategy func(*DynamicRetryState, Result, error) RetryTransition
	hooks         RetryHooks[Result]
}

// DynamicRetryState contains internal state information and gives a read-only
// view into the current status of the DynamicRetry.
type DynamicRetryState struct {
	retry          Retry
	currPolicy     string
	lastTransition RetryTransition
	attempts       int
}

type TransitionKind int

const (
	TransitionContinue TransitionKind = iota
	TransitionFastFail
	TransitionReset
	TransitionNewPolicy
)

// RetryTransition is a marker interface for the different kinds of retry
// transitions that can be returned by the retry strategy.
type RetryTransition interface {
	transitionKind() TransitionKind
}

// Continue is a transition that indicates the retry loop should continue
// retrying.
func Continue() continueT {
	return continueT{}
}

// FastFail is a transition that indicates the retry loop should terminate
// immediately with the provided error.
func FastFail(err error) fastFail {
	return fastFail{err: err}
}

// Reset is a transition that indicates the retry loop should reset the current
// retry policy.
func Reset() reset {
	return reset{}
}

// NewPolicy is a transition that indicates the retry loop should switch to a
// new policy.
func NewPolicy(policy string) newPolicy {
	return newPolicy{policy: policy}
}

func NewDynamicRetry[Result any](sv *settings.Values) *DynamicRetry[Result] {
	return &DynamicRetry[Result]{
		sv: sv,
		// The default retry evaluator simply continues retrying.
		retryStrategy: func(
			_ *DynamicRetryState, _ Result, _ error,
		) RetryTransition {
			return Continue()
		},
	}
}

// WithStrategy sets the retry strategy for the DynamicRetry. The strategy
// function is called after each failed attempt and determines the next action to
// take (continue retrying, fail permanently, fast fail, reset, or transition to
// a new policy). You can assume that err is non-nil when the strategy is called.
func (d *DynamicRetry[Result]) WithStrategy(
	strategy func(*DynamicRetryState, Result, error) RetryTransition,
) *DynamicRetry[Result] {
	d.retryStrategy = strategy
	return d
}

// WithPolicies sets the mapping of names to retry.Options for the DynamicRetry.
func (d *DynamicRetry[Result]) WithPolicies(policies map[string]Options) *DynamicRetry[Result] {
	d.policies = policies
	return d
}

// Do executes the provided function with retries according to the dynamic
// retry strategy. The initial policy is provided as an argument. The function
// returns the result of the operation or an error if all retries are exhausted
// or a permanent failure is decided.
func (d *DynamicRetry[Result]) Do(
	operation string, do func() (Result, error), initialPolicy string,
) (Result, error) {
	return d.DoCtx(
		context.Background(),
		operation,
		func(_ context.Context) (Result, error) {
			return do()
		},
		initialPolicy,
	)
}

// DoCtx is the same as Do, except the operation function receives a context.
func (d *DynamicRetry[Result]) DoCtx(
	ctx context.Context,
	operation string,
	do func(ctx context.Context) (Result, error),
	initialPolicy string,
) (result Result, err error) {
	d.currPolicy = initialPolicy
	policy, ok := d.policies[initialPolicy]
	if !ok {
		return result, errors.AssertionFailedf(
			"unknown initial retry policy %v in operation '%s'", initialPolicy, operation,
		)
	}
	d.attempts = -1 // Attempts are zero based and we increment at the start of the loop.
	for d.retry = StartWithCtx(ctx, policy); d.retry.Next(); {
		result, err = do(ctx)
		d.attempts++
		if d.hooks.OnAttempt != nil {
			if hookErr := d.hooks.OnAttempt(&d.DynamicRetryState, result, err); hookErr != nil {
				return result, hookErr
			}
		}
		if err == nil {
			return result, nil
		}
		transition := d.retryStrategy(&d.DynamicRetryState, result, err)
		d.lastTransition = transition

		switch transition := transition.(type) {
		case fastFail:
			if d.hooks.OnFastFail != nil {
				if hookErr := d.hooks.OnFastFail(&d.DynamicRetryState, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			return result, transition.err
		case reset:
			log.Dev.Infof(ctx, "operation '%s' resetting retry after error: %+v", operation, err)
			d.retry.Reset()
			if d.hooks.OnReset != nil {
				if hookErr := d.hooks.OnReset(&d.DynamicRetryState, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case newPolicy:
			d.currPolicy = transition.policy
			newPolicy, ok := d.policies[d.currPolicy]
			if !ok {
				return result, errors.AssertionFailedf(
					"unknown retry policy %v in operation '%s'", d.currPolicy, operation,
				)
			}
			log.Dev.Infof(
				ctx, "operation '%s' changing to retry policy %s after error: %+v",
				operation, d.currPolicy, err,
			)
			d.retry = StartWithCtx(ctx, newPolicy)
			d.retry.Reset()
			if d.hooks.OnNewPolicy != nil {
				if hookErr := d.hooks.OnNewPolicy(&d.DynamicRetryState, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case continueT:
		default:
			logcrash.ReportOrPanic(
				ctx, d.sv,
				"unknown retry transition %T in operation '%s'",
				transition, operation,
			)
		}
		if d.hooks.OnContinue != nil {
			if hookErr := d.hooks.OnContinue(&d.DynamicRetryState, result, err); hookErr != nil {
				return result, hookErr
			}
		}
		log.Dev.Warningf(ctx, "retrying operation '%s' after error: %+v", operation, err)
	}

	// If retry loop exited due to context cancellation, return context error
	// instead.
	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	log.Dev.Warningf(
		ctx, "operation '%s' exhausted all retries in policy %v with final error: %+v",
		operation,
		d.currPolicy,
		err,
	)
	return result, err
}

// OnAttempt sets the hook to be called after each retry attempt, regardless of
// the outcome.
func (d *DynamicRetry[Result]) OnAttempt(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnAttempt = hook
	return d
}

// OnContinue sets the hook to be called after each retry attempt that results
// in a continuation.
func (d *DynamicRetry[Result]) OnContinue(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnContinue = hook
	return d
}

// OnFastFail sets the hook to be called when the retry loop decides to fast
// fail.
func (d *DynamicRetry[Result]) OnFastFail(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnFastFail = hook
	return d
}

// OnReset sets the hook to be called when the retry loop resets the retry
// policy. The hook is called after the policy has been reset.
func (d *DynamicRetry[Result]) OnReset(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnReset = hook
	return d
}

// OnNewPolicy sets the hook to be called when the retry loop transitions to a
// new policy. The hook is called after the policy has been changed and the new
// retry policy has been applied.
func (d *DynamicRetry[Result]) OnNewPolicy(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnNewPolicy = hook
	return d
}

// CurrentRetryAttempt returns the number of the last attempt within the current
// retry policy (0-based index).
func (d *DynamicRetryState) CurrentRetryAttempt() int {
	return d.retry.CurrentAttempt()
}

// Policy returns the current policy of the DynamicRetry.
func (d *DynamicRetryState) Policy() string {
	return d.currPolicy
}

// LastTransition returns the kind of the last transition.
func (d *DynamicRetryState) LastTransition() TransitionKind {
	if d.lastTransition == nil {
		return TransitionContinue
	}
	return d.lastTransition.transitionKind()
}

// CurrentAttempt returns the number of the last attempt across all policies
// (0-based index).
func (d *DynamicRetryState) CurrentAttempt() int {
	return d.attempts
}

type continueT struct{}

func (continueT) transitionKind() TransitionKind {
	return TransitionContinue
}

type fastFail struct {
	err error
}

func (fastFail) transitionKind() TransitionKind {
	return TransitionFastFail
}

type reset struct{}

func (reset) transitionKind() TransitionKind {
	return TransitionReset
}

type newPolicy struct {
	policy string
}

func (newPolicy) transitionKind() TransitionKind {
	return TransitionNewPolicy
}
