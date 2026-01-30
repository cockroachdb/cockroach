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
type retryHook[R any] func(DynamicRetry[R], R, error) error

type RetryHooks[R any] struct {
	OnContinue retryHook[R]
	OnFastFail retryHook[R]
	OnReset    retryHook[R]
	OnNewState retryHook[R]
}

// DynamicRetry is a retry mechanism that allows for dynamic switching between
// retry policies based on the outcome of each retry attempt. It supports
// multiple retry.Options mapped to different states, and a user-defined
// strategy that determines the next action after each attempt. The supported
// actions are:
//   - Continue retrying with the current policy.
//   - Fast fail with a specific error.
//   - Reset the current retry policy.
//   - Transition to a new state with its associated retry policy.
//
// Additionally, hooks can be registered to execute custom logic between state
// transitions.
type DynamicRetry[Result any] struct {
	operation      string
	sv             *settings.Values
	retry          Retry
	currState      string
	lastTransition RetryTransition
	states         map[string]Options
	retryStrategy  func(DynamicRetry[Result], Result, error) RetryTransition
	hooks          RetryHooks[Result]
	attempts       int
}

type TransitionKind int

const (
	TransitionContinue TransitionKind = iota
	TransitionFastFail
	TransitionReset
	TransitionNewState
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

// NewState is a transition that indicates the retry loop should switch to a
// new state with its associated retry policy.
func NewState(state string) newState {
	return newState{state: state}
}

func NewDynamicRetry[Result any](operation string, sv *settings.Values) *DynamicRetry[Result] {
	return &DynamicRetry[Result]{
		operation: operation,
		sv:        sv,
		// The default retry evaluator simply continues retrying.
		retryStrategy: func(
			_ DynamicRetry[Result], _ Result, _ error,
		) RetryTransition {
			return Continue()
		},
	}
}

// WithStrategy sets the retry strategy for the DynamicRetry. The strategy
// function is called after each failed attempt and determines the next action to
// take (continue retrying, fail permanently, fast fail, reset, or transition to
// a new state). You can assume that err is non-nil when the strategy is called.
// If the strategy returns a non-nil error, it will replace the original error
// from the oepration.
func (d *DynamicRetry[Result]) WithStrategy(
	strategy func(DynamicRetry[Result], Result, error) RetryTransition,
) *DynamicRetry[Result] {
	d.retryStrategy = strategy
	return d
}

// WithStates sets the mapping of states to retry options for the DynamicRetry.
func (d *DynamicRetry[Result]) WithStates(states map[string]Options) *DynamicRetry[Result] {
	d.states = states
	return d
}

// Do executes the provided function with retries according to the dynamic
// retry strategy. The initial state is provided as an argument. The function
// returns the result of the operation or an error if all retries are exhausted
// or a permanent failure is decided.
func (d *DynamicRetry[Result]) Do(do func() (Result, error), initialState string) (Result, error) {
	return d.DoCtx(context.Background(), func(_ context.Context) (Result, error) {
		return do()
	}, initialState)
}

// DoCtx is the same as Do, except the operation function receives a context.
func (d *DynamicRetry[Result]) DoCtx(
	ctx context.Context, do func(ctx context.Context) (Result, error), initialState string,
) (result Result, err error) {
	d.attempts = 0
	d.currState = initialState
	initialPolicy, ok := d.states[initialState]
	if !ok {
		return result, errors.AssertionFailedf(
			"unknown initial retry state %v in operation '%s'", initialState, d.operation,
		)
	}
	for d.retry = StartWithCtx(ctx, initialPolicy); d.retry.Next(); {
		result, err = do(ctx)
		d.attempts++
		if err == nil {
			return result, nil
		}
		transition := d.retryStrategy(*d, result, err)
		d.lastTransition = transition

		switch transition := transition.(type) {
		case fastFail:
			if d.hooks.OnFastFail != nil {
				if hookErr := d.hooks.OnFastFail(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			return result, transition.err
		case reset:
			log.Dev.Infof(ctx, "operation '%s' resetting retry after error: %+v", d.operation, err)
			d.retry.Reset()
			if d.hooks.OnReset != nil {
				if hookErr := d.hooks.OnReset(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case newState:
			d.currState = transition.state
			newPolicy, ok := d.states[d.currState]
			if !ok {
				return result, errors.AssertionFailedf(
					"unknown retry state %v in operation '%s'", d.currState, d.operation,
				)
			}
			log.Dev.Infof(ctx, "operation '%s' changing retry policy after error: %+v", d.operation, err)
			d.retry = StartWithCtx(ctx, newPolicy)
			d.retry.Reset()
			if d.hooks.OnNewState != nil {
				if hookErr := d.hooks.OnNewState(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case continueT:
		default:
			logcrash.ReportOrPanic(
				ctx, d.sv,
				"unknown retry transition %d in operation '%s'",
				transition, d.operation,
			)
		}
		if d.hooks.OnContinue != nil {
			if hookErr := d.hooks.OnContinue(*d, result, err); hookErr != nil {
				return result, hookErr
			}
		}
		log.Dev.Warningf(ctx, "retrying operation '%s' after error: %+v", d.operation, err)
	}

	// If retry loop exited due to context cancellation, return context error
	// instead.
	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	log.Dev.Warningf(
		ctx, "operation '%s' exhausted all retries in state %v with final error: %+v",
		d.operation,
		d.currState,
		err,
	)
	return result, err
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

// OnNewState sets the hook to be called when the retry loop transitions to a
// new state. The hook is called after the state has been changed and the new
// retry policy has been applied.
func (d *DynamicRetry[Result]) OnNewState(hook retryHook[Result]) *DynamicRetry[Result] {
	d.hooks.OnNewState = hook
	return d
}

// Retry returns the underlying retry mechanism used by the DynamicRetry.
func (d DynamicRetry[Result]) Retry() Retry {
	return d.retry
}

// State returns the current state of the DynamicRetry.
func (d DynamicRetry[Result]) State() string {
	return d.currState
}

// LastTransition returns the kind of the last transition.
func (d DynamicRetry[Result]) LastTransition() TransitionKind {
	return d.lastTransition.transitionKind()
}

// Returns the number of attempts made so far on the last call of `Do`.
func (d DynamicRetry[Result]) Attempts() int {
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

type newState struct {
	state string
}

func (newState) transitionKind() TransitionKind {
	return TransitionNewState
}
