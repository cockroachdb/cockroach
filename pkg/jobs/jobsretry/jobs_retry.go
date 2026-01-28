// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsretry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type RetryTransition int

const (
	Continue RetryTransition = iota
	FailPermanently
	FastFail
	Reset
	NewState
)

// retryHook is a function that is called when a specific retry event occurs. If
// an error is returned, the retry loop will terminate with that error.
type retryHook[R any, S comparable] func(DynamicRetry[R, S], R, error) error

type RetryHooks[R any, S comparable] struct {
	OnContinue        retryHook[R, S]
	OnFailPermanently retryHook[R, S]
	OnFastFail        retryHook[R, S]
	OnReset           retryHook[R, S]
	OnNewState        retryHook[R, S]
}

type DynamicRetry[Result any, State comparable] struct {
	ctx            context.Context
	operation      string
	sv             *settings.Values
	retry          retry.Retry
	currState      State
	lastTransition RetryTransition
	states         map[State]retry.Options
	retryStrategy  func(DynamicRetry[Result, State], Result, error) (RetryTransition, State, error)
	hooks          RetryHooks[Result, State]
}

func NewDynamicRetry[Result any, State comparable](
	ctx context.Context, operation string, sv *settings.Values,
) *DynamicRetry[Result, State] {
	return &DynamicRetry[Result, State]{
		ctx:       ctx,
		operation: operation,
		sv:        sv,
		// The default retry evaluator simply continues retrying.
		retryStrategy: func(
			_ DynamicRetry[Result, State], _ Result, _ error,
		) (RetryTransition, State, error) {
			var zero State
			return Continue, zero, nil
		},
	}
}

// WithStrategy sets the retry strategy for the DynamicRetry. The strategy
// function is called after each failed attempt and determines the next action to
// take (continue retrying, fail permanently, fast fail, reset, or transition to
// a new state). You can assume that err is non-nil when the strategy is called.
// If the strategy returns a non-nil error, it will replace the original error
// from the oepration.
func (d *DynamicRetry[Result, State]) WithStrategy(
	strategy func(DynamicRetry[Result, State], Result, error) (RetryTransition, State, error),
) *DynamicRetry[Result, State] {
	d.retryStrategy = strategy
	return d
}

// WithStates sets the mapping of states to retry options for the DynamicRetry.
func (d *DynamicRetry[Result, State]) WithStates(
	states map[State]retry.Options,
) *DynamicRetry[Result, State] {
	d.states = states
	return d
}

// Do executes the provided function with retries according to the dynamic
// retry strategy. The initial state is provided as an argument. The function
// returns the result of the operation or an error if all retries are exhausted
// or a permanent failure is decided.
func (d *DynamicRetry[Result, State]) Do(
	do func(ctx context.Context) (Result, error), initialState State,
) (result Result, err error) {
	d.currState = initialState
	initialPolicy, ok := d.states[initialState]
	if !ok {
		return result, errors.AssertionFailedf(
			"unknown initial retry state %v in operation '%s'", initialState, d.operation,
		)
	}
	for d.retry = retry.StartWithCtx(d.ctx, initialPolicy); d.retry.Next(); {
		result, err = do(d.ctx)
		if err == nil {
			return result, nil
		}
		transition, newState, newErr := d.retryStrategy(*d, result, err)
		if newErr != nil {
			err = newErr
		}
		d.lastTransition = transition

		switch transition {
		case FailPermanently:
			if d.hooks.OnFailPermanently != nil {
				if hookErr := d.hooks.OnFailPermanently(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			return result, jobs.MarkAsPermanentJobError(
				errors.Wrapf(err, "operation '%s' failed permanently", d.operation),
			)
		case FastFail:
			if d.hooks.OnFastFail != nil {
				if hookErr := d.hooks.OnFastFail(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			return result, err
		case Reset:
			log.Dev.Infof(d.ctx, "operation '%s' resetting retry after error: %+v", d.operation, err)
			d.retry.Reset()
			if d.hooks.OnReset != nil {
				if hookErr := d.hooks.OnReset(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case NewState:
			d.currState = newState
			newPolicy, ok := d.states[d.currState]
			if !ok {
				return result, errors.AssertionFailedf(
					"unknown retry state %v in operation '%s'", d.currState, d.operation,
				)
			}
			log.Dev.Infof(d.ctx, "operation '%s' changing retry policy after error: %+v", d.operation, err)
			d.retry = retry.StartWithCtx(d.ctx, newPolicy)
			d.retry.Reset()
			if d.hooks.OnNewState != nil {
				if hookErr := d.hooks.OnNewState(*d, result, err); hookErr != nil {
					return result, hookErr
				}
			}
			continue
		case Continue:
		default:
			logcrash.ReportOrPanic(
				d.ctx, d.sv,
				"unknown retry transition %d in operation '%s'",
				transition, d.operation,
			)
		}
		if d.hooks.OnContinue != nil {
			if hookErr := d.hooks.OnContinue(*d, result, err); hookErr != nil {
				return result, hookErr
			}
		}
		log.Dev.Warningf(d.ctx, "retrying operation '%s' after error: %+v", d.operation, err)
	}
	log.Dev.Warningf(
		d.ctx, "operation '%s' exhausted all retries in state %v with final error: %+v",
		d.operation,
		d.currState,
		err,
	)
	return result, err
}

// OnContinue sets the hook to be called after each retry attempt that results
// in a continuation.
func (d *DynamicRetry[Result, State]) OnContinue(
	hook retryHook[Result, State],
) *DynamicRetry[Result, State] {
	d.hooks.OnContinue = hook
	return d
}

// OnFailPermanently sets the hook to be called when the retry loop decides to
// fail permanently.
func (d *DynamicRetry[Result, State]) OnFailPermanently(
	hook retryHook[Result, State],
) *DynamicRetry[Result, State] {
	d.hooks.OnFailPermanently = hook
	return d
}

// OnFastFail sets the hook to be called when the retry loop decides to fast
// fail.
func (d *DynamicRetry[Result, State]) OnFastFail(
	hook retryHook[Result, State],
) *DynamicRetry[Result, State] {
	d.hooks.OnFastFail = hook
	return d
}

// OnReset sets the hook to be called when the retry loop resets the retry
// policy. The hook is called after the policy has been reset.
func (d *DynamicRetry[Result, State]) OnReset(
	hook retryHook[Result, State],
) *DynamicRetry[Result, State] {
	d.hooks.OnReset = hook
	return d
}

// OnNewState sets the hook to be called when the retry loop transitions to a
// new state. The hook is called after the state has been changed and the new
// retry policy has been applied.
func (d *DynamicRetry[Result, State]) OnNewState(
	hook retryHook[Result, State],
) *DynamicRetry[Result, State] {
	d.hooks.OnNewState = hook
	return d
}

// Retry returns the underlying retry mechanism used by the DynamicRetry.
func (d DynamicRetry[Result, State]) Retry() retry.Retry {
	return d.retry
}

// State returns the current state of the DynamicRetry.
func (d DynamicRetry[Result, State]) State() State {
	return d.currState
}

// LastTransition returns the last retry transition that occurred.
func (d DynamicRetry[Result, State]) LastTransition() RetryTransition {
	return d.lastTransition
}
