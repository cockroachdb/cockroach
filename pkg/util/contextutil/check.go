// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package contextutil

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CheckContext is returned by WithCheck() and allows the Checker to
// be set lazily in the case where it cannot be constructed until
// after the context has been created.
type CheckContext interface {
	context.Context
	SetChecker(Checker)
}

// Checker is a function that returns a sampled value and whether or not
// that value was acceptable.  It is valid to return nil for the sampled
// value if retrieving them from a TooManyChecksFailedError isn't
// required.
type Checker func() (interface{}, bool)

type checkContext struct {
	context.Context
	checker     Checker
	err         error
	errHook     func(error *TooManyChecksFailedError) error
	maxFailures int
	period      time.Duration
}

// Err implements the context.Context interface.  We use this to inject
// our own error into a caller.
func (c *checkContext) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.Context.Err()
}

// SetChecker allows the checker to be replaced or set lazily when
// the Checker cannot be created until after the context is created.
func (c *checkContext) SetChecker(checker Checker) {
	c.checker = checker
}

var defaultCheckContext = checkContext{
	errHook:     func(e *TooManyChecksFailedError) error { return e },
	maxFailures: 1,
	period:      time.Minute,
}

// An Option is used to configure the Context created by WithCheck.
type Option func(*checkContext)

// OptionChecker sets the Checker that will be used by WithCheck.
func OptionChecker(checker Checker) Option {
	if checker == nil {
		panic("checker must not be nil")
	}
	return func(ctx *checkContext) {
		ctx.checker = checker
	}
}

// OptionErrorHook allows a custom error to be injected into the context
// when it has been canceled.
func OptionErrorHook(hook func(error *TooManyChecksFailedError) error) Option {
	if hook == nil {
		panic("hook must not be nil")
	}
	return func(ctx *checkContext) {
		ctx.errHook = hook
	}
}

// OptionMaxFailures sets the maximum number of consecutive check
// failures before the context will be canceled.
func OptionMaxFailures(max int) Option {
	if max <= 0 {
		panic("maxFailures must be > 0")
	}
	return func(ctx *checkContext) {
		ctx.maxFailures = max
	}
}

// OptionPeriod sets how often the check function will be called.
func OptionPeriod(period time.Duration) Option {
	if period <= 0 {
		panic("period must be > 0")
	}
	return func(ctx *checkContext) {
		ctx.period = period
	}
}

// TooManyChecksFailedError provides more details about why a context
// was canceled.
type TooManyChecksFailedError struct {
	// The most recent samples.
	Samples []interface{}
}

// Error implements the error interface.
func (e *TooManyChecksFailedError) Error() string {
	return fmt.Sprintf("too many checks failed: %s", e.Samples)
}

// WithCheck constructs a child context that will be canceled
// when the given checker fails more than maxFailures consecutive
// checks.
//
// Callers must either provide OptionChecker() or call
// CheckContext.SetChecker() before the check runs, or a warning
// will be logged and the check will be considered to have failed.
//
// If the check fails for maxFailures consecutive periods, the returned
// Context will be canceled and its Err() will return a
// TooManyChecksFailedError, which can be examined for the offending
// sampled values.
func WithCheck(parent context.Context, options ...Option) (CheckContext, context.CancelFunc) {
	// Use baked-in cancellation mechanism and start up a goroutine
	// to call the user-provided check logic.  We wrap the cancellable
	// context in our own shim so that we can inject a custom error
	// if the context is canceled due to check failures.
	cancelCtx, cancel := context.WithCancel(parent)

	ctx := defaultCheckContext
	ctx.Context = cancelCtx
	for _, opt := range options {
		opt(&ctx)
	}

	go func() {
		samples := make([]interface{}, 0, ctx.maxFailures)
		// Start with a full collection of health credits.
		health := ctx.maxFailures

		// Timer to wake up for each check period.
		timer := timeutil.NewTimer()
		defer timer.Stop()

		for {
			timer.Reset(ctx.period)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// Set per docs on Timer.
				timer.Read = true

				// Invoke user logic.
				var sample interface{}
				var ok bool
				checker := ctx.checker
				if checker == nil {
					log.Warning(&ctx, "no checker has been configured")
					ok = false
				} else {
					sample, ok = checker()
				}

				// Collect only maxFailures samples.
				if len(samples) == ctx.maxFailures {
					samples = append(samples[1:len(samples):ctx.maxFailures], sample)
				} else {
					samples = append(samples, sample)
				}

				switch {
				case !ok:
					// Decrement health and cancel if we're out of credits.
					health--
					if health == 0 {
						ctx.err = ctx.errHook(&TooManyChecksFailedError{samples})
						cancel()
						return
					}
				case health < ctx.maxFailures:
					// Increment health if it's below the cap.
					health++
				}
			}
		}
	}()

	return &ctx, cancel
}
