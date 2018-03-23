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

package rate

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var defaultConfig = config{
	checkPeriod: time.Minute,
	minRate:     NewRate(1, time.Minute),
	maxFailures: 1,
}

type config struct {
	context.Context
	checkPeriod time.Duration
	maxFailures int
	minRate     Rate
	rater       Rater
	warnFn      func()
}

// SetRater implements the StallContext interface.
func (c *config) SetRater(r Rater) {
	c.rater = r
}

// A StallDetectedError will be returned from a context created by
// WithStallPrevention when a stall has been detected.
type StallDetectedError struct {
	Actual Rate
	Min    Rate
}

func (e *StallDetectedError) Error() string {
	return fmt.Sprintf("work has stalled: %s < %s", e.Actual, e.Min)
}

// Option is used to configure the behavior of WithStallPrevention.
type Option func(config *config)

// OptionCheckPeriod controls how often the current rate is checked
// before canceling the context.  This value should be chosen relative
// to the expected duration of the task.
func OptionCheckPeriod(period time.Duration) Option {
	return func(cfg *config) {
		cfg.checkPeriod = period
	}
}

// OptionMaxFailures controls how many consecutive check periods the
// observed rate is below the minimum rate before the context will
// be canceled.
func OptionMaxFailures(maxFailures int) Option {
	return func(cfg *config) {
		cfg.maxFailures = maxFailures
	}
}

// OptionMinRate sets the minimum rate at which events must occur.
func OptionMinRate(rate Rate) Option {
	return func(cfg *config) {
		cfg.minRate = rate
	}
}

// OptionRater sets the source of the Rates that will be used.
func OptionRater(rater Rater) Option {
	return func(cfg *config) {
		cfg.rater = rater
	}
}

// OptionWarnFunction provides a callback to be invoked when the rate
// has dropped below the minimum (e.g. to provide extra logging).
func OptionWarnFunction(fn func()) Option {
	return func(cfg *config) {
		cfg.warnFn = fn
	}
}

// StallContext is a specialization of Context that allows the Rater
// to be set lazily.  This is used when the context must be created
// before it's possible to calculate rate information.
type StallContext interface {
	context.Context
	// SetRater is used to set or replace the Rater associated with the
	// context.  This may be called safely at any time.
	SetRater(r Rater)
}

// WithStallPrevention constructs a child context that will be canceled
// when a rate drops below a certain threshold.
//
// Note that in order to enable cancellation, a Rater must be provided,
// either through OptionRater() or by calling StallContext.SetRater().
// The latter way is useful when the context must be created before
// it is possible to determine a rate (e.g. cancelable IO operations).
//
// If the check period expires without a Rater having been provided,
// a warning will be logged and the rate will be considered to be 0.
func WithStallPrevention(
	parent context.Context, options ...Option,
) (StallContext, context.CancelFunc) {
	cfg := defaultConfig
	for _, opt := range options {
		opt(&cfg)
	}
	var cancel context.CancelFunc
	cfg.Context, cancel = contextutil.WithCheck(parent,
		// Just do the rate comparison.
		contextutil.OptionChecker(
			func() (interface{}, bool) {
				maybeRater := cfg.rater
				if maybeRater == nil {
					log.Warning(parent, "no Rater has been set")
					return 0, false
				}
				rate := maybeRater.Rate()
				ok := rate >= cfg.minRate
				if !ok && cfg.warnFn != nil {
					cfg.warnFn()
				}
				return rate, ok
			}),
		// Inject a more obvious error type.
		contextutil.OptionErrorHook(func(error *contextutil.TooManyChecksFailedError) error {
			ret := &StallDetectedError{
				Actual: error.Samples[len(error.Samples)-1].(Rate),
				Min:    cfg.minRate,
			}
			return ret
		}),
		// Passthrough options.
		contextutil.OptionMaxFailures(cfg.maxFailures),
		contextutil.OptionPeriod(cfg.checkPeriod),
	)

	return &cfg, cancel
}
