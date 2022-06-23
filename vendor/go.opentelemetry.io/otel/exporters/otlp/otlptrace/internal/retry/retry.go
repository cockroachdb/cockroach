// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// DefaultConfig are the recommended defaults to use.
var DefaultConfig = Config{
	Enabled:         true,
	InitialInterval: 5 * time.Second,
	MaxInterval:     30 * time.Second,
	MaxElapsedTime:  time.Minute,
}

// Config defines configuration for retrying batches in case of export failure
// using an exponential backoff.
type Config struct {
	// Enabled indicates whether to not retry sending batches in case of
	// export failure.
	Enabled bool
	// InitialInterval the time to wait after the first failure before
	// retrying.
	InitialInterval time.Duration
	// MaxInterval is the upper bound on backoff interval. Once this value is
	// reached the delay between consecutive retries will always be
	// `MaxInterval`.
	MaxInterval time.Duration
	// MaxElapsedTime is the maximum amount of time (including retries) spent
	// trying to send a request/batch.  Once this value is reached, the data
	// is discarded.
	MaxElapsedTime time.Duration
}

// RequestFunc wraps a request with retry logic.
type RequestFunc func(context.Context, func(context.Context) error) error

// EvaluateFunc returns if an error is retry-able and if an explicit throttle
// duration should be honored that was included in the error.
type EvaluateFunc func(error) (bool, time.Duration)

func (c Config) RequestFunc(evaluate EvaluateFunc) RequestFunc {
	if !c.Enabled {
		return func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		}
	}

	// Do not use NewExponentialBackOff since it calls Reset and the code here
	// must call Reset after changing the InitialInterval (this saves an
	// unnecessary call to Now).
	b := &backoff.ExponentialBackOff{
		InitialInterval:     c.InitialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         c.MaxInterval,
		MaxElapsedTime:      c.MaxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	return func(ctx context.Context, fn func(context.Context) error) error {
		for {
			err := fn(ctx)
			if err == nil {
				return nil
			}

			retryable, throttle := evaluate(err)
			if !retryable {
				return err
			}

			bOff := b.NextBackOff()
			if bOff == backoff.Stop {
				return fmt.Errorf("max retry time elapsed: %w", err)
			}

			// Wait for the greater of the backoff or throttle delay.
			var delay time.Duration
			if bOff > throttle {
				delay = bOff
			} else {
				elapsed := b.GetElapsedTime()
				if b.MaxElapsedTime != 0 && elapsed+throttle > b.MaxElapsedTime {
					return fmt.Errorf("max retry time would elapse: %w", err)
				}
				delay = throttle
			}

			if err := waitFunc(ctx, delay); err != nil {
				return err
			}
		}
	}
}

// Allow override for testing.
var waitFunc = wait

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	return nil
}
