// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package retry

import (
	"math"
	"math/rand"
	"time"

	"golang.org/x/net/context"
)

// Options provides reusable configuration of Retry objects.
type Options struct {
	InitialBackoff      time.Duration   // Default retry backoff interval
	MaxBackoff          time.Duration   // Maximum retry backoff interval
	Multiplier          float64         // Default backoff constant
	MaxRetries          int             // Maximum number of attempts (0 for infinite)
	RandomizationFactor float64         // Randomize the backoff interval by constant
	Closer              <-chan struct{} // Optionally end retry loop channel close.
}

// Retry implements the public methods necessary to control an exponential-
// backoff retry loop.
type Retry struct {
	opts           Options
	ctxDoneChan    <-chan struct{}
	currentAttempt int
	isReset        bool
}

// Start returns a new Retry initialized to some default values. The Retry can
// then be used in an exponential-backoff retry loop.
func Start(opts Options) Retry {
	return StartWithCtx(nil, opts)
}

// StartWithCtx returns a new Retry initialized to some default values. The
// Retry can then be used in an exponential-backoff retry loop. If the provided
// context is canceled (see Context.Done), the retry loop ends early.
func StartWithCtx(ctx context.Context, opts Options) Retry {
	if opts.InitialBackoff == 0 {
		opts.InitialBackoff = 50 * time.Millisecond
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 2 * time.Second
	}
	if opts.RandomizationFactor == 0 {
		opts.RandomizationFactor = 0.15
	}
	if opts.Multiplier == 0 {
		opts.Multiplier = 2
	}

	r := Retry{opts: opts}
	if ctx != nil {
		r.ctxDoneChan = ctx.Done()
	}
	r.Reset()
	return r
}

// Reset resets the Retry to its initial state, meaning that the next call to
// Next will return true immediately and subsequent calls will behave as if
// they had followed the very first attempt (i.e. their backoffs will be
// short).
func (r *Retry) Reset() {
	select {
	case <-r.opts.Closer:
		// When the closer has fired, you can't keep going.
		return
	case <-r.ctxDoneChan:
		// When the context was canceled, you can't keep going.
		return
	default:
	}
	r.currentAttempt = 0
	r.isReset = true
}

// CurrentAttempt it is zero initially and increases with each call to Next()
// which does not immediately follow a Reset().
func (r *Retry) CurrentAttempt() int {
	return r.currentAttempt
}

func (r Retry) retryIn() time.Duration {
	backoff := float64(r.opts.InitialBackoff) * math.Pow(r.opts.Multiplier, float64(r.currentAttempt))
	if maxBackoff := float64(r.opts.MaxBackoff); backoff > maxBackoff {
		backoff = maxBackoff
	}

	var delta = r.opts.RandomizationFactor * backoff
	// Get a random value from the range [backoff - delta, backoff + delta].
	// The formula used below has a +1 because time.Duration is an int64, and the
	// conversion floors the float64.
	return time.Duration(backoff - delta + rand.Float64()*(2*delta+1))
}

// Next returns whether the retry loop should continue, and blocks for the
// appropriate length of time before yielding back to the caller. If a stopper
// is present, Next will eagerly return false when the stopper is stopped.
func (r *Retry) Next() bool {
	if r.isReset {
		r.isReset = false
		return true
	}

	if r.opts.MaxRetries > 0 && r.currentAttempt == r.opts.MaxRetries {
		return false
	}

	// Wait before retry.
	select {
	case <-time.After(r.retryIn()):
		r.currentAttempt++
		return true
	case <-r.opts.Closer:
		return false
	case <-r.ctxDoneChan:
		return false
	}
}
