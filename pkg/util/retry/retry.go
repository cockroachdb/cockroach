// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package retry

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Options provides reusable configuration of Retry objects.
type Options struct {
	InitialBackoff      time.Duration   // Default retry backoff interval
	MaxBackoff          time.Duration   // Maximum retry backoff interval
	Multiplier          float64         // Default backoff constant
	MaxRetries          int             // Maximum number of attempts (0 for infinite)
	RandomizationFactor float64         // Randomize the backoff interval by constant
	Closer              <-chan struct{} // Optionally end retry loop channel close
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
	return StartWithCtx(context.Background(), opts)
}

// StartWithCtx returns a new Retry initialized to some default values. The
// Retry can then be used in an exponential-backoff retry loop. If the provided
// context is canceled (see Context.Done), the retry loop ends early, but will
// always run at least once.
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

	var r Retry
	r.opts = opts
	r.ctxDoneChan = ctx.Done()
	r.mustReset()
	return r
}

// Reset resets the Retry to its initial state, meaning that the next call to
// Next will return true immediately and subsequent calls will behave as if they
// had followed the very first attempt (i.e. their backoffs will be short). The
// exception to this is if the provided Closer has fired or context has been
// canceled, in which case subsequent calls to Next will still return false
// immediately.
func (r *Retry) Reset() {
	select {
	case <-r.opts.Closer:
		// When the closer has fired, you can't keep going.
	case <-r.ctxDoneChan:
		// When the context was canceled, you can't keep going.
	default:
		r.mustReset()
	}
}

func (r *Retry) mustReset() {
	r.currentAttempt = 0
	r.isReset = true
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
// appropriate length of time before yielding back to the caller.
//
// Next is guaranteed to return true on its first call. As such, a retry loop
// can be thought of as implementing do-while semantics (i.e. always running at
// least once). Otherwide, if a context and/or closer is present, Next will
// return false if the context is canceled and/or the closer fires while the
// method is waiting.
func (r *Retry) Next() bool {
	if r.isReset {
		r.isReset = false
		return true
	}

	if r.opts.MaxRetries > 0 && r.currentAttempt >= r.opts.MaxRetries {
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

// closedC is returned from Retry.NextCh whenever a retry
// can begin immediately.
var closedC = func() chan time.Time {
	c := make(chan time.Time)
	close(c)
	return c
}()

// NextCh returns a channel which will receive when the next retry
// interval has expired.
func (r *Retry) NextCh() <-chan time.Time {
	if r.isReset {
		r.isReset = false
		return closedC
	}
	r.currentAttempt++
	if r.opts.MaxRetries > 0 && r.currentAttempt > r.opts.MaxRetries {
		return nil
	}
	return time.After(r.retryIn())
}

// WithMaxAttempts is a helper that runs fn N times and collects the last err.
// The function will terminate early if the provided context is canceled, but it
// guarantees that fn will run at least once.
func WithMaxAttempts(ctx context.Context, opts Options, n int, fn func() error) error {
	if n <= 0 {
		return errors.Errorf("max attempts should not be 0 or below, got: %d", n)
	}

	opts.MaxRetries = n - 1
	var err error
	for r := StartWithCtx(ctx, opts); r.Next(); {
		err = fn()
		if err == nil {
			return nil
		}
	}
	if err == nil {
		log.Fatal(ctx, "never ran function in WithMaxAttempts")
	}
	return err
}

// ForDuration will retry the given function until it either returns
// without error, or the given duration has elapsed. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at the specified duration.
//
// This function is DEPRECATED! Please use one of the other functions in this
// package that takes context cancellation into account.
//
// TODO(benesch): remove this function and port its callers to a context-
// sensitive API.
func ForDuration(duration time.Duration, fn func() error) error {
	deadline := timeutil.Now().Add(duration)
	var lastErr error
	for wait := time.Duration(1); timeutil.Now().Before(deadline); wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if wait > time.Second {
			wait = time.Second
		}
		time.Sleep(wait)
	}
	return lastErr
}
