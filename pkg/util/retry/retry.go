// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	InitialBackoff time.Duration // Default retry backoff interval
	MaxBackoff     time.Duration // Maximum retry backoff interval
	Multiplier     float64       // Default backoff constant
	// Randomize the backoff interval by constant. Set to -1 to disable.
	RandomizationFactor float64
	Closer              <-chan struct{} // Optionally end retry loop channel close
	// Maximum number of retries; attempts = MaxRetries + 1. (0 for infinite)
	MaxRetries int
	// MaxDuration is the maximum duration for which the retry loop will make
	// attempts. Once the deadline has elapsed, the loop will stop attempting
	// retries.
	// The loop will run for at least one iteration. (0 for infinite)
	MaxDuration time.Duration
	// PreemptivelyCancel indicates whether the retry loop should cancel itself if
	// it determines that the next backoff would exceed the MaxDuration.
	PreemptivelyCancel bool
	// Clock is used to control the time source for the retry loop. Intended for
	// testing purposes. Should be nil in production code.
	Clock timeutil.TimeSource
}

// Retry implements the public methods necessary to control an exponential-
// backoff retry loop.
type Retry struct {
	opts           Options
	ctx            context.Context
	currentAttempt int
	isReset        bool
	deadline       time.Time // Deadline for the retry loop if MaxDuration is set.

	// Testing hook that is called when the retry loop is waiting for the backoff.
	// If no max duration is set, deadline will be zero. Passes in the actual wait
	// time between each retry attempt (accounting for max duration).
	// Set here instead of options to allow Options to be compared.
	backingOffHook func(backoff time.Duration)
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
	} else if opts.RandomizationFactor < 0 {
		opts.RandomizationFactor = 0 // Disable randomization.
	}
	if opts.Multiplier == 0 {
		opts.Multiplier = 2
	}
	if opts.Clock == nil {
		opts.Clock = timeutil.DefaultTimeSource{}
	}

	var r Retry
	r.opts = opts
	r.ctx = ctx
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
	case <-r.ctx.Done():
		// When the context was canceled, you can't keep going.
	default:
		r.mustReset()
	}
}

func (r *Retry) mustReset() {
	r.currentAttempt = 0
	r.isReset = true
	if r.opts.MaxDuration != 0 {
		r.deadline = r.opts.Clock.Now().Add(r.opts.MaxDuration)
	} else {
		r.deadline = time.Time{}
	}
}

// retryIn returns the duration to wait before the next retry attempt.
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

// calcDurationScopedBackoff calculates the duration to wait before the next
// attempt, taking into account the MaxDuration option. It returns the computed
// backoff duration, the actual wait duration (if the backoff exceeds the max
// duration), and a boolean indicating whether the retry should be attempted.
func (r Retry) calcDurationScopedBackoff() (time.Duration, time.Duration, bool) {
	backoff := r.retryIn()
	actualWait := backoff
	shouldAttempt := true
	if r.opts.MaxDuration != 0 && !r.opts.Clock.Now().Add(backoff).Before(r.deadline) {
		// If the backoff would exceed the deadline, we return the remaining time
		// until the deadline instead.
		shouldAttempt = false
		actualWait = max(r.deadline.Sub(r.opts.Clock.Now()), 0)
	}
	return backoff, actualWait, shouldAttempt
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
	if r.retryLimitReached() {
		return false
	}

	backoff, actualWait, shouldAttempt := r.calcDurationScopedBackoff()

	if !shouldAttempt && r.opts.PreemptivelyCancel {
		log.VEventf(
			r.ctx, 2 /* level */, "preemptively canceling retry loop as backoff would exceed MaxDuration",
		)
		return false
	}

	timer := r.opts.Clock.NewTimer()
	timer.Reset(actualWait)
	timerCh := timer.Ch()
	defer timer.Stop()

	log.VEventfDepth(r.ctx, 1 /* depth */, 2 /* level */, "will retry after %s", backoff)

	if r.backingOffHook != nil {
		r.backingOffHook(actualWait)
	}

	select {
	case <-r.opts.Closer:
		return false
	case <-r.ctx.Done():
		return false
	case <-timerCh:
		if shouldAttempt {
			r.currentAttempt++
		}
		return shouldAttempt
	}
}

func (r *Retry) retryLimitReached() bool {
	return (r.opts.MaxRetries > 0 && r.currentAttempt >= r.opts.MaxRetries) ||
		(r.opts.MaxDuration > 0 && !r.opts.Clock.Now().Before(r.deadline))
}

// immediateCh creates a channel that is immediately written to with the
// provided value and then closed.
func immediateCh(v bool) chan bool {
	c := make(chan bool, 1)
	c <- v
	close(c)
	return c
}

var closedCh = immediateCh(false)

// NextCh returns a channel which will receive when the next retry
// interval has expired. If the received value is true, it indicates a retry
// should be made. If the received value is false, it indicates that no retry
// should be made.
// Note: This does not respect the Closer or context cancellation and it is the
// caller's responsibility to manage the lifecycle.
func (r *Retry) NextCh() <-chan bool {
	if r.isReset {
		r.isReset = false
		return immediateCh(true)
	}
	if r.retryLimitReached() {
		return closedCh
	}

	backoff, actualWait, shouldAttempt := r.calcDurationScopedBackoff()

	if !shouldAttempt && r.opts.PreemptivelyCancel {
		log.VEventf(
			r.ctx, 2 /* level */, "preemptively canceling retry loop as backoff would exceed MaxDuration",
		)
		return closedCh
	}

	timer := r.opts.Clock.NewTimer()
	timer.Reset(actualWait)
	timerCh := timer.Ch()

	if r.backingOffHook != nil {
		r.backingOffHook(actualWait)
	}

	log.VEventfDepth(r.ctx, 1 /* depth */, 2 /* level */, "will retry after %s", backoff)

	ch := make(chan bool, 1)
	if shouldAttempt {
		r.currentAttempt++
	}
	go func() {
		defer timer.Stop()
		<-timerCh
		ch <- shouldAttempt
	}()

	return ch
}

// CurrentAttempt returns the current attempt (0-based index)
func (r *Retry) CurrentAttempt() int {
	return r.currentAttempt
}

// Do invokes the closure according to the retry options until it returns
// success or no more retries are possible. Always returns an error unless the
// return is prompted by a successful invocation of `fn`.
func (opts Options) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	var err error
	for r := StartWithCtx(ctx, opts); r.Next(); {
		err = fn(ctx)
		if err == nil {
			return nil
		}
	}
	if err == nil {
		return errors.AssertionFailedf("never invoked function in Do")
	}
	return err
}

// DoWithRetryable invokes the closure according to the retry options until it
// returns success or a non-retryable error. Always returns an error unless the
// return is prompted by a successful invocation of `fn`.
func (opts Options) DoWithRetryable(
	ctx context.Context, fn func(ctx context.Context) (retryable bool, err error),
) error {
	var err error
	for r := StartWithCtx(ctx, opts); r.Next(); {
		var retryable bool
		retryable, err = fn(ctx)
		if err == nil {
			return nil
		}
		if !retryable {
			return err
		}
	}
	if err == nil {
		return errors.AssertionFailedf("never invoked function in DoWithRetryable")
	}
	return err
}

// WithMaxAttempts is a helper that runs fn N times and collects the last err.
// The function will terminate early if the provided context is canceled, but it
// guarantees that fn will run at least once.
func WithMaxAttempts(ctx context.Context, opts Options, n int, fn func() error) error {
	if n == 1 {
		return fn()
	}
	if n <= 0 {
		return errors.New("can't ask for zero attempts")
	}
	opts.MaxRetries = n - 1 // >= 1
	return opts.Do(ctx, func(ctx context.Context) error {
		return fn()
	})
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
