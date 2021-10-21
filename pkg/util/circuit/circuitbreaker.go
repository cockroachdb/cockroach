// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package circuit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var errBreakerOpen = errors.New("breaker open")

// ErrBreakerOpen is a reference error that matches the errors returned
// from Breaker.Err(), i.e. `errors.Is(err, ErrBreakerOpen()) can be
// used to check whether an error originated from some Breaker.
func ErrBreakerOpen() error {
	return errBreakerOpen
}

type errAndCh struct {
	err error
	ch  chan struct{}
}

// Breaker is a circuit breaker. Before the operation that the breaker
// protects is carried out, Breaker.Err() is checked. If this returns an
// error, the operation should not be attempted. If no error is returned, the
// operation may be attempted. If it fails, the resulting error should be passed
// to Breaker.Report() which, depending on the options with which the breaker
// was created, will trip the breaker. When a breaker is tripped, an
// asynchronous probe is launched that determines when to reset the breaker,
// and until then all calls to `Err()` return an error.
type Breaker struct {
	opts unsafe.Pointer // *Options

	errAndCh unsafe.Pointer // atomic
	probing  int32          // atomic
}

// Options are the arguments to NewBreaker. All fields are required.
type Options struct {
	Name         string
	ShouldTrip   func(err error) error
	AsyncProbe   func(report func(error), done func())
	EventHandler EventHandler
}

// NewBreaker instantiates a new circuit breaker.
func NewBreaker(opts Options) *Breaker {
	if opts.EventHandler == nil {
		opts.EventHandler = EventLogBridge{
			Logf: func(redact.SafeString, ...interface{}) {},
		}
	}
	br := &Breaker{
		opts: unsafe.Pointer(&opts),
	}
	br.resetWithoutEvent() // init errAndCh
	return br
}

// Opts returns the active options.
func (b *Breaker) Opts() Options {
	return *(*Options)(atomic.LoadPointer(&b.opts))
}

// Reconfigure swaps the active options for the supplied replacement.
func (b *Breaker) Reconfigure(opts Options) {
	atomic.SwapPointer(&b.opts, unsafe.Pointer(&opts))
}

// Err returns an informative error if the breaker is tripped. If the breaker
// is not tripped, no error is returned.
//
// If an error is returned, `errors.Is(err, ErrBreakerOpen())` will be
// true.
func (b *Breaker) Err() error {
	if err := (*errAndCh)(atomic.LoadPointer(&b.errAndCh)).err; err != nil {
		b.maybeTriggerProbe()
		return err
	}
	return nil
}

func (b *Breaker) definitelyErr() error {
	if err := b.Err(); err != nil {
		return err
	}
	return ErrBreakerOpen()
}

// Signal returns a channel that will be closed once the breaker trips
// and a function returning a pertinent error.
//
// This can be used to abort long-running operations when the circuit
// breaker detects a problem half-way through. The second argument should
// be called when the channel is closed. It is guaranteed to return an
// error.
// Note that `b.Err()` should not be relied upon to return an error after
// the channel closed, as the breaker may rapidly have untripped again.
func (b *Breaker) Signal() (chan struct{}, func() error) {
	return (*errAndCh)(atomic.LoadPointer(&b.errAndCh)).ch, b.definitelyErr
}

type breakerErrorMark Breaker

func (b *breakerErrorMark) Error() string {
	return fmt.Sprintf("originated at breaker %s (%p)", (*Breaker)(b).Opts().Name, b)
}

var closedCh = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Report reports a (non-nil) error to the breaker. Depending on the breaker's
// ShouldTrip configuration, this may trip the breaker. If so (or the breaker is
// already tripped), the error that would be returned from subsequent calls to
// Err() is returned.
func (b *Breaker) Report(err error) error {
	if err == nil {
		// Defense in depth: you're not supposed to pass a nil error in,
		// but if it happens it's simply ignored.
		return b.Err()
	}
	// Give shouldTrip a chance to massage the error.
	markErr := (*breakerErrorMark)(b)
	if errors.Is(err, markErr) {
		// The input error originated from this breaker. This shouldn't
		// happen but since it is happening, we want to avoid creating
		// longer and longer error chains below.
		return b.Err()
	}
	err = b.Opts().ShouldTrip(err)
	if err == nil {
		// Should not trip. Return current state of breaker.
		return b.Err()
	}

	// Update the error. This may overwrite an earlier error, which is fine:
	// We want the breaker to reflect a recent error as this is more helpful.
	storeErr := errors.Mark(errors.Mark(err, ErrBreakerOpen()), markErr)
	prevErrAndCh := (*errAndCh)(atomic.SwapPointer(&b.errAndCh, unsafe.Pointer(&errAndCh{
		err: storeErr,
		ch:  closedCh,
	})))
	// Note that `prevErrAndCh.err` is immutable, and determines whether `ch` was
	// closed on creation. If it is still open, this is the only place that will ever
	// try to close it due to the atomic swap above.
	if prevErrAndCh.err == nil {
		close(prevErrAndCh.ch)
	}

	b.Opts().EventHandler.OnTrip(b, prevErrAndCh.err, storeErr)
	b.maybeTriggerProbe()
	return storeErr
}

func (b *Breaker) maybeTriggerProbe() {
	if !atomic.CompareAndSwapInt32(&b.probing, 0, 1) {
		// A probe is already running.
		return
	}
	b.Opts().EventHandler.OnProbeLaunched(b)
	var once sync.Once
	b.Opts().AsyncProbe(
		func(err error) {
			if err != nil {
				b.Report(err)
			} else {
				b.Reset()
			}
		},
		func() {
			// Avoid potential problems when probe calls done() multiple times.
			// It shouldn't do that, but mistakes happen.
			once.Do(func() {
				b.Opts().EventHandler.OnProbeDone(b)
				atomic.StoreInt32(&b.probing, 0)
			})
		})
}

// Reset resets (i.e. un-trips, if it was tripped) the breaker.
// Outside of testing, there should be no reason to call this
// as it is the probe's job to reset the breaker if appropriate.
func (b *Breaker) Reset() {
	b.Opts().EventHandler.OnReset(b)
	b.resetWithoutEvent()
}

func (b *Breaker) resetWithoutEvent() {
	atomic.StorePointer(&b.errAndCh, unsafe.Pointer(&errAndCh{
		ch: make(chan struct{}),
	}))
}
