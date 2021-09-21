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
// from BreakerV2.Err(), i.e. `errors.Is(err, ErrBreakerOpen()) can be
// used to check whether an error originated from some BreakerV2.
func ErrBreakerOpen() error {
	return errBreakerOpen
}

type structErr struct {
	error
}

// BreakerV2 is a circuit breaker. Before the operation that the breaker
// protects is carried out, BreakerV2.Err() is checked. If this returns an
// error, the operation should not be attempted. If no error is returned, the
// operation may be attempted. If it fails, the resulting error should be passed
// to BreakerV2.Report() which, depending on the options with which the breaker
// was created, will trip the breaker. When a breaker is tripped, an
// asynchronous probe is launched that determines when to reset the breaker,
// and until then all calls to `Err()` return an error.
type BreakerV2 struct {
	opts unsafe.Pointer // *OptionsV2

	err     unsafe.Pointer // atomic
	probing int32          // atomic
}

// OptionsV2 are the arguments to NewBreaker. All fields are required.
type OptionsV2 struct {
	Name         string
	ShouldTrip   func(err error) error
	AsyncProbe   func(report func(error), done func())
	EventHandler EventHandler
}

// NewBreakerV2 instantiates a new circuit breaker.
func NewBreakerV2(opts OptionsV2) *BreakerV2 {
	if opts.EventHandler == nil {
		opts.EventHandler = EventLogBridge{
			Logf: func(redact.SafeString, ...interface{}) {},
		}
	}
	return &BreakerV2{
		opts: unsafe.Pointer(&opts),
	}
}

// Opts returns the active options.
func (b *BreakerV2) Opts() OptionsV2 {
	return *(*OptionsV2)(atomic.LoadPointer(&b.opts))
}

// Reconfigure swaps the active options for the supplied replacement.
func (b *BreakerV2) Reconfigure(opts OptionsV2) {
	atomic.SwapPointer(&b.opts, unsafe.Pointer(&opts))
}

// Err returns an informative error if the breaker is tripped. If the breaker
// is not tripped, no error is returned.
//
// If an error is returned, `errors.Is(err, ErrBreakerOpen())` will be
// true.
func (b *BreakerV2) Err() error {
	if wErr := (*structErr)(atomic.LoadPointer(&b.err)); wErr != nil {
		// The breaker is open.
		b.maybeTriggerProbe()
		return wErr.error
	}
	// The happy case.
	return nil
}

type breakerErrorMark BreakerV2

func (b *breakerErrorMark) Error() string {
	return fmt.Sprintf("originated at breaker %s (%p)", (*BreakerV2)(b).Opts().Name, b)
}

// Report reports a (non-nil) error to the breaker. Depending on the
// breaker's ShouldTrip configuration, this may trip the breaker.
func (b *BreakerV2) Report(err error) {
	if err == nil {
		// Defense in depth: you're not supposed to pass a nil error in,
		// but if it happens it's simply ignored.
		return
	}
	// Give shouldTrip a chance to massage the error.
	markErr := (*breakerErrorMark)(b)
	if errors.Is(err, markErr) {
		// The input error originated from this breaker. This shouldn't
		// happen but since it is happening, we want to avoid creating
		// longer and longer error chains below.
		return
	}
	err = b.Opts().ShouldTrip(err)
	if err == nil {
		// Should not trip.
		return
	}

	// Update the error. This may overwrite an earlier error, which is fine:
	// We want the breaker to reflect a recent error as this is more helpful.
	storeErr := errors.Mark(errors.Mark(err, ErrBreakerOpen()), markErr)
	var prevErr error
	if prev := (*structErr)(atomic.SwapPointer(&b.err, unsafe.Pointer(&structErr{error: storeErr}))); prev != nil {
		prevErr = prev.error
	}
	b.Opts().EventHandler.OnTrip(b, prevErr, storeErr)
	b.maybeTriggerProbe()
}

func (b *BreakerV2) maybeTriggerProbe() {
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
func (b *BreakerV2) Reset() {
	b.Opts().EventHandler.OnReset(b)
	atomic.StorePointer(&b.err, unsafe.Pointer(nil))
}
