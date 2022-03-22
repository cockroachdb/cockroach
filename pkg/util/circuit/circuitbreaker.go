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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Breaker is a circuit breaker. Before initiating an operation protected by the
// Breaker, Breaker.Signal should be called. This provides a channel and error
// getter that operate very similarly to context.Context's Done and Err methods.
// Err can be checked to fail-fast requests before initiating work and the
// channel can be used to abort an ongoing operation when the breaker trips (at
// which point Err returns a non-nil error).
//
// The Breaker trips when Report is called, which is typically the case when the
// operation is attempted and fails in a way that the caller believes will cause
// all other operations protected by the Breaker to fail as well. A tripped
// Breaker will launch asynchronous probes that reset the breaker as soon as
// possible.
type Breaker struct {
	mu struct {
		syncutil.RWMutex
		*Options // always replaced wholesale
		// errAndCh stores a channel and the error that should be returned when that
		// channel is closed. When an error is first reported, the error is set and
		// the channel is closed. On any subsequent errors (or reset), a new
		// &errAndCh{} is allocated (and its error set and channel closed). This
		// keeps the results of Signal() stable, i.e. the caller will get the error
		// that closed "their" channel, and in particular they are guaranteed to get
		// an error, while keeping Signal() allocation-free.
		//
		// This can be leaked out of the lock (read-only); to write `err` (and, to
		// keep things simple, to close ch), need the exclusive lock. See also the
		// comments inside of errAndCh.
		errAndCh *errAndCh

		probing bool
	}
}

// NewBreaker instantiates a new circuit breaker.
func NewBreaker(opts Options) *Breaker {
	br := &Breaker{}
	br.mu.errAndCh = br.newErrAndCh()
	br.Reconfigure(opts)
	return br
}

// Signal returns a channel that is closed once the breaker trips and a function
// (which may be invoked multiple times) returning a pertinent error. This is
// similar to context.Context's Done() and Err().
//
// The returned method will return the error that closed the channel (and nil
// before the channel is closed), even if the breaker has already un-tripped in
// the meantime. This means that Signal should be re-invoked before each attempt
// at carrying out an operation. Non-nil errors will always be derived from
// ErrBreakerOpen, i.e. errors.Is(err, ErrBreakerOpen) will be true.
//
// Signal is allocation-free and suitable for use in performance-sensitive code
// paths. See ExampleBreaker_Signal for a usage example.
func (b *Breaker) Signal() interface {
	Err() error
	C() <-chan struct{}
} {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// NB: we need to return errAndCh here, returning (errAndCh.C(), errAndCh.Err)
	// allocates.
	return b.mu.errAndCh
}

// HasMark returns whether the error has an error mark that is unique to this
// breaker. In other words, the error originated at this Breaker.
//
// TODO(tbg): I think this doesn't work as advertised. Two breakers on different
// systems might produce wire-identical `b.errMark()`s. We really want a wrapping
// error which we can then retrieve & check for pointer equality with `b`.
func (b *Breaker) HasMark(err error) bool {
	return errors.Is(err, b.errMark())
}

func (b *Breaker) errMark() error {
	return (*breakerErrorMark)(b)
}

// Report reports a (non-nil) error to the breaker. This will trip the Breaker.
func (b *Breaker) Report(err error) {
	if err == nil {
		// Defense in depth: you're not supposed to pass a nil error in,
		// but if it happens it's simply ignored.
		return
	}
	// Give shouldTrip a chance to massage the error.
	if b.HasMark(err) {
		// The input error originated from this breaker. This shouldn't
		// happen but since it is happening, we want to avoid creating
		// longer and longer error chains below.
		return
	}

	// Update the error. This may overwrite an earlier error, which is fine:
	// We want the breaker to reflect a recent error as this is more helpful.
	storeErr := errors.Mark(errors.Mark(err, ErrBreakerOpen), b.errMark())

	// When the Breaker first trips, we populate the error and close the channel.
	// When the error changes, we have to replace errAndCh wholesale (that's the
	// contract, we can't mutate err once it's not nil) so we make a new channel
	// that is then promptly closed.
	b.mu.Lock()
	prevErr := b.mu.errAndCh.err
	if prevErr != nil {
		b.mu.errAndCh = b.newErrAndCh()
	}
	// We get to write the error since we have exclusive access via b.mu.
	b.mu.errAndCh.err = storeErr
	close(b.mu.errAndCh.ch)
	b.mu.Unlock()

	opts := b.Opts()
	opts.EventHandler.OnTrip(b, prevErr, storeErr)
	if prevErr == nil {
		// If the breaker wasn't previously tripped, trigger the probe to give the
		// Breaker a shot at healing right away. If the breaker is already tripped,
		// we don't want to trigger another probe as the probe itself calls Report
		// and we don't want a self-perpetuating loop of probe invocations. Instead,
		// we only probe when clients are actively asking the Breaker for its
		// status, via Breaker.Signal.
		b.maybeTriggerProbe()
	}
}

// Reset resets (i.e. un-trips, if it was tripped) the breaker.
// Outside of testing, there should be no reason to call this
// as it is the probe's job to reset the breaker if appropriate.
func (b *Breaker) Reset() {
	b.Opts().EventHandler.OnReset(b)
	b.mu.Lock()
	defer b.mu.Unlock()
	// Avoid replacing errAndCh if it wasn't tripped. Otherwise,
	// clients waiting on it would never get cancelled even if the
	// breaker did trip in the future.
	if wasTripped := b.mu.errAndCh.err != nil; wasTripped {
		b.mu.errAndCh = b.newErrAndCh()
	}
}

// String returns the Breaker's name.
func (b *Breaker) String() string {
	return redact.StringWithoutMarkers(b)
}

// SafeFormat implements redact.SafeFormatter.
func (b *Breaker) SafeFormat(s interfaces.SafePrinter, _ rune) {
	s.Print(b.Opts().Name)
}

// Opts returns the active options.
func (b *Breaker) Opts() Options {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return *b.mu.Options
}

// Reconfigure swaps the active options for the supplied replacement. The breaker
// will not be reset.
func (b *Breaker) Reconfigure(opts Options) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.Options = &opts
}

func (b *Breaker) maybeTriggerProbe() {
	b.mu.Lock()
	if b.mu.probing || b.mu.errAndCh.err == nil {
		b.mu.Unlock()
		// A probe is already running or the breaker is not currently tripped. The
		// latter case can occur since maybeTriggerProbe is invoked from
		// errAndCh.Err(), where the `errAndCh` might be old. (Recall that errAndCh
		// is passed out to the client and provides a stable view of the breaker
		// state, while the current state may already be different). For example,
		// in the below code, a probe would be triggered an hour after the breaker
		// tripped:
		//
		//   sig := br.Signal()
		//   <-sig.C():
		//   time.Sleep(time.Hour)
		//   _ = sig.Err() // maybeTriggerProbe()
		return
	}
	b.mu.probing = true
	opts := *b.mu.Options // ok to leak out from under the lock
	b.mu.Unlock()

	opts.EventHandler.OnProbeLaunched(b)
	var once sync.Once
	opts.AsyncProbe(
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
				opts.EventHandler.OnProbeDone(b)
				b.mu.Lock()
				defer b.mu.Unlock()
				b.mu.probing = false
			})
		})
}

func (b *Breaker) newErrAndCh() *errAndCh {
	return &errAndCh{
		maybeTriggerProbe: b.maybeTriggerProbe,
		ch:                make(chan struct{}),
	}
}

type errAndCh struct {
	// maybeTriggerProbe is called when Err() returns non-nil. This indicates that
	// the Breaker is tripped and that there is a caller that has a vested
	// interest in the breaker trying to heal.
	maybeTriggerProbe func() // immutable
	ch                chan struct{}
	// INVARIANT: err can only be written once, immediately before closing ch
	// (i.e. writer needs to maintain this externally). It can only be read after
	// ch is closed (use `Err()`).
	err error
}

func (eac *errAndCh) C() <-chan struct{} {
	return eac.ch
}

func (eac *errAndCh) Err() error {
	select {
	case <-eac.ch:
		eac.maybeTriggerProbe()
		return eac.err
	default:
		return nil
	}
}

// ErrBreakerOpen is a reference error that matches the errors returned
// from Breaker.Err(), i.e. `errors.Is(err, ErrBreakerOpen) can be
// used to check whether an error originated from some Breaker.
var ErrBreakerOpen = errors.New("breaker open")

type breakerErrorMark Breaker

func (b *breakerErrorMark) Error() string {
	return fmt.Sprintf("originated at breaker %s", (*Breaker)(b).Opts().Name)
}
