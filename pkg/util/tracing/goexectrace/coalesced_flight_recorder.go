// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/exp/trace"
)

type CoalescedFlightRecorder struct {
	opts CoalescedFlightRecorderOptions

	fr *trace.FlightRecorder

	// Locking notes: regMu < dumpMu (if both held, regMu acquired first).
	regMu struct {
		syncutil.Mutex
		refcount int64

		shutdownTimer *time.Timer
	}
	dumpMu struct {
		syncutil.Mutex
		pending *future.AwaitableFuture[error]
		name    string

		stop bool // if set, call stop after finishing dump
	}
}

type CoalescedFlightRecorderOptions struct {
	UniqueFileName func() string
	CreateFile     func(string) (io.Writer, error)
	IdleDuration   time.Duration
	Warningf       func(string, ...interface{})
}

func NewCoalescedFlightRecorder(opts CoalescedFlightRecorderOptions) *CoalescedFlightRecorder {
	cfr := &CoalescedFlightRecorder{
		fr:   trace.NewFlightRecorder(),
		opts: opts,
	}
	cfr.regMu.shutdownTimer = time.AfterFunc(24*time.Hour, cfr.stop)
	cfr.regMu.shutdownTimer.Stop()
	return cfr
}

type FlightRecorderHandle interface {
	Dump() (string, future.AwaitableFuture[error])
	Release()
}

func (cfr *CoalescedFlightRecorder) New() FlightRecorderHandle {
	cfr.regMu.Lock()
	defer cfr.regMu.Unlock()
	cfr.regMu.refcount++
	if cfr.regMu.refcount == 1 {
		cfr.regMu.shutdownTimer.Stop()
	}
	if !cfr.fr.Enabled() {
		if err := cfr.fr.Start(); err != nil {
			cfr.opts.Warningf("starting coalesced flight recorder: %s", err)
		}
	}
	return (*flightRecorderHandle)(cfr)
}

type flightRecorderHandle CoalescedFlightRecorder

func (h *flightRecorderHandle) Release() {
	h.regMu.Lock()
	defer h.regMu.Unlock()
	h.regMu.refcount--
	resetTimer := h.regMu.refcount == 0

	if resetTimer {
		h.regMu.shutdownTimer.Reset(h.opts.IdleDuration)
	}
}

func (cfr *CoalescedFlightRecorder) stop() {
	// Hold both mutexes. Holding regMu makes sure that we properly serialize
	// with any concurrent attempts to start the flight recorder.
	// Holding dumpMu makes sure that the call to `fr.Stop` never
	// blocks on an in-flight `fr.WriteTo`. If one is inflight, we instead
	// delegate to the dumping goroutine to call us again once the dump is done.
	cfr.regMu.Lock()
	defer cfr.regMu.Unlock()
	cfr.dumpMu.Lock()
	defer cfr.dumpMu.Unlock()
	if cfr.dumpMu.pending != nil {
		cfr.dumpMu.stop = true
		return
	}

	// Consider this example:
	// - 0 registrations
	// - timer expires
	// - stop fires (hasn't acquired mutexes yet)
	// - registration comes in, resets timer
	// - stop must realize to not shut down the flight recorder: refcount check
	//   below achieves this.
	// - second invocation of stop (which may also occur, see example below is
	//   harmless, bails out in the same way).
	//
	// Consider also:
	// - 0 registrations
	// - timer expires
	// - stop fires (hasn't acquired mutexes yet)
	// - registration comes in, resets timer, quickly unregisters again (so
	//   refcount is once again zero)
	// - above timer reset may queue a second invocation of stop for execution
	// - first stop executes, stops flight recorder
	// - second stop executes, bails since flight recorder already disabled.
	if cfr.regMu.refcount != 0 || !cfr.fr.Enabled() {
		// time.AfterFunc may reinvoke the callback multiple times (if the timer
		// is reset while the function invocation is inflight). This won't happen
		// concurrently since we're holding locks, but we may execute multiple
		// times.

		return
	}

	if err := cfr.fr.Stop(); err != nil {
		cfr.opts.Warningf("stopping coalesced flight recorder: %s", err)
	}
}

func (h *flightRecorderHandle) Dump() (string, future.AwaitableFuture[error]) {
	var launch bool

	h.dumpMu.Lock()
	if h.dumpMu.pending == nil {
		f := future.MakeAwaitableFuture[error](&future.ErrorFuture{})
		h.dumpMu.pending = &f
		h.dumpMu.name = h.opts.UniqueFileName()
		launch = true
	}
	fut := *h.dumpMu.pending
	name := h.dumpMu.name
	h.dumpMu.Unlock()

	if launch {
		go (*CoalescedFlightRecorder)(h).dumpAsync(name, fut)
	}
	return name, fut
}

func (cfr *CoalescedFlightRecorder) dumpAsync(name string, fut future.AwaitableFuture[error]) {
	work := func(name string) error {
		w, err := cfr.opts.CreateFile(name)
		if err != nil {
			return err
		}
		wclose := func() error { return nil }
		if wc, ok := w.(io.Closer); ok {
			wclose = wc.Close
		}

		if _, err := cfr.fr.WriteTo(w); err != nil {
			_ = wclose()
			return err
		}
		return wclose()
	}
	err := work(name)
	fut.Set(err)
	cfr.dumpMu.Lock()
	stopping := cfr.dumpMu.stop
	cfr.dumpMu.pending, cfr.dumpMu.name, cfr.dumpMu.stop = nil, "", false
	cfr.dumpMu.Unlock()

	if stopping {
		cfr.stop()
	}
}
