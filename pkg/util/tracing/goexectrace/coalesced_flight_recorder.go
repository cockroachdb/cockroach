// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"io"
	"path/filepath"
	runtimetrace "runtime/trace"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/trace"
)

var errTraceInProgress = errors.New(
	"execution trace already being recorded")

type CoalescedFlightRecorder struct {
	opts CoalescedFlightRecorderOptions

	fr *trace.FlightRecorder

	// NB: there should be a cool-down period here. I believe that successive
	// calls to Dump end up writing essentially the same data out multiple times.
	// For example, if writing the data takes 1s, and we get a fresh call to Dump
	// every 1s, and the retention size is 100mb, in steady each byte in the buffer
	// gets written to 100 dumps.
	// Maybe we could momentarily turn off and re-enable the flight recorder right
	// when WriteTo returns (which clears the history)? That way, dumped data is
	// guaranteed to be non-overlapping.

	// Locking notes: dumpMu < regMu (if both held, dumpMu acquired first).
	regMu struct {
		syncutil.Mutex
		refcount int64

		shutdownTimer *time.Timer
	}

	// dumpMu holds the inflight dump operation, if any. Access to the mutex is
	// only brief, and not held across I/O.
	dumpMu struct {
		syncutil.Mutex
		q []dumpWork // either 1 or 2 elems
	}
}

type dumpWork struct {
	result future.AwaitableFuture[error]
	name   string
	sync   bool
	write  func(io.Writer) error
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
	cfr.fr.SetPeriod(10 * time.Second)
	const mb = 1 << 20
	cfr.fr.SetSize(25 * mb)
	cfr.regMu.shutdownTimer = time.AfterFunc(24*time.Hour, cfr.stop)
	cfr.regMu.shutdownTimer.Stop()
	return cfr
}

func (cfr *CoalescedFlightRecorder) Dir() string {
	return filepath.Dir(cfr.opts.UniqueFileName())
}

type FlightRecorderHandle interface {
	// Dump (immediately) returns the name of a file to which the flight
	// recording is being written. The future, once ready, returns whether
	// the write operation succeeded.
	//
	// Can be used concurrently.
	Dump() (string, future.AwaitableFuture[error])
	// RecordSyncTrace takes a synchronous trace for the given duration. This
	// will clear the flight recorder and start a new trace; overlapping requests
	// to dump a trace will be coalesced.
	// The provided writer may be nil. If it is not nil, it receives a copy of
	// the recorded trace.
	//
	// Only one call to RecordSyncTrace can be active at any given time, across
	// all handles; concurrent calls will fail. Direct calls to the runtime's
	// facilities for capturing execution traces or ongoing requests to the
	// execution trace pprof endpoint will also cause RecordSyncTrace to return
	// an error.
	RecordSyncTrace(context.Context, time.Duration, io.Writer) (string, error)
	Release()
}

func (cfr *CoalescedFlightRecorder) New() FlightRecorderHandle {
	cfr.regMu.Lock()
	defer cfr.regMu.Unlock()
	cfr.regMu.refcount++
	if cfr.regMu.refcount == 1 {
		// The first handle stops the timer.
		// See Release for the counterpart.
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
		// The last Release resets the timer.
		h.regMu.shutdownTimer.Reset(h.opts.IdleDuration)
	}
}

func (cfr *CoalescedFlightRecorder) stop() {
	cfr.dumpMu.Lock()
	defer cfr.dumpMu.Unlock()

	cfr.regMu.Lock()
	defer cfr.regMu.Unlock()

	if dumping := len(cfr.dumpMu.q) > 0; dumping && cfr.regMu.refcount == 0 {
		// There's an inflight dump, but no reference. This could mean
		// that raced with a short-lived handle acquisition that triggered
		// work. Instead of adding more synchronization with the dump
		// worker, simply reset the timer to check again after an additional
		// idle duration.
		cfr.regMu.shutdownTimer.Reset(cfr.opts.IdleDuration)
		return
	}

	// Consider this example:
	// - 0 registrations
	// - timer expires
	// - stop fires (hasn't acquired mutexes yet)
	// - registration comes in, resets timer
	// - stop must realize to not shut down the flight recorder: refcount check
	//   below achieves this.
	// - second invocation of stop (which may also occur, see example below, is
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
	return (*CoalescedFlightRecorder)(h).dump(nil)
}

func (h *flightRecorderHandle) RecordSyncTrace(
	ctx context.Context, duration time.Duration, optW io.Writer,
) (string, error) {
	if duration <= 0 {
		return "", errors.New("positive duration required")
	}
	// NB: caller has a handle so we know the flight recorder is active
	// throughout. This is used in takeSyncProfile.

	cfr := (*CoalescedFlightRecorder)(h)

	writeTo := func(w io.Writer) (err error) {
		if optW != nil {
			w = io.MultiWriter(w, optW)
		}
		// We need to turn off flight recording while synchronously recording
		// a trace. This will lose its ring buffer, but in effect the synchronous
		// trace becomes the ring buffer for the specified duration.
		//
		// Since writeTo is called from the dumper goroutine, and since
		// RecordSyncTrace holds a handle, we don't have to worry about races such
		// as the flight recorder getting disabled due to a lack of handles.
		fr := cfr.fr
		if err := fr.Stop(); err != nil {
			return err
		}
		defer func() {
			if err2 := fr.Start(); err2 != nil {
				err = errors.CombineErrors(err, err2)
			}
		}()

		if err := runtimetrace.Start(w); err != nil {
			return err
		}
		defer runtimetrace.Stop()
		select {
		case <-time.After(duration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	name, fut := cfr.dump(writeTo)
	err1, errCtx := fut.Wait(ctx)
	return name, errors.CombineErrors(errCtx, err1)
}

func (cfr *CoalescedFlightRecorder) dump(
	maybeSyncWriteTo func(w io.Writer) error,
) (string, future.AwaitableFuture[error]) {
	cfr.dumpMu.Lock()
	defer cfr.dumpMu.Unlock()

	sync := maybeSyncWriteTo != nil

	if sync {
		pendingSync := false
		for _, dw := range cfr.dumpMu.q {
			pendingSync = pendingSync || dw.sync
		}

		if pendingSync {
			// Sync dumps want two things:
			// - start soon
			// - record for the specified duration
			//
			// For that reason, we don't allow sync dumps to coalesce onto other sync
			// dumps. This is because sync dumps specify an exact duration and also
			// expect to be taken at a particular point in time.
			//
			// However, a sync dump can "attach" to a flight recorder dump, in which
			// case the sync dump is started as part of the inflight work. This avoids
			// spuriously failing a sync dump as a result of an ongoing flight
			// recorder dump (which should be fast, since it's only flushing existing
			// data to a writer).
			//
			// As a result of this logic, the queue never has more than two
			// elements: Dump always coalesces, and there can only be one
			// additional sync trace in the queue.
			return "", future.MakeAwaitableFuture(future.MakeCompletedErrorFuture(errTraceInProgress))
		}
	}

	launch := len(cfr.dumpMu.q) == 0

	var name string
	var fut future.AwaitableFuture[error]
	if queue := launch || sync; queue {
		write := maybeSyncWriteTo
		if write == nil {
			write = func(w io.Writer) error {
				_, err := cfr.fr.WriteTo(w)
				return err
			}
		}

		fut = future.MakeAwaitableFuture[error](&future.ErrorFuture{})
		name = cfr.opts.UniqueFileName()
		cfr.dumpMu.q = append(cfr.dumpMu.q, dumpWork{
			result: fut,
			name:   name,
			sync:   sync,
			write:  write,
		})
	} else { // coalesce onto head of queue
		name = cfr.dumpMu.q[0].name
		fut = cfr.dumpMu.q[0].result
	}

	if launch {
		// This request is the first item in the queue, so launch a worker
		// that will keep processing the queue until it's empty.
		// Note that while the worker processes the queue, new items may be
		// added. In effect, this worker _could_ run forever, though in practice
		// we expect these to be short-lived as high-frequency calls to Dump
		// are an antipattern.
		go cfr.handleQueue()
	}

	return name, fut
}

func (cfr *CoalescedFlightRecorder) handleQueue() {
	for {
		if cfr.handleQueueOne() {
			return
		}
	}
}

func (cfr *CoalescedFlightRecorder) handleQueueOne() (done bool) {
	// Grab first queue item (leaving it in the queue so more
	// requests can coalesce onto it).
	qw := func() dumpWork {
		cfr.dumpMu.Lock()
		defer cfr.dumpMu.Unlock()
		if len(cfr.dumpMu.q) == 0 {
			// This shouldn't happen.
			cfr.dumpMu.Unlock()
			return dumpWork{}
		}
		return cfr.dumpMu.q[0]
	}()

	// Do the work! (Note: no lock held).
	cfr.dumpOne(qw.name, qw.result, qw.write)

	// Remove first item from queue.
	return func() bool {
		cfr.dumpMu.Lock()
		defer cfr.dumpMu.Unlock()
		copy(cfr.dumpMu.q, cfr.dumpMu.q[1:])
		cfr.dumpMu.q = cfr.dumpMu.q[:len(cfr.dumpMu.q)-1]
		done = len(cfr.dumpMu.q) == 0
		return done
	}()

}

func (cfr *CoalescedFlightRecorder) dumpOne(
	name string, fut future.AwaitableFuture[error], write func(w io.Writer) error,
) {
	work := func() (err error) {
		w, err := cfr.opts.CreateFile(name)
		if err != nil {
			return err
		}
		wclose := func() error { return nil }
		if wc, ok := w.(io.Closer); ok {
			wclose = wc.Close
		}
		defer func() {
			err = errors.CombineErrors(err, wclose())
		}()

		if err := write(w); err != nil {
			_ = wclose()
			return err
		}
		return nil
	}

	fut.Set(work())

}
