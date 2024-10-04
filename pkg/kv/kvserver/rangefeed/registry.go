// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// Send blocks until it sends m, the stream is done, or the stream breaks.
	// Send must be safe to call on the same stream in different goroutines.
	Send(*kvpb.RangeFeedEvent) error
}

// Shared event is an entry stored in registration channel. Each entry is
// specific to registration but allocation is shared between all registrations
// to track memory budgets. event itself could either be shared or not in case
// we optimized unused fields in it based on registration options.
type sharedEvent struct {
	event *kvpb.RangeFeedEvent
	alloc *SharedBudgetAllocation
}

var sharedEventSyncPool = sync.Pool{
	New: func() interface{} {
		return new(sharedEvent)
	},
}

func getPooledSharedEvent(e sharedEvent) *sharedEvent {
	ev := sharedEventSyncPool.Get().(*sharedEvent)
	*ev = e
	return ev
}

func putPooledSharedEvent(e *sharedEvent) {
	*e = sharedEvent{}
	sharedEventSyncPool.Put(e)
}

// registration is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys.
// Updates are delivered to its stream until one of the following
// conditions is met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration is unregistered its error
// channel is sent an error to inform it that the registration
// has finished.
type registration struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	metrics          *Metrics

	// Output.
	stream Stream
	done   *future.ErrorFuture
	unreg  func()
	// Internal.
	id            int64
	keys          interval.Range
	buf           chan *sharedEvent
	blockWhenFull bool // if true, block when buf is full (for tests)

	mu struct {
		sync.Locker
		// True if this registration buffer has overflowed, dropping a live event.
		// This will cause the registration to exit with an error once the buffer
		// has been emptied.
		overflowed bool
		// Boolean indicating if all events have been output to stream. Used only
		// for testing.
		caughtUp bool
		// Management of the output loop goroutine, used to ensure proper teardown.
		outputLoopCancelFn func()
		disconnected       bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed.
		// If output loop was not started and catchUpIter is non-nil at the time
		// that disconnect is called, it is closed by disconnect.
		catchUpIter *CatchUpIterator
	}
}

func newRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	bufferSz int,
	blockWhenFull bool,
	metrics *Metrics,
	stream Stream,
	unregisterFn func(),
	done *future.ErrorFuture,
) registration {
	r := registration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		metrics:          metrics,
		stream:           stream,
		done:             done,
		unreg:            unregisterFn,
		buf:              make(chan *sharedEvent, bufferSz),
		blockWhenFull:    blockWhenFull,
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.caughtUp = true
	r.mu.catchUpIter = catchUpIter
	return r
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (r *registration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	r.validateEvent(event)
	e := getPooledSharedEvent(sharedEvent{event: r.maybeStripEvent(event), alloc: alloc})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.overflowed {
		return
	}
	alloc.Use()
	select {
	case r.buf <- e:
		r.mu.caughtUp = false
	default:
		// If we're asked to block (in tests), do a blocking send after releasing
		// the mutex -- otherwise, the output loop won't be able to consume from the
		// channel. We optimistically attempt the non-blocking send above first,
		// since we're already holding the mutex.
		if r.blockWhenFull {
			r.mu.Unlock()
			select {
			case r.buf <- e:
				r.mu.Lock()
				r.mu.caughtUp = false
			case <-ctx.Done():
				r.mu.Lock()
				alloc.Release(ctx)
			}
			return
		}
		// Buffer exceeded and we are dropping this event. Registration will need
		// a catch-up scan.
		r.mu.overflowed = true
		alloc.Release(ctx)
	}
}

// validateEvent checks that the event contains enough information for the
// registation.
func (r *registration) validateEvent(event *kvpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Key: %v", t))
		}
		if t.Value.RawBytes == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.RawBytes: %v", t))
		}
		if t.Value.Timestamp.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.Timestamp: %v", t))
		}
	case *kvpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedCheckpoint.Span.Key: %v", t))
		}
	case *kvpb.RangeFeedSSTable:
		if len(t.Data) == 0 {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Data: %v", t))
		}
		if len(t.Span.Key) == 0 {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Span: %v", t))
		}
		if t.WriteTS.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Timestamp: %v", t))
		}
	case *kvpb.RangeFeedDeleteRange:
		if len(t.Span.Key) == 0 || len(t.Span.EndKey) == 0 {
			panic(fmt.Sprintf("unexpected empty key in RangeFeedDeleteRange.Span: %v", t))
		}
		if t.Timestamp.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedDeleteRange.Timestamp: %v", t))
		}
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (r *registration) maybeStripEvent(event *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
	ret := event
	copyOnWrite := func() interface{} {
		if ret == event {
			ret = event.ShallowCopy()
		}
		return ret.GetValue()
	}

	switch t := ret.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.PrevValue.IsPresent() && !r.withDiff {
			// If no registrations for the current Range are requesting previous
			// values, then we won't even retrieve them on the Raft goroutine.
			// However, if any are and they overlap with an update then the
			// previous value on the corresponding events will be populated.
			// If we're in this case and any other registrations don't want
			// previous values then we'll need to strip them.
			t = copyOnWrite().(*kvpb.RangeFeedValue)
			t.PrevValue = roachpb.Value{}
		}
	case *kvpb.RangeFeedCheckpoint:
		if !t.Span.EqualValue(r.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(r.span) {
				panic(fmt.Sprintf("registration span %v larger than checkpoint span %v", r.span, t.Span))
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = r.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(r.span); !i.Equal(t.Span) {
			t = copyOnWrite().(*kvpb.RangeFeedDeleteRange)
			t.Span = i.Clone()
		}
	case *kvpb.RangeFeedSSTable:
		// SSTs are always sent in their entirety, it is up to the caller to
		// filter out irrelevant entries.
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
	return ret
}

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration.
// Safe to run multiple times, but subsequent errors would be discarded.
func (r *registration) disconnect(pErr *kvpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.mu.disconnected {
		if r.mu.catchUpIter != nil {
			r.mu.catchUpIter.Close()
			r.mu.catchUpIter = nil
		}
		if r.mu.outputLoopCancelFn != nil {
			r.mu.outputLoopCancelFn()
		}
		r.mu.disconnected = true
		r.done.Set(pErr.GoError())
	}
}

// outputLoop is the operational loop for a single registration. The behavior
// is as thus:
//
// 1. If a catch-up scan is indicated, run one before beginning the proper
// output loop.
// 2. After catch-up is complete, begin reading from the registration buffer
// channel and writing to the output stream until the buffer is empty *and*
// the overflow flag has been set.
//
// The loop exits with any error encountered, if the provided context is
// canceled, or when the buffer has overflowed and all pre-overflow entries
// have been emitted.
func (r *registration) outputLoop(ctx context.Context) error {
	// If the registration has a catch-up scan, run it.
	if err := r.maybeRunCatchUpScan(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}

	// Normal buffered output loop.
	for {
		overflowed := false
		r.mu.Lock()
		if len(r.buf) == 0 {
			overflowed = r.mu.overflowed
			r.mu.caughtUp = true
		}
		r.mu.Unlock()
		if overflowed {
			return newErrBufferCapacityExceeded().GoError()
		}

		select {
		case nextEvent := <-r.buf:
			err := r.stream.Send(nextEvent.event)
			nextEvent.alloc.Release(ctx)
			putPooledSharedEvent(nextEvent)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stream.Context().Done():
			return r.stream.Context().Err()
		}
	}
}

func (r *registration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	r.mu.Lock()
	if r.mu.disconnected {
		// The registration has already been disconnected.
		r.mu.Unlock()
		return
	}
	ctx, r.mu.outputLoopCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()
	err := r.outputLoop(ctx)
	r.disconnect(kvpb.NewError(err))
}

// drainAllocations should be done after registration is disconnected from
// processor to release all memory budget that its pending events hold.
func (r *registration) drainAllocations(ctx context.Context) {
	for {
		select {
		case e, ok := <-r.buf:
			if !ok {
				return
			}
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		default:
			return
		}
	}
}

// maybeRunCatchUpScan starts a catch-up scan which will output entries for all
// recorded changes in the replica that are newer than the catchUpTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (r *registration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := r.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		r.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	return catchUpIter.CatchUpScan(ctx, r.stream.Send, r.withDiff, r.withFiltering)
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
}

func (r registration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchUpTimestamp)
}

// registry holds a set of registrations and manages their lifecycle.
type registry struct {
	metrics *Metrics
	tree    interval.Tree // *registration items
	idAlloc int64
}

func makeRegistry(metrics *Metrics) registry {
	return registry{
		metrics: metrics,
		tree:    interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// Len returns the number of registrations in the registry.
func (reg *registry) Len() int {
	return reg.tree.Len()
}

// NewFilter returns a operation filter reflecting the registrations
// in the registry.
func (reg *registry) NewFilter() *Filter {
	return newFilterFromRegistry(reg)
}

// Register adds the provided registration to the registry.
func (reg *registry) Register(r *registration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		panic(err)
	}
}

func (reg *registry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry) PublishToOverlapping(
	ctx context.Context,
	span roachpb.Span,
	event *kvpb.RangeFeedEvent,
	omitInRangefeeds bool,
	alloc *SharedBudgetAllocation,
) {
	// Determine the earliest starting timestamp that a registration
	// can have while still needing to hear about this event.
	var minTS hlc.Timestamp
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		minTS = t.Value.Timestamp
	case *kvpb.RangeFeedSSTable:
		minTS = t.WriteTS
	case *kvpb.RangeFeedDeleteRange:
		minTS = t.Timestamp
	case *kvpb.RangeFeedCheckpoint:
		// Always publish checkpoint notifications, regardless of a registration's
		// starting timestamp.
		//
		// TODO(dan): It's unclear if this is the right contract, it's certainly
		// surprising. Revisit this once RangeFeed has more users.
		minTS = hlc.MaxTimestamp
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}

	reg.forOverlappingRegs(span, func(r *registration) (bool, *kvpb.Error) {
		// Don't publish events if they:
		// 1. are equal to or less than the registration's starting timestamp, or
		// 2. have OmitInRangefeeds = true and this registration has opted into filtering.
		if r.catchUpTimestamp.Less(minTS) && !(r.withFiltering && omitInRangefeeds) {
			r.publish(ctx, event, alloc)
		}
		return false, nil
	})
}

// Unregister removes a registration from the registry. It is assumed that the
// registration has already been disconnected, this is intended only to clean
// up the registry.
// We also drain all pending events for the sake of memory accounting. To do
// that we rely on a fact that caller is not going to post any more events
// concurrently or after this function is called.
func (reg *registry) Unregister(ctx context.Context, r *registration) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		panic(err)
	}
	r.drainAllocations(ctx)
}

// DisconnectAllOnShutdown disconnectes all registrations on processor shutdown.
// This is different from normal disconnect as registrations won't be able to
// perform Unregister when processor's work loop is already terminated.
// This method will cleanup metrics controlled by registry itself beside posting
// errors to registrations.
// TODO: this should be revisited as part of
// https://github.com/cockroachdb/cockroach/issues/110634
func (reg *registry) DisconnectAllOnShutdown(pErr *kvpb.Error) {
	reg.metrics.RangeFeedRegistrations.Dec(int64(reg.tree.Len()))
	reg.DisconnectWithErr(all, pErr)
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry) Disconnect(span roachpb.Span) {
	reg.DisconnectWithErr(span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(span, func(r *registration) (bool, *kvpb.Error) {
		return true /* disconned */, pErr
	})
}

// all is a span that overlaps with all registrations.
var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *registry) forOverlappingRegs(
	span roachpb.Span, fn func(*registration) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(*registration)
		dis, pErr := fn(r)
		if dis {
			r.disconnect(pErr)
			toDelete = append(toDelete, i)
		}
		return false
	}
	if span.EqualValue(all) {
		reg.tree.Do(matchFn)
	} else {
		reg.tree.DoMatching(matchFn, span.AsRange())
	}

	if len(toDelete) == reg.tree.Len() {
		reg.tree.Clear()
	} else if len(toDelete) == 1 {
		if err := reg.tree.Delete(toDelete[0], false /* fast */); err != nil {
			panic(err)
		}
	} else if len(toDelete) > 1 {
		for _, i := range toDelete {
			if err := reg.tree.Delete(i, true /* fast */); err != nil {
				panic(err)
			}
		}
		reg.tree.AdjustRanges()
	}
}

// Wait for this registration to completely process its internal buffer.
func (r *registration) waitForCaughtUp() error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.Start(opts); re.Next(); {
		r.mu.Lock()
		caughtUp := len(r.buf) == 0 && r.mu.caughtUp
		r.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	return errors.Errorf("registration %v failed to empty in time", r.Range())
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (r *registration) detachCatchUpIter() *CatchUpIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchUpIter := r.mu.catchUpIter
	r.mu.catchUpIter = nil
	return catchUpIter
}

// waitForCaughtUp waits for all registrations overlapping the given span to
// completely process their internal buffers.
func (reg *registry) waitForCaughtUp(span roachpb.Span) error {
	var outerErr error
	reg.forOverlappingRegs(span, func(r *registration) (bool, *kvpb.Error) {
		if outerErr == nil {
			outerErr = r.waitForCaughtUp()
		}
		return false, nil
	})
	return outerErr
}
