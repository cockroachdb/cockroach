// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/sched"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type registrationEvent int

// Scheduler events
const (
	data    registrationEvent = 1 << 2
	catchup registrationEvent = 1 << 3
)

// registration2 is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys.
// Updates are delivered to its stream until one of the following
// conditions is met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration2 is unregistered its error
// channel is sent an error to inform it that the registration2
// has finished.
// TODO(oleg): rename types and files to reflect that it is scheduler based.
type registration2 struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	metrics          *Metrics

	// catchUpIterConstructor is used to construct the catchUpIter if necessary.
	// The reason this constructor is plumbed down is to make sure that the
	// iterator does not get constructed too late in server shutdown. However,
	// it must also be stored in the struct to ensure that it is not constructed
	// too late, after the raftMu has been dropped. Thus, this function, if
	// non-nil, will be used to populate mu.catchUpIter while the registration
	// is being registered by the processor.
	catchUpIterConstructor CatchUpIteratorConstructor

	// Output.
	stream Stream
	sched  sched.ClientScheduler
	done   *future.ErrorFuture
	unreg  func()
	// Internal.
	id   int64
	keys interval.Range
	buf  chan *sharedEvent
	// TODO: add this, alternatively we need a wrapper that allows scheduling
	// entries without id knowledge which will be added by wrapper. Then we can
	// have non mux ones using another fixed pool
	// scheduler *CallbackScheduler
	// stream ID

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
		outputLoopCancelFn func() // This is now only stopping catch up scan.
		disconnected       bool
		// processFinished is called when last scheduler process is called for the
		// registration to let processor do any necessary cleanup.
		processFinished    func(*registration2)

		// catchUpIter is populated on the Processor's goroutine while the
		// Replica.raftMu is still held. If it is non-nil at the time that
		// disconnect is called, it is closed by disconnect.
		catchUpIter *CatchUpIterator
		// catchUpFinished is set to true when catch up iterator finishes to enable
		// event scheduling.
		catchUpFinished bool
	}
}

func newRegistration2(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIterConstructor CatchUpIteratorConstructor,
	withDiff bool,
	bufferSz int,
	metrics *Metrics,
	stream Stream,
	sched sched.ClientScheduler,
	unregisterFn func(),
	done *future.ErrorFuture,
) registration2 {
	r := registration2{
		span:                   span,
		catchUpTimestamp:       startTS,
		catchUpIterConstructor: catchUpIterConstructor,
		withDiff:               withDiff,
		metrics:                metrics,
		stream:                 stream,
		sched:                  sched,
		done:                   done,
		unreg:                  unregisterFn,
		buf:                    make(chan *sharedEvent, bufferSz),
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.caughtUp = true
	return r
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (r *registration2) publish(
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
		if r.mu.catchUpFinished {
			// Schedule event for processing.
			r.sched.Schedule(int(data))
			// log.Infof(ctx, "scheduling data event")
		} else {
			log.VEventf(ctx, 2, "registration still in catch up mode, not scheduling processing")
		}
	default:
		// Buffer exceeded and we are dropping this event. Registration will need
		// a catch-up scan.
		r.mu.overflowed = true
		alloc.Release(ctx)
	}
}

// validateEvent checks that the event contains enough information for the
// registation.
func (r *registration2) validateEvent(event *kvpb.RangeFeedEvent) {
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
func (r *registration2) maybeStripEvent(event *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
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
// error to the output error stream for the registration. This also sets the
// disconnected flag on the registration, preventing it from being disconnected
// again.
func (r *registration2) disconnect(pErr *kvpb.Error) {
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
/*
func (r *registration2) outputLoopZ(ctx context.Context) error {
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
*/

// Process
func (r *registration2) process(e int) int {
	log.VEventf(context.Background(), 3, "registration process event %d", e)
	ev := registrationEvent(e)
	if ev&catchup != 0 {
		panic("we don't support catch up using scheduler yet")
	}
	if ev&data != 0 {
		err := func() error {
			// TODO(oleg): Need to limit batch size here.
			for {
				select {
				case nextEvent := <-r.buf:
					err := r.stream.Send(nextEvent.event)
					nextEvent.alloc.Release(context.Background())
					putPooledSharedEvent(nextEvent)
					if err != nil {
						// Trigger termination of registration below.
						return err
					}
				default:
					return nil
				}
			}
		}()
		if err != nil {
			// Terminate scheduler callback and report error.
			// where do we stash error?
			r.disconnect(kvpb.NewError(err))
			return sched.Stopped
		}
		// Check overflown status first and abort if needed,
		// TODO(oleg): doesn't make much sense currently as we always catch up here.
		// see comment above about batching.
		overflowed := false
		r.mu.Lock()
		if len(r.buf) == 0 {
			overflowed = r.mu.overflowed
			r.mu.caughtUp = true
		}
		r.mu.Unlock()
		if overflowed {
			r.disconnect(newErrBufferCapacityExceeded())
			return sched.Stopped
		}
	}
	if e&sched.Stopped != 0 {
		r.mu.Lock()
		unreg := r.mu.processFinished
		r.mu.Unlock()
		if unreg != nil {
			// We need to offload unregistration to a separate goroutine as it could
			// be slow as it needs to interact with processor work loop.
			go unreg(r)
		}
		return 0
	}
	return 0
}

func (r *registration2) registerCallback() error {
	return r.sched.Register(r.process)
}

// Must run in the background.
func (r *registration2) startEventProcessing(
	ctx context.Context,
	_forStacks roachpb.RangeID,
	processFinished func(*registration2),
) {
	r.mu.Lock()
	if r.mu.disconnected {
		// The registration has already been disconnected.
		r.mu.Unlock()
		return
	}
	ctx, r.mu.outputLoopCancelFn = context.WithCancel(ctx)
	r.mu.processFinished = processFinished
	r.mu.Unlock()

	go func() {
		if err := r.maybeRunCatchUpScan(ctx); err != nil {
			err = errors.Wrap(err, "catch-up scan failed")
			log.Errorf(ctx, "%v", err)
			// We didn't start scheduling events yet, but we can perform that in
			// uniform manner and post to scheduler?
			r.disconnect(kvpb.NewError(err))
			return
		}
		log.Info(ctx, "finished catch up scan on registration")

		r.mu.Lock()
		r.mu.catchUpFinished = true
		enqueue := len(r.buf) > 0
		r.mu.Unlock()
		if enqueue {
			log.Info(ctx, "found pending event after initial scan, scheduling")
			r.sched.Schedule(int(data))
		}
	}()
}

/*
func (r *registration2) runOutputLoopZ(ctx context.Context, _forStacks roachpb.RangeID) {
	r.mu.Lock()
	if r.mu.disconnected {
		// The registration has already been disconnected.
		r.mu.Unlock()
		return
	}
	ctx, r.mu.outputLoopCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()
	err := r.outputLoopZ(ctx)
	r.disconnect(kvpb.NewError(err))
}
*/

// drainAllocations should be done after registration is disconnected from
// processor to release all memory budget that its pending events hold.
func (r *registration2) drainAllocations(ctx context.Context) {
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

// maybeRunCatchUpScan runs a catch-up scan which will output entries for all
// recorded changes in the replica that are newer than the catchUpTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (r *registration2) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := r.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		r.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	return catchUpIter.CatchUpScan(ctx, r.stream.Send, r.withDiff)
}

// ID implements interval.Interface.
func (r *registration2) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration2) Range() interval.Range {
	return r.keys
}

func (r *registration2) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchUpTimestamp)
}

// Wait for this registration to completely process its internal buffer.
func (r *registration2) waitForCaughtUp() error {
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

// maybeConstructCatchUpIter calls the catchUpIterConstructor and attaches
// the catchUpIter to be detached in the catchUpScan or closed on disconnect.
func (r *registration2) maybeConstructCatchUpIter() {
	if r.catchUpIterConstructor == nil {
		return
	}

	catchUpIter := r.catchUpIterConstructor(r.span, r.catchUpTimestamp)
	r.catchUpIterConstructor = nil

	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.catchUpIter = catchUpIter
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (r *registration2) detachCatchUpIter() *CatchUpIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchUpIter := r.mu.catchUpIter
	r.mu.catchUpIter = nil
	return catchUpIter
}

// registry holds a set of registrations and manages their lifecycle.
type registry2 struct {
	metrics *Metrics
	tree    interval.Tree // *registration items
	idAlloc int64
}

func makeRegistry2(metrics *Metrics) registry2 {
	return registry2{
		metrics: metrics,
		tree:    interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// Len returns the number of registrations in the registry.
func (reg *registry2) Len() int {
	return reg.tree.Len()
}

// NewFilter returns a operation filter reflecting the registrations
// in the registry.
func (reg *registry2) NewFilter() *Filter {
	return newFilterFromRegistry2(reg)
}

// Register adds the provided registration to the registry.
func (reg *registry2) Register(r *registration2) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		panic(err)
	}
}

func (reg *registry2) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry2) PublishToOverlapping(
	ctx context.Context, span roachpb.Span, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
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

	reg.forOverlappingRegs(span, func(r *registration2) (bool, *kvpb.Error) {
		// Don't publish events if they are equal to or less
		// than the registration's starting timestamp.
		if r.catchUpTimestamp.Less(minTS) {
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
func (reg *registry2) Unregister(ctx context.Context, r *registration2) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		panic(err)
	}
	r.drainAllocations(ctx)
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry2) Disconnect(span roachpb.Span) {
	reg.DisconnectWithErr(span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry2) DisconnectWithErr(span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(span, func(r *registration2) (bool, *kvpb.Error) {
		return true /* disconned */, pErr
	})
}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *registry2) forOverlappingRegs(
	span roachpb.Span, fn func(*registration2) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(*registration2)
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

// waitForCaughtUp waits for all registrations overlapping the given span to
// completely process their internal buffers.
func (reg *registry2) waitForCaughtUp(span roachpb.Span) error {
	var outerErr error
	reg.forOverlappingRegs(span, func(r *registration2) (bool, *kvpb.Error) {
		if outerErr == nil {
			outerErr = r.waitForCaughtUp()
		}
		return false, nil
	})
	return outerErr
}
