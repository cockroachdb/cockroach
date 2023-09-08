// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NewBufferedStream returns buffered stream for registrations to use and
// unbuffered stream that could be used to send catchUpScan without exhausting
// stream buffer and memory budget.
type NewBufferedStream func(drained func()) BufferedStream

// nonBufferedRegistry holds a set of registrations and manages their lifecycle.
type nonBufferedRegistry struct {
	metrics *Metrics
	tree    interval.Tree // *nonBufferedRegistration items
	idAlloc int64
}

func makeNonBufferedRegistry(metrics *Metrics) nonBufferedRegistry {
	return nonBufferedRegistry{
		metrics: metrics,
		tree:    interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// Len returns the number of registrations in the registry.
func (reg *nonBufferedRegistry) Len() int {
	return reg.tree.Len()
}

// NewFilter returns a operation filter reflecting the registrations
// in the registry.
func (reg *nonBufferedRegistry) NewFilter() *Filter {
	return newFilterFromFilterTree(reg.tree)
}

// Register adds the provided registration to the registry.
func (reg *nonBufferedRegistry) Register(r *nonBufferedRegistration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		panic(err)
	}
}

func (reg *nonBufferedRegistry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *nonBufferedRegistry) PublishToOverlapping(
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

	reg.forOverlappingRegs(span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
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
func (reg *nonBufferedRegistry) Unregister(r *nonBufferedRegistration) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		panic(err)
	}
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *nonBufferedRegistry) Disconnect(span roachpb.Span) {
	reg.DisconnectWithErr(span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *nonBufferedRegistry) DisconnectWithErr(span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
		return true /* disconnected */, pErr
	})
}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *nonBufferedRegistry) forOverlappingRegs(
	span roachpb.Span, fn func(*nonBufferedRegistration) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(*nonBufferedRegistration)
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

type nonBufferedRegistration struct {
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	metrics          *Metrics
	id               int64
	keys             interval.Range
	stream           BufferedStream

	mu struct {
		syncutil.Mutex

		// disconnected indicates that disconnect was run once and all resources
		// were cleaned up.
		disconnected bool

		// Catch up state of registration.

		// catchUpBuf holds realtime updates for the registration while catchUp
		// scan is performed. publish requests write to the chan if it is non nil.
		catchUpBuf chan *sharedEvent
		// catchupOverflow set to true if writes to catchUpBuf exceed its capacity.
		catchupOverflow bool
		// catchUpScanCancel cancel context of catch up scan to terminate early.
		catchUpScanCancel func()
	}
	// catchUpIter is populated on the Processor's goroutine while the
	// Replica.raftMu is still held.
	catchUpIter *CatchUpIterator
	// catchUpDrained is closed when catch up is finished, tests and callback can
	// wait for allocations to be released if needed.
	catchUpDrained chan struct{}
}

var _ interval.Interface = (*nonBufferedRegistration)(nil)
var _ filterable = (*nonBufferedRegistration)(nil)

func newNonBufferedRegistration(
	span roachpb.Span, startTS hlc.Timestamp, withDiff bool, metrics *Metrics,
) *nonBufferedRegistration {
	r := &nonBufferedRegistration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		metrics:          metrics,
		catchUpDrained:   make(chan struct{}),
	}
	return r
}

func (r *nonBufferedRegistration) connect(
	stream BufferedStream, catchUpBufferSize int, catchUpIter *CatchUpIterator,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected {
		// It is possible that stream was terminated before we had a chance to
		// connect this registration.
		close(r.catchUpDrained)
		if catchUpIter != nil {
			catchUpIter.Close()
		}
		return false
	}
	if catchUpIter == nil {
		close(r.catchUpDrained)
	} else {
		r.mu.catchUpBuf = make(chan *sharedEvent, catchUpBufferSize)
		r.catchUpIter = catchUpIter
	}
	r.stream = stream
	return true
}

func (r *nonBufferedRegistration) needsPrev() bool {
	return r.withDiff
}

// Range implements interval.Interface
func (r *nonBufferedRegistration) Range() interval.Range {
	return r.keys
}

// ID implements interval.Interface
func (r *nonBufferedRegistration) ID() uintptr {
	return uintptr(r.id)
}

func (r *nonBufferedRegistration) runCatchupScan(ctx context.Context, _ roachpb.RangeID) {
	ctx, cancelled := r.setupCatchupScan(ctx)

	// If no catch up iterator, then we don't have any resources allocated and
	// can exit immediately.
	if r.catchUpIter == nil {
		return
	}

	// If cancellation from underlying stream arrived before we can setup scan
	// we still need to discard catch up buffer and close catchup iterator.
	if cancelled {
		r.discardCatchUpBuffer(ctx)
		r.catchUpIter.Close()
		r.catchUpIter = nil
		close(r.catchUpDrained)
		return
	}

	start := timeutil.Now()
	defer func() {
		r.catchUpIter.Close()
		r.catchUpIter = nil
		close(r.catchUpDrained)
		r.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()
	err := r.catchUpIter.CatchUpScan(ctx, r.stream.SendUnbuffered, r.withDiff)

	// If catchup is successful, publish pending event accumulated from processor.
	if err == nil {
		err = r.publishCatchUpBuffer(ctx)
	}

	if err != nil {
		r.discardCatchUpBuffer(ctx)
		// Send error so that downstream knows stream failed just in case.
		r.stream.SendError(kvpb.NewError(err))
	}
}

// setupCatchupScan atomically sets up catch up scan. this state controls
// cancellation behaviour. if cancellation happens before start then we need
// to cleanup iterator and catch up buffer, if it happens after, we need to
// cancel async task and wait for it to drain events.
func (r *nonBufferedRegistration) setupCatchupScan(
	ctx context.Context,
) (catchupCtx context.Context, cancelled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchupCtx = ctx
	if r.catchUpIter != nil {
		catchupCtx, r.mu.catchUpScanCancel = context.WithCancel(ctx)
	}
	return catchupCtx, r.mu.disconnected
}

func (r *nonBufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	drainCurrent := func() error {
		for {
			select {
			case e := <-r.mu.catchUpBuf:
				r.stream.Send(e.event, e.alloc)
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		}
	}

	// First we drain without lock to avoid unnecessary blocking of publish.
	if err := drainCurrent(); err != nil {
		return err
	}
	// Now drain again under lock to ensure we don't leave behind events written
	// while we were draining.
	r.mu.Lock()
	defer r.mu.Unlock()
	err := drainCurrent()
	r.mu.catchUpScanCancel = nil
	if err != nil {
		return err
	}
	if r.mu.catchupOverflow {
		err = newErrBufferCapacityExceeded().GoError()
	}
	if err == nil {
		// If we successfully wrote buffer, and no data was lost because of overflow
		// enable normal writes.
		r.mu.catchUpBuf = nil
	}
	return err
}

func (r *nonBufferedRegistration) discardCatchUpBuffer(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	func() {
		for {
			select {
			case e := <-r.mu.catchUpBuf:
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()
	if r.mu.catchUpBuf != nil {
		r.mu.catchUpBuf = nil
	}
}

func (r *nonBufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	validateEvent(event)

	if r.maybePutInCatchupBuffer(ctx, event, alloc) {
		return
	}

	r.stream.Send(r.maybeStripEvent(event), alloc)
}

func (r *nonBufferedRegistration) maybePutInCatchupBuffer(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// If any of those is not nil that means we still need to process catch up
	// scan or we are actively processing catch up scan.
	if r.mu.catchUpBuf == nil {
		return false
	}
	if !r.mu.catchupOverflow {
		e := getPooledSharedEvent(sharedEvent{
			event: event,
			alloc: alloc,
		})
		alloc.Use()
		select {
		case r.mu.catchUpBuf <- e:
		default:
			alloc.Release(ctx)
			putPooledSharedEvent(e)
			r.mu.catchupOverflow = true
		}
	}
	return true
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (r *nonBufferedRegistration) maybeStripEvent(event *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
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

// abortAndDisconnectNonStarted disposes of registration that didn't start yet.
// when it happens we already know that catch up won't start and that no events
// were placed in catchup buffer so we can explicitly close iter only before
// delegating to usual cleanup.
func (r *nonBufferedRegistration) abortAndDisconnectNonStarted(pErr *kvpb.Error) {
	if r.catchUpIter != nil {
		r.catchUpIter.Close()
		r.catchUpIter = nil
		close(r.catchUpDrained)
	}
	r.disconnect(pErr)
}

func (r *nonBufferedRegistration) disconnect(pErr *kvpb.Error) {
	r.cancelCatchUp()
	r.stream.SendError(pErr)
}

func (r *nonBufferedRegistration) cancelCatchUp() {
	defer func() {
		<-r.catchUpDrained
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected {
		return
	}
	r.mu.disconnected = true
	// Case where catch up is in progress and we need to terminate and drain.
	if r.mu.catchUpScanCancel != nil {
		r.mu.catchUpScanCancel()
	}
}
