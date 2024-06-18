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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

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

type registration struct {
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	metrics          *Metrics

	stream BufferedStream
	id     int64
	keys   interval.Range

	mu struct {
		sync.Locker
		catchUpOverflow     bool
		catchUpScanCancelFn func()
		disconnected        bool
		catchUpIter         *CatchUpIterator
		catchUpBuf          chan *sharedEvent
	}
}

func newRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	bufferSz int,
	metrics *Metrics,
	stream BufferedStream,
) registration {
	r := registration{
		span:             span,
		catchUpTimestamp: startTS,
		withFiltering:    withFiltering,
		withDiff:         withDiff,
		metrics:          metrics,
		stream:           stream,
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.catchUpBuf = make(chan *sharedEvent, bufferSz)
	r.mu.catchUpIter = catchUpIter
	return r
}

func newPooledSharedEvent() *sharedEvent {
	ev := sharedEventSyncPool.Get().(*sharedEvent)
	return ev
}

func (r *registration) maybePutInCatchUpBuffer(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.catchUpBuf == nil {
		return false
	}

	if !r.mu.catchUpOverflow {
		// don't put in stream -> just skip the value we will publish error after
		// draining everything in catch up buf later
		return true
	}
	e := newPooledSharedEvent()
	e.event = event
	e.alloc = alloc

	alloc.Use(ctx)
	select {
	case r.mu.catchUpBuf <- e:
	default:
		alloc.Release(ctx)
		putPooledSharedEvent(e)
		r.mu.catchUpOverflow = true
	}
	return true
}

func validateEvent(event *kvpb.RangeFeedEvent) {
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

func (r *registration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	validateEvent(event)

	if r.maybePutInCatchUpBuffer(ctx, event, alloc) {
		return
	}

	// should we check for disconnected here? -> we need a way to stop writes once disconnected
	r.stream.SendBuffered(r.maybeStripEvent(ctx, event), alloc)
}

// assertEvent asserts that the event contains the necessary data.
func (r *registration) assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Key: %v", t)
		}
		if t.Value.RawBytes == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.RawBytes: %v", t)
		}
		if t.Value.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedCheckpoint.Span.Key: %v", t)
		}
	case *kvpb.RangeFeedSSTable:
		if len(t.Data) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Data: %v", t)
		}
		if len(t.Span.Key) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Span: %v", t)
		}
		if t.WriteTS.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedDeleteRange:
		if len(t.Span.Key) == 0 || len(t.Span.EndKey) == 0 {
			log.Fatalf(ctx, "unexpected empty key in RangeFeedDeleteRange.Span: %v", t)
		}
		if t.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedDeleteRange.Timestamp: %v", t)
		}
	default:
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (r *registration) maybeStripEvent(
	ctx context.Context, event *kvpb.RangeFeedEvent,
) *kvpb.RangeFeedEvent {
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
				log.Fatalf(ctx, "registration span %v larger than checkpoint span %v", r.span, t.Span)
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
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
	return ret
}
func (r *registration) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected {
		return
	}
	r.mu.disconnected = true

	if r.mu.catchUpIter != nil {
		r.mu.catchUpIter.Close()
		r.mu.catchUpIter = nil
	}
	if r.mu.catchUpScanCancelFn != nil {
		r.mu.catchUpScanCancelFn()
		r.mu.catchUpScanCancelFn = nil
	}

	// TODO(wenyihu6): check where we should drain
	r.drainCatchUpBufferRLocked()
}

func (r *registration) disconnect(pErr *kvpb.Error) {
	r.cleanup()
	r.stream.SendError(pErr)
}

func (r *registration) drainCatchUpBufferRLocked() {
	if r.mu.catchUpBuf == nil {
		return
	}
	func() {
		for {
			select {
			case e := <-r.mu.catchUpBuf:
				e.alloc.Release(context.Background())
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()
	r.mu.catchUpBuf = nil
}

func (r *registration) drainAndPublishCatchUpBuffer(ctx context.Context) error {
	for {
		select {
		case e := <-r.mu.catchUpBuf:
			// TODO(wenyihu6): check if we need ok
			r.stream.SendBuffered(e.event, e.alloc)
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		case <-ctx.Done(): // watch catch up scan cancel context
			return ctx.Err()
		default:
			// catch up buffer drained already possibly disconnected before this happens
			return nil
		}
	}
}

func (r *registration) publishCatchUpBuffer(ctx context.Context) error {
	// TODO(wenyihu6): check how we can do it less pessimistic and without holding
	// lock to avoid unnecessary blocking during publish
	//
	// possible that we are already disconnected and catch up buffer would just be nil here
	// also possible that ctx is already cancelled and we should return ctx.Err() instead
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.drainAndPublishCatchUpBuffer(ctx); err != nil {
		// check if we need to set anything before returning err like catch up scan cancel
		return err
	}

	// still drain them and publish but publish error after it
	if r.mu.catchUpOverflow {
		return newErrBufferCapacityExceeded().GoError()
	}

	// success: make sure no future writes go to catch up buffer
	r.mu.catchUpScanCancelFn = nil
	r.mu.catchUpBuf = nil
	return nil
}

func (r *registration) setUpAndMaybeRunCatchUpScan(ctx context.Context) error {
	r.mu.Lock()
	if r.mu.disconnected {
		// The registration has already been disconnected.
		r.mu.Unlock()
		return nil
		// TODO(wenyihu6): disconnected but still has more clean up to do for catch
		// up iter and catch up buf -> make sure r.disconnect take care of it
	}

	ctx, r.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()
	return r.maybeRunCatchUpScan(ctx)
}

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

	err := catchUpIter.CatchUpScan(ctx, r.stream.SendUnbuffered, r.withDiff, r.withFiltering)

	if err != nil || ctx.Err() != nil {
		err = errors.Wrap(errors.CombineErrors(err, ctx.Err()), "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}

	return r.publishCatchUpBuffer(ctx)
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
}

func (r *registration) String() string {
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
func (reg *registry) Register(ctx context.Context, r *registration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		// TODO(erikgrinaker): these errors should arguably be returned.
		log.Fatalf(ctx, "%v", err)
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
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}

	reg.forOverlappingRegs(ctx, span, func(r *registration) (bool, *kvpb.Error) {
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
		log.Fatalf(ctx, "%v", err)
	}
	//r.drainAllocations(ctx)
	// r.drainCatchUpBuffer(ctx) is now called as part of rangefeed level cleanup
}

// DisconnectAllOnShutdown disconnectes all registrations on processor shutdown.
// This is different from normal disconnect as registrations won't be able to
// perform Unregister when processor's work loop is already terminated.
// This method will cleanup metrics controlled by registry itself beside posting
// errors to registrations.
// TODO: this should be revisited as part of
// https://github.com/cockroachdb/cockroach/issues/110634
func (reg *registry) DisconnectAllOnShutdown(ctx context.Context, pErr *kvpb.Error) {
	reg.metrics.RangeFeedRegistrations.Dec(int64(reg.tree.Len()))
	reg.DisconnectWithErr(ctx, all, pErr)
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry) Disconnect(ctx context.Context, span roachpb.Span) {
	reg.DisconnectWithErr(ctx, span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(ctx context.Context, span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(ctx, span, func(r *registration) (bool, *kvpb.Error) {
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
	ctx context.Context,
	span roachpb.Span,
	fn func(*registration) (disconnect bool, pErr *kvpb.Error),
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
			log.Fatalf(ctx, "%v", err)
		}
	} else if len(toDelete) > 1 {
		for _, i := range toDelete {
			if err := reg.tree.Delete(i, true /* fast */); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}
		reg.tree.AdjustRanges()
	}
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (r *registration) detachCatchUpIter() *CatchUpIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchUpIter := r.mu.catchUpIter
	r.mu.catchUpIter = nil
	return catchUpIter
}
