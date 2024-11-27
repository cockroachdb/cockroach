// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Disconnector defines an interface for disconnecting a registration. It is
// returned to node.MuxRangefeed to allow node level stream manager to
// disconnect a registration.
type Disconnector interface {
	// Disconnect disconnects the registration with the provided error. Safe to
	// run multiple times, but subsequent errors would be discarded.
	Disconnect(pErr *kvpb.Error)
	// IsDisconnected returns whether the registration has been disconnected.
	// Disconnected is a permanent state; once IsDisconnected returns true, it
	// always returns true
	IsDisconnected() bool
}

// registration defines an interface for registration that can be added to a
// processor registry. Implemented by bufferedRegistration.
type registration interface {
	Disconnector

	// publish sends the provided event to the registration. It is up to the
	// registration implementation to decide how to handle the event and how to
	// prevent missing events.
	publish(ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	// runOutputLoop runs the output loop for the registration. The output loop is
	// meant to be run in a separate goroutine.
	runOutputLoop(ctx context.Context, forStacks roachpb.RangeID)
	// drainAllocations drains all pending allocations when being unregistered
	// from processor if any.
	drainAllocations(ctx context.Context)
	// waitForCaughtUp waits for the registration to forward all buffered events
	// if any.
	waitForCaughtUp(ctx context.Context) error
	// setID sets the id field of the registration.
	setID(int64)
	// setSpanAsKeys sets the keys field to the span of the registration.
	setSpanAsKeys()
	// getSpan returns the span of the registration.
	getSpan() roachpb.Span
	// getCatchUpTimestamp returns the catchUpTimestamp of the registration.
	getCatchUpTimestamp() hlc.Timestamp
	// getWithDiff returns the withDiff field of the registration.
	getWithDiff() bool
	// getWithFiltering returns the withFiltering field of the registration.
	getWithFiltering() bool
	// getWithOmitRemote returns the withOmitRemote field of the registration.
	getWithOmitRemote() bool
	// Range returns the keys field of the registration.
	Range() interval.Range
	// ID returns the id field of the registration as a uintptr.
	ID() uintptr

	// shouldUnregister returns true if this registration should be unregistered
	// by unregisterMarkedRegistrations. UnregisterMarkedRegistrations is called
	// by the rangefeed scheduler when it has been informed of an unregister
	// request.
	shouldUnregister() bool
	// setShouldUnregister sets shouldUnregister to true. Used by the rangefeed
	// processor in response to an unregister request.
	setShouldUnregister()
}

// baseRegistration is a common base for all registration types. It is intended
// to be embedded in an actual registration struct.
type baseRegistration struct {
	streamCtx      context.Context
	span           roachpb.Span
	withDiff       bool
	withFiltering  bool
	withOmitRemote bool
	// removeRegFromProcessor is called to remove the registration from its
	// processor. This is provided by the creator of the registration and called
	// during disconnect(). Since it is called during disconnect it must be
	// non-blocking.
	removeRegFromProcessor func(registration)

	catchUpTimestamp hlc.Timestamp // exclusive
	id               int64         // internal
	keys             interval.Range
	shouldUnreg      atomic.Bool
}

// ID implements interval.Interface.
func (r *baseRegistration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *baseRegistration) Range() interval.Range {
	return r.keys
}

func (r *baseRegistration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchUpTimestamp)
}

func (r *baseRegistration) setID(id int64) {
	r.id = id
}

func (r *baseRegistration) setSpanAsKeys() {
	r.keys = r.span.AsRange()
}

func (r *baseRegistration) getSpan() roachpb.Span {
	return r.span
}

func (r *baseRegistration) getCatchUpTimestamp() hlc.Timestamp {
	return r.catchUpTimestamp
}

func (r *baseRegistration) getWithFiltering() bool {
	return r.withFiltering
}

func (r *baseRegistration) getWithOmitRemote() bool {
	return r.withOmitRemote
}

func (r *baseRegistration) shouldUnregister() bool {
	return r.shouldUnreg.Load()
}

func (r *baseRegistration) setShouldUnregister() {
	r.shouldUnreg.Store(true)
}

func (r *baseRegistration) getWithDiff() bool {
	return r.withDiff
}

// assertEvent asserts that the event contains the necessary data.
func (r *baseRegistration) assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
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
func (r *baseRegistration) maybeStripEvent(
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
func (reg *registry) Register(ctx context.Context, r registration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.setID(reg.nextID())
	r.setSpanAsKeys()
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
	valueMetadata logicalOpMetadata,
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

	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
		// Don't publish events if they:
		// 1. are equal to or less than the registration's starting timestamp, or
		// 2. have OmitInRangefeeds = true and this registration has opted into filtering, or
		// 3. have OmitRemote = true and this value is from a remote cluster.
		if r.getCatchUpTimestamp().Less(minTS) && !(r.getWithFiltering() && valueMetadata.omitInRangefeeds) && (!r.getWithOmitRemote() || valueMetadata.originID == 0) {
			r.publish(ctx, event, alloc)
		}
		return false, nil
	})
}

// DisconnectAllOnShutdown disconnectes all registrations on processor shutdown.
// This is different from normal disconnect as registrations won't be able to
// perform Unregister when processor's work loop is already terminated.
// This method will cleanup metrics controlled by registry itself beside posting
// errors to registrations.
// TODO: this should be revisited as part of
// https://github.com/cockroachdb/cockroach/issues/110634
func (reg *registry) DisconnectAllOnShutdown(ctx context.Context, pErr *kvpb.Error) {
	reg.DisconnectWithErr(ctx, all, pErr)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(ctx context.Context, span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
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
	ctx context.Context, span roachpb.Span, fn func(registration) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(registration)
		dis, pErr := fn(r)
		if dis {
			r.Disconnect(pErr)
			toDelete = append(toDelete, i)
		}
		return false
	}
	if span.EqualValue(all) {
		reg.tree.Do(matchFn)
	} else {
		reg.tree.DoMatching(matchFn, span.AsRange())
	}
	reg.remove(ctx, toDelete)
}

func (reg *registry) remove(ctx context.Context, toDelete []interval.Interface) {
	// We only ever call remote on values we know exist in the
	// registry, so we can assume we can decrement this by the
	// lenght of the input.
	reg.metrics.RangeFeedRegistrations.Dec(int64(len(toDelete)))
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

// unregisterMarkedRegistrations iterates the registery and removes any
// registrations where shouldUnregister() returns true. This is called by the
// rangefeed processor in response to an async unregistration request.
//
// See the comment on (*ScheduledProcessor).unregisterClientAsync for more
// details.
func (reg *registry) unregisterMarkedRegistrations(ctx context.Context) {
	var toDelete []interval.Interface
	reg.tree.Do(func(i interval.Interface) (done bool) {
		r := i.(registration)
		if r.shouldUnregister() {
			toDelete = append(toDelete, i)
		}
		return false
	})
	reg.remove(ctx, toDelete)
}

// waitForCaughtUp waits for all registrations overlapping the given span to
// completely process their internal buffers.
func (reg *registry) waitForCaughtUp(ctx context.Context, span roachpb.Span) error {
	var outerErr error
	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
		if outerErr == nil {
			outerErr = r.waitForCaughtUp(ctx)
		}
		return false, nil
	})
	return outerErr
}
