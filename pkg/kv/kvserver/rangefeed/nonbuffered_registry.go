// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type nonBufferedRegistration struct {
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	metrics          *Metrics
	id               int64
	keys             interval.Range
	withFiltering    bool
}

var _ interval.Interface = (*nonBufferedRegistration)(nil)
var _ filterable = (*nonBufferedRegistration)(nil)

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

type nonBufferedRegistry struct {
	metrics *Metrics
	tree    interval.Tree // *nonBufferedRegistration items
	idAlloc int64
}

// TODO(wenyihu6): check on alloc memory accounting again

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
	return newFilterFromRegistry(reg.tree)
}

// Register adds the provided registration to the registry.
func (reg *nonBufferedRegistry) Register(ctx context.Context, r *nonBufferedRegistration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		// TODO(erikgrinaker): these errors should arguably be returned.
		log.Fatalf(ctx, "%v", err)
	}
}

func (reg *nonBufferedRegistry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *nonBufferedRegistry) PublishToOverlapping(
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

	reg.forOverlappingRegs(ctx, span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
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
func (reg *nonBufferedRegistry) Unregister(ctx context.Context, r *registration) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		log.Fatalf(ctx, "%v", err)
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
func (reg *nonBufferedRegistry) DisconnectAllOnShutdown(ctx context.Context, pErr *kvpb.Error) {
	reg.metrics.RangeFeedRegistrations.Dec(int64(reg.tree.Len()))
	reg.DisconnectWithErr(ctx, all, pErr)
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *nonBufferedRegistry) Disconnect(ctx context.Context, span roachpb.Span) {
	reg.DisconnectWithErr(ctx, span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *nonBufferedRegistry) DisconnectWithErr(
	ctx context.Context, span roachpb.Span, pErr *kvpb.Error,
) {
	reg.forOverlappingRegs(ctx, span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
		return true /* disconned */, pErr
	})
}

// all is a span that overlaps with all registrations.
var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *nonBufferedRegistry) forOverlappingRegs(
	ctx context.Context,
	span roachpb.Span,
	fn func(*nonBufferedRegistration) (disconnect bool, pErr *kvpb.Error),
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
