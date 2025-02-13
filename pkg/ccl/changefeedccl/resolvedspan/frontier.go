// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resolvedspan

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/checkpoint"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// AggregatorFrontier wraps a resolvedSpanFrontier with additional
// checks specific to how change aggregators process boundaries.
type AggregatorFrontier struct {
	resolvedSpanFrontier
}

// NewAggregatorFrontier returns a new AggregatorFrontier.
func NewAggregatorFrontier(
	statementTime hlc.Timestamp, initialHighWater hlc.Timestamp, spans ...roachpb.Span,
) (*AggregatorFrontier, error) {
	rsf, err := newResolvedSpanFrontier(statementTime, initialHighWater, spans...)
	if err != nil {
		return nil, err
	}
	return &AggregatorFrontier{
		resolvedSpanFrontier: *rsf,
	}, nil
}

// ForwardResolvedSpan forwards the progress of a resolved span and also does
// some boundary validation.
func (f *AggregatorFrontier) ForwardResolvedSpan(
	ctx context.Context, r jobspb.ResolvedSpan,
) (bool, error) {
	switch boundaryType := r.BoundaryType; boundaryType {
	case jobspb.ResolvedSpan_NONE:
	case jobspb.ResolvedSpan_BACKFILL, jobspb.ResolvedSpan_EXIT, jobspb.ResolvedSpan_RESTART:
		// Boundary resolved events should be ingested from the schema feed
		// serially, where the changefeed won't ever observe a new schema change
		// boundary until it has progressed past the current boundary.
		if err := f.assertBoundaryNotEarlier(ctx, r); err != nil {
			return false, err
		}
	default:
		return false, errors.AssertionFailedf("unknown boundary type: %v", boundaryType)
	}
	return f.resolvedSpanFrontier.ForwardResolvedSpan(r)
}

// EntriesWithBoundaryType is analogous to Entries on span.Frontier, with the
// difference that the supplied callback also accepts the boundary type of the
// span, which will be non-NONE for spans at the boundary.
func (f *AggregatorFrontier) EntriesWithBoundaryType(
	fn func(roachpb.Span, hlc.Timestamp, jobspb.ResolvedSpan_BoundaryType) (done span.OpResult),
) {
	f.Entries(func(span roachpb.Span, timestamp hlc.Timestamp) (done span.OpResult) {
		var boundaryType jobspb.ResolvedSpan_BoundaryType
		if ok, bt := f.boundary.At(timestamp); ok {
			boundaryType = bt
		}
		return fn(span, timestamp, boundaryType)
	})
}

// CoordinatorFrontier wraps a resolvedSpanFrontier with additional
// checks specific to how the coordinator/change frontier processes boundaries.
type CoordinatorFrontier struct {
	resolvedSpanFrontier

	// backfills is a sorted list of timestamps for ongoing backfills.
	// Usually there will only be one, but since aggregators run
	// backfills in parallel without synchronization, there may be
	// multiple backfills happening at one time.
	backfills []hlc.Timestamp
}

// NewCoordinatorFrontier returns a new CoordinatorFrontier.
func NewCoordinatorFrontier(
	statementTime hlc.Timestamp, initialHighWater hlc.Timestamp, spans ...roachpb.Span,
) (*CoordinatorFrontier, error) {
	rsf, err := newResolvedSpanFrontier(statementTime, initialHighWater, spans...)
	if err != nil {
		return nil, err
	}
	return &CoordinatorFrontier{
		resolvedSpanFrontier: *rsf,
	}, nil
}

// ForwardResolvedSpan forwards the progress of a resolved span and also does
// some boundary validation.
func (f *CoordinatorFrontier) ForwardResolvedSpan(
	ctx context.Context, r jobspb.ResolvedSpan,
) (bool, error) {
	switch boundaryType := r.BoundaryType; boundaryType {
	case jobspb.ResolvedSpan_NONE:
	case jobspb.ResolvedSpan_BACKFILL:
		// The coordinator frontier collects resolved spans from all the
		// aggregators. Since a BACKFILL schema change does not cause an
		// aggregator to shut down, an aggregator may encounter a second
		// schema change (and send resolved spans for that second schema
		// change) before the frontier has received resolved spans for the
		// first BACKFILL schema change from all aggregators. Thus, as long as
		// it is a BACKFILL we have already seen, then it is fine for it to be
		// an earlier timestamp than the latest boundary.
		boundaryTS := r.Timestamp
		_, ok := slices.BinarySearchFunc(f.backfills, boundaryTS, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
			return elem.Compare(ts)
		})
		if ok {
			break
		}
		if err := f.assertBoundaryNotEarlier(ctx, r); err != nil {
			return false, err
		}
		f.backfills = append(f.backfills, boundaryTS)
	case jobspb.ResolvedSpan_EXIT, jobspb.ResolvedSpan_RESTART:
		// EXIT and RESTART are final boundaries that cause the changefeed
		// processors to all move to draining and so should not be followed
		// by any other boundaries.
		if err := f.assertBoundaryNotEarlier(ctx, r); err != nil {
			return false, err
		}
	default:
		return false, errors.AssertionFailedf("unknown boundary type: %v", boundaryType)
	}
	frontierChanged, err := f.resolvedSpanFrontier.ForwardResolvedSpan(r)
	if err != nil {
		return false, err
	}
	// If the frontier changed, we check if the frontier has advanced past any known backfills.
	if frontierChanged {
		frontier := f.Frontier()
		i, _ := slices.BinarySearchFunc(f.backfills, frontier, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
			return elem.Compare(ts)
		})
		f.backfills = f.backfills[i:]
	}
	return frontierChanged, nil
}

// InBackfill returns whether a resolved span is part of an ongoing backfill
// (either an initial scan backfill or a schema change backfill).
// NB: Since the CoordinatorFrontier consolidates the frontiers of
// multiple change aggregators, there may be more than one concurrent backfill
// happening at different timestamps.
func (f *CoordinatorFrontier) InBackfill(r jobspb.ResolvedSpan) bool {
	boundaryTS := r.Timestamp
	_, ok := slices.BinarySearchFunc(f.backfills, boundaryTS, func(elem hlc.Timestamp, ts hlc.Timestamp) int {
		return elem.Next().Compare(ts)
	})
	if ok {
		return true
	}

	return f.resolvedSpanFrontier.InBackfill(r)
}

// MakeCheckpoint creates a checkpoint based on the current state of the frontier.
func (f *CoordinatorFrontier) MakeCheckpoint(
	maxBytes int64, metrics *checkpoint.Metrics,
) *jobspb.TimestampSpansMap {
	return checkpoint.Make(f.Frontier(), f.Entries, maxBytes, metrics)
}

// spanFrontier is a type alias to make it possible to embed and forward calls
// (e.g. Frontier()) to the underlying span.Frontier.
type spanFrontier = span.Frontier

// resolvedSpanFrontier wraps a spanFrontier with additional bookkeeping fields
// used to track resolved spans for a changefeed and methods for computing
// lagging and checkpoint spans.
type resolvedSpanFrontier struct {
	spanFrontier

	// statementTime is the statement time of the changefeed.
	statementTime hlc.Timestamp

	// initialHighWater is either zero for a new changefeed or the
	// recovered highwater mark for a resumed changefeed.
	initialHighWater hlc.Timestamp

	// latestTS indicates the most recent timestamp that any span in the frontier
	// has ever been forwarded to.
	latestTS hlc.Timestamp

	// boundary stores the latest-known non-NONE resolved span boundary.
	boundary resolvedSpanBoundary
}

// newResolvedSpanFrontier returns a new resolvedSpanFrontier.
func newResolvedSpanFrontier(
	statementTime hlc.Timestamp, initialHighWater hlc.Timestamp, spans ...roachpb.Span,
) (*resolvedSpanFrontier, error) {
	sf, err := span.MakeFrontierAt(initialHighWater, spans...)
	if err != nil {
		return nil, err
	}
	sf = span.MakeConcurrentFrontier(sf)
	return &resolvedSpanFrontier{
		spanFrontier:     sf,
		statementTime:    statementTime,
		initialHighWater: initialHighWater,
	}, nil
}

// ForwardResolvedSpan forwards the progress of a resolved span.
func (f *resolvedSpanFrontier) ForwardResolvedSpan(r jobspb.ResolvedSpan) (bool, error) {
	f.latestTS.Forward(r.Timestamp)
	if r.BoundaryType != jobspb.ResolvedSpan_NONE {
		newBoundary := resolvedSpanBoundary{
			ts:  r.Timestamp,
			typ: r.BoundaryType,
		}
		f.boundary.Forward(newBoundary)
	}
	return f.Forward(r.Span, r.Timestamp)
}

// AtBoundary returns true at the single moment when all watched spans
// have reached a boundary and no spans after the boundary have been received.
func (f *resolvedSpanFrontier) AtBoundary() (
	bool,
	jobspb.ResolvedSpan_BoundaryType,
	hlc.Timestamp,
) {
	frontier := f.Frontier()
	frontierAtBoundary, boundaryType := f.boundary.At(frontier)
	if !frontierAtBoundary {
		return false, 0, hlc.Timestamp{}
	}
	latestAtBoundary, _ := f.boundary.At(f.latestTS)
	if !latestAtBoundary {
		return false, 0, hlc.Timestamp{}
	}
	return true, boundaryType, frontier
}

// InBackfill returns whether a resolved span is part of an ongoing backfill
// (either an initial scan backfill or a schema change backfill).
func (f *resolvedSpanFrontier) InBackfill(r jobspb.ResolvedSpan) bool {
	frontier := f.Frontier()

	// The scan for the initial backfill results in spans sent at statementTime.
	if frontier.IsEmpty() {
		return r.Timestamp.Equal(f.statementTime)
	}

	// If the backfill is occurring after any initial scan (non-empty frontier),
	// then it can only be in a schema change backfill, where the scan is
	// performed immediately after the boundary timestamp.
	atBoundary, typ := f.boundary.At(frontier)
	backfilling := atBoundary && typ == jobspb.ResolvedSpan_BACKFILL
	// If the schema change backfill was paused and resumed, the initialHighWater
	// is read from the job progress and is equal to the old BACKFILL boundary.
	restarted := frontier.Equal(f.initialHighWater)
	if backfilling || restarted {
		return r.Timestamp.Equal(frontier.Next())
	}

	return false
}

// assertBoundaryNotEarlier is a helper method provided to assert that a
// resolved span does not have an earlier boundary than the existing one.
func (f *resolvedSpanFrontier) assertBoundaryNotEarlier(
	ctx context.Context, r jobspb.ResolvedSpan,
) error {
	boundaryType := r.BoundaryType
	if boundaryType == jobspb.ResolvedSpan_NONE {
		return errors.AssertionFailedf("assertBoundaryNotEarlier should not be called for NONE boundary")
	}
	boundaryTS := r.Timestamp
	if f.boundary.After(boundaryTS) {
		newBoundary := newResolvedSpanBoundary(boundaryTS, boundaryType)
		err := errors.AssertionFailedf("received resolved span for %s "+
			"with %v, which is earlier than previously received %v",
			r.Span, newBoundary, f.boundary)
		log.Errorf(ctx, "error while forwarding boundary resolved span: %v", err)
		return err
	}
	return nil
}

// HasLaggingSpans returns whether the frontier has lagging spans as defined
// by whether the frontier trails the latest timestamp by at least
// changefeedbase.SpanCheckpointLagThreshold.
func (f *resolvedSpanFrontier) HasLaggingSpans(sv *settings.Values) bool {
	lagThresholdNanos := int64(changefeedbase.SpanCheckpointLagThreshold.Get(sv))
	if lagThresholdNanos == 0 {
		return false
	}
	frontier := f.Frontier()
	if frontier.IsEmpty() {
		frontier = f.statementTime
	}
	return frontier.Add(lagThresholdNanos, 0).Less(f.latestTS)
}

// resolvedSpanBoundary encapsulates a resolved span boundary, which is
// the timestamp and type of boundary. Boundaries are usually the
// result of schema changes but can also occur if a changefeed is
// initial-scan-only or has an end time configured. The type of the
// boundary indicates what action the changefeed should take when the
// frontier reaches the boundary timestamp.
//
// resolvedSpanBoundary values are communicated to the changeFrontier via
// resolved messages sent from the changeAggregator's. The policy regarding
// which schema change events lead to a resolvedSpanBoundary is controlled
// by the KV feed based on OptSchemaChangeEvents and OptSchemaChangePolicy.
//
// When the changeFrontier receives a ResolvedSpan with a final boundary type
// (i.e. EXIT or RESTART), it will wait until the boundary is reached by all
// watched spans and then drain and exit/restart the changefeed.
type resolvedSpanBoundary struct {
	ts  hlc.Timestamp
	typ jobspb.ResolvedSpan_BoundaryType
}

// newResolvedSpanBoundary returns a new resolvedSpanBoundary.
func newResolvedSpanBoundary(
	ts hlc.Timestamp, typ jobspb.ResolvedSpan_BoundaryType,
) *resolvedSpanBoundary {
	return &resolvedSpanBoundary{ts: ts, typ: typ}
}

// At returns whether a timestamp is equal to the boundary timestamp
// and if so, the boundary type as well.
func (b *resolvedSpanBoundary) At(ts hlc.Timestamp) (bool, jobspb.ResolvedSpan_BoundaryType) {
	if ts.Equal(b.ts) {
		return true, b.typ
	}
	return false, 0
}

// After returns whether a timestamp is later than the boundary timestamp.
func (b *resolvedSpanBoundary) After(ts hlc.Timestamp) bool {
	return ts.Less(b.ts)
}

// Forward forwards the boundary to the new boundary if it is later.
// It returns true if the boundary changed and false otherwise.
func (b *resolvedSpanBoundary) Forward(newBoundary resolvedSpanBoundary) bool {
	if newBoundary.After(b.ts) {
		*b = newBoundary
		return true
	}
	return false
}

// SafeFormat implements the redact.SafeFormatter interface.
func (b *resolvedSpanBoundary) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("%v boundary (%v)", b.typ, b.ts)
}
