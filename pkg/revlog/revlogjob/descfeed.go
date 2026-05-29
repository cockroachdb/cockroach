// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// descRangefeedSource subscribes to the descriptor stream and pumps
// rangefeed events into events and errors into errs. The returned
// stop function tears down the subscription. The interface is the
// seam between runDescFeed and the rangefeed library: production
// uses factoryDescRangefeedSource (one rangefeed.Factory call against
// system.descriptor); tests use a fake that scripts events directly.
type descRangefeedSource interface {
	Subscribe(
		ctx context.Context,
		startTS hlc.Timestamp,
		events chan<- rangefeedEvent,
		errs chan<- error,
	) (stop func(), err error)
}

// factoryDescRangefeedSource subscribes one rangefeed against
// system.descriptor via a rangefeed.Factory. Fixed system.descriptor
// span resolved from the codec at construction.
type factoryDescRangefeedSource struct {
	factory  *rangefeed.Factory
	descSpan roachpb.Span
}

// newFactoryDescRangefeedSource builds a production
// descRangefeedSource backed by the given factory + codec.
func newFactoryDescRangefeedSource(
	factory *rangefeed.Factory, codec keys.SQLCodec,
) *factoryDescRangefeedSource {
	return &factoryDescRangefeedSource{
		factory: factory,
		descSpan: roachpb.Span{
			Key:    codec.DescMetadataPrefix(),
			EndKey: codec.DescMetadataPrefix().PrefixEnd(),
		},
	}
}

// Subscribe implements descRangefeedSource by opening one rangefeed
// against system.descriptor.
func (s *factoryDescRangefeedSource) Subscribe(
	ctx context.Context, startTS hlc.Timestamp, events chan<- rangefeedEvent, errs chan<- error,
) (func(), error) {
	rf, err := s.factory.RangeFeed(ctx, "revlog-descfeed",
		[]roachpb.Span{s.descSpan}, startTS,
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			select {
			case events <- rangefeedEvent{value: v}:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(false),
		rangefeed.WithOnCheckpoint(
			func(ctx context.Context, cp *kvpb.RangeFeedCheckpoint) {
				select {
				case events <- rangefeedEvent{checkpoint: cp}:
				case <-ctx.Done():
				}
			}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			select {
			case errs <- err:
			case <-ctx.Done():
			}
		}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "starting descriptor rangefeed")
	}
	return rf.Close, nil
}

// ErrScopeTerminated signals that scope.Terminated returned true
// and the writer should exit successfully. Callers translate it
// into clean job completion, not an error.
var ErrScopeTerminated = errors.New("revlogjob: scope terminated")

// replanSignal carries the per-iteration scope-widening payload from
// the descfeed to the outer flow loop. The outer loop cancels the
// running inner flow and re-plans with the new spans, using
// StartTS as the new flow's rangefeed start time.
type replanSignal struct {
	// Spans is the full new resolved span set the next flow should
	// run over. Includes both already-running and newly-introduced
	// spans.
	Spans []roachpb.Span
	// NewSpans is the subset of Spans that did not appear in any
	// prior coverage entry — i.e. the spans introduced by this
	// widening. The descfeed registers these with TickManager at
	// zero before signaling so the new flow's per-span frontier
	// accounts for them.
	NewSpans []roachpb.Span
	// StartTS is the timestamp the next flow's producer rangefeeds
	// should subscribe at: min(currentDataFrontier,
	// earliestWideningTsInBatch). Old spans are pre-forwarded to
	// their resume positions so the rangefeed library's per-span
	// init-scan gate skips them; new spans (at zero) get scanned at
	// StartTS, materializing their pre-existing state.
	StartTS hlc.Timestamp
}

// descFeedSignals is the descfeed's outbound channel of
// scope-state-change events to the outer flow loop. Buffered (1) so
// the descfeed doesn't block waiting for the outer loop to drain.
type descFeedSignals struct {
	// replan delivers the new resolved span set + restart start ts
	// when widening is detected. The descfeed writes the coverage
	// entry first, registers the new spans with TickManager, then
	// sends. The outer loop tears down the running flow and
	// re-plans with these spans.
	replan chan replanSignal
}

func newDescFeedSignals() *descFeedSignals {
	return &descFeedSignals{replan: make(chan replanSignal, 1)}
}

// runDescFeed subscribes one rangefeed on system.descriptor. Value
// events are buffered as they arrive; on each rangefeed checkpoint
// the buffered values with ts <= checkpoint.ResolvedTS are drained
// in HLC order and processed as a batch (see processBatch). Each
// checkpoint also forwards the manager's descriptor frontier so the
// tick close loop can advance.
//
// On a widening within a batch (a value that adds spans not in the
// previously-known scope) the descfeed registers the new spans with
// the manager's frontier at zero and signals exactly ONE replan to
// the outer flow loop. The signal carries the new full span set,
// the newly-introduced subset, and the restart start ts
// (min(currentDataFrontier, earliestWideningTsInBatch)).
//
// Returns when ctx is cancelled, the scope terminates
// (ErrScopeTerminated), or the rangefeed errors terminally.
func runDescFeed(
	ctx context.Context,
	source descRangefeedSource,
	codec keys.SQLCodec,
	scope Scope,
	manager *TickManager,
	es cloud.ExternalStorage,
	startHLC hlc.Timestamp,
	initialSpans []roachpb.Span,
	sigs *descFeedSignals,
) error {
	state := &descFeedState{
		scope:        scope,
		manager:      manager,
		es:           es,
		codec:        codec,
		sigs:         sigs,
		lastSpans:    cloneSpans(initialSpans),
		lastSpansSet: true,
	}

	eventsCh := make(chan rangefeedEvent, 256)
	errCh := make(chan error, 1)

	stop, err := source.Subscribe(ctx, startHLC, eventsCh, errCh)
	if err != nil {
		return errors.Wrap(err, "subscribing descriptor rangefeed")
	}
	defer stop()

	for {
		select {
		case ev := <-eventsCh:
			switch {
			case ev.value != nil:
				state.bufferValue(ev.value)
			case ev.checkpoint != nil:
				if err := state.processBatch(ctx, ev.checkpoint.ResolvedTS); err != nil {
					return err
				}
				if err := manager.ForwardDescFrontier(ctx, ev.checkpoint.ResolvedTS); err != nil {
					return err
				}
			}
		case err := <-errCh:
			return errors.Wrap(err, "descriptor rangefeed internal error")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// descFeedState carries the per-feed mutable state. Only the
// single dispatcher goroutine in runDescFeed touches it, so no
// locking is needed.
type descFeedState struct {
	scope   Scope
	manager *TickManager
	es      cloud.ExternalStorage
	codec   keys.SQLCodec
	sigs    *descFeedSignals

	// pending holds buffered descriptor-row updates awaiting the
	// next checkpoint at or above their MVCC ts. Each batch processes
	// pending entries in HLC order, then drops them. Coverage and
	// schema-delta writes are driven from the batch, not from
	// individual value arrivals.
	pending []*kvpb.RangeFeedValue

	// lastSpans is the most recently written coverage span set,
	// used to diff against newly-resolved spans on each in-scope
	// event. lastSpansSet=false means no coverage entry has been
	// written yet — the next span computation always writes.
	lastSpans    []roachpb.Span
	lastSpansSet bool
}

// bufferValue appends a value event to the pending buffer. Called
// once per rangefeed value arrival; the actual schema-delta and
// coverage writes happen later in processBatch keyed off the next
// checkpoint.
func (s *descFeedState) bufferValue(v *kvpb.RangeFeedValue) {
	s.pending = append(s.pending, v)
}

// processBatch drains every pending value with ts <= resolvedTS in
// HLC order, writes its schema delta, advances the known coverage
// across each per-value scope transition, and (if any value widened
// scope) registers new spans with the manager and sends one replan
// signal at the end of the batch.
//
// Termination is checked at the end of the batch if the resolved
// span set is empty at resolvedTS — the only state in which
// scope.Terminated can flip true.
func (s *descFeedState) processBatch(ctx context.Context, resolvedTS hlc.Timestamp) error {
	if len(s.pending) == 0 {
		// Even with no values, an empty-scope termination check
		// is harmless: it short-circuits unless lastSpans is
		// already empty, which is rare. Keep the early return so
		// we don't pay it on every checkpoint.
		return nil
	}

	// Pop entries with ts <= resolvedTS. Stable-sort the prefix in
	// HLC order so coverage entries are written in the order
	// transitions actually committed.
	keep := s.pending[:0]
	batch := make([]*kvpb.RangeFeedValue, 0, len(s.pending))
	for _, v := range s.pending {
		if v.Value.Timestamp.LessEq(resolvedTS) {
			batch = append(batch, v)
		} else {
			keep = append(keep, v)
		}
	}
	s.pending = keep
	if len(batch) == 0 {
		return nil
	}
	sort.SliceStable(batch, func(i, j int) bool {
		return batch[i].Value.Timestamp.Less(batch[j].Value.Timestamp)
	})

	var earliestWideningTs hlc.Timestamp
	var newSpansAggregate []roachpb.Span
	var lastBatchSpans []roachpb.Span
	for _, v := range batch {
		spans, widened, newSpans, err := s.handleBatchedValue(ctx, v)
		if err != nil {
			return err
		}
		lastBatchSpans = spans
		if widened {
			if earliestWideningTs.IsEmpty() || v.Value.Timestamp.Less(earliestWideningTs) {
				earliestWideningTs = v.Value.Timestamp
			}
			newSpansAggregate = unionSpans(newSpansAggregate, newSpans)
		}
	}

	if !earliestWideningTs.IsEmpty() {
		// Pick the restart start ts: min(currentDataFrontier,
		// earliestWideningTs). The data frontier may be higher
		// than the widening (we've already flushed past it for
		// pre-existing spans), in which case starting the new
		// rangefeed at the widening ts would re-deliver events
		// already in the log; conversely, the widening ts may be
		// higher (we're behind), in which case starting at the
		// data frontier captures everything.
		//
		// Compute this BEFORE registering the new spans with the
		// manager: AddSpansAt seeds the new spans at zero, which
		// would drag the manager's data frontier to zero and
		// destroy the min logic.
		startTS := s.manager.DataFrontier()
		if earliestWideningTs.Less(startTS) {
			startTS = earliestWideningTs
		}
		// Register the new spans with the manager's frontier at
		// zero so the next flow's per-span ForwardFrontier calls
		// for them aren't silently dropped and so tick-close
		// gating waits for them too. Do this before the replan
		// signal so the outer loop's snapshot picks them up.
		if err := s.manager.AddSpansAt(newSpansAggregate, hlc.Timestamp{}); err != nil {
			return errors.Wrap(err, "registering newly-in-scope spans with manager")
		}
		signal := replanSignal{
			Spans:    cloneSpans(lastBatchSpans),
			NewSpans: cloneSpans(newSpansAggregate),
			StartTS:  startTS,
		}
		// Drain-and-replace: the channel is buffered-1 and holds
		// the *oldest* unread value, so a naive non-blocking send
		// would silently drop this widening's payload if the
		// outer flow loop hasn't drained the previous one.
		// Replace it so the next replan uses the latest payload
		// (which subsumes any older widening's spans by
		// definition).
		select {
		case s.sigs.replan <- signal:
		default:
			<-s.sigs.replan
			s.sigs.replan <- signal
		}
	}

	// Empty resolved spans is the only state in which Terminated
	// can flip true; skip the check otherwise (avoids a catalog
	// load per batch).
	if len(lastBatchSpans) == 0 {
		terminated, err := s.scope.Terminated(ctx, resolvedTS)
		if err != nil {
			return errors.Wrap(err, "checking scope termination")
		}
		if terminated {
			// TODO(dt): defer termination until the next tick
			// boundary closes past resolvedTS. As-is, any events
			// the producer has buffered for the still-open tick
			// (the partial tick between the last close and the
			// scope-dissolving change) are lost when the flow is
			// torn down. A RESTORE at AOST in that partial-tick
			// window would see "table existed, no events." The
			// fix: wait for manager.LastClosed() > resolvedTS
			// before returning ErrScopeTerminated, letting the
			// in-flight tick close normally.
			return ErrScopeTerminated
		}
	}
	return nil
}

// handleBatchedValue processes one descriptor-row update from a
// batch: writes the schema delta, recomputes the resolved span set,
// and writes a coverage entry if the span set differs from the
// previously-known one. Returns the post-value span set, whether
// the change widened scope (added spans not previously covered),
// and the set of newly-introduced spans (empty if not widening).
//
// Tombstones (zero-payload rangefeed values) are always written
// without consulting Matches — the prior version's row was in
// scope for us to observe it at all, and tombstones are tiny.
func (s *descFeedState) handleBatchedValue(
	ctx context.Context, v *kvpb.RangeFeedValue,
) (spans []roachpb.Span, widened bool, newSpans []roachpb.Span, _ error) {
	descID, err := s.codec.DecodeDescMetadataID(v.Key)
	if err != nil {
		log.Dev.VInfof(ctx, 2,
			"revlogjob: descfeed skipping non-descriptor key %s: %v", v.Key, err)
		return s.lastSpans, false, nil, nil
	}
	desc, err := decodeDescriptorValue(&v.Value)
	if err != nil {
		return nil, false, nil, errors.Wrapf(err,
			"decoding descriptor %d at %s", descID, v.Value.Timestamp)
	}
	if desc != nil && !s.scope.Matches(desc) {
		return s.lastSpans, false, nil, nil
	}

	if err := revlog.WriteSchemaDesc(
		ctx, s.es, v.Value.Timestamp, descpb.ID(descID), desc,
	); err != nil {
		return nil, false, nil, errors.Wrapf(err,
			"writing schema delta for desc %d at %s", descID, v.Value.Timestamp)
	}

	newSpanSet, err := s.scope.Spans(ctx, v.Value.Timestamp)
	if err != nil {
		return nil, false, nil, errors.Wrap(err,
			"recomputing scope spans after descriptor change")
	}
	if s.lastSpansSet && sameSpans(s.lastSpans, newSpanSet) {
		return s.lastSpans, false, nil, nil
	}

	widened = s.lastSpansSet && !spansSubset(newSpanSet, s.lastSpans)
	if widened {
		newSpans = spansDifference(newSpanSet, s.lastSpans)
	}
	s.lastSpans = cloneSpans(newSpanSet)
	s.lastSpansSet = true
	if err := revlog.WriteCoverage(ctx, s.es, revlogpb.Coverage{
		EffectiveFrom: v.Value.Timestamp,
		Scope:         s.scope.String(),
		Spans:         newSpanSet,
	}); err != nil {
		return nil, false, nil, errors.Wrapf(err,
			"writing coverage transition at %s", v.Value.Timestamp)
	}
	return newSpanSet, widened, newSpans, nil
}

// decodeDescriptorValue extracts a *descpb.Descriptor from a
// rangefeed value. A zero-length value (tombstone — the row was
// deleted from system.descriptor) is signaled by returning
// (nil, nil).
func decodeDescriptorValue(v *roachpb.Value) (*descpb.Descriptor, error) {
	if len(v.RawBytes) == 0 {
		return nil, nil
	}
	b, err := descbuilder.FromSerializedValue(v)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return b.BuildImmutable().DescriptorProto(), nil
}

// sameSpans reports whether two span slices contain the same
// spans in the same order. Coverage diff cares only about set
// equality, but spansForAllTableIndexes returns merged-and-sorted
// spans deterministically, so order equality is a tighter and
// cheaper check.
func sameSpans(a, b []roachpb.Span) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].EndKey, b[i].EndKey) {
			return false
		}
	}
	return true
}

// spansSubset reports whether every span in inner is fully
// contained in some span of outer. Both inputs are
// merged-and-sorted (per spansForAllTableIndexes), so a linear
// walk with a moving outer cursor is sufficient.
func spansSubset(inner, outer []roachpb.Span) bool {
	j := 0
	for _, in := range inner {
		for j < len(outer) && bytes.Compare(outer[j].EndKey, in.Key) <= 0 {
			j++
		}
		if j >= len(outer) {
			return false
		}
		if bytes.Compare(outer[j].Key, in.Key) > 0 ||
			bytes.Compare(in.EndKey, outer[j].EndKey) > 0 {
			return false
		}
	}
	return true
}

// spansDifference returns the spans in superset that are not
// covered by subset. Both inputs are merged-and-sorted; the result
// is the keyspace newly introduced by superset relative to subset.
//
// For the descfeed's widening case we typically expect each
// newly-introduced span to be entirely outside the prior coverage
// (a CREATE TABLE allocates a fresh /Table/<id>/ keyspace), so the
// implementation is the simple "emit the parts of each superset
// span not covered by any subset span" walk; partial overlaps
// degrade gracefully.
func spansDifference(superset, subset []roachpb.Span) []roachpb.Span {
	var out []roachpb.Span
	for _, sup := range superset {
		cur := sup
		for _, sub := range subset {
			if bytes.Compare(sub.EndKey, cur.Key) <= 0 {
				continue
			}
			if bytes.Compare(sub.Key, cur.EndKey) >= 0 {
				break
			}
			// sub overlaps cur: trim cur on the left, then split.
			if bytes.Compare(sub.Key, cur.Key) > 0 {
				out = append(out, roachpb.Span{Key: cur.Key, EndKey: sub.Key})
			}
			if bytes.Compare(sub.EndKey, cur.EndKey) >= 0 {
				cur = roachpb.Span{}
				break
			}
			cur = roachpb.Span{Key: sub.EndKey, EndKey: cur.EndKey}
		}
		if cur.Key != nil && bytes.Compare(cur.Key, cur.EndKey) < 0 {
			out = append(out, cur)
		}
	}
	return out
}

// unionSpans returns the union of two merged-and-sorted span
// slices. Used to aggregate newly-introduced spans across the
// values in one batch before registering them with the manager.
func unionSpans(a, b []roachpb.Span) []roachpb.Span {
	if len(a) == 0 {
		return cloneSpans(b)
	}
	if len(b) == 0 {
		return cloneSpans(a)
	}
	combined := make([]roachpb.Span, 0, len(a)+len(b))
	combined = append(combined, a...)
	combined = append(combined, b...)
	sort.Slice(combined, func(i, j int) bool {
		return bytes.Compare(combined[i].Key, combined[j].Key) < 0
	})
	out := combined[:0]
	for _, sp := range combined {
		if n := len(out); n > 0 && bytes.Compare(out[n-1].EndKey, sp.Key) >= 0 {
			if bytes.Compare(out[n-1].EndKey, sp.EndKey) < 0 {
				out[n-1].EndKey = sp.EndKey
			}
			continue
		}
		out = append(out, sp)
	}
	return out
}

// cloneSpans returns a deep copy of the given span slice.
func cloneSpans(in []roachpb.Span) []roachpb.Span {
	out := make([]roachpb.Span, len(in))
	for i, sp := range in {
		out[i].Key = append(roachpb.Key(nil), sp.Key...)
		out[i].EndKey = append(roachpb.Key(nil), sp.EndKey...)
	}
	// Defensive sort — caller may already pass sorted spans.
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	return out
}
