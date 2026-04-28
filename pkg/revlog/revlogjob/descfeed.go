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

// ErrScopeTerminated signals that scope.Terminated returned true
// and the writer should exit successfully. Callers translate it
// into clean job completion, not an error.
var ErrScopeTerminated = errors.New("revlogjob: scope terminated")

// descFeedSignals is the descfeed's outbound channel of
// scope-state-change events to the outer flow loop. Both fields
// are buffered (1) so the descfeed doesn't block waiting for the
// outer loop to drain.
type descFeedSignals struct {
	// replan delivers the new resolved span set when widening (or
	// any non-narrowing change) is detected. The descfeed writes
	// the coverage entry first, then sends. The outer loop tears
	// down the running flow and re-plans with these spans.
	replan chan []roachpb.Span
}

func newDescFeedSignals() *descFeedSignals {
	return &descFeedSignals{replan: make(chan []roachpb.Span, 1)}
}

// runDescFeed subscribes one rangefeed on system.descriptor and
// dispatches its events to handleValue (descriptor changes) and
// the manager's descriptor frontier (checkpoints). Returns when
// ctx is cancelled, the scope terminates, or the rangefeed
// errors terminally.
//
// On widening events handleValue writes the new coverage entry
// and posts the new span set to sigs.replan; the caller is
// expected to tear down its current DistSQL flow and re-plan
// with the new spans (so the previously-uncovered new spans
// actually get a producer rangefeed subscribed to them).
func runDescFeed(
	ctx context.Context,
	factory *rangefeed.Factory,
	codec keys.SQLCodec,
	scope Scope,
	manager *TickManager,
	es cloud.ExternalStorage,
	startHLC hlc.Timestamp,
	initialSpans []roachpb.Span,
	sigs *descFeedSignals,
) error {
	descSpan := roachpb.Span{
		Key:    codec.DescMetadataPrefix(),
		EndKey: codec.DescMetadataPrefix().PrefixEnd(),
	}

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

	rf, err := factory.RangeFeed(ctx, "revlog-descfeed",
		[]roachpb.Span{descSpan}, startHLC,
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			select {
			case eventsCh <- rangefeedEvent{value: v}:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(false),
		rangefeed.WithOnCheckpoint(
			func(ctx context.Context, cp *kvpb.RangeFeedCheckpoint) {
				select {
				case eventsCh <- rangefeedEvent{checkpoint: cp}:
				case <-ctx.Done():
				}
			}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
		}),
	)
	if err != nil {
		return errors.Wrap(err, "starting descriptor rangefeed")
	}
	defer rf.Close()

	for {
		select {
		case ev := <-eventsCh:
			switch {
			case ev.value != nil:
				if err := state.handleValue(ctx, ev.value); err != nil {
					return err
				}
			case ev.checkpoint != nil:
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

	// lastSpans is the most recently written coverage span set,
	// used to diff against newly-resolved spans on each
	// in-scope event. lastSpansSet=false means no coverage entry
	// has been written yet — the next span computation always
	// writes.
	lastSpans    []roachpb.Span
	lastSpansSet bool
}

// handleValue processes one descriptor-row update: writes the
// schema delta, writes a coverage entry if the resolved span set
// changed, and signals termination if the scope dissolved.
//
// Tombstones (zero-payload rangefeed values) are always written
// without consulting Matches — the prior version's row was in
// scope for us to observe it at all, and tombstones are tiny.
func (s *descFeedState) handleValue(ctx context.Context, v *kvpb.RangeFeedValue) error {
	descID, err := s.codec.DecodeDescMetadataID(v.Key)
	if err != nil {
		log.Dev.VInfof(ctx, 2,
			"revlogjob: descfeed skipping non-descriptor key %s: %v", v.Key, err)
		return nil
	}
	desc, err := decodeDescriptorValue(&v.Value)
	if err != nil {
		return errors.Wrapf(err, "decoding descriptor %d at %s", descID, v.Value.Timestamp)
	}
	if desc != nil && !s.scope.Matches(desc) {
		return nil
	}

	if err := revlog.WriteSchemaDesc(
		ctx, s.es, v.Value.Timestamp, descpb.ID(descID), desc,
	); err != nil {
		return errors.Wrapf(err,
			"writing schema delta for desc %d at %s", descID, v.Value.Timestamp)
	}

	newSpans, err := s.scope.Spans(ctx, v.Value.Timestamp)
	if err != nil {
		return errors.Wrap(err, "recomputing scope spans after descriptor change")
	}
	if !s.lastSpansSet || !sameSpans(s.lastSpans, newSpans) {
		widening := s.lastSpansSet && !spansSubset(newSpans, s.lastSpans)
		s.lastSpans = cloneSpans(newSpans)
		s.lastSpansSet = true
		if err := revlog.WriteCoverage(ctx, s.es, revlogpb.Coverage{
			EffectiveFrom: v.Value.Timestamp,
			Scope:         s.scope.String(),
			Spans:         newSpans,
		}); err != nil {
			return errors.Wrapf(err,
				"writing coverage transition at %s", v.Value.Timestamp)
		}
		// Widening: the running producers' rangefeed subscriptions
		// don't include the newly-in-scope keyspace, so events for
		// it would never reach the log. Signal the outer flow loop
		// to tear down and re-plan with the new spans; the new
		// producers' rangefeeds catch up the missed window from the
		// resumed frontier.
		if widening {
			// Drain-and-replace: the channel is buffered-1 and holds
			// the *oldest* unread value, so a naive non-blocking send
			// would silently drop this widening's spans if the outer
			// flow loop hasn't yet drained a previous signal. Replace
			// the buffered value so the next replan uses the latest
			// spans (which subsume any older widening's by definition).
			signal := cloneSpans(newSpans)
			select {
			case s.sigs.replan <- signal:
			default:
				<-s.sigs.replan
				s.sigs.replan <- signal
			}
		}
	}

	// Empty resolved spans is the only state in which Terminated
	// can flip true; skip the check otherwise (avoids a catalog
	// load per descriptor change).
	if len(newSpans) == 0 {
		terminated, err := s.scope.Terminated(ctx, v.Value.Timestamp)
		if err != nil {
			return errors.Wrap(err, "checking scope termination")
		}
		if terminated {
			// TODO(dt): defer termination until the next tick
			// boundary closes past v.Value.Timestamp. As-is, any
			// events the producer has buffered for the still-open
			// tick (the partial tick between the last close and the
			// scope-dissolving change) are lost when the flow is
			// torn down. A RESTORE at AOST in that partial-tick
			// window would see "table existed, no events." The fix:
			// wait for manager.LastClosed() > v.Value.Timestamp
			// before returning ErrScopeTerminated, letting the
			// in-flight tick close normally.
			return ErrScopeTerminated
		}
	}
	return nil
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
