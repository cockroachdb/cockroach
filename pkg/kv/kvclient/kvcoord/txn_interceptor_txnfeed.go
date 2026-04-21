// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

// TrackedReadsMaxSize is a byte threshold for tracking the read spans of a
// transaction for TxnFeed consumers. When the read footprint exceeds this
// threshold, spans are condensed (merged within range boundaries) to reduce
// memory usage. Unlike the txnSpanRefresher, which drops all spans when its
// budget is exceeded, the txnFeedReadTracker always retains a (possibly
// condensed) approximation of the read footprint.
var TrackedReadsMaxSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.transaction.max_read_spans_bytes",
	"maximum number of bytes used to track read spans for txnfeed in transactions",
	1<<22, /* 4 MB */
	settings.WithPublic,
)

// txnFeedReadTracker is a txnInterceptor that tracks the read spans of a
// transaction for TxnFeed consumers. When a transaction commits and
// kv.txnfeed.enabled is true, the accumulated read spans are attached to the
// EndTxnRequest so that the server can include them in CommitTxnOp events.
//
// Read spans are tracked independently from the txnSpanRefresher's refresh
// footprint because the two have different lifecycle and condensing semantics.
// The span refresher clears all spans when its memory budget is exceeded
// (losing fidelity entirely), and also clears spans on read-timestamp steps
// (Read Committed). The txnFeedReadTracker instead condenses spans when over
// budget (matching the pipeliner's lockFootprint behavior). Spans are only
// cleared on epoch bumps, since a restarted epoch invalidates prior reads.
// This ensures that TxnFeed consumers always receive at least an approximation
// of the read footprint.
type txnFeedReadTracker struct {
	wrapped lockedSender
	st      *cluster.Settings
	riGen   rangeIteratorFactory

	// readFootprint contains key spans which were read during the transaction.
	// These are accumulated on every successful response and condensed when the
	// memory budget (TrackedReadsMaxSize) is exceeded. On a committing EndTxn
	// with txnfeed enabled, the spans are attached to the EndTxnRequest.
	readFootprint condensableSpanSet
}

// SendLocked implements the lockedSender interface.
func (t *txnFeedReadTracker) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// If the batch contains a committing EndTxn and txnfeed is enabled, attach
	// the accumulated read spans before forwarding.
	enabled := kvserverbase.TxnFeedEnabled.Get(&t.st.SV)
	if enabled {
		if args, hasET := ba.GetArg(kvpb.EndTxn); hasET {
			et := args.(*kvpb.EndTxnRequest)
			if et.Commit && !t.readFootprint.empty() {
				et.ReadSpans = append(
					et.ReadSpans[:0:0], t.readFootprint.asSlice()...,
				)
			}
		}
	}

	br, pErr := t.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// Track read spans from the response. Only track when txnfeed is enabled
	// and the transaction is still pending (no point tracking after commit).
	if br.Txn == nil || br.Txn.Status != roachpb.PENDING {
		return br, nil
	}
	if !enabled {
		return br, nil
	}
	if err := t.appendReadSpans(ba, br); err != nil {
		return nil, kvpb.NewError(err)
	}
	t.maybeCondenseReadSpans(ctx)
	return br, nil
}

// appendReadSpans extracts read spans from the batch request/response and
// inserts them into the read footprint.
func (t *txnFeedReadTracker) appendReadSpans(ba *kvpb.BatchRequest, br *kvpb.BatchResponse) error {
	return ba.RefreshSpanIterate(br, func(span roachpb.Span) {
		t.readFootprint.insert(span)
	})
}

// maybeCondenseReadSpans condenses the read footprint if it exceeds the memory
// budget. Unlike the span refresher, which invalidates and clears spans on
// budget exhaustion, this method always retains the condensed spans.
func (t *txnFeedReadTracker) maybeCondenseReadSpans(ctx context.Context) {
	maxBytes := TrackedReadsMaxSize.Get(&t.st.SV)
	if t.readFootprint.bytes >= maxBytes {
		t.readFootprint.maybeCondense(ctx, t.riGen, maxBytes)
	}
}

// setWrapped implements the txnInterceptor interface.
func (t *txnFeedReadTracker) setWrapped(wrapped lockedSender) {
	t.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnFeedReadTracker) populateLeafInputState(*roachpb.LeafTxnInputState, interval.Tree) {
}

// initializeLeaf is part of the txnInterceptor interface.
func (*txnFeedReadTracker) initializeLeaf(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnFeedReadTracker) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnFeedReadTracker) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (t *txnFeedReadTracker) epochBumpedLocked() {
	t.readFootprint.clear()
}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnFeedReadTracker) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnFeedReadTracker) rollbackToSavepointLocked(context.Context, savepoint) {}

// releaseSavepointLocked is part of the txnInterceptor interface.
func (*txnFeedReadTracker) releaseSavepointLocked(context.Context, *savepoint) {}

// closeLocked implements the txnInterceptor interface.
func (*txnFeedReadTracker) closeLocked() {}
