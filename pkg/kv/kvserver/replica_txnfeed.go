// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// TxnFeed registers a TxnFeed stream on this replica. It runs a catch-up scan
// under raftMu, then registers the stream for live events. The returned
// Disconnector can be used to tear down the registration.
func (r *Replica) TxnFeed(
	ctx context.Context, args *kvpb.TxnFeedRequest, stream txnfeed.Stream,
) (txnfeed.Disconnector, error) {
	ctx = r.AnnotateCtx(ctx)

	if !txnfeed.Enabled.Get(&r.ClusterSettings().SV) {
		return nil, errors.New("kv.txnfeed.enabled is not set")
	}

	rSpan, err := keys.SpanAddr(args.AnchorSpan)
	if err != nil {
		return nil, err
	}

	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		checkTS = r.Clock().Now()
	}

	// Lock raftMu so that the catch-up scan snapshot and registration are
	// atomic — no committed txn events are missed between the two.
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	if err := r.checkExecutionCanProceedForRangeFeed(ctx, rSpan, checkTS); err != nil {
		return nil, err
	}

	// Ensure we have a TxnFeed processor for this range.
	p := r.getTxnFeedProcessorRaftMuLocked()
	if p == nil {
		p = r.initTxnFeedProcessorRaftMuLocked()
	}

	// Create a catch-up snapshot under raftMu. Register closes the snapshot
	// immediately if no catch-up scan is needed, or transfers ownership to
	// the registration's catch-up goroutine.
	snap := r.store.StateEngine().NewSnapshot()

	return p.Register(ctx, rSpan, args.Timestamp, snap, stream)
}

// initTxnFeedProcessorRaftMuLocked creates a new TxnFeed processor for this
// replica and stores it. Must be called under raftMu.
func (r *Replica) initTxnFeedProcessorRaftMuLocked() *txnfeed.Processor {
	desc := r.Desc()
	p := txnfeed.NewProcessor(txnfeed.Config{
		AmbientContext: r.AmbientContext,
		Span:           desc.RSpan(),
		Stopper:        r.store.stopper,
	})
	r.setTxnFeedProcessor(p)
	return p
}

// getTxnFeedProcessorRaftMuLocked returns the current TxnFeed processor, or
// nil if none exists.
func (r *Replica) getTxnFeedProcessorRaftMuLocked() *txnfeed.Processor {
	r.txnFeedMu.RLock()
	defer r.txnFeedMu.RUnlock()
	return r.txnFeedMu.proc
}

func (r *Replica) setTxnFeedProcessor(p *txnfeed.Processor) {
	r.txnFeedMu.Lock()
	defer r.txnFeedMu.Unlock()
	r.txnFeedMu.proc = p
}

// handleCommitTxnOpsRaftMuLocked delivers committed transaction ops from Raft
// apply to the TxnFeed processor. Called under raftMu.
func (r *Replica) handleCommitTxnOpsRaftMuLocked(
	ctx context.Context, ops *kvserverpb.CommitTxnOps,
) {
	p := r.getTxnFeedProcessorRaftMuLocked()
	if p == nil {
		return
	}
	p.ConsumeCommitTxnOps(ctx, ops)
}

// forwardClosedTSForTxnFeedRaftMuLocked forwards the closed timestamp to the
// TxnFeed processor. Called under raftMu.
func (r *Replica) forwardClosedTSForTxnFeedRaftMuLocked(
	ctx context.Context, closedTS hlc.Timestamp,
) {
	p := r.getTxnFeedProcessorRaftMuLocked()
	if p == nil {
		return
	}
	p.ForwardClosedTS(ctx, closedTS)
}

// disconnectTxnFeedWithErr stops the TxnFeed processor and disconnects all
// registrations with the given error. Used during splits, merges, etc.
func (r *Replica) disconnectTxnFeedWithErr(pErr *kvpb.Error) {
	r.txnFeedMu.Lock()
	defer r.txnFeedMu.Unlock()
	if r.txnFeedMu.proc != nil {
		r.txnFeedMu.proc.StopWithErr(pErr)
		r.txnFeedMu.proc = nil
	}
}
