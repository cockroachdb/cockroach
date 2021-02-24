// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// DefaultDeclareKeys is the default implementation of Command.DeclareKeys.
func DefaultDeclareKeys(
	_ ImmutableRangeState, header roachpb.Header, req roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	access := spanset.SpanReadWrite
	if roachpb.IsReadOnly(req) && !roachpb.IsLocking(req) {
		access = spanset.SpanReadOnly
	}
	latchSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
}

// DefaultDeclareIsolatedKeys is similar to DefaultDeclareKeys, but it declares
// both lock spans in addition to latch spans. When used, commands will wait on
// locks and wait-queues owned by other transactions before evaluating. This
// ensures that the commands are fully isolated from conflicting transactions
// when it evaluated.
func DefaultDeclareIsolatedKeys(
	_ ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	access := spanset.SpanReadWrite
	timestamp := header.Timestamp
	if roachpb.IsReadOnly(req) && !roachpb.IsLocking(req) {
		access = spanset.SpanReadOnly
		if header.Txn != nil {
			// For transactional reads, acquire read latches all the way up to
			// the transaction's uncertainty limit, because reads may observe
			// writes all the way up to this timestamp.
			//
			// It is critical that reads declare latches up through their
			// uncertainty interval so that they are properly synchronized with
			// earlier writes that may have a happened-before relationship with
			// the read. These writes could not have completed and returned to
			// the client until they were durable in the Range's Raft log.
			// However, they may not have been applied to the replica's state
			// machine by the time the write was acknowledged, because Raft
			// entry application occurs asynchronously with respect to the
			// writer (see AckCommittedEntriesBeforeApplication). Latching is
			// the only mechanism that ensures that any observers of the write
			// wait for the write apply before reading.
			timestamp.Forward(header.Txn.GlobalUncertaintyLimit)
		}
	}
	latchSpans.AddMVCC(access, req.Header().Span(), timestamp)
	lockSpans.AddNonMVCC(access, req.Header().Span())
}

// DeclareKeysForBatch adds all keys that the batch with the provided header
// touches to the given SpanSet. This does not include keys touched during the
// processing of the batch's individual commands.
func DeclareKeysForBatch(
	rs ImmutableRangeState, header roachpb.Header, latchSpans *spanset.SpanSet,
) {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.AbortSpanKey(rs.GetRangeID(), header.Txn.ID),
		})
	}
}

// declareAllKeys declares a non-MVCC write over every addressable key. This
// guarantees that the caller conflicts with any other command because every
// command must declare at least one addressable key, which is tested against
// in TestRequestsSerializeWithAllKeys.
func declareAllKeys(latchSpans *spanset.SpanSet) {
	// NOTE: we don't actually know what the end key of the Range will
	// be at the time of request evaluation (see ImmutableRangeState),
	// so we simply declare a latch over the entire keyspace. This may
	// extend beyond the Range, but this is ok for the purpose of
	// acquiring latches.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.LocalPrefix, EndKey: keys.LocalMax})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey})
}

// CommandArgs contains all the arguments to a command.
// TODO(bdarnell): consider merging with kvserverbase.FilterArgs (which
// would probably require removing the EvalCtx field due to import order
// constraints).
type CommandArgs struct {
	EvalCtx EvalContext
	Header  roachpb.Header
	Args    roachpb.Request
	// *Stats should be mutated to reflect any writes made by the command.
	Stats                 *enginepb.MVCCStats
	LocalUncertaintyLimit hlc.Timestamp
}
