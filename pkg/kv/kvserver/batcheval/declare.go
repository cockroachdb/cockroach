// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// DefaultDeclareKeys is the default implementation of Command.DeclareKeys.
func DefaultDeclareKeys(
	_ ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	access := spanset.SpanReadWrite
	if kvpb.IsReadOnly(req) && !kvpb.IsLocking(req) {
		access = spanset.SpanReadOnly
	}
	latchSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
	return nil
}

// DefaultDeclareIsolatedKeys is similar to DefaultDeclareKeys, but it declares
// both lock spans in addition to latch spans. When used, commands will wait on
// locks and wait-queues owned by other transactions before evaluating. This
// ensures that the commands are fully isolated from conflicting transactions
// when it evaluated.
func DefaultDeclareIsolatedKeys(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	var access spanset.SpanAccess
	var str lock.Strength
	timestamp := header.Timestamp

	if kvpb.IsReadOnly(req) && !kvpb.IsLocking(req) {
		str = lock.None
		access = spanset.SpanReadOnly
		// For non-locking reads, acquire read latches all the way up to the
		// request's worst-case (i.e. global) uncertainty limit, because reads may
		// observe writes all the way up to this timestamp.
		//
		// It is critical that reads declare latches up through their uncertainty
		// interval so that they are properly synchronized with earlier writes that
		// may have a happened-before relationship with the read. These writes could
		// not have completed and returned to the client until they were durable in
		// the Range's Raft log. However, they may not have been applied to the
		// replica's state machine by the time the write was acknowledged, because
		// Raft entry application occurs asynchronously with respect to the writer
		// (see AckCommittedEntriesBeforeApplication). Latching is the only
		// mechanism that ensures that any observers of the write wait for the write
		// apply before reading.
		//
		// NOTE: we pass an empty lease status here, which means that observed
		// timestamps collected by transactions will not be used. The actual
		// uncertainty interval used by the request may be smaller (i.e. contain a
		// local limit), but we can't determine that until after we have declared
		// keys, acquired latches, and consulted the replica's lease.
		in := uncertainty.ComputeInterval(header, kvserverpb.LeaseStatus{}, maxOffset)
		timestamp.Forward(in.GlobalLimit)
	} else {
		str = lock.Intent
		access = spanset.SpanReadWrite
		// Get the correct lock strength to use for {lock,latch} spans if we're
		// dealing with locking read requests.
		if readOnlyReq, ok := req.(kvpb.LockingReadRequest); ok {
			var dur lock.Durability
			str, dur = readOnlyReq.KeyLocking()
			switch str {
			case lock.None:
				// One reason we can be in this branch is if someone has asked for a
				// replicated non-locking read. Detect this nonsensical case to better
				// word the error message.
				if dur == lock.Replicated {
					return errors.AssertionFailedf(
						"incompatible key locking strength %s and durability %s", str.String(), dur.String(),
					)
				} else {
					return errors.AssertionFailedf("unexpected non-locking read handling")
				}
			case lock.Shared:
				access = spanset.SpanReadOnly
				// Unlike non-locking reads, shared-locking reads are isolated from
				// writes at all timestamps[1] (not just the request's timestamp); so we
				// acquire a read latch at max timestamp. See the shared locks RFC for
				// more details.
				//
				// [1] This allows the transaction that issued the shared-locking read to
				// get arbitrarily bumped without needing to worry about its read-refresh
				// failing. Unlike non-locking reads, where the read set is only protected
				// from concurrent writers operating at lower timestamps, a shared-locking
				// read extends this protection to all timestamps.
				timestamp = hlc.MaxTimestamp
				if dur == lock.Replicated && header.Txn != nil {
					// Concurrent replicated shared lock attempts by the same transaction
					// need to be isolated from one another. We acquire a write latch on
					// a per-transaction local key to achieve this. See
					// https://github.com/cockroachdb/cockroach/issues/109668.
					latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
						Key: keys.ReplicatedSharedLocksTransactionLatchingKey(rs.GetRangeID(), header.Txn.ID),
					})
				}
			case lock.Exclusive:
				// Reads that acquire exclusive locks acquire write latches at the
				// request's timestamp. This isolates them from all concurrent writes,
				// all concurrent shared-locking reads, and non-locking reads at higher
				// timestamps[1].
				//
				// [1] This won't be required once non-locking reads stop blocking on
				// exclusive locks. Once that happens, exclusive locks should acquire
				// write latches at hlc.MaxTimestamp.
				access = spanset.SpanReadWrite
			default:
				return errors.AssertionFailedf("unexpected lock strength %s", str)
			}
		}
	}
	latchSpans.AddMVCC(access, req.Header().Span(), timestamp)
	lockSpans.Add(str, req.Header().Span())
	return nil
}

// DeclareKeysForRefresh determines whether a Refresh request should declare
// locks and go through the lock table or not. The former is done in versions >=
// V23_2_RemoveLockTableWaiterTouchPush where Refresh requests use a
// WaitPolicy_Error, so they do not block on conflict. To ensure correct
// behavior during migrations, check for WaitPolicy_Error before declaring locks.
// TODO(mira): Remove after V23_2_RemoveLockTableWaiterTouchPush is deleted.
func DeclareKeysForRefresh(
	irs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lss *lockspanset.LockSpanSet,
	dur time.Duration,
) error {
	if header.WaitPolicy == lock.WaitPolicy_Error {
		return DefaultDeclareIsolatedKeys(irs, header, req, latchSpans, lss, dur)
	} else {
		return DefaultDeclareKeys(irs, header, req, latchSpans, lss, dur)
	}
}

// DeclareKeysForBatch adds all keys that the batch with the provided header
// touches to the given SpanSet. This does not include keys touched during the
// processing of the batch's individual commands.
func DeclareKeysForBatch(
	rs ImmutableRangeState, header *kvpb.Header, latchSpans *spanset.SpanSet,
) error {
	// If the batch is transactional and has acquired locks, we will check if the
	// transaction has been aborted during evaluation (see checkIfTxnAborted), so
	// declare a read latch on the AbortSpan key.
	if header.Txn != nil && header.Txn.IsLocking() {
		header.Txn.AssertInitialized(context.TODO())
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.AbortSpanKey(rs.GetRangeID(), header.Txn.ID),
		})
	}
	return nil
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
	Header  kvpb.Header
	Args    kvpb.Request
	Now     hlc.ClockTimestamp
	// *Stats should be mutated to reflect any writes made by the command.
	Stats *enginepb.MVCCStats
	// ScanStats should be mutated to reflect Get, Scan, ReverseScan,
	// ExportRequest reads made by the command.
	ScanStats             *kvpb.ScanStats
	Concurrency           *concurrency.Guard
	Uncertainty           uncertainty.Interval
	DontInterleaveIntents bool
	OmitInRangefeeds      bool
}
