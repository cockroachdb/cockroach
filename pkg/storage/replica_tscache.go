// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/tscache"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// setTimestampCacheLowWaterMark updates the low water mark of the timestamp
// cache to the provided timestamp for all key ranges owned by the provided
// Range descriptor. This ensures that no future writes in either the local or
// global keyspace are allowed at times equal to or earlier than this timestamp,
// which could invalidate prior reads.
func setTimestampCacheLowWaterMark(
	tc tscache.Cache, desc *roachpb.RangeDescriptor, ts hlc.Timestamp,
) {
	for _, keyRange := range rditer.MakeReplicatedKeyRanges(desc) {
		tc.SetLowWater(keyRange.Start.Key, keyRange.End.Key, ts)
	}
}

// updateTimestampCache updates the timestamp cache in order to set a low water
// mark for the timestamp at which mutations to keys overlapping the provided
// request can write, such that they don't re-write history.
func (r *Replica) updateTimestampCache(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	addToTSCache := r.store.tsCache.Add
	if util.RaceEnabled {
		addToTSCache = checkedTSCacheUpdate(r.store.Clock().Now(), r.store.tsCache, ba, br, pErr)
	}
	// Update the timestamp cache using the timestamp at which the batch
	// was executed. Note this may have moved forward from ba.Timestamp,
	// as when the request is retried locally on WriteTooOldErrors.
	ts := ba.Timestamp
	if br != nil {
		ts = br.Timestamp
	}
	var txnID uuid.UUID
	if ba.Txn != nil {
		txnID = ba.Txn.ID
	}
	for i, union := range ba.Requests {
		args := union.GetInner()
		if !roachpb.UpdatesTimestampCache(args) {
			continue
		}
		// Skip update if there's an error and it's not for this index
		// or the request doesn't update the timestamp cache on errors.
		if pErr != nil {
			if index := pErr.Index; !roachpb.UpdatesTimestampCacheOnError(args) ||
				index == nil || int32(i) != index.Index {
				continue
			}
		}
		header := args.Header()
		start, end := header.Key, header.EndKey
		switch t := args.(type) {
		case *roachpb.EndTxnRequest:
			// EndTxn requests that finalize their transaction add the
			// transaction key to the write timestamp cache as a tombstone to
			// ensure replays and concurrent requests aren't able to recreate
			// the transaction record.
			//
			// It inserts the timestamp of the final batch in the transaction.
			// This timestamp must necessarily be equal to or greater than the
			// transaction's MinTimestamp, which is consulted in
			// CanCreateTxnRecord.
			if br.Txn.Status.IsFinalized() {
				key := keys.TransactionKey(start, txnID)
				addToTSCache(key, nil, ts, txnID, false /* readCache */)
			}
		case *roachpb.RecoverTxnRequest:
			// A successful RecoverTxn request may or may not have finalized the
			// transaction that it was trying to recover. If so, then we add the
			// transaction's key to the write timestamp cache as a tombstone to
			// ensure that replays and concurrent requests aren't able to
			// recreate the transaction record. This parallels what we do in the
			// EndTxn request case.
			//
			// Insert the timestamp of the batch, which we asserted during
			// command evaluation was equal to or greater than the transaction's
			// MinTimestamp.
			recovered := br.Responses[i].GetInner().(*roachpb.RecoverTxnResponse).RecoveredTxn
			if recovered.Status.IsFinalized() {
				key := keys.TransactionKey(start, recovered.ID)
				addToTSCache(key, nil, ts, recovered.ID, false /* readCache */)
			}
		case *roachpb.PushTxnRequest:
			// A successful PushTxn request bumps the timestamp cache for
			// the pushee's transaction key. The pushee will consult the
			// timestamp cache when creating its record. If the push left
			// the transaction in a PENDING state (PUSH_TIMESTAMP) then we
			// update the read timestamp cache. This will cause the creator
			// of the transaction record to forward its provisional commit
			// timestamp to honor the result of this push. If the push left
			// the transaction in an ABORTED state (PUSH_ABORT) then we
			// update the write timestamp cache. This will prevent the
			// creation of the transaction record entirely.
			pushee := br.Responses[i].GetInner().(*roachpb.PushTxnResponse).PusheeTxn

			var readCache bool
			switch pushee.Status {
			case roachpb.PENDING:
				readCache = true
			case roachpb.ABORTED:
				readCache = false
			case roachpb.STAGING:
				// No need to update the timestamp cache. If a transaction
				// is in this state then it must have a transaction record.
				continue
			case roachpb.COMMITTED:
				// No need to update the timestamp cache. It was already
				// updated by the corresponding EndTxn request.
				continue
			}
			key := keys.TransactionKey(start, pushee.ID)
			addToTSCache(key, nil, pushee.WriteTimestamp, t.PusherTxn.ID, readCache)
		case *roachpb.ConditionalPutRequest:
			// ConditionalPut only updates on ConditionFailedErrors. On other
			// errors, no information is returned. On successful writes, the
			// intent already protects against writes underneath the read.
			if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
				addToTSCache(start, end, ts, txnID, true /* readCache */)
			}
		case *roachpb.InitPutRequest:
			// InitPut only updates on ConditionFailedErrors. On other errors,
			// no information is returned. On successful writes, the intent
			// already protects against writes underneath the read.
			if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
				addToTSCache(start, end, ts, txnID, true /* readCache */)
			}
		case *roachpb.ScanRequest:
			resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
			if resp.ResumeSpan != nil {
				// Note that for forward scan, the resume span will start at
				// the (last key read).Next(), which is actually the correct
				// end key for the span to update the timestamp cache.
				end = resp.ResumeSpan.Key
			}
			addToTSCache(start, end, ts, txnID, true /* readCache */)
		case *roachpb.ReverseScanRequest:
			resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
			if resp.ResumeSpan != nil {
				// Note that for reverse scans, the resume span's end key is
				// an open interval. That means it was read as part of this op
				// and won't be read on resume. It is the correct start key for
				// the span to update the timestamp cache.
				start = resp.ResumeSpan.EndKey
			}
			addToTSCache(start, end, ts, txnID, true /* readCache */)
		case *roachpb.QueryIntentRequest:
			missing := false
			if pErr != nil {
				switch t := pErr.GetDetail().(type) {
				case *roachpb.IntentMissingError:
					missing = true
				case *roachpb.TransactionRetryError:
					// QueryIntent will return a TxnRetry(SERIALIZABLE) error
					// if a transaction is querying its own intent and finds
					// it pushed.
					//
					// NB: we check the index of the error above, so this
					// TransactionRetryError should indicate a missing intent
					// from the QueryIntent request. However, bumping the
					// timestamp cache wouldn't cause a correctness issue
					// if we found the intent.
					missing = t.Reason == roachpb.RETRY_SERIALIZABLE
				}
			} else {
				missing = !br.Responses[i].GetInner().(*roachpb.QueryIntentResponse).FoundIntent
			}
			if missing {
				// If the QueryIntent determined that the intent is missing
				// then we update the timestamp cache at the intent's key to
				// the intent's transactional timestamp. This will prevent
				// the intent from ever being written in the future. We use
				// an empty transaction ID so that we block the intent
				// regardless of whether it is part of the current batch's
				// transaction or not.
				addToTSCache(start, end, t.Txn.WriteTimestamp, uuid.UUID{}, true /* readCache */)
			}
		default:
			addToTSCache(start, end, ts, txnID, !roachpb.UpdatesWriteTimestampCache(args))
		}
	}
}

// checkedTSCacheUpdate wraps tscache.Cache and asserts that any update to the
// cache is at or below the specified time.
func checkedTSCacheUpdate(
	now hlc.Timestamp,
	tc tscache.Cache,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) func(roachpb.Key, roachpb.Key, hlc.Timestamp, uuid.UUID, bool) {
	return func(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID, readCache bool) {
		if now.Less(ts) {
			panic(fmt.Sprintf("Unsafe timestamp cache update! Cannot add timestamp %s to timestamp "+
				"cache after evaluating %v (resp=%v; err=%v) with local hlc clock at timestamp %s. "+
				"The timestamp cache update could be lost on a lease transfer.", ts, ba, br, pErr, now))
		}
		tc.Add(start, end, ts, txnID, readCache)
	}
}

// txnsPushedDueToClosedTimestamp is a telemetry counter for the number of
// batch requests which have been pushed due to the closed timestamp.
var batchesPushedDueToClosedTimestamp telemetry.Counter

func init() {
	batchesPushedDueToClosedTimestamp = telemetry.GetCounter("kv.closed_timestamp.txns_pushed")
}

// applyTimestampCache moves the batch timestamp forward depending on
// the presence of overlapping entries in the timestamp cache. If the
// batch is transactional, the txn timestamp and the txn.WriteTooOld
// bool are updated.
//
// Two important invariants of Cockroach: 1) encountering a more
// recently written value means transaction restart. 2) values must
// be written with a greater timestamp than the most recent read to
// the same key. Check the timestamp cache for reads/writes which
// are at least as recent as the timestamp of this write. The cmd must
// update its timestamp to be greater than more recent values in the
// timestamp cache. When the write returns, the updated timestamp
// will inform the batch response timestamp or batch response txn
// timestamp.
//
// minReadTS is used as a per-request low water mark for the value returned from
// the read timestamp cache. That is, if the read timestamp cache returns a
// value below minReadTS, minReadTS (without an associated txn id) will be used
// instead to adjust the batch's timestamp.
func (r *Replica) applyTimestampCache(
	ctx context.Context, ba *roachpb.BatchRequest, minReadTS hlc.Timestamp,
) bool {
	// bumpedDueToMinReadTS is set to true if the highest timestamp bump encountered
	// below is due to the minReadTS.
	var bumpedDueToMinReadTS bool
	var bumped bool

	for _, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.ConsultsTimestampCache(args) {
			header := args.Header()

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID := r.store.tsCache.GetMaxRead(header.Key, header.EndKey)
			var forwardedToMinReadTS bool
			if rTS.Forward(minReadTS) {
				forwardedToMinReadTS = true
				rTxnID = uuid.Nil
			}
			nextRTS := rTS.Next()
			var bumpedCurReq bool
			if ba.Txn != nil {
				if ba.Txn.ID != rTxnID {
					if ba.Txn.WriteTimestamp.Less(nextRTS) {
						txn := ba.Txn.Clone()
						bumpedCurReq = txn.WriteTimestamp.Forward(nextRTS)
						ba.Txn = txn
					}
				}
			} else {
				bumpedCurReq = ba.Timestamp.Forward(nextRTS)
			}
			// Preserve bumpedDueToMinReadTS if we did not just bump or set it
			// appropriately if we did.
			bumpedDueToMinReadTS = (!bumpedCurReq && bumpedDueToMinReadTS) || (bumpedCurReq && forwardedToMinReadTS)
			bumped, bumpedCurReq = bumped || bumpedCurReq, false

			// On more recent writes, forward the timestamp and set the write
			// too old boolean for transactions. Note that currently only EndTxn
			// and DeleteRange requests update the write timestamp cache.
			wTS, wTxnID := r.store.tsCache.GetMaxWrite(header.Key, header.EndKey)
			nextWTS := wTS.Next()
			if ba.Txn != nil {
				if ba.Txn.ID != wTxnID {
					if ba.Txn.WriteTimestamp.Less(nextWTS) {
						txn := ba.Txn.Clone()
						bumpedCurReq = txn.WriteTimestamp.Forward(nextWTS)
						txn.WriteTooOld = true
						ba.Txn = txn
					}
				}
			} else {
				bumpedCurReq = ba.Timestamp.Forward(nextWTS)
			}
			// Clear bumpedDueToMinReadTS if we just bumped due to the write tscache.
			bumpedDueToMinReadTS = !bumpedCurReq && bumpedDueToMinReadTS
			bumped = bumped || bumpedCurReq
		}
	}
	if bumpedDueToMinReadTS {
		telemetry.Inc(batchesPushedDueToClosedTimestamp)
	}
	return bumped
}

// CanCreateTxnRecord determines whether a transaction record can be created for
// the provided transaction information. Callers must provide the transaction's
// minimum timestamp across all epochs, along with its ID and its key.
//
// If the method return true, it also returns the minimum provisional commit
// timestamp that the record can be created with. If the method returns false,
// it returns the reason that transaction record was rejected. If the method
// ever determines that a transaction record must be rejected, it will continue
// to reject that transaction going forwards.
//
// The method performs two critical roles:
// 1. It protects against replayed requests or new requests from a transaction's
//    coordinator that could otherwise cause a transaction record to be created
//    after the transaction has already been finalized and its record cleaned up.
// 2. It serves as the mechanism by which successful push requests convey
//    information to transactions who have not yet written their transaction
//    record. In doing so, it ensures that transaction records are created with a
//    sufficiently high timestamp after a successful PushTxn(TIMESTAMP) and ensures
//    that transactions records are never created at all after a successful
//    PushTxn(ABORT). As a result of this mechanism, a transaction never needs to
//    explicitly create the transaction record for contending transactions.
//
// This is detailed in the transaction record state machine below:
//
// +-----------------------------------+
// | vars                              |
// |-----------------------------------|
// | v1 = rTSCache[txn.id] = timestamp |
// | v2 = wTSCache[txn.id] = timestamp |
// +-----------------------------------+
// | operations                        |
// |-----------------------------------|
// | v -> t = forward v by timestamp t |
// +-----------------------------------+
//
//                  PushTxn(TIMESTAMP)                                HeartbeatTxn
//                  then: v1 -> push.ts                             then: update record
//                      +------+                                        +------+
//    PushTxn(ABORT)    |      |        HeartbeatTxn                    |      |   PushTxn(TIMESTAMP)
//   then: v2 -> txn.ts |      v        if: v2 < txn.orig               |      v  then: update record
//                 +-----------------+  then: txn.ts -> v1      +--------------------+
//            +----|                 |  else: fail              |                    |----+
//            |    |                 |------------------------->|                    |    |
//            |    |  no txn record  |                          | txn record written |    |
//            +--->|                 |  EndTxn(STAGING)         |     [pending]      |<---+
//                 |                 |__  if: v2 < txn.orig     |                    |
//                 +-----------------+  \__ then: v2 -> txn.ts  +--------------------+
//                    |            ^       \__ else: fail       _/   |            ^
//                    |            |          \__             _/     |            |
// EndTxn(!STAGING)   |            |             \__        _/       | EndTxn(STAGING)
// if: v2 < txn.orig  |   Eager GC |                \____ _/______   |            |
// then: v2 -> txn.ts |      or    |                    _/        \  |            | HeartbeatTxn
// else: fail         |   GC queue |  /----------------/          |  |            | if: epoch update
//                    v            | v    EndTxn(!STAGING)        v  v            |
//                +--------------------+  or PushTxn(ABORT)     +--------------------+
//                |                    |  then: v2 -> txn.ts    |                    |
//           +--->|                    |<-----------------------|                    |----+
//           |    | txn record written |                        | txn record written |    |
//           |    |     [finalized]    |                        |      [staging]     |    |
//           +----|                    |                        |                    |<---+
//   PushTxn(*)   +--------------------+                        +--------------------+
//   then: no-op                    ^   PushTxn(*) + RecoverTxn    |              EndTxn(STAGING)
//                                  |     then: v2 -> txn.ts       |              or HeartbeatTxn
//                                  +------------------------------+            then: update record
//
//
// In the diagram, CanCreateTxnRecord is consulted in all three of the
// state transitions that move away from the "no txn record" state.
// Updating v1 and v2 is performed in updateTimestampCache.
//
// The are three separate simplifications to the transaction model that would
// allow us to simplify this state machine:
//
// 1. as discussed on the comment on txnHeartbeater, it is reasonable to expect
//    that we will eventually move away from tracking transaction liveness on a
//    per-transaction basis. This means that we would no longer need transaction
//    heartbeats and would never need to write a transaction record until a
//    transaction is ready to complete.
//
// 2. one of the two possibilities for the "txn record written [finalized]" state
//    is that the transaction record is aborted. There used to be two reasons to
//    persist transaction records with the ABORTED status. The first was because
//    doing so was the only way for concurrent actors to prevent the record from
//    being re-written by the transaction going forward. The concurrent actor would
//    write an aborted transaction record and then wait for the GC to clean it up
//    later. The other reasons for writing the transaction records with the ABORTED
//    status was because these records could point at intents, which assisted the
//    cleanup process for these intents. However, this only held for ABORTED
//    records written on behalf of the transaction coordinator itself. If a
//    transaction was aborted by a concurrent actor, its record would not
//    immediately contain any of the transaction's intents.
//
//    The first reason here no longer holds. Concurrent actors now bump the write
//    timestamp cache when aborting a transaction, which has the same effect as
//    writing an ABORTED transaction record. The second reason still holds but is
//    fairly weak. A transaction coordinator can kick off intent resolution for an
//    aborted transaction without needing to write these intents into the record
//    itself. In the worst case, this intent resolution fails and each intent is
//    cleaned up individually as it is discovered. All in all, neither
//    justification for this state holds much weight anymore.
//
// 3. the other possibility for the "txn record written [finalized]" state is that
//    the transaction record is committed. This state is currently critical for the
//    transaction model because intent resolution cannot begin before a transaction
//    record enters this state. However, this doesn't need to be the case forever.
//    There are proposals to modify the state of committed key-value writes
//    slightly such that intent resolution could be run for implicitly committed
//    transactions while their transaction record remains in the  "txn record
//    written [staging]" state. For this to work, the recovery mechanism for
//    indeterminate commit errors would need to be able to determine whether an
//    intent or a **committed value** indicated the success of a write that was
//    in-flight at the time the transaction record was staged. This poses
//    challenges migration and garbage collection, but it would have a number of
//    performance benefits.
//
// If we were to perform change #1, we could remove the "txn record written
// [pending]" state. If we were to perform change #2 and #3, we could remove the
// "txn record written [finalized]" state. All together, this would leave us
// with only two states that the transaction record could be in, written or not
// written. At that point, it begins to closely resemble any other write in the
// system.
//
func (r *Replica) CanCreateTxnRecord(
	txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
) (ok bool, minCommitTS hlc.Timestamp, reason roachpb.TransactionAbortedReason) {
	// Consult the timestamp cache with the transaction's key. The timestamp
	// cache is used in two ways for transactions without transaction records.
	// The read timestamp cache is used to push the timestamp of transactions
	// that don't have transaction records. The write timestamp cache is used
	// to abort transactions entirely that don't have transaction records.
	//
	// Using this strategy, we enforce the invariant that only requests sent
	// from a transaction's own coordinator can create its transaction record.
	// However, once a transaction record is written, other concurrent actors
	// can modify it. This is reflected in the diagram above.
	key := keys.TransactionKey(txnKey, txnID)

	// Look in the read timestamp cache to see if there is an entry for this
	// transaction, which indicates the minimum timestamp that the transaction
	// can commit at. This is used by pushers to push the timestamp of a
	// transaction that hasn't yet written its transaction record.
	minCommitTS, _ = r.store.tsCache.GetMaxRead(key, nil /* end */)

	// Also look in the write timestamp cache to see if there is an entry for
	// this transaction, which would indicate this transaction has already been
	// finalized or was already aborted by a concurrent transaction. If there is
	// an entry, then we return a retriable error: if this is a re-evaluation,
	// then the error will be transformed into an ambiguous one higher up.
	// Otherwise, if the client is still waiting for a result, then this cannot
	// be a "replay" of any sort.
	wTS, wTxnID := r.store.tsCache.GetMaxWrite(key, nil /* end */)
	// Compare against the minimum timestamp that the transaction could have
	// written intents at.
	if !wTS.Less(txnMinTS) {
		switch wTxnID {
		case txnID:
			// If we find our own transaction ID then an EndTxn request sent by
			// our coordinator has already been processed. We might be a replay,
			// or we raced with an asynchronous abort. Either way, return an
			// error.
			return false, minCommitTS, roachpb.ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY
		case uuid.Nil:
			// On lease transfers the timestamp cache is reset with the transfer
			// time as the low water mark. The timestamp cache may also lose
			// information when bumping its low water mark due to memory
			// constraints. If this replica recently obtained the lease or if
			// the timestamp cache recently bumped its low water mark, this case
			// will be true for new txns, even if they're not a replay. We force
			// these txns to retry.
			return false, minCommitTS, roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED_POSSIBLE_REPLAY
		default:
			// If we find another transaction's ID then that transaction has
			// aborted us before our transaction record was written. It obeyed
			// the restriction that it couldn't create a transaction record for
			// us, so it bumped the write timestamp cache instead to prevent us
			// from ever creating a transaction record.
			return false, minCommitTS, roachpb.ABORT_REASON_ABORTED_RECORD_FOUND
		}
	}
	return true, minCommitTS, 0
}
