// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// updateTimestampCache updates the timestamp cache in order to set a low water
// mark for the timestamp at which mutations to keys overlapping the provided
// request can write, such that they don't re-write history.
func (r *Replica) updateTimestampCache(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	tc := r.store.tsCache
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
		if roachpb.UpdatesTimestampCache(args) {
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
			case *roachpb.EndTransactionRequest:
				// EndTransaction adds the transaction key to the write
				// timestamp cache as a tombstone to ensure replays and
				// concurrent requests aren't able to create a new
				// transaction record.
				//
				// It inserts the timestamp of the final batch in the
				// transaction. This timestamp must necessarily be equal
				// to or greater than the transaction's OrigTimestamp,
				// which is consulted in CanCreateTxnRecord.
				key := keys.TransactionKey(start, txnID)
				tc.Add(key, nil, ts, txnID, false /* readCache */)
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

				// Update the local clock to the pushee's new timestamp. This
				// ensures that we can safely update the timestamp cache based
				// on this value. The pushee's timestamp is not reflected in
				// the batch request's or response's timestamp.
				r.store.Clock().Update(pushee.Timestamp)

				key := keys.TransactionKey(start, pushee.ID)
				readCache := pushee.Status != roachpb.ABORTED
				tc.Add(key, nil, pushee.Timestamp, t.PusherTxn.ID, readCache)
			case *roachpb.ConditionalPutRequest:
				if pErr != nil {
					// ConditionalPut still updates on ConditionFailedErrors.
					// TODO(nvanbenschoten): do we need similar logic for InitPutRequest?
					if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); !ok {
						continue
					}
				}
				tc.Add(start, end, ts, txnID, true /* readCache */)
			case *roachpb.ScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for forward scan, the resume span will start at
					// the (last key read).Next(), which is actually the correct
					// end key for the span to update the timestamp cache.
					end = resp.ResumeSpan.Key
				}
				tc.Add(start, end, ts, txnID, true /* readCache */)
			case *roachpb.ReverseScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for reverse scans, the resume span's end key is
					// an open interval. That means it was read as part of this op
					// and won't be read on resume. It is the correct start key for
					// the span to update the timestamp cache.
					start = resp.ResumeSpan.EndKey
				}
				tc.Add(start, end, ts, txnID, true /* readCache */)
			case *roachpb.QueryIntentRequest:
				if t.IfMissing == roachpb.QueryIntentRequest_PREVENT {
					resp := br.Responses[i].GetInner().(*roachpb.QueryIntentResponse)
					if !resp.FoundIntent {
						// If the QueryIntent request has an "if missing" behavior
						// of PREVENT and the intent is missing then we update the
						// timestamp cache at the intent's key to the intent's
						// transactional timestamp. This will prevent the intent
						// from ever being written in the future. We use an empty
						// transaction ID so that we block the intent regardless
						// of whether it is part of the current batch's transaction
						// or not.
						tc.Add(start, end, t.Txn.Timestamp, uuid.UUID{}, true /* readCache */)
					}
				}
			case *roachpb.RefreshRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			case *roachpb.RefreshRangeRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			default:
				tc.Add(start, end, ts, txnID, !roachpb.UpdatesWriteTimestampCache(args))
			}
		}
	}
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
//
// The timestamp cache also has a role in preventing replays of BeginTransaction
// reordered after an EndTransaction. If that's detected, an error will be
// returned.
func (r *Replica) applyTimestampCache(
	ctx context.Context, ba *roachpb.BatchRequest, minReadTS hlc.Timestamp,
) (bool, *roachpb.Error) {
	var bumped bool
	for _, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.ConsultsTimestampCache(args) {
			header := args.Header()

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID := r.store.tsCache.GetMaxRead(header.Key, header.EndKey)
			if rTS.Forward(minReadTS) {
				rTxnID = uuid.Nil
			}
			if ba.Txn != nil {
				if ba.Txn.ID != rTxnID {
					nextTS := rTS.Next()
					if ba.Txn.Timestamp.Less(nextTS) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(nextTS) || bumped
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(rTS.Next()) || bumped
			}

			// On more recent writes, forward the timestamp and set the
			// write too old boolean for transactions. Note that currently
			// only EndTransaction and DeleteRange requests update the
			// write timestamp cache.
			wTS, wTxnID := r.store.tsCache.GetMaxWrite(header.Key, header.EndKey)
			if ba.Txn != nil {
				if ba.Txn.ID != wTxnID {
					if !wTS.Less(ba.Txn.Timestamp) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(wTS.Next()) || bumped
						txn.WriteTooOld = true
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(wTS.Next()) || bumped
			}
		}
	}
	return bumped, nil
}

// CanCreateTxnRecord determines whether a transaction record can be created
// for the provided transaction information. Callers should provide an upper
// bound on the transaction's minimum timestamp across all epochs (typically
// the original timestamp of its first epoch). If this is not exact then the
// method may return false positives (i.e. it determines that the record could
// be created when it actually couldn't) but will never return false negatives
// (i.e. it determines that the record could not be created when it actually
// could).
//
// Because of this, callers who intend to write the transaction record should
// always provide an exact minimum timestamp. They can't provide their
// provisional commit timestamp because it may have moved forward over the
// course of a single epoch and they can't provide their (current epoch's)
// OrigTimestamp because it may have moved forward over a series of prior
// epochs. Either of these timestamps might be above the timestamp that a
// successful aborter might have used when aborting the transaction. Instead,
// they should provide their epoch-zero original timestamp.
//
// If the method return true, it also returns the minimum provisional commit
// timestamp that the record can be created with. If the method returns false,
// it returns the reason that transaction record was rejected. If the method
// ever determines that a transaction record must be rejected, it will continue
// to reject that transaction going forwards.
//
// The method performs two critical roles:
// 1. It protects against replays or other requests that could otherwise cause
//    a transaction record to be created after the transaction has already been
//    finalized and its record cleaned up.
// 2. It serves as the mechanism by which successful push requests convey
//    information to transactions who have not yet written their transaction
//    record. In doing so, it protects against transaction records from being
//    created with too low of a timestamp.
//
// This is detailed in the transaction record state machine below:
//
// +-----------------------------------+
// |vars                               |
// |-----------------------------------|
// |v1 = rTSCache[txn.id]   = timestamp|
// |v2 = wTSCache[txn.id]   = timestamp|
// |v3 = txnSpanGCThreshold = timestamp|
// +-----------------------------------+
//
//                PushTxn(TIMESTAMP)                               HeartbeatTxn
//                then: v1 = push.ts                            then: update record
//                   +------+                                       +------+
//  PushTxn(ABORT)   |      |       BeginTxn or HeartbeatTxn        |      |   PushTxn(TIMESTAMP)
// then: v2 = txn.ts |      v       if: v2 < txn.orig               |      v  then: update record
//                +---------------+   & v3 < txn.orig        +--------------------+
//           +----|               | then: txn.ts.forward(v1) |                    |----+
//           |    |               | else: fail               |                    |    |
//           |    | no txn record |------------------------->| txn record written |    |
//           +--->|               |                          |     [pending]      |<---+
//                |               |-+                        |                    |
//                +---------------+  \                       +--------------------+
//                  ^          ^      \                             /           /
//                  |           \      \ EndTxn                    / EndTxn    /
//                  |            \      \ if: same conditions     / then: v2 = txn.ts
//                  |             \      \    as above           /           /
//                  |     Eager GC \      \ then: v2 = txn.ts   /           /
//                  |    if: EndTxn \      \ else: fail        /           / PushTxn(ABORT)
//                  |    transition  \      v                 v           / then: v2 = txn.ts
//                  |         taken   \    +--------------------+        /
//                  |                  \   |                    |       /
//                   \                  +--|                    |      /
//                    \                    | txn record written |<----+
//                     +-------------------|     [finalized]    |
//                       GC queue          |                    |
//                     then: v3 = now()    +--------------------+
//                                                ^      |
//                                                |      | PushTxn(*)
//                                                +------+ then: no-op
//
//
// In the diagram, CanCreateTxnRecord is consulted in both of the state
// transitions that move away from the "no txn record" state. Updating
// v1 and v2 is performed in updateTimestampCache.
func (r *Replica) CanCreateTxnRecord(
	txnID uuid.UUID, txnKey []byte, txnMinTSUpperBound hlc.Timestamp,
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
	if !wTS.Less(txnMinTSUpperBound) {
		switch wTxnID {
		case txnID:
			// If we find our own transaction ID then an EndTransaction request
			// sent by our coordinator has already been processed. We might be a
			// replay, or we raced with an asynchronous abort. Either way, return
			// an error.
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

	// Disallow creation or modification of a transaction record if its original
	// timestamp is before the TxnSpanGCThreshold, as in that case our transaction
	// may already have been aborted by a concurrent actor which encountered one
	// of our intents (which may have been written before our transaction record).
	//
	// See #9265.
	//
	// TODO(nvanbenschoten): If we forwarded the Range's entire local txn span in
	// the write timestamp cache when we performed a GC then we wouldn't need this
	// check at all. Another alternative is that we could rely on the PushTxn(ABORT)
	// that the GC queue sends to bump the write timestamp cache on aborted txns.
	if !r.GetTxnSpanGCThreshold().Less(txnMinTSUpperBound) {
		return false, minCommitTS, roachpb.ABORT_REASON_NEW_TXN_RECORD_TOO_OLD
	}

	return true, minCommitTS, 0
}
