// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tscache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// addToTSCacheChecked adds the specified timestamp to the timestamp cache
// covering the range of keys from start to end. Before doing so, the function
// performs a few assertions to check for proper use of the timestamp cache.
func (r *Replica) addToTSCacheChecked(
	ctx context.Context,
	st *kvserverpb.LeaseStatus,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
	start, end roachpb.Key,
	ts hlc.Timestamp,
	txnID uuid.UUID,
) {
	// All updates to the timestamp cache must be performed below the expiration
	// time of the leaseholder. This ensures correctness if the lease expires
	// and is acquired by a new replica that begins serving writes immediately
	// to the same keys at the next lease's start time.
	if exp := st.Expiration(); exp.LessEq(ts) {
		log.Fatalf(ctx, "Unsafe timestamp cache update! Cannot add timestamp %s to timestamp "+
			"cache after evaluating %v (resp=%v; err=%v) with lease expiration %v. The timestamp "+
			"cache update could be lost of a non-cooperative lease change.", ts, ba, br, pErr, exp)
	}
	r.store.tsCache.Add(start, end, ts, txnID)
}

// updateTimestampCache updates the timestamp cache in order to set a low
// watermark for the timestamp at which mutations to keys overlapping the
// provided request can write, such that they don't re-write history. It can be
// called before or after a batch is done evaluating. A nil `br` indicates that
// this method is being called before the batch is done evaluating.
func (r *Replica) updateTimestampCache(
	ctx context.Context,
	st *kvserverpb.LeaseStatus,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) {
	addToTSCache := func(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID) {
		r.addToTSCacheChecked(ctx, st, ba, br, pErr, start, end, ts, txnID)
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
	beforeEval := br == nil && pErr == nil
	for i, union := range ba.Requests {
		req := union.GetInner()
		if !roachpb.UpdatesTimestampCache(req) {
			continue
		}
		var resp roachpb.Response
		if br != nil {
			resp = br.Responses[i].GetInner()
		}
		// Skip update if there's an error and it's not for this index
		// or the request doesn't update the timestamp cache on errors.
		if pErr != nil {
			if index := pErr.Index; !roachpb.UpdatesTimestampCacheOnError(req) ||
				index == nil || int32(i) != index.Index {
				continue
			}
		}
		header := req.Header()
		start, end := header.Key, header.EndKey

		if ba.WaitPolicy == lock.WaitPolicy_SkipLocked && roachpb.CanSkipLocked(req) {
			if ba.IndexFetchSpec != nil {
				log.Errorf(ctx, "%v", errors.AssertionFailedf("unexpectedly IndexFetchSpec is set with SKIP LOCKED wait policy"))
			}
			// If the request is using a SkipLocked wait policy, it behaves as if run
			// at a lower isolation level for any keys that it skips over. If the read
			// request did not return a key, it does not make a claim about whether
			// that key does or does not exist or what the key's value was at the
			// read's MVCC timestamp. Instead, it only makes a claim about the set of
			// keys that are returned. For those keys which were not skipped and were
			// returned (and often locked, if combined with a locking strength, though
			// this is not required), serializable isolation is enforced by adding
			// point reads to the timestamp cache.
			//
			// We can view this as equating the effect of a ranged read request like:
			//  [Scan("a", "e")] -> returning ["a", "c"]
			// to that of a set of point read requests like:
			//  [Get("a"), Get("c")]
			if err := roachpb.ResponseKeyIterate(req, resp, func(key roachpb.Key) {
				addToTSCache(key, nil, ts, txnID)
			}); err != nil {
				log.Errorf(ctx, "error iterating over response keys while "+
					"updating timestamp cache for ba=%v, br=%v: %v", ba, br, err)
			}
			continue
		}

		switch t := req.(type) {
		case *roachpb.EndTxnRequest:
			// EndTxn requests record a tombstone in the timestamp cache to ensure
			// replays and concurrent requests aren't able to recreate the transaction
			// record.
			//
			// It inserts the timestamp of the final batch in the transaction. This
			// timestamp must necessarily be equal to or greater than the
			// transaction's MinTimestamp, which is consulted in CanCreateTxnRecord.
			key := transactionTombstoneMarker(start, txnID)
			addToTSCache(key, nil, ts, txnID)
		case *roachpb.HeartbeatTxnRequest:
			// HeartbeatTxn requests record a tombstone entry when the record is
			// initially written. This is used when considering potential 1PC
			// evaluation, avoiding checking for a transaction record on disk.
			key := transactionTombstoneMarker(start, txnID)
			addToTSCache(key, nil, ts, txnID)
		case *roachpb.RecoverTxnRequest:
			// A successful RecoverTxn request may or may not have finalized the
			// transaction that it was trying to recover. If so, then we record
			// a tombstone to the timestamp cache to ensure that replays and
			// concurrent requests aren't able to recreate the transaction record.
			// This parallels what we do in the EndTxn request case.
			//
			// Insert the timestamp of the batch, which we asserted during
			// command evaluation was equal to or greater than the transaction's
			// MinTimestamp.
			recovered := resp.(*roachpb.RecoverTxnResponse).RecoveredTxn
			if recovered.Status.IsFinalized() {
				key := transactionTombstoneMarker(start, recovered.ID)
				addToTSCache(key, nil, ts, recovered.ID)
			}
		case *roachpb.PushTxnRequest:
			// A successful PushTxn request bumps the timestamp cache for the
			// pushee's transaction key. The pushee will consult the timestamp
			// cache when creating its record - see CanCreateTxnRecord.
			//
			// If the push left the transaction in a PENDING state
			// (PUSH_TIMESTAMP) then we add a "push" marker to the timestamp
			// cache with the push time. This will cause the creator of the
			// transaction record to forward its provisional commit timestamp to
			// honor the result of this push, preventing it from committing at
			// any prior time.
			//
			// If the push left the transaction in an ABORTED state (PUSH_ABORT)
			// then we add a "tombstone" marker to the timestamp cache wth the
			// pushee's minimum timestamp. This will prevent the creation of the
			// transaction record entirely.
			pushee := resp.(*roachpb.PushTxnResponse).PusheeTxn

			var tombstone bool
			switch pushee.Status {
			case roachpb.PENDING:
				tombstone = false
			case roachpb.ABORTED:
				tombstone = true
			case roachpb.STAGING:
				// No need to update the timestamp cache. If a transaction
				// is in this state then it must have a transaction record.
				continue
			case roachpb.COMMITTED:
				// No need to update the timestamp cache. It was already
				// updated by the corresponding EndTxn request.
				continue
			}

			var key roachpb.Key
			var pushTS hlc.Timestamp
			if tombstone {
				// NOTE: we only push to the pushee's MinTimestamp and not to
				// its WriteTimestamp here because we don't want to add an entry
				// to the timestamp cache at a higher time than the request's
				// timestamp. The request's PushTo field is validated to be less
				// than or equal to its timestamp during evaluation. However, a
				// PUSH_ABORT may not contain a PushTo timestamp or may contain
				// a PushTo timestamp that lags the pushee's WriteTimestamp. So
				// if we were to bump the timestamp cache to the WriteTimestamp,
				// we could risk adding an entry at a time in advance of the
				// local clock.
				key = transactionTombstoneMarker(start, pushee.ID)
				pushTS = pushee.MinTimestamp
			} else {
				key = transactionPushMarker(start, pushee.ID)
				pushTS = pushee.WriteTimestamp
			}
			addToTSCache(key, nil, pushTS, t.PusherTxn.ID)
		case *roachpb.ConditionalPutRequest:
			// ConditionalPut only updates on ConditionFailedErrors. On other
			// errors, no information is returned. On successful writes, the
			// intent already protects against writes underneath the read.
			if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
				addToTSCache(start, end, ts, txnID)
			}
		case *roachpb.InitPutRequest:
			// InitPut only updates on ConditionFailedErrors. On other errors,
			// no information is returned. On successful writes, the intent
			// already protects against writes underneath the read.
			if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); ok {
				addToTSCache(start, end, ts, txnID)
			}
		case *roachpb.GetRequest:
			if !beforeEval && resp.(*roachpb.GetResponse).ResumeSpan != nil {
				// The request did not evaluate. Ignore it.
				continue
			}
			addToTSCache(start, end, ts, txnID)
		case *roachpb.ScanRequest:
			if !beforeEval && resp.(*roachpb.ScanResponse).ResumeSpan != nil {
				resume := resp.(*roachpb.ScanResponse).ResumeSpan
				if start.Equal(resume.Key) {
					// The request did not evaluate. Ignore it.
					continue
				}
				// Note that for forward scan, the resume span will start at
				// the (last key read).Next(), which is actually the correct
				// end key for the span to update the timestamp cache.
				end = resume.Key
			}
			addToTSCache(start, end, ts, txnID)
		case *roachpb.ReverseScanRequest:
			if !beforeEval && resp.(*roachpb.ReverseScanResponse).ResumeSpan != nil {
				resume := resp.(*roachpb.ReverseScanResponse).ResumeSpan
				if end.Equal(resume.EndKey) {
					// The request did not evaluate. Ignore it.
					continue
				}
				// Note that for reverse scans, the resume span's end key is
				// an open interval. That means it was read as part of this op
				// and won't be read on resume. It is the correct start key for
				// the span to update the timestamp cache.
				start = resume.EndKey
			}
			addToTSCache(start, end, ts, txnID)
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
				missing = !resp.(*roachpb.QueryIntentResponse).FoundIntent
			}
			if missing {
				// If the QueryIntent determined that the intent is missing
				// then we update the timestamp cache at the intent's key to
				// the intent's transactional timestamp. This will prevent
				// the intent from ever being written in the future. We use
				// an empty transaction ID so that we block the intent
				// regardless of whether it is part of the current batch's
				// transaction or not.
				addToTSCache(start, end, t.Txn.WriteTimestamp, uuid.UUID{})
			}
		default:
			addToTSCache(start, end, ts, txnID)
		}
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
// the timestamp cache. That is, if the timestamp cache returns a value below
// minReadTS, minReadTS (without an associated txn id) will be used instead to
// adjust the batch's timestamp.
func (r *Replica) applyTimestampCache(
	ctx context.Context, ba *roachpb.BatchRequest, minReadTS hlc.Timestamp,
) bool {
	// bumpedDueToMinReadTS is set to true if the highest timestamp bump encountered
	// below is due to the minReadTS.
	var bumpedDueToMinReadTS bool
	var bumped bool
	var conflictingTxn uuid.UUID

	for _, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.AppliesTimestampCache(args) {
			header := args.Header()

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID := r.store.tsCache.GetMax(header.Key, header.EndKey)
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
			if bumpedCurReq && (rTxnID != uuid.Nil) {
				conflictingTxn = rTxnID
			}
			// Preserve bumpedDueToMinReadTS if we did not just bump or set it
			// appropriately if we did.
			bumpedDueToMinReadTS = (!bumpedCurReq && bumpedDueToMinReadTS) || (bumpedCurReq && forwardedToMinReadTS)
			bumped, bumpedCurReq = bumped || bumpedCurReq, false
		}
	}
	if bumped {
		bumpedTS := ba.Timestamp
		if ba.Txn != nil {
			bumpedTS = ba.Txn.WriteTimestamp
		}

		if bumpedDueToMinReadTS {
			telemetry.Inc(batchesPushedDueToClosedTimestamp)
			log.VEventf(ctx, 2, "bumped write timestamp due to closed ts: %s", minReadTS)
		} else {
			conflictMsg := "conflicting txn unknown"
			if conflictingTxn != uuid.Nil {
				conflictMsg = "conflicting txn: " + conflictingTxn.Short()
			}
			log.VEventf(ctx, 2, "bumped write timestamp to %s; %s", bumpedTS, redact.Safe(conflictMsg))
		}
	}
	return bumped
}

// CanCreateTxnRecord determines whether a transaction record can be created for
// the provided transaction information. Callers must provide the transaction's
// minimum timestamp across all epochs, along with its ID and its key.
//
// If the method returns false, it returns the reason that transaction record
// was rejected. If the method ever determines that a transaction record must be
// rejected, it will continue to reject that transaction going forwards.
//
// The method performs two critical roles:
//
//  1. It protects against replayed requests or new requests from a
//     transaction's coordinator that could otherwise cause a transaction record
//     to be created after the transaction has already been finalized and its
//     record cleaned up.
//
//  2. It serves as the mechanism through which successful PushTxn(ABORT)
//     requests convey information to pushee transactions who have not yet
//     written their transaction record. In doing so, it ensures that
//     transactions records are never created for the pushee after a successful
//     PushTxn(ABORT). As a result of this mechanism, a pusher transaction never
//     needs to create the transaction record for contending transactions that
//     it forms write-write conflicts with.
//
//     NOTE: write-read conflicts result in PushTxn(TIMESTAMP) requests, which
//     coordinate with pushee transactions through the MinTxnCommitTS mechanism
//     (see below).
//
// In addition, it is used when considering 1PC evaluation, to avoid checking
// for a transaction record on disk.
//
// This is detailed in the transaction record state machine below:
//
//	+----------------------------------------------------+
//	| vars                                               |
//	|----------------------------------------------------|
//	| v1 = tsCache[push_marker(txn.id)]      = timestamp |
//	| v2 = tsCache[tombstone_marker(txn.id)] = timestamp |
//	+----------------------------------------------------+
//	| operations                                         |
//	|----------------------------------------------------|
//	| v -> t = forward v by timestamp t                  |
//	+----------------------------------------------------+
//
//	                                                                   HeartbeatTxn
//	                 PushTxn(TIMESTAMP)                              then: update record
//	                 then: v1 -> push.ts                                   v2 -> txn.ts
//	                     +------+                                        +------+
//	   PushTxn(ABORT)    |      |        HeartbeatTxn                    |      |   PushTxn(TIMESTAMP)
//	  then: v2 -> txn.ts |      v        if: v2 < txn.orig               |      v  then: v1 -> push.ts
//	                +-----------------+  then: v2 -> txn.ts      +--------------------+
//	           +----|                 |  else: fail              |                    |----+
//	           |    |                 |------------------------->|                    |    |
//	           |    |  no txn record  |                          | txn record written |    |
//	           +--->|                 |  EndTxn(STAGING)         |     [pending]      |<---+
//	                |                 |__  if: v2 < txn.orig     |                    |
//	                +-----------------+  \__ then: txn.ts -> v1  +--------------------+
//	                   |            ^       \__ else: fail       _/   |            ^
//	EndTxn(!STAGING)   |            |          \__             _/     | EndTxn(STAGING)
//	if: v2 < txn.orig  |            |             \__        _/       | then: txn.ts -> v1
//	then: txn.ts -> v1 |   Eager GC |                \____ _/______   |            |
//	      v2 -> txn.ts |      or    |                    _/        \  |            | HeartbeatTxn
//	else: fail         |   GC queue |  /----------------/          |  |            | if: epoch update
//	                   v            | v    EndTxn(!STAGING)        v  v            |
//	               +--------------------+  or PushTxn(ABORT)     +--------------------+
//	               |                    |  then: txn.ts -> v1    |                    |
//	               |                    |        v2 -> txn.ts    |                    |
//	          +--->|                    |                        |                    |----+
//	          |    | txn record written |                        | txn record written |    |
//	          |    |     [finalized]    |                        |      [staging]     |    |
//	          +----|                    |                        |                    |<---+
//	  PushTxn(*)   +--------------------+  EndTxn(!STAGING)      +--------------------+
//	  then: no-op                    ^     or PushTxn(*) + RecoverTxn   |          EndTxn(STAGING)
//	                                 |     then: v2 -> txn.ts           |          or HeartbeatTxn
//	                                 +----------------------------------+        then: update record
//
// In the diagram, CanCreateTxnRecord is consulted in all three of the
// state transitions that move away from the "no txn record" state.
// Updating v1 and v2 is performed in updateTimestampCache.
//
// The are three separate simplifications to the transaction model that would
// allow us to simplify this state machine:
//
//  1. as discussed on the comment on txnHeartbeater, it is reasonable to expect
//     that we will eventually move away from tracking transaction liveness on
//     a per-transaction basis. This means that we would no longer need
//     transaction heartbeats and would never need to write a transaction record
//     until a transaction is ready to complete.
//
//  2. one of the two possibilities for the "txn record written [finalized]"
//     state is that the transaction record is aborted. There used to be two
//     reasons to persist transaction records with the ABORTED status. The first
//     was because doing so was the only way for concurrent actors to prevent
//     the record from being re-written by the transaction going forward. The
//     concurrent actor would write an aborted transaction record and then wait
//     for the GC to clean it up later. The other reasons for writing the
//     transaction records with the ABORTED status was because these records
//     could point at intents, which assisted the cleanup process for these
//     intents. However, this only held for ABORTED records written on behalf
//     of the transaction coordinator itself. If a transaction was aborted by a
//     concurrent actor, its record would not immediately contain any of the
//     transaction's intents.
//
//     The first reason here no longer holds. Concurrent actors now bump the
//     timestamp cache when aborting a transaction, which has the same
//     effect as writing an ABORTED transaction record. See the "tombstone
//     marker". The second reason still holds but is fairly weak. A transaction
//     coordinator can kick off intent resolution for an aborted transaction
//     without needing to write these intents into the record itself. In the
//     worst case, this intent resolution fails and each intent is cleaned up
//     individually as it is discovered. All in all, neither justification for
//     this state holds much weight anymore.
//
//  3. the other possibility for the "txn record written [finalized]" state is
//     that the transaction record is committed. This state is currently
//     critical for the transaction model because intent resolution cannot begin
//     before a transaction record enters this state. However, this doesn't need
//     to be the case forever. There are proposals to modify the state of
//     committed key-value writes slightly such that intent resolution could be
//     run for implicitly committed transactions while their transaction record
//     remains in the  "txn record written [staging]" state. For this to work,
//     the recovery mechanism for indeterminate commit errors would need to be
//     able to determine whether an intent or a **committed value** indicated
//     the success of a write that was in-flight at the time the transaction
//     record was staged. This poses challenges migration and garbage
//     collection, but it would have a number of performance benefits.
//
// If we were to perform change #1, we could remove the "txn record written
// [pending]" state. If we were to perform change #2 and #3, we could remove the
// "txn record written [finalized]" state. All together, this would leave us
// with only two states that the transaction record could be in, written or not
// written. At that point, it begins to closely resemble any other write in the
// system.
func (r *Replica) CanCreateTxnRecord(
	ctx context.Context, txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
) (ok bool, reason roachpb.TransactionAbortedReason) {
	// Consult the timestamp cache with the transaction's key. The timestamp cache
	// is used to abort transactions that don't have transaction records.
	//
	// Using this strategy, we enforce the invariant that only requests sent
	// from a transaction's own coordinator can create its transaction record.
	// However, once a transaction record is written, other concurrent actors
	// can modify it. This is reflected in the diagram above.
	tombstoneKey := transactionTombstoneMarker(txnKey, txnID)

	// Look in the timestamp cache to see if there is a tombstone entry for
	// this transaction, which indicates that this transaction has already written
	// a transaction record. If there is an entry, then we return a retriable
	// error: if this is a re-evaluation, then the error will be transformed into
	// an ambiguous one higher up. Otherwise, if the client is still waiting for
	// a result, then this cannot be a "replay" of any sort.
	tombstoneTimestamp, tombstoneTxnID := r.store.tsCache.GetMax(tombstoneKey, nil /* end */)
	// Compare against the minimum timestamp that the transaction could have
	// written intents at.
	if txnMinTS.LessEq(tombstoneTimestamp) {
		switch tombstoneTxnID {
		case txnID:
			// If we find our own transaction ID then a transaction record has already
			// been written. We might be a replay (e.g. a DistSender retry), or we
			// raced with an asynchronous abort. Either way, return an error.
			//
			// TODO(andrei): We could keep a bit more info in the tscache to return a
			// different error for COMMITTED transactions. If the EndTxn(commit) was
			// the only request in the batch, this this would be sufficient for the
			// client to swallow the error and declare the transaction as committed.
			// If there were other requests in the EndTxn batch, then the client would
			// still have trouble reconstructing the result, but at least it could
			// provide a non-ambiguous error to the application.
			return false, roachpb.ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY
		case uuid.Nil:
			lease, _ /* nextLease */ := r.GetLease()
			// Recognize the case where a lease started recently. Lease transfers bump
			// the ts cache low water mark.
			if tombstoneTimestamp == lease.Start.ToTimestamp() {
				return false, roachpb.ABORT_REASON_NEW_LEASE_PREVENTS_TXN
			}
			return false, roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED
		default:
			// If we find another transaction's ID then that transaction has
			// aborted us before our transaction record was written. It obeyed
			// the restriction that it couldn't create a transaction record for
			// us, so it recorded a tombstone cache instead to prevent us
			// from ever creating a transaction record.
			return false, roachpb.ABORT_REASON_ABORTED_RECORD_FOUND
		}
	}

	return true, 0
}

// MinTxnCommitTS determines the minimum timestamp at which a transaction with
// the provided ID and key can commit.
//
// The method serves as a mechanism through which successful PushTxn(TIMESTAMP)
// requests convey information to pushee transactions. In doing so, it ensures
// that transaction records are committed with a sufficiently high timestamp
// after a successful PushTxn(TIMESTAMP). As a result of this mechanism, a
// transaction never needs to write to the transaction record for contending
// transactions that it forms write-read conflicts with.
//
// NOTE: write-write conflicts result in PushTxn(ABORT) requests, which
// coordinate with pushee transactions through the CanCreateTxnRecord mechanism
// (see above).
//
// The mechanism is detailed in the transaction record state machine above on
// CanCreateTxnRecord.
func (r *Replica) MinTxnCommitTS(
	ctx context.Context, txnID uuid.UUID, txnKey []byte,
) hlc.Timestamp {
	// Look in the timestamp cache to see if there is a push marker entry for this
	// transaction, which contains the minimum timestamp that the transaction can
	// commit at. This is used by pushers to push the timestamp of a transaction
	// without writing to the pushee's transaction record.
	pushKey := transactionPushMarker(txnKey, txnID)
	minCommitTS, _ := r.store.tsCache.GetMax(pushKey, nil /* end */)
	return minCommitTS
}

// Pseudo range local key suffixes used to construct "marker" keys for use
// with the timestamp cache. These must not collide with a real range local
// key suffix defined in pkg/keys/constants.go.
var (
	localTxnTombstoneMarkerSuffix = roachpb.RKey("tmbs")
	localTxnPushMarkerSuffix      = roachpb.RKey("push")
)

// transactionTombstoneMarker returns the key used as a marker indicating that a
// particular txn has written a transaction record (which may or may not still
// exist). It serves as a guard against recreating a transaction record after it
// has been cleaned up (i.e. by a BeginTxn being evaluated out of order or
// arriving after another txn Push(Abort)'ed the txn). It is also used to check
// for existing txn records when considering 1PC evaluation without hitting
// disk.
func transactionTombstoneMarker(key roachpb.Key, txnID uuid.UUID) roachpb.Key {
	return keys.MakeRangeKey(keys.MustAddr(key), localTxnTombstoneMarkerSuffix, txnID.GetBytes())
}

// transactionPushMarker returns the key used as a marker indicating that a
// particular txn was pushed before writing its transaction record. It is used
// as a marker in the timestamp cache indicating that the transaction was pushed
// in case the push happens before there's a transaction record.
func transactionPushMarker(key roachpb.Key, txnID uuid.UUID) roachpb.Key {
	return keys.MakeRangeKey(keys.MustAddr(key), localTxnPushMarkerSuffix, txnID.GetBytes())
}

// GetCurrentReadSummary returns a new ReadSummary reflecting all reads served
// by the range to this point.
func (r *Replica) GetCurrentReadSummary(ctx context.Context) rspb.ReadSummary {
	sum := collectReadSummaryFromTimestampCache(r.store.tsCache, r.Desc())
	// Forward the read summary by the range's closed timestamp, because any
	// replica could have served reads below this time. We also return the
	// closed timestamp separately, in case callers want it split out.
	closedTS := r.GetCurrentClosedTimestamp(ctx)
	sum.Merge(rspb.FromTimestamp(closedTS))
	return sum
}

// collectReadSummaryFromTimestampCache constucts a read summary for the range
// with the specified descriptor using the timestamp cache.
func collectReadSummaryFromTimestampCache(
	tc tscache.Cache, desc *roachpb.RangeDescriptor,
) rspb.ReadSummary {
	var s rspb.ReadSummary
	s.Local.LowWater, _ = tc.GetMax(
		keys.MakeRangeKeyPrefix(desc.StartKey),
		keys.MakeRangeKeyPrefix(desc.EndKey),
	)
	userKeys := desc.KeySpan()
	s.Global.LowWater, _ = tc.GetMax(
		userKeys.Key.AsRawKey(),
		userKeys.EndKey.AsRawKey(),
	)

	return s
}

// applyReadSummaryToTimestampCache updates the timestamp cache to reflect the
// reads present in the provided read summary. This ensures that no future
// writes in either the local or global keyspace are allowed to invalidate
// ("write underneath") prior reads.
func applyReadSummaryToTimestampCache(
	tc tscache.Cache, desc *roachpb.RangeDescriptor, s rspb.ReadSummary,
) {
	tc.Add(
		keys.MakeRangeKeyPrefix(desc.StartKey),
		keys.MakeRangeKeyPrefix(desc.EndKey),
		s.Local.LowWater,
		uuid.Nil, /* txnID */
	)
	userKeys := desc.KeySpan()
	tc.Add(
		userKeys.Key.AsRawKey(),
		userKeys.EndKey.AsRawKey(),
		s.Global.LowWater,
		uuid.Nil, /* txnID */
	)
}
