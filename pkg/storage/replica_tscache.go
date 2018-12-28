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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// updateTimestampCache updates the timestamp cache in order to set a low water
// mark for the timestamp at which mutations to keys overlapping the provided
// request can write, such that they don't re-write history.
func (r *Replica) updateTimestampCache(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	readOnlyUseReadCache := true
	if r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset {
		// Clockless mode: all reads count as writes.
		readOnlyUseReadCache = false
	}

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
			case *roachpb.ConditionalPutRequest:
				if pErr != nil {
					// ConditionalPut still updates on ConditionFailedErrors.
					if _, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); !ok {
						continue
					}
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
			case *roachpb.ScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for forward scan, the resume span will start at
					// the (last key read).Next(), which is actually the correct
					// end key for the span to update the timestamp cache.
					end = resp.ResumeSpan.Key
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
			case *roachpb.ReverseScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
				if resp.ResumeSpan != nil {
					// Note that for reverse scans, the resume span's end key is
					// an open interval. That means it was read as part of this op
					// and won't be read on resume. It is the correct start key for
					// the span to update the timestamp cache.
					start = resp.ResumeSpan.EndKey
				}
				tc.Add(start, end, ts, txnID, readOnlyUseReadCache)
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
						tc.Add(start, end, t.Txn.Timestamp, uuid.UUID{}, readOnlyUseReadCache)
					}
				}
			case *roachpb.RefreshRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			case *roachpb.RefreshRangeRequest:
				tc.Add(start, end, ts, txnID, !t.Write /* readCache */)
			default:
				readCache := readOnlyUseReadCache
				if roachpb.UpdatesWriteTimestampCache(args) {
					readCache = false
				}
				tc.Add(start, end, ts, txnID, readCache)
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
// for the provided transaction. If not, it returns the reason that transaction
// record was rejected. If the method ever determines that a transaction record
// must be rejected, it will continue to reject that transaction going forwards.
func (r *Replica) CanCreateTxnRecord(
	txn *roachpb.Transaction,
) (bool, roachpb.TransactionAbortedReason) {
	// We make comparisons below against the epoch zero timestamp because the
	// transaction's provisional commit timestamp may be moved forward over the
	// course of a single epoch and its original timestamp may have moved
	// forward over the course of a series of epochs.
	minTxnTS, _ := txn.InclusiveTimeBounds()

	// We look in the timestamp cache to see if there is an entry for this
	// transaction, which would indicate this transaction has already been
	// finalized. If there is an entry, then we return a retriable error: if
	// this is a re-evaluation, then the error will be transformed into an
	// ambiguous one higher up. Otherwise, if the client is still waiting for a
	// result, then this cannot be a "replay" of any sort.
	key := keys.TransactionKey(txn.Key, txn.ID)
	wTS, wTxnID := r.store.tsCache.GetMaxWrite(key, nil /* end */)
	// GetMaxWrite will only find a timestamp interval with an associated txnID
	// on the TransactionKey if an EndTxnReq has been processed. All other
	// timestamp intervals will have no associated txnID and will be due to the
	// low-water mark.
	switch wTxnID {
	case txn.ID:
		return false, roachpb.ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY
	case uuid.UUID{} /* noTxnID */ :
		if !wTS.Less(minTxnTS) {
			// On lease transfers the timestamp cache is reset with the transfer
			// time as the low-water mark, so if this replica recently obtained
			// the lease, this case will be true for new txns, even if they're
			// not a replay. We force these txns to retry.
			return false, roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED_POSSIBLE_REPLAY
		}
	default:
		log.Fatalf(context.Background(), "unexpected tscache interval (%s,%s) for txn %s",
			wTS, wTxnID, txn)
	}

	// Disallow creation or modification of a transaction record if its original
	// timestamp is before the TxnSpanGCThreshold, as in that case our transaction
	// may already have been aborted by a concurrent actor which encountered one
	// of our intents (which may have been written before our transaction record).
	//
	// See #9265.
	threshold := r.GetTxnSpanGCThreshold()
	if minTxnTS.Less(threshold) {
		return false, roachpb.ABORT_REASON_NEW_TXN_RECORD_TOO_OLD
	}

	return true, 0
}
