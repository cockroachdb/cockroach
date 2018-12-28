// Copyright 2014 The Cockroach Authors.
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

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// ErrTransactionUnsupported is returned when a non-transactional command is
// evaluated in the context of a transaction.
var ErrTransactionUnsupported = errors.New("not supported within a transaction")

// VerifyTransaction runs sanity checks verifying that the transaction in the
// header and the request are compatible.
func VerifyTransaction(h roachpb.Header, args roachpb.Request) error {
	if h.Txn == nil {
		return errors.Errorf("no transaction specified to %s", args.Method())
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return errors.Errorf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	return nil
}

// WriteAbortSpanOnResolve returns true if the abort span must be written when
// the transaction with the given status is resolved.
func WriteAbortSpanOnResolve(status roachpb.TransactionStatus) bool {
	return status == roachpb.ABORTED
}

// SetAbortSpan clears any AbortSpan entry if poison is false.
// Otherwise, if poison is true, creates an entry for this transaction
// in the AbortSpan to prevent future reads or writes from
// spuriously succeeding on this range.
func SetAbortSpan(
	ctx context.Context,
	rec EvalContext,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txn enginepb.TxnMeta,
	poison bool,
) error {
	// Read the current state of the AbortSpan so we can detect when
	// no changes are needed. This can help us avoid unnecessary Raft
	// proposals.
	var curEntry roachpb.AbortSpanEntry
	exists, err := rec.AbortSpan().Get(ctx, batch, txn.ID, &curEntry)
	if err != nil {
		return err
	}

	if !poison {
		if !exists {
			return nil
		}
		return rec.AbortSpan().Del(ctx, batch, ms, txn.ID)
	}

	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  txn.Priority,
	}
	if exists && curEntry.Equal(entry) {
		return nil
	}
	return rec.AbortSpan().Put(ctx, batch, ms, txn.ID, &entry)
}

// CanPushWithPriority returns true if the given pusher can push the pushee
// based on its priority.
func CanPushWithPriority(pusher, pushee *roachpb.Transaction) bool {
	return (pusher.Priority > roachpb.MinTxnPriority && pushee.Priority == roachpb.MinTxnPriority) ||
		(pusher.Priority == roachpb.MaxTxnPriority && pushee.Priority < pusher.Priority)
}

// CanCreateTxnRecord determines whether a transaction record can be created
// for the provided transaction. If not, it returns an updated transaction
// that would be allowed to write a transaction record, along with an error.
//
// TODO(nvanbenschoten): This function can be used in cmd_push_txn.go and
// cmd_query_txn.go to determine whether a transaction record is allowed to be
// synthesized to get around the complications of eagerly GCed transaction
// records. I believe this is exactly what @andreimatei was suggesting. To do
// this, we'll need to address the TODO comment in below. If we do make this the
// case, update this comment to reflect that once this method returns false, it
// will never return true for the same txn.
func CanCreateTxnRecord(
	ctx context.Context, rec EvalContext, txn *roachpb.Transaction,
) (ok bool, errTxn *roachpb.Transaction, err error) {
	// We look in the timestamp cache to see if there is a tombstone entry for
	// this transaction, which would indicate this transaction has already been
	// finalized. If there is one, then we return a retriable into an ambiguous
	// one higher up. Otherwise, if the client is still waiting for a result,
	// then this cannot be a "replay" of any sort.
	//
	// The retriable error we return is a TransactionAbortedError, instructing
	// the client to create a new transaction. Since a transaction record
	// doesn't exist, there's no point in the client to continue with the
	// existing transaction at a new epoch.
	tombTS, tombTxnID := rec.GetTxnTombstoneFromTimestampCache(txn)
	// GetTxnTombstoneFromTimestampCache will only find a timestamp interval
	// with an associated txnID on the TransactionKey if an EndTxnReq has been
	// processed. All other timestamp intervals will have no associated txnID
	// and will be due to the low-water mark.
	switch tombTxnID {
	case txn.ID:
		newTxn := txn.Clone()
		newTxn.Status = roachpb.ABORTED
		newTxn.Timestamp.Forward(tombTS.Next())
		return false, &newTxn, roachpb.NewTransactionAbortedError(
			roachpb.ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY,
		)
	case uuid.UUID{} /* noTxnID */ :
		// TODO(nvanbenschoten): I think we should be comparing against
		// txn.OrigTimestamp, not txn.Timestamp, if we want this to make
		// definitive decisions about whether a transaction record will *never*
		// be able to be written. We can't allow a concurrent txn to use this to
		// decided that a transaction can never write a txn record, then have
		// that txn bump its timestamp and squeeze around this protection.
		if !tombTS.Less(txn.Timestamp) {
			// On lease transfers the timestamp cache is reset with the transfer
			// time as the low-water mark, so if this replica recently obtained
			// the lease, this case will be true for new txns, even if they're
			// not a replay. We move the timestamp forward and return retry.
			newTxn := txn.Clone()
			newTxn.Status = roachpb.ABORTED
			newTxn.Timestamp.Forward(tombTS.Next())
			return false, &newTxn, roachpb.NewTransactionAbortedError(
				roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED_POSSIBLE_REPLAY,
			)
		}
	default:
		log.Fatalf(ctx, "unexpected tscache interval (%s,%s) for txn %s",
			tombTS, tombTxnID, txn)
	}

	// Disallow creation or modification of a transaction record if its original
	// timestamp is before the TxnSpanGCThreshold, as in that case our transaction
	// may already have been aborted by a concurrent actor which encountered one
	// of our intents (which may have been written before this entry). We compare
	// against the original timestamp because the transaction's provisional commit
	// timestamp may be moved forward over the course of a single epoch.
	//
	// See #9265.
	threshold := rec.GetTxnSpanGCThreshold()
	if txn.OrigTimestamp.Less(threshold) {
		newTxn := txn.Clone()
		newTxn.Status = roachpb.ABORTED
		newTxn.Timestamp.Forward(threshold)
		return false, &newTxn, roachpb.NewTransactionAbortedError(
			roachpb.ABORT_REASON_NEW_TXN_RECORD_TOO_OLD,
		)
	}

	return true, nil, nil
}
