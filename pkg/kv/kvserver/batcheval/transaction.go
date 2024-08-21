// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ErrTransactionUnsupported is returned when a non-transactional command is
// evaluated in the context of a transaction.
var ErrTransactionUnsupported = errors.AssertionFailedf("not supported within a transaction")

// VerifyTransaction runs sanity checks verifying that the transaction in the
// header and the request are compatible.
func VerifyTransaction(
	h kvpb.Header, args kvpb.Request, permittedStatuses ...roachpb.TransactionStatus,
) error {
	if h.Txn == nil {
		return errors.AssertionFailedf("no transaction specified to %s", args.Method())
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return errors.AssertionFailedf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	statusPermitted := false
	for _, s := range permittedStatuses {
		if h.Txn.Status == s {
			statusPermitted = true
			break
		}
	}
	if !statusPermitted {
		reason := kvpb.TransactionStatusError_REASON_UNKNOWN
		if h.Txn.Status == roachpb.COMMITTED {
			reason = kvpb.TransactionStatusError_REASON_TXN_COMMITTED
		}
		return kvpb.NewTransactionStatusError(reason,
			redact.Sprintf("cannot perform %s with txn status %v", args.Method(), h.Txn.Status))
	}
	return nil
}

// WriteAbortSpanOnResolve returns true if the abort span must be written when
// the transaction with the given status is resolved. It avoids instructing the
// caller to write to the abort span if the caller didn't actually remove any
// intents but intends to poison.
func WriteAbortSpanOnResolve(status roachpb.TransactionStatus, poison, removedIntents bool) bool {
	if status != roachpb.ABORTED {
		// Only update the AbortSpan for aborted transactions.
		return false
	}
	if !poison {
		// We can remove any entries from the AbortSpan.
		return true
	}
	// We only need to add AbortSpan entries for transactions that we have
	// invalidated by removing intents. This avoids leaking AbortSpan entries if
	// a request raced with txn record GC and mistakenly interpreted a committed
	// txn as aborted only to return to the intent it wanted to push and find it
	// already resolved. We're only required to write an entry if we do
	// something that could confuse/invalidate a zombie transaction.
	return removedIntents
}

// UpdateAbortSpan clears any AbortSpan entry if poison is false. Otherwise, if
// poison is true, it creates an entry for this transaction in the AbortSpan to
// prevent future reads or writes from spuriously succeeding on this range.
func UpdateAbortSpan(
	ctx context.Context,
	rec EvalContext,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	txn enginepb.TxnMeta,
	poison bool,
) error {
	// Read the current state of the AbortSpan so we can detect when
	// no changes are needed. This can help us avoid unnecessary Raft
	// proposals.
	var curEntry roachpb.AbortSpanEntry
	exists, err := rec.AbortSpan().Get(ctx, readWriter, txn.ID, &curEntry)
	if err != nil {
		return err
	}

	if !poison {
		if !exists {
			return nil
		}
		return rec.AbortSpan().Del(ctx, readWriter, ms, txn.ID)
	}

	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.WriteTimestamp,
		Priority:  txn.Priority,
	}
	if exists && curEntry.Equal(entry) {
		return nil
	}
	// curEntry already escapes, so assign entry to curEntry and pass
	// that to Put instead of allowing entry to escape as well.
	curEntry = entry
	return rec.AbortSpan().Put(ctx, readWriter, ms, txn.ID, &curEntry)
}

// CanCreateTxnRecord determines whether a transaction record can be created for
// the provided transaction. If not, the function will return an error.
func CanCreateTxnRecord(ctx context.Context, rec EvalContext, txn *roachpb.Transaction) error {
	// The transaction could not have written a transaction record previously
	// with a timestamp below txn.MinTimestamp.
	ok, reason := rec.CanCreateTxnRecord(ctx, txn.ID, txn.Key, txn.MinTimestamp)
	if !ok {
		log.VEventf(ctx, 2, "txn tombstone present; transaction has been aborted")
		return kvpb.NewTransactionAbortedError(reason)
	}
	// Verify that if the transaction record is being created, the client thinks
	// it's in the PENDING state.
	if txn.Status != roachpb.PENDING {
		return errors.AssertionFailedf(
			"cannot create transaction record with non-PENDING transaction: %v", txn)
	}
	return nil
}

// BumpToMinTxnCommitTS increases the provided transaction's write timestamp to
// the minimum timestamp at which it is allowed to commit. The transaction must
// be PENDING.
func BumpToMinTxnCommitTS(ctx context.Context, rec EvalContext, txn *roachpb.Transaction) {
	if txn.Status != roachpb.PENDING {
		log.Fatalf(ctx, "non-pending txn passed to BumpToMinTxnCommitTS: %v", txn)
	}
	minCommitTS := rec.MinTxnCommitTS(ctx, txn.ID, txn.Key)
	if bumped := txn.WriteTimestamp.Forward(minCommitTS); bumped {
		log.VEventf(ctx, 2, "write timestamp bumped by txn tombstone to: %s", txn.WriteTimestamp)
	}
}

// SynthesizeTxnFromMeta creates a synthetic transaction object from
// the provided transaction metadata. The synthetic transaction is not
// meant to be persisted, but can serve as a representation of the
// transaction for outside observation. The function also checks
// whether it is possible for the transaction to ever create a
// transaction record in the future. If not, the returned transaction
// will be marked as ABORTED and it is safe to assume that the
// transaction record will never be written in the future.
//
// Note that the Transaction object returned by this function is
// inadequate to perform further KV reads or to perform intent
// resolution on its behalf, even if its state is PENDING. This is
// because the original Transaction object may have been partially
// rolled back and marked some of its intents as "ignored"
// (txn.IgnoredSeqNums != nil), but this state is not stored in
// TxnMeta. Proceeding to KV reads or intent resolution without this
// information would cause a partial rollback, if any, to be reverted
// and yield inconsistent data.
func SynthesizeTxnFromMeta(
	ctx context.Context, rec EvalContext, txn enginepb.TxnMeta,
) roachpb.Transaction {
	synth := roachpb.TransactionRecord{
		TxnMeta: txn,
		Status:  roachpb.PENDING,
		// Set the LastHeartbeat timestamp to the transactions's MinTimestamp. We
		// use this as an indication of client activity. Note that we cannot use
		// txn.WriteTimestamp for that purpose, as the WriteTimestamp could have
		// been bumped by other pushers.
		LastHeartbeat: txn.MinTimestamp,
	}

	// If the transaction metadata's min timestamp is empty this intent must
	// have been written by a transaction coordinator before v19.2. We can just
	// consider the transaction to be aborted, because its coordinator must now
	// be dead.
	if txn.MinTimestamp.IsEmpty() {
		synth.Status = roachpb.ABORTED
		return synth.AsTransaction()
	}

	// Determine whether the record could ever be allowed to be written in the
	// future. The transaction could not have written a transaction record
	// previously with a timestamp below txn.MinTimestamp.
	ok, _ := rec.CanCreateTxnRecord(ctx, txn.ID, txn.Key, txn.MinTimestamp)
	if ok {
		// Forward the provisional commit timestamp by the minimum timestamp that
		// the transaction would be able to commit at.
		minCommitTS := rec.MinTxnCommitTS(ctx, txn.ID, txn.Key)
		synth.WriteTimestamp.Forward(minCommitTS)
	} else {
		// Mark the transaction as ABORTED because it is uncommittable.
		synth.Status = roachpb.ABORTED
	}
	return synth.AsTransaction()
}
