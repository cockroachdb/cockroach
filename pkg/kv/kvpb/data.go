// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvpb contains basic utilities for the kv layer.
package kvpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// PrepareTransactionForRetry returns a new Transaction to be used for retrying
// the original Transaction. Depending on the error, this might return an
// already-existing Transaction with an incremented epoch and timestamp, an
// already-existing Transaction with the same epoch and an incremented
// timestamp, or a completely new Transaction. For details, see the comment on
// TransactionRetryWithProtoRefreshError.NextTransaction.
//
// The caller should generally check that the error was meant for this
// Transaction before calling this.
//
// pri is the priority that should be used when giving the restarted transaction
// the chance to get a higher priority. Not used when the transaction is being
// aborted.
func PrepareTransactionForRetry(
	pErr *Error, pri roachpb.UserPriority, clock *hlc.Clock,
) (roachpb.Transaction, error) {
	if pErr == nil {
		return roachpb.Transaction{}, errors.AssertionFailedf("nil error")
	}
	if pErr.TransactionRestart() == TransactionRestart_NONE {
		return roachpb.Transaction{}, errors.AssertionFailedf(
			"invalid retryable error (%T): %s", pErr.GetDetail(), pErr)
	}
	if pErr.GetTxn() == nil {
		return roachpb.Transaction{}, errors.AssertionFailedf(
			"missing txn for retryable error: %s", pErr)
	}

	txn := *pErr.GetTxn()
	aborted := false
	switch tErr := pErr.GetDetail().(type) {
	case *TransactionAbortedError:
		// The txn coming with a TransactionAbortedError is not supposed to be used
		// for the restart. Instead, a brand new transaction is created.
		aborted = true
		// TODO(andrei): Should we preserve the ObservedTimestamps across the
		// restart?
		errTxnPri := txn.Priority
		// Start the new transaction at the current time from the local clock.
		// The local hlc should have been advanced to at least the error's
		// timestamp already.
		now := clock.NowAsClockTimestamp()
		txn = roachpb.MakeTransaction(
			txn.Name,
			nil, // baseKey
			txn.IsoLevel,
			// We have errTxnPri, but this wants a roachpb.UserPriority. So
			// we're going to overwrite the priority below.
			roachpb.NormalUserPriority,
			now.ToTimestamp(),
			clock.MaxOffset().Nanoseconds(),
			txn.CoordinatorNodeID,
			admissionpb.WorkPriority(txn.AdmissionPriority),
			txn.OmitInRangefeeds,
		)
		// Use the priority communicated back by the server.
		txn.Priority = errTxnPri
	case *ReadWithinUncertaintyIntervalError:
		txn.WriteTimestamp.Forward(tErr.RetryTimestamp())
	case *TransactionPushError:
		// Increase timestamp if applicable, ensuring that we're just ahead of
		// the pushee.
		txn.WriteTimestamp.Forward(tErr.PusheeTxn.WriteTimestamp)
		txn.UpgradePriority(tErr.PusheeTxn.Priority - 1)
	case *TransactionRetryError:
		// Transaction.Timestamp has already been forwarded to be ahead of any
		// timestamp cache entries or newer versions which caused the restart.
		if tErr.Reason == RETRY_SERIALIZABLE {
			// For RETRY_SERIALIZABLE case, we want to bump timestamp further than
			// timestamp cache.
			// This helps transactions that had their read timestamp fixed (See
			// roachpb.Transaction.ReadTimestampFixed for details on when it happens)
			// or transactions that hit read-write contention and can't bump
			// read timestamp because of later writes.
			// Upon retry, we want those transactions to restart on now() instead of
			// closed ts to give them some time to complete without a need to refresh
			// read spans yet again and possibly fail.
			// The tradeoff here is that transactions that failed because they were
			// waiting on locks or were slowed down in their first epoch for any other
			// reason (e.g. lease transfers, network congestion, node failure, etc.)
			// would have a chance to retry and succeed, but transactions that are
			// just slow would still retry indefinitely and delay transactions that
			// try to write to the keys this transaction reads because reads are not
			// in the past anymore.
			now := clock.Now()
			txn.WriteTimestamp.Forward(now)
		}
	case *WriteTooOldError:
		// Increase the timestamp to the ts at which we've actually written.
		txn.WriteTimestamp.Forward(tErr.RetryTimestamp())
	case *IntentMissingError:
		// IntentMissingErrors are not expected to be handled at this level;
		// We instead expect the txnPipeliner to transform them into a
		// TransactionRetryErrors(RETRY_ASYNC_WRITE_FAILURE) error.
		return roachpb.Transaction{}, errors.AssertionFailedf(
			"unexpected intent missing error (%T); should be transformed into retry error", pErr.GetDetail())
	default:
		return roachpb.Transaction{}, errors.AssertionFailedf(
			"invalid retryable err (%T): %s", pErr.GetDetail(), pErr)
	}
	if !aborted {
		if txn.Status.IsFinalized() {
			return roachpb.Transaction{}, errors.AssertionFailedf(
				"transaction unexpectedly finalized in (%T): %s", pErr.GetDetail(), pErr)
		}
		// If the transaction is not aborted, the retry error instead tells the
		// transaction to adjust its current read snapshot (to txn.WriteTimestamp)
		// to avoid some form of isolation or consistency related retry condition.
		// The handling of these errors is isolation level dependent.
		//
		// For isolation levels that use a single read timestamp across the entire
		// transaction (snapshot and serializable), transactions are only permitted
		// to adjust their read snapshot if they restart from the beginning and
		// discard all prior writes. In these cases, we perform the restart below by
		// incrementing the transaction's epoch.
		//
		// For isolation levels that use per-statement read timestamps which may
		// differ between statements (read committed), transactions are permitted to
		// adjust their read snapshot mid-transaction as long as they restart the
		// current statement. In these cases, we advance the transaction's read
		// timestamp below without incrementing the epoch and without discarding any
		// prior writes. The user of the transaction (e.g. the SQL layer) is
		// responsible for employing savepoints to selectively discard the writes
		// from the current statement when it retries that statement.
		if !txn.IsoLevel.PerStatementReadSnapshot() {
			txn.Restart(pri, txn.Priority, txn.WriteTimestamp)
		} else {
			txn.BumpReadTimestamp(txn.WriteTimestamp)
		}
	}
	return txn, nil
}

// TransactionRefreshTimestamp returns whether the supplied error is a retry
// error that can be discarded if the transaction in the error is refreshed. If
// true, the function returns the timestamp that the Transaction object should
// be refreshed at in order to discard the error and avoid a restart.
func TransactionRefreshTimestamp(pErr *Error) (bool, hlc.Timestamp) {
	txn := pErr.GetTxn()
	if txn == nil {
		return false, hlc.Timestamp{}
	}
	timestamp := txn.WriteTimestamp
	switch err := pErr.GetDetail().(type) {
	case *TransactionRetryError:
		if err.Reason != RETRY_SERIALIZABLE && err.Reason != RETRY_WRITE_TOO_OLD {
			return false, hlc.Timestamp{}
		}
	case *WriteTooOldError:
		// TODO(andrei): Chances of success for on write-too-old conditions might be
		// usually small: if our txn previously read the key that generated this
		// error, obviously the refresh will fail. It might be worth trying to
		// detect these cases and save the futile attempt; we'd need to have access
		// to the key that generated the error.
		timestamp.Forward(err.RetryTimestamp())
	case *ReadWithinUncertaintyIntervalError:
		timestamp.Forward(err.RetryTimestamp())
	default:
		return false, hlc.Timestamp{}
	}
	return true, timestamp
}

// LeaseAppliedIndex is attached to every Raft message and is used for replay
// protection.
type LeaseAppliedIndex uint64

// SafeValue implements the redact.SafeValue interface.
func (s LeaseAppliedIndex) SafeValue() {}

// RaftTerm represents the term of a raft message. This corresponds to Term in
// HardState.Term in the Raft library. That type is a uint64, so it is necessary
// to cast to/from that type when dealing with the Raft library, however
// internally RaftTerm is used for all fields in CRDB.
type RaftTerm uint64

// SafeValue implements the redact.SafeValue interface.
func (s RaftTerm) SafeValue() {}

// RaftIndex represents the term of a raft message. This corresponds to Index in
// HardState.Index in the Raft library. That type is a uint64, so it is
// necessary to cast to/from that type when dealing with the Raft library,
// however internally RaftIndex is used for all fields in CRDB.
type RaftIndex uint64

// SafeValue implements the redact.SafeValue interface.
func (s RaftIndex) SafeValue() {}
