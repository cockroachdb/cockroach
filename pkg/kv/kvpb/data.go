// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// PrepareTransactionForRetry returns a new Transaction to be used for retrying
// the original Transaction. Depending on the error, this might return an
// already-existing Transaction with an incremented epoch, or a completely new
// Transaction.
//
// The caller should generally check that the error was meant for this
// Transaction before calling this.
//
// pri is the priority that should be used when giving the restarted transaction
// the chance to get a higher priority. Not used when the transaction is being
// aborted.
//
// In case retryErr tells us that a new Transaction needs to be created,
// isolation and name help initialize this new transaction.
func PrepareTransactionForRetry(
	ctx context.Context, pErr *Error, pri roachpb.UserPriority, clock *hlc.Clock,
) roachpb.Transaction {
	if pErr.TransactionRestart() == TransactionRestart_NONE {
		log.Fatalf(ctx, "invalid retryable err (%T): %s", pErr.GetDetail(), pErr)
	}

	if pErr.GetTxn() == nil {
		log.Fatalf(ctx, "missing txn for retryable error: %s", pErr)
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
			// We have errTxnPri, but this wants a roachpb.UserPriority. So
			// we're going to overwrite the priority below.
			roachpb.NormalUserPriority,
			now.ToTimestamp(),
			clock.MaxOffset().Nanoseconds(),
			txn.CoordinatorNodeID,
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
			// This helps transactions that had their commit timestamp fixed (See
			// roachpb.Transaction.CommitTimestampFixed for details on when it happens)
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
		log.Fatalf(
			ctx, "unexpected intent missing error (%T); should be transformed into retry error", pErr.GetDetail(),
		)
	default:
		log.Fatalf(ctx, "invalid retryable err (%T): %s", pErr.GetDetail(), pErr)
	}
	if !aborted {
		if txn.Status.IsFinalized() {
			log.Fatalf(ctx, "transaction unexpectedly finalized in (%T): %s", pErr.GetDetail(), pErr)
		}
		txn.Restart(pri, txn.Priority, txn.WriteTimestamp)
	}
	return txn
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
