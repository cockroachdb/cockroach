// Copyright 2019 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.RecoverTxn, declareKeysRecoverTransaction, RecoverTxn)
}

func declareKeysRecoverTransaction(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	rr := req.(*roachpb.RecoverTxnRequest)
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rr.Txn.Key, rr.Txn.ID)})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(rs.GetRangeID(), rr.Txn.ID)})
}

// RecoverTxn attempts to recover the specified transaction from an
// indeterminate commit state. Transactions enter this state when abandoned
// after updating their transaction record with a STAGING status. The RecoverTxn
// operation is invoked by a caller who encounters a transaction in this state
// after they have already queried all of the STAGING transaction's declared
// in-flight writes. The caller specifies whether all of these in-flight writes
// were found to have succeeded or whether at least one of them was prevented
// from ever succeeding. This is used by RecoverTxn to determine whether the
// result of the recovery should be committing the abandoned transaction or
// aborting it.
func RecoverTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RecoverTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.RecoverTxnResponse)

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if h.WriteTimestamp().Less(args.Txn.MinTimestamp) {
		// This condition must hold for the timestamp cache access in
		// SynthesizeTxnFromMeta and the timestamp cache update in
		// Replica.updateTimestampCache to be safe.
		return result.Result{}, errors.AssertionFailedf("RecoverTxn request timestamp %s less than txn MinTimestamp %s",
			h.Timestamp, args.Txn.MinTimestamp)
	}
	if !args.Key.Equal(args.Txn.Key) {
		return result.Result{}, errors.AssertionFailedf("RecoverTxn request key %s does not match txn key %s",
			args.Key, args.Txn.Key)
	}
	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch transaction record; if missing, attempt to synthesize one.
	if ok, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &reply.RecoveredTxn, storage.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// The transaction's record must have been removed already. If all
		// writes were found then it must have committed and if not then it
		// could have committed or could have aborted.
		//
		// Synthesize it from the provided TxnMeta to have something to return.
		// The synthesized record should have an ABORTED status because it was
		// already GCed. If not, something went wrong for us to get to this
		// point. Just like with PushTxn, we allow an ABORTED status to be
		// returned even if it is possible that the transaction was actually
		// COMMITTED. This is safe because a COMMITTED transaction must have
		// resolved all of its intents before garbage collecting its intents.
		synthTxn := SynthesizeTxnFromMeta(ctx, cArgs.EvalCtx, args.Txn)
		if synthTxn.Status != roachpb.ABORTED {
			err := errors.Errorf("txn record synthesized with non-ABORTED status: %v", synthTxn)
			return result.Result{}, err
		}
		reply.RecoveredTxn = synthTxn
		return result.Result{}, nil
	}

	// Determine whether to continue with recovery based on the state of
	// the transaction record and whether or not the transaction was found
	// to be implicitly committed.
	if args.ImplicitlyCommitted {
		// Finding all writes means that the transaction was at one point
		// implicitly committed. It should not be possible for it to have
		// changed its epoch or timestamp, and the only other valid status
		// for it to have is COMMITTED.
		switch reply.RecoveredTxn.Status {
		case roachpb.PENDING, roachpb.ABORTED:
			// Once implicitly committed, the transaction should never move back
			// to the PENDING status and it should never be ABORTED.
			//
			// In order for the second statement to be true, we need to ensure
			// that transaction records that are GCed after being COMMITTED are
			// never re-written as ABORTED. We used to allow this to happen when
			// PushTxn requests found missing transaction records because it was
			// harmless, but we now use the timestamp cache to avoid
			// needing to ever do so. If this ever becomes possible again, we'll
			// need to relax this check.
			return result.Result{}, errors.AssertionFailedf(
				"programming error: found %s record for implicitly committed transaction: %v",
				reply.RecoveredTxn.Status, reply.RecoveredTxn,
			)
		case roachpb.STAGING, roachpb.COMMITTED:
			if was, is := args.Txn.Epoch, reply.RecoveredTxn.Epoch; was != is {
				return result.Result{}, errors.AssertionFailedf(
					"programming error: epoch change by implicitly committed transaction: %v->%v", was, is,
				)
			}
			if was, is := args.Txn.WriteTimestamp, reply.RecoveredTxn.WriteTimestamp; was != is {
				return result.Result{}, errors.AssertionFailedf(
					"programming error: timestamp change by implicitly committed transaction: %v->%v", was, is,
				)
			}
			if reply.RecoveredTxn.Status == roachpb.COMMITTED {
				// The transaction commit was already made explicit.
				return result.Result{}, nil
			}
			// Continue with recovery.
		default:
			return result.Result{}, errors.AssertionFailedf("bad txn status: %s", reply.RecoveredTxn)
		}
	} else {
		// Did the transaction change its epoch or timestamp in such a
		// way that it would be allowed to continue trying to commit?
		legalChange := args.Txn.Epoch < reply.RecoveredTxn.Epoch ||
			args.Txn.WriteTimestamp.Less(reply.RecoveredTxn.WriteTimestamp)

		switch reply.RecoveredTxn.Status {
		case roachpb.ABORTED:
			// The transaction was aborted by some other process.
			return result.Result{}, nil
		case roachpb.COMMITTED:
			// If we believe we successfully prevented a write that was in-flight
			// while a transaction was performing a parallel commit then we would
			// expect that the transaction record could only be committed if it has
			// a higher epoch or timestamp (see legalChange). This is true if we did
			// actually prevent the in-flight write.
			//
			// However, due to QueryIntent's implementation, a successful intent
			// write that was already resolved after the parallel commit finished
			// can be mistaken for a missing in-flight write by a recovery process.
			// This ambiguity is harmless, as the transaction stays committed either
			// way, but it means that we can't be quite as strict about what we
			// assert here as we would like to be.
			//
			// If QueryIntent could detect that a resolved intent satisfied its
			// query then we could assert that the transaction record can only be
			// COMMITTED if legalChange=true.
			return result.Result{}, nil
		case roachpb.PENDING:
			if args.Txn.Epoch < reply.RecoveredTxn.Epoch {
				// Recovery not immediately needed because the transaction is
				// still in progress.
				return result.Result{}, nil
			}

			// We should never hit this. The transaction recovery process will only
			// ever be launched for a STAGING transaction and it is not possible for
			// a transaction to move back to the PENDING status in the same epoch.
			return result.Result{}, errors.AssertionFailedf(
				"programming error: cannot recover PENDING transaction in same epoch: %s", reply.RecoveredTxn,
			)
		case roachpb.STAGING:
			if legalChange {
				// Recovery not immediately needed because the transaction is
				// still in progress.
				return result.Result{}, nil
			}
			// Continue with recovery.
		default:
			return result.Result{}, errors.AssertionFailedf("bad txn status: %s", reply.RecoveredTxn)
		}
	}

	// Merge all of the transaction's in-flight writes into its lock
	// spans set and clear the in-flight write set. Make sure to re-sort
	// and merge the lock spans to eliminate duplicates.
	for _, w := range reply.RecoveredTxn.InFlightWrites {
		sp := roachpb.Span{Key: w.Key}
		reply.RecoveredTxn.LockSpans = append(reply.RecoveredTxn.LockSpans, sp)
	}
	reply.RecoveredTxn.LockSpans, _ = roachpb.MergeSpans(&reply.RecoveredTxn.LockSpans)
	reply.RecoveredTxn.InFlightWrites = nil

	// Recover the transaction based on whether or not all of its writes
	// succeeded. If all of the writes succeeded then the transaction was
	// implicitly committed and an acknowledgement of success may have already
	// been returned to clients. If not, then we should have prevented the
	// transaction from ever becoming implicitly committed at this timestamp
	// using a QueryIntent, so we're free to abort the transaction record.
	if args.ImplicitlyCommitted {
		reply.RecoveredTxn.Status = roachpb.COMMITTED
	} else {
		reply.RecoveredTxn.Status = roachpb.ABORTED
	}
	txnRecord := reply.RecoveredTxn.AsRecord()
	if err := storage.MVCCPutProto(ctx, readWriter, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
		return result.Result{}, err
	}

	// TODO(nvanbenschoten): This could use result.FromEndTxn to trigger
	// intent resolution for the recovered transaction's intents. To do
	// that, we might need to plumb in a "poison" flag on the RecoverTxn
	// request.
	result := result.Result{}
	result.Local.UpdatedTxns = []*roachpb.Transaction{&reply.RecoveredTxn}
	return result, nil
}
