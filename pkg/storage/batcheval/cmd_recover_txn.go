// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.RecoverTxn, declareKeysRecoverTransaction, RecoverTxn)
}

func declareKeysRecoverTransaction(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	rr := req.(*roachpb.RecoverTxnRequest)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rr.Txn.Key, rr.Txn.ID)})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, rr.Txn.ID)})
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
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RecoverTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.RecoverTxnResponse)

	if cArgs.Header.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if !bytes.Equal(args.Key, args.Txn.Key) {
		return result.Result{}, errors.Errorf("request key %s does not match txn key %s", args.Key, args.Txn.Key)
	}
	if h.Timestamp.Less(args.Txn.Timestamp) {
		// This condition must hold for the timestamp cache access/update to be safe.
		return result.Result{}, errors.Errorf("request timestamp %s less than txn timestamp %s", h.Timestamp, args.Txn.Timestamp)
	}
	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch transaction record; if missing, attempt to synthesize one.
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.Timestamp{}, &reply.RecoveredTxn, engine.MVCCGetOptions{},
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
		synthTxn := SynthesizeTxnFromMeta(cArgs.EvalCtx, args.Txn)
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
		case roachpb.ABORTED:
			return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
				"programming error: found ABORTED record for implicitly committed transaction: %v", reply.RecoveredTxn,
			))
		case roachpb.PENDING:
			// Once implicitly committed, the transaction should never move back
			// to the PENDING status.
			return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
				"programming error: found PENDING record for implicitly committed transaction: %v", reply.RecoveredTxn,
			))
		case roachpb.STAGING, roachpb.COMMITTED:
			if was, is := args.Txn.Epoch, reply.RecoveredTxn.Epoch; was != is {
				return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
					"programming error: epoch change by implicitly committed transaction: %v->%v", was, is,
				))
			}
			if was, is := args.Txn.Timestamp, reply.RecoveredTxn.Timestamp; was != is {
				return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
					"programming error: timestamp change by implicitly committed transaction: %v->%v", was, is,
				))
			}
			if reply.RecoveredTxn.Status == roachpb.COMMITTED {
				// The transaction commit was already made explicit.
				return result.Result{}, nil
			}
			// Continue with recovery.
		default:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn status: %s", reply.RecoveredTxn),
			)
		}
	} else {
		// Did the transaction change its epoch or timestamp in such a
		// way that it would be allowed to continue trying to commit?
		legalChange := args.Txn.Epoch < reply.RecoveredTxn.Epoch ||
			args.Txn.Timestamp.Less(reply.RecoveredTxn.Timestamp)

		switch reply.RecoveredTxn.Status {
		case roachpb.ABORTED:
			// The transaction was aborted by some other process.
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
			return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
				"programming error: cannot recover PENDING transaction in same epoch: %s", reply.RecoveredTxn,
			))
		case roachpb.COMMITTED:
			// If we successfully prevented a write that was in-flight while a
			// transaction was performing a parallel commit when we should never
			// find that transaction committed without having bumped either its
			// epoch or timestamp.
			if !legalChange {
				return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
					"programming error: found COMMITTED record for prevented implicit commit: %v", reply.RecoveredTxn,
				))
			}
			// The transaction was committed with a higher epoch or timestamp.
			return result.Result{}, nil
		case roachpb.STAGING:
			if legalChange {
				// Recovery not immediately needed because the transaction is
				// still in progress.
				return result.Result{}, nil
			}
			// Continue with recovery.
		default:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn status: %s", reply.RecoveredTxn),
			)
		}
	}

	// Merge all of the transaction's in-flight writes into its intent
	// spans set and clear the in-flight write set. Make sure to re-sort
	// and merge the intent spans to eliminate duplicates.
	for _, w := range reply.RecoveredTxn.InFlightWrites {
		sp := roachpb.Span{Key: w.Key}
		reply.RecoveredTxn.IntentSpans = append(reply.RecoveredTxn.IntentSpans, sp)
	}
	reply.RecoveredTxn.IntentSpans, _ = roachpb.MergeSpans(reply.RecoveredTxn.IntentSpans)
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
	if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
		return result.Result{}, err
	}

	// TODO(nvanbenschoten): This could use result.FromEndTxn to trigger
	// intent resolution for the recovered transaction's intents. To do
	// that, we might need to plumb in a "poison" flag on the RecoverTxn
	// request.
	result := result.Result{}
	result.Local.UpdatedTxns = &[]*roachpb.Transaction{&reply.RecoveredTxn}
	return result, nil
}
