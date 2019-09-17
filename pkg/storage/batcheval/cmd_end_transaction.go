// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
)

// TxnAutoGC controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var TxnAutoGC = true

func init() {
	RegisterCommand(roachpb.EndTransaction, declareKeysEndTransaction, EndTransaction)
}

func declareKeysEndTransaction(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	et := req.(*roachpb.EndTransactionRequest)
	declareKeysWriteTransaction(desc, header, req, spans)
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		abortSpanAccess := spanset.SpanReadOnly
		if !et.Commit && et.Poison {
			abortSpanAccess = spanset.SpanReadWrite
		}
		spans.Add(abortSpanAccess, roachpb.Span{
			Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID),
		})
	}

	// If the request is intending to finalize the transaction record then it
	// needs to declare a few extra keys.
	if !et.IsParallelCommit() {
		// All requests that intent on resolving local intents need to depend on
		// the range descriptor because they need to determine which intents are
		// within the local range.
		spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})

		// The spans may extend beyond this Range, but it's ok for the
		// purpose of acquiring latches. The parts in our Range will
		// be resolved eagerly.
		for _, span := range et.IntentSpans {
			spans.Add(spanset.SpanReadWrite, span)
		}

		if et.InternalCommitTrigger != nil {
			if st := et.InternalCommitTrigger.SplitTrigger; st != nil {
				// Splits may read from the entire pre-split range (they read
				// from the LHS in all cases, and the RHS only when the existing
				// stats contain estimates), but they need to declare a write
				// access to block all other concurrent writes. We block writes
				// to the RHS because they will fail if applied after the split,
				// and writes to the LHS because their stat deltas will
				// interfere with the non-delta stats computed as a part of the
				// split. (see
				// https://github.com/cockroachdb/cockroach/issues/14881)
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    st.LeftDesc.StartKey.AsRawKey(),
					EndKey: st.RightDesc.EndKey.AsRawKey(),
				})
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    keys.MakeRangeKeyPrefix(st.LeftDesc.StartKey),
					EndKey: keys.MakeRangeKeyPrefix(st.RightDesc.EndKey).PrefixEnd(),
				})
				leftRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(header.RangeID)
				spans.Add(spanset.SpanReadOnly, roachpb.Span{
					Key:    leftRangeIDPrefix,
					EndKey: leftRangeIDPrefix.PrefixEnd(),
				})

				rightRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(st.RightDesc.RangeID)
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    rightRangeIDPrefix,
					EndKey: rightRangeIDPrefix.PrefixEnd(),
				})
				rightRangeIDUnreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(st.RightDesc.RangeID)
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    rightRangeIDUnreplicatedPrefix,
					EndKey: rightRangeIDUnreplicatedPrefix.PrefixEnd(),
				})

				spans.Add(spanset.SpanReadOnly, roachpb.Span{
					Key: keys.RangeLastReplicaGCTimestampKey(st.LeftDesc.RangeID),
				})
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key: keys.RangeLastReplicaGCTimestampKey(st.RightDesc.RangeID),
				})

				spans.Add(spanset.SpanReadOnly, roachpb.Span{
					Key:    abortspan.MinKey(header.RangeID),
					EndKey: abortspan.MaxKey(header.RangeID)})
			}
			if mt := et.InternalCommitTrigger.MergeTrigger; mt != nil {
				// Merges write to the left side's abort span and the right side's data
				// and range-local spans. They also read from the right side's range ID
				// span.
				leftRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(header.RangeID)
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    leftRangeIDPrefix,
					EndKey: leftRangeIDPrefix.PrefixEnd(),
				})
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    mt.RightDesc.StartKey.AsRawKey(),
					EndKey: mt.RightDesc.EndKey.AsRawKey(),
				})
				spans.Add(spanset.SpanReadWrite, roachpb.Span{
					Key:    keys.MakeRangeKeyPrefix(mt.RightDesc.StartKey),
					EndKey: keys.MakeRangeKeyPrefix(mt.RightDesc.EndKey),
				})
				spans.Add(spanset.SpanReadOnly, roachpb.Span{
					Key:    keys.MakeRangeIDReplicatedPrefix(mt.RightDesc.RangeID),
					EndKey: keys.MakeRangeIDReplicatedPrefix(mt.RightDesc.RangeID).PrefixEnd(),
				})
			}
		}
	}
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter. Rolling back
// an already rolled-back txn is ok.
func EndTransaction(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.EndTransactionRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	reply := resp.(*roachpb.EndTransactionResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING, roachpb.STAGING, roachpb.ABORTED); err != nil {
		return result.Result{}, err
	}

	// If a 1PC txn was required and we're in EndTransaction, something went wrong.
	if args.Require1PC {
		return result.Result{}, roachpb.NewTransactionStatusError("could not commit in one phase as requested")
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	// Fetch existing transaction.
	var existingTxn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.Timestamp{}, &existingTxn, engine.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// No existing transaction record was found - create one by writing it
		// below in updateFinalizedTxn.
		reply.Txn = h.Txn.Clone()

		// Verify that it is safe to create the transaction record. We only need
		// to perform this verification for commits. Rollbacks can always write
		// an aborted txn record.
		if args.Commit {
			if err := CanCreateTxnRecord(cArgs.EvalCtx, reply.Txn); err != nil {
				return result.Result{}, err
			}
		}
	} else {
		// We're using existingTxn on the reply, although it can be stale
		// compared to the Transaction in the request (e.g. the Sequence,
		// and various timestamps). We must be careful to update it with the
		// supplied ba.Txn if we return it with an error which might be
		// retried, as for example to avoid client-side serializable restart.
		reply.Txn = &existingTxn

		// Verify that we can either commit it or abort it (according
		// to args.Commit), and also that the Timestamp and Epoch have
		// not suffered regression.
		switch reply.Txn.Status {
		case roachpb.COMMITTED:
			return result.Result{}, roachpb.NewTransactionCommittedStatusError()

		case roachpb.ABORTED:
			if !args.Commit {
				// The transaction has already been aborted by other.
				// Do not return TransactionAbortedError since the client anyway
				// wanted to abort the transaction.
				desc := cArgs.EvalCtx.Desc()
				externalIntents, err := resolveLocalIntents(ctx, desc, batch, ms, args, reply.Txn, cArgs.EvalCtx)
				if err != nil {
					return result.Result{}, err
				}
				if err := updateFinalizedTxn(
					ctx, batch, ms, key, args, reply.Txn, externalIntents,
				); err != nil {
					return result.Result{}, err
				}
				// Use alwaysReturn==true because the transaction is definitely
				// aborted, no matter what happens to this command.
				return result.FromEndTxn(reply.Txn, true /* alwaysReturn */, args.Poison), nil
			}
			// If the transaction was previously aborted by a concurrent writer's
			// push, any intents written are still open. It's only now that we know
			// them, so we return them all for asynchronous resolution (we're
			// currently not able to write on error, but see #1989).
			//
			// Similarly to above, use alwaysReturn==true. The caller isn't trying
			// to abort, but the transaction is definitely aborted and its intents
			// can go.
			reply.Txn.IntentSpans = args.IntentSpans
			return result.FromEndTxn(reply.Txn, true /* alwaysReturn */, args.Poison),
				roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_ABORTED_RECORD_FOUND)

		case roachpb.PENDING, roachpb.STAGING:
			if h.Txn.Epoch < reply.Txn.Epoch {
				return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
					"programming error: epoch regression: %d", h.Txn.Epoch,
				))
			} else if h.Txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(h.Txn.OrigTimestamp) {
				// The transaction record can only ever be pushed forward, so it's an
				// error if somehow the transaction record has an earlier timestamp
				// than the original transaction timestamp.
				return result.Result{}, roachpb.NewTransactionStatusError(fmt.Sprintf(
					"programming error: timestamp regression: %s", h.Txn.OrigTimestamp,
				))
			}

		default:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn status: %s", reply.Txn),
			)
		}

		// Update the existing txn with the supplied txn.
		reply.Txn.Update(h.Txn)
	}

	var pd result.Result

	// Attempt to commit or abort the transaction per the args.Commit parameter.
	if args.Commit {
		if retry, reason, extraMsg := IsEndTransactionTriggeringRetryError(reply.Txn, args); retry {
			return result.Result{}, roachpb.NewTransactionRetryError(reason, extraMsg)
		}

		// If the transaction needs to be staged as part of an implicit commit
		// before being explicitly committed, write the staged transaction
		// record and return without running commit triggers or resolving local
		// intents.
		if args.IsParallelCommit() {
			// It's not clear how to combine transaction recovery with commit
			// triggers, so for now we don't allow them to mix. This shouldn't
			// cause any issues and the txn coordinator knows not to mix them.
			if ct := args.InternalCommitTrigger; ct != nil {
				err := errors.Errorf("cannot stage transaction with a commit trigger: %+v", ct)
				return result.Result{}, err
			}

			reply.Txn.Status = roachpb.STAGING
			reply.StagingTimestamp = reply.Txn.Timestamp
			if err := updateStagingTxn(ctx, batch, ms, key, args, reply.Txn); err != nil {
				return result.Result{}, err
			}
			return result.Result{}, nil
		}

		// Else, the transaction can be explicitly committed.
		reply.Txn.Status = roachpb.COMMITTED

		// Merge triggers must run before intent resolution as the merge trigger
		// itself contains intents, in the RightData snapshot, that will be owned
		// and thus resolved by the new range.
		//
		// While it might seem cleaner to simply rely on asynchronous intent
		// resolution here, these intents must be resolved synchronously. We
		// maintain the invariant that there are no intents on local range
		// descriptors that belong to committed transactions. This allows nodes,
		// during startup, to infer that any lingering intents belong to in-progress
		// transactions and thus the pre-intent value can safely be used.
		if mt := args.InternalCommitTrigger.GetMergeTrigger(); mt != nil {
			mergeResult, err := mergeTrigger(ctx, cArgs.EvalCtx, batch.(engine.Batch),
				ms, mt, reply.Txn.Timestamp)
			if err != nil {
				return result.Result{}, err
			}
			if err := pd.MergeAndDestroy(mergeResult); err != nil {
				return result.Result{}, err
			}
		}
	} else {
		reply.Txn.Status = roachpb.ABORTED
	}

	// Resolve intents on the local range synchronously so that their resolution
	// ends up in the same Raft entry. There should always be at least one because
	// we position the transaction record next to the first write of a transaction.
	// This avoids the need for the intentResolver to have to return to this range
	// to resolve intents for this transaction in the future.
	desc := cArgs.EvalCtx.Desc()
	externalIntents, err := resolveLocalIntents(ctx, desc, batch, ms, args, reply.Txn, cArgs.EvalCtx)
	if err != nil {
		return result.Result{}, err
	}
	if err := updateFinalizedTxn(ctx, batch, ms, key, args, reply.Txn, externalIntents); err != nil {
		return result.Result{}, err
	}

	// Run the rest of the commit triggers if successfully committed.
	if reply.Txn.Status == roachpb.COMMITTED {
		triggerResult, err := RunCommitTrigger(ctx, cArgs.EvalCtx, batch.(engine.Batch),
			ms, args, reply.Txn)
		if err != nil {
			return result.Result{}, roachpb.NewReplicaCorruptionError(err)
		}
		if err := pd.MergeAndDestroy(triggerResult); err != nil {
			return result.Result{}, err
		}
	}

	// Note: there's no need to clear the AbortSpan state if we've successfully
	// finalized a transaction, as there's no way in which an abort cache entry
	// could have been written (the txn would already have been in
	// state=ABORTED).
	//
	// Summary of transaction replay protection after EndTransaction: When a
	// transactional write gets replayed over its own resolved intents, the
	// write will succeed but only as an intent with a newer timestamp (with a
	// WriteTooOldError). However, the replayed intent cannot be resolved by a
	// subsequent replay of this EndTransaction call because the txn timestamp
	// will be too old. Replays of requests which attempt to create a new txn
	// record (BeginTransaction, HeartbeatTxn, or EndTransaction) never succeed
	// because EndTransaction inserts in the write timestamp cache in Replica's
	// updateTimestampCache method, forcing the call to CanCreateTxnRecord to
	// return false, resulting in a transaction retry error. If the replay
	// didn't attempt to create a txn record, any push will immediately succeed
	// as a missing txn record on push where CanCreateTxnRecord returns false
	// succeeds. In both cases, the txn will be GC'd on the slow path.
	//
	// We specify alwaysReturn==false because if the commit fails below Raft, we
	// don't want the intents to be up for resolution. That should happen only
	// if the commit actually happens; otherwise, we risk losing writes.
	intentsResult := result.FromEndTxn(reply.Txn, false /* alwaysReturn */, args.Poison)
	intentsResult.Local.UpdatedTxns = &[]*roachpb.Transaction{reply.Txn}
	if err := pd.MergeAndDestroy(intentsResult); err != nil {
		return result.Result{}, err
	}
	return pd, nil
}

// IsEndTransactionExceedingDeadline returns true if the transaction
// exceeded its deadline.
func IsEndTransactionExceedingDeadline(t hlc.Timestamp, args *roachpb.EndTransactionRequest) bool {
	return args.Deadline != nil && !t.Less(*args.Deadline)
}

// IsEndTransactionTriggeringRetryError returns true if the
// EndTransactionRequest cannot be committed and needs to return a
// TransactionRetryError. It also returns the reason and possibly an extra
// message to be used for the error.
func IsEndTransactionTriggeringRetryError(
	txn *roachpb.Transaction, args *roachpb.EndTransactionRequest,
) (retry bool, reason roachpb.TransactionRetryReason, extraMsg string) {
	// If we saw any WriteTooOldErrors, we must restart to avoid lost
	// update anomalies.
	if txn.WriteTooOld {
		retry, reason = true, roachpb.RETRY_WRITE_TOO_OLD
	} else {
		origTimestamp := txn.OrigTimestamp
		origTimestamp.Forward(txn.RefreshedTimestamp)
		isTxnPushed := txn.Timestamp != origTimestamp

		// Return a transaction retry error if the commit timestamp isn't equal to
		// the txn timestamp.
		if isTxnPushed {
			retry, reason = true, roachpb.RETRY_SERIALIZABLE
		}
	}

	// A transaction can still avoid a retry under certain conditions.
	if retry && CanForwardCommitTimestampWithoutRefresh(txn, args) {
		retry, reason = false, 0
	}

	// However, a transaction must obey its deadline, if set.
	if !retry && IsEndTransactionExceedingDeadline(txn.Timestamp, args) {
		exceededBy := txn.Timestamp.GoTime().Sub(args.Deadline.GoTime())
		fromStart := txn.Timestamp.GoTime().Sub(txn.OrigTimestamp.GoTime())
		extraMsg = fmt.Sprintf(
			"txn timestamp pushed too much; deadline exceeded by %s (%s > %s), "+
				"original timestamp %s ago (%s)",
			exceededBy, txn.Timestamp, args.Deadline, fromStart, txn.OrigTimestamp)
		retry, reason = true, roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED
	}
	return retry, reason, extraMsg
}

// CanForwardCommitTimestampWithoutRefresh returns whether a txn can be
// safely committed with a timestamp above its read timestamp without
// requiring a read refresh (see txnSpanRefresher). This requires that
// the transaction's timestamp has not leaked and that the transaction
// has encountered no spans which require refreshing at the forwarded
// timestamp. If either of those conditions are true, a client-side
// retry is required.
func CanForwardCommitTimestampWithoutRefresh(
	txn *roachpb.Transaction, args *roachpb.EndTransactionRequest,
) bool {
	return !txn.OrigTimestampWasObserved && args.NoRefreshSpans
}

const intentResolutionBatchSize = 500

// resolveLocalIntents synchronously resolves any intents that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing. Note that there is a maximum intent resolution
// allowance of intentResolutionBatchSize meant to avoid creating a
// batch which is too large for Raft. Any local intents which exceed
// the allowance are treated as external and are resolved
// asynchronously with the external intents.
func resolveLocalIntents(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args *roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
	evalCtx EvalContext,
) ([]roachpb.Span, error) {
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		desc = &mergeTrigger.LeftDesc
	}

	iter := batch.NewIterator(engine.IterOptions{
		UpperBound: desc.EndKey.AsRawKey(),
	})
	iterAndBuf := engine.GetBufUsingIter(iter)
	defer iterAndBuf.Cleanup()

	var externalIntents []roachpb.Span
	var resolveAllowance int64 = intentResolutionBatchSize
	if args.InternalCommitTrigger != nil {
		// If this is a system transaction (such as a split or merge), don't enforce the resolve allowance.
		// These transactions rely on having their intents resolved synchronously.
		resolveAllowance = math.MaxInt64
	}
	for _, span := range args.IntentSpans {
		if err := func() error {
			if resolveAllowance == 0 {
				externalIntents = append(externalIntents, span)
				return nil
			}
			intent := roachpb.Intent{Span: span, Txn: txn.TxnMeta, Status: txn.Status}
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !storagebase.ContainsKey(*desc, span.Key) {
					externalIntents = append(externalIntents, span)
					return nil
				}
				resolveMS := ms
				resolveAllowance--
				return engine.MVCCResolveWriteIntentUsingIter(ctx, batch, iterAndBuf, resolveMS, intent)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := storagebase.IntersectSpan(span, *desc)
			externalIntents = append(externalIntents, outSpans...)
			if inSpan != nil {
				intent.Span = *inSpan
				num, resumeSpan, err := engine.MVCCResolveWriteIntentRangeUsingIter(ctx, batch, iterAndBuf, ms, intent, resolveAllowance)
				if err != nil {
					return err
				}
				if evalCtx.EvalKnobs().NumKeysEvaluatedForRangeIntentResolution != nil {
					atomic.AddInt64(evalCtx.EvalKnobs().NumKeysEvaluatedForRangeIntentResolution, num)
				}
				resolveAllowance -= num
				if resumeSpan != nil {
					if resolveAllowance != 0 {
						log.Fatalf(ctx, "expected resolve allowance to be exactly 0 resolving %s; got %d", intent.Span, resolveAllowance)
					}
					externalIntents = append(externalIntents, *resumeSpan)
				}
				return nil
			}
			return nil
		}(); err != nil {
			return nil, errors.Wrapf(err, "resolving intent at %s on end transaction [%s]", span, txn.Status)
		}
	}
	// If the poison arg is set, make sure to set the abort span entry.
	if args.Poison && txn.Status == roachpb.ABORTED {
		if err := SetAbortSpan(ctx, evalCtx, batch, ms, txn.TxnMeta, true /* poison */); err != nil {
			return nil, err
		}
	}

	return externalIntents, nil
}

// updateStagingTxn persists the STAGING transaction record with updated status
// (and possibly timestamp). It persists the record with the EndTransaction
// request's declared in-flight writes along with all of the transaction's
// (local and remote) intents.
func updateStagingTxn(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	key []byte,
	args *roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
) error {
	txn.IntentSpans = args.IntentSpans
	txn.InFlightWrites = args.InFlightWrites
	txnRecord := txn.AsRecord()
	return engine.MVCCPutProto(ctx, batch, ms, key, hlc.Timestamp{}, nil /* txn */, &txnRecord)
}

// updateFinalizedTxn persists the COMMITTED or ABORTED transaction record with
// updated status (and possibly timestamp). If we've already resolved all
// intents locally, we actually delete the record right away - no use in keeping
// it around.
func updateFinalizedTxn(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	key []byte,
	args *roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
	externalIntents []roachpb.Span,
) error {
	if TxnAutoGC && len(externalIntents) == 0 {
		if log.V(2) {
			log.Infof(ctx, "auto-gc'ed %s (%d intents)", txn.Short(), len(args.IntentSpans))
		}
		return engine.MVCCDelete(ctx, batch, ms, key, hlc.Timestamp{}, nil /* txn */)
	}
	txn.IntentSpans = externalIntents
	txn.InFlightWrites = nil
	txnRecord := txn.AsRecord()
	return engine.MVCCPutProto(ctx, batch, ms, key, hlc.Timestamp{}, nil /* txn */, &txnRecord)
}

// RunCommitTrigger runs the commit trigger from an end transaction request.
func RunCommitTrigger(
	ctx context.Context,
	rec EvalContext,
	batch engine.Batch,
	ms *enginepb.MVCCStats,
	args *roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
) (result.Result, error) {
	ct := args.InternalCommitTrigger
	if ct == nil {
		return result.Result{}, nil
	}

	if ct.GetSplitTrigger() != nil {
		newMS, trigger, err := splitTrigger(
			ctx, rec, batch, *ms, ct.SplitTrigger, txn.Timestamp,
		)
		*ms = newMS
		return trigger, err
	}
	if crt := ct.GetChangeReplicasTrigger(); crt != nil {
		// TODO(tbg): once we support atomic replication changes, check that
		// crt.Added() and crt.Removed() don't intersect (including mentioning
		// the same replica more than once individually) because it would be
		// silly (though possible) to have to attach semantics to that.
		return changeReplicasTrigger(ctx, rec, batch, crt), nil
	}
	if ct.GetModifiedSpanTrigger() != nil {
		var pd result.Result
		if ct.ModifiedSpanTrigger.SystemConfigSpan {
			// Check if we need to gossip the system config.
			// NOTE: System config gossiping can only execute correctly if
			// the transaction record is located on the range that contains
			// the system span. If a transaction is created which modifies
			// both system *and* non-system data, it should be ensured that
			// the transaction record itself is on the system span. This can
			// be done by making sure a system key is the first key touched
			// in the transaction.
			if rec.ContainsKey(keys.SystemConfigSpan.Key) {
				if err := pd.MergeAndDestroy(
					result.Result{
						Local: result.LocalResult{
							MaybeGossipSystemConfig: true,
						},
					},
				); err != nil {
					return result.Result{}, err
				}
			} else {
				log.Errorf(ctx, "System configuration span was modified, but the "+
					"modification trigger is executing on a non-system range. "+
					"Configuration changes will not be gossiped.")
			}
		}
		if nlSpan := ct.ModifiedSpanTrigger.NodeLivenessSpan; nlSpan != nil {
			if err := pd.MergeAndDestroy(
				result.Result{
					Local: result.LocalResult{
						MaybeGossipNodeLiveness: nlSpan,
					},
				},
			); err != nil {
				return result.Result{}, err
			}
		}
		return pd, nil
	}
	if ct.GetMergeTrigger() != nil {
		// Merge triggers were handled earlier, before intent resolution.
		return result.Result{}, nil
	}
	if sbt := ct.GetStickyBitTrigger(); sbt != nil {
		newDesc := *rec.Desc()
		if sbt.StickyBit != (hlc.Timestamp{}) {
			newDesc.StickyBit = &sbt.StickyBit
		} else {
			newDesc.StickyBit = nil
		}
		var res result.Result
		res.Replicated.State = &storagepb.ReplicaState{
			Desc: &newDesc,
		}
		return res, nil
	}

	log.Fatalf(ctx, "unknown commit trigger: %+v", ct)
	return result.Result{}, nil
}

// splitTrigger is called on a successful commit of a transaction
// containing an AdminSplit operation. It copies the AbortSpan for
// the new range and recomputes stats for both the existing, left hand
// side (LHS) range and the right hand side (RHS) range. For
// performance it only computes the stats for the original range (the
// left hand side) and infers the RHS stats by subtracting from the
// original stats. We compute the LHS stats because the split key
// computation ensures that we do not create large LHS
// ranges. However, this optimization is only possible if the stats
// are fully accurate. If they contain estimates, stats for both the
// LHS and RHS are computed.
//
// Splits are complicated. A split is initiated when a replica receives an
// AdminSplit request. Note that this request (and other "admin" requests)
// differs from normal requests in that it doesn't go through Raft but instead
// allows the lease holder Replica to act as the orchestrator for the
// distributed transaction that performs the split. As such, this request is
// only executed on the lease holder replica and the request is redirected to
// the lease holder if the recipient is a follower.
//
// Splits do not require the lease for correctness (which is good, because we
// only check that the lease is held at the beginning of the operation, and
// have no way to ensure that it is continually held until the end). Followers
// could perform splits too, and the only downside would be that if two splits
// were attempted concurrently (or a split and a ChangeReplicas), one would
// fail. The lease is used to designate one replica for this role and avoid
// wasting time on splits that may fail.
//
// The processing of splits is divided into two phases. The first phase occurs
// in Replica.AdminSplit. In that phase, the split-point is computed, and a
// transaction is started which updates both the LHS and RHS range descriptors
// and the meta range addressing information. (If we're splitting a meta2 range
// we'll be updating the meta1 addressing, otherwise we'll be updating the
// meta2 addressing). That transaction includes a special SplitTrigger flag on
// the EndTransaction request. Like all transactions, the requests within the
// transaction are replicated via Raft, including the EndTransaction request.
//
// The second phase of split processing occurs when each replica for the range
// encounters the SplitTrigger. Processing of the SplitTrigger happens below,
// in Replica.splitTrigger. The processing of the SplitTrigger occurs in two
// stages. The first stage operates within the context of an engine.Batch and
// updates all of the on-disk state for the old and new ranges atomically. The
// second stage is invoked when the batch commits and updates the in-memory
// state, creating the new replica in memory and populating its timestamp cache
// and registering it with the store.
//
// There is lots of subtlety here. The easy scenario is that all of the
// replicas process the SplitTrigger before processing any Raft message for RHS
// (right hand side) of the newly split range. Something like:
//
//         Node A             Node B             Node C
//     ----------------------------------------------------
// range 1   |                  |                  |
//           |                  |                  |
//      SplitTrigger            |                  |
//           |             SplitTrigger            |
//           |                  |             SplitTrigger
//           |                  |                  |
//     ----------------------------------------------------
// split finished on A, B and C |                  |
//           |                  |                  |
// range 2   |                  |                  |
//           | ---- MsgVote --> |                  |
//           | ---------------------- MsgVote ---> |
//
// But that ideal ordering is not guaranteed. The split is "finished" when two
// of the replicas have appended the end-txn request containing the
// SplitTrigger to their Raft log. The following scenario is possible:
//
//         Node A             Node B             Node C
//     ----------------------------------------------------
// range 1   |                  |                  |
//           |                  |                  |
//      SplitTrigger            |                  |
//           |             SplitTrigger            |
//           |                  |                  |
//     ----------------------------------------------------
// split finished on A and B    |                  |
//           |                  |                  |
// range 2   |                  |                  |
//           | ---- MsgVote --> |                  |
//           | --------------------- MsgVote ---> ???
//           |                  |                  |
//           |                  |             SplitTrigger
//
// In this scenario, C will create range 2 upon reception of the MsgVote from
// A, though locally that span of keys is still part of range 1. This is
// possible because at the Raft level ranges are identified by integer IDs and
// it isn't until C receives a snapshot of range 2 from the leader that it
// discovers the span of keys it covers. In order to prevent C from fully
// initializing range 2 in this instance, we prohibit applying a snapshot to a
// range if the snapshot overlaps another range. See Store.canApplySnapshotLocked.
//
// But while a snapshot may not have been applied at C, an uninitialized
// Replica was created. An uninitialized Replica is one which belongs to a Raft
// group but for which the range descriptor has not been received. This Replica
// will have participated in the Raft elections. When we're creating the new
// Replica below we take control of this uninitialized Replica and stop it from
// responding to Raft messages by marking it "destroyed". Note that we use the
// Replica.mu.destroyed field for this, but we don't do everything that
// Replica.Destroy does (so we should probably rename that field in light of
// its new uses). In particular we don't touch any data on disk or leave a
// tombstone. This is especially important because leaving a tombstone would
// prevent the legitimate recreation of this replica.
//
// There is subtle synchronization here that is currently controlled by the
// Store.processRaft goroutine. In particular, the serial execution of
// Replica.handleRaftReady by Store.processRaft ensures that an uninitialized
// RHS won't be concurrently executing in Replica.handleRaftReady because we're
// currently running on that goroutine (i.e. Replica.splitTrigger is called on
// the processRaft goroutine).
//
// TODO(peter): The above synchronization needs to be fixed. Using a single
// goroutine for executing Replica.handleRaftReady is undesirable from a
// performance perspective. Likely we will have to add a mutex to Replica to
// protect handleRaftReady and to grab that mutex below when marking the
// uninitialized Replica as "destroyed". Hopefully we'll also be able to remove
// Store.processRaftMu.
//
// Note that in this more complex scenario, A (which performed the SplitTrigger
// first) will create the associated Raft group for range 2 and start
// campaigning immediately. It is possible for B to receive MsgVote requests
// before it has applied the SplitTrigger as well. Both B and C will vote for A
// (and preserve the records of that vote in their HardState). It is critically
// important for Raft correctness that we do not lose the records of these
// votes. After electing A the Raft leader for range 2, A will then attempt to
// send a snapshot to B and C and we'll fall into the situation above where a
// snapshot is received for a range before it has finished splitting from its
// sibling and is thus rejected. An interesting subtlety here: A will send a
// snapshot to B and C because when range 2 is initialized we were careful set
// synthesize its HardState to set its Raft log index to 10. If we had instead
// used log index 0, Raft would have believed the group to be empty, but the
// RHS has something. Using a non-zero initial log index causes Raft to believe
// that there is a discarded prefix to the log and will thus send a snapshot to
// followers.
//
// A final point of clarification: when we split a range we're splitting the
// data the range contains. But we're not forking or splitting the associated
// Raft group. Instead, we're creating a new Raft group to control the RHS of
// the split. That Raft group is starting from an empty Raft log (positioned at
// log entry 10) and a snapshot of the RHS of the split range.
//
// After the split trigger returns, the on-disk state of the right-hand side
// will be suitable for instantiating the right hand side Replica, and
// a suitable trigger is returned, along with the updated stats which represent
// the LHS delta caused by the split (i.e. all writes in the current batch
// which went to the left-hand side, minus the kv pairs which moved to the
// RHS).
//
// These stats are suitable for returning up the callstack like those for
// regular commands; the corresponding delta for the RHS is part of the
// returned trigger and is handled by the Store.
func splitTrigger(
	ctx context.Context,
	rec EvalContext,
	batch engine.Batch,
	bothDeltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	ts hlc.Timestamp,
) (enginepb.MVCCStats, result.Result, error) {
	// TODO(andrei): should this span be a child of the ctx's (if any)?
	sp := rec.ClusterSettings().Tracer.StartRootSpan(
		"split", logtags.FromContext(ctx), tracing.NonRecordableSpan,
	)
	defer sp.Finish()
	desc := rec.Desc()
	if !bytes.Equal(desc.StartKey, split.LeftDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.RightDesc.EndKey) {
		return enginepb.MVCCStats{}, result.Result{}, errors.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.LeftDesc.StartKey, split.LeftDesc.EndKey,
			split.RightDesc.StartKey, split.RightDesc.EndKey, desc)
	}

	// Compute the absolute stats for the (post-split) LHS. No more
	// modifications to it are allowed after this line.

	leftMS, err := rditer.ComputeStatsForRange(&split.LeftDesc, batch, ts.WallTime)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to compute stats for LHS range after split")
	}
	log.Event(ctx, "computed stats for left hand side range")

	h := splitStatsHelperInput{
		AbsPreSplitBothEstimated: rec.GetMVCCStats(),
		DeltaBatchEstimated:      bothDeltaMS,
		AbsPostSplitLeft:         leftMS,
		AbsPostSplitRightFn: func() (enginepb.MVCCStats, error) {
			rightMS, err := rditer.ComputeStatsForRange(
				&split.RightDesc, batch, ts.WallTime,
			)
			return rightMS, errors.Wrap(
				err,
				"unable to compute stats for RHS range after split",
			)
		},
	}
	return splitTriggerHelper(ctx, rec, batch, h, split, ts)
}

// splitTriggerHelper continues the work begun by splitTrigger, but has a
// reduced scope that has all stats-related concerns bundled into a
// splitStatsHelper.
func splitTriggerHelper(
	ctx context.Context,
	rec EvalContext,
	batch engine.Batch,
	statsInput splitStatsHelperInput,
	split *roachpb.SplitTrigger,
	ts hlc.Timestamp,
) (enginepb.MVCCStats, result.Result, error) {
	// TODO(d4l3k): we should check which side of the split is smaller
	// and compute stats for it instead of having a constraint that the
	// left hand side is smaller.

	// NB: the replicated post-split left hand keyspace is frozen at this point.
	// Only the RHS can be mutated (and we do so to seed its state).

	// Copy the last replica GC timestamp. This value is unreplicated,
	// which is why the MVCC stats are set to nil on calls to
	// MVCCPutProto.
	replicaGCTS, err := rec.GetLastReplicaGCTimestamp(ctx)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to fetch last replica GC timestamp")
	}
	if err := engine.MVCCPutProto(ctx, batch, nil, keys.RangeLastReplicaGCTimestampKey(split.RightDesc.RangeID), hlc.Timestamp{}, nil, &replicaGCTS); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to copy last replica GC timestamp")
	}

	h, err := makeSplitStatsHelper(statsInput)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, err
	}

	// Initialize the RHS range's AbortSpan by copying the LHS's.
	if err := rec.AbortSpan().CopyTo(
		ctx, batch, batch, h.AbsPostSplitRight(), ts, split.RightDesc.RangeID,
	); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, err
	}

	// Note: we don't copy the queue last processed times. This means
	// we'll process the RHS range in consistency and time series
	// maintenance queues again possibly sooner than if we copied. The
	// intent is to limit post-raft logic.

	// Now that we've computed the stats for the RHS so far, we persist them.
	// This looks a bit more complicated than it really is: updating the stats
	// also changes the stats, and we write not only the stats but a complete
	// initial state. Additionally, since bothDeltaMS is tracking writes to
	// both sides, we need to update it as well.
	{
		// Various pieces of code rely on a replica's lease never being unitialized,
		// but it's more than that - it ensures that we properly initialize the
		// timestamp cache, which is only populated on the lease holder, from that
		// of the original Range.  We found out about a regression here the hard way
		// in #7899. Prior to this block, the following could happen:
		// - a client reads key 'd', leaving an entry in the timestamp cache on the
		//   lease holder of [a,e) at the time, node one.
		// - the range [a,e) splits at key 'c'. [c,e) starts out without a lease.
		// - the replicas of [a,e) on nodes one and two both process the split
		//   trigger and thus copy their timestamp caches to the new right-hand side
		//   Replica. However, only node one's timestamp cache contains information
		//   about the read of key 'd' in the first place.
		// - node two becomes the lease holder for [c,e). Its timestamp cache does
		//   not know about the read at 'd' which happened at the beginning.
		// - node two can illegally propose a write to 'd' at a lower timestamp.
		//
		// TODO(tschottdorf): why would this use r.store.Engine() and not the
		// batch?
		leftLease, err := MakeStateLoader(rec).LoadLease(ctx, rec.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to load lease")
		}
		if (leftLease == roachpb.Lease{}) {
			log.Fatalf(ctx, "LHS of split has no lease")
		}

		replica, found := split.RightDesc.GetReplicaDescriptor(leftLease.Replica.StoreID)
		if !found {
			return enginepb.MVCCStats{}, result.Result{}, errors.Errorf(
				"pre-split lease holder %+v not found in post-split descriptor %+v",
				leftLease.Replica, split.RightDesc,
			)
		}
		rightLease := leftLease
		rightLease.Replica = replica

		gcThreshold, err := MakeStateLoader(rec).LoadGCThreshold(ctx, rec.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to load GCThreshold")
		}
		if (*gcThreshold == hlc.Timestamp{}) {
			log.VEventf(ctx, 1, "LHS's GCThreshold of split is not set")
		}

		// We're about to write the initial state for the replica. We migrated
		// the formerly replicated truncated state into unreplicated keyspace
		// in 2.2., but this range may still be using the replicated version
		// and we need to make a decision about what to use for the RHS that
		// is consistent across the followers: do for the RHS what the LHS
		// does: if the LHS has the legacy key, initialize the RHS with a
		// legacy key as well.
		//
		// See VersionUnreplicatedRaftTruncatedState.
		truncStateType := stateloader.TruncatedStateUnreplicated
		if found, err := engine.MVCCGetProto(
			ctx,
			batch,
			keys.RaftTruncatedStateLegacyKey(rec.GetRangeID()),
			hlc.Timestamp{},
			nil,
			engine.MVCCGetOptions{},
		); err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to load legacy truncated state")
		} else if found {
			truncStateType = stateloader.TruncatedStateLegacyReplicated
		}

		// Writing the initial state is subtle since this also seeds the Raft
		// group. It becomes more subtle due to proposer-evaluated Raft.
		//
		// We are writing to the right hand side's Raft group state in this
		// batch so we need to synchronize with anything else that could be
		// touching that replica's Raft state. Specifically, we want to prohibit
		// an uninitialized Replica from receiving a message for the right hand
		// side range and performing raft processing. This is achieved by
		// serializing execution of uninitialized Replicas in Store.processRaft
		// and ensuring that no uninitialized Replica is being processed while
		// an initialized one (like the one currently being split) is being
		// processed.
		//
		// Since the right hand side of the split's Raft group may already
		// exist, we must be prepared to absorb an existing HardState. The Raft
		// group may already exist because other nodes could already have
		// processed the split and started talking to our node, prompting the
		// creation of a Raft group that can vote and bump its term, but not
		// much else: it can't receive snapshots because those intersect the
		// pre-split range; it can't apply log commands because it needs a
		// snapshot first.
		//
		// However, we can't absorb the right-hand side's HardState here because
		// we only *evaluate* the proposal here, but by the time it is
		// *applied*, the HardState could have changed. We do this downstream of
		// Raft, in splitPostApply, where we write the last index and the
		// HardState via a call to synthesizeRaftState. Here, we only call
		// writeInitialReplicaState which essentially writes a ReplicaState
		// only.

		*h.AbsPostSplitRight(), err = stateloader.WriteInitialReplicaState(
			ctx, batch, *h.AbsPostSplitRight(), split.RightDesc, rightLease,
			*gcThreshold, rec.ClusterSettings().Version.Version().Version, truncStateType,
		)
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to write initial Replica state")
		}
	}

	var pd result.Result
	// This makes sure that no reads are happening in parallel; see #3148.
	pd.Replicated.BlockReads = true
	pd.Replicated.Split = &storagepb.Split{
		SplitTrigger: *split,
		// NB: the RHSDelta is identical to the stats for the newly created right
		// hand side range (i.e. it goes from zero to its stats).
		RHSDelta: *h.AbsPostSplitRight(),
	}

	// HACK(tbg): ContainsEstimates isn't an additive group (there isn't a
	// -true), and instead of "-true" we'll emit a "true". This will all be
	// fixed when #37583 lands (and the version is active). For now hard-code
	// false and there's also code below Raft that interprets this (coming from
	// a split) as a signal to reset the ContainsEstimates field to false (see
	// applyRaftCommand).
	deltaPostSplitLeft := h.DeltaPostSplitLeft()
	deltaPostSplitLeft.ContainsEstimates = false
	return deltaPostSplitLeft, pd, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge transaction.
// It writes data from the right-hand range into the left-hand range and
// recomputes stats for the left-hand range.
func mergeTrigger(
	ctx context.Context,
	rec EvalContext,
	batch engine.Batch,
	ms *enginepb.MVCCStats,
	merge *roachpb.MergeTrigger,
	ts hlc.Timestamp,
) (result.Result, error) {
	desc := rec.Desc()
	if !bytes.Equal(desc.StartKey, merge.LeftDesc.StartKey) {
		return result.Result{}, errors.Errorf("LHS range start keys do not match: %s != %s",
			desc.StartKey, merge.LeftDesc.StartKey)
	}
	if !desc.EndKey.Less(merge.LeftDesc.EndKey) {
		return result.Result{}, errors.Errorf("original LHS end key is not less than the post merge end key: %s >= %s",
			desc.EndKey, merge.LeftDesc.EndKey)
	}

	if err := abortspan.New(merge.RightDesc.RangeID).CopyTo(
		ctx, batch, batch, ms, ts, merge.LeftDesc.RangeID,
	); err != nil {
		return result.Result{}, err
	}

	// The stats for the merged range are the sum of the LHS and RHS stats, less
	// the RHS's replicated range ID stats. The only replicated range ID keys we
	// copy from the RHS are the keys in the abort span, and we've already
	// accounted for those stats above.
	ms.Add(merge.RightMVCCStats)
	{
		ridPrefix := keys.MakeRangeIDReplicatedPrefix(merge.RightDesc.RangeID)
		iter := batch.NewIterator(engine.IterOptions{UpperBound: ridPrefix.PrefixEnd()})
		defer iter.Close()
		sysMS, err := iter.ComputeStats(
			engine.MakeMVCCMetadataKey(ridPrefix),
			engine.MakeMVCCMetadataKey(ridPrefix.PrefixEnd()),
			0 /* nowNanos */)
		if err != nil {
			return result.Result{}, err
		}
		ms.Subtract(sysMS)
	}

	var pd result.Result
	pd.Replicated.BlockReads = true
	pd.Replicated.Merge = &storagepb.Merge{
		MergeTrigger: *merge,
	}
	return pd, nil
}

func changeReplicasTrigger(
	ctx context.Context, rec EvalContext, batch engine.Batch, change *roachpb.ChangeReplicasTrigger,
) result.Result {
	var pd result.Result
	// After a successful replica addition or removal check to see if the
	// range needs to be split. Splitting usually takes precedence over
	// replication via configuration of the split and replicate queues, but
	// if the split occurs concurrently with the replicas change the split
	// can fail and won't retry until the next scanner cycle. Re-queuing
	// the replica here removes that latency.
	pd.Local.MaybeAddToSplitQueue = true

	// Gossip the first range whenever the range descriptor changes. We also
	// gossip the first range whenever the lease holder changes, but that might
	// not have occurred if a replica was being added or the non-lease-holder
	// replica was being removed. Note that we attempt the gossiping even from
	// the removed replica in case it was the lease-holder and it is still
	// holding the lease.
	pd.Local.GossipFirstRange = rec.IsFirstRange()

	var desc roachpb.RangeDescriptor
	if change.Desc != nil {
		// Trigger proposed by a 19.2+ node (and we're a 19.2+ node as well).
		desc = *change.Desc
	} else {
		// Trigger proposed by a 19.1 node. Reconstruct descriptor from deprecated
		// fields.
		desc = *rec.Desc()
		desc.SetReplicas(roachpb.MakeReplicaDescriptors(change.DeprecatedUpdatedReplicas))
		desc.NextReplicaID = change.DeprecatedNextReplicaID
	}

	pd.Replicated.State = &storagepb.ReplicaState{
		Desc: &desc,
	}
	pd.Replicated.ChangeReplicas = &storagepb.ChangeReplicas{
		ChangeReplicasTrigger: *change,
	}

	return pd
}
