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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// evaluateCommand delegates to the eval method for the given
// roachpb.Request. The returned Result may be partially valid
// even if an error is returned. maxKeys is the number of scan results
// remaining for this batch (MaxInt64 for no limit).
func evaluateCommand(
	ctx context.Context,
	raftCmdID storagebase.CmdIDKey,
	index int,
	batch engine.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.Request,
	reply roachpb.Response,
) (result.Result, *roachpb.Error) {

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return result.Result{}, nil
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := rec.EvalKnobs().TestingEvalFilter; filter != nil {
		filterArgs := storagebase.FilterArgs{
			Ctx:   ctx,
			CmdID: raftCmdID,
			Index: index,
			Sid:   rec.StoreID(),
			Req:   args,
			Hdr:   h,
		}
		if pErr := filter(filterArgs); pErr != nil {
			log.Infof(ctx, "test injecting error: %s", pErr)
			return result.Result{}, pErr
		}
	}

	var err error
	var pd result.Result

	if cmd, ok := batcheval.LookupCommand(args.Method()); ok {
		cArgs := batcheval.CommandArgs{
			EvalCtx: rec,
			Header:  h,
			// Some commands mutate their arguments, so give each invocation
			// its own copy (shallow to mimic earlier versions of this code
			// in which args were passed by value instead of pointer).
			Args:    args.ShallowCopy(),
			MaxKeys: maxKeys,
			Stats:   ms,
		}
		pd, err = cmd.Eval(ctx, batch, cArgs, reply)
	} else {
		err = errors.Errorf("unrecognized command %s", args.Method())
	}

	if h.ReturnRangeInfo {
		header := reply.Header()
		lease, _ := rec.GetLease()
		desc := rec.Desc()
		header.RangeInfos = []roachpb.RangeInfo{
			{
				Desc:  *desc,
				Lease: lease,
			},
		}
		reply.SetHeader(header)
	}

	// TODO(peter): We'd like to assert that the hlc clock is always updated
	// correctly, but various tests insert versioned data without going through
	// the proper channels. See TestPushTxnUpgradeExistingTxn for an example.
	//
	// if header.Txn != nil && !header.Txn.Timestamp.Less(h.Timestamp) {
	// 	if now := r.store.Clock().Now(); now.Less(header.Txn.Timestamp) {
	// 		log.Fatalf(ctx, "hlc clock not updated: %s < %s", now, header.Txn.Timestamp)
	// 	}
	// }

	if log.V(2) {
		log.Infof(ctx, "evaluated %s command %+v: %+v, err=%v", args.Method(), args, reply, err)
	}

	// Create a roachpb.Error by initializing txn from the request/response header.
	var pErr *roachpb.Error
	if err != nil {
		txn := reply.Header().Txn
		if txn == nil {
			txn = h.Txn
		}
		pErr = roachpb.NewErrorWithTxn(err, txn)
	}

	return pd, pErr
}

func init() {
	// TODO(tschottdorf): move EndTransaction into batcheval. In doing so,
	// unexport DeclareKeysWriteTransaction.
	batcheval.RegisterCommand(roachpb.EndTransaction, declareKeysEndTransaction, evalEndTransaction)
}

func declareKeysEndTransaction(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	batcheval.DeclareKeysWriteTransaction(desc, header, req, spans)
	et := req.(*roachpb.EndTransactionRequest)
	// The spans may extend beyond this Range, but it's ok for the
	// purpose of the command queue. The parts in our Range will
	// be resolved eagerly.
	for _, span := range et.IntentSpans {
		spans.Add(spanset.SpanReadWrite, span)
	}
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID)})
	}

	// All transactions depend on the range descriptor because they need
	// to determine which intents are within the local range.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})

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
			// Merges write to the left side and delete and read from the right.
			leftRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(header.RangeID)
			spans.Add(spanset.SpanReadWrite, roachpb.Span{
				Key:    leftRangeIDPrefix,
				EndKey: leftRangeIDPrefix.PrefixEnd(),
			})

			rightRangeIDPrefix := keys.MakeRangeIDPrefix(mt.RightDesc.RangeID)
			spans.Add(spanset.SpanReadWrite, roachpb.Span{
				Key:    rightRangeIDPrefix,
				EndKey: rightRangeIDPrefix.PrefixEnd(),
			})
			spans.Add(spanset.SpanReadOnly, roachpb.Span{
				Key:    keys.MakeRangeKeyPrefix(mt.RightDesc.StartKey),
				EndKey: keys.MakeRangeKeyPrefix(mt.RightDesc.EndKey).PrefixEnd(),
			})

		}
	}
}

// evalEndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter. Rolling back
// an already rolled-back txn is ok.
func evalEndTransaction(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.EndTransactionRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	reply := resp.(*roachpb.EndTransactionResponse)

	if err := batcheval.VerifyTransaction(h, args); err != nil {
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
		ctx, batch, key, hlc.Timestamp{}, true, nil, &existingTxn,
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		return result.Result{}, roachpb.NewTransactionNotFoundStatusError()
	}
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
		return result.Result{}, roachpb.NewTransactionStatusError("already committed")

	case roachpb.ABORTED:
		if !args.Commit {
			// The transaction has already been aborted by other.
			// Do not return TransactionAbortedError since the client anyway
			// wanted to abort the transaction.
			desc := cArgs.EvalCtx.Desc()
			externalIntents, err := resolveLocalIntents(ctx, desc, batch, ms, *args, reply.Txn, cArgs.EvalCtx)
			if err != nil {
				return result.Result{}, err
			}
			if err := updateTxnWithExternalIntents(
				ctx, batch, ms, *args, reply.Txn, externalIntents,
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
		reply.Txn.Intents = args.IntentSpans
		return result.FromEndTxn(reply.Txn, true /* alwaysReturn */, args.Poison), roachpb.NewTransactionAbortedError()

	case roachpb.PENDING:
		if h.Txn.Epoch < reply.Txn.Epoch {
			// TODO(tschottdorf): this leaves the Txn record (and more
			// importantly, intents) dangling; we can't currently write on
			// error. Would panic, but that makes TestEndTransactionWithErrors
			// awkward.
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("epoch regression: %d", h.Txn.Epoch),
			)
		} else if h.Txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(h.Txn.OrigTimestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the original transaction timestamp.

			// TODO(tschottdorf): see above comment on epoch regression.
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("timestamp regression: %s", h.Txn.OrigTimestamp),
			)
		}

	default:
		return result.Result{}, roachpb.NewTransactionStatusError(
			fmt.Sprintf("bad txn status: %s", reply.Txn),
		)
	}

	// Update the existing txn with the supplied txn.
	reply.Txn.Update(h.Txn)

	// Set transaction status to COMMITTED or ABORTED as per the
	// args.Commit parameter.
	if args.Commit {
		if retry, reason := isEndTransactionTriggeringRetryError(reply.Txn, *args); retry {
			return result.Result{}, roachpb.NewTransactionRetryError(reason)
		}

		if isEndTransactionExceedingDeadline(reply.Txn.Timestamp, *args) {
			// If the deadline has lapsed return an error and rely on the client
			// issuing a Rollback() that aborts the transaction and cleans up
			// intents. Unfortunately, we're returning an error and unable to
			// write on error (see #1989): we can't write ABORTED into the master
			// transaction record which remains PENDING, and thus rely on the
			// client to issue a Rollback() for cleanup.
			//
			// N.B. This deadline test is expected to be a Noop for Serializable
			// transactions; unless the client misconfigured the txn, the deadline can
			// only be expired if the txn has been pushed, and pushed Serializable
			// transactions are detected above.
			return result.Result{}, roachpb.NewTransactionStatusError(
				"transaction deadline exceeded")
		}

		reply.Txn.Status = roachpb.COMMITTED
	} else {
		reply.Txn.Status = roachpb.ABORTED
	}

	desc := cArgs.EvalCtx.Desc()
	externalIntents, err := resolveLocalIntents(ctx, desc, batch, ms, *args, reply.Txn, cArgs.EvalCtx)
	if err != nil {
		return result.Result{}, err
	}
	if err := updateTxnWithExternalIntents(ctx, batch, ms, *args, reply.Txn, externalIntents); err != nil {
		return result.Result{}, err
	}

	// Run triggers if successfully committed.
	var pd result.Result
	if reply.Txn.Status == roachpb.COMMITTED {
		var err error
		if pd, err = runCommitTrigger(ctx, cArgs.EvalCtx, batch.(engine.Batch), ms, *args, reply.Txn); err != nil {
			return result.Result{}, NewReplicaCorruptionError(err)
		}
	}

	// Note: there's no need to clear the AbortSpan state if we've
	// successfully finalized a transaction, as there's no way in which an abort
	// cache entry could have been written (the txn would already have been in
	// state=ABORTED).
	//
	// Summary of transaction replay protection after EndTransaction: When a
	// transactional write gets replayed over its own resolved intents, the
	// write will succeed but only as an intent with a newer timestamp (with a
	// WriteTooOldError). However, the replayed intent cannot be resolved by a
	// subsequent replay of this EndTransaction call because the txn timestamp
	// will be too old. Replays which include a BeginTransaction never succeed
	// because EndTransaction inserts in the write timestamp cache, forcing the
	// BeginTransaction to fail with a transaction retry error. If the replay
	// didn't include a BeginTransaction, any push will immediately succeed as a
	// missing txn record on push sets the transaction to aborted. In both
	// cases, the txn will be GC'd on the slow path.
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

// isEndTransactionExceedingDeadline returns true if the transaction
// exceeded its deadline.
func isEndTransactionExceedingDeadline(t hlc.Timestamp, args roachpb.EndTransactionRequest) bool {
	return args.Deadline != nil && args.Deadline.Less(t)
}

// isEndTransactionTriggeringRetryError returns true if the
// EndTransactionRequest cannot be committed and needs to return a
// TransactionRetryError.
func isEndTransactionTriggeringRetryError(
	txn *roachpb.Transaction, args roachpb.EndTransactionRequest,
) (retry bool, reason roachpb.TransactionRetryReason) {
	// If we saw any WriteTooOldErrors, we must restart to avoid lost
	// update anomalies.
	if txn.WriteTooOld {
		retry, reason = true, roachpb.RETRY_WRITE_TOO_OLD
	} else {
		origTimestamp := txn.OrigTimestamp
		origTimestamp.Forward(txn.RefreshedTimestamp)
		isTxnPushed := txn.Timestamp != origTimestamp

		// If the isolation level is SERIALIZABLE, return a transaction
		// retry error if the commit timestamp isn't equal to the txn
		// timestamp.
		if isTxnPushed {
			if txn.Isolation == enginepb.SERIALIZABLE {
				retry, reason = true, roachpb.RETRY_SERIALIZABLE
			} else if txn.RetryOnPush {
				// If pushing requires a retry and the transaction was pushed, retry.
				retry, reason = true, roachpb.RETRY_DELETE_RANGE
			}
		}
	}

	// A serializable transaction can still avoid a retry under certain conditions.
	if retry && txn.IsSerializable() && canForwardSerializableTimestamp(txn, args.NoRefreshSpans) {
		retry, reason = false, 0
	}

	return retry, reason
}

// canForwardSerializableTimestamp returns whether a serializable txn can
// be safely committed with a forwarded timestamp. This requires that
// the transaction's timestamp has not leaked and that the transaction
// has encountered no spans which require refreshing at the forwarded
// timestamp. If either of those conditions are true, a cient-side
// retry is required.
func canForwardSerializableTimestamp(txn *roachpb.Transaction, noRefreshSpans bool) bool {
	return !txn.OrigTimestampWasObserved && noRefreshSpans
}

// resolveLocalIntents synchronously resolves any intents that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing. Note that there is a maximum intent resolution
// allowance of intentResolverBatchSize meant to avoid creating a
// batch which is too large for Raft. Any local intents which exceed
// the allowance are treated as external and are resolved
// asynchronously with the external intents.
func resolveLocalIntents(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
	evalCtx batcheval.EvalContext,
) ([]roachpb.Span, error) {
	var preMergeDesc *roachpb.RangeDescriptor
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		preMergeDesc = desc
		desc = &mergeTrigger.LeftDesc
	}

	min, max := txn.InclusiveTimeBounds()
	iter := batch.NewTimeBoundIterator(min, max)
	iterAndBuf := engine.GetBufUsingIter(iter)
	defer iterAndBuf.Cleanup()

	var externalIntents []roachpb.Span
	var resolveAllowance int64 = intentResolverBatchSize
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
				if !containsKey(*desc, span.Key) {
					externalIntents = append(externalIntents, span)
					return nil
				}
				resolveMS := ms
				if preMergeDesc != nil && !containsKey(*preMergeDesc, span.Key) {
					// If this transaction included a merge and the intents
					// are from the subsumed range, ignore the intent resolution
					// stats, as they will already be accounted for during the
					// merge trigger.
					resolveMS = nil
				}
				resolveAllowance--
				return engine.MVCCResolveWriteIntentUsingIter(ctx, batch, iterAndBuf, resolveMS, intent)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
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
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving intent at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	// If the poison arg is set, make sure to set the abort span entry.
	if args.Poison && txn.Status == roachpb.ABORTED {
		if err := batcheval.SetAbortSpan(ctx, evalCtx, batch, ms, txn.TxnMeta, true /* poison */); err != nil {
			return nil, err
		}
	}

	return externalIntents, nil
}

// updateTxnWithExternalIntents persists the transaction record with
// updated status (& possibly timestamp). If we've already resolved
// all intents locally, we actually delete the record right away - no
// use in keeping it around.
func updateTxnWithExternalIntents(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
	externalIntents []roachpb.Span,
) error {
	key := keys.TransactionKey(txn.Key, txn.ID)
	if txnAutoGC && len(externalIntents) == 0 {
		if log.V(2) {
			log.Infof(ctx, "auto-gc'ed %s (%d intents)", txn.Short(), len(args.IntentSpans))
		}
		return engine.MVCCDelete(ctx, batch, ms, key, hlc.Timestamp{}, nil /* txn */)
	}
	txn.Intents = externalIntents
	return engine.MVCCPutProto(ctx, batch, ms, key, hlc.Timestamp{}, nil /* txn */, txn)
}

// intersectSpan takes an intent and a descriptor. It then splits the
// intent's range into up to three pieces: A first piece which is contained in
// the Range, and a slice of up to two further intents which are outside of the
// key range. An intent for which [Key, EndKey) is empty does not result in any
// intents; thus intersectIntent only applies to intent ranges.
// A range-local intent range is never split: It's returned as either
// belonging to or outside of the descriptor's key range, and passing an intent
// which begins range-local but ends non-local results in a panic.
// TODO(tschottdorf): move to proto, make more gen-purpose - kv.truncate does
// some similar things.
func intersectSpan(
	span roachpb.Span, desc roachpb.RangeDescriptor,
) (middle *roachpb.Span, outside []roachpb.Span) {
	start, end := desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey()
	if len(span.EndKey) == 0 {
		outside = append(outside, span)
		return
	}
	if bytes.Compare(span.Key, keys.LocalRangeMax) < 0 {
		if bytes.Compare(span.EndKey, keys.LocalRangeMax) >= 0 {
			panic(fmt.Sprintf("a local intent range may not have a non-local portion: %s", span))
		}
		if containsKeyRange(desc, span.Key, span.EndKey) {
			return &span, nil
		}
		return nil, append(outside, span)
	}
	// From now on, we're dealing with plain old key ranges - no more local
	// addressing.
	if bytes.Compare(span.Key, start) < 0 {
		// Intent spans a part to the left of [start, end).
		iCopy := span
		if bytes.Compare(start, span.EndKey) < 0 {
			iCopy.EndKey = start
		}
		span.Key = iCopy.EndKey
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(end, span.EndKey) < 0 {
		// Intent spans a part to the right of [start, end).
		iCopy := span
		if bytes.Compare(iCopy.Key, end) < 0 {
			iCopy.Key = end
		}
		span.EndKey = iCopy.Key
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(span.Key, start) >= 0 && bytes.Compare(end, span.EndKey) >= 0 {
		middle = &span
	}
	return
}

func runCommitTrigger(
	ctx context.Context,
	rec batcheval.EvalContext,
	batch engine.Batch,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
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
	if ct.GetMergeTrigger() != nil {
		return mergeTrigger(ctx, rec, batch, ms, ct.MergeTrigger, txn.Timestamp)
	}
	if crt := ct.GetChangeReplicasTrigger(); crt != nil {
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
	log.Fatalf(ctx, "unknown commit trigger: %+v", ct)
	return result.Result{}, nil
}

// AdminSplit divides the range into into two ranges using args.SplitKey.
func (r *Replica) AdminSplit(
	ctx context.Context, args roachpb.AdminSplitRequest,
) (reply roachpb.AdminSplitResponse, pErr *roachpb.Error) {
	if len(args.SplitKey) == 0 {
		return roachpb.AdminSplitResponse{}, roachpb.NewErrorf("cannot split range with no key provided")
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 10
	for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
		// Admin commands always require the range lease to begin (see
		// executeAdminBatch), but we may have lost it while in this retry loop.
		// Without the lease, a replica's local descriptor can be arbitrarily
		// stale, which will result in a ConditionFailedError. To avoid this,
		// we make sure that we still have the lease before each attempt.
		if _, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return roachpb.AdminSplitResponse{}, pErr
		}

		reply, _, pErr = r.adminSplitWithDescriptor(ctx, args, r.Desc())
		// On seeing a ConditionFailedError or an AmbiguousResultError, retry the
		// command with the updated descriptor.
		switch pErr.GetDetail().(type) {
		case *roachpb.ConditionFailedError:
		case *roachpb.AmbiguousResultError:
		default:
			return reply, pErr
		}
	}
	// If we broke out of the loop after MaxRetries, return the last error.
	return roachpb.AdminSplitResponse{}, pErr
}

func maybeDescriptorChangedError(desc *roachpb.RangeDescriptor, err error) (string, bool) {
	if detail, ok := err.(*roachpb.ConditionFailedError); ok {
		// Provide a better message in the common case that the range being changed
		// was already changed by a concurrent transaction.
		var actualDesc roachpb.RangeDescriptor
		if err := detail.ActualValue.GetProto(&actualDesc); err == nil {
			if desc.RangeID == actualDesc.RangeID && !desc.Equal(actualDesc) {
				return fmt.Sprintf("descriptor changed: [expected] %s != [actual] %s",
					desc, actualDesc), true
			}
		}
	}
	return "", false
}

// adminSplitWithDescriptor divides the range into into two ranges, using
// either args.SplitKey (if provided) or an internally computed key that aims
// to roughly equipartition the range by size. The split is done inside of a
// distributed txn which writes updated left and new right hand side range
// descriptors, and updates the range addressing metadata. The handover of
// responsibility for the reassigned key range is carried out seamlessly
// through a split trigger carried out as part of the commit of that
// transaction.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. An
// operation which might split a range should obtain a copy of the range's
// current descriptor before making the decision to split. If the decision is
// affirmative the descriptor is passed to AdminSplit, which performs a
// Conditional Put on the RangeDescriptor to ensure that no other operation has
// modified the range in the time the decision was being made.
// TODO(tschottdorf): should assert that split key is not a local key.
//
// See the comment on splitTrigger for details on the complexities.
func (r *Replica) adminSplitWithDescriptor(
	ctx context.Context, args roachpb.AdminSplitRequest, desc *roachpb.RangeDescriptor,
) (_ roachpb.AdminSplitResponse, validSplitKey bool, _ *roachpb.Error) {
	var reply roachpb.AdminSplitResponse

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	log.Event(ctx, "split begins")
	var splitKey roachpb.RKey
	{
		// Once a split occurs, it can't be rolled back even on a downgrade, so
		// we only allow meta2 splits if the minimum supported version is
		// VersionMeta2Splits, as opposed to allowing them if the active version
		// is VersionMeta2Splits.
		allowMeta2Splits := r.store.cfg.Settings.Version.IsMinSupported(cluster.VersionMeta2Splits)

		var foundSplitKey roachpb.Key
		if len(args.SplitKey) == 0 {
			// Find a key to split by size.
			var err error
			targetSize := r.GetMaxBytes() / 2
			foundSplitKey, err = engine.MVCCFindSplitKey(
				ctx, r.store.engine, desc.StartKey, desc.EndKey, targetSize, allowMeta2Splits)
			if err != nil {
				return reply, false, roachpb.NewErrorf("unable to determine split key: %s", err)
			}
			if foundSplitKey == nil {
				// No suitable split key could be found.
				return reply, false, nil
			}
		} else {
			// If the key that routed this request to this range is now out of this
			// range's bounds, return an error for the client to try again on the
			// correct range.
			if !containsKey(*desc, args.Span.Key) {
				return reply, false,
					roachpb.NewError(roachpb.NewRangeKeyMismatchError(args.Span.Key, args.Span.Key, desc))
			}
			foundSplitKey = args.SplitKey
		}

		if !containsKey(*desc, foundSplitKey) {
			return reply, false,
				roachpb.NewErrorf("requested split key %s out of bounds of %s", args.SplitKey, r)
		}

		var err error
		splitKey, err = keys.Addr(foundSplitKey)
		if err != nil {
			return reply, false, roachpb.NewError(err)
		}
		if !splitKey.Equal(foundSplitKey) {
			return reply, false, roachpb.NewErrorf("cannot split range at range-local key %s", splitKey)
		}
		if !engine.IsValidSplitKey(foundSplitKey, allowMeta2Splits) {
			return reply, false, roachpb.NewErrorf("cannot split range at key %s", splitKey)
		}
	}

	// If the range starts at the splitKey, we treat the AdminSplit
	// as a no-op and return success instead of throwing an error.
	if desc.StartKey.Equal(splitKey) {
		log.Event(ctx, "range already split")
		return reply, false, nil
	}
	log.Event(ctx, "found split key")

	// Create right hand side range descriptor with the newly-allocated Range ID.
	rightDesc, err := r.store.NewRangeDescriptor(ctx, splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, true,
			roachpb.NewErrorf("unable to allocate right hand side range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	leftDesc := *desc
	leftDesc.EndKey = splitKey

	log.Infof(ctx, "initiating a split of this range at key %s [r%d]",
		splitKey, rightDesc.RangeID)

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "split closure begins")
		defer log.Event(ctx, "split closure ends")
		txn.SetDebugName(splitTxnName)
		// Update existing range descriptor for left hand side of
		// split. Note that we mutate the descriptor for the left hand
		// side of the split first to locate the txn record there.
		{
			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(leftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, desc, leftDesc); err != nil {
				return err
			}
			// Commit this batch first to ensure that the transaction record
			// is created in the right place (split trigger relies on this),
			// but also to ensure the transaction record is created _before_
			// intents for the RHS range descriptor or addressing records.
			// Keep in mind that the BeginTransaction request is injected
			// to accompany the first write request, but if part of a batch
			// which spans ranges, the dist sender does not guarantee the
			// order which parts of the split batch arrive.
			//
			// Sending the batch containing only the first write guarantees
			// the transaction record is written first, preventing cases
			// where splits are aborted early due to conflicts with meta
			// intents (see #9265).
			log.Event(ctx, "updating LHS descriptor")
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log the split into the range event log.
		// TODO(spencer): event logging API should accept a batch
		// instead of a transaction; there's no reason this logging
		// shouldn't be done in parallel via the batch with the updated
		// range addressing.
		if err := r.store.logSplit(ctx, txn, leftDesc, *rightDesc); err != nil {
			return err
		}

		b := txn.NewBatch()

		// Create range descriptor for right hand side of the split.
		rightDescKey := keys.RangeDescriptorKey(rightDesc.StartKey)
		if err := updateRangeDescriptor(b, rightDescKey, nil, *rightDesc); err != nil {
			return err
		}

		// Update range descriptor addressing record(s).
		if err := splitRangeAddressing(b, rightDesc, &leftDesc); err != nil {
			return err
		}

		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: *rightDesc,
				},
			},
		})

		// Commit txn with final batch (RHS descriptor and meta).
		log.Event(ctx, "commit txn with batch containing RHS descriptor and meta records")
		return txn.Run(ctx, b)
	}); err != nil {
		// The ConditionFailedError can occur because the descriptors acting
		// as expected values in the CPuts used to update the left or right
		// range descriptors are picked outside the transaction. Return
		// ConditionFailedError in the error detail so that the command can be
		// retried.
		pErr := roachpb.NewError(err)
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			pErr.Message = fmt.Sprintf("split at key %s failed: %s", splitKey, msg)
		} else {
			pErr.Message = fmt.Sprintf("split at key %s failed: %s", splitKey, err)
		}
		return reply, true, pErr
	}
	return reply, true, nil
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
	rec batcheval.EvalContext,
	batch engine.Batch,
	bothDeltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	ts hlc.Timestamp,
) (enginepb.MVCCStats, result.Result, error) {
	// TODO(tschottdorf): should have an incoming context from the corresponding
	// EndTransaction, but the plumbing has not been done yet.
	sp := rec.Tracer().StartSpan("split")
	defer sp.Finish()
	desc := rec.Desc()
	if !bytes.Equal(desc.StartKey, split.LeftDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.RightDesc.EndKey) {
		return enginepb.MVCCStats{}, result.Result{}, errors.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.LeftDesc.StartKey, split.LeftDesc.EndKey,
			split.RightDesc.StartKey, split.RightDesc.EndKey, rec)
	}

	// Preserve stats for pre-split range, excluding the current batch.
	origBothMS := rec.GetMVCCStats()

	// TODO(d4l3k): we should check which side of the split is smaller
	// and compute stats for it instead of having a constraint that the
	// left hand side is smaller.

	// Compute (absolute) stats for LHS range. This means that no more writes
	// to the LHS must happen below this point.
	leftMS, err := rditer.ComputeStatsForRange(&split.LeftDesc, batch, ts.WallTime)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to compute stats for LHS range after split")
	}
	log.Event(ctx, "computed stats for left hand side range")

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

	// Initialize the RHS range's AbortSpan by copying the LHS's.
	seqCount, err := rec.AbortSpan().CopyInto(batch, &bothDeltaMS, split.RightDesc.RangeID)
	if err != nil {
		// TODO(tschottdorf): ReplicaCorruptionError.
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to copy AbortSpan to RHS split range")
	}
	log.Eventf(ctx, "copied AbortSpan (%d entries)", seqCount)

	// Compute (absolute) stats for RHS range.
	var rightMS enginepb.MVCCStats
	if origBothMS.ContainsEstimates || bothDeltaMS.ContainsEstimates {
		// Because either the original stats or the delta stats contain
		// estimate values, we cannot perform arithmetic to determine the
		// new range's stats. Instead, we must recompute by iterating
		// over the keys and counting.
		rightMS, err = rditer.ComputeStatsForRange(&split.RightDesc, batch, ts.WallTime)
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to compute stats for RHS range after split")
		}
	} else {
		// Because neither the original stats nor the delta stats contain
		// estimate values, we can safely perform arithmetic to determine the
		// new range's stats. The calculation looks like:
		//   rhs_ms = orig_both_ms - orig_left_ms + right_delta_ms
		//          = orig_both_ms - left_ms + left_delta_ms + right_delta_ms
		//          = orig_both_ms - left_ms + delta_ms
		// where the following extra helper variables are used:
		// - orig_left_ms: the left-hand side key range, before the split
		// - (left|right)_delta_ms: the contributions to bothDeltaMS in this batch,
		//   itemized by the side of the split.
		//
		// Note that the result of that computation never has ContainsEstimates
		// set due to none of the inputs having it.

		// Start with the full stats before the split.
		rightMS = origBothMS
		// Remove stats from the left side of the split, at the same time adding
		// the batch contributions for the right-hand side.
		rightMS.Subtract(leftMS)
		rightMS.Add(bothDeltaMS)
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
		preRightMS := rightMS // for bothDeltaMS

		// Account for MVCCStats' own contribution to the RHS range's statistics.
		if err := engine.AccountForSelf(&rightMS, split.RightDesc.RangeID); err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to account for enginepb.MVCCStats's own stats impact")
		}

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
		leftLease, err := batcheval.MakeStateLoader(rec).LoadLease(ctx, rec.Engine())
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

		gcThreshold, err := batcheval.MakeStateLoader(rec).LoadGCThreshold(ctx, rec.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to load GCThreshold")
		}
		if (*gcThreshold == hlc.Timestamp{}) {
			log.VEventf(ctx, 1, "LHS's GCThreshold of split is not set")
		}

		txnSpanGCThreshold, err := batcheval.MakeStateLoader(rec).LoadTxnSpanGCThreshold(ctx, rec.Engine())
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to load TxnSpanGCThreshold")
		}
		if (*txnSpanGCThreshold == hlc.Timestamp{}) {
			log.VEventf(ctx, 1, "LHS's TxnSpanGCThreshold of split is not set")
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
		rightMS, err = writeInitialReplicaState(
			ctx, rec.ClusterSettings(), batch, rightMS, split.RightDesc,
			rightLease, *gcThreshold, *txnSpanGCThreshold,
		)
		if err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to write initial Replica state")
		}

		if !rec.ClusterSettings().Version.IsActive(cluster.VersionSplitHardStateBelowRaft) {
			// Write an initial state upstream of Raft even though it might
			// clobber downstream simply because that's what 1.0 does and if we
			// don't write it here, then a 1.0 version applying it as a follower
			// won't write a HardState at all and is guaranteed to crash.
			rsl := stateloader.Make(rec.ClusterSettings(), split.RightDesc.RangeID)
			if err := rsl.SynthesizeRaftState(ctx, batch); err != nil {
				return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to synthesize initial Raft state")
			}
		}

		bothDeltaMS.Subtract(preRightMS)
		bothDeltaMS.Add(rightMS)
	}

	// Compute how much data the left-hand side has shed by splitting.
	// We've already recomputed that in absolute terms, so all we need to do is
	// to turn it into a delta so the upstream machinery can digest it.
	leftDeltaMS := leftMS // start with new left-hand side absolute stats
	recStats := rec.GetMVCCStats()
	leftDeltaMS.Subtract(recStats)        // subtract pre-split absolute stats
	leftDeltaMS.ContainsEstimates = false // if there were any, recomputation removed them

	// Perform a similar computation for the right hand side. The difference
	// is that there isn't yet a Replica which could apply these stats, so
	// they will go into the trigger to make the Store (which keeps running
	// counters) aware.
	rightDeltaMS := bothDeltaMS
	rightDeltaMS.Subtract(leftDeltaMS)
	var pd result.Result
	// This makes sure that no reads are happening in parallel; see #3148.
	pd.Replicated.BlockReads = true
	pd.Replicated.Split = &storagebase.Split{
		SplitTrigger: *split,
		RHSDelta:     rightDeltaMS,
	}
	return leftDeltaMS, pd, nil
}

// AdminMerge extends this range to subsume the range that comes next
// in the key space. The merge is performed inside of a distributed
// transaction which writes the left hand side range descriptor (the
// subsuming range) and deletes the range descriptor for the right
// hand side range (the subsumed range). It also updates the range
// addressing metadata. The handover of responsibility for the
// reassigned key range is carried out seamlessly through a merge
// trigger carried out as part of the commit of that transaction.  A
// merge requires that the two ranges are collocated on the same set
// of replicas.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) AdminMerge(
	ctx context.Context, args roachpb.AdminMergeRequest,
) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse

	origLeftDesc := r.Desc()
	if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
		// Merging the final range doesn't make sense.
		return reply, roachpb.NewErrorf("cannot merge final range")
	}

	updatedLeftDesc := *origLeftDesc

	// Lookup right hand side range (subsumed). This really belongs
	// inside the transaction for consistency, but it is important (for
	// transaction record placement) that the first action inside the
	// transaction is the conditional put to change the left hand side's
	// descriptor end key. We look up the descriptor here only to get
	// the new end key and then repeat the lookup inside the
	// transaction.
	{
		rightRng := r.store.LookupReplica(origLeftDesc.EndKey, nil)
		if rightRng == nil {
			return reply, roachpb.NewErrorf("ranges not collocated")
		}

		updatedLeftDesc.EndKey = rightRng.Desc().EndKey
		log.Infof(ctx, "initiating a merge of %s into this range", rightRng)
	}

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "merge closure begins")
		txn.SetDebugName(mergeTxnName)
		// Update the range descriptor for the receiving range.
		{
			b := txn.NewBatch()
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, origLeftDesc, updatedLeftDesc); err != nil {
				return err
			}
			// Commit this batch on its own to ensure that the transaction record
			// is created in the right place (our triggers rely on this).
			log.Event(ctx, "updating LHS descriptor")
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Do a consistent read of the right hand side's range descriptor.
		rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)
		var rightDesc roachpb.RangeDescriptor
		if err := txn.GetProto(ctx, rightDescKey, &rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return errors.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if !bytes.Equal(rightDesc.EndKey, updatedLeftDesc.EndKey) {
			// This merge raced with a split of the right-hand range.
			// TODO(bdarnell): needs a test.
			return errors.Errorf("range changed during merge; %s != %s", rightDesc.EndKey, updatedLeftDesc.EndKey)
		}
		if !replicaSetsEqual(origLeftDesc.Replicas, rightDesc.Replicas) {
			return errors.Errorf("ranges not collocated")
		}

		b := txn.NewBatch()

		// Remove the range descriptor for the deleted range.
		b.Del(rightDescKey)

		if err := mergeRangeAddressing(b, origLeftDesc, &updatedLeftDesc); err != nil {
			return err
		}
		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				MergeTrigger: &roachpb.MergeTrigger{
					LeftDesc:  updatedLeftDesc,
					RightDesc: rightDesc,
				},
			},
		})
		log.Event(ctx, "attempting commit")
		return txn.Run(ctx, b)
	}); err != nil {
		return reply, roachpb.NewErrorf("merge of range into %d failed: %s", origLeftDesc.RangeID, err)
	}

	return reply, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
//
// TODO(tschottdorf): give mergeTrigger more idiomatic stats computation as
// in splitTrigger.
func mergeTrigger(
	ctx context.Context,
	rec batcheval.EvalContext,
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

	rightRangeID := merge.RightDesc.RangeID
	if rightRangeID <= 0 {
		return result.Result{}, errors.Errorf("RHS range ID must be provided: %d", rightRangeID)
	}

	// Compute stats for premerged range, including current transaction.
	mergedMS := rec.GetMVCCStats()
	mergedMS.Add(*ms)
	// We will recompute the stats below and update the state, so when the
	// batch commits it has already taken ms into account.
	*ms = enginepb.MVCCStats{}

	// Add in stats for right hand side of merge, excluding system-local
	// stats, which will need to be recomputed.
	rightMS, err := stateloader.Make(rec.ClusterSettings(), rightRangeID).LoadMVCCStats(ctx, batch)
	if err != nil {
		return result.Result{}, err
	}
	rightMS.SysBytes, rightMS.SysCount = 0, 0
	mergedMS.Add(rightMS)

	// Copy the RHS range's AbortSpan to the new LHS one.
	if _, err := rec.AbortSpan().CopyFrom(ctx, batch, &mergedMS, rightRangeID); err != nil {
		return result.Result{}, errors.Errorf("unable to copy AbortSpan to new split range: %s", err)
	}

	// Remove the RHS range's metadata. Note that we don't need to
	// keep track of stats here, because we already set the right range's
	// system-local stats contribution to 0.
	localRangeIDKeyPrefix := keys.MakeRangeIDPrefix(rightRangeID)
	if _, _, _, err := engine.MVCCDeleteRange(ctx, batch, nil, localRangeIDKeyPrefix, localRangeIDKeyPrefix.PrefixEnd(), math.MaxInt64, hlc.Timestamp{}, nil, false); err != nil {
		return result.Result{}, errors.Errorf("cannot remove range metadata %s", err)
	}

	// Add in the stats for the RHS range's range keys.
	iter := batch.NewIterator(false)
	defer iter.Close()
	localRangeKeyStart := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.RightDesc.StartKey))
	localRangeKeyEnd := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.RightDesc.EndKey))
	msRange, err := iter.ComputeStats(localRangeKeyStart, localRangeKeyEnd, ts.WallTime)
	if err != nil {
		return result.Result{}, errors.Errorf("unable to compute RHS range's local stats: %s", err)
	}
	mergedMS.Add(msRange)

	// Set stats for updated range.
	if err := batcheval.MakeStateLoader(rec).SetMVCCStats(ctx, batch, &mergedMS); err != nil {
		return result.Result{}, errors.Errorf("unable to write MVCC stats: %s", err)
	}

	// Clear the timestamp cache. In case both the LHS and RHS replicas
	// held their respective range leases, we could merge the timestamp
	// caches for efficiency. But it's unlikely and not worth the extra
	// logic and potential for error.

	*ms = rec.GetMVCCStats()
	mergedMS.Subtract(*ms)
	*ms = mergedMS

	var pd result.Result
	pd.Replicated.BlockReads = true
	pd.Replicated.Merge = &storagebase.Merge{
		MergeTrigger: *merge,
	}
	return pd, nil
}

func changeReplicasTrigger(
	ctx context.Context,
	rec batcheval.EvalContext,
	batch engine.Batch,
	change *roachpb.ChangeReplicasTrigger,
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

	var cpy roachpb.RangeDescriptor
	{
		desc := rec.Desc()
		cpy = *desc
	}
	cpy.Replicas = change.UpdatedReplicas
	cpy.NextReplicaID = change.NextReplicaID
	// TODO(tschottdorf): duplication of Desc with the trigger below, should
	// likely remove it from the trigger.
	pd.Replicated.State = &storagebase.ReplicaState{
		Desc: &cpy,
	}
	pd.Replicated.ChangeReplicas = &storagebase.ChangeReplicas{
		ChangeReplicasTrigger: *change,
	}

	return pd
}

type snapshotError struct {
	cause error
}

func (s *snapshotError) Error() string {
	return fmt.Sprintf("snapshot failed: %s", s.cause.Error())
}

// IsSnapshotError returns true iff the error indicates a preemptive
// snapshot failed.
func IsSnapshotError(err error) bool {
	_, ok := err.(*snapshotError)
	return ok
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "adminSplitWithDescriptor" for more information on this pattern.
//
// Changing the replicas for a range is complicated. A change is initiated by
// the "replicate" queue when it encounters a range which has too many
// replicas, too few replicas or requires rebalancing. Addition and removal of
// a replica is divided into four phases. The first phase, which occurs in
// Replica.ChangeReplicas, is performed via a distributed transaction which
// updates the range descriptor and the meta range addressing information. This
// transaction includes a special ChangeReplicasTrigger on the EndTransaction
// request. A ConditionalPut of the RangeDescriptor implements the optimistic
// lock on the RangeDescriptor mentioned previously. Like all transactions, the
// requests within the transaction are replicated via Raft, including the
// EndTransaction request.
//
// The second phase of processing occurs when the batch containing the
// EndTransaction is proposed to raft. This proposing occurs on whatever
// replica received the batch, usually, but not always the range lease
// holder. defaultProposeRaftCommandLocked notices that the EndTransaction
// contains a ChangeReplicasTrigger and proposes a ConfChange to Raft (via
// raft.RawNode.ProposeConfChange).
//
// The ConfChange is propagated to all of the replicas similar to a normal Raft
// command, though additional processing is done inside of Raft. A Replica
// encounters the ConfChange in Replica.handleRaftReady and executes it using
// raft.RawNode.ApplyConfChange. If a new replica was added the Raft leader
// will start sending it heartbeat messages and attempting to bring it up to
// date. If a replica was removed, it is at this point that the Raft leader
// will stop communicating with it.
//
// The fourth phase of change replicas occurs when each replica for the range
// encounters the ChangeReplicasTrigger when applying the EndTransaction
// request. The replica will update its local range descriptor so as to contain
// the new set of replicas. If the replica is the one that is being removed, it
// will queue itself for removal with replicaGCQueue.
//
// Note that a removed replica may not see the EndTransaction containing the
// ChangeReplicasTrigger. The ConfChange operation will be applied as soon as a
// quorum of nodes have committed it. If the removed replica is down or the
// message is dropped for some reason the removed replica will not be
// notified. The replica GC queue will eventually discover and cleanup this
// state.
//
// When a new replica is added, it will have to catch up to the state of the
// other replicas. The Raft leader automatically handles this by either sending
// the new replica Raft log entries to apply, or by generating and sending a
// snapshot. See Replica.Snapshot and Replica.Entries.
//
// Note that Replica.ChangeReplicas returns when the distributed transaction
// has been committed to a quorum of replicas in the range. The actual
// replication of data occurs asynchronously via a snapshot or application of
// Raft log entries. This is important for the replicate queue to be aware
// of. A node can process hundreds or thousands of ChangeReplicas operations
// per second even though the actual replication of data proceeds at a much
// slower base. In order to avoid having this background replication overwhelm
// the system, replication is throttled via a reservation system. When
// allocating a new replica for a range, the replicate queue reserves space for
// that replica on the target store via a ReservationRequest. (See
// StorePool.reserve). The reservation is fulfilled when the snapshot is
// applied.
//
// TODO(peter): There is a rare scenario in which a replica can be brought up
// to date via Raft log replay. In this scenario, the reservation will be left
// dangling until it expires. See #7849.
//
// TODO(peter): Describe preemptive snapshots. Preemptive snapshots are needed
// for the replicate queue to function properly. Currently the replicate queue
// will fire off as many replica additions as possible until it starts getting
// reservations denied at which point it will ignore the replica until the next
// scanner cycle.
func (r *Replica) ChangeReplicas(
	ctx context.Context,
	changeType roachpb.ReplicaChangeType,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	reason RangeLogEventReason,
	details string,
) error {
	return r.changeReplicas(ctx, changeType, target, desc, SnapshotRequest_REBALANCE, reason, details)
}

func (r *Replica) changeReplicas(
	ctx context.Context,
	changeType roachpb.ReplicaChangeType,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason RangeLogEventReason,
	details string,
) error {
	repDesc := roachpb.ReplicaDescriptor{
		NodeID:  target.NodeID,
		StoreID: target.StoreID,
	}
	repDescIdx := -1  // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas {
		nodeUsedByExistingRep := existingRep.NodeID == repDesc.NodeID
		nodeUsed = nodeUsed || nodeUsedByExistingRep

		if nodeUsedByExistingRep && existingRep.StoreID == repDesc.StoreID {
			repDescIdx = i
			repDesc.ReplicaID = existingRep.ReplicaID
			break
		}
	}

	rangeID := desc.RangeID
	updatedDesc := *desc
	updatedDesc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)

	switch changeType {
	case roachpb.ADD_REPLICA:
		// If the replica exists on the remote node, no matter in which store,
		// abort the replica add.
		if nodeUsed {
			if repDescIdx != -1 {
				return errors.Errorf("%s: unable to add replica %v which is already present", r, repDesc)
			}
			return errors.Errorf("%s: unable to add replica %v; node already has a replica", r, repDesc)
		}

		// Prohibit premature raft log truncation. We set the pending index to 1
		// here until we determine what it is below. This removes a small window of
		// opportunity for the raft log to get truncated after the snapshot is
		// generated.
		if err := r.setPendingSnapshotIndex(1); err != nil {
			return err
		}
		defer r.clearPendingSnapshotIndex()

		// Send a pre-emptive snapshot. Note that the replica to which this
		// snapshot is addressed has not yet had its replica ID initialized; this
		// is intentional, and serves to avoid the following race with the replica
		// GC queue:
		//
		// - snapshot received, a replica is lazily created with the "real" replica ID
		// - the replica is eligible for GC because it is not yet a member of the range
		// - GC queue runs, creating a raft tombstone with the replica's ID
		// - the replica is added to the range
		// - lazy creation of the replica fails due to the raft tombstone
		//
		// Instead, the replica GC queue will create a tombstone with replica ID
		// zero, which is never legitimately used, and thus never interferes with
		// raft operations. Racing with the replica GC queue can still partially
		// negate the benefits of pre-emptive snapshots, but that is a recoverable
		// degradation, not a catastrophic failure.
		//
		// NB: A closure is used here so that we can release the snapshot as soon
		// as it has been applied on the remote and before the ChangeReplica
		// operation is processed. This is important to allow other ranges to make
		// progress which might be required for this ChangeReplicas operation to
		// complete. See #10409.
		if err := r.sendSnapshot(ctx, repDesc, snapTypePreemptive, priority); err != nil {
			return err
		}

		repDesc.ReplicaID = updatedDesc.NextReplicaID
		updatedDesc.NextReplicaID++
		updatedDesc.Replicas = append(updatedDesc.Replicas, repDesc)

	case roachpb.REMOVE_REPLICA:
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if repDescIdx == -1 {
			return errors.Errorf("%s: unable to remove replica %v which is not present", r, repDesc)
		}
		updatedDesc.Replicas[repDescIdx] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
		updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
	}

	descKey := keys.RangeDescriptorKey(desc.StartKey)

	if err := r.store.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		log.Event(ctx, "attempting txn")
		txn.SetDebugName(replicaChangeTxnName)
		// TODO(tschottdorf): oldDesc is used for sanity checks related to #7224.
		// Remove when that has been solved. The failure mode is likely based on
		// prior divergence of the Replica (in which case the check below does not
		// fire because everything reads from the local, diverged, set of data),
		// so we don't expect to see this fail in practice ever.
		oldDesc := new(roachpb.RangeDescriptor)
		if err := txn.GetProto(ctx, descKey, oldDesc); err != nil {
			return err
		}
		log.Infof(ctx, "change replicas (%v %s): read existing descriptor %s",
			changeType, repDesc, oldDesc)

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(b, descKey, desc, updatedDesc); err != nil {
				return err
			}

			// Run transaction up to this point to create txn record early (see #9265).
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		if err := r.store.logChange(
			ctx, txn, changeType, repDesc, updatedDesc, reason, details,
		); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		b := txn.NewBatch()

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, &updatedDesc); err != nil {
			return err
		}

		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
					ChangeType:      changeType,
					Replica:         repDesc,
					UpdatedReplicas: updatedDesc.Replicas,
					NextReplicaID:   updatedDesc.NextReplicaID,
				},
			},
		})
		if err := txn.Run(ctx, b); err != nil {
			log.Event(ctx, err.Error())
			return err
		}

		if oldDesc.RangeID != 0 && !oldDesc.Equal(desc) {
			// We read the previous value, it wasn't what we supposedly used in
			// the CPut, but we still overwrote in the CPut above.
			panic(fmt.Sprintf("committed replica change, but oldDesc != assumedOldDesc:\n%+v\n%+v\nnew desc:\n%+v",
				oldDesc, desc, updatedDesc))
		}
		return nil
	}); err != nil {
		log.Event(ctx, err.Error())
		if msg, ok := maybeDescriptorChangedError(desc, err); ok {
			return errors.Wrapf(err, "change replicas of r%d failed: %s", rangeID, msg)
		}
		return errors.Wrapf(err, "change replicas of r%d failed", rangeID)
	}
	log.Event(ctx, "txn complete")
	return nil
}

// sendSnapshot sends a snapshot of the replica state to the specified
// replica. This is used for both preemptive snapshots that are performed
// before adding a replica to a range, and for Raft-initiated snapshots that
// are used to bring a replica up to date that has fallen too far
// behind. Currently only invoked from replicateQueue and raftSnapshotQueue. Be
// careful about adding additional calls as generating a snapshot is moderately
// expensive.
func (r *Replica) sendSnapshot(
	ctx context.Context,
	repDesc roachpb.ReplicaDescriptor,
	snapType string,
	priority SnapshotRequest_Priority,
) error {
	snap, err := r.GetSnapshot(ctx, snapType)
	if err != nil {
		return errors.Wrapf(err, "%s: failed to generate %s snapshot", r, snapType)
	}
	defer snap.Close()
	log.Event(ctx, "generated snapshot")

	fromRepDesc, err := r.GetReplicaDescriptor()
	if err != nil {
		return errors.Wrapf(err, "%s: change replicas failed", r)
	}

	if snapType == snapTypePreemptive {
		if err := r.setPendingSnapshotIndex(snap.RaftSnap.Metadata.Index); err != nil {
			return err
		}
	}

	status := r.RaftStatus()
	if status == nil {
		return errors.New("raft status not initialized")
	}

	req := SnapshotRequest_Header{
		State: snap.State,
		RaftMessageRequest: RaftMessageRequest{
			RangeID:     r.RangeID,
			FromReplica: fromRepDesc,
			ToReplica:   repDesc,
			Message: raftpb.Message{
				Type:     raftpb.MsgSnap,
				To:       uint64(repDesc.ReplicaID),
				From:     uint64(fromRepDesc.ReplicaID),
				Term:     status.Term,
				Snapshot: snap.RaftSnap,
			},
		},
		RangeSize: r.GetMVCCStats().Total(),
		// Recipients can choose to decline preemptive snapshots.
		CanDecline: snapType == snapTypePreemptive,
		Priority:   priority,
	}
	sent := func() {
		r.store.metrics.RangeSnapshotsGenerated.Inc(1)
	}
	if err := r.store.cfg.Transport.SendSnapshot(
		ctx, r.store.allocator.storePool, req, snap, r.store.Engine().NewBatch, sent); err != nil {
		return &snapshotError{err}
	}
	return nil
}

// replicaSetsEqual is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func replicaSetsEqual(a, b []roachpb.ReplicaDescriptor) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[roachpb.StoreID]int)
	for _, replica := range a {
		set[replica.StoreID]++
	}

	for _, replica := range b {
		set[replica.StoreID]--
	}

	for _, value := range set {
		if value != 0 {
			return false
		}
	}

	return true
}

// updateRangeDescriptor adds a ConditionalPut on the range descriptor. The
// conditional put verifies that changes to the range descriptor are made in a
// well-defined order, preventing a scenario where a wayward replica which is
// no longer part of the original Raft group comes back online to form a
// splinter group with a node which was also a former replica, and hijacks the
// range descriptor. This is a last line of defense; other mechanisms should
// prevent rogue replicas from getting this far (see #768).
//
// oldDesc can be nil, meaning that the key is expected to not exist.
//
// Note that in addition to using this method to update the on-disk range
// descriptor, a CommitTrigger must be used to update the in-memory
// descriptor; it will not automatically be copied from newDesc.
// TODO(bdarnell): store the entire RangeDescriptor in the CommitTrigger
// and load it automatically instead of reconstructing individual
// changes.
func updateRangeDescriptor(
	b *client.Batch,
	descKey roachpb.Key,
	oldDesc *roachpb.RangeDescriptor,
	newDesc roachpb.RangeDescriptor,
) error {
	if err := newDesc.Validate(); err != nil {
		return err
	}
	// This is subtle: []byte(nil) != interface{}(nil). A []byte(nil) refers to
	// an empty value. An interface{}(nil) refers to a non-existent value. So
	// we're careful to construct an interface{}(nil) when oldDesc is nil.
	var oldValue interface{}
	if oldDesc != nil {
		oldBytes, err := protoutil.Marshal(oldDesc)
		if err != nil {
			return err
		}
		oldValue = oldBytes
	}
	newValue, err := protoutil.Marshal(&newDesc)
	if err != nil {
		return err
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}

// TestingRelocateRange relocates a given range to a given set of stores. The first
// store in the slice becomes the new leaseholder.
//
// This is best-effort; if replication queues are enabled and a change in
// membership happens at the same time, there will be errors.
func TestingRelocateRange(
	ctx context.Context,
	db *client.DB,
	rangeDesc roachpb.RangeDescriptor,
	targets []roachpb.ReplicationTarget,
) error {
	// Step 1: Add any stores that don't already have a replica of the range.
	//
	// TODO(radu): we can't have multiple replicas on different stores on the same
	// node, which can lead to some odd corner cases where we would have to first
	// remove some replicas (currently these cases fail).

	var addTargets []roachpb.ReplicationTarget
	for _, t := range targets {
		found := false
		for _, replicaDesc := range rangeDesc.Replicas {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, t)
		}
	}

	canRetry := func(err error) bool {
		whitelist := []string{
			snapshotApplySemBusyMsg,
			IntersectingSnapshotMsg,
		}
		for _, substr := range whitelist {
			if strings.Contains(err.Error(), substr) {
				return true
			}
		}
		return false
	}

	for len(addTargets) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		target := addTargets[0]
		if err := db.AdminChangeReplicas(
			ctx, rangeDesc.StartKey.AsRawKey(), roachpb.ADD_REPLICA, []roachpb.ReplicationTarget{target},
		); err != nil {
			returnErr := errors.Wrapf(err, "while adding target %v", target)
			if !canRetry(err) {
				return returnErr
			}
			log.Warning(ctx, returnErr)
			continue
		}
		addTargets = addTargets[1:]
	}

	// Step 2: Transfer the lease to the first target. This needs to happen before
	// we remove replicas or we may try to remove the lease holder.

	transferLease := func() {
		if err := db.AdminTransferLease(
			ctx, rangeDesc.StartKey.AsRawKey(), targets[0].StoreID,
		); err != nil {
			log.Warningf(ctx, "while transferring lease: %s", err)
		}
	}

	transferLease()

	// Step 3: Remove any replicas that are not targets.

	var removeTargets []roachpb.ReplicationTarget
	for _, replicaDesc := range rangeDesc.Replicas {
		found := false
		for _, t := range targets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			removeTargets = append(removeTargets, roachpb.ReplicationTarget{
				StoreID: replicaDesc.StoreID,
				NodeID:  replicaDesc.NodeID,
			})
		}
	}

	for len(removeTargets) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		target := removeTargets[0]
		transferLease()
		if err := db.AdminChangeReplicas(
			ctx, rangeDesc.StartKey.AsRawKey(), roachpb.REMOVE_REPLICA, []roachpb.ReplicationTarget{target},
		); err != nil {
			log.Warningf(ctx, "while removing target %v: %s", target, err)
			if !canRetry(err) {
				return err
			}
			continue
		}
		removeTargets = removeTargets[1:]
	}
	return ctx.Err()
}

// adminScatter moves replicas and leaseholders for a selection of ranges.
func (r *Replica) adminScatter(
	ctx context.Context, args roachpb.AdminScatterRequest,
) (roachpb.AdminScatterResponse, error) {
	sysCfg, ok := r.store.cfg.Gossip.GetSystemConfig()
	if !ok {
		log.Infof(ctx, "scatter failed (system config not yet available)")
		return roachpb.AdminScatterResponse{}, errors.New("system config not yet available")
	}

	rq := r.store.replicateQueue
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}

	// Loop until the replicate queue decides there is nothing left to do for the
	// range. Note that we disable lease transfers until the final step as
	// transferring the lease prevents any further action on this node.
	var allowLeaseTransfer bool
	canTransferLease := func() bool { return allowLeaseTransfer }
	for re := retry.StartWithCtx(ctx, retryOpts); re.Next(); {
		requeue, err := rq.processOneChange(ctx, r, sysCfg, canTransferLease, false /* dryRun */, true /* disableStatsBasedRebalancing */)
		if err != nil {
			if IsSnapshotError(err) {
				continue
			}
			break
		}
		if !requeue {
			if allowLeaseTransfer {
				break
			}
			allowLeaseTransfer = true
		}
		re.Reset()
	}

	// If we've been asked to randomize the leases beyond what the replicate
	// queue would do on its own (#17341), do so after the replicate queue is
	// done by transferring the lease to any of the given N replicas with
	// probability 1/N of choosing each.
	if args.RandomizeLeases && r.OwnsValidLease(r.store.Clock().Now()) {
		desc := r.Desc()
		newLeaseholderIdx := rand.Intn(len(desc.Replicas))
		targetStoreID := desc.Replicas[newLeaseholderIdx].StoreID
		if targetStoreID != r.store.StoreID() {
			if err := r.AdminTransferLease(ctx, targetStoreID); err != nil {
				log.Warningf(ctx, "failed to scatter lease to s%d: %s", targetStoreID, err)
			}
		}
	}

	desc := r.Desc()
	return roachpb.AdminScatterResponse{
		Ranges: []roachpb.AdminScatterResponse_Range{{
			Span: roachpb.Span{
				Key:    desc.StartKey.AsRawKey(),
				EndKey: desc.EndKey.AsRawKey(),
			},
		}},
	}, nil
}
