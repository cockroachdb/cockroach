// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// MaxMVCCStatCountDiff defines the maximum number of units (e.g. keys or
// intents) that is acceptable for an individual MVCC stat to diverge from the
// real value when computed during splits. If this threshold is
// exceeded, the split will fall back to computing 100% accurate stats.
// It takes effect only if kv.split.estimated_mvcc_stats.enabled is true.
var MaxMVCCStatCountDiff = settings.RegisterIntSetting(
	settings.SystemVisible,
	"kv.split.max_mvcc_stat_count_diff",
	"defines the max number of units that are acceptable for an individual "+
		"MVCC stat to diverge; needs kv.split.estimated_mvcc_stats.enabled to be true",
	5000)

// MaxMVCCStatBytesDiff defines the maximum number of bytes (e.g. keys bytes or
// intents bytes) that is acceptable for an individual MVCC stat to diverge
// from the real value when computed during splits. If this threshold is
// exceeded, the split will fall back to computing 100% accurate stats.
// It takes effect only if kv.split.estimated_mvcc_stats.enabled is true.
var MaxMVCCStatBytesDiff = settings.RegisterIntSetting(
	settings.SystemVisible,
	"kv.split.max_mvcc_stat_bytes_diff",
	"defines the max number of bytes that are acceptable for an individual "+
		"MVCC stat to diverge; needs kv.split.estimated_mvcc_stats.enabled to be true",
	5120000) // 5.12 MB = 1% of the max range size

func init() {
	RegisterReadWriteCommand(kvpb.EndTxn, declareKeysEndTxn, EndTxn)
}

// declareKeysWriteTransaction is the shared portion of
// declareKeys{End,Heartbeat}Transaction.
func declareKeysWriteTransaction(
	_ ImmutableRangeState, header *kvpb.Header, req kvpb.Request, latchSpans *spanset.SpanSet,
) error {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
			Key: keys.TransactionKey(req.Header().Key, header.Txn.ID),
		})
	}
	return nil
}

func declareKeysEndTxn(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	et := req.(*kvpb.EndTxnRequest)
	if err := declareKeysWriteTransaction(rs, header, req, latchSpans); err != nil {
		return err
	}
	var minTxnTS hlc.Timestamp
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		minTxnTS = header.Txn.MinTimestamp
		abortSpanAccess := spanset.SpanReadOnly
		if !et.Commit {
			// Rollback EndTxn requests may write to the abort span, either if
			// their Poison flag is set, in which case they will add an abort
			// span entry, or if their Poison flag is not set and an abort span
			// entry already exists on this Range, in which case they will clear
			// that entry.
			abortSpanAccess = spanset.SpanReadWrite
		}
		latchSpans.AddNonMVCC(abortSpanAccess, roachpb.Span{
			Key: keys.AbortSpanKey(rs.GetRangeID(), header.Txn.ID),
		})
	}

	// If the request is intending to finalize the transaction record then it
	// needs to declare a few extra keys.
	if !et.IsParallelCommit() {
		// All requests that intend on resolving local locks need to depend on
		// the range descriptor because they need to determine which locks are
		// within the local range.
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.RangeDescriptorKey(rs.GetStartKey()),
		})

		// The spans may extend beyond this Range, but it's ok for the
		// purpose of acquiring latches. The parts in our Range will
		// be resolved eagerly.
		for _, span := range et.LockSpans {
			latchSpans.AddMVCC(spanset.SpanReadWrite, span, minTxnTS)
		}

		if et.InternalCommitTrigger != nil {
			if st := et.InternalCommitTrigger.SplitTrigger; st != nil {
				// Splits may read from the entire pre-split range (they read
				// from the LHS in all cases, and the RHS only when the existing
				// stats contain estimates). Splits declare non-MVCC read access
				// across the entire LHS to block all concurrent writes to the
				// LHS because their stat deltas will interfere with the
				// non-delta stats computed as a part of the split. Splits
				// declare non-MVCC write access across the entire RHS to block
				// all concurrent reads and writes to the RHS because they will
				// fail if applied after the split. (see
				// https://github.com/cockroachdb/cockroach/issues/14881)
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key:    st.LeftDesc.StartKey.AsRawKey(),
					EndKey: st.LeftDesc.EndKey.AsRawKey(),
				})
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key:    st.RightDesc.StartKey.AsRawKey(),
					EndKey: st.RightDesc.EndKey.AsRawKey(),
				})
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key:    keys.MakeRangeKeyPrefix(st.LeftDesc.StartKey),
					EndKey: keys.MakeRangeKeyPrefix(st.RightDesc.EndKey).PrefixEnd(),
				})

				leftRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(rs.GetRangeID())
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key:    leftRangeIDPrefix,
					EndKey: leftRangeIDPrefix.PrefixEnd(),
				})
				rightRangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(st.RightDesc.RangeID)
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key:    rightRangeIDPrefix,
					EndKey: rightRangeIDPrefix.PrefixEnd(),
				})

				rightRangeIDUnreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(st.RightDesc.RangeID)
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key:    rightRangeIDUnreplicatedPrefix,
					EndKey: rightRangeIDUnreplicatedPrefix.PrefixEnd(),
				})

				// NB: the RHS LastReplicaGCTimestampKey, to which the LHS timestamp is
				// copied, is covered above by the SpanReadWrite for the entire RHS
				// unreplicated RangeID-local span.
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key: keys.RangeLastReplicaGCTimestampKey(st.LeftDesc.RangeID),
				})

				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key:    abortspan.MinKey(rs.GetRangeID()),
					EndKey: abortspan.MaxKey(rs.GetRangeID()),
				})

				// Protect range tombstones from collection by GC to avoid interference
				// with MVCCStats calculation.
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
				})
			}
			if mt := et.InternalCommitTrigger.MergeTrigger; mt != nil {
				// Merges copy over the RHS abort span to the LHS, and compute
				// replicated range ID stats over the RHS in the merge trigger.
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key:    abortspan.MinKey(mt.LeftDesc.RangeID),
					EndKey: abortspan.MaxKey(mt.LeftDesc.RangeID).PrefixEnd(),
				})
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key:    keys.MakeRangeIDReplicatedPrefix(mt.RightDesc.RangeID),
					EndKey: keys.MakeRangeIDReplicatedPrefix(mt.RightDesc.RangeID).PrefixEnd(),
				})
				// Merges incorporate the prior read summary from the RHS into
				// the LHS, which ensures that the current and all future
				// leaseholders on the joint range respect reads served on the
				// RHS.
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key: keys.RangePriorReadSummaryKey(mt.LeftDesc.RangeID),
				})
				// Merge will update GC hint if set, so we need to get a write latch
				// on the left side and we already have a read latch on RHS for
				// replicated keys.
				latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
					Key: keys.RangeGCHintKey(mt.LeftDesc.RangeID),
				})

				// Merges need to adjust MVCC stats for merged MVCC range tombstones
				// that straddle the ranges, by peeking to the left and right of the RHS
				// start key. Since Prevish() is imprecise, we must also ensure we don't
				// go outside of the LHS bounds.
				leftPeekBound := mt.RightDesc.StartKey.AsRawKey().Prevish(roachpb.PrevishKeyLength)
				rightPeekBound := mt.RightDesc.StartKey.AsRawKey().Next()
				if leftPeekBound.Compare(mt.LeftDesc.StartKey.AsRawKey()) < 0 {
					leftPeekBound = mt.LeftDesc.StartKey.AsRawKey()
				}
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key:    leftPeekBound,
					EndKey: rightPeekBound,
				})
				// Protect range tombstones from collection by GC to avoid interference
				// with MVCCStats calculation.
				latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
					Key: keys.MVCCRangeKeyGCKey(mt.LeftDesc.RangeID),
				})
			}
		}
	}
	return nil
}

// EndTxn either commits or aborts (rolls back) an extant transaction according
// to the args.Commit parameter. Rolling back an already rolled-back txn is ok.
// TODO(nvanbenschoten): rename this file to cmd_end_txn.go once some of andrei's
// recent PRs have landed.
func EndTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.EndTxnRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	reply := resp.(*kvpb.EndTxnResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING, roachpb.PREPARED, roachpb.STAGING, roachpb.ABORTED); err != nil {
		return result.Result{}, err
	}
	if args.Require1PC {
		// If a 1PC txn was required and we're in EndTxn, we've failed to evaluate
		// the batch as a 1PC. We shouldn't have gotten here; if we couldn't
		// evaluate as 1PC, evaluateWriteBatch() was supposed to short-circuit.
		return result.Result{}, errors.AssertionFailedf("unexpectedly trying to evaluate EndTxn with the Require1PC flag set")
	}
	if args.Commit && args.Poison {
		return result.Result{}, errors.AssertionFailedf("cannot poison during a committing EndTxn request")
	}
	if args.Prepare {
		if !args.Commit {
			return result.Result{}, errors.AssertionFailedf("cannot prepare a rollback")
		}
		if args.IsParallelCommit() {
			return result.Result{}, errors.AssertionFailedf("cannot prepare a parallel commit")
		}
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	// Fetch existing transaction.
	var existingTxn roachpb.Transaction
	log.VEventf(
		ctx, 2, "checking to see if transaction record already exists for txn: %s", h.Txn,
	)
	recordAlreadyExisted, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &existingTxn, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
	)
	if err != nil {
		return result.Result{}, err
	} else if !recordAlreadyExisted {
		log.VEvent(ctx, 2, "no existing txn record found")
		// No existing transaction record was found - create one by writing it
		// below in updateFinalizedTxn.
		reply.Txn = h.Txn.Clone()

		// Verify that it is safe to create the transaction record. We only need
		// to perform this verification for commits. Rollbacks can always write
		// an aborted txn record.
		if args.Commit {
			if err := CanCreateTxnRecord(ctx, cArgs.EvalCtx, reply.Txn); err != nil {
				log.VEventf(ctx, 2, "cannot create transaction record: %v", err)
				return result.Result{}, err
			}
		}
	} else {
		log.VEventf(ctx, 2, "existing transaction record found: %s", existingTxn)
		// We're using existingTxn on the reply, although it can be stale
		// compared to the Transaction in the request (e.g. the Sequence,
		// and various timestamps). We must be careful to update it with the
		// supplied ba.Txn if we return it with an error which might be
		// retried, as for example to avoid client-side serializable restart.
		reply.Txn = existingTxn.Clone()

		// Verify that we can either commit it or abort it (according
		// to args.Commit), and also that the Timestamp and Epoch have
		// not suffered regression.
		switch reply.Txn.Status {
		case roachpb.COMMITTED:
			// This can happen if the coordinator had left the transaction in the
			// implicitly committed state, and is now coming to clean it up. Someone
			// else must have performed the STAGING->COMMITTED transition in the
			// meantime. The TransactionStatusError is going to be handled by the
			// txnCommitter interceptor.
			log.VEventf(ctx, 2, "transaction found to be already committed")
			return result.Result{}, kvpb.NewTransactionStatusError(
				kvpb.TransactionStatusError_REASON_TXN_COMMITTED,
				"already committed")

		case roachpb.ABORTED:
			// The transaction has already been aborted by someone else.
			log.VEventf(
				ctx, 2, "transaction %s found to have be already aborted (by someone else)", reply.Txn,
			)
			if !args.Commit {
				// Do not return TransactionAbortedError since the client anyway
				// wanted to abort the transaction.
				resolvedLocks, _, externalLocks, err := resolveLocalLocks(ctx, readWriter, cArgs.EvalCtx, ms, args, reply.Txn)
				if err != nil {
					return result.Result{}, err
				}
				if err := updateFinalizedTxn(
					ctx, readWriter, cArgs.EvalCtx, ms, key, args, reply.Txn, recordAlreadyExisted, externalLocks,
				); err != nil {
					return result.Result{}, err
				}
				// Use alwaysReturn==true because the transaction is definitely
				// aborted, no matter what happens to this command.
				res := result.FromEndTxn(reply.Txn, true /* alwaysReturn */, args.Poison)
				res.Local.ResolvedLocks = resolvedLocks
				return res, nil
			}
			// If the transaction was previously aborted by a concurrent writer's
			// push, any intents written are still open. It's only now that we know
			// them, so we return them all for asynchronous resolution (we're
			// currently not able to write on error, but see #1989).
			//
			// Similarly to above, use alwaysReturn==true. The caller isn't trying
			// to abort, but the transaction is definitely aborted and its locks
			// can go.
			reply.Txn.LockSpans = args.LockSpans
			return result.FromEndTxn(reply.Txn, true /* alwaysReturn */, args.Poison),
				kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_ABORTED_RECORD_FOUND)

		case roachpb.PENDING:
			if h.Txn.Epoch < reply.Txn.Epoch {
				return result.Result{}, errors.AssertionFailedf(
					"programming error: epoch regression: %d", h.Txn.Epoch)
			}

		case roachpb.PREPARED:
			if h.Txn.Epoch != reply.Txn.Epoch {
				return result.Result{}, errors.AssertionFailedf(
					"programming error: epoch mismatch with prepared transaction: %d != %d", h.Txn.Epoch, reply.Txn.Epoch)
			}
			if args.IsParallelCommit() {
				return result.Result{}, errors.AssertionFailedf(
					"programming error: cannot parallel commit a prepared transaction")
			}

		case roachpb.STAGING:
			switch {
			case h.Txn.Epoch < reply.Txn.Epoch:
				return result.Result{}, errors.AssertionFailedf(
					"programming error: epoch regression: %d", h.Txn.Epoch)
			case h.Txn.Epoch == reply.Txn.Epoch:
				if args.Prepare {
					return result.Result{}, errors.AssertionFailedf(
						"programming error: cannot prepare a staging transaction")
				}
			case h.Txn.Epoch > reply.Txn.Epoch:
				// If the EndTxn carries a newer epoch than a STAGING txn record, we do
				// not consider the transaction to be performing a parallel commit and
				// potentially already implicitly committed because we know that the
				// transaction restarted since entering the STAGING state.
				log.VEventf(ctx, 2, "request with newer epoch %d than STAGING txn record; parallel commit must have failed", h.Txn.Epoch)
				reply.Txn.Status = roachpb.PENDING
			default:
				panic("unreachable")
			}

		default:
			return result.Result{}, errors.AssertionFailedf("bad txn status: %s", reply.Txn)
		}

		// Update the existing txn with the supplied txn.
		reply.Txn.Update(h.Txn)
	}

	// Attempt to commit or abort the transaction per the args.Commit parameter.
	if args.Commit {
		// Bump the transaction's provisional commit timestamp to account for any
		// transaction pushes, if necessary. See the state machine diagram in
		// Replica.CanCreateTxnRecord for details.
		switch {
		case !recordAlreadyExisted, existingTxn.Status == roachpb.PENDING:
			BumpToMinTxnCommitTS(ctx, cArgs.EvalCtx, reply.Txn)
		case existingTxn.Status == roachpb.PREPARED:
			// Don't check timestamp cache. The transaction could not have been pushed
			// while its record was in the PREPARED state. Furthermore, checking the
			// timestamp cache and increasing the commit timestamp at this point would
			// be incorrect, because the transaction must not fail to commit after
			// being prepared.
		case existingTxn.Status == roachpb.STAGING:
			// Don't check timestamp cache. The transaction could not have been pushed
			// while its record was in the STAGING state so checking is unnecessary.
			// Furthermore, checking the timestamp cache and increasing the commit
			// timestamp at this point would be incorrect, because the transaction may
			// have entered the implicit commit state.
		default:
			panic("unreachable")
		}

		// Determine whether the transaction's commit is successful or should
		// trigger a retry error.
		// NOTE: if the transaction is in the implicit commit state and this EndTxn
		// request is marking the commit as explicit, this check must succeed. We
		// assert this in txnCommitter.makeTxnCommitExplicitAsync.
		if retry, reason, extraMsg := IsEndTxnTriggeringRetryError(reply.Txn, args.Deadline); retry {
			return result.Result{}, kvpb.NewTransactionRetryError(reason, extraMsg)
		}

		// If the transaction is being prepared to commit, mark it as such. Do not
		// proceed to release locks or resolve intents.
		if args.Prepare {
			reply.Txn.Status = roachpb.PREPARED
			if err := updatePreparedTxn(ctx, readWriter, ms, key, args, reply.Txn); err != nil {
				return result.Result{}, err
			}
			return result.Result{}, nil
		}

		// If the transaction needs to be staged as part of an implicit commit
		// before being explicitly committed, write the staged transaction
		// record and return without running commit triggers or resolving local
		// locks.
		if args.IsParallelCommit() {
			// It's not clear how to combine transaction recovery with commit
			// triggers, so for now we don't allow them to mix. This shouldn't
			// cause any issues and the txn coordinator knows not to mix them.
			if ct := args.InternalCommitTrigger; ct != nil {
				err := errors.Errorf("cannot stage transaction with a commit trigger: %+v", ct)
				return result.Result{}, err
			}

			reply.Txn.Status = roachpb.STAGING
			reply.StagingTimestamp = reply.Txn.WriteTimestamp
			if err := updateStagingTxn(ctx, readWriter, ms, key, args, reply.Txn); err != nil {
				return result.Result{}, err
			}
			return result.Result{}, nil
		}

		// Else, the transaction can be explicitly committed.
		reply.Txn.Status = roachpb.COMMITTED
	} else {
		// If the transaction is STAGING, we can only move it to ABORTED if it is
		// *not* already implicitly committed. On the commit path, the transaction
		// coordinator is deliberate to only ever issue an EndTxn(commit) once the
		// transaction has reached an implicit commit state. However, on the
		// rollback path, the transaction coordinator does not make the opposite
		// guarantee that it will never issue an EndTxn(abort) once the transaction
		// has reached (or if it still could reach) an implicit commit state.
		//
		// As a result, on the rollback path, we don't trust the transaction's
		// coordinator to be an authoritative source of truth about whether the
		// transaction is implicitly committed. In other words, we don't consider
		// this EndTxn(abort) to be a claim that the transaction is not implicitly
		// committed. The transaction's coordinator may have just given up on the
		// transaction before it heard the outcome of a commit attempt. So in this
		// case, we return an IndeterminateCommitError to trigger the transaction
		// recovery protocol and transition the transaction record to a finalized
		// state (COMMITTED or ABORTED).
		//
		// Interestingly, because intents are not currently resolved until after an
		// implicitly committed transaction has been moved to an explicit commit
		// state (i.e. its record has moved from STAGING to COMMITTED), no other
		// transaction could see the effect of an implicitly committed transaction
		// that was erroneously rolled back. This means that such a mistake does not
		// actually compromise atomicity. Regardless, such a transition is confusing
		// and can cause errors in transaction recovery code. We would also like to
		// begin resolving intents earlier, while a transaction is still implicitly
		// committed. Doing so is only possible if we can guarantee that under no
		// circumstances can an implicitly committed transaction be rolled back.
		if reply.Txn.Status == roachpb.STAGING {
			// Note that reply.Txn has been updated with the Txn from the request
			// header. But, the transaction might have been pushed since it was
			// written. In fact, the transaction from the request header might
			// actually be in a state that _would have_ been implicitly committed IF
			// it had been able to write a transaction record with this new state. We
			// use the transaction record from disk to avoid erroneously attempting to
			// commit this transaction during recovery. Attempting to commit the
			// transaction based on the pushed timestamp would result in an assertion
			// failure.
			if !recordAlreadyExisted {
				return result.Result{}, errors.AssertionFailedf("programming error: transaction in STAGING without transaction record")
			}
			err := kvpb.NewIndeterminateCommitError(existingTxn)
			log.VEventf(ctx, 1, "%v", err)
			return result.Result{}, err
		}

		reply.Txn.Status = roachpb.ABORTED
	}

	// Resolve locks on the local range synchronously so that their resolution
	// ends up in the same Raft entry. There should always be at least one because
	// we position the transaction record next to the first lock acquired by a
	// transaction. This avoids the need for the intentResolver to have to return
	// to this range to resolve locks for this transaction in the future.
	// TODO(nvanbenschoten): clean up the handling of args and reply.Txn in these
	// functions. Ideally, only reply.Txn would be passed through and fields from
	// args would be extracted. This would help us re-use LockSpans from the txn
	// record when they're not provided in args.
	resolvedLocks, releasedReplLocks, externalLocks, err := resolveLocalLocks(
		ctx, readWriter, cArgs.EvalCtx, ms, args, reply.Txn)
	if err != nil {
		return result.Result{}, err
	}
	if err := updateFinalizedTxn(
		ctx, readWriter, cArgs.EvalCtx, ms, key, args, reply.Txn, recordAlreadyExisted, externalLocks,
	); err != nil {
		return result.Result{}, err
	}

	// Note: there's no need to clear the AbortSpan state if we've successfully
	// finalized a transaction, as there's no way in which an abort cache entry
	// could have been written (the txn would already have been in
	// state=ABORTED).
	//
	// Summary of transaction replay protection after EndTxn: When a
	// transactional write gets replayed over its own resolved intents, the
	// write will succeed but only as an intent with a newer timestamp (with a
	// WriteTooOldError). However, the replayed intent cannot be resolved by a
	// subsequent replay of this EndTxn call because the txn timestamp will be
	// too old. Replays of requests which attempt to create a new txn record
	// (HeartbeatTxn or EndTxn) never succeed because EndTxn inserts in the
	// timestamp cache in Replica's updateTimestampCache method, forcing
	// the call to CanCreateTxnRecord to return false, resulting in a
	// transaction retry error. If the replay didn't attempt to create a txn
	// record, any push will immediately succeed as a missing txn record on push
	// where CanCreateTxnRecord returns false succeeds. In both cases, the txn
	// will be GC'd on the slow path.
	//
	// We specify alwaysReturn==false because if the commit fails below Raft, we
	// don't want the locks to be up for resolution. That should happen only if
	// the commit actually happens; otherwise, we risk losing writes.
	txnResult := result.FromEndTxn(reply.Txn, false /* alwaysReturn */, args.Poison)
	txnResult.Local.UpdatedTxns = []*roachpb.Transaction{reply.Txn}
	txnResult.Local.ResolvedLocks = resolvedLocks

	if reply.Txn.Status == roachpb.COMMITTED {
		if len(releasedReplLocks) != 0 {
			// Return that local replicated {shared, exclusive} locks were released by
			// the committing transaction. If such locks were released, we still need
			// to make sure other transactions can't write underneath the
			// transaction's commit timestamp to the key spans previously protected by
			// the locks. We return the spans on the response and update the timestamp
			// cache a few layers above to ensure this.
			reply.ReplicatedLocalLocksReleasedOnCommit = releasedReplLocks
			log.VEventf(
				ctx, 2, "committed transaction released local replicated shared/exclusive locks",
			)
		}

		// Run the commit triggers if successfully committed.
		triggerResult, err := RunCommitTrigger(
			ctx, cArgs.EvalCtx, readWriter.(storage.Batch), ms, args, reply.Txn,
		)
		if err != nil {
			// Commit triggers might fail in a way the doesn't mean that the replica
			// is corrupted. In this case, we need to reset the reply to avoid
			// returning to the client that the txn is committed. If that happened,
			// the client throws an error due to a sanity check regarding a failed txn
			// shouldn't be committed.
			reply.Reset()
			return result.Result{}, err
		}
		if err := txnResult.MergeAndDestroy(triggerResult); err != nil {
			return result.Result{}, err
		}
	}

	return txnResult, nil
}

// IsEndTxnExceedingDeadline returns true if the transaction's provisional
// commit timestamp exceeded its deadline. If so, the transaction should not be
// allowed to commit.
func IsEndTxnExceedingDeadline(commitTS hlc.Timestamp, deadline hlc.Timestamp) bool {
	return !deadline.IsEmpty() && deadline.LessEq(commitTS)
}

// IsEndTxnTriggeringRetryError returns true if the Transaction cannot be
// committed and needs to return a TransactionRetryError. It also returns the
// reason and possibly an extra message to be used for the error.
func IsEndTxnTriggeringRetryError(
	txn *roachpb.Transaction, deadline hlc.Timestamp,
) (retry bool, reason kvpb.TransactionRetryReason, extraMsg redact.RedactableString) {
	if !txn.IsoLevel.ToleratesWriteSkew() && txn.WriteTimestamp != txn.ReadTimestamp {
		// Return a transaction retry error if the commit timestamp isn't equal to
		// the txn timestamp.
		return true, kvpb.RETRY_SERIALIZABLE, ""
	}
	if IsEndTxnExceedingDeadline(txn.WriteTimestamp, deadline) {
		// A transaction must obey its deadline, if set.
		exceededBy := txn.WriteTimestamp.GoTime().Sub(deadline.GoTime())
		extraMsg = redact.Sprintf(
			"txn timestamp pushed too much; deadline exceeded by %s (%s > %s)",
			exceededBy, txn.WriteTimestamp, deadline)
		return true, kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED, extraMsg
	}
	return false, 0, ""
}

const lockResolutionBatchSize = 500
const lockResolutionBatchByteSize = 4 << 20 // 4 MB.

// resolveLocalLocks synchronously resolves any locks that are local to this
// range in the same batch and returns those lock spans. The remainder are
// collected and returned so that they can be handed off to asynchronous
// processing. Note that there is a maximum lock resolution allowance of
// lockResolutionBatchSize meant to avoid creating a batch which is too large
// for Raft. Any local locks which exceed the allowance are treated as
// external and are resolved asynchronously with the external locks.
func resolveLocalLocks(
	ctx context.Context,
	readWriter storage.ReadWriter,
	evalCtx EvalContext,
	ms *enginepb.MVCCStats,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
) (resolvedLocks []roachpb.LockUpdate, releasedReplLocks, externalLocks []roachpb.Span, _ error) {
	var resolveAllowance int64 = lockResolutionBatchSize
	var targetBytes int64 = lockResolutionBatchByteSize
	if args.InternalCommitTrigger != nil {
		// If this is a system transaction (such as a split or merge), don't
		// enforce the resolve allowance. These transactions rely on having
		// their locks resolved synchronously.
		resolveAllowance = 0
		targetBytes = 0
	}
	return resolveLocalLocksWithPagination(ctx, readWriter, evalCtx, ms, args, txn, resolveAllowance, targetBytes)
}

// resolveLocalLocksWithPagination is resolveLocalLocks but with a max key and
// target bytes limit.
func resolveLocalLocksWithPagination(
	ctx context.Context,
	readWriter storage.ReadWriter,
	evalCtx EvalContext,
	ms *enginepb.MVCCStats,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
	maxKeys int64,
	targetBytes int64,
) (resolvedLocks []roachpb.LockUpdate, releasedReplLocks, externalLocks []roachpb.Span, _ error) {
	desc := evalCtx.Desc()
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which locks are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		desc = &mergeTrigger.LeftDesc
	}

	remainingLockSpans := args.LockSpans
	f := func(maxKeys, targetBytes int64) (numKeys int64, numBytes int64, resumeReason kvpb.ResumeReason, err error) {
		if len(remainingLockSpans) == 0 {
			return 0, 0, 0, iterutil.StopIteration()
		}
		span := remainingLockSpans[0]
		remainingLockSpans = remainingLockSpans[1:]
		update := roachpb.MakeLockUpdate(txn, span)
		if len(span.EndKey) == 0 {
			// For single-key lock updates, do a KeyAddress-aware check of
			// whether it's contained in our Range.
			if !kvserverbase.ContainsKey(desc, span.Key) {
				externalLocks = append(externalLocks, span)
				return 0, 0, 0, nil
			}
			// It may be tempting to reuse an iterator here, but this call
			// can create the iterator with Prefix:true which is much faster
			// than seeking -- especially for intents that are missing, e.g.
			// due to async intent resolution. See:
			// https://github.com/cockroachdb/cockroach/issues/64092
			//
			// Note that the underlying pebbleIterator will still be reused
			// since readWriter is a pebbleBatch in the typical case.
			ok, numBytes, resumeSpan, wereReplLocksReleased, err := storage.MVCCResolveWriteIntent(ctx, readWriter, ms, update,
				storage.MVCCResolveWriteIntentOptions{TargetBytes: targetBytes})
			if err != nil {
				return 0, 0, 0, errors.Wrapf(err, "resolving write intent at %s on end transaction [%s]", span, txn.Status)
			}
			if ok {
				numKeys = 1
			}
			if resumeSpan != nil {
				externalLocks = append(externalLocks, *resumeSpan)
				resumeReason = kvpb.RESUME_BYTE_LIMIT
			} else {
				// !ok && resumeSpan == nil is a valid condition that means
				// that no intent was found.
				resolvedLocks = append(resolvedLocks, update)
				// If requested, replace point tombstones with range tombstones.
				if ok && evalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes {
					if err := storage.ReplacePointTombstonesWithRangeTombstones(
						ctx, spanset.DisableUndeclaredSpanAssertions(readWriter),
						ms, update.Key, update.EndKey); err != nil {
						return 0, 0, 0, errors.Wrapf(err,
							"replacing point tombstones with range tombstones for write intent at %s on end transaction [%s]",
							span, txn.Status)
					}
				}
			}
			if wereReplLocksReleased {
				releasedReplLocks = append(releasedReplLocks, update.Span)
			}
			return numKeys, numBytes, resumeReason, nil
		}
		// For update ranges, cut into parts inside and outside our key
		// range. Resolve locally inside, delegate the rest. In particular,
		// an update range for range-local data is correctly considered local.
		inSpan, outSpans := kvserverbase.IntersectSpan(span, desc)
		externalLocks = append(externalLocks, outSpans...)
		if inSpan != nil {
			update.Span = *inSpan
			numKeys, numBytes, resumeSpan, resumeReason, wereReplLocksReleased, err :=
				storage.MVCCResolveWriteIntentRange(ctx, readWriter, ms, update,
					storage.MVCCResolveWriteIntentRangeOptions{MaxKeys: maxKeys, TargetBytes: targetBytes},
				)
			if err != nil {
				return 0, 0, 0, errors.Wrapf(err, "resolving write intent range at %s on end transaction [%s]", span, txn.Status)
			}
			if evalCtx.EvalKnobs().NumKeysEvaluatedForRangeIntentResolution != nil {
				atomic.AddInt64(evalCtx.EvalKnobs().NumKeysEvaluatedForRangeIntentResolution, numKeys)
			}
			if resumeSpan != nil {
				update.EndKey = resumeSpan.Key
				externalLocks = append(externalLocks, *resumeSpan)
			}
			resolvedLocks = append(resolvedLocks, update)
			// If requested, replace point tombstones with range tombstones.
			if evalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes {
				if err := storage.ReplacePointTombstonesWithRangeTombstones(
					ctx, spanset.DisableUndeclaredSpanAssertions(readWriter),
					ms, update.Key, update.EndKey); err != nil {
					return 0, 0, 0, errors.Wrapf(err,
						"replacing point tombstones with range tombstones for write intent range at %s on end transaction [%s]",
						span, txn.Status)
				}
			}
			if wereReplLocksReleased {
				releasedReplLocks = append(releasedReplLocks, update.Span)
			}
			return numKeys, numBytes, resumeReason, nil
		}
		return 0, 0, 0, nil
	}

	numKeys, _, _, err := storage.MVCCPaginate(ctx, maxKeys, targetBytes, false /* allowEmpty */, f)
	if err != nil {
		return nil, nil, nil, err
	}

	externalLocks = append(externalLocks, remainingLockSpans...)

	removedAny := numKeys > 0
	if WriteAbortSpanOnResolve(txn.Status, args.Poison, removedAny) {
		if err := UpdateAbortSpan(ctx, evalCtx, readWriter, ms, txn.TxnMeta, args.Poison); err != nil {
			return nil, nil, nil, err
		}
	}
	return resolvedLocks, releasedReplLocks, externalLocks, nil
}

// updatePreparedTxn persists the PREPARED transaction record with updated
// status (and possibly timestamp). It persists the record with all of the
// transaction's (local and remote) locks.
func updatePreparedTxn(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	key []byte,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
) error {
	txn.LockSpans = args.LockSpans
	txn.InFlightWrites = nil
	txnRecord := txn.AsRecord()
	return storage.MVCCPutProto(
		ctx, readWriter, key, hlc.Timestamp{}, &txnRecord,
		storage.MVCCWriteOptions{Stats: ms, Category: fs.BatchEvalReadCategory})
}

// updateStagingTxn persists the STAGING transaction record with updated status
// (and possibly timestamp). It persists the record with the EndTxn request's
// declared in-flight writes along with all of the transaction's (local and
// remote) locks.
func updateStagingTxn(
	ctx context.Context,
	readWriter storage.ReadWriter,
	ms *enginepb.MVCCStats,
	key []byte,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
) error {
	txn.LockSpans = args.LockSpans
	txn.InFlightWrites = args.InFlightWrites
	txnRecord := txn.AsRecord()
	return storage.MVCCPutProto(
		ctx, readWriter, key, hlc.Timestamp{}, &txnRecord,
		storage.MVCCWriteOptions{Stats: ms, Category: fs.BatchEvalReadCategory})
}

// updateFinalizedTxn persists the COMMITTED or ABORTED transaction record with
// updated status (and possibly timestamp). If we've already resolved all locks
// locally, we actually delete the record right away - no use in keeping it
// around.
func updateFinalizedTxn(
	ctx context.Context,
	readWriter storage.ReadWriter,
	evalCtx EvalContext,
	ms *enginepb.MVCCStats,
	key []byte,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
	recordAlreadyExisted bool,
	externalLocks []roachpb.Span,
) error {
	opts := storage.MVCCWriteOptions{Stats: ms, Category: fs.BatchEvalReadCategory}
	if !evalCtx.EvalKnobs().DisableTxnAutoGC && len(externalLocks) == 0 {
		if log.V(2) {
			log.KvExec.Infof(ctx, "auto-gc'ed %s (%d locks)", txn.Short(), len(args.LockSpans))
		}
		if !recordAlreadyExisted {
			// Nothing to delete, so there's no use writing a deletion tombstone. This
			// can help avoid sending a proposal through Raft, if nothing else in the
			// BatchRequest writes.
			return nil
		}
		_, _, err := storage.MVCCDelete(ctx, readWriter, key, hlc.Timestamp{}, opts)
		return err
	}
	txn.LockSpans = externalLocks
	txn.InFlightWrites = nil
	txnRecord := txn.AsRecord()
	return storage.MVCCPutProto(ctx, readWriter, key, hlc.Timestamp{}, &txnRecord, opts)
}

// RunCommitTrigger runs the commit trigger from an end transaction request.
func RunCommitTrigger(
	ctx context.Context,
	rec EvalContext,
	batch storage.Batch,
	ms *enginepb.MVCCStats,
	args *kvpb.EndTxnRequest,
	txn *roachpb.Transaction,
) (result.Result, error) {
	if fn := rec.EvalKnobs().CommitTriggerError; fn != nil {
		if err := fn(); err != nil {
			return result.Result{}, err
		}
	}
	ct := args.InternalCommitTrigger
	if ct == nil {
		return result.Result{}, nil
	}

	// The transaction is committing with a commit trigger. This means that it has
	// side-effects beyond those of the intents that it has written.

	// The transaction should not have a commit timestamp in the future of present
	// time. Such cases should be caught in maybeCommitWaitBeforeCommitTrigger
	// before getting here, which should sleep for long enough to ensure that the
	// local clock leads the commit timestamp. An error here may indicate that the
	// transaction's commit timestamp was bumped after it acquired latches.
	if rec.Clock().Now().Less(txn.WriteTimestamp) {
		return result.Result{}, errors.AssertionFailedf("txn %s with %s commit trigger needs "+
			"commit wait. Was its timestamp bumped after acquiring latches?", txn, ct.Kind())
	}

	// Used by both splits and merges.
	maybeWrapReplicaCorruptionError := func(ctx context.Context, err error) error {
		if err == nil {
			log.KvExec.Fatalf(ctx, "unexpected nil error")
		}
		if info := pebble.ExtractDataCorruptionInfo(err); info != nil {
			// Data corruption errors due to external SSTable references getting
			// deleted should not be wrapped in replica corruption errors. This
			// ensures that we simply fail the split or merge and propagate the error,
			// but don't crash the process. In such cases, an excise command should be
			// used to get out of this data corruption situation.
			return err
		}
		// Otherwise, fail the split or merge with a critical error that crashes the
		// process. Reporting a replica corruption error ensures this. See
		// setCorruptRaftMuLocked.
		return kvpb.MaybeWrapReplicaCorruptionError(ctx, err)
	}

	// Stage the commit trigger's side-effects so that they will go into effect on
	// each Replica when the corresponding Raft log entry is applied. Only one
	// commit trigger can be set.
	if ct.GetSplitTrigger() != nil {
		sl := MakeStateLoader(rec)
		lhsLease, err := sl.LoadLease(ctx, batch)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(
				ctx, errors.Wrap(err, "unable to load lease"),
			)
		}
		gcThreshold, err := sl.LoadGCThreshold(ctx, batch)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(
				ctx, errors.Wrap(err, "unable to load GCThreshold"),
			)
		}
		gcHint, err := sl.LoadGCHint(ctx, batch)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(
				ctx, errors.Wrap(err, "unable to load GCHint"),
			)
		}
		replicaVersion, err := sl.LoadVersion(ctx, batch)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(
				ctx, errors.Wrap(err, "unable to load replica version"),
			)
		}
		in := SplitTriggerHelperInput{
			LeftLease:      lhsLease,
			GCThreshold:    gcThreshold,
			GCHint:         gcHint,
			ReplicaVersion: replicaVersion,
		}

		newMS, res, err := splitTrigger(
			ctx, rec, batch, *ms, ct.SplitTrigger, in, txn.WriteTimestamp,
		)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(ctx, err)
		}
		*ms = newMS
		return res, nil
	}
	if mt := ct.GetMergeTrigger(); mt != nil {
		res, err := mergeTrigger(ctx, rec, batch, ms, mt, txn.WriteTimestamp)
		if err != nil {
			return result.Result{}, maybeWrapReplicaCorruptionError(ctx, err)
		}
		return res, nil
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
	if sbt := ct.GetStickyBitTrigger(); sbt != nil {
		newDesc := *rec.Desc()
		newDesc.StickyBit = sbt.StickyBit
		var res result.Result
		res.Replicated.State = &kvserverpb.ReplicaState{
			Desc: &newDesc,
		}
		return res, nil
	}

	log.KvExec.Fatalf(ctx, "unknown commit trigger: %+v", ct)
	return result.Result{}, nil
}

// splitTrigger is called on a successful commit of a transaction containing an
// AdminSplit operation. It copies the AbortSpan for the new range and
// recomputes stats for both the existing, left hand side (LHS) range and the
// right hand side (RHS) range. For performance it only computes the stats for
// one side of the range and infers the stats for the other side by subtracting
// from the original stats. The choice of which side to scan is controlled by a
// heuristic. This choice defaults to scanning the LHS stats and inferring the
// RHS because the split key computation performed by the splitQueue ensures
// that we do not create large LHS ranges. However, if the RHS's global keyspace
// is entirely empty, it is scanned first instead. An example where we expect
// this heuristic to choose the RHS is bulk ingestion, which often splits off
// empty ranges and benefits from scanning the empty RHS when computing stats.
// Regardless of the choice of which side to scan first, the optimization to
// infer the other side's stats is only possible if the stats are fully accurate
// (ContainsEstimates = 0). If they contain estimates, stats for both the LHS
// and RHS are computed.
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
// the EndTxn request. Like all transactions, the requests within the
// transaction are replicated via Raft, including the EndTxn request.
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
//	    Node A             Node B             Node C
//	----------------------------------------------------
//
// range 1   |                  |                  |
//
//	      |                  |                  |
//	 SplitTrigger            |                  |
//	      |             SplitTrigger            |
//	      |                  |             SplitTrigger
//	      |                  |                  |
//	----------------------------------------------------
//
// split finished on A, B and C |                  |
//
//	|                  |                  |
//
// range 2   |                  |                  |
//
//	| ---- MsgVote --> |                  |
//	| ---------------------- MsgVote ---> |
//
// But that ideal ordering is not guaranteed. The split is "finished" when two
// of the replicas have appended the end-txn request containing the
// SplitTrigger to their Raft log. The following scenario is possible:
//
//	    Node A             Node B             Node C
//	----------------------------------------------------
//
// range 1   |                  |                  |
//
//	      |                  |                  |
//	 SplitTrigger            |                  |
//	      |             SplitTrigger            |
//	      |                  |                  |
//	----------------------------------------------------
//
// split finished on A and B    |                  |
//
//	|                  |                  |
//
// range 2   |                  |                  |
//
//	| ---- MsgVote --> |                  |
//	| --------------------- MsgVote ---> ???
//	|                  |                  |
//	|                  |             SplitTrigger
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
	batch storage.Batch,
	bothDeltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	in SplitTriggerHelperInput,
	ts hlc.Timestamp,
) (enginepb.MVCCStats, result.Result, error) {
	desc := rec.Desc()
	if !bytes.Equal(desc.StartKey, split.LeftDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.RightDesc.EndKey) {
		return enginepb.MVCCStats{}, result.Result{}, errors.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.LeftDesc.StartKey, split.LeftDesc.EndKey,
			split.RightDesc.StartKey, split.RightDesc.EndKey, desc)
	}

	// Determine which side to scan first when computing the post-split stats. We
	// scan the left-hand side first unless the right side's global keyspace is
	// entirely empty. In cases where the range's stats do not already contain
	// estimates, only one side needs to be scanned.
	// TODO(nvanbenschoten): this is a simple heuristic. If we had a cheap way to
	// determine the relative sizes of the LHS and RHS, we could be more
	// sophisticated here and always choose to scan the cheaper side.
	emptyRHS, err := storage.MVCCIsSpanEmpty(ctx, batch, storage.MVCCIsSpanEmptyOptions{
		StartKey: split.RightDesc.StartKey.AsRawKey(),
		EndKey:   split.RightDesc.EndKey.AsRawKey(),
	})
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrapf(err,
			"unable to determine whether right hand side of split is empty")
	}

	// The intentInterleavingIterator doesn't like iterating over spans containing
	// both local and global keys. Here we only care about global keys.
	spanWithNoLocals := split.LeftDesc.KeySpan().AsRawSpanWithNoLocals()
	emptyLHS, err := storage.MVCCIsSpanEmpty(ctx, batch, storage.MVCCIsSpanEmptyOptions{
		StartKey: spanWithNoLocals.Key, EndKey: spanWithNoLocals.EndKey,
	})
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrapf(err,
			"unable to determine whether left hand side of split is empty")
	}

	rangeKeyDeltaMS, err := computeSplitRangeKeyStatsDelta(ctx, batch, split.LeftDesc, split.RightDesc)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err,
			"unable to compute range key stats delta for RHS")
	}

	// Retrieve MVCC Stats from the current batch instead of using stats from
	// execution context. Stats in the context could diverge from storage snapshot
	// of current request when lease extensions are applied. Lease expiration is
	// a special case that updates stats without obtaining latches and thus can
	// execute concurrently with splitTrigger. As a result we must not write
	// absolute stats values for LHS based on this value, always produce a delta
	// since underlying stats in storage could change. At the same time it is safe
	// to write absolute RHS side stats since we hold lock for values, and
	// "unprotected" lease key don't yet exist until this split operation creates
	// RHS replica.
	currentStats, err := MakeStateLoader(rec).LoadMVCCStats(ctx, batch)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err,
			"unable to fetch original range mvcc stats for split")
	}

	h := splitStatsHelperInput{
		AbsPreSplitBothStored:    currentStats,
		DeltaBatchEstimated:      bothDeltaMS,
		DeltaRangeKey:            rangeKeyDeltaMS,
		PreSplitLeftUser:         split.PreSplitLeftUserStats,
		PreSplitStats:            split.PreSplitStats,
		PostSplitScanLeftFn:      makeScanStatsFn(ctx, batch, ts, &split.LeftDesc, "left hand side", false /* excludeUserSpans */),
		PostSplitScanRightFn:     makeScanStatsFn(ctx, batch, ts, &split.RightDesc, "right hand side", false /* excludeUserSpans */),
		PostSplitScanLocalLeftFn: makeScanStatsFn(ctx, batch, ts, &split.LeftDesc, "local left hand side", true /* excludeUserSpans */),
		ScanRightFirst:           splitScansRightForStatsFirst || emptyRHS,
		LeftUserIsEmpty:          emptyLHS,
		RightUserIsEmpty:         emptyRHS,
		MaxCountDiff:             MaxMVCCStatCountDiff.Get(&rec.ClusterSettings().SV),
		MaxBytesDiff:             MaxMVCCStatBytesDiff.Get(&rec.ClusterSettings().SV),
		UseEstimatesBecauseExternalBytesArePresent: split.UseEstimatesBecauseExternalBytesArePresent,
	}
	return splitTriggerHelper(ctx, rec, batch, in, h, split, ts)
}

// TestingSplitTrigger is a wrapper around splitTrigger that is exported for
// testing purposes.
func TestingSplitTrigger(
	ctx context.Context,
	rec EvalContext,
	batch storage.Batch,
	bothDeltaMS enginepb.MVCCStats,
	split *roachpb.SplitTrigger,
	in SplitTriggerHelperInput,
	ts hlc.Timestamp,
) (enginepb.MVCCStats, result.Result, error) {
	return splitTrigger(ctx, rec, batch, bothDeltaMS, split, in, ts)
}

// splitScansRightForStatsFirst controls whether the left hand side or the right
// hand side of the split is scanned first on the leaseholder when evaluating
// the split trigger. In practice, the splitQueue wants to scan the left hand
// side because the split key computation ensures that we do not create large
// LHS ranges. However, to improve test coverage, we use a metamorphic value.
var splitScansRightForStatsFirst = metamorphic.ConstantWithTestBool(
	"split-scans-right-for-stats-first", false)

// DisableMetamorphicSplitScansRightForStatsFirst disables the
// splitScansRightForStatsFirst metamorphic bool for the duration of a test,
// resetting it at the end.
func DisableMetamorphicSplitScansRightForStatsFirst(t interface {
	Helper()
	Cleanup(func())
}) {
	t.Helper()
	if splitScansRightForStatsFirst {
		splitScansRightForStatsFirst = false
		t.Cleanup(func() {
			splitScansRightForStatsFirst = true
		})
	}
}

// makeScanStatsFn constructs a splitStatsScanFn for the provided post-split
// range descriptor which computes the range's statistics.
func makeScanStatsFn(
	ctx context.Context,
	reader storage.Reader,
	ts hlc.Timestamp,
	sideDesc *roachpb.RangeDescriptor,
	sideName string,
	excludeUserSpans bool,
) splitStatsScanFn {
	computeStatsFn := rditer.ComputeStatsForRange
	if excludeUserSpans {
		computeStatsFn = rditer.ComputeStatsForRangeExcludingUser
	}
	return func() (enginepb.MVCCStats, error) {
		sideMS, err := computeStatsFn(ctx, sideDesc, reader, fs.BatchEvalReadCategory, ts.WallTime)
		if err != nil {
			return enginepb.MVCCStats{}, errors.Wrapf(err,
				"unable to compute stats for %s range after split", sideName)
		}
		log.Eventf(ctx, "computed stats for %s range", sideName)
		return sideMS, nil
	}
}

// SplitTriggerHelperInput contains metadata needed by the RHS when running the
// splitTriggerHelper.
type SplitTriggerHelperInput struct {
	LeftLease      roachpb.Lease
	GCThreshold    *hlc.Timestamp
	GCHint         *roachpb.GCHint
	ReplicaVersion roachpb.Version
}

// splitTriggerHelper continues the work begun by splitTrigger, but has a
// reduced scope that has all stats-related concerns bundled into a
// splitStatsHelper.
//
// TODO(arul): consider having this function write keys to the batch in sorted
// order, much like how destroyReplicaImpl does.
func splitTriggerHelper(
	ctx context.Context,
	rec EvalContext,
	batch storage.Batch,
	in SplitTriggerHelperInput,
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
	// MVCCBlindPutProto.
	replicaGCTS, err := rec.GetLastReplicaGCTimestamp(ctx)
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to fetch last replica GC timestamp")
	}

	if err := storage.MVCCBlindPutProto(
		ctx, spanset.DisableForbiddenSpanAssertions(batch),
		keys.RangeLastReplicaGCTimestampKey(split.RightDesc.RangeID), hlc.Timestamp{},
		&replicaGCTS, storage.MVCCWriteOptions{Category: fs.BatchEvalReadCategory}); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to copy last replica GC timestamp")
	}

	// Compute the absolute stats for the (post-split) ranges. No more
	// modifications to the left hand side are allowed after this line and any
	// modifications to the right hand side are accounted for by updating the
	// helper's AbsPostSplitRight() reference.
	var h splitStatsHelper
	// There are a few conditions under which we want to fall back to accurate
	// stats computation:
	// 1. There are no pre-computed stats for the LHS. This can happen if
	// kv.split.estimated_mvcc_stats.enabled is disabled, or if the leaseholder
	// node is running an older version. Pre-computed stats are necessary for
	// makeEstimatedSplitStatsHelper to estimate the stats.
	// Note that PreSplitLeftUserStats can also be equal to enginepb.MVCCStats{}
	// when the user LHS stats are all zero, but in that case it's ok to fall back
	// to accurate stats computation because scanning the empty LHS is not
	// expensive.
	noPreComputedStats := split.PreSplitLeftUserStats == enginepb.MVCCStats{}
	// 2. This is a manual split. Manual splits issued via AdminSplit are used in
	// bulk operations, like import, and tests to split many ranges out of the
	// same original range. Pre-computing the LHS user stats for each of these
	// ranges concurrently causes CPU spikes and split slowness; issuing repeated
	// RecomputeStats requests for the same range contributes even more and can
	// cause contention on the range descriptor.
	manualSplit := split.ManualSplit
	// 3. If either side contains no user data; scanning the empty ranges is
	// cheap.
	emptyLeftOrRight := statsInput.LeftUserIsEmpty || statsInput.RightUserIsEmpty
	// 4. If the user pre-split stats differ significantly from the current stats
	// stored on disk. Note that the current stats on disk were corrected in
	// AdminSplit, so any differences we see here are due to writes concurrent
	// with this split (not compounded estimates from previous splits).
	preComputedStatsDiff := !statsInput.AbsPreSplitBothStored.HasUserDataCloseTo(
		statsInput.PreSplitStats, statsInput.MaxCountDiff, statsInput.MaxBytesDiff)
	// 5. If we haven't been asked to use estimated stats because of
	// external bytes being present in the underlying store. This should
	// only be true when an online restore has recently been performed.
	shouldUseCrudeEstimates := statsInput.UseEstimatesBecauseExternalBytesArePresent &&
		statsInput.AbsPreSplitBothStored.ContainsEstimates > 0

	computeAccurateStats := (noPreComputedStats || manualSplit || emptyLeftOrRight || preComputedStatsDiff)
	computeAccurateStats = computeAccurateStats && !shouldUseCrudeEstimates
	if computeAccurateStats {
		var reason redact.RedactableString
		if noPreComputedStats {
			reason = "there are no pre-split LHS stats (or they're empty)"
		} else if manualSplit {
			reason = "this is a manual split"
		} else if emptyLeftOrRight {
			reason = "the in-split LHS or RHS is empty"
		} else {
			reason = redact.Sprintf("the pre-split user stats differ too much "+
				"from the in-split stats; pre-split: %+v, in-split: %+v",
				statsInput.PreSplitStats, statsInput.AbsPreSplitBothStored)
		}
		log.KvDistribution.Infof(ctx, "falling back to accurate stats computation because %v", reason)
		h, err = makeSplitStatsHelper(statsInput)
	} else if statsInput.UseEstimatesBecauseExternalBytesArePresent {
		h, err = makeCrudelyEstimatedSplitStatsHelper(statsInput)
	} else {
		h, err = makeEstimatedSplitStatsHelper(statsInput)
	}
	if err != nil {
		return enginepb.MVCCStats{}, result.Result{}, err
	}

	// Initialize the RHS range's AbortSpan by copying the LHS's.
	if err := rec.AbortSpan().CopyTo(
		ctx, batch, batch, h.AbsPostSplitRight(), ts, split.RightDesc.RangeID,
		gc.TxnCleanupThreshold.Get(&rec.ClusterSettings().SV),
	); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, err
	}

	// Copy the last consistency checker run timestamp from the LHS to the RHS.
	// This avoids running the consistency checker on the RHS immediately after
	// the split.
	lastTS := hlc.Timestamp{}
	// TODO(arul): instead of fetching the consistency checker timestamp here
	// like this, we should instead pass it using the SplitTriggerHelperInput to
	// make it easier to test.
	if _, err := storage.MVCCGetProto(ctx, batch,
		keys.QueueLastProcessedKey(split.LeftDesc.StartKey, "consistencyChecker"),
		hlc.Timestamp{}, &lastTS, storage.MVCCGetOptions{}); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err,
			"unable to fetch the last consistency checker run for LHS")
	}

	if err := storage.MVCCPutProto(ctx, batch,
		keys.QueueLastProcessedKey(split.RightDesc.StartKey, "consistencyChecker"),
		hlc.Timestamp{}, &lastTS,
		storage.MVCCWriteOptions{Stats: h.AbsPostSplitRight(), Category: fs.BatchEvalReadCategory}); err != nil {
		return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err,
			"unable to copy the last consistency checker run to RHS")
	}

	// Note: we don't copy the queue last processed times. This means
	// we'll process the RHS range in consistency and time series
	// maintenance queues again possibly sooner than if we copied. The
	// lock is to limit post-raft logic.

	// Now that we've computed the stats for the RHS so far, we persist them.
	// This looks a bit more complicated than it really is: updating the stats
	// also changes the stats, and we write not only the stats but a complete
	// initial state. Additionally, since bothDeltaMS is tracking writes to
	// both sides, we need to update it as well.
	{
		// Various pieces of code rely on a replica's lease never being uninitialized,
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
		if in.LeftLease.Empty() {
			log.KvExec.Fatalf(ctx, "LHS of split has no lease")
		}

		// Copy the lease from the left-hand side of the split over to the
		// right-hand side so that it can immediately start serving requests.
		// When doing so, we need to make a few modifications.
		rightLease := in.LeftLease
		// Rebind the lease to the existing leaseholder store's replica from the
		// right-hand side's descriptor.
		var ok bool
		rightLease.Replica, ok = split.RightDesc.GetReplicaDescriptor(in.LeftLease.Replica.StoreID)
		if !ok {
			return enginepb.MVCCStats{}, result.Result{}, errors.Errorf(
				"pre-split lease holder %+v not found in post-split descriptor %+v",
				in.LeftLease.Replica, split.RightDesc,
			)
		}
		// Convert leader leases into expiration-based leases. A leader lease is
		// tied to a specific raft leadership term within a specific raft group.
		// During a range split, we initialize a new raft group on the right-hand
		// side, so a leader lease term from the left-hand side is unusable. Once
		// the right-hand side elects a leader and collocates the lease and leader,
		// it can promote the expiration-based lease back to a leader lease.
		if rightLease.Type() == roachpb.LeaseLeader {
			exp := rec.Clock().Now().Add(int64(rec.GetRangeLeaseDuration()), 0)
			rightLease.Expiration = &exp
			rightLease.Term = 0
			rightLease.MinExpiration = hlc.Timestamp{}
		}
		if in.GCThreshold.IsEmpty() {
			log.VEventf(ctx, 1, "LHS's GCThreshold of split is not set")
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
		if *h.AbsPostSplitRight(), err = kvstorage.WriteInitialReplicaState(
			ctx, batch, *h.AbsPostSplitRight(), split.RightDesc, rightLease,
			*in.GCThreshold, *in.GCHint, in.ReplicaVersion,
		); err != nil {
			return enginepb.MVCCStats{}, result.Result{}, errors.Wrap(err, "unable to write initial Replica state")
		}
	}

	var pd result.Result
	pd.Replicated.Split = &kvserverpb.Split{
		SplitTrigger: *split,
		// NB: the RHSDelta is identical to the stats for the newly created right
		// hand side range (i.e. it goes from zero to its stats).
		RHSDelta: *h.AbsPostSplitRight(),
	}
	// Set DoTimelyApplicationToAllReplicas since splits that are not applied on
	// all replicas eventually cause snapshots for the RHS to be sent to
	// replicas that already have the unsplit range, *and* these snapshots are
	// rejected (which is very wasteful). See the long comment in
	// split_delay_helper.go for more details.
	pd.Replicated.DoTimelyApplicationToAllReplicas = true

	pd.Local.Metrics = &result.Metrics{
		SplitsWithEstimatedStats:     h.splitsWithEstimates,
		SplitEstimatedTotalBytesDiff: h.estimatedTotalBytesDiff,
	}
	deltaPostSplitLeft := h.DeltaPostSplitLeft()
	return deltaPostSplitLeft, pd, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge transaction.
// It calculates stats for the LHS by merging in RHS stats, and copies over the
// abort span entries from the RHS.
func mergeTrigger(
	ctx context.Context,
	rec EvalContext,
	batch storage.Batch,
	ms *enginepb.MVCCStats,
	merge *roachpb.MergeTrigger,
	ts hlc.Timestamp,
) (result.Result, error) {
	desc := rec.Desc()
	if !bytes.Equal(desc.StartKey, merge.LeftDesc.StartKey) {
		return result.Result{}, errors.AssertionFailedf("LHS range start keys do not match: %s != %s",
			desc.StartKey, merge.LeftDesc.StartKey)
	}
	if !desc.EndKey.Less(merge.LeftDesc.EndKey) {
		return result.Result{}, errors.AssertionFailedf("original LHS end key is not less than the post merge end key: %s >= %s",
			desc.EndKey, merge.LeftDesc.EndKey)
	}

	if err := abortspan.New(merge.RightDesc.RangeID).CopyTo(
		ctx, batch, batch, ms, ts, merge.LeftDesc.RangeID,
		gc.TxnCleanupThreshold.Get(&rec.ClusterSettings().SV),
	); err != nil {
		return result.Result{}, err
	}

	// If we collected a read summary from the right-hand side when freezing it,
	// merge that summary into the left-hand side's prior read summary. In the
	// usual case, the RightReadSummary in the MergeTrigger will be used to
	// update the left-hand side's leaseholder's timestamp cache when applying
	// the merge trigger's Raft log entry. However, if the left-hand side's
	// leaseholder hears about the merge through a Raft snapshot, the merge
	// trigger will not be available, so it will need to use the range's prior
	// read summary to update its timestamp cache to ensure that it does not
	// serve any writes that invalidate previous reads served on the right-hand
	// side range. See TestStoreRangeMergeTimestampCache for an example of where
	// this behavior is necessary.
	//
	// This communication from the RHS to the LHS is handled differently from
	// how we copy over the abortspan. In this case, the read summary is passed
	// through the SubsumeResponse and into the MergeTrigger. In the abortspan's
	// case, we read from local RHS replica (which may not be the leaseholder)
	// directly in this method. The primary reason why these are different is
	// because the RHS's persistent read summary may not be up-to-date, as it is
	// not updated by the SubsumeRequest.
	if merge.RightReadSummary != nil {
		mergedSum := merge.RightReadSummary.Clone()
		if priorSum, err := readsummary.Load(ctx, batch, rec.GetRangeID()); err != nil {
			return result.Result{}, err
		} else if priorSum != nil {
			mergedSum.Merge(*priorSum)
		}
		// Compress the persisted read summary, as it will likely never be needed.
		mergedSum.Compress(0)
		if err := readsummary.Set(ctx, batch, rec.GetRangeID(), ms, mergedSum); err != nil {
			return result.Result{}, err
		}
	}

	// The stats for the merged range are the sum of the LHS and RHS stats
	// adjusted for range key merges (which is the inverse of the split
	// adjustment).
	ms.Add(merge.RightMVCCStats)
	msRangeKeyDelta, err := computeSplitRangeKeyStatsDelta(ctx, batch, merge.LeftDesc, merge.RightDesc)
	if err != nil {
		return result.Result{}, err
	}
	ms.Subtract(msRangeKeyDelta)

	// The RHS's replicated range ID stats are subtracted -- the only replicated
	// range ID keys we copy from the RHS are the keys in the abort span, and
	// we've already accounted for those stats above.
	//
	// NB: RangeIDLocalMVCCStats is introduced in 23.2 to mitigate a SysBytes race
	// with lease requests (which ignore latches). For 23.1 compatibility, we fall
	// back to computing it here when not set. We don't need a version gate since
	// it's only used at evaluation time and doesn't affect below-Raft state.
	if merge.RightRangeIDLocalMVCCStats != (enginepb.MVCCStats{}) {
		ms.Subtract(merge.RightRangeIDLocalMVCCStats)
	}

	var pd result.Result
	pd.Replicated.Merge = &kvserverpb.Merge{
		MergeTrigger: *merge,
	}
	// Set DoTimelyApplicationToAllReplicas so that merges are applied on all
	// replicas. This is not technically necessary since even though
	// Replica.AdminMerge calls waitForApplication, that call happens earlier in
	// the merge distributed txn, when sending a kvpb.SubsumeRequest. But since
	// we have force-flushed once during the merge txn anyway, we choose to
	// complete the merge story and finish the merge on all replicas.
	pd.Replicated.DoTimelyApplicationToAllReplicas = true

	{
		// If we have GC hints populated that means we are trying to perform
		// optimized garbage removal in future.
		// We will try to merge both hints if possible and set new hint on LHS.
		lhsLoader := MakeStateLoader(rec)
		lhsHint, err := lhsLoader.LoadGCHint(ctx, batch)
		if err != nil {
			return result.Result{}, err
		}
		rhsLoader := kvstorage.MakeStateLoader(merge.RightDesc.RangeID)
		rhsHint, err := rhsLoader.LoadGCHint(ctx, batch)
		if err != nil {
			return result.Result{}, err
		}
		if lhsHint.Merge(rhsHint, rec.GetMVCCStats().HasNoUserData(), merge.RightMVCCStats.HasNoUserData()) {
			if err := lhsLoader.SetGCHint(ctx, batch, ms, lhsHint); err != nil {
				return result.Result{}, err
			}
			pd.Replicated.State = &kvserverpb.ReplicaState{
				GCHint: lhsHint,
			}
		}
	}
	return pd, nil
}

func changeReplicasTrigger(
	_ context.Context, rec EvalContext, _ storage.Batch, change *roachpb.ChangeReplicasTrigger,
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

	pd.Replicated.State = &kvserverpb.ReplicaState{
		Desc: change.Desc,
	}
	pd.Replicated.ChangeReplicas = &kvserverpb.ChangeReplicas{
		ChangeReplicasTrigger: *change,
	}

	return pd
}

// computeSplitRangeKeyStatsDelta computes the delta in MVCCStats caused by
// the splitting of range keys that straddle the range split point. The inverse
// applies during range merges. Consider a range key [a-foo)@1 split at cc:
//
// Before: [a-foo)@1  RangeKeyCount=1 RangeKeyBytes=15
// LHS:    [a-cc)@1   RangeKeyCount=1 RangeKeyBytes=14
// RHS:    [cc-foo)@1 RangeKeyCount=1 RangeKeyBytes=16
//
// If the LHS is computed directly then the RHS is calculated as:
//
// RHS = Before - LHS = RangeKeyCount=0 RangeKeyBytes=1
//
// This is clearly incorrect. This function determines the delta such that:
//
// RHS = Before - LHS + Delta = RangeKeyCount=1 RangeKeyBytes=16
//
// This is equivalent to the contribution of the range key fragmentation, since
// the stats computations are commutative. This can also be used to compute the
// merge contribution, which is the inverse of the fragmentation, since the
// range keys will already have been merged in Pebble by the time this is
// called.
func computeSplitRangeKeyStatsDelta(
	ctx context.Context, r storage.Reader, lhs, rhs roachpb.RangeDescriptor,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats

	// We construct the tightest possible bounds around the split point, but make
	// sure to stay within the Raft ranges since Prevish() is imprecise.
	splitKey := rhs.StartKey.AsRawKey()
	leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
		splitKey.Prevish(roachpb.PrevishKeyLength), splitKey.Next(),
		lhs.StartKey.AsRawKey(), rhs.EndKey.AsRawKey())

	iter, err := r.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		KeyTypes:     storage.IterKeyTypeRangesOnly,
		LowerBound:   leftPeekBound,
		UpperBound:   rightPeekBound,
		ReadCategory: fs.BatchEvalReadCategory,
	})
	if err != nil {
		return ms, err
	}
	defer iter.Close()

	if cmp, rangeKeys, err := storage.PeekRangeKeysRight(iter, splitKey); err != nil {
		return enginepb.MVCCStats{}, err
	} else if cmp < 0 {
		ms.Add(storage.UpdateStatsOnRangeKeySplit(splitKey, rangeKeys.Versions))
	}

	return ms, nil
}
