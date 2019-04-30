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

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// executeWriteBatch is the entry point for client requests which may mutate the
// range's replicated state. Requests taking this path are evaluated and ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Concretely,
//
// - Latches for the keys affected by the command are acquired (i.e.
//   tracked as in-flight mutations).
// - In doing so, we wait until no overlapping mutations are in flight.
// - The timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - A RaftCommand is constructed. If proposer-evaluated KV is active,
//   the request is evaluated and the Result is placed in the
//   RaftCommand. If not, the request itself is added to the command.
// - The proposal is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - The result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, its latches are released, and
//   its result (which could be an error) is returned to the client.
//
// Returns exactly one of a response, an error or re-evaluation reason.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	startTime := timeutil.Now()

	if err := r.maybeBackpressureWriteBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	spans, err := r.collectSpans(&ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	var endCmds *endCmds
	if !ba.IsLeaseRequest() {
		// Acquire latches to prevent overlapping commands from executing until
		// this command completes. Note that this must be done before getting
		// the max timestamp for the key(s), as timestamp cache is only updated
		// after preceding commands have been run to successful completion.
		log.Event(ctx, "acquire latches")
		var err error
		endCmds, err = r.beginCmds(ctx, &ba, spans)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	// Guarantee we release the latches that we just acquired. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		if endCmds != nil {
			endCmds.done(br, pErr)
		}
	}()

	var lease roachpb.Lease
	var status storagepb.LeaseStatus
	// For lease commands, use the provided previous lease for verification.
	if ba.IsSingleSkipLeaseCheckRequest() {
		lease = ba.GetPrevLeaseForLeaseRequest()
	} else {
		// Other write commands require that this replica has the range
		// lease.
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr
		}
		lease = status.Lease
	}
	r.limitTxnMaxTimestamp(ctx, &ba, status)

	minTS, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	defer untrack(ctx, 0, 0, 0) // covers all error returns below

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if bumped, pErr := r.applyTimestampCache(ctx, &ba, minTS); pErr != nil {
		return nil, pErr
	} else if bumped {
		// If we bump the transaction's timestamp, we must absolutely
		// tell the client in a response transaction (for otherwise it
		// doesn't know about the incremented timestamp). Response
		// transactions are set far away from this code, but at the time
		// of writing, they always seem to be set. Since that is a
		// likely target of future micro-optimization, this assertion is
		// meant to protect against future correctness anomalies.
		defer func() {
			if br != nil && ba.Txn != nil && br.Txn == nil {
				log.Fatalf(ctx, "assertion failed: transaction updated by "+
					"timestamp cache, but transaction returned in response; "+
					"updated timestamp would have been lost (recovered): "+
					"%s in batch %s", ba.Txn, ba,
				)
			}
		}()
	}

	log.Event(ctx, "applied timestamp cache")

	ch, tryAbandon, maxLeaseIndex, pErr := r.evalAndPropose(ctx, lease, ba, endCmds, spans)
	if pErr != nil {
		if maxLeaseIndex != 0 {
			log.Fatalf(
				ctx, "unexpected max lease index %d assigned to failed proposal: %s, error %s",
				maxLeaseIndex, ba, pErr,
			)
		}
		return nil, pErr
	}
	// A max lease index of zero is returned when no proposal was made or a lease was proposed.
	// In both cases, we don't need to communicate a MLAI. Furthermore, for lease proposals we
	// cannot communicate under the lease's epoch. Instead the code calls EmitMLAI explicitly
	// as a side effect of stepping up as leaseholder.
	if maxLeaseIndex != 0 {
		untrack(ctx, ctpb.Epoch(lease.Epoch), r.RangeID, ctpb.LAI(maxLeaseIndex))
	}

	// After the command is proposed to Raft, invoking endCmds.done is now the
	// responsibility of processRaftCommand.
	endCmds = nil

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	tBegin := timeutil.Now()

	for {
		select {
		case propResult := <-ch:
			// Semi-synchronously process any intents that need resolving here in
			// order to apply back pressure on the client which generated them. The
			// resolution is semi-synchronous in that there is a limited number of
			// outstanding asynchronous resolution tasks allowed after which
			// further calls will block.
			if len(propResult.Intents) > 0 {
				// TODO(peter): Re-proposed and canceled (but executed) commands can
				// both leave intents to GC that don't hit this code path. No good
				// solution presents itself at the moment and such intents will be
				// resolved on reads.
				if err := r.store.intentResolver.CleanupIntentsAsync(ctx, propResult.Intents, true /* allowSync */); err != nil {
					log.Warning(ctx, err)
				}
			}
			if len(propResult.EndTxns) > 0 {
				if err := r.store.intentResolver.CleanupTxnIntentsAsync(ctx, r.RangeID, propResult.EndTxns, true /* allowSync */); err != nil {
					log.Warning(ctx, err)
				}
			}
			return propResult.Reply, propResult.Err
		case <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx, `have been waiting %.2fs for proposing command %s.
This range is likely unavailable.
Please submit this message at

  https://github.com/cockroachdb/cockroach/issues/new/choose

along with

	https://yourhost:8080/#/reports/range/%d

and the following Raft status: %+v`,
				timeutil.Since(tBegin).Seconds(),
				ba,
				r.RangeID,
				r.RaftStatus(),
			)
			r.store.metrics.SlowRaftRequests.Inc(1)
			defer func() {
				r.store.metrics.SlowRaftRequests.Dec(1)
				log.Infof(
					ctx,
					"slow command %s finished after %.2fs with error %v",
					ba,
					timeutil.Since(tBegin).Seconds(),
					pErr,
				)
			}()

		case <-ctxDone:
			// If our context was canceled, return an AmbiguousResultError
			// if the command isn't already being executed and using our
			// context, in which case we expect it to finish soon. The
			// AmbiguousResultError indicates to caller that the command may
			// have executed.
			if tryAbandon() {
				log.VEventf(ctx, 2, "context cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error()))
			}
			ctxDone = nil
		case <-shouldQuiesce:
			// If shutting down, return an AmbiguousResultError if the
			// command isn't already being executed and using our context,
			// in which case we expect it to finish soon. AmbiguousResultError
			// indicates to caller that the command may have executed. If
			// tryAbandon fails, we iterate through the loop again to wait
			// for the command to finish.
			if tryAbandon() {
				log.VEventf(ctx, 2, "shutdown cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown"))
			}
			shouldQuiesce = nil
		}
	}
}

// evaluateWriteBatch evaluates the supplied batch.
//
// If the batch is transactional and has all the hallmarks of a 1PC
// commit (i.e. includes BeginTransaction & EndTransaction, and
// there's nothing to suggest that the transaction will require retry
// or restart), the batch's txn is stripped and it's executed as an
// atomic batch write. If the writes cannot all be completed at the
// intended timestamp, the batch's txn is restored and it's
// re-executed in full. This allows it to lay down intents and return
// an appropriate retryable error.
func (r *Replica) evaluateWriteBatch(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *spanset.SpanSet,
) (engine.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	ms := enginepb.MVCCStats{}
	// If not transactional or there are indications that the batch's txn will
	// require restart or retry, execute as normal.
	if isOnePhaseCommit(ba, r.store.TestingKnobs()) {
		_, hasBegin := ba.GetArg(roachpb.BeginTransaction)
		arg, _ := ba.GetArg(roachpb.EndTransaction)
		etArg := arg.(*roachpb.EndTransactionRequest)

		// Try executing with transaction stripped. We use the transaction timestamp
		// to write any values as it may have been advanced by the timestamp cache.
		strippedBa := ba
		strippedBa.Timestamp = strippedBa.Txn.Timestamp
		strippedBa.Txn = nil
		if hasBegin {
			strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs
		} else {
			strippedBa.Requests = ba.Requests[:len(ba.Requests)-1] // strip end txn req
		}

		// If there were no refreshable spans earlier in the txn
		// (e.g. earlier gets or scans), then the batch can be retried
		// locally in the event of write too old errors.
		retryLocally := etArg.NoRefreshSpans && !ba.Txn.OrigTimestampWasObserved

		// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
		rec := NewReplicaEvalContext(r, spans)
		batch, br, res, pErr := r.evaluateWriteBatchWithLocalRetries(
			ctx, idKey, rec, &ms, strippedBa, spans, retryLocally,
		)
		if pErr == nil && (ba.Timestamp == br.Timestamp ||
			(retryLocally && !batcheval.IsEndTransactionExceedingDeadline(br.Timestamp, *etArg))) {
			clonedTxn := ba.Txn.Clone()
			clonedTxn.Status = roachpb.COMMITTED
			// Make sure the returned txn has the actual commit
			// timestamp. This can be different if the stripped batch was
			// executed at the server's hlc now timestamp.
			clonedTxn.Timestamp = br.Timestamp

			// If the end transaction is not committed, clear the batch and mark the status aborted.
			if !etArg.Commit {
				clonedTxn.Status = roachpb.ABORTED
				batch.Close()
				batch = r.store.Engine().NewBatch()
				ms = enginepb.MVCCStats{}
			} else {
				// Run commit trigger manually.
				innerResult, err := batcheval.RunCommitTrigger(ctx, rec, batch, &ms, *etArg, clonedTxn)
				if err != nil {
					return batch, ms, br, res, roachpb.NewErrorf("failed to run commit trigger: %s", err)
				}
				if err := res.MergeAndDestroy(innerResult); err != nil {
					return batch, ms, br, res, roachpb.NewError(err)
				}
			}

			br.Txn = clonedTxn
			// Add placeholder responses for begin & end transaction requests.
			var resps []roachpb.ResponseUnion
			if hasBegin {
				resps = make([]roachpb.ResponseUnion, len(br.Responses)+2)
				resps[0].MustSetInner(&roachpb.BeginTransactionResponse{})
				copy(resps[1:], br.Responses)
			} else {
				resps = append(br.Responses, roachpb.ResponseUnion{})
			}
			resps[len(resps)-1].MustSetInner(&roachpb.EndTransactionResponse{OnePhaseCommit: true})
			br.Responses = resps
			return batch, ms, br, res, nil
		}

		ms = enginepb.MVCCStats{}

		// Handle the case of a required one phase commit transaction.
		if etArg.Require1PC {
			if pErr != nil {
				return batch, ms, nil, result.Result{}, pErr
			} else if ba.Timestamp != br.Timestamp {
				err := roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN, "Require1PC batch pushed")
				return batch, ms, nil, result.Result{}, roachpb.NewError(err)
			}
			log.Fatal(ctx, "unreachable")
		}

		batch.Close()
		log.VEventf(ctx, 2, "1PC execution failed, reverting to regular execution for batch")
	}

	rec := NewReplicaEvalContext(r, spans)
	// We can retry locally if this is a non-transactional request.
	canRetry := ba.Txn == nil
	batch, br, res, pErr := r.evaluateWriteBatchWithLocalRetries(ctx, idKey, rec, &ms, ba, spans, canRetry)
	return batch, ms, br, res, pErr
}

// evaluateWriteBatchWithLocalRetries invokes evaluateBatch and
// retries in the event of a WriteTooOldError at a higher timestamp if
// canRetry is true.
func (r *Replica) evaluateWriteBatchWithLocalRetries(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
	spans *spanset.SpanSet,
	canRetry bool,
) (batch engine.Batch, br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	for retries := 0; ; retries++ {
		if batch != nil {
			batch.Close()
		}
		batch = r.store.Engine().NewBatch()
		var opLogger *engine.OpLoggerBatch
		if RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
			// TODO(nvanbenschoten): once we get rid of the RangefeedEnabled
			// cluster setting we'll need a way to turn this on when any
			// replica (not just the leaseholder) wants it and off when no
			// replicas want it. This turns out to be pretty involved.
			//
			// The current plan is to:
			// - create a range-id local key that stores all replicas that are
			//   subscribed to logical operations, along with their corresponding
			//   liveness epoch.
			// - create a new command that adds or subtracts replicas from this
			//   structure. The command will be a write across the entire replica
			//   span so that it is serialized with all writes.
			// - each replica will add itself to this set when it first needs
			//   logical ops. It will then wait until it sees the replicated command
			//   that added itself pop out through Raft so that it knows all
			//   commands that are missing logical ops are gone.
			// - It will then proceed as normal, relying on the logical ops to
			//   always be included on the raft commands. When its no longer
			//   needs logical ops, it will remove itself from the set.
			// - The leaseholder will have a new queue to detect registered
			//   replicas that are no longer live and remove them from the
			//   set to prevent "leaking" subscriptions.
			// - The condition here to add logical logging will be:
			//     if len(replicaState.logicalOpsSubs) > 0 { ... }
			//
			// An alternative to this is the reduce the cost of the including
			// the logical op log to a negligible amount such that it can be
			// included on all raft commands, regardless of whether any replica
			// has a rangefeed running or not.
			//
			// Another alternative is to make the setting table/zone-scoped
			// instead of a fine-grained per-replica state.
			opLogger = engine.NewOpLoggerBatch(batch)
			batch = opLogger
		}
		if util.RaceEnabled {
			batch = spanset.NewBatch(batch, spans)
		}

		br, res, pErr = evaluateBatch(ctx, idKey, batch, rec, ms, ba, false /* readOnly */)
		// If we can retry, set a higher batch timestamp and continue.
		if wtoErr, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); ok && canRetry {
			// Allow one retry only; a non-txn batch containing overlapping
			// spans will always experience WriteTooOldError.
			if retries == 1 {
				break
			}
			ba.Timestamp = wtoErr.ActualTimestamp
			continue
		}
		if opLogger != nil {
			res.LogicalOpLog = &storagepb.LogicalOpLog{
				Ops: opLogger.LogicalOps(),
			}
		}
		break
	}
	return
}

// isOnePhaseCommit returns true iff the BatchRequest contains all commands in
// the transaction, starting with BeginTransaction and ending with
// EndTransaction. One phase commits are disallowed if (1) the transaction has
// already been flagged with a write too old error, or (2) if isolation is
// serializable and the commit timestamp has been forwarded, or (3) the
// transaction exceeded its deadline, or (4) the testing knobs disallow optional
// one phase commits and the BatchRequest does not require one phase commit.
func isOnePhaseCommit(ba roachpb.BatchRequest, knobs *StoreTestingKnobs) bool {
	if ba.Txn == nil {
		return false
	}
	if !ba.IsCompleteTransaction() {
		return false
	}
	arg, _ := ba.GetArg(roachpb.EndTransaction)
	etArg := arg.(*roachpb.EndTransactionRequest)
	if batcheval.IsEndTransactionExceedingDeadline(ba.Txn.Timestamp, *etArg) {
		return false
	}
	if retry, _, _ := batcheval.IsEndTransactionTriggeringRetryError(ba.Txn, *etArg); retry {
		return false
	}
	return !knobs.DisableOptional1PC || etArg.Require1PC
}
