// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// optimizePuts searches for contiguous runs of Put & CPut commands in
// the supplied request union. Any run which exceeds a minimum length
// threshold employs a full order iterator to determine whether the
// range of keys being written is empty. If so, then the run can be
// set to put "blindly", meaning no iterator need be used to read
// existing values during the MVCC write.
// The caller should use the returned slice (which is either equal to
// the input slice, or has been shallow-copied appropriately to avoid
// mutating the original requests).
func optimizePuts(
	reader storage.Reader, origReqs []roachpb.RequestUnion, distinctSpans bool,
) []roachpb.RequestUnion {
	var minKey, maxKey roachpb.Key
	var unique map[string]struct{}
	if !distinctSpans {
		unique = make(map[string]struct{}, len(origReqs))
	}
	// Returns false on occurrence of a duplicate key.
	maybeAddPut := func(key roachpb.Key) bool {
		// Note that casting the byte slice key to a string does not allocate.
		if unique != nil {
			if _, ok := unique[string(key)]; ok {
				return false
			}
			unique[string(key)] = struct{}{}
		}
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
		return true
	}

	firstUnoptimizedIndex := len(origReqs)
	for i, r := range origReqs {
		switch t := r.GetInner().(type) {
		case *roachpb.PutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.ConditionalPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.InitPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		}
		firstUnoptimizedIndex = i
		break
	}

	if firstUnoptimizedIndex < optimizePutThreshold { // don't bother if below this threshold
		return origReqs
	}
	// iter is being used to find the parts of the key range that is empty. We
	// don't need to see intents for this purpose since intents also have
	// provisional values that we will see.
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		KeyTypes: storage.IterKeyTypePointsAndRanges,
		// We want to include maxKey in our scan. Since UpperBound is exclusive, we
		// need to set it to the key after maxKey.
		UpperBound: maxKey.Next(),
	})
	defer iter.Close()

	// If there are enough puts in the run to justify calling seek,
	// we can determine whether any part of the range being written
	// is "virgin" and set the puts to write blindly.
	// Find the first non-empty key in the run.
	iter.SeekGE(storage.MakeMVCCMetadataKey(minKey))
	var iterKey roachpb.Key
	if ok, err := iter.Valid(); err != nil {
		// TODO(bdarnell): return an error here instead of silently
		// running without the optimization?
		log.Errorf(context.TODO(), "Seek returned error; disabling blind-put optimization: %+v", err)
		return origReqs
	} else if ok && bytes.Compare(iter.Key().Key, maxKey) <= 0 {
		iterKey = iter.Key().Key
	}
	// Set the prefix of the run which is being written to virgin
	// keyspace to "blindly" put values.
	reqs := append([]roachpb.RequestUnion(nil), origReqs...)
	for i := range reqs[:firstUnoptimizedIndex] {
		inner := reqs[i].GetInner()
		if iterKey == nil || bytes.Compare(iterKey, inner.Header().Key) > 0 {
			switch t := inner.(type) {
			case *roachpb.PutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.ConditionalPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.InitPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			default:
				log.Fatalf(context.TODO(), "unexpected non-put request: %s", t)
			}
		}
	}
	return reqs
}

// evaluateBatch evaluates a batch request by splitting it up into its
// individual commands, passing them to evaluateCommand, and combining
// the results.
func evaluateBatch(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	readWriter storage.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba *roachpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
	evalPath batchEvalPath,
) (_ *roachpb.BatchResponse, _ result.Result, retErr *roachpb.Error) {
	defer func() {
		// Ensure that errors don't carry the WriteTooOld flag set. The client
		// handles non-error responses with the WriteTooOld flag set, and errors
		// with this flag set confuse it.
		if retErr != nil && retErr.GetTxn() != nil {
			retErr.GetTxn().WriteTooOld = false
		}
	}()

	// NB: Don't mutate BatchRequest directly.
	baReqs := ba.Requests

	// baHeader is the header passed to evaluate each command in the batch.
	// After each command, it is updated with the result and used again for
	// the next batch. At the end of evaluation, it is used to populate the
	// response header (timestamp, txn, etc).
	baHeader := ba.Header

	br := ba.CreateReply()

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(baReqs) >= optimizePutThreshold && evalPath == readWrite {
		baReqs = optimizePuts(readWriter, baReqs, baHeader.DistinctSpans)
	}

	// Create a clone of the transaction to store the new txn state produced on
	// the return/error path.
	if baHeader.Txn != nil {
		baHeader.Txn = baHeader.Txn.Clone()

		// Check whether this transaction has been aborted, if applicable. This
		// applies to reads and writes once a transaction that has begun to
		// acquire locks (see #2231 for more about why we check for aborted
		// transactions on reads). Note that 1PC transactions have had their
		// transaction field cleared by this point so we do not execute this
		// check in that case.
		if baHeader.Txn.IsLocking() {
			// We don't check the abort span for a couple of special requests:
			// - if the request is asking to abort the transaction, then don't check the
			// AbortSpan; we don't want the request to be rejected if the transaction
			// has already been aborted.
			// - heartbeats don't check the abort span. If the txn is aborted, they'll
			// return an aborted proto in their otherwise successful response.
			// TODO(nvanbenschoten): Let's remove heartbeats from this allowlist when
			// we rationalize the TODO in txnHeartbeater.heartbeat.
			if !ba.IsSingleAbortTxnRequest() && !ba.IsSingleHeartbeatTxnRequest() {
				if pErr := checkIfTxnAborted(ctx, rec, readWriter, *baHeader.Txn); pErr != nil {
					return nil, result.Result{}, pErr
				}
			}
		}
	}

	var mergedResult result.Result

	// WriteTooOldErrors have particular handling. When a request encounters the
	// error, we'd like to lay down an intent in order to avoid writers being
	// starved. So, for blind writes, we swallow the error and instead we set the
	// WriteTooOld flag on the response. For non-blind writes (e.g. CPut), we
	// can't do that and so we just return the WriteTooOldError - see note on
	// IsRead() stanza below. Upon receiving either a WriteTooOldError or a
	// response with the WriteTooOld flag set, the client will attempt to bump
	// the txn's read timestamp through a refresh. If successful, the client
	// will retry this batch (in both cases).
	//
	// In any case, evaluation of the current batch always continue after a
	// WriteTooOldError in order to find out if there's more conflicts and chose
	// a final write timestamp.
	var writeTooOldState struct {
		err *roachpb.WriteTooOldError
		// cantDeferWTOE is set when a WriteTooOldError cannot be deferred past the
		// end of the current batch.
		cantDeferWTOE bool
	}

	// TODO(tbg): if we introduced an "executor" helper here that could carry state
	// across the slots in the batch while we execute them, this code could come
	// out a lot less ad-hoc.
	for index, union := range baReqs {
		// Execute the command.
		args := union.GetInner()

		if baHeader.Txn != nil {
			// Set the Request's sequence number on the TxnMeta for this
			// request. The MVCC layer (currently) uses TxnMeta to
			// pass input arguments, such as the seqnum at which a
			// request operates.
			baHeader.Txn.Sequence = args.Header().Sequence
		}

		// If a unittest filter was installed, check for an injected error; otherwise, continue.
		if filter := rec.EvalKnobs().TestingEvalFilter; filter != nil {
			filterArgs := kvserverbase.FilterArgs{
				Ctx:     ctx,
				CmdID:   idKey,
				Index:   index,
				Sid:     rec.StoreID(),
				Req:     args,
				Version: rec.ClusterSettings().Version.ActiveVersionOrEmpty(ctx).Version,
				Hdr:     baHeader,
			}
			if pErr := filter(filterArgs); pErr != nil {
				if pErr.GetTxn() == nil {
					pErr.SetTxn(baHeader.Txn)
				}
				log.Infof(ctx, "test injecting error: %s", pErr)
				return nil, result.Result{}, pErr
			}
		}

		reply := br.Responses[index].GetInner()

		if seq := args.Header().KVNemesisSeq.Get(); seq != 0 {
			ctx = kvnemesisutil.WithSeq(ctx, seq)
		}

		// Note that `reply` is populated even when an error is returned: it
		// may carry a response transaction and in the case of WriteTooOldError
		// (which is sometimes deferred) it is fully populated.
		curResult, err := evaluateCommand(
			ctx, readWriter, rec, ms, baHeader, args, reply, g, st, ui, evalPath,
		)

		if filter := rec.EvalKnobs().TestingPostEvalFilter; filter != nil {
			filterArgs := kvserverbase.FilterArgs{
				Ctx:   ctx,
				CmdID: idKey,
				Index: index,
				Sid:   rec.StoreID(),
				Req:   args,
				Hdr:   baHeader,
				Err:   err,
			}
			if pErr := filter(filterArgs); pErr != nil {
				if pErr.GetTxn() == nil {
					pErr.SetTxn(baHeader.Txn)
				}
				log.Infof(ctx, "test injecting error: %s", pErr)
				return nil, result.Result{}, pErr
			}
		}

		// If this request is transactional, we now have potentially two
		// transactions floating around: the one on baHeader.Txn (always
		// there in a txn) and possibly a newer version in `reply.Header().Txn`.
		// Absorb the update into baHeader.Txn and write back a nil Transaction
		// to the header to ensure that there is only one version going forward.
		if headerCopy := reply.Header(); baHeader.Txn != nil && headerCopy.Txn != nil {
			baHeader.Txn.Update(headerCopy.Txn)
			headerCopy.Txn = nil
			reply.SetHeader(headerCopy)
		}

		if err != nil {
			// If an EndTxn wants to restart because of a write too old, we
			// might have a better error to return to the client.
			if retErr := (*roachpb.TransactionRetryError)(nil); errors.As(err, &retErr) &&
				retErr.Reason == roachpb.RETRY_WRITE_TOO_OLD &&
				args.Method() == roachpb.EndTxn && writeTooOldState.err != nil {
				err = writeTooOldState.err
				// Don't defer this error. We could perhaps rely on the client observing
				// the WriteTooOld flag and retry the batch, but we choose not too.
				writeTooOldState.cantDeferWTOE = true
			} else if wtoErr := (*roachpb.WriteTooOldError)(nil); errors.As(err, &wtoErr) {
				// We got a WriteTooOldError. We continue on to run all
				// commands in the batch in order to determine the highest
				// timestamp for more efficient retries. If the batch is
				// transactional, we continue to lay down intents so that
				// other concurrent overlapping transactions are forced
				// through intent resolution and the chances of this batch
				// succeeding when it will be retried are increased.
				if writeTooOldState.err != nil {
					writeTooOldState.err.ActualTimestamp.Forward(
						wtoErr.ActualTimestamp)
				} else {
					writeTooOldState.err = wtoErr
				}

				// For read-write requests that observe key-value state, we don't have
				// the option of leaving an intent behind when they encounter a
				// WriteTooOldError, so we have to return an error instead of a response
				// with the WriteTooOld flag set (which would also leave intents
				// behind). These requests need to be re-evaluated at the bumped
				// timestamp in order for their results to be valid. The current
				// evaluation resulted in an result that could well be different from
				// what the request would return if it were evaluated at the bumped
				// timestamp, which would cause the request to be rejected if it were
				// sent again with the same sequence number after a refresh.
				//
				// Similarly, for read-only requests that encounter a WriteTooOldError,
				// we don't have the option of returning a response with the WriteTooOld
				// flag set because a response is not even generated in tandem with the
				// WriteTooOldError. We could fix this and then allow WriteTooOldErrors
				// to be deferred in these cases, but doing so would buy more into the
				// extremely error-prone approach of retuning responses and errors
				// together throughout the MVCC read path. Doing so is not desirable as
				// it has repeatedly caused bugs in the past. Instead, we'd like to get
				// rid of this pattern entirely and instead address the TODO below.
				//
				// TODO(andrei): What we really want to do here is either speculatively
				// evaluate the request at the bumped timestamp and return that
				// speculative result, or leave behind an unreplicated lock that won't
				// prevent the request for evaluating again at the same sequence number
				// but at a bumped timestamp.
				if !roachpb.IsBlindWrite(args) {
					writeTooOldState.cantDeferWTOE = true
				}

				if baHeader.Txn != nil {
					log.VEventf(ctx, 2, "setting WriteTooOld because of key: %s. wts: %s -> %s",
						args.Header().Key, baHeader.Txn.WriteTimestamp, wtoErr.ActualTimestamp)
					baHeader.Txn.WriteTimestamp.Forward(wtoErr.ActualTimestamp)
					baHeader.Txn.WriteTooOld = true
				} else {
					// For non-transactional requests, there's nowhere to defer the error
					// to. And the request has to fail because non-transactional batches
					// should read and write at the same timestamp.
					writeTooOldState.cantDeferWTOE = true
				}

				// Clear error; we're done processing the WTOE for now and we'll return
				// to considering it below after we've evaluated all requests.
				err = nil
			}
		}

		// Even on error, we need to propagate the result of evaluation.
		//
		// TODO(tbg): find out if that's true and why and improve the comment.
		if err := mergedResult.MergeAndDestroy(curResult); err != nil {
			log.Fatalf(
				ctx,
				"unable to absorb Result: %s\ndiff(new, old): %s",
				err, pretty.Diff(curResult, mergedResult),
			)
		}

		if err != nil {
			pErr := roachpb.NewErrorWithTxn(err, baHeader.Txn)
			// Initialize the error index.
			pErr.SetErrorIndex(int32(index))

			return nil, mergedResult, pErr
		}

		// If the last request was carried out with a limit, subtract the number
		// of results from the limit going forward. Exhausting the limit results
		// in a limit of -1. This makes sure that we still execute the rest of
		// the batch, but with limit-aware operations returning no data.
		h := reply.Header()
		if limit, retResults := baHeader.MaxSpanRequestKeys, h.NumKeys; limit != 0 && retResults > 0 {
			if retResults < limit {
				baHeader.MaxSpanRequestKeys -= retResults
			} else {
				// The limit was either exceeded (which is allowed in some cases
				// like when WholeRowsOfSize is set and AllowEmpty is false) or
				// was exactly used up, so drop to -1 instead of zero (which
				// would mean "no limit").
				baHeader.MaxSpanRequestKeys = -1
			}
		}
		// Same as for MaxSpanRequestKeys above, keep track of the limit and make
		// sure to fall through to -1 instead of hitting zero (which means no
		// limit). We have to check the ResumeReason as well, since e.g. a Scan
		// response may not include the value that pushed it across the limit.
		if baHeader.TargetBytes > 0 {
			if h.ResumeReason == roachpb.RESUME_BYTE_LIMIT {
				baHeader.TargetBytes = -1
			} else if baHeader.TargetBytes > h.NumBytes {
				baHeader.TargetBytes -= h.NumBytes
			} else {
				baHeader.TargetBytes = -1
			}
		}
	}

	// If we made it here, there was no error during evaluation, with the exception of
	// a deferred WTOE. If it can't be deferred - return it now; otherwise it is swallowed.
	// Note that we don't attach an Index to the returned Error.
	//
	// TODO(tbg): we could attach the index of the first WriteTooOldError seen, but does
	// that buy us anything?
	if writeTooOldState.cantDeferWTOE {
		// NB: we can't do any error wrapping here yet due to compatibility with 20.2 nodes;
		// there needs to be an ErrorDetail here.
		return nil, mergedResult, roachpb.NewErrorWithTxn(writeTooOldState.err, baHeader.Txn)
	}

	// The batch evaluation will not return an error (i.e. either everything went
	// fine or we're deferring a WriteTooOldError by having bumped
	// baHeader.Txn.WriteTimestamp).

	// Update the batch response timestamp field to the timestamp at which the
	// batch's reads were evaluated.
	if baHeader.Txn != nil {
		// If transactional, send out the final transaction entry with the reply.
		br.Txn = baHeader.Txn
		// Note that br.Txn.ReadTimestamp might be higher than baHeader.Timestamp if
		// we had an EndTxn that decided that it can refresh to something higher
		// than baHeader.Timestamp because there were no refresh spans.
		if br.Txn.ReadTimestamp.Less(baHeader.Timestamp) {
			log.Fatalf(ctx, "br.Txn.ReadTimestamp < ba.Timestamp (%s < %s). ba: %s",
				br.Txn.ReadTimestamp, baHeader.Timestamp, ba)
		}
		br.Timestamp = br.Txn.ReadTimestamp
	} else {
		br.Timestamp = baHeader.Timestamp
	}

	return br, mergedResult, nil
}

// evaluateCommand delegates to the eval method for the given
// roachpb.Request. The returned Result may be partially valid
// even if an error is returned. maxKeys is the number of scan results
// remaining for this batch (MaxInt64 for no limit).
func evaluateCommand(
	ctx context.Context,
	readWriter storage.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.Request,
	reply roachpb.Response,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
	evalPath batchEvalPath,
) (result.Result, error) {
	var err error
	var pd result.Result

	if cmd, ok := batcheval.LookupCommand(args.Method()); ok {
		var now hlc.ClockTimestamp
		if st != nil {
			now = st.Now
		}
		cArgs := batcheval.CommandArgs{
			EvalCtx:               rec,
			Header:                h,
			Args:                  args,
			Now:                   now,
			Stats:                 ms,
			Concurrency:           g,
			Uncertainty:           ui,
			DontInterleaveIntents: evalPath == readOnlyWithoutInterleavedIntents,
		}

		if cmd.EvalRW != nil {
			pd, err = cmd.EvalRW(ctx, readWriter, cArgs, reply)
		} else {
			pd, err = cmd.EvalRO(ctx, readWriter, cArgs, reply)
		}
	} else {
		return result.Result{}, errors.Errorf("unrecognized command %s", args.Method())
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		trunc := func(s string) string {
			const maxLen = 256
			if len(s) > maxLen {
				return s[:maxLen-3] + "..."
			}
			return s
		}
		var resp string
		if reply.Size() > 1024 /* 1kb */ {
			// Avoid printing the entire response before truncating.
			header := reply.Header()
			resp = trunc(header.String())
		} else {
			resp = trunc(reply.String())
		}
		log.VEventf(ctx, 2, "evaluated %s command %s, txn=%v : resp=%s, err=%v",
			args.Method(), trunc(args.String()), h.Txn, resp, err)
	}
	return pd, err
}

// canDoServersideRetry looks at the error produced by evaluating ba (or the
// WriteTooOldFlag in br.Txn if there's no error) and decides if it's possible
// to retry the batch evaluation at a higher timestamp. Retrying is sometimes
// possible in case of some retriable errors which ask for higher timestamps:
// for transactional requests, retrying is possible if the transaction had not
// performed any prior reads that need refreshing.
//
// This function is called both below and above latching, which is indicated by
// the concurrency guard argument. The concurrency guard, if not nil, indicates
// that the caller is holding latches and cannot adjust its timestamp beyond the
// limits of what is protected by those latches. If the concurrency guard is
// nil, the caller indicates that it is not holding latches and can therefore
// more freely adjust its timestamp because it will re-acquire latches at
// whatever timestamp the batch is bumped to.
//
// deadline, if not nil, specifies the highest timestamp (exclusive) at which
// the request can be evaluated. If ba is a transactional request, then deadline
// cannot be specified; a transaction's deadline comes from it's EndTxn request.
//
// If true is returned, ba and ba.Txn will have been updated with the new
// timestamp.
func canDoServersideRetry(
	ctx context.Context,
	pErr *roachpb.Error,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	g *concurrency.Guard,
	deadline hlc.Timestamp,
) bool {
	if ba.Txn != nil {
		if !ba.CanForwardReadTimestamp {
			return false
		}
		if !deadline.IsEmpty() {
			log.Fatal(ctx, "deadline passed for transactional request")
		}
		if etArg, ok := ba.GetArg(roachpb.EndTxn); ok {
			et := etArg.(*roachpb.EndTxnRequest)
			deadline = et.Deadline
		}
	}

	var newTimestamp hlc.Timestamp
	if ba.Txn != nil {
		if pErr != nil {
			var ok bool
			ok, newTimestamp = roachpb.TransactionRefreshTimestamp(pErr)
			if !ok {
				return false
			}
		} else {
			if !br.Txn.WriteTooOld {
				log.Fatalf(ctx, "expected the WriteTooOld flag to be set")
			}
			newTimestamp = br.Txn.WriteTimestamp
		}
	} else {
		if pErr == nil {
			log.Fatalf(ctx, "canDoServersideRetry called for non-txn request without error")
		}
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.WriteTooOldError:
			newTimestamp = tErr.RetryTimestamp()

		case *roachpb.ReadWithinUncertaintyIntervalError:
			newTimestamp = tErr.RetryTimestamp()

		default:
			return false
		}
	}

	if batcheval.IsEndTxnExceedingDeadline(newTimestamp, deadline) {
		return false
	}
	return tryBumpBatchTimestamp(ctx, ba, g, newTimestamp)
}

// canReadOnlyRequestDropLatchesBeforeEval determines whether the batch request
// can potentially resolve its conflicts upfront (by scanning just the lock
// table first), bump the ts cache, release latches and then proceed with
// evaluation. Only non-locking read requests that aren't being evaluated under
// the `OptimisticEval` path are eligible for this optimization.
func canReadOnlyRequestDropLatchesBeforeEval(ba *roachpb.BatchRequest, g *concurrency.Guard) bool {
	if g == nil {
		// NB: A nil guard indicates that the caller is not holding latches.
		return false
	}
	switch ba.Header.ReadConsistency {
	case roachpb.CONSISTENT:
	// TODO(aayush): INCONSISTENT and READ_UNCOMMITTED reads do not care about
	// resolving lock conflicts at all. Yet, they can still drop latches early and
	// evaluate once they've pinned their pebble engine state. We should consider
	// supporting this by letting these kinds of requests drop latches early while
	// also skipping the initial validation step of scanning the lock table.
	case roachpb.INCONSISTENT, roachpb.READ_UNCOMMITTED:
		return false
	default:
		panic(fmt.Sprintf("unexpected ReadConsistency: %s", ba.Header.ReadConsistency))
	}
	switch g.EvalKind {
	case concurrency.PessimisticEval, concurrency.PessimisticAfterFailedOptimisticEval:
	case concurrency.OptimisticEval:
		// Requests going through the optimistic path are not allowed to drop their
		// latches before evaluation since we do not know upfront the extent to
		// which they will end up reading, and thus we cannot determine how much of
		// the timestamp cache to update.
		return false
	default:
		panic(fmt.Sprintf("unexpected EvalKind: %v", g.EvalKind))
	}
	// Only non-locking reads are eligible. This is because requests that need to
	// lock the keys that they end up reading need to be isolated against other
	// conflicting requests during their execution. Thus, they cannot release
	// their latches before evaluation.
	if ba.IsLocking() {
		return false
	}
	switch ba.WaitPolicy {
	case lock.WaitPolicy_Block, lock.WaitPolicy_Error:
	case lock.WaitPolicy_SkipLocked:
		// SkipLocked requests should only bump the timestamp cache over the keys
		// that they actually ended up reading, and not the keys they ended up
		// skipping over. Thus, they are not allowed to drop their latches before
		// evaluation.
		return false
	default:
		panic(fmt.Sprintf("unexpected WaitPolicy: %s", ba.WaitPolicy))
	}
	// We allow all non-locking, pessimistically evaluating read requests to try
	// and resolve their conflicts upfront.
	for _, req := range ba.Requests {
		inner := req.GetInner()
		switch inner.(type) {
		case *roachpb.ExportRequest, *roachpb.GetRequest, *roachpb.ScanRequest, *roachpb.ReverseScanRequest:
		default:
			return false
		}
	}
	return true
}
