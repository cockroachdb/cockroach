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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"golang.org/x/time/rate"
)

var sentryIssue46720Limiter = rate.NewLimiter(0.1, 1) // 1 every 10s

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
	iter := reader.NewIterator(storage.IterOptions{
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
	readOnly bool,
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
	baHeader := ba.Header
	br := ba.CreateReply()

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(baReqs) >= optimizePutThreshold && !readOnly {
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

		// Note that responses are populated even when an error is returned.
		// TODO(tschottdorf): Change that. IIRC there is nontrivial use of it currently.
		reply := br.Responses[index].GetInner()

		var curResult result.Result
		var pErr *roachpb.Error

		curResult, pErr = evaluateCommand(
			ctx, idKey, index, readWriter, rec, ms, baHeader, args, reply)

		// If an EndTxn wants to restart because of a write too old, we
		// might have a better error to return to the client.
		retErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError)
		if ok && retErr.Reason == roachpb.RETRY_WRITE_TOO_OLD &&
			args.Method() == roachpb.EndTxn && writeTooOldState.err != nil {
			pErr.SetDetail(writeTooOldState.err)
			// Don't defer this error. We could perhaps rely on the client observing
			// the WriteTooOld flag and retry the batch, but we choose not too.
			writeTooOldState.cantDeferWTOE = true
		}

		if err := mergedResult.MergeAndDestroy(curResult); err != nil {
			// TODO(tschottdorf): see whether we really need to pass nontrivial
			// Result up on error and if so, formalize that.
			log.Fatalf(
				ctx,
				"unable to absorb Result: %s\ndiff(new, old): %s",
				err, pretty.Diff(curResult, mergedResult),
			)
		}

		if pErr != nil {
			// Initialize the error index.
			pErr.SetErrorIndex(int32(index))

			switch tErr := pErr.GetDetail().(type) {
			case *roachpb.WriteTooOldError:
				// We got a WriteTooOldError. We continue on to run all
				// commands in the batch in order to determine the highest
				// timestamp for more efficient retries. If the batch is
				// transactional, we continue to lay down intents so that
				// other concurrent overlapping transactions are forced
				// through intent resolution and the chances of this batch
				// succeeding when it will be retried are increased.
				if writeTooOldState.err != nil {
					writeTooOldState.err.ActualTimestamp.Forward(
						tErr.ActualTimestamp)
				} else {
					writeTooOldState.err = tErr
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
						args.Header().Key, baHeader.Txn.WriteTimestamp, tErr.ActualTimestamp)
					baHeader.Txn.WriteTimestamp.Forward(tErr.ActualTimestamp)
					baHeader.Txn.WriteTooOld = true
				} else {
					// For non-transactional requests, there's nowhere to defer the error
					// to. And the request has to fail because non-transactional batches
					// should read and write at the same timestamp.
					writeTooOldState.cantDeferWTOE = true
				}

				// Clear pErr; we're done processing the WTOE for now and we'll return
				// to considering it below after we've evaluated all requests.
				pErr = nil
			default:
				return nil, mergedResult, pErr
			}
		}

		// If the last request was carried out with a limit, subtract the number
		// of results from the limit going forward. Exhausting the limit results
		// in a limit of -1. This makes sure that we still execute the rest of
		// the batch, but with limit-aware operations returning no data.
		if limit, retResults := baHeader.MaxSpanRequestKeys, reply.Header().NumKeys; limit > 0 {
			if retResults > limit {
				index, retResults, limit := index, retResults, limit // don't alloc unless branch taken
				err := errorutil.UnexpectedWithIssueErrorf(46652,
					"received %d results, limit was %d (original limit: %d, batch=%s idx=%d)",
					errors.Safe(retResults), errors.Safe(limit),
					errors.Safe(ba.Header.MaxSpanRequestKeys),
					errors.Safe(ba.Summary()), errors.Safe(index))
				if sentryIssue46720Limiter.Allow() {
					log.Errorf(ctx, "%v", err)
					errorutil.SendReport(ctx, &rec.ClusterSettings().SV, err)
				}
				return nil, mergedResult, roachpb.NewError(err)
			} else if retResults < limit {
				baHeader.MaxSpanRequestKeys -= retResults
			} else {
				// They were equal, so drop to -1 instead of zero (which would
				// mean "no limit").
				baHeader.MaxSpanRequestKeys = -1
			}
		} else if limit < 0 {
			if retResults > 0 {
				index, retResults := index, retResults // don't alloc unless branch taken
				log.Fatalf(ctx,
					"received %d results, limit was exhausted (original limit: %d, batch=%s idx=%d)",
					errors.Safe(retResults), errors.Safe(ba.Header.MaxSpanRequestKeys),
					errors.Safe(ba.Summary()), errors.Safe(index))
			}
		}
		// Same as for MaxSpanRequestKeys above, keep track of the limit and
		// make sure to fall through to -1 instead of hitting zero (which
		// means no limit).
		if baHeader.TargetBytes > 0 {
			retBytes := reply.Header().NumBytes
			if baHeader.TargetBytes > retBytes {
				baHeader.TargetBytes -= retBytes
			} else {
				baHeader.TargetBytes = -1
			}
		}

		// If transactional, we use ba.Txn for each individual command and
		// accumulate updates to it. Once accumulated, we then remove the Txn
		// from each individual response.
		// TODO(spencer,tschottdorf): need copy-on-write behavior for the
		//   updated batch transaction / timestamp.
		if baHeader.Txn != nil {
			if header := reply.Header(); header.Txn != nil {
				baHeader.Txn.Update(header.Txn)
				header.Txn = nil
				reply.SetHeader(header)
			}
		}
	}

	if writeTooOldState.err != nil {
		if baHeader.Txn != nil && baHeader.Txn.Status.IsCommittedOrStaging() {
			log.Fatalf(ctx, "committed txn with writeTooOld err: %s", writeTooOldState.err)
		}
	}

	// If there's a write too old error that we can't defer, return it.
	if writeTooOldState.cantDeferWTOE {
		return nil, mergedResult, roachpb.NewErrorWithTxn(writeTooOldState.err, baHeader.Txn)
	}

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
	raftCmdID kvserverbase.CmdIDKey,
	index int,
	readWriter storage.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.Request,
	reply roachpb.Response,
) (result.Result, *roachpb.Error) {
	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := rec.EvalKnobs().TestingEvalFilter; filter != nil {
		filterArgs := kvserverbase.FilterArgs{
			Ctx:   ctx,
			CmdID: raftCmdID,
			Index: index,
			Sid:   rec.StoreID(),
			Req:   args,
			Hdr:   h,
		}
		if pErr := filter(filterArgs); pErr != nil {
			if pErr.GetTxn() == nil {
				pErr.SetTxn(h.Txn)
			}
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
			Args:    args,
			Stats:   ms,
		}

		if cmd.EvalRW != nil {
			pd, err = cmd.EvalRW(ctx, readWriter, cArgs, reply)
		} else {
			pd, err = cmd.EvalRO(ctx, readWriter, cArgs, reply)
		}
	} else {
		err = errors.Errorf("unrecognized command %s", args.Method())
	}

	if h.ReturnRangeInfo {
		returnRangeInfo(reply, rec)
	}

	// TODO(peter): We'd like to assert that the hlc clock is always updated
	// correctly, but various tests insert versioned data without going through
	// the proper channels. See TestPushTxnUpgradeExistingTxn for an example.
	//
	// if header.Txn != nil && h.Timestamp.LessEq(header.Txn.Timestamp) {
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

// returnRangeInfo populates RangeInfos in the response if the batch
// requested them.
func returnRangeInfo(reply roachpb.Response, rec batcheval.EvalContext) {
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

// canDoServersideRetry looks at the error produced by evaluating ba (or the
// WriteTooOldFlag in br.Txn if there's no error) and decides if it's possible
// to retry the batch evaluation at a higher timestamp. Retrying is sometimes
// possible in case of some retriable errors which ask for higher timestamps:
// for transactional requests, retrying is possible if the transaction had not
// performed any prior reads that need refreshing.
//
// deadline, if not nil, specifies the highest timestamp (exclusive) at which
// the request can be evaluated. If ba is a transactional request, then dealine
// cannot be specified; a transaction's deadline comes from it's EndTxn request.
//
// If true is returned, ba and ba.Txn will have been updated with the new
// timestamp.
func canDoServersideRetry(
	ctx context.Context,
	pErr *roachpb.Error,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	latchSpans *spanset.SpanSet,
	deadline *hlc.Timestamp,
) bool {
	if ba.Txn != nil {
		if deadline != nil {
			log.Fatal(ctx, "deadline passed for transactional request")
		}
		canFwdRTS := ba.CanForwardReadTimestamp
		if etArg, ok := ba.GetArg(roachpb.EndTxn); ok {
			// If the request provided an EndTxn request, also check its
			// CanCommitAtHigherTimestamp flag. This ensures that we're backwards
			// compatable and gives us a chance to make sure that these flags are
			// in-sync until the CanCommitAtHigherTimestamp is migrated away.
			et := etArg.(*roachpb.EndTxnRequest)
			canFwdCTS := batcheval.CanForwardCommitTimestampWithoutRefresh(ba.Txn, et)
			if canFwdRTS && !canFwdCTS {
				log.Fatalf(ctx, "unexpected mismatch between Batch.CanForwardReadTimestamp "+
					"(%+v) and EndTxn.CanCommitAtHigherTimestamp (%+v)", ba, et)
			}
			canFwdRTS = canFwdCTS
			deadline = et.Deadline
		}
		if !canFwdRTS {
			return false
		}
	}
	var newTimestamp hlc.Timestamp

	if pErr != nil {
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.WriteTooOldError:
			// Locking scans hit WriteTooOld errors if they encounter values at
			// timestamps higher than their read timestamps. The encountered
			// timestamps are guaranteed to be greater than the txn's read
			// timestamp, but not its write timestamp. So, when determining what
			// the new timestamp should be, we make sure to not regress the
			// txn's write timestamp.
			newTimestamp = tErr.ActualTimestamp
			if ba.Txn != nil {
				newTimestamp.Forward(pErr.GetTxn().WriteTimestamp)
			}
		case *roachpb.TransactionRetryError:
			if ba.Txn == nil {
				// TODO(andrei): I don't know if TransactionRetryError is possible for
				// non-transactional batches, but some tests inject them for 1PC
				// transactions. I'm not sure how to deal with them, so let's not retry.
				return false
			}
			newTimestamp = pErr.GetTxn().WriteTimestamp
		default:
			// TODO(andrei): Handle other retriable errors too.
			return false
		}
	} else {
		if !br.Txn.WriteTooOld {
			log.Fatalf(ctx, "programming error: expected the WriteTooOld flag to be set")
		}
		newTimestamp = br.Txn.WriteTimestamp
	}

	if deadline != nil && deadline.LessEq(newTimestamp) {
		return false
	}
	return tryBumpBatchTimestamp(ctx, ba, newTimestamp, latchSpans)
}
