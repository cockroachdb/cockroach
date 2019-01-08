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
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// evaluateBatch evaluates a batch request by splitting it up into its
// individual commands, passing them to evaluateCommand, and combining
// the results.
func evaluateBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	batch engine.ReadWriter,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
	readOnlyBatch bool,
) (*roachpb.BatchResponse, result.Result, *roachpb.Error) {
	br := ba.CreateReply()

	maxKeys := int64(math.MaxInt64)
	if ba.Header.MaxSpanRequestKeys != 0 {
		// We have a batch of requests with a limit. We keep track of how many
		// remaining keys we can touch.
		maxKeys = ba.Header.MaxSpanRequestKeys
	}

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(ba.Requests) >= optimizePutThreshold && !readOnlyBatch {
		ba.Requests = optimizePuts(batch, ba.Requests, ba.Header.DistinctSpans)
	}

	// Create a shallow clone of the transaction to store the new txn
	// state produced on the return/error path. We use a shallow clone
	// because we only modify a few non-pointer fields (Sequence,
	// DeprecatedBatchIndex, WriteTooOld, Timestamp): a shallow clone saves a
	// few allocs.
	if ba.Txn != nil {
		txnShallow := *ba.Txn
		ba.Txn = &txnShallow

		// Check whether this transaction has been aborted, if applicable.
		// This applies to writes that leave intents (the use of the
		// IsTransactionWrite flag excludes operations like HeartbeatTxn),
		// and reads that occur in a transaction that has already written
		// (see #2231 for more about why we check for aborted transactions
		// on reads). Note that 1PC transactions have had their
		// transaction field cleared by this point so we do not execute
		// this check in that case.
		if ba.IsTransactionWrite() || ba.Txn.Writing {
			// We don't check the abort span for a couple of special requests:
			// - if the request is asking to abort the transaction, then don't check the
			// AbortSpan; we don't want the request to be rejected if the transaction
			// has already been aborted.
			// - heartbeats don't check the abort span. If the txn is aborted, they'll
			// return an aborted proto in their otherwise successful response.
			singleAbort := ba.IsSingleEndTransactionRequest() &&
				!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
			if !singleAbort && !ba.IsSingleHeartbeatTxnRequest() {
				if pErr := checkIfTxnAborted(ctx, rec, batch, *ba.Txn); pErr != nil {
					return nil, result.Result{}, pErr
				}
			}
		}
	}

	var result result.Result
	var writeTooOldErr *roachpb.Error
	returnWriteTooOldErr := true

	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()
		if ba.Txn != nil {
			// Sequence numbers used to be set on each BatchRequest instead of
			// on each individual Request. This meant that all Requests in a
			// BatchRequest shared the same sequence number, so a BatchIndex was
			// augmented to provide an ordering between them. Individual
			// Requests were later given their own sequence numbers, so the
			// BatchIndex was no longer necessary.
			if seqNum := args.Header().Sequence; seqNum != 0 {
				// Set the Request's sequence number on the TxnMeta for this
				// request. Each request will set their own sequence number on
				// the TxnMeta, which is stored as part of an intent.
				ba.Txn.Sequence = seqNum
			}
		}
		// Note that responses are populated even when an error is returned.
		// TODO(tschottdorf): Change that. IIRC there is nontrivial use of it currently.
		reply := br.Responses[index].GetInner()
		curResult, pErr := evaluateCommand(ctx, idKey, index, batch, rec, ms, ba.Header, maxKeys, args, reply)

		if err := result.MergeAndDestroy(curResult); err != nil {
			// TODO(tschottdorf): see whether we really need to pass nontrivial
			// Result up on error and if so, formalize that.
			log.Fatalf(
				ctx,
				"unable to absorb Result: %s\ndiff(new, old): %s",
				err, pretty.Diff(curResult, result),
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
				if writeTooOldErr != nil {
					writeTooOldErr.GetDetail().(*roachpb.WriteTooOldError).ActualTimestamp.Forward(tErr.ActualTimestamp)
				} else {
					writeTooOldErr = pErr
					// For transactions, we want to swallow the write too old error
					// and just move the transaction timestamp forward and set the
					// WriteTooOld flag. See below for exceptions.
					if ba.Txn != nil {
						returnWriteTooOldErr = false
					}
				}
				// Set the flag to return a WriteTooOldError with the max timestamp
				// encountered evaluating the entire batch on cput and inc requests.
				// Because both of these requests must have their keys refreshed on
				// commit with Transaction.WriteTooOld is true, and that refresh will
				// fail, we'd be otherwise guaranteed to do a client-side retry.
				// Returning an error allows a txn-coord-side retry.
				switch args.(type) {
				case *roachpb.ConditionalPutRequest:
					// Conditional puts are an exception. Here, it makes less sense to
					// continue because it's likely that the cput will fail on retry (a
					// newer value is less likely to match the expected value). It's
					// better to return the WriteTooOldError directly, allowing the txn
					// coord sender to retry if it can refresh all other spans encountered
					// already during the transaction, and then, if the cput results in a
					// condition failed error, report that back to the client instead of a
					// retryable error.
					returnWriteTooOldErr = true
				case *roachpb.IncrementRequest:
					// Increments are an exception for similar reasons. If we wait until
					// commit, we'll need a client-side retry, so we return immediately
					// to see if we can do a txn coord sender retry instead.
					returnWriteTooOldErr = true
				case *roachpb.InitPutRequest:
					// Init puts are also an exception. There's no reason to believe they
					// will succeed on a retry, so better to short circuit and return the
					// write too old error.
					returnWriteTooOldErr = true
				}
				if ba.Txn != nil {
					ba.Txn.Timestamp.Forward(tErr.ActualTimestamp)
					ba.Txn.WriteTooOld = true
				}
				// Clear pErr; we're done processing it by having moved the
				// batch or txn timestamps forward and set WriteTooOld if this
				// is a transactional write. The EndTransaction will detect
				// this pushed timestamp and return a TransactionRetryError.
				pErr = nil
			default:
				return nil, result, pErr
			}
		}

		if maxKeys != math.MaxInt64 {
			retResults := reply.Header().NumKeys
			if retResults > maxKeys {
				log.Fatalf(ctx, "received %d results, limit was %d", retResults, maxKeys)
			}
			maxKeys -= retResults
		}

		// If transactional, we use ba.Txn for each individual command and
		// accumulate updates to it.
		// TODO(spencer,tschottdorf): need copy-on-write behavior for the
		//   updated batch transaction / timestamp.
		if ba.Txn != nil {
			if txn := reply.Header().Txn; txn != nil {
				ba.Txn.Update(txn)
			}
		}
	}

	// If there's a write too old error, return now that we've found
	// the high water timestamp for retries.
	if writeTooOldErr != nil && returnWriteTooOldErr {
		return nil, result, writeTooOldErr
	}

	if ba.Txn != nil {
		// If transactional, send out the final transaction entry with the reply.
		br.Txn = ba.Txn
		// If the transaction committed, forward the response
		// timestamp to the commit timestamp in case we were able to
		// optimize and commit at a higher timestamp without higher-level
		// retry (i.e. there were no refresh spans and the commit timestamp
		// wasn't leaked).
		if ba.Txn.Status == roachpb.COMMITTED {
			br.Timestamp.Forward(ba.Txn.Timestamp)
		}
	}
	// Always update the batch response timestamp field to the timestamp at
	// which the batch executed.
	br.Timestamp.Forward(ba.Timestamp)

	return br, result, nil
}

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
		returnRangeInfo(reply, rec)
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
