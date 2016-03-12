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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"math"

	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/gogo/protobuf/proto"
)

// executeCmd switches over the method and multiplexes to execute the appropriate storage API
// command. It returns the response, an error, and a slice of intents that were skipped during
// execution.  If an error is returned, any returned intents should still be resolved.
// remScanResults is the number of scan results remaining for this batch (MaxInt64 for no
// limit).
func (r *Replica) executeCmd(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, remScanResults int64,
	args roachpb.Request) (roachpb.Response, []roachpb.Intent, *roachpb.Error) {
	ts := h.Timestamp

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return &roachpb.NoopResponse{}, nil, nil
	}

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, nil, roachpb.NewErrorWithTxn(err, h.Txn)
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := r.store.ctx.TestingMocker.TestingCommandFilter; filter != nil {
		err := filter(r.store.StoreID(), args, h)
		if err != nil {
			pErr := roachpb.NewErrorWithTxn(err, h.Txn)
			log.Infof("test injecting error: %s", pErr)
			return nil, nil, pErr
		}
	}

	// Update the node clock with the serviced request. This maintains a
	// high water mark for all ops serviced, so that received ops
	// without a timestamp specified are guaranteed one higher than any
	// op already executed for overlapping keys.
	r.store.Clock().Update(ts)

	var reply roachpb.Response
	var intents []roachpb.Intent
	var err error
	switch tArgs := args.(type) {
	case *roachpb.GetRequest:
		var resp roachpb.GetResponse
		resp, intents, err = r.Get(batch, h, *tArgs)
		reply = &resp
	case *roachpb.PutRequest:
		var resp roachpb.PutResponse
		resp, err = r.Put(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.ConditionalPutRequest:
		var resp roachpb.ConditionalPutResponse
		resp, err = r.ConditionalPut(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.IncrementRequest:
		var resp roachpb.IncrementResponse
		resp, err = r.Increment(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.DeleteRequest:
		var resp roachpb.DeleteResponse
		resp, err = r.Delete(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.DeleteRangeRequest:
		var resp roachpb.DeleteRangeResponse
		resp, err = r.DeleteRange(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.ScanRequest:
		var resp roachpb.ScanResponse
		resp, intents, err = r.Scan(batch, h, remScanResults, *tArgs)
		reply = &resp
	case *roachpb.ReverseScanRequest:
		var resp roachpb.ReverseScanResponse
		resp, intents, err = r.ReverseScan(batch, h, remScanResults, *tArgs)
		reply = &resp
	case *roachpb.BeginTransactionRequest:
		var resp roachpb.BeginTransactionResponse
		resp, err = r.BeginTransaction(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.EndTransactionRequest:
		var resp roachpb.EndTransactionResponse
		resp, intents, err = r.EndTransaction(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.RangeLookupRequest:
		var resp roachpb.RangeLookupResponse
		resp, intents, err = r.RangeLookup(batch, h, *tArgs)
		reply = &resp
	case *roachpb.HeartbeatTxnRequest:
		var resp roachpb.HeartbeatTxnResponse
		resp, err = r.HeartbeatTxn(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.GCRequest:
		var resp roachpb.GCResponse
		resp, err = r.GC(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.PushTxnRequest:
		var resp roachpb.PushTxnResponse
		resp, err = r.PushTxn(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.ResolveIntentRequest:
		var resp roachpb.ResolveIntentResponse
		resp, err = r.ResolveIntent(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.ResolveIntentRangeRequest:
		var resp roachpb.ResolveIntentRangeResponse
		resp, err = r.ResolveIntentRange(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.MergeRequest:
		var resp roachpb.MergeResponse
		resp, err = r.Merge(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.TruncateLogRequest:
		var resp roachpb.TruncateLogResponse
		resp, err = r.TruncateLog(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.LeaderLeaseRequest:
		var resp roachpb.LeaderLeaseResponse
		resp, err = r.LeaderLease(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.ComputeChecksumRequest:
		var resp roachpb.ComputeChecksumResponse
		resp, err = r.ComputeChecksum(batch, ms, h, *tArgs)
		reply = &resp
	case *roachpb.VerifyChecksumRequest:
		var resp roachpb.VerifyChecksumResponse
		resp, err = r.VerifyChecksum(batch, ms, h, *tArgs)
		reply = &resp
	default:
		err = util.Errorf("unrecognized command %s", args.Method())
	}

	if log.V(2) {
		log.Infof("executed %s command %+v: %+v, err=%s", args.Method(), args, reply, err)
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
	return reply, intents, pErr
}

// Get returns the value for a specified key.
func (r *Replica) Get(batch engine.Engine, h roachpb.Header, args roachpb.GetRequest) (roachpb.GetResponse, []roachpb.Intent, error) {
	var reply roachpb.GetResponse

	val, intents, err := engine.MVCCGet(batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	reply.Value = val
	return reply, intents, err
}

// Put sets the value for a specified key.
func (r *Replica) Put(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.PutRequest) (roachpb.PutResponse, error) {
	var reply roachpb.PutResponse

	return reply, engine.MVCCPut(batch, ms, args.Key, h.Timestamp, args.Value, h.Txn)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Replica) ConditionalPut(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.ConditionalPutRequest) (roachpb.ConditionalPutResponse, error) {
	var reply roachpb.ConditionalPutResponse

	return reply, engine.MVCCConditionalPut(batch, ms, args.Key, h.Timestamp, args.Value, args.ExpValue, h.Txn)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Replica) Increment(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.IncrementRequest) (roachpb.IncrementResponse, error) {
	var reply roachpb.IncrementResponse

	newVal, err := engine.MVCCIncrement(batch, ms, args.Key, h.Timestamp, h.Txn, args.Increment)
	reply.NewValue = newVal
	return reply, err
}

// Delete deletes the key and value specified by key.
func (r *Replica) Delete(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.DeleteRequest) (roachpb.DeleteResponse, error) {
	var reply roachpb.DeleteResponse

	return reply, engine.MVCCDelete(batch, ms, args.Key, h.Timestamp, h.Txn)
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Replica) DeleteRange(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.DeleteRangeRequest) (roachpb.DeleteRangeResponse, error) {
	var reply roachpb.DeleteRangeResponse
	deleted, err := engine.MVCCDeleteRange(batch, ms, args.Key, args.EndKey, args.MaxEntriesToDelete, h.Timestamp, h.Txn, args.ReturnKeys)
	reply.Keys = deleted
	return reply, err
}

// scanMaxResultsValue returns the max results value to pass to a scan or reverse scan request (0
// for no limit).
//    remScanResults is the number of remaining results for this batch (MaxInt64 for no
// limit).
//    scanMaxResults is the limit in this scan request (0 for no limit)
func scanMaxResultsValue(remScanResults int64, scanMaxResults int64) int64 {
	if remScanResults == math.MaxInt64 {
		// Unlimited batch.
		return scanMaxResults
	}
	if scanMaxResults != 0 && scanMaxResults < remScanResults {
		// Scan limit is less than remaining batch limit.
		return scanMaxResults
	}
	// Reamining batch limit is less than scan limit.
	return remScanResults
}

// Scan scans the key range specified by start key through end key in ascending order up to some
// maximum number of results. remScanResults stores the number of scan results remaining for this
// batch (MaxInt64 for no limit).
func (r *Replica) Scan(batch engine.Engine, h roachpb.Header, remScanResults int64,
	args roachpb.ScanRequest) (roachpb.ScanResponse, []roachpb.Intent, error) {
	if remScanResults == 0 {
		// We can't return any more results; skip the scan
		return roachpb.ScanResponse{}, nil, nil
	}
	maxResults := scanMaxResultsValue(remScanResults, args.MaxResults)

	rows, intents, err := engine.MVCCScan(batch, args.Key, args.EndKey, maxResults, h.Timestamp,
		h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	return roachpb.ScanResponse{Rows: rows}, intents, err
}

// ReverseScan scans the key range specified by start key through end key in descending order up to
// some maximum number of results. remScanResults stores the number of scan results remaining for
// this batch (MaxInt64 for no limit).
func (r *Replica) ReverseScan(batch engine.Engine, h roachpb.Header, remScanResults int64,
	args roachpb.ReverseScanRequest) (roachpb.ReverseScanResponse, []roachpb.Intent, error) {
	if remScanResults == 0 {
		// We can't return any more results; skip the scan
		return roachpb.ReverseScanResponse{}, nil, nil
	}
	maxResults := scanMaxResultsValue(remScanResults, args.MaxResults)

	rows, intents, err := engine.MVCCReverseScan(batch, args.Key, args.EndKey, maxResults,
		h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	return roachpb.ReverseScanResponse{Rows: rows}, intents, err
}

func verifyTransaction(h roachpb.Header, args roachpb.Request) error {
	if h.Txn == nil {
		return util.Errorf("no transaction specified to HeartbeatTxn")
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return util.Errorf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	return nil
}

// BeginTransaction writes the initial transaction record. Fails in
// the event that a transaction record is already written. This may
// occur if a transaction is started with a batch containing writes
// to different ranges, and the range containing the txn record fails
// to receive the write batch before a heartbeat or txn push is
// performed first and aborts the transaction.
func (r *Replica) BeginTransaction(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.BeginTransactionRequest) (roachpb.BeginTransactionResponse, error) {
	var reply roachpb.BeginTransactionResponse

	if err := verifyTransaction(h, &args); err != nil {
		return reply, err
	}
	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	// Verify transaction does not already exist.
	txn := roachpb.Transaction{}
	if ok, err := engine.MVCCGetProto(batch, key, roachpb.ZeroTimestamp, true, nil, &txn); err != nil {
		return reply, err
	} else if ok {
		reply.Txn = &txn
		// Check whether someone has come in ahead and already aborted the
		// txn.
		if txn.Status == roachpb.ABORTED {
			return reply, roachpb.NewTransactionAbortedError()
		} else if txn.Status == roachpb.PENDING && h.Txn.Epoch > txn.Epoch {
			// On a transaction retry there will be an extant txn record but
			// this run should have an upgraded epoch. This is a pass
			// through to set the new transaction record.
		} else {
			return reply, roachpb.NewTransactionStatusError("non-aborted transaction exists already")
		}
	}

	// Write the txn record.
	err := engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, h.Txn)
	return reply, err
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
// TODO(tschottdorf): return nil reply on any error. The error itself
// must be the authoritative source of information.
func (r *Replica) EndTransaction(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.EndTransactionRequest) (roachpb.EndTransactionResponse, []roachpb.Intent, error) {
	var reply roachpb.EndTransactionResponse
	ts := h.Timestamp // all we're going to use from the header.

	if err := verifyTransaction(h, &args); err != nil {
		return reply, nil, err
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	// Fetch existing transaction.
	reply.Txn = &roachpb.Transaction{}
	if ok, err := engine.MVCCGetProto(batch, key, roachpb.ZeroTimestamp, true, nil, reply.Txn); err != nil {
		return reply, nil, err
	} else if !ok {
		return reply, nil, util.Errorf("transaction does not exist: %s on store %d", h.Txn, r.store.StoreID())
	}

	if args.Deadline != nil && args.Deadline.Less(ts) {
		reply.Txn.Status = roachpb.ABORTED
		// FIXME(#3037):
		// If the deadline has lapsed, return all the intents for
		// resolution. Unfortunately, since we're (a) returning an error,
		// and (b) not able to write on error (see #1989), we can't write
		// ABORTED into the master transaction record, which remains
		// PENDING, and that's pretty bad.
		return reply, roachpb.AsIntents(args.IntentSpans, reply.Txn), roachpb.NewTransactionAbortedError()
	}

	// Verify that we can either commit it or abort it (according
	// to args.Commit), and also that the Timestamp and Epoch have
	// not suffered regression.
	if reply.Txn.Status == roachpb.COMMITTED {
		return reply, nil, roachpb.NewTransactionStatusError("already committed")
	} else if reply.Txn.Status == roachpb.ABORTED {
		// If the transaction was previously aborted by a concurrent
		// writer's push, any intents written are still open. It's only now
		// that we know them, so we return them all for asynchronous
		// resolution (we're currently not able to write on error, but
		// see #1989).
		return reply, roachpb.AsIntents(args.IntentSpans, reply.Txn), roachpb.NewTransactionAbortedError()
	} else if h.Txn.Epoch < reply.Txn.Epoch {
		// TODO(tschottdorf): this leaves the Txn record (and more
		// importantly, intents) dangling; we can't currently write on
		// error. Would panic, but that makes TestEndTransactionWithErrors
		// awkward.
		return reply, nil, roachpb.NewTransactionStatusError(fmt.Sprintf("epoch regression: %d", h.Txn.Epoch))
	} else if h.Txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(h.Txn.OrigTimestamp) {
		// The transaction record can only ever be pushed forward, so it's an
		// error if somehow the transaction record has an earlier timestamp
		// than the original transaction timestamp.

		// TODO(tschottdorf): see above comment on epoch regression.
		return reply, nil, roachpb.NewTransactionStatusError(fmt.Sprintf("timestamp regression: %s", h.Txn.OrigTimestamp))
	}

	// Take max of requested epoch and existing epoch. The requester
	// may have incremented the epoch on retries.
	if reply.Txn.Epoch < h.Txn.Epoch {
		reply.Txn.Epoch = h.Txn.Epoch
	}
	// Take max of requested priority and existing priority. This isn't
	// terribly useful, but we do it for completeness.
	if reply.Txn.Priority < h.Txn.Priority {
		reply.Txn.Priority = h.Txn.Priority
	}

	// Take max of requested timestamp and possibly "pushed" txn
	// record timestamp as the final commit timestamp.
	// TODO(tschottdorf): shouldn't have to be done here.
	reply.Txn.Timestamp.Forward(ts)

	// Set transaction status to COMMITTED or ABORTED as per the
	// args.Commit parameter. If the transaction deadline is set and has
	// elapsed, abort.
	if args.Commit {
		// If the isolation level is SERIALIZABLE, return a transaction
		// retry error if the commit timestamp isn't equal to the txn
		// timestamp.
		if h.Txn.Isolation == roachpb.SERIALIZABLE && !reply.Txn.Timestamp.Equal(h.Txn.OrigTimestamp) {
			return reply, nil, roachpb.NewTransactionRetryError()
		}
		reply.Txn.Status = roachpb.COMMITTED
	} else {
		reply.Txn.Status = roachpb.ABORTED
	}

	// Resolve any explicit intents. All that are local to this range get
	// resolved synchronously in the same batch. The remainder are collected
	// and handed off to asynchronous processing.
	desc := r.Desc()
	var preMergeDesc *roachpb.RangeDescriptor
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		preMergeDesc = desc
		desc = &mergeTrigger.UpdatedDesc
	}

	iterAndBuf := engine.GetIterAndBuf(batch)
	defer iterAndBuf.Cleanup()

	var externalIntents []roachpb.Intent
	for _, span := range args.IntentSpans {
		if err := func() error {
			intent := roachpb.Intent{Span: span, Txn: reply.Txn.TxnMeta, Status: reply.Txn.Status}
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalIntents = append(externalIntents, intent)
					return nil
				}
				resolveMs := ms
				if preMergeDesc != nil && !containsKey(*preMergeDesc, span.Key) {
					// If this transaction included a merge and the intents
					// are from the subsumed range, ignore the intent resolution
					// stats, as they will already be accounted for during the
					// merge trigger.
					resolveMs = nil
				}
				return engine.MVCCResolveWriteIntentUsingIter(batch, iterAndBuf, resolveMs, intent)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				outIntent := intent
				outIntent.Span = span
				externalIntents = append(externalIntents, outIntent)
			}
			if inSpan != nil {
				intent.Span = *inSpan
				_, err := engine.MVCCResolveWriteIntentRangeUsingIter(batch, iterAndBuf, ms, intent, 0)
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving intent at %s on end transaction [%s]: %s", span, reply.Txn.Status, err))
		}
	}

	// Persist the transaction record with updated status (& possibly timestamp).
	// If we've already resolved all intents locally, we actually delete the
	// record right away - no use in keeping it around.
	{
		var err error
		if txnAutoGC && len(externalIntents) == 0 {
			if log.V(1) {
				log.Infof("auto-gc'ed %s (%d intents)", h.Txn.Short(), len(args.IntentSpans))
			}
			err = engine.MVCCDelete(batch, ms, key, roachpb.ZeroTimestamp, nil /* txn */)
		} else {
			reply.Txn.Intents = make([]roachpb.Span, len(externalIntents))
			for i := range externalIntents {
				reply.Txn.Intents[i] = externalIntents[i].Span
			}
			err = engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil /* txn */, reply.Txn)
		}
		if err != nil {
			return reply, nil, err
		}
	}

	// Run triggers if successfully committed.
	if reply.Txn.Status == roachpb.COMMITTED {
		ct := args.InternalCommitTrigger
		if ct != nil {
			// Hold readMu across the application of any commit trigger.
			// This makes sure that no reads are happening in parallel;
			// see #3148.
			r.readOnlyCmdMu.Lock()
			batch.Defer(r.readOnlyCmdMu.Unlock)
		}

		if err := func() error {
			if ct.GetSplitTrigger() != nil {
				if err := r.splitTrigger(batch, ms, ct.SplitTrigger); err != nil {
					return err
				}
				*ms = engine.MVCCStats{} // clear stats, as split recomputed.
			}
			if ct.GetMergeTrigger() != nil {
				if err := r.mergeTrigger(batch, ms, ct.MergeTrigger); err != nil {
					return err
				}
				*ms = engine.MVCCStats{} // clear stats, as merge recomputed.
			}
			if ct.GetChangeReplicasTrigger() != nil {
				if err := r.changeReplicasTrigger(batch, ct.ChangeReplicasTrigger); err != nil {
					return err
				}
			}
			if ct.GetModifiedSpanTrigger() != nil {
				if ct.ModifiedSpanTrigger.SystemConfigSpan {
					// Check if we need to gossip the system config.
					batch.Defer(r.maybeGossipSystemConfig)
				}
			}
			return nil
		}(); err != nil {
			r.readOnlyCmdMu.Unlock() // since the batch.Defer above won't run
			// TODO(tschottdorf): should an error here always amount to a
			// ReplicaCorruptionError?
			log.Errorf("Range %d transaction commit trigger fail: %s", r.RangeID, err)
			return reply, nil, err
		}
	}

	// If the transaction just ended (via a successful EndTransaction call),
	// we can purge the local sequence cache state. Why? We are on the range to
	// which the transaction is anchored, and replay protection isn't needed
	// any more. When a transactional write gets replayed over its own resolved
	// intent, the write will fail (WriteTooOldError), so the case which
	// requires our attention is that of a yet unresolved intent. We're ok on
	// this range; after all, we are resolving all local intents in this same
	// batch. The only interesting replay here could be another lonely
	// EndTransaction, which is fine either way (if we auto-gced our entry,
	// that will fail due to missing BeginTransaction; otherwise, there's a
	// non-pending entry here). BeginTransaction itself can't replay; it always
	// comes in a batch with a write and will thus fail.
	return reply, externalIntents, r.clearSequenceCache(batch, ms, false /* !poison */, reply.Txn.TxnMeta, reply.Txn.Status)
}

// intersectSpan takes an intent and a descriptor. It then splits the
// intent's range into up to three pieces: A first piece which is contained in
// the Range, and a slice of up to two further intents which are outside of the
// key range. An intent for which [Key, EndKey) is empty does not result in any
// intents; thus intersectIntent only applies to intent ranges.
// A range-local intent range is never split: It's returned as either
// belonging to or outside of the descriptor's key range, and passing an intent
// which begins range-local but ends non-local results in a panic.
// TODO(tschottdorf) move to proto, make more gen-purpose - kv.truncate does
// some similar things.
func intersectSpan(span roachpb.Span, desc roachpb.RangeDescriptor) (middle *roachpb.Span, outside []roachpb.Span) {
	start, end := desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey()
	if len(span.EndKey) == 0 {
		outside = append(outside, span)
		return
	}
	if bytes.Compare(span.Key, keys.LocalRangeMax) < 0 {
		if bytes.Compare(span.EndKey, keys.LocalRangeMax) >= 0 {
			panic("a local intent range may not have a non-local portion")
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

// RangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix prepended. The Range Metadata
// Key for an ordinary key can be generated with the `keys.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and dispatching it to
// RangeLookup.
//
// Note that the Range Metadata Key sent to RangeLookup is NOT the key
// at which the desired RangeDescriptor is stored. Instead, this method returns
// the RangeDescriptor stored at the _lowest_ existing key which is _greater_
// than the given key. The returned RangeDescriptor will thus contain the
// ordinary key which was originally used to generate the Range Metadata Key
// sent to RangeLookup.
//
// The "Range Metadata Key" for a range is built by appending the end key of
// the range to the respective meta prefix.
//
// Lookups for range metadata keys usually want to read inconsistently, but
// some callers need a consistent result; both are supported.
//
// This method has an important optimization in the inconsistent case: instead
// of just returning the request RangeDescriptor, it also returns a slice of
// additional range descriptors immediately consecutive to the desired
// RangeDescriptor. This is intended to serve as a sort of caching pre-fetch,
// so that the requesting nodes can aggressively cache RangeDescriptors which
// are likely to be desired by their current workload. The Reverse flag
// specifies whether descriptors are prefetched in descending or ascending
// order.
func (r *Replica) RangeLookup(batch engine.Engine, h roachpb.Header, args roachpb.RangeLookupRequest) (roachpb.RangeLookupResponse, []roachpb.Intent, error) {
	var reply roachpb.RangeLookupResponse
	ts := h.Timestamp // all we're going to use from the header.
	key := keys.Addr(args.Key)
	if !key.Equal(args.Key) {
		return reply, nil, util.Errorf("illegal lookup of range-local key")
	}

	rangeCount := int64(args.MaxRanges)
	if rangeCount < 1 {
		return reply, nil, util.Errorf("Range lookup specified invalid maximum range count %d: must be > 0", rangeCount)
	}
	consistent := h.ReadConsistency != roachpb.INCONSISTENT
	if consistent && args.ConsiderIntents {
		return reply, nil, util.Errorf("can not read consistently and special-case intents")
	}
	if args.ConsiderIntents {
		// Disable prefetching; the caller only cares about a single intent,
		// and the code below simplifies considerably.
		rangeCount = 1
	}

	var checkAndUnmarshal func(roachpb.Value) (*roachpb.RangeDescriptor, error)

	var kvs []roachpb.KeyValue // kv descriptor pairs in scan order
	var intents []roachpb.Intent
	if !args.Reverse {
		// If scanning forward, there's no special "checking": Just decode the
		// descriptor and return it.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var rd roachpb.RangeDescriptor
			if err := v.GetProto(&rd); err != nil {
				return nil, err
			}
			return &rd, nil
		}

		// We want to search for the metadata key greater than
		// args.Key. Scan for both the requested key and the keys immediately
		// afterwards, up to MaxRanges.
		startKey, endKey, err := keys.MetaScanBounds(key)
		if err != nil {
			return reply, nil, err
		}

		// Scan for descriptors.
		kvs, intents, err = engine.MVCCScan(batch, startKey, endKey, rangeCount,
			ts, consistent, h.Txn)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return reply, nil, err
		}
	} else {
		// Use MVCCScan to get first the first range. There are three cases:
		// 1. args.Key is not an endpoint of the range and
		// 2a. The args.Key is the start/end key of the range.
		// 2b. Even worse, the body of args.Key is roachpb.KeyMax.
		// In the first case, we need use the MVCCScan() to get the first
		// range descriptor, because ReverseScan can't do the work. If we
		// have ranges [a,c) and [c,f) and the reverse scan request's key
		// range is [b,d), then d.Next() is less than "f", and so the meta
		// row {f->[c,f)} would be ignored by MVCCReverseScan. In case 2a,
		// the range descriptor received by MVCCScan will be filtered before
		// results are returned: With ranges [c,f) and [f,z), reverse scan
		// on [d,f) receives the descriptor {z->[f,z)}, which is discarded
		// below since it's not being asked for. Finally, in case 2b, we
		// don't even attempt the forward scan because it's neither defined
		// nor required.
		// Note that Meta1KeyMax is admissible: it means we're looking for
		// the range descriptor that houses Meta2KeyMax, and a forward scan
		// handles it correctly.
		// In this case, checkAndUnmarshal is more complicated: It needs
		// to weed out descriptors from the forward scan above, which could
		// return a result or an intent we're not supposed to return.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var r roachpb.RangeDescriptor
			if err := v.GetProto(&r); err != nil {
				return nil, err
			}
			if !keys.Addr(keys.RangeMetaKey(r.StartKey)).Less(key) {
				// This is the case in which we've picked up an extra descriptor
				// we don't want.
				return nil, nil
			}
			// We actually want this descriptor.
			return &r, nil
		}

		if key.Less(keys.Addr(keys.Meta2KeyMax)) {
			startKey, endKey, err := keys.MetaScanBounds(key)
			if err != nil {
				return reply, nil, err
			}

			kvs, intents, err = engine.MVCCScan(batch, startKey, endKey, 1,
				ts, consistent, h.Txn)
			if err != nil {
				return reply, nil, err
			}

		}
		// We want to search for the metadata key just less or equal to
		// args.Key. Scan in reverse order for both the requested key and the
		// keys immediately backwards, up to MaxRanges.
		startKey, endKey, err := keys.MetaReverseScanBounds(key)
		if err != nil {
			return reply, nil, err
		}
		// Reverse scan for descriptors.
		revKvs, revIntents, err := engine.MVCCReverseScan(batch, startKey, endKey, rangeCount,
			ts, consistent, h.Txn)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return reply, nil, err
		}

		// Merge the results, the total ranges may be bigger than rangeCount.
		kvs = append(kvs, revKvs...)
		intents = append(intents, revIntents...)
	}

	var rds []roachpb.RangeDescriptor // corresponding unmarshaled descriptors
	if args.ConsiderIntents && len(intents) > 0 && rand.Intn(2) == 0 {
		// NOTE (subtle): dangling intents on meta records are peculiar: It's not
		// clear whether the intent or the previous value point to the correct
		// location of the Range. It gets even more complicated when there are
		// split-related intents or a txn record colocated with a replica
		// involved in the split. Since we cannot know the correct answer, we
		// choose randomly between the pre- and post- transaction values when
		// the ConsiderIntents flag is set (typically after retrying on
		// addressing-related errors). If we guess wrong, the client will try
		// again and get the other value (within a few tries).
		for _, intent := range intents {
			val, _, err := engine.MVCCGetAsTxn(batch, intent.Key, intent.Txn.Timestamp, true, intent.Txn)
			if err != nil {
				return reply, nil, err
			}

			if val == nil {
				// Intent is a deletion.
				continue
			}
			rd, err := checkAndUnmarshal(*val)
			if err != nil {
				return reply, nil, err
			}
			// If this is a descriptor we're allowed to return,
			// do just that and call it a day.
			if rd != nil {
				kvs = []roachpb.KeyValue{{Key: intent.Key, Value: *val}}
				rds = []roachpb.RangeDescriptor{*rd}
				break
			}
		}
	}

	// Decode all scanned range descriptors which haven't been unmarshaled yet.
	for _, kv := range kvs[len(rds):] {
		// TODO(tschottdorf) Candidate for a ReplicaCorruptionError.
		rd, err := checkAndUnmarshal(kv.Value)
		if err != nil {
			return reply, nil, err
		}
		if rd != nil {
			rds = append(rds, *rd)
		}
	}

	if count := int64(len(rds)); count == 0 {
		// No matching results were returned from the scan. This should
		// never happen with the above logic.
		panic(fmt.Sprintf("RangeLookup dispatched to correct range, but no matching RangeDescriptor was found: %s", args.Key))
	} else if count > rangeCount {
		// We've possibly picked up an extra descriptor if we're in reverse
		// mode due to the initial forward scan.
		rds = rds[:rangeCount]
	}

	reply.Ranges = rds
	return reply, intents, nil
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func (r *Replica) HeartbeatTxn(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.HeartbeatTxnRequest) (roachpb.HeartbeatTxnResponse, error) {
	var reply roachpb.HeartbeatTxnResponse

	if err := verifyTransaction(h, &args); err != nil {
		return reply, err
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	var txn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(batch, key, roachpb.ZeroTimestamp, true, nil, &txn); err != nil {
		return reply, err
	} else if !ok {
		// If no existing transaction record was found, skip heartbeat.
		// This could mean the heartbeat is a delayed relic or it could
		// mean that the BeginTransaction call was delayed. In either
		// case, there's no reason to persist a new transaction record.
		return reply, util.Errorf("heartbeat for transaction %s failed; record not present", h.Txn)
	}

	if txn.Status == roachpb.PENDING {
		if txn.LastHeartbeat == nil {
			txn.LastHeartbeat = &roachpb.Timestamp{}
		}
		txn.LastHeartbeat.Forward(args.Now)
		if err := engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, &txn); err != nil {
			return reply, err
		}
	}

	reply.Txn = &txn
	return reply, nil
}

// GC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func (r *Replica) GC(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.GCRequest) (roachpb.GCResponse, error) {
	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	keys := make([]roachpb.GCRequest_GCKey, 0, len(args.Keys))
	for _, k := range args.Keys {
		if r.ContainsKey(k.Key) {
			keys = append(keys, k)
		}
	}

	var reply roachpb.GCResponse
	// Garbage collect the specified keys by expiration timestamps.
	err := engine.MVCCGarbageCollect(batch, ms, keys, h.Timestamp)
	return reply, err
}

// PushTxn resolves conflicts between concurrent txns (or
// between a non-transactional reader or writer and a txn) in several
// ways depending on the statuses and priorities of the conflicting
// transactions. The PushTxn operation is invoked by a
// "pusher" (the writer trying to abort a conflicting txn or the
// reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PushTxn -- the pushee txn whose intent(s) caused the
// conflict). A pusher is either transactional, in which case
// PushTxn is completely initialized, or not, in which case the
// PushTxn has only the priority set.
//
// Txn already committed/aborted: If pushee txn is committed or
// aborted return success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use its as LastHeartbeat. If current time -
// LastHeartbeat > 2 * DefaultHeartbeatInterval, then the pushee txn
// should be either pushed forward, aborted, or confirmed not pending,
// depending on value of Request.PushType.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of
// args.PushType. If args.PushType is PUSH_ABORT, set txn.Status to
// ABORTED, and priority to one less than the pusher's priority and
// return success. If args.PushType is PUSH_TIMESTAMP, set
// txn.Timestamp to just after PushTo.
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Replica) PushTxn(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.PushTxnRequest) (roachpb.PushTxnResponse, error) {
	var reply roachpb.PushTxnResponse

	if args.PusherTxn.ID != nil {
		reply.Txn = &args.PusherTxn
	}

	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return reply, util.Errorf("request key %s should match pushee's txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction; if missing, we're allowed to abort.
	existTxn := &roachpb.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, existTxn)
	if err != nil {
		return reply, err
	}
	// There are three cases in which there is no transaction entry, in
	// decreasing likelihood:
	// * the pushee is still active; it just hasn't committed, restarted,
	//   aborted or heartbeat yet.
	// * the pushee resolved its intents synchronously on successful commit;
	//   in this case, the transaction record of the pushee is also removed.
	//   Note that in this case, the intent which prompted this PushTxn
	//   doesn't exist any more.
	// * the pushee timed out or was aborted and the intent not cleaned up,
	//   but the transaction record garbage collected.
	//
	// We currently make no attempt at guessing which one it is, though we
	// could (see #1939). Instead, a new entry is always written.
	//
	// TODO(tschottdorf): we should actually improve this when we
	// garbage-collect aborted transactions, or we run the risk of a push
	// recreating a GC'ed transaction as PENDING, which is an error if it
	// has open intents (which is likely if someone pushes it).
	if !ok {
		// The transaction doesn't exist on disk; we're allowed to abort it.
		// TODO(tschottdorf): especially for SNAPSHOT transactions, there's
		// something to win here by not aborting, but instead pushing the
		// timestamp. For SERIALIZABLE it's less important, but still better
		// to have them restart than abort. See #3344.
		// TODO(tschottdorf): double-check for problems emanating from
		// using a trivial Transaction proto here. Maybe some fields ought
		// to receive dummy values.
		reply.PusheeTxn.TxnMeta = args.PusheeTxn
		reply.PusheeTxn.Status = roachpb.ABORTED
		return reply, engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, &reply.PusheeTxn)
	}
	// Start with the persisted transaction record as final transaction.
	reply.PusheeTxn = existTxn.Clone()
	// The pusher might be aware of a newer version of the pushee.
	reply.PusheeTxn.Timestamp.Forward(args.PusheeTxn.Timestamp)
	if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
		reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != roachpb.PENDING {
		// Trivial noop.
		return reply, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == roachpb.PUSH_TIMESTAMP && args.PushTo.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return reply, nil
	}

	// pusherWins bool is true in the event the pusher prevails.
	var pusherWins bool

	priority := args.PusherTxn.Priority

	// Check for txn timeout.
	if reply.PusheeTxn.LastHeartbeat == nil {
		reply.PusheeTxn.LastHeartbeat = &reply.PusheeTxn.Timestamp
	}
	if args.Now.Equal(roachpb.ZeroTimestamp) {
		return reply, util.Errorf("the field Now must be provided")
	}
	// Compute heartbeat expiration (all replicas must see the same result).
	expiry := args.Now
	expiry.WallTime -= 2 * DefaultHeartbeatInterval.Nanoseconds()

	if reply.PusheeTxn.LastHeartbeat.Less(expiry) {
		if log.V(1) {
			log.Infof("pushing expired txn %s", reply.PusheeTxn)
		}
		pusherWins = true
		// When cleaning up, actually clean up (as opposed to simply pushing
		// the garbage in the path of future writers).
		args.PushType = roachpb.PUSH_ABORT
	} else if reply.PusheeTxn.Isolation == roachpb.SNAPSHOT && args.PushType == roachpb.PUSH_TIMESTAMP {
		if log.V(1) {
			log.Infof("pushing timestamp for snapshot isolation txn")
		}
		pusherWins = true
	} else if args.PushType == roachpb.PUSH_TOUCH {
		// If just attempting to cleanup old or already-committed txns, don't push.
		pusherWins = false
	} else if reply.PusheeTxn.Priority < priority ||
		(reply.PusheeTxn.Priority == priority && args.PusherTxn.ID != nil &&
			args.PusherTxn.Timestamp.Less(reply.PusheeTxn.Timestamp)) {
		// Pusher wins based on priority; if priorities are equal, order
		// by lower txn timestamp.
		if log.V(1) {
			log.Infof("pushing intent from txn with lower priority %s vs %d", reply.PusheeTxn, priority)
		}
		pusherWins = true
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(reply.PusheeTxn)
		if log.V(1) {
			log.Info(err)
		}
		return reply, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.PushType == roachpb.PUSH_ABORT {
		reply.PusheeTxn.Status = roachpb.ABORTED
	} else if args.PushType == roachpb.PUSH_TIMESTAMP {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.PushTo
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, &reply.PusheeTxn); err != nil {
		return reply, err
	}
	return reply, nil
}

// clearSequenceCache clears out the sequence cache and optionally poisons it
// (creating a new special entry). shouldPoison should be set when the
// transaction in question may still be in use, and causes the sequence cache
// to be "poisoned": the transaction will not be able to access the range again
// (preventing anomalies as in #2231).
//
// Clearing out the sequence cache reduces the work the GC queue and range
// splits/merges (which need to copy the whole sequence cache) have to carry
// out.
// TODO(tschottdorf): early restarts are currently disabled, see TODO within.
func (r *Replica) clearSequenceCache(batch engine.Engine, ms *engine.MVCCStats, shouldPoison bool, txn roachpb.TxnMeta, status roachpb.TransactionStatus) (err error) {
	var poison uint32
	if shouldPoison {
		switch status {
		case roachpb.PENDING:
			// A SNAPSHOT transaction doesn't care if we push its intent.
			if txn.Isolation != roachpb.SNAPSHOT {
				poison = SequencePoisonRestart
			}
		default:
			// When committing or aborting, we don't want the transaction (or accidental
			// replays) to come back.
			poison = SequencePoisonAbort
		}
	}

	// Clear out before (maybe) poisoning unless it's PENDING. No need for old
	// stuff to accumulate.
	if status != roachpb.PENDING || poison != 0 {
		if err := r.sequence.Del(batch, ms, txn.ID); err != nil {
			return err
		}
	}

	if poison == 0 {
		return nil
	}

	// The snake bites.
	return r.sequence.Put(batch, ms, txn.ID, txn.Epoch, poison,
		txn.Key, txn.Timestamp, nil)
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func (r *Replica) ResolveIntent(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.ResolveIntentRequest) (roachpb.ResolveIntentResponse, error) {
	var reply roachpb.ResolveIntentResponse

	intent := roachpb.Intent{
		Span:   args.Span,
		Txn:    args.IntentTxn,
		Status: args.Status,
	}
	if err := engine.MVCCResolveWriteIntent(batch, ms, intent); err != nil {
		return reply, err
	}
	return reply, r.clearSequenceCache(batch, ms, args.Poison, args.IntentTxn, intent.Status)
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func (r *Replica) ResolveIntentRange(batch engine.Engine, ms *engine.MVCCStats,
	h roachpb.Header, args roachpb.ResolveIntentRangeRequest) (roachpb.ResolveIntentRangeResponse, error) {
	var reply roachpb.ResolveIntentRangeResponse

	intent := roachpb.Intent{
		Span:   args.Span,
		Txn:    args.IntentTxn,
		Status: args.Status,
	}

	if _, err := engine.MVCCResolveWriteIntentRange(batch, ms, intent, 0); err != nil {
		return reply, err
	}
	return reply, r.clearSequenceCache(batch, ms, args.Poison, args.IntentTxn, intent.Status)
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used
// by CockroachDB for the efficient accumulation of certain
// values. Due to the difficulty of making these operations
// transactional, merges are not currently exposed directly to
// clients. Merged values are explicitly not MVCC data.
func (r *Replica) Merge(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.MergeRequest) (roachpb.MergeResponse, error) {
	var reply roachpb.MergeResponse

	return reply, engine.MVCCMerge(batch, ms, args.Key, h.Timestamp, args.Value)
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func (r *Replica) TruncateLog(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.TruncateLogRequest) (roachpb.TruncateLogResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply roachpb.TruncateLogResponse

	// After a merge, it's possible that this request was sent to the wrong
	// range based on the start key. This will cancel the request if this is not
	// the range specified in the request body.
	if r.RangeID != args.RangeID {
		log.Infof("range %d: attempting to truncate raft logs for another range %d. Normally this is due to a merge and can be ignored.",
			r.RangeID, args.RangeID)
		return reply, nil
	}

	// Have we already truncated this log? If so, just return without an error.
	firstIndex, err := r.FirstIndex()
	if err != nil {
		return reply, err
	}

	if firstIndex >= args.Index {
		if log.V(3) {
			log.Infof("range %d: attempting to truncate previously truncated raft log. FirstIndex:%d, TruncateFrom:%d",
				r.RangeID, firstIndex, args.Index)
		}
		return reply, nil
	}

	// args.Index is the first index to keep.
	term, err := r.Term(args.Index - 1)
	if err != nil {
		return reply, err
	}
	start := keys.RaftLogKey(r.RangeID, 0)
	end := keys.RaftLogKey(r.RangeID, args.Index)
	if err = batch.Iterate(engine.MakeMVCCMetadataKey(start), engine.MakeMVCCMetadataKey(end),
		func(kv engine.MVCCKeyValue) (bool, error) {
			return false, batch.Clear(kv.Key)
		}); err != nil {
		return reply, err
	}
	tState := roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}
	return reply, engine.MVCCPutProto(batch, ms, keys.RaftTruncatedStateKey(r.RangeID), roachpb.ZeroTimestamp, nil, &tState)
}

// LeaderLease sets the leader lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range leader are commenced, including
// clearing the command queue and timestamp cache.
func (r *Replica) LeaderLease(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.LeaderLeaseRequest) (roachpb.LeaderLeaseResponse, error) {
	// maybeGossipSystemConfig cannot be called while the replica is locked,
	// so we defer it here so it is called once the replica lock is released.
	defer r.maybeGossipSystemConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply roachpb.LeaderLeaseResponse

	prevLease := r.mu.leaderLease
	isExtension := prevLease.Replica.StoreID == args.Lease.Replica.StoreID
	effectiveStart := args.Lease.Start
	// We return this error in "normal" lease-overlap related failures.
	rErr := &roachpb.LeaseRejectedError{
		Existing:  *prevLease,
		Requested: args.Lease,
	}

	// Verify details of new lease request. The start of this lease must
	// obviously precede its expiration.
	if !args.Lease.Start.Less(args.Lease.Expiration) {
		rErr.Message = "expiration precedes start"
		return reply, rErr
	}

	// Verify that requestion replica is part of the current replica set.
	if idx, _ := r.mu.desc.FindReplica(args.Lease.Replica.StoreID); idx == -1 {
		rErr.Message = "replica not found"
		return reply, rErr
	}

	// Wind the start timestamp back as far towards the previous lease as we
	// can. That'll make sure that when multiple leases are requested out of
	// order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed unless
	// they have a "real" issue with a previous lease. Example: Assuming no
	// previous lease, one request for [5, 15) followed by one for [0, 15)
	// would fail without this optimization. With it, the first request
	// effectively gets the lease for [0, 15), which the second one can commit
	// again (even extending your own lease is possible; see below).
	//
	// If this is our lease (or no prior lease exists), we effectively absorb
	// the old lease. This allows multiple requests from the same replica to
	// merge without ticking away from the minimal common start timestamp. It
	// also has the positive side-effect of fixing #3561, which was caused by
	// the absence of replay protection.
	if prevLease.Replica.StoreID == 0 || isExtension {
		effectiveStart.Backward(prevLease.Start)
	} else {
		effectiveStart.Backward(prevLease.Expiration.Next())
	}

	if isExtension {
		if effectiveStart.Less(prevLease.Start) {
			rErr.Message = "extension moved start timestamp backwards"
			return reply, rErr
		}
		// Note that the lease expiration can be shortened by the holder.
		// This could be used to effect a faster lease handoff.
	} else if effectiveStart.Less(prevLease.Expiration) {
		rErr.Message = "requested lease overlaps previous lease"
		return reply, rErr
	}

	args.Lease.Start = effectiveStart

	// Store the lease to disk & in-memory.
	if err := engine.MVCCPutProto(batch, ms, keys.RangeLeaderLeaseKey(r.RangeID), roachpb.ZeroTimestamp, nil, &args.Lease); err != nil {
		return reply, err
	}
	r.mu.leaderLease = &args.Lease

	// If this replica is a new holder of the lease, update the
	// low water mark in the timestamp cache. We add the maximum
	// clock offset to account for any difference in clocks
	// between the expiration (set by a remote node) and this
	// node.
	if r.mu.leaderLease.Replica.StoreID == r.store.StoreID() &&
		prevLease.Replica.StoreID != r.mu.leaderLease.Replica.StoreID {
		r.mu.tsCache.SetLowWater(prevLease.Expiration.Add(int64(r.store.Clock().MaxOffset()), 0))
		log.Infof("range %d: new leader lease %s", r.RangeID, args.Lease)
	}

	return reply, nil
}

// CheckConsistency runs a consistency check on the range. It first applies
// a ComputeChecksum command on the range. It then applies a VerifyChecksum
// command passing along a locally computed checksum for the range.
func (r *Replica) CheckConsistency(args roachpb.CheckConsistencyRequest, desc *roachpb.RangeDescriptor) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	key := desc.StartKey.AsRawKey()
	endKey := desc.EndKey.AsRawKey()
	notifyChan := make(chan []byte, 1)
	id := uuid.MakeV4()
	r.setChecksumNotify(id, notifyChan)
	// Send a ComputeChecksum to all the replicas of the range.
	start := time.Now()
	{
		var ba roachpb.BatchRequest
		ba.RangeID = r.Desc().RangeID
		checkArgs := &roachpb.ComputeChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			Version:    replicaChecksumVersion,
			ChecksumID: id,
		}
		ba.Add(checkArgs)
		_, pErr := r.Send(r.context(), ba)
		if pErr != nil {
			return roachpb.CheckConsistencyResponse{}, pErr
		}
	}
	// wait for local checksum and collect it.
	checksum := <-notifyChan

	if checksum == nil {
		// Don't bother verifying the checksum.
		return roachpb.CheckConsistencyResponse{}, roachpb.NewErrorf("unable to compute checksum for range [%v, %v]", key, endKey)
	}

	// Wait for a bit to improve the probability that all
	// the replicas have computed their checksum. We do this
	// because VerifyChecksum blocks on every replica until the
	// computed checksum is available.
	computeChecksumDuration := time.Since(start)
	time.Sleep(computeChecksumDuration)
	// Send a VerifyChecksum to all the replicas of the range.
	{
		var ba roachpb.BatchRequest
		ba.RangeID = r.Desc().RangeID
		checkArgs := &roachpb.VerifyChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			Version:    replicaChecksumVersion,
			ChecksumID: id,
			Checksum:   checksum,
		}
		ba.Add(checkArgs)
		_, pErr := r.Send(r.context(), ba)
		if pErr != nil {
			return roachpb.CheckConsistencyResponse{}, pErr
		}
	}
	return roachpb.CheckConsistencyResponse{}, nil
}

const (
	replicaChecksumVersion    = 0
	replicaChecksumGCInterval = time.Hour
)

// setChecksumNotify sets a notification channel on which the checksum
// from the result of ComputeChecksum is sent.
func (r *Replica) setChecksumNotify(id uuid.UUID, notify chan []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.checksumNotify[id]; ok {
		panic(fmt.Sprintf("id %v already exists", id))
	}
	r.mu.checksumNotify[id] = notify
	return
}

// computeChecksumDone adds the computed checksum, sets a deadline for GCing the
// checksum, and sends out a notification.
func (r *Replica) computeChecksumDone(id uuid.UUID, sha []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.mu.checksums[id]; ok {
		c.ok = true
		c.checksum = sha
		c.gcTimestamp = time.Now().Add(replicaChecksumGCInterval)
		r.mu.checksums[id] = c
	} else {
		panic("checksum has been GCed")
	}
	notify := r.mu.checksumNotify[id]
	delete(r.mu.checksumNotify, id)
	if notify != nil {
		notify <- sha
	}
}

// ComputeChecksum starts the process of computing a checksum on the
// replica at a particular snapshot. The checksum is later verified
// through the VerifyChecksum request.
func (r *Replica) ComputeChecksum(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.ComputeChecksumRequest) (roachpb.ComputeChecksumResponse, error) {
	if args.Version != replicaChecksumVersion {
		log.Errorf("Incompatible versions: e=%d, v=%d", replicaChecksumVersion, args.Version)
		return roachpb.ComputeChecksumResponse{}, nil
	}
	stopper := r.store.Stopper()
	id := args.ChecksumID
	now := time.Now()
	r.mu.Lock()
	if _, ok := r.mu.checksums[id]; ok {
		// A previous attempt was made to compute the checksum.
		r.mu.Unlock()
		return roachpb.ComputeChecksumResponse{}, nil
	}

	// GC old entries.
	var oldEntries []uuid.UUID
	for id, val := range r.mu.checksums {
		// The timestamp is only valid when the checksum is set.
		if val.checksum != nil && now.After(val.gcTimestamp) {
			oldEntries = append(oldEntries, id)
		}
	}
	for _, id := range oldEntries {
		delete(r.mu.checksums, id)
	}

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[id] = replicaChecksum{}
	desc := *r.mu.desc
	r.mu.Unlock()
	snap := r.store.NewSnapshot()

	// Compute SHA asynchronously and store it in a map by UUID.
	if !stopper.RunAsyncTask(func() {
		defer snap.Close()
		sha, err := r.sha512(desc, snap)
		if err != nil {
			log.Error(err)
			sha = nil
		}
		r.computeChecksumDone(id, sha)
	}) {
		defer snap.Close()
		// Set checksum to nil.
		r.computeChecksumDone(id, nil)
	}
	return roachpb.ComputeChecksumResponse{}, nil
}

// sha512 computes the SHA512 hash of all the replica data at the snapshot.
func (r *Replica) sha512(desc roachpb.RangeDescriptor, snap engine.Engine) ([]byte, error) {
	hasher := sha512.New()
	// Iterate over all the data in the range.
	iter := newReplicaDataIterator(&desc, snap, true /* replicatedOnly */)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		// Encode the length of the key and value.
		if err := binary.Write(hasher, binary.LittleEndian, int64(len(key.Key))); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, int64(len(value))); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(key.Key); err != nil {
			return nil, err
		}
		timestamp, err := key.Timestamp.Marshal()
		if err != nil {
			return nil, err
		}
		if _, err := hasher.Write(timestamp); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(value); err != nil {
			return nil, err
		}
	}
	sha := make([]byte, 0, sha512.Size)
	return hasher.Sum(sha), nil
}

// maybeSetChecksumNotify may set a channel to receive a checksum
// notification if the checksum is unavailable.
func (r *Replica) maybeSetChecksumNotify(id uuid.UUID) chan []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.mu.checksums[id]; ok && !c.ok {
		// Checksum hasn't been computed yet; set notification.
		if r.mu.checksumNotify[id] != nil {
			panic("existing notification exists")
		}
		notify := make(chan []byte, 1)
		r.mu.checksumNotify[id] = notify
		return notify
	}
	return nil
}

// VerifyChecksum verifies the checksum that was computed through a
// ComputeChecksum request.
func (r *Replica) VerifyChecksum(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.VerifyChecksumRequest) (roachpb.VerifyChecksumResponse, error) {
	if args.Version != replicaChecksumVersion {
		log.Errorf("Incompatible versions: e=%d, v=%d", replicaChecksumVersion, args.Version)
		return roachpb.VerifyChecksumResponse{}, nil
	}
	id := args.ChecksumID
	notify := r.maybeSetChecksumNotify(id)
	if notify != nil {
		now := time.Now()
		<-notify
		if log.V(1) {
			log.Info("waited for compute checksum for %s", time.Since(now))
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.mu.checksums[id]; ok {
		if c.ok {
			if c.checksum != nil && !bytes.Equal(c.checksum, args.Checksum) {
				if p := r.store.ctx.TestingMocker.BadChecksumPanic; p != nil {
					p()
				} else {
					// TODO(.*): see #5051.
					log.Errorf("checksum mismatch: e = %x, v = %x", args.Checksum, c.checksum)
				}
			}
		} else {
			panic("received checksum notification but no checksum")
		}
	}
	return roachpb.VerifyChecksumResponse{}, nil
}

// AdminSplit divides the range into into two ranges, using either
// args.SplitKey (if provided) or an internally computed key that aims to
// roughly equipartition the range by size. The split is done inside of
// a distributed txn which writes updated and new range descriptors, and
// updates the range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a split trigger
// carried out as part of the commit of that transaction.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. An
// operation which might split a range should obtain a copy of the range's
// current descriptor before making the decision to split. If the decision is
// affirmative the descriptor is passed to AdminSplit, which performs a
// Conditional Put on the RangeDescriptor to ensure that no other operation has
// modified the range in the time the decision was being made.
// TODO(tschottdorf): should assert that split key is not a local key.
func (r *Replica) AdminSplit(ctx context.Context, args roachpb.AdminSplitRequest, desc *roachpb.RangeDescriptor) (roachpb.AdminSplitResponse, *roachpb.Error) {
	var reply roachpb.AdminSplitResponse
	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	sp.LogEvent("split begins")
	var splitKey roachpb.RKey
	{
		foundSplitKey := args.SplitKey
		if len(foundSplitKey) == 0 {
			snap := r.store.NewSnapshot()
			defer snap.Close()
			var err error
			foundSplitKey, err = engine.MVCCFindSplitKey(snap, desc.RangeID, desc.StartKey, desc.EndKey)
			if err != nil {
				return reply, roachpb.NewErrorf("unable to determine split key: %s", err)
			}
		} else if !r.ContainsKey(foundSplitKey) {
			return reply, roachpb.NewError(roachpb.NewRangeKeyMismatchError(args.SplitKey, args.SplitKey, desc))
		}

		foundSplitKey, err := keys.MakeSplitKey(foundSplitKey)
		if err != nil {
			return reply, roachpb.NewErrorf("cannot split range at key %s: %v", splitKey, err)
		}

		splitKey = keys.Addr(foundSplitKey)
		if !splitKey.Equal(foundSplitKey) {
			return reply, roachpb.NewErrorf("cannot split range at range-local key %s", splitKey)
		}
		if !engine.IsValidSplitKey(foundSplitKey) {
			return reply, roachpb.NewErrorf("cannot split range at key %s", splitKey)
		}
	}

	// First verify this condition so that it will not return
	// roachpb.NewRangeKeyMismatchError if splitKey equals to desc.EndKey,
	// otherwise it will cause infinite retry loop.
	if desc.StartKey.Equal(splitKey) || desc.EndKey.Equal(splitKey) {
		return reply, roachpb.NewErrorf("range is already split at key %s", splitKey)
	}
	sp.LogEvent("found split key")

	// Create new range descriptor with newly-allocated replica IDs and Range IDs.
	newDesc, err := r.store.NewRangeDescriptor(splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, roachpb.NewErrorf("unable to allocate new range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = splitKey

	log.Infof("initiating a split of %s at key %s", r, splitKey)

	if err := r.store.DB().Txn(func(txn *client.Txn) *roachpb.Error {
		sp.LogEvent("split closure begins")
		defer sp.LogEvent("split closure ends")
		// Create range descriptor for second half of split.
		// Note that this put must go first in order to locate the
		// transaction record on the correct range.
		b := &client.Batch{}
		desc1Key := keys.RangeDescriptorKey(newDesc.StartKey)
		if err := updateRangeDescriptor(b, desc1Key, nil, newDesc); err != nil {
			return roachpb.NewError(err)
		}
		// Update existing range descriptor for first half of split.
		desc2Key := keys.RangeDescriptorKey(updatedDesc.StartKey)
		if err := updateRangeDescriptor(b, desc2Key, desc, &updatedDesc); err != nil {
			return roachpb.NewError(err)
		}
		// Update range descriptor addressing record(s).
		if err := splitRangeAddressing(b, newDesc, &updatedDesc); err != nil {
			return roachpb.NewError(err)
		}
		if err := txn.Run(b); err != nil {
			return err
		}
		// Log the split into the range event log.
		if err := r.store.logSplit(txn, updatedDesc, *newDesc); err != nil {
			return err
		}
		// Update the RangeTree.
		b = &client.Batch{}
		if pErr := InsertRange(txn, b, newDesc.StartKey); pErr != nil {
			return pErr
		}
		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		b.InternalAddRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					UpdatedDesc: updatedDesc,
					NewDesc:     *newDesc,
					// Designate this store as the preferred leader for the new
					// range. The choice of store here doesn't matter for
					// correctness, but for best performance it should be one
					// that we believe is currently up.
					InitialLeaderStoreID: r.store.StoreID(),
				},
			},
		})
		sp.LogEvent("attempting commit")
		return txn.Run(b)
	}); err != nil {
		return reply, roachpb.NewErrorf("split at key %s failed: %s", splitKey, err)
	}

	return reply, nil
}

// splitTrigger is called on a successful commit of an AdminSplit
// transaction. It copies the sequence cache for the new range and
// recomputes stats for both the existing, updated range and the new
// range.
func (r *Replica) splitTrigger(batch engine.Engine, ms *engine.MVCCStats, split *roachpb.SplitTrigger) error {
	// TODO(tschottdorf): should have an incoming context from the corresponding
	// EndTransaction, but the plumbing has not been done yet.
	sp := r.store.Tracer().StartSpan("split")
	defer sp.Finish()
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, split.UpdatedDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.NewDesc.EndKey) {
		return util.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.UpdatedDesc.StartKey, split.UpdatedDesc.EndKey,
			split.NewDesc.StartKey, split.NewDesc.EndKey, r)
	}

	// Preserve stats for presplit range and begin computing stats delta
	// for current transaction.
	origStats := r.GetMVCCStats()
	deltaMs := *ms

	// Account for MVCCStats' own contribution to the new range's statistics.
	if err := deltaMs.AccountForSelf(split.NewDesc.RangeID); err != nil {
		return util.Errorf("unable to account for MVCCStats's own stats impact: %s", err)
	}

	// Compute stats for updated range.
	now := r.store.Clock().Timestamp()
	leftMs, err := ComputeStatsForRange(&split.UpdatedDesc, batch, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for updated range after split: %s", err)
	}
	sp.LogEvent("computed stats for old range")
	if err := r.stats.SetMVCCStats(batch, leftMs); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Copy the last replica GC and verification timestamps. These
	// values are unreplicated, which is why the MVCC stats are set to
	// nil on calls to MVCCPutProto.
	replicaGCTS, err := r.getLastReplicaGCTimestamp()
	if err != nil {
		return util.Errorf("unable to fetch last replica GC timestamp: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeLastReplicaGCTimestampKey(split.NewDesc.RangeID), roachpb.ZeroTimestamp, nil, &replicaGCTS); err != nil {
		return util.Errorf("unable to copy last replica GC timestamp: %s", err)
	}
	verifyTS, err := r.getLastVerificationTimestamp()
	if err != nil {
		return util.Errorf("unable to fetch last verification timestamp: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeLastVerificationTimestampKey(split.NewDesc.RangeID), roachpb.ZeroTimestamp, nil, &verifyTS); err != nil {
		return util.Errorf("unable to copy last verification timestamp: %s", err)
	}

	// Initialize the new range's sequence cache by copying the original's.
	seqCount, err := r.sequence.CopyInto(batch, &deltaMs, split.NewDesc.RangeID)
	if err != nil {
		// TODO(tschottdorf): ReplicaCorruptionError.
		return util.Errorf("unable to copy sequence cache to new split range: %s", err)
	}
	sp.LogEvent(fmt.Sprintf("copied sequence cache (%d entries)", seqCount))

	// Add the new split replica to the store. This step atomically
	// updates the EndKey of the updated replica and also adds the
	// new replica to the store's replica map.
	newRng, err := NewReplica(&split.NewDesc, r.store, 0)
	if err != nil {
		return err
	}

	rightMs := deltaMs
	// Add in the original range's stats.
	rightMs.Add(origStats)
	// Remove stats from the left side of the split.
	rightMs.Subtract(leftMs)
	if err = newRng.stats.SetMVCCStats(batch, rightMs); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}
	sp.LogEvent("computed stats for new range")

	// Copy the timestamp cache into the new range.
	r.mu.Lock()
	newRng.mu.Lock()
	r.mu.tsCache.MergeInto(newRng.mu.tsCache, true /* clear */)
	newRng.mu.Unlock()
	r.mu.Unlock()
	sp.LogEvent("copied timestamp cache")

	// Note: you must not use the trace inside of this defer since it may
	// run after the trace has already completed.
	batch.Defer(func() {
		if err := r.store.SplitRange(r, newRng); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update Store after split: %s", err)
		}

		// Update store stats with difference in stats before and after split.
		r.store.metrics.addMVCCStats(deltaMs)

		// To avoid leaving the new range unavailable as it waits to elect
		// its leader, one (and only one) of the nodes should start an
		// election as soon as the split is processed.
		//
		// If there is only one replica, raft instantly makes it the leader.
		// Otherwise, we must trigger a campaign here.
		//
		// TODO(bdarnell): The asynchronous campaign can cause a problem
		// with merges, by recreating a replica that should have been
		// destroyed. This will probably be addressed as a part of the
		// general work to be done for merges
		// (https://github.com/cockroachdb/cockroach/issues/2433), but for
		// now we're safe because we only perform the asynchronous
		// campaign when there are multiple replicas, and we only perform
		// merges in testing scenarios with a single replica.
		// Note that in multi-node scenarios the async campaign is safe
		// because it has exactly the same behavior as an incoming message
		// from another node; the problem here is only with merges.
		if len(split.NewDesc.Replicas) > 1 && r.store.StoreID() == split.InitialLeaderStoreID {
			// Schedule the campaign a short time in the future. As
			// followers process the split, they destroy and recreate their
			// raft groups, which can cause messages to be dropped. In
			// general a shorter delay (perhaps all the way down to zero) is
			// better in production, because the race is rare and the worst
			// case scenario is that we simply wait for an election timeout.
			// However, the test for this feature disables election timeouts
			// and relies solely on this campaign trigger, so it is unacceptably
			// flaky without a bit of a delay.
			r.store.stopper.RunAsyncTask(func() {
				time.Sleep(10 * time.Millisecond)
				// TODO(bdarnell): make sure newRng hasn't been removed
				newRng.mu.Lock()
				_ = newRng.mu.raftGroup.Campaign()
				newRng.mu.Unlock()
			})
		}
	})

	return nil
}

// AdminMerge extends this range to subsume the range that comes next in
// the key space. The merge is performed inside of a distributed
// transaction which writes the updated range descriptor for the subsuming range
// and deletes the range descriptor for the subsumed one. It also updates the
// range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a merge trigger
// carried out as part of the commit of that transaction.
// A merge requires that the two ranges are collocated on the same set of replicas.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) AdminMerge(ctx context.Context, args roachpb.AdminMergeRequest, origLeftDesc *roachpb.RangeDescriptor) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse
	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
		// Merging the final range doesn't make sense.
		return reply, roachpb.NewErrorf("cannot merge final range")
	}

	updatedLeftDesc := *origLeftDesc

	// Lookup subsumed (right) range. This really belongs inside the
	// transaction for consistency, but it is important (for transaction
	// record placement) that the first action inside the transaction is
	// the conditional put to change the left descriptor's end key. We
	// look up the descriptor here only to get the new end key and then
	// repeat the lookup inside the transaction.
	{
		rightRng := r.store.LookupReplica(origLeftDesc.EndKey, nil)
		if rightRng == nil {
			return reply, roachpb.NewErrorf("ranges not collocated")
		}

		updatedLeftDesc.EndKey = rightRng.Desc().EndKey
		log.Infof("initiating a merge of %s into %s", rightRng, r)
	}

	if err := r.store.DB().Txn(func(txn *client.Txn) *roachpb.Error {
		sp.LogEvent("merge closure begins")
		// Update the range descriptor for the receiving range.
		{
			b := &client.Batch{}
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, origLeftDesc, &updatedLeftDesc); err != nil {
				return roachpb.NewError(err)
			}
			// Commit this batch on its own to ensure that the transaction record
			// is created in the right place (our triggers rely on this).
			sp.LogEvent("updating left descriptor")
			if err := txn.Run(b); err != nil {
				return err
			}
		}

		// Do a consistent read of the second range descriptor.
		rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)
		var rightDesc roachpb.RangeDescriptor
		if err := txn.GetProto(rightDescKey, &rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return roachpb.NewErrorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if !bytes.Equal(rightDesc.EndKey, updatedLeftDesc.EndKey) {
			// This merge raced with a split of the right-hand range.
			// TODO(bdarnell): needs a test.
			return roachpb.NewErrorf("range changed during merge; %s != %s", rightDesc.EndKey, updatedLeftDesc.EndKey)
		}
		if !replicaSetsEqual(origLeftDesc.Replicas, rightDesc.Replicas) {
			return roachpb.NewErrorf("ranges not collocated")
		}

		b := &client.Batch{}

		// Remove the range descriptor for the deleted range.
		b.Del(rightDescKey)

		if err := mergeRangeAddressing(b, origLeftDesc, &updatedLeftDesc); err != nil {
			return roachpb.NewError(err)
		}

		// Update the RangeTree.
		if pErr := DeleteRange(txn, b, rightDesc.StartKey); pErr != nil {
			return pErr
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		b.InternalAddRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				MergeTrigger: &roachpb.MergeTrigger{
					UpdatedDesc:  updatedLeftDesc,
					SubsumedDesc: rightDesc,
				},
			},
		})
		sp.LogEvent("attempting commit")
		return txn.Run(b)
	}); err != nil {
		return reply, roachpb.NewErrorf("merge of range into %d failed: %s", origLeftDesc.RangeID, err)
	}

	return reply, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
func (r *Replica) mergeTrigger(batch engine.Engine, ms *engine.MVCCStats, merge *roachpb.MergeTrigger) error {
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, merge.UpdatedDesc.StartKey) {
		return util.Errorf("range and updated range start keys do not match: %s != %s",
			desc.StartKey, merge.UpdatedDesc.StartKey)
	}

	if !desc.EndKey.Less(merge.UpdatedDesc.EndKey) {
		return util.Errorf("range end key is not less than the post merge end key: %s >= %s",
			desc.EndKey, merge.UpdatedDesc.EndKey)
	}

	subsumedRangeID := merge.SubsumedDesc.RangeID
	if subsumedRangeID <= 0 {
		return util.Errorf("subsumed range ID must be provided: %d", subsumedRangeID)
	}

	// Compute stats for premerged range, including current transaction.
	var mergedMs = r.GetMVCCStats()
	mergedMs.Add(*ms)

	// Add in stats for right half of merge, excluding system-local stats, which
	// will need to be recomputed.
	var rightMs engine.MVCCStats
	if err := engine.MVCCGetRangeStats(batch, subsumedRangeID, &rightMs); err != nil {
		return err
	}
	rightMs.SysBytes, rightMs.SysCount = 0, 0
	mergedMs.Add(rightMs)

	// Copy the subsumed range's sequence cache to the subsuming one.
	_, err := r.sequence.CopyFrom(batch, &mergedMs, subsumedRangeID)
	if err != nil {
		return util.Errorf("unable to copy sequence cache to new split range: %s", err)
	}

	// Remove the subsumed range's metadata. Note that we don't need to
	// keep track of stats here, because we already set the right range's
	// system-local stats contribution to 0.
	localRangeIDKeyPrefix := keys.MakeRangeIDPrefix(subsumedRangeID)
	if _, err := engine.MVCCDeleteRange(batch, nil, localRangeIDKeyPrefix, localRangeIDKeyPrefix.PrefixEnd(), 0, roachpb.ZeroTimestamp, nil, false); err != nil {
		return util.Errorf("cannot remove range metadata %s", err)
	}

	// Add in the stats for the subsumed range's range keys.
	iter := batch.NewIterator(nil)
	defer iter.Close()
	localRangeKeyStart := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.SubsumedDesc.StartKey))
	localRangeKeyEnd := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.SubsumedDesc.EndKey))
	now := r.store.Clock().Timestamp()
	msRange, err := iter.ComputeStats(localRangeKeyStart, localRangeKeyEnd, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute subsumed range's local stats: %s", err)
	}
	mergedMs.Add(msRange)

	// Set stats for updated range.
	if err = r.stats.SetMVCCStats(batch, mergedMs); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Clear the timestamp cache. In the case that this replica and the
	// subsumed replica each held their respective leader leases, we
	// could merge the timestamp caches for efficiency. But it's unlikely
	// and not worth the extra logic and potential for error.
	r.mu.Lock()
	r.mu.tsCache.Clear(r.store.Clock())
	r.mu.Unlock()

	batch.Defer(func() {
		if err := r.store.MergeRange(r, merge.UpdatedDesc.EndKey, subsumedRangeID); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update store after merging range: %s", err)
		}
	})
	return nil
}

func (r *Replica) changeReplicasTrigger(batch engine.Engine, change *roachpb.ChangeReplicasTrigger) error {
	cpy := *r.Desc()
	cpy.Replicas = change.UpdatedReplicas
	cpy.NextReplicaID = change.NextReplicaID
	if err := r.setDesc(&cpy); err != nil {
		return err
	}
	// If we're removing the current replica, add it to the range GC queue.
	if change.ChangeType == roachpb.REMOVE_REPLICA && r.store.StoreID() == change.Replica.StoreID {
		// Defer this to make it run as late as possible, maximizing the chances
		// that the other nodes have finished this command as well (since
		// processing the removal from the queue looks up the Range at the
		// leader, being too early here turns this into a no-op).
		batch.Defer(func() {
			if err := r.store.replicaGCQueue.Add(r, 1.0); err != nil {
				// Log the error; this shouldn't prevent the commit; the range
				// will be GC'd eventually.
				log.Errorf("unable to add range %s to GC queue: %s", r, err)
			}
		})
	}
	return nil
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) ChangeReplicas(changeType roachpb.ReplicaChangeType, replica roachpb.ReplicaDescriptor, desc *roachpb.RangeDescriptor) error {

	// Validate the request and prepare the new descriptor.
	updatedDesc := *desc
	updatedDesc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)
	err := func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		found := -1       // tracks NodeID && StoreID
		nodeUsed := false // tracks NodeID only
		for i, existingRep := range desc.Replicas {
			nodeUsed = nodeUsed || existingRep.NodeID == replica.NodeID
			if existingRep.NodeID == replica.NodeID &&
				existingRep.StoreID == replica.StoreID {
				found = i
				replica.ReplicaID = existingRep.ReplicaID
				break
			}
		}
		if changeType == roachpb.ADD_REPLICA {
			// If the replica exists on the remote node, no matter in which store,
			// abort the replica add.
			if nodeUsed {
				return util.Errorf("adding replica %v which is already present in range %d",
					replica, desc.RangeID)
			}
			replica.ReplicaID = updatedDesc.NextReplicaID
			updatedDesc.NextReplicaID++
			updatedDesc.Replicas = append(updatedDesc.Replicas, replica)
		} else if changeType == roachpb.REMOVE_REPLICA {
			// If that exact node-store combination does not have the replica,
			// abort the removal.
			if found == -1 {
				return util.Errorf("removing replica %v which is not present in range %d",
					replica, desc.RangeID)
			}
			updatedDesc.Replicas[found] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
			updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
		}
		return nil
	}()
	if err != nil {
		return err
	}

	pErr := r.store.DB().Txn(func(txn *client.Txn) *roachpb.Error {
		// Important: the range descriptor must be the first thing touched in the transaction
		// so the transaction record is co-located with the range being modified.
		b := &client.Batch{}
		descKey := keys.RangeDescriptorKey(updatedDesc.StartKey)

		if err := updateRangeDescriptor(b, descKey, desc, &updatedDesc); err != nil {
			return roachpb.NewError(err)
		}

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, &updatedDesc); err != nil {
			return roachpb.NewError(err)
		}

		// Run transaction up to this point.
		if err := txn.Run(b); err != nil {
			return err
		}

		// Log replica change into range event log.
		if err := r.store.logChange(txn, changeType, replica, updatedDesc); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		b = &client.Batch{}
		b.InternalAddRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
					ChangeType:      changeType,
					Replica:         replica,
					UpdatedReplicas: updatedDesc.Replicas,
					NextReplicaID:   updatedDesc.NextReplicaID,
				},
			},
		})
		return txn.Run(b)
	})
	if pErr != nil {
		return util.Errorf("change replicas of %d failed: %s", desc.RangeID, pErr)
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
// Note that in addition to using this method to update the on-disk range
// descriptor, a CommitTrigger must be used to update the in-memory
// descriptor; it will not automatically be copied from newDesc.
// TODO(bdarnell): store the entire RangeDescriptor in the CommitTrigger
// and load it automatically instead of reconstructing individual
// changes.
func updateRangeDescriptor(b *client.Batch, descKey roachpb.Key, oldDesc, newDesc *roachpb.RangeDescriptor) error {
	if err := newDesc.Validate(); err != nil {
		return err
	}
	// This is subtle: []byte(nil) != interface{}(nil). A []byte(nil) refers to
	// an empty value. An interface{}(nil) refers to a non-existent value. So
	// we're careful to construct an interface{}(nil) when oldDesc is nil.
	var oldValue interface{}
	if oldDesc != nil {
		oldBytes, err := proto.Marshal(oldDesc)
		if err != nil {
			return err
		}
		oldValue = oldBytes
	}
	newValue, err := proto.Marshal(newDesc)
	if err != nil {
		return err
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}
