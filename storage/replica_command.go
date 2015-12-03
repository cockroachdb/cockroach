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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
)

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command. It returns the response, an error,
// and a slice of intents that were skipped during execution.
// If an error is returned, any returned intents should still be resolved.
func (r *Replica) executeCmd(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.Request) (roachpb.Response, []roachpb.Intent, error) {
	// Verify key is contained within range here to catch any range split
	// or merge activity.
	ts := h.Timestamp

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return &roachpb.NoopResponse{}, nil, nil
	}

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, nil, err
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if TestingCommandFilter != nil {
		if err := TestingCommandFilter(args, h); err != nil {
			return nil, nil, err
		}
	}

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
		resp, intents, err = r.Scan(batch, h, *tArgs)
		reply = &resp
	case *roachpb.ReverseScanRequest:
		var resp roachpb.ReverseScanResponse
		resp, intents, err = r.ReverseScan(batch, h, *tArgs)
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
	default:
		err = util.Errorf("unrecognized command %s", args.Method())
	}

	if log.V(2) {
		log.Infof("executed %s command %+v: %+v, err=%s", args.Method(), args, reply, err)
	}

	// Update the node clock with the serviced request. This maintains a
	// high water mark for all ops serviced, so that received ops
	// without a timestamp specified are guaranteed one higher than any
	// op already executed for overlapping keys.
	r.store.Clock().Update(ts)

	// Propagate the request timestamp (which may have changed).
	// TODO(tschottdorf): really? Think this should be done by executeBatch.
	reply.Header().Timestamp = ts

	// A ReadWithinUncertaintyIntervalError contains the timestamp of the value
	// that provoked the conflict. However, we forward the timestamp to the
	// node's time here. The reason is that the caller (which is always
	// transactional when this error occurs) in our implementation wants to
	// use this information to extract a timestamp after which reads from
	// the nodes are causally consistent with the transaction. This allows
	// the node to be classified as without further uncertain reads for the
	// remainder of the transaction.
	// See the comment on roachpb.Transaction.CertainNodes.
	if tErr, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); ok {
		// Note that we can use this node's clock (which may be different from
		// other replicas') because this error attaches the existing timestamp
		// to the node itself when retrying.
		tErr.ExistingTimestamp.Forward(r.store.Clock().Now())
	}

	return reply, intents, err
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

	numDel, err := engine.MVCCDeleteRange(batch, ms, args.Key, args.EndKey, args.MaxEntriesToDelete, h.Timestamp, h.Txn)
	reply.NumDeleted = numDel
	return reply, err
}

// Scan scans the key range specified by start key through end key in ascending
// order up to some maximum number of results.
func (r *Replica) Scan(batch engine.Engine, h roachpb.Header, args roachpb.ScanRequest) (roachpb.ScanResponse, []roachpb.Intent, error) {
	var reply roachpb.ScanResponse

	rows, intents, err := engine.MVCCScan(batch, args.Key, args.EndKey, args.MaxResults, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	reply.Rows = rows
	return reply, intents, err
}

// ReverseScan scans the key range specified by start key through end key in
// descending order up to some maximum number of results.
func (r *Replica) ReverseScan(batch engine.Engine, h roachpb.Header, args roachpb.ReverseScanRequest) (roachpb.ReverseScanResponse, []roachpb.Intent, error) {
	var reply roachpb.ReverseScanResponse

	rows, intents, err := engine.MVCCReverseScan(batch, args.Key, args.EndKey, args.MaxResults, h.Timestamp,
		h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	reply.Rows = rows
	return reply, intents, err
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
		// Check whether someone has come in ahead and already aborted the
		// txn.
		if txn.Status == roachpb.ABORTED {
			return reply, roachpb.NewTransactionAbortedError(&txn)
		} else if txn.Status == roachpb.PENDING && h.Txn.Epoch > txn.Epoch {
			// On a transaction retry there will be an extant txn record but
			// this run should have an upgraded epoch. This is a pass
			// through to set the new transaction record.
		} else {
			return reply, roachpb.NewTransactionStatusError(txn, "non-aborted transaction exists already")
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

	deadline := args.Deadline
	deadlineLapsed := deadline != nil && deadline.Less(ts)

	if deadlineLapsed {
		reply.Txn.Status = roachpb.ABORTED
	}

	if deadlineLapsed {
		// FIXME(#3037):
		// If the deadline has lapsed, return all the intents for
		// resolution. Unfortunately, since we're (a) returning an error,
		// and (b) not able to write on error (see #1989), we can't write
		// ABORTED into the master transaction record, which remains
		// PENDING, and that's pretty bad.
		return reply, roachpb.AsIntents(args.IntentSpans, reply.Txn), roachpb.NewTransactionAbortedError(reply.Txn)
	}

	// Verify that we can either commit it or abort it (according
	// to args.Commit), and also that the Timestamp and Epoch have
	// not suffered regression.
	if reply.Txn.Status == roachpb.COMMITTED {
		return reply, nil, roachpb.NewTransactionStatusError(*reply.Txn, "already committed")
	} else if reply.Txn.Status == roachpb.ABORTED {
		// If the transaction was previously aborted by a concurrent
		// writer's push, any intents written are still open. It's only now
		// that we know them, so we return them all for asynchronous
		// resolution (we're currently not able to write on error, but
		// see #1989).
		return reply, roachpb.AsIntents(args.IntentSpans, reply.Txn), roachpb.NewTransactionAbortedError(reply.Txn)
	} else if h.Txn.Epoch < reply.Txn.Epoch {
		// TODO(tschottdorf): this leaves the Txn record (and more
		// importantly, intents) dangling; we can't currently write on
		// error. Would panic, but that makes TestEndTransactionWithErrors
		// awkward.
		return reply, nil, roachpb.NewTransactionStatusError(*reply.Txn, fmt.Sprintf("epoch regression: %d", h.Txn.Epoch))
	} else if h.Txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(h.Txn.OrigTimestamp) {
		// The transaction record can only ever be pushed forward, so it's an
		// error if somehow the transaction record has an earlier timestamp
		// than the original transaction timestamp.

		// TODO(tschottdorf): see above comment on epoch regression.
		return reply, nil, roachpb.NewTransactionStatusError(*reply.Txn, fmt.Sprintf("timestamp regression: %s", h.Txn.OrigTimestamp))
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
			return reply, nil, roachpb.NewTransactionRetryError(reply.Txn)
		}
		reply.Txn.Status = roachpb.COMMITTED
	} else {
		reply.Txn.Status = roachpb.ABORTED
	}

	// Resolve any explicit intents. All that are local to this range get
	// resolved synchronously in the same batch. The remainder are collected
	// and handed off to asynchronous processing.
	desc := *r.Desc()
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		desc = mergeTrigger.UpdatedDesc
	}

	var externalIntents []roachpb.Intent
	for _, span := range args.IntentSpans {
		if err := func() error {
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(desc, span.Key) {
					externalIntents = append(externalIntents, roachpb.Intent{Span: span, Txn: *reply.Txn})
					return nil
				}
				return engine.MVCCResolveWriteIntent(batch, ms,
					span.Key, reply.Txn.Timestamp, reply.Txn)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, desc)
			for _, span := range outSpans {
				externalIntents = append(externalIntents, roachpb.Intent{Span: span, Txn: *reply.Txn})
			}
			if inSpan != nil {
				_, err := engine.MVCCResolveWriteIntentRange(batch, ms,
					inSpan.Key, inSpan.EndKey, 0, reply.Txn.Timestamp, reply.Txn)
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
				*ms = engine.MVCCStats{} // clear stats, as split will recompute from scratch.
				if err := r.splitTrigger(batch, ct.SplitTrigger); err != nil {
					return err
				}
			}
			if ct.GetMergeTrigger() != nil {
				*ms = engine.MVCCStats{} // clear stats, as merge will recompute from scratch.
				if err := r.mergeTrigger(batch, ct.MergeTrigger); err != nil {
					return err
				}
			}
			if ct.GetChangeReplicasTrigger() != nil {
				if err := r.changeReplicasTrigger(ct.ChangeReplicasTrigger); err != nil {
					return err
				}
			}
			if ct.GetModifiedSpanTrigger() != nil {
				if ct.ModifiedSpanTrigger.SystemDBSpan {
					// Check if we need to gossip the system config.
					batch.Defer(r.maybeGossipSystemConfig)
				}
			}
			return nil
		}(); err != nil {
			r.readOnlyCmdMu.Unlock() // since the batch.Defer above won't run
			// TODO(tschottdorf): should an error here always amount to a
			// ReplicaCorruptionError?
			log.Errorf("Range %d transaction commit trigger fail: %s", r.Desc().RangeID, err)
			return reply, nil, err
		}
	}

	return reply, externalIntents, nil
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
			key, txn := intent.Key, &intent.Txn
			val, _, err := engine.MVCCGet(batch, key, txn.Timestamp, true, txn)
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
				kvs = []roachpb.KeyValue{{Key: key, Value: *val}}
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
	ts := h.Timestamp // all we're going to use from the header.

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
		txn.LastHeartbeat.Forward(ts)
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
	var reply roachpb.GCResponse

	// Garbage collect the specified keys by expiration timestamps.
	if err := engine.MVCCGarbageCollect(batch, ms, args.Keys, h.Timestamp); err != nil {
		return reply, err
	}

	// Store the GC metadata for this range.
	key := keys.RangeGCMetadataKey(r.Desc().RangeID)
	if err := engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, &args.GCMeta); err != nil {
		return reply, err
	}
	return reply, nil
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
// args.PushType. If args.PushType is ABORT_TXN, set txn.Status to
// ABORTED, and priority to one less than the pusher's priority and
// return success. If args.PushType is PUSH_TIMESTAMP, set
// txn.Timestamp to just after PushTo.
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Replica) PushTxn(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.PushTxnRequest) (roachpb.PushTxnResponse, error) {
	var reply roachpb.PushTxnResponse

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
	if ok {
		// Start with the persisted transaction record as final transaction.
		reply.PusheeTxn = *existTxn.Clone()
		// Upgrade the epoch, timestamp and priority as necessary.
		if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
			reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		}
		reply.PusheeTxn.Timestamp.Forward(args.PusheeTxn.Timestamp)
		if reply.PusheeTxn.Priority < args.PusheeTxn.Priority {
			reply.PusheeTxn.Priority = args.PusheeTxn.Priority
		}
	} else {
		// The transaction doesn't exist on disk; we're allowed to abort it.
		// TODO(tschottdorf): for GC purposes, don't always want to write
		// a new entry, but #3219 shows that we need to do so carefully.

		reply.PusheeTxn = *args.PusheeTxn.Clone()
		reply.PusheeTxn.Status = roachpb.ABORTED
		return reply, engine.MVCCPutProto(batch, ms, key, roachpb.ZeroTimestamp, nil, &reply.PusheeTxn)
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
		args.PushType = roachpb.ABORT_TXN
	} else if reply.PusheeTxn.Isolation == roachpb.SNAPSHOT && args.PushType == roachpb.PUSH_TIMESTAMP {
		if log.V(1) {
			log.Infof("pushing timestamp for snapshot isolation txn")
		}
		pusherWins = true
	} else if args.PushType == roachpb.CLEANUP_TXN {
		// If just attempting to cleanup old or already-committed txns, don't push.
		pusherWins = false
	} else if reply.PusheeTxn.Priority < priority ||
		(reply.PusheeTxn.Priority == priority && len(args.PusherTxn.ID) != 0 &&
			args.PusherTxn.Timestamp.Less(reply.PusheeTxn.Timestamp)) {
		// Pusher wins based on priority; if priorities are equal, order
		// by lower txn timestamp.
		if log.V(1) {
			log.Infof("pushing intent from txn with lower priority %s vs %d", reply.PusheeTxn, priority)
		}
		pusherWins = true
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(args.PusherTxn, reply.PusheeTxn)
		if log.V(1) {
			log.Info(err)
		}
		return reply, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.PushType == roachpb.ABORT_TXN {
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

// maybePoison inspects the given transaction and, if it's either been aborted
// or pushed despite being serializable, poisons the sequence cache entry
// accordingly so that the transaction will be forced to abort or restart,
// respectively, upon returning to this Range.
func (r *Replica) maybePoison(batch engine.Engine, shouldPoison bool, txn roachpb.Transaction) error {
	if !shouldPoison {
		return nil
	}
	var poison uint32
	switch txn.Status {
	case roachpb.ABORTED:
		poison = roachpb.SequencePoisonAbort
	case roachpb.PENDING:
		poison = roachpb.SequencePoisonRestart
		if txn.Isolation == roachpb.SNAPSHOT || txn.Timestamp.Equal(txn.OrigTimestamp) {
			return nil
		}
	default:
		return nil
	}

	return r.sequence.Put(batch, txn.ID, txn.Epoch, poison,
		txn.Key, txn.Timestamp, nil)
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func (r *Replica) ResolveIntent(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.ResolveIntentRequest) (roachpb.ResolveIntentResponse, error) {
	var reply roachpb.ResolveIntentResponse

	if err := engine.MVCCResolveWriteIntent(batch, ms, args.Key, h.Timestamp, &args.IntentTxn); err != nil {
		return reply, err
	}
	return reply, r.maybePoison(batch, args.Poison, args.IntentTxn)
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func (r *Replica) ResolveIntentRange(batch engine.Engine, ms *engine.MVCCStats,
	h roachpb.Header, args roachpb.ResolveIntentRangeRequest) (roachpb.ResolveIntentRangeResponse, error) {
	var reply roachpb.ResolveIntentRangeResponse

	if _, err := engine.MVCCResolveWriteIntentRange(batch, ms, args.Key, args.EndKey, 0, h.Timestamp, &args.IntentTxn); err != nil {
		return reply, err
	}
	return reply, r.maybePoison(batch, args.Poison, args.IntentTxn)
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used by
// Cockroach for the efficient accumulation of certain values. Due to the
// difficulty of making these operations transactional, merges are not currently
// exposed directly to clients. Merged values are explicitly not MVCC data.
func (r *Replica) Merge(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.MergeRequest) (roachpb.MergeResponse, error) {
	var reply roachpb.MergeResponse

	return reply, engine.MVCCMerge(batch, ms, args.Key, args.Value)
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func (r *Replica) TruncateLog(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.TruncateLogRequest) (roachpb.TruncateLogResponse, error) {
	var reply roachpb.TruncateLogResponse

	rangeID := r.Desc().RangeID

	// After a merge, it's possible that this request was sent to the wrong
	// range based on the start key. This will cancel the request if this is not
	// the range specified in the request body.
	if rangeID != args.RangeID {
		log.Infof("range %d: attempting to truncate raft logs for another range %d. Normally this is due to a merge and can be ignored.",
			rangeID, args.RangeID)
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
				rangeID, firstIndex, args.Index)
		}
		return reply, nil
	}

	// args.Index is the first index to keep.
	term, err := r.Term(args.Index - 1)
	if err != nil {
		return reply, err
	}
	start := keys.RaftLogKey(rangeID, 0)
	end := keys.RaftLogKey(rangeID, args.Index)
	if err = batch.Iterate(engine.MVCCEncodeKey(start), engine.MVCCEncodeKey(end), func(kv engine.MVCCKeyValue) (bool, error) {
		return false, batch.Clear(kv.Key)
	}); err != nil {
		return reply, err
	}
	tState := roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}
	return reply, engine.MVCCPutProto(batch, ms, keys.RaftTruncatedStateKey(rangeID), roachpb.ZeroTimestamp, nil, &tState)
}

// LeaderLease sets the leader lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range leader are commenced, including
// clearing the command queue and timestamp cache.
func (r *Replica) LeaderLease(batch engine.Engine, ms *engine.MVCCStats, h roachpb.Header, args roachpb.LeaderLeaseRequest) (roachpb.LeaderLeaseResponse, error) {
	var reply roachpb.LeaderLeaseResponse

	r.Lock()
	defer r.Unlock()

	prevLease := r.getLease()
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
	desc := r.Desc()
	if idx, _ := desc.FindReplica(args.Lease.Replica.StoreID); idx == -1 {
		rErr.Message = "replica not found"
		return reply, rErr
	}

	// Wind the start timestamp back as far to the previous lease's expiration
	// as we can. That'll make sure that when multiple leases are requested out
	// of order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed unless
	// they have a "real" issue with a previous lease. Example: Assuming no
	// previous lease, one request for [5, 15) followed by one for [0, 15)
	// would fail without this optimization. With it, the first request
	// effectively gets the lease for [0, 15), which the second one can commit
	// again (even extending your own lease is possible; see below).
	//
	// If no old lease exists or this is our lease, we don't need to add an
	// extra tick. This allows multiple requests from the same replica to
	// merge without ticking away from the minimal common start timestamp.
	if prevLease.Replica.StoreID == 0 || isExtension {
		// TODO(tschottdorf) Think about whether it'd be better to go all the
		// way back to prevLease.Start(), so that whenever the last lease is
		// the own one, the original start is preserved.
		effectiveStart.Backward(prevLease.Expiration)
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

	rangeID := desc.RangeID

	// Store the lease to disk & in-memory.
	if err := engine.MVCCPutProto(batch, ms, keys.RaftLeaderLeaseKey(rangeID), roachpb.ZeroTimestamp, nil, &args.Lease); err != nil {
		return reply, err
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(&args.Lease))

	// If this replica is a new holder of the lease, update the
	// low water mark in the timestamp cache. We add the maximum
	// clock offset to account for any difference in clocks
	// between the expiration (set by a remote node) and this
	// node.
	if r.getLease().Replica.StoreID == r.store.StoreID() &&
		prevLease.Replica.StoreID != r.getLease().Replica.StoreID {
		r.tsCache.SetLowWater(prevLease.Expiration.Add(int64(r.store.Clock().MaxOffset()), 0))
		log.Infof("range %d: new leader lease %s", rangeID, args.Lease)
	}

	// Gossip system config if this range includes the system span.
	if r.ContainsKey(keys.SystemDBSpan.Key) {
		r.maybeGossipSystemConfigLocked()
	}
	return reply, nil
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
func (r *Replica) AdminSplit(args roachpb.AdminSplitRequest, desc *roachpb.RangeDescriptor) (roachpb.AdminSplitResponse, error) {
	var reply roachpb.AdminSplitResponse

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	var splitKey roachpb.RKey
	{
		foundSplitKey := args.SplitKey
		if len(foundSplitKey) == 0 {
			snap := r.store.NewSnapshot()
			defer snap.Close()
			var err error
			foundSplitKey, err = engine.MVCCFindSplitKey(snap, desc.RangeID, desc.StartKey, desc.EndKey)
			if err != nil {
				return reply, util.Errorf("unable to determine split key: %s", err)
			}
		} else if !r.ContainsKey(foundSplitKey) {
			return reply, roachpb.NewRangeKeyMismatchError(args.SplitKey, args.SplitKey, desc)
		}

		splitKey = keys.Addr(foundSplitKey)
		if !splitKey.Equal(foundSplitKey) {
			return reply, util.Errorf("cannot split range at range-local key %s", splitKey)
		}
		if !engine.IsValidSplitKey(foundSplitKey) {
			return reply, util.Errorf("cannot split range at key %s", splitKey)
		}
	}

	// First verify this condition so that it will not return
	// roachpb.NewRangeKeyMismatchError if splitKey equals to desc.EndKey,
	// otherwise it will cause infinite retry loop.
	if desc.StartKey.Equal(splitKey) || desc.EndKey.Equal(splitKey) {
		return reply, util.Errorf("range is already split at key %s", splitKey)
	}

	// Create new range descriptor with newly-allocated replica IDs and Range IDs.
	newDesc, err := r.store.NewRangeDescriptor(splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, util.Errorf("unable to allocate new range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = splitKey

	log.Infof("initiating a split of %s at key %s", r, splitKey)

	if err := r.store.DB().Txn(func(txn *client.Txn) error {
		// Create range descriptor for second half of split.
		// Note that this put must go first in order to locate the
		// transaction record on the correct range.
		b := &client.Batch{}
		desc1Key := keys.RangeDescriptorKey(newDesc.StartKey)
		if err := updateRangeDescriptor(b, desc1Key, nil, newDesc); err != nil {
			return err
		}
		// Update existing range descriptor for first half of split.
		desc2Key := keys.RangeDescriptorKey(updatedDesc.StartKey)
		if err := updateRangeDescriptor(b, desc2Key, desc, &updatedDesc); err != nil {
			return err
		}
		// Update range descriptor addressing record(s).
		if err := splitRangeAddressing(b, newDesc, &updatedDesc); err != nil {
			return err
		}
		if err := txn.Run(b); err != nil {
			return err
		}
		// Update the RangeTree.
		b = &client.Batch{}
		if err := InsertRange(txn, b, newDesc.StartKey); err != nil {
			return err
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
		return txn.Run(b)
	}); err != nil {
		return reply, util.Errorf("split at key %s failed: %s", splitKey, err)
	}

	return reply, nil
}

func (r *Replica) computeStats(d *roachpb.RangeDescriptor, e engine.Engine, nowNanos int64) (engine.MVCCStats, error) {
	iter := e.NewIterator(false)
	defer iter.Close()

	ms := &engine.MVCCStats{}
	for _, r := range makeReplicaKeyRanges(d) {
		err := iter.ComputeStats(ms, r.start, r.end, nowNanos)
		if err != nil {
			return *ms, err
		}
	}
	return *ms, nil
}

// splitTrigger is called on a successful commit of an AdminSplit
// transaction. It copies the sequence cache for the new range and
// recomputes stats for both the existing, updated range and the new
// range.
func (r *Replica) splitTrigger(batch engine.Engine, split *roachpb.SplitTrigger) error {
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, split.UpdatedDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.NewDesc.EndKey) {
		return util.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.UpdatedDesc.StartKey, split.UpdatedDesc.EndKey,
			split.NewDesc.StartKey, split.NewDesc.EndKey, r)
	}

	// Copy the GC metadata.
	gcMeta, err := r.GetGCMetadata()
	if err != nil {
		return util.Errorf("unable to fetch GC metadata: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeGCMetadataKey(split.NewDesc.RangeID), roachpb.ZeroTimestamp, nil, gcMeta); err != nil {
		return util.Errorf("unable to copy GC metadata: %s", err)
	}

	// Copy the last verification timestamp.
	verifyTS, err := r.GetLastVerificationTimestamp()
	if err != nil {
		return util.Errorf("unable to fetch last verification timestamp: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeLastVerificationTimestampKey(split.NewDesc.RangeID), roachpb.ZeroTimestamp, nil, &verifyTS); err != nil {
		return util.Errorf("unable to copy last verification timestamp: %s", err)
	}

	// Compute stats for updated range.
	now := r.store.Clock().Timestamp()
	ms, err := r.computeStats(&split.UpdatedDesc, batch, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for updated range after split: %s", err)
	}
	if err := r.stats.SetMVCCStats(batch, ms); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Initialize the new range's sequence cache by copying the original's.
	if err = r.sequence.CopyInto(batch, split.NewDesc.RangeID); err != nil {
		// TODO(tschottdorf): ReplicaCorruptionError.
		return util.Errorf("unable to copy sequence cache to new split range: %s", err)
	}

	// Add the new split replica to the store. This step atomically
	// updates the EndKey of the updated replica and also adds the
	// new replica to the store's replica map.
	newRng, err := NewReplica(&split.NewDesc, r.store)
	if err != nil {
		return err
	}

	// Compute stats for new range.
	ms, err = r.computeStats(&split.NewDesc, batch, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for new range after split: %s", err)
	}
	if err = newRng.stats.SetMVCCStats(batch, ms); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Copy the timestamp cache into the new range. Commit triggers already
	// acquire the read lock since concurrent reads could add updates that
	// never make it to the new Range (see #3148), so all we grab here is
	// the actual lock (at time of writing, this isn't necessary but for
	// convention).
	r.Lock()
	r.tsCache.MergeInto(newRng.tsCache, true /* clear */)
	r.Unlock()

	batch.Defer(func() {
		if err := r.store.SplitRange(r, newRng); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update Store after split: %s", err)
		}

		// To avoid leaving the new range unavailable as it waits to elect
		// its leader, one (and only one) of the nodes should start an
		// election as soon as the split is processed.
		if r.store.StoreID() == split.InitialLeaderStoreID {
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
				r.store.multiraft.Campaign(split.NewDesc.RangeID)
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
func (r *Replica) AdminMerge(args roachpb.AdminMergeRequest, origLeftDesc *roachpb.RangeDescriptor) (roachpb.AdminMergeResponse, error) {
	var reply roachpb.AdminMergeResponse

	if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
		// Merging the final range doesn't make sense.
		return reply, util.Errorf("cannot merge final range")
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
			return reply, util.Errorf("ranges not collocated")
		}

		updatedLeftDesc.EndKey = rightRng.Desc().EndKey
		log.Infof("initiating a merge of %s into %s", rightRng, r)
	}

	if err := r.store.DB().Txn(func(txn *client.Txn) error {
		// Update the range descriptor for the receiving range.
		{
			b := &client.Batch{}
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, origLeftDesc, &updatedLeftDesc); err != nil {
				return err
			}
			// Commit this batch on its own to ensure that the transaction record
			// is created in the right place (our triggers rely on this).
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
			return util.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if !bytes.Equal(rightDesc.EndKey, updatedLeftDesc.EndKey) {
			// This merge raced with a split of the right-hand range.
			// TODO(bdarnell): needs a test.
			return util.Errorf("range changed during merge; %s != %s", rightDesc.EndKey, updatedLeftDesc.EndKey)
		}
		if !replicaSetsEqual(origLeftDesc.Replicas, rightDesc.Replicas) {
			return util.Errorf("ranges not collocated")
		}

		b := &client.Batch{}

		// Remove the range descriptor for the deleted range.
		b.Del(rightDescKey)

		if err := mergeRangeAddressing(b, origLeftDesc, &updatedLeftDesc); err != nil {
			return err
		}

		// Update the RangeTree.
		if err := DeleteRange(txn, b, rightDesc.StartKey); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		b.InternalAddRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				MergeTrigger: &roachpb.MergeTrigger{
					UpdatedDesc:     updatedLeftDesc,
					SubsumedRangeID: rightDesc.RangeID,
				},
			},
		})
		return txn.Run(b)
	}); err != nil {
		return reply, util.Errorf("merge of range into %d failed: %s", origLeftDesc.RangeID, err)
	}

	return reply, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
func (r *Replica) mergeTrigger(batch engine.Engine, merge *roachpb.MergeTrigger) error {
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, merge.UpdatedDesc.StartKey) {
		return util.Errorf("range and updated range start keys do not match: %s != %s",
			desc.StartKey, merge.UpdatedDesc.StartKey)
	}

	if !desc.EndKey.Less(merge.UpdatedDesc.EndKey) {
		return util.Errorf("range end key is not less than the post merge end key: %s >= %s",
			desc.EndKey, merge.UpdatedDesc.EndKey)
	}

	if merge.SubsumedRangeID <= 0 {
		return util.Errorf("subsumed  range ID must be provided: %d", merge.SubsumedRangeID)
	}

	// Copy the subsumed range's sequence cache to the subsuming one.
	if err := r.sequence.CopyFrom(batch, merge.SubsumedRangeID); err != nil {
		return util.Errorf("unable to copy sequence cache to new split range: %s", err)
	}

	// Remove the subsumed range's metadata.
	localRangeKeyPrefix := keys.MakeRangeIDPrefix(merge.SubsumedRangeID)
	if _, err := engine.MVCCDeleteRange(batch, nil, localRangeKeyPrefix, localRangeKeyPrefix.PrefixEnd(), 0, roachpb.ZeroTimestamp, nil); err != nil {
		return util.Errorf("cannot remove range metadata %s", err)
	}

	// Compute stats for updated range.
	now := r.store.Clock().Timestamp()
	ms, err := r.computeStats(&merge.UpdatedDesc, batch, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for the range after merge: %s", err)
	}
	if err = r.stats.SetMVCCStats(batch, ms); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Clear the timestamp cache. In the case that this replica and the
	// subsumed replica each held their respective leader leases, we
	// could merge the timestamp caches for efficiency. But it's unlikely
	// and not worth the extra logic and potential for error.
	r.Lock()
	r.tsCache.Clear(r.store.Clock())
	r.Unlock()

	batch.Defer(func() {
		if err := r.store.MergeRange(r, merge.UpdatedDesc.EndKey, merge.SubsumedRangeID); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update store after merging range: %s", err)
		}
	})
	return nil
}

func (r *Replica) changeReplicasTrigger(change *roachpb.ChangeReplicasTrigger) error {
	defer r.clearPendingChangeReplicas()
	cpy := *r.Desc()
	cpy.Replicas = change.UpdatedReplicas
	cpy.NextReplicaID = change.NextReplicaID
	if err := r.setDesc(&cpy); err != nil {
		return err
	}
	// If we're removing the current replica, add it to the range GC queue.
	if change.ChangeType == roachpb.REMOVE_REPLICA && r.store.StoreID() == change.Replica.StoreID {
		if err := r.store.replicaGCQueue.Add(r, 1.0); err != nil {
			// Log the error; this shouldn't prevent the commit; the range
			// will be GC'd eventually.
			log.Errorf("unable to add range %s to GC queue: %s", r, err)
		}
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
	r.Lock()
	for r.pendingReplica.value.ReplicaID != 0 {
		r.pendingReplica.Wait()
	}

	// Validate the request and prepare the new descriptor.
	updatedDesc := *desc
	updatedDesc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)
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

		// We need to be able to look up replica information before the change
		// is official.
		r.pendingReplica.value = replica
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

	r.Unlock()

	err := r.store.DB().Txn(func(txn *client.Txn) error {
		// Important: the range descriptor must be the first thing touched in the transaction
		// so the transaction record is co-located with the range being modified.
		b := &client.Batch{}
		descKey := keys.RangeDescriptorKey(updatedDesc.StartKey)

		if err := updateRangeDescriptor(b, descKey, desc, &updatedDesc); err != nil {
			return err
		}

		// Update range descriptor addressing record(s).
		if err := updateRangeAddressing(b, &updatedDesc); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
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
	if err != nil {
		r.clearPendingChangeReplicas()
		return util.Errorf("change replicas of %d failed: %s", desc.RangeID, err)
	}
	return nil
}

func (r *Replica) clearPendingChangeReplicas() {
	r.Lock()
	r.pendingReplica.value = roachpb.ReplicaDescriptor{}
	r.pendingReplica.Broadcast()
	r.Unlock()
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
	var oldValue []byte
	if oldDesc != nil {
		var err error
		if oldValue, err = proto.Marshal(oldDesc); err != nil {
			return err
		}
	}
	newValue, err := proto.Marshal(newDesc)
	if err != nil {
		return err
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}
