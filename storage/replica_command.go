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
	"unsafe"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command. It returns the response, an error,
// and a slice of intents that were skipped during execution.
// If an error is returned, any returned intents should still be resolved.
func (r *Replica) executeCmd(batch engine.Engine, ms *engine.MVCCStats, args proto.Request) (proto.Response, []proto.Intent, error) {
	// Verify key is contained within range here to catch any range split
	// or merge activity.
	header := args.Header()

	if err := r.checkCmdHeader(header); err != nil {
		return nil, nil, err
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if TestingCommandFilter != nil {
		if err := TestingCommandFilter(args); err != nil {
			return nil, nil, err
		}
	}

	var reply proto.Response
	var intents []proto.Intent
	var err error
	switch tArgs := args.(type) {
	case *proto.GetRequest:
		var resp proto.GetResponse
		resp, intents, err = r.Get(batch, *tArgs)
		reply = &resp
	case *proto.PutRequest:
		var resp proto.PutResponse
		resp, err = r.Put(batch, ms, *tArgs)
		reply = &resp
	case *proto.ConditionalPutRequest:
		var resp proto.ConditionalPutResponse
		resp, err = r.ConditionalPut(batch, ms, *tArgs)
		reply = &resp
	case *proto.IncrementRequest:
		var resp proto.IncrementResponse
		resp, err = r.Increment(batch, ms, *tArgs)
		reply = &resp
	case *proto.DeleteRequest:
		var resp proto.DeleteResponse
		resp, err = r.Delete(batch, ms, *tArgs)
		reply = &resp
	case *proto.DeleteRangeRequest:
		var resp proto.DeleteRangeResponse
		resp, err = r.DeleteRange(batch, ms, *tArgs)
		reply = &resp
	case *proto.ScanRequest:
		var resp proto.ScanResponse
		resp, intents, err = r.Scan(batch, *tArgs)
		reply = &resp
	case *proto.EndTransactionRequest:
		var resp proto.EndTransactionResponse
		resp, intents, err = r.EndTransaction(batch, ms, *tArgs)
		reply = &resp
	case *proto.RangeLookupRequest:
		var resp proto.RangeLookupResponse
		resp, intents, err = r.RangeLookup(batch, *tArgs)
		reply = &resp
	case *proto.HeartbeatTxnRequest:
		var resp proto.HeartbeatTxnResponse
		resp, err = r.HeartbeatTxn(batch, ms, *tArgs)
		reply = &resp
	case *proto.GCRequest:
		var resp proto.GCResponse
		resp, err = r.GC(batch, ms, *tArgs)
		reply = &resp
	case *proto.PushTxnRequest:
		var resp proto.PushTxnResponse
		resp, err = r.PushTxn(batch, ms, *tArgs)
		reply = &resp
	case *proto.ResolveIntentRequest:
		var resp proto.ResolveIntentResponse
		resp, err = r.ResolveIntent(batch, ms, *tArgs)
		reply = &resp
	case *proto.ResolveIntentRangeRequest:
		var resp proto.ResolveIntentRangeResponse
		resp, err = r.ResolveIntentRange(batch, ms, *tArgs)
		reply = &resp
	case *proto.MergeRequest:
		var resp proto.MergeResponse
		resp, err = r.Merge(batch, ms, *tArgs)
		reply = &resp
	case *proto.TruncateLogRequest:
		var resp proto.TruncateLogResponse
		resp, err = r.TruncateLog(batch, ms, *tArgs)
		reply = &resp
	case *proto.LeaderLeaseRequest:
		var resp proto.LeaderLeaseResponse
		resp, err = r.LeaderLease(batch, ms, *tArgs)
		reply = &resp
	default:
		err = util.Errorf("unrecognized command %s", args.Method())
	}

	if log.V(2) {
		log.Infof("executed %s command %+v: %+v", args.Method(), args, reply)
	}

	// Update the node clock with the serviced request. This maintains a
	// high water mark for all ops serviced, so that received ops
	// without a timestamp specified are guaranteed one higher than any
	// op already executed for overlapping keys.
	r.rm.Clock().Update(header.Timestamp)

	// Propagate the request timestamp (which may have changed).
	reply.Header().Timestamp = header.Timestamp

	// A ReadWithinUncertaintyIntervalError contains the timestamp of the value
	// that provoked the conflict. However, we forward the timestamp to the
	// node's time here. The reason is that the caller (which is always
	// transactional when this error occurs) in our implementation wants to
	// use this information to extract a timestamp after which reads from
	// the nodes are causally consistent with the transaction. This allows
	// the node to be classified as without further uncertain reads for the
	// remainder of the transaction.
	// See the comment on proto.Transaction.CertainNodes.
	if tErr, ok := err.(*proto.ReadWithinUncertaintyIntervalError); ok {
		// Note that we can use this node's clock (which may be different from
		// other replicas') because this error attaches the existing timestamp
		// to the node itself when retrying.
		tErr.ExistingTimestamp.Forward(r.rm.Clock().Now())
	}

	return reply, intents, err
}

// Get returns the value for a specified key.
func (r *Replica) Get(batch engine.Engine, args proto.GetRequest) (proto.GetResponse, []proto.Intent, error) {
	var reply proto.GetResponse

	val, intents, err := engine.MVCCGet(batch, args.Key, args.Timestamp, args.ReadConsistency == proto.CONSISTENT, args.Txn)
	reply.Value = val
	return reply, intents, err
}

// Put sets the value for a specified key.
func (r *Replica) Put(batch engine.Engine, ms *engine.MVCCStats, args proto.PutRequest) (proto.PutResponse, error) {
	var reply proto.PutResponse

	return reply, engine.MVCCPut(batch, ms, args.Key, args.Timestamp, args.Value, args.Txn)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Replica) ConditionalPut(batch engine.Engine, ms *engine.MVCCStats, args proto.ConditionalPutRequest) (proto.ConditionalPutResponse, error) {
	var reply proto.ConditionalPutResponse

	return reply, engine.MVCCConditionalPut(batch, ms, args.Key, args.Timestamp, args.Value, args.ExpValue, args.Txn)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Replica) Increment(batch engine.Engine, ms *engine.MVCCStats, args proto.IncrementRequest) (proto.IncrementResponse, error) {
	var reply proto.IncrementResponse

	newVal, err := engine.MVCCIncrement(batch, ms, args.Key, args.Timestamp, args.Txn, args.Increment)
	reply.NewValue = newVal
	return reply, err
}

// Delete deletes the key and value specified by key.
func (r *Replica) Delete(batch engine.Engine, ms *engine.MVCCStats, args proto.DeleteRequest) (proto.DeleteResponse, error) {
	var reply proto.DeleteResponse

	return reply, engine.MVCCDelete(batch, ms, args.Key, args.Timestamp, args.Txn)
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Replica) DeleteRange(batch engine.Engine, ms *engine.MVCCStats, args proto.DeleteRangeRequest) (proto.DeleteRangeResponse, error) {
	var reply proto.DeleteRangeResponse

	numDel, err := engine.MVCCDeleteRange(batch, ms, args.Key, args.EndKey, args.MaxEntriesToDelete, args.Timestamp, args.Txn)
	reply.NumDeleted = numDel
	return reply, err
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Replica) Scan(batch engine.Engine, args proto.ScanRequest) (proto.ScanResponse, []proto.Intent, error) {
	var reply proto.ScanResponse

	rows, intents, err := engine.MVCCScan(batch, args.Key, args.EndKey, args.MaxResults, args.Timestamp, args.ReadConsistency == proto.CONSISTENT, args.Txn)
	reply.Rows = rows
	return reply, intents, err
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Replica) EndTransaction(batch engine.Engine, ms *engine.MVCCStats, args proto.EndTransactionRequest) (proto.EndTransactionResponse, []proto.Intent, error) {
	var reply proto.EndTransactionResponse

	if args.Txn == nil {
		return reply, nil, util.Errorf("no transaction specified to EndTransaction")
	}
	// Make a copy of the transaction in case it's mutated below.
	txn := *args.Txn
	key := keys.TransactionKey(txn.Key, txn.ID)

	// Fetch existing transaction if possible.
	reply.Txn = &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, true, nil, reply.Txn)
	if err != nil {
		return reply, nil, err
	}
	if !ok {
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.Txn = &txn
	}

	for i := range args.Intents {
		// Set the transaction into the intents (TxnCoordSender avoids sending
		// all of that redundant info over the wire).
		// This needs to be done before the commit status check because
		// the TransactionAbortedError branch below returns these intents.
		args.Intents[i].Txn = *(reply.Txn)
	}

	// If the transaction record already exists, verify that we can either
	// commit it or abort it (according to args.Commit), and also that the
	// Timestamp and Epoch have not suffered regression.
	if ok {
		if reply.Txn.Status == proto.COMMITTED {
			return reply, nil, proto.NewTransactionStatusError(reply.Txn, "already committed")
		} else if reply.Txn.Status == proto.ABORTED {
			// This case is special: If a transaction is aborted by a
			// concurrent writer's push, the intents are not known on abort.
			// When the owning coordinator comes along and tries to commit
			// (with a list of intents), this error results, and we want to
			// abort the intents, not leave them dangling. That's why we return
			// them and make sure that skipped intents are always resolved.
			//
			// TODO(tschottdorf): In fact, we would want to do the local
			// resolution as before - but that means we would need to commit a
			// batch. This is going to be a significant refactor, so leaving it
			// for now.
			return reply, args.Intents, proto.NewTransactionAbortedError(reply.Txn)
		} else if txn.Epoch < reply.Txn.Epoch {
			return reply, nil, proto.NewTransactionStatusError(reply.Txn, fmt.Sprintf("epoch regression: %d", txn.Epoch))
		} else if txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(txn.OrigTimestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the original transaction timestamp.
			return reply, nil, proto.NewTransactionStatusError(reply.Txn, fmt.Sprintf("timestamp regression: %s", txn.OrigTimestamp))
		}

		// Take max of requested epoch and existing epoch. The requester
		// may have incremented the epoch on retries.
		if reply.Txn.Epoch < txn.Epoch {
			reply.Txn.Epoch = txn.Epoch
		}
		// Take max of requested priority and existing priority. This isn't
		// terribly useful, but we do it for completeness.
		if reply.Txn.Priority < txn.Priority {
			reply.Txn.Priority = txn.Priority
		}
	}

	// Take max of requested timestamp and possibly "pushed" txn
	// record timestamp as the final commit timestamp.
	if reply.Txn.Timestamp.Less(args.Timestamp) {
		reply.Txn.Timestamp = args.Timestamp
	}

	// Set transaction status to COMMITTED or ABORTED as per the
	// args.Commit parameter.
	if args.Commit {
		// If the isolation level is SERIALIZABLE, return a transaction
		// retry error if the commit timestamp isn't equal to the txn
		// timestamp.
		if args.Txn.Isolation == proto.SERIALIZABLE && !reply.Txn.Timestamp.Equal(args.Txn.OrigTimestamp) {
			return reply, nil, proto.NewTransactionRetryError(reply.Txn)
		}
		reply.Txn.Status = proto.COMMITTED
	} else {
		reply.Txn.Status = proto.ABORTED
	}

	// Resolve any explicit intents. All that are local to this range get
	// resolved synchronously in the same batch. The remainder are collected
	// and handed off to asynchronous processing.
	desc := *(r.Desc())
	if wideDesc := args.GetInternalCommitTrigger().GetMergeTrigger().GetUpdatedDesc(); wideDesc.RangeID != 0 {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		desc = wideDesc
	}

	var externalIntents []proto.Intent
	for _, intent := range args.Intents {
		if err := func() error {
			if len(intent.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(desc, intent.Key) {
					externalIntents = append(externalIntents, intent)
					return nil
				}
				return engine.MVCCResolveWriteIntent(batch, ms,
					intent.Key, reply.Txn.Timestamp, reply.Txn)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			insideIntent, outsideIntents := intersectIntent(intent, desc)
			externalIntents = append(externalIntents, outsideIntents...)
			if insideIntent != nil {
				_, err := engine.MVCCResolveWriteIntentRange(batch, ms,
					intent.Key, intent.EndKey, 0, reply.Txn.Timestamp, reply.Txn)
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving intent at %s on end transaction [%s]: %s", intent, reply.Txn.Status, err))
		}
	}

	// Persist the transaction record with updated status (& possibly timestamp).
	// TODO(tschottdorf): if all intents resolved, can kill transaction record.
	// Otherwise, we'll need to store all external intents along with the record,
	// and any resolve command should also update the record, gc'ing it when the
	// last intent goes. With that, we shouldn't even need a GC queue for Txn
	// records. But we'll probably need another command to mark an intent as
	// resolved since special-casing EndTransaction more seems unwise.
	if err := engine.MVCCPutProto(batch, ms, key, proto.ZeroTimestamp, nil, reply.Txn); err != nil {
		return reply, nil, err
	}

	// Run triggers if successfully committed. Any failures running
	// triggers will set an error and prevent the batch from committing.
	if ct := args.InternalCommitTrigger; ct != nil {
		// Run appropriate trigger.
		if reply.Txn.Status == proto.COMMITTED {
			if ct.SplitTrigger != nil {
				*ms = engine.MVCCStats{} // clear stats, as split will recompute from scratch.
				if err := r.splitTrigger(batch, ct.SplitTrigger); err != nil {
					return reply, nil, err
				}
			} else if ct.MergeTrigger != nil {
				*ms = engine.MVCCStats{} // clear stats, as merge will recompute from scratch.
				if err := r.mergeTrigger(batch, ct.MergeTrigger); err != nil {
					return reply, nil, err
				}
			} else if ct.ChangeReplicasTrigger != nil {
				if err := r.changeReplicasTrigger(ct.ChangeReplicasTrigger); err != nil {
					return reply, nil, err
				}
			}
		}
	}

	return reply, externalIntents, nil
}

// intersectIntent takes an intent and a descriptor. It then splits the
// intent's range into up to three pieces: A first piece which is contained in
// the Range, and a slice of up to two further intents which are outside of the
// key range. An intent for which [Key, EndKey) is empty does not result in any
// intents; thus intersectIntent only applies to intent ranges.
// A range-local intent range is never split: It's returned as either
// belonging to or outside of the descriptor's key range, and passing an intent
// which begins range-local but ends non-local results in a panic.
func intersectIntent(intent proto.Intent, desc proto.RangeDescriptor) (middle *proto.Intent, outside []proto.Intent) {
	start, end := desc.StartKey, desc.EndKey
	if !intent.Key.Less(intent.EndKey) {
		return
	}
	if !start.Less(end) {
		panic("can not intersect with empty key range")
	}
	if intent.Key.Less(keys.LocalRangeMax) {
		if !intent.EndKey.Less(keys.LocalRangeMax) {
			panic("a local intent range may not have a non-local portion")
		}
		if containsKeyRange(desc, intent.Key, intent.EndKey) {
			return &intent, nil
		}
		return nil, append(outside, intent)
	}
	// From now on, we're dealing with plain old key ranges - no more local
	// addressing.
	if intent.Key.Less(start) {
		// Intent spans a part to the left of [start, end).
		iCopy := intent
		if start.Less(intent.EndKey) {
			iCopy.EndKey = start
		}
		intent.Key = iCopy.EndKey
		outside = append(outside, iCopy)
	}
	if intent.Key.Less(intent.EndKey) && end.Less(intent.EndKey) {
		// Intent spans a part to the right of [start, end).
		iCopy := intent
		if iCopy.Key.Less(end) {
			iCopy.Key = end
		}
		intent.EndKey = iCopy.Key
		outside = append(outside, iCopy)
	}
	if intent.Key.Less(intent.EndKey) && !intent.Key.Less(start) && !end.Less(intent.EndKey) {
		middle = &intent
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
// the range to the meta[12] prefix because the RocksDB iterator only supports
// a Seek() interface which acts as a Ceil(). Using the start key of the range
// would cause Seek() to find the key after the meta indexing record we're
// looking for, which would result in having to back the iterator up, an option
// which is both less efficient and not available in all cases.
//
// Lookups for range metadata keys usually want to read inconsistently, but
// some callers need a consistent result; both are supported.
//
// This method has an important optimization in the inconsistent case: instead
// of just returning the request RangeDescriptor, it also returns a slice of
// additional range descriptors immediately consecutive to the desired
// RangeDescriptor. This is intended to serve as a sort of caching pre-fetch,
// so that the requesting nodes can aggressively cache RangeDescriptors which
// are likely to be desired by their current workload.
func (r *Replica) RangeLookup(batch engine.Engine, args proto.RangeLookupRequest) (proto.RangeLookupResponse, []proto.Intent, error) {
	var reply proto.RangeLookupResponse

	if err := keys.ValidateRangeMetaKey(args.Key); err != nil {
		return reply, nil, err
	}

	rangeCount := int64(args.MaxRanges)
	if rangeCount < 1 {
		return reply, nil, util.Errorf("Range lookup specified invalid maximum range count %d: must be > 0", rangeCount)
	}
	consistent := args.ReadConsistency != proto.INCONSISTENT
	if consistent && args.IgnoreIntents {
		return reply, nil, util.Errorf("can not read consistently and skip intents")
	}
	if args.IgnoreIntents || consistent {
		// Disable prefetching; in those cases the caller only cares about
		// a single intent, and the code below simplifies considerably.
		rangeCount = 1
	}

	// We want to search for the metadata key just greater than args.Key. Scan
	// for both the requested key and the keys immediately afterwards, up to
	// MaxRanges.
	startKey, endKey := keys.MetaScanBounds(args.Key)
	// Scan for descriptors.
	kvs, intents, err := engine.MVCCScan(batch, startKey, endKey, rangeCount,
		args.Timestamp, consistent, args.Txn)
	if err != nil {
		// An error here is likely a WriteIntentError when reading consistently.
		return reply, nil, err
	}
	if args.IgnoreIntents && len(intents) > 0 {
		// NOTE (subtle): in general, we want to try to clean up dangling
		// intents on meta records. However, if we're in the process of
		// cleaning up a dangling intent on a meta record by pushing the
		// transaction, we don't want to create an infinite loop:
		//
		// intent! -> push-txn -> range-lookup -> intent! -> etc...
		//
		// Instead we want:
		//
		// intent! -> push-txn -> range-lookup -> ignore intent, return old/new ranges
		//
		// On the range-lookup from a push transaction, we therefore
		// want to suppress WriteIntentErrors and return a value
		// anyway. But which value? We don't know whether the range
		// update succeeded or failed, but if we don't return the
		// correct range descriptor we may not be able to find the
		// transaction to push. Since we cannot know the correct answer,
		// we choose randomly between the pre- and post- transaction
		// values. If we guess wrong, the client will try again and get
		// the other value (within a few tries).
		if rand.Intn(2) == 0 {
			key, txn := intents[0].Key, &intents[0].Txn
			val, _, err := engine.MVCCGet(batch, key, txn.Timestamp, true, txn)
			if err != nil {
				return reply, nil, err
			}
			kvs = []proto.KeyValue{{Key: key, Value: *val}}
		}
	}

	if len(kvs) == 0 {
		// No matching results were returned from the scan. This could
		// indicate a very bad system error, but for now we will just
		// treat it as a retryable Key Mismatch error.
		err := proto.NewRangeKeyMismatchError(args.Key, args.EndKey, r.Desc())
		log.Errorf("RangeLookup dispatched to correct range, but no matching RangeDescriptor was found. %s", err)
		return reply, nil, err
	}

	// Decode all scanned range descriptors, stopping if a range is encountered
	// which does not have the same metadata prefix as the queried key.
	rds := make([]proto.RangeDescriptor, len(kvs))
	for i := range kvs {
		// TODO(tschottdorf) Candidate for a ReplicaCorruptionError.
		if err = gogoproto.Unmarshal(kvs[i].Value.Bytes, &rds[i]); err != nil {
			return reply, nil, err
		}
	}
	reply.Ranges = rds

	return reply, intents, nil
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func (r *Replica) HeartbeatTxn(batch engine.Engine, ms *engine.MVCCStats, args proto.HeartbeatTxnRequest) (proto.HeartbeatTxnResponse, error) {
	var reply proto.HeartbeatTxnResponse

	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	var txn proto.Transaction
	if ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, true, nil, &txn); err != nil {
		return reply, err
	} else if !ok {
		// If no existing transaction record was found, initialize to a
		// shallow copy of the transaction in the request header. We copy
		// to avoid mutating the original below.
		txn = *args.Txn
	}

	if txn.Status == proto.PENDING {
		if txn.LastHeartbeat == nil {
			txn.LastHeartbeat = &proto.Timestamp{}
		}
		if txn.LastHeartbeat.Less(args.Header().Timestamp) {
			*txn.LastHeartbeat = args.Header().Timestamp
		}
		if err := engine.MVCCPutProto(batch, ms, key, proto.ZeroTimestamp, nil, &txn); err != nil {
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
func (r *Replica) GC(batch engine.Engine, ms *engine.MVCCStats, args proto.GCRequest) (proto.GCResponse, error) {
	var reply proto.GCResponse

	// Garbage collect the specified keys by expiration timestamps.
	if err := engine.MVCCGarbageCollect(batch, ms, args.Keys, args.Timestamp); err != nil {
		return reply, err
	}

	// Store the GC metadata for this range.
	key := keys.RangeGCMetadataKey(r.Desc().RangeID)
	if err := engine.MVCCPutProto(batch, ms, key, proto.ZeroTimestamp, nil, &args.GCMeta); err != nil {
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
// conflict).
//
// Txn already committed/aborted: If pushee txn is committed or
// aborted return success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use PushTxn.Timestamp as LastHeartbeat. If
// current time - LastHeartbeat > 2 * DefaultHeartbeatInterval, then
// the pushee txn should be either pushed forward, aborted, or
// confirmed not pending, depending on value of Request.PushType.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of
// args.PushType. If args.PushType is ABORT_TXN, set txn.Status to
// ABORTED, and priority to one less than the pusher's priority and
// return success. If args.PushType is PUSH_TIMESTAMP, set
// txn.Timestamp to pusher's Timestamp + 1 (note that we use the
// pusher's Args.Timestamp, not Txn.Timestamp because the args
// timestamp can advance during the txn).
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Replica) PushTxn(batch engine.Engine, ms *engine.MVCCStats, args proto.PushTxnRequest) (proto.PushTxnResponse, error) {
	var reply proto.PushTxnResponse

	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return reply, util.Errorf("request key %s should match pushee's txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp,
		true /* consistent */, nil /* txn */, existTxn)
	if err != nil {
		return reply, err
	}
	if ok {
		// Start with the persisted transaction record as final transaction.
		reply.PusheeTxn = gogoproto.Clone(existTxn).(*proto.Transaction)
		// Upgrade the epoch, timestamp and priority as necessary.
		if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
			reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		}
		reply.PusheeTxn.Timestamp.Forward(args.PusheeTxn.Timestamp)
		if reply.PusheeTxn.Priority < args.PusheeTxn.Priority {
			reply.PusheeTxn.Priority = args.PusheeTxn.Priority
		}
	} else {
		// Some sanity checks for case where we don't find a transaction record.
		if args.PusheeTxn.LastHeartbeat != nil {
			return reply, proto.NewTransactionStatusError(&args.PusheeTxn,
				"no txn persisted, yet intent has heartbeat")
		} else if args.PusheeTxn.Status != proto.PENDING {
			return reply, proto.NewTransactionStatusError(&args.PusheeTxn,
				fmt.Sprintf("no txn persisted, yet intent has status %s", args.PusheeTxn.Status))
		}
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.PusheeTxn = gogoproto.Clone(&args.PusheeTxn).(*proto.Transaction)
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != proto.PENDING {
		// Trivial noop.
		return reply, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == proto.PUSH_TIMESTAMP && args.Timestamp.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return reply, nil
	}

	// pusherWins bool is true in the event the pusher prevails.
	var pusherWins bool

	// If there's no incoming transaction, the pusher is non-transactional.
	// We make a random priority, biased by specified
	// args.Header().UserPriority in this case.
	var priority int32
	if args.Txn != nil {
		priority = args.Txn.Priority
	} else {
		// Make sure we have a deterministic random number when generating
		// a priority for this txn-less request, so all replicas see same priority.
		randGen := rand.New(rand.NewSource(int64(reply.PusheeTxn.Priority) ^ args.Timestamp.WallTime))
		priority = proto.MakePriority(randGen, args.GetUserPriority())
	}

	// Check for txn timeout.
	if reply.PusheeTxn.LastHeartbeat == nil {
		reply.PusheeTxn.LastHeartbeat = &reply.PusheeTxn.Timestamp
	}
	if args.Now.Equal(proto.ZeroTimestamp) {
		return reply, util.Error("the field Now must be provided")
	}
	// Compute heartbeat expiration (all replicas must see the same result).
	expiry := args.Now
	expiry.Forward(args.Timestamp) // if Timestamp is ahead, use that
	expiry.WallTime -= 2 * DefaultHeartbeatInterval.Nanoseconds()

	if reply.PusheeTxn.LastHeartbeat.Less(expiry) {
		if log.V(1) {
			log.Infof("pushing expired txn %s", reply.PusheeTxn)
		}
		pusherWins = true
	} else if reply.PusheeTxn.Isolation == proto.SNAPSHOT && args.PushType == proto.PUSH_TIMESTAMP {
		if log.V(1) {
			log.Infof("pushing timestamp for snapshot isolation txn")
		}
		pusherWins = true
	} else if args.PushType == proto.CLEANUP_TXN {
		// If just attempting to cleanup old or already-committed txns, don't push.
		pusherWins = false
	} else if reply.PusheeTxn.Priority < priority ||
		(reply.PusheeTxn.Priority == priority && args.Txn != nil &&
			args.Txn.Timestamp.Less(reply.PusheeTxn.Timestamp)) {
		// Pusher wins based on priority; if priorities are equal, order
		// by lower txn timestamp.
		if log.V(1) {
			log.Infof("pushing intent from txn with lower priority %s vs %d", reply.PusheeTxn, priority)
		}
		pusherWins = true
	}

	if !pusherWins {
		err := proto.NewTransactionPushError(args.Txn, reply.PusheeTxn)
		if log.V(1) {
			log.Info(err)
		}
		return reply, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.PushType == proto.ABORT_TXN {
		reply.PusheeTxn.Status = proto.ABORTED
	} else if args.PushType == proto.PUSH_TIMESTAMP {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.Timestamp
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(batch, ms, key, proto.ZeroTimestamp, nil, reply.PusheeTxn); err != nil {
		return reply, err
	}
	return reply, nil
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func (r *Replica) ResolveIntent(batch engine.Engine, ms *engine.MVCCStats, args proto.ResolveIntentRequest) (proto.ResolveIntentResponse, error) {
	var reply proto.ResolveIntentResponse

	if args.Txn == nil {
		return reply, util.Errorf("no transaction specified to ResolveIntent")
	}
	if err := engine.MVCCResolveWriteIntent(batch, ms, args.Key, args.Timestamp, args.Txn); err != nil {
		return reply, err
	}
	return reply, nil
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func (r *Replica) ResolveIntentRange(batch engine.Engine, ms *engine.MVCCStats,
	args proto.ResolveIntentRangeRequest) (proto.ResolveIntentRangeResponse, error) {
	var reply proto.ResolveIntentRangeResponse

	if args.Txn == nil {
		return reply, util.Errorf("no transaction specified to ResolveIntentRange")
	}
	_, err := engine.MVCCResolveWriteIntentRange(batch, ms, args.Key, args.EndKey, 0, args.Timestamp, args.Txn)
	return reply, err
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used by
// Cockroach for the efficient accumulation of certain values. Due to the
// difficulty of making these operations transactional, merges are not currently
// exposed directly to clients. Merged values are explicitly not MVCC data.
func (r *Replica) Merge(batch engine.Engine, ms *engine.MVCCStats, args proto.MergeRequest) (proto.MergeResponse, error) {
	var reply proto.MergeResponse

	return reply, engine.MVCCMerge(batch, ms, args.Key, args.Value)
}

// TruncateLog discards a prefix of the raft log.
func (r *Replica) TruncateLog(batch engine.Engine, ms *engine.MVCCStats, args proto.TruncateLogRequest) (proto.TruncateLogResponse, error) {
	var reply proto.TruncateLogResponse

	// args.Index is the first index to keep.
	term, err := r.Term(args.Index - 1)
	if err != nil {
		return reply, err
	}
	start := keys.RaftLogKey(r.Desc().RangeID, 0)
	end := keys.RaftLogKey(r.Desc().RangeID, args.Index)
	if err = batch.Iterate(engine.MVCCEncodeKey(start), engine.MVCCEncodeKey(end), func(kv proto.RawKeyValue) (bool, error) {
		return false, batch.Clear(kv.Key)
	}); err != nil {
		return reply, err
	}
	ts := proto.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}
	return reply, engine.MVCCPutProto(batch, ms, keys.RaftTruncatedStateKey(r.Desc().RangeID), proto.ZeroTimestamp, nil, &ts)
}

// LeaderLease sets the leader lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range leader are commenced, including
// clearing the command queue and timestamp cache.
func (r *Replica) LeaderLease(batch engine.Engine, ms *engine.MVCCStats, args proto.LeaderLeaseRequest) (proto.LeaderLeaseResponse, error) {
	var reply proto.LeaderLeaseResponse

	r.Lock()
	defer r.Unlock()

	prevLease := r.getLease()
	isExtension := prevLease.RaftNodeID == args.Lease.RaftNodeID
	effectiveStart := args.Lease.Start
	// We return this error in "normal" lease-overlap related failures.
	rErr := &proto.LeaseRejectedError{
		Existing:  *prevLease,
		Requested: args.Lease,
	}

	// Verify details of new lease request. The start of this lease must
	// obviously precede its expiration.
	if !args.Lease.Start.Less(args.Lease.Expiration) {
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
	if prevLease.RaftNodeID == 0 || isExtension {
		// TODO(tschottdorf) Think about whether it'd be better to go all the
		// way back to prevLease.Start(), so that whenever the last lease is
		// the own one, the original start is preserved.
		effectiveStart.Backward(prevLease.Expiration)
	} else {
		effectiveStart.Backward(prevLease.Expiration.Next())
	}

	if isExtension {
		if effectiveStart.Less(prevLease.Start) {
			return reply, rErr
		}
		// Note that the lease expiration can be shortened by the holder.
		// This could be used to effect a faster lease handoff.
	} else if effectiveStart.Less(prevLease.Expiration) {
		return reply, rErr
	}

	args.Lease.Start = effectiveStart

	// Store the lease to disk & in-memory.
	if err := engine.MVCCPutProto(batch, ms, keys.RaftLeaderLeaseKey(r.Desc().RangeID), proto.ZeroTimestamp, nil, &args.Lease); err != nil {
		return reply, err
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(&args.Lease))

	// If this replica is a new holder of the lease, update the
	// low water mark in the timestamp cache. We add the maximum
	// clock offset to account for any difference in clocks
	// between the expiration (set by a remote node) and this
	// node.
	if r.getLease().RaftNodeID == r.rm.RaftNodeID() && prevLease.RaftNodeID != r.getLease().RaftNodeID {
		r.tsCache.SetLowWater(prevLease.Expiration.Add(int64(r.rm.Clock().MaxOffset()), 0))
		log.Infof("range %d: new leader lease %s", r.Desc().RangeID, args.Lease)
	}

	// Gossip configs in the event this range contains config info.
	r.maybeGossipConfigsLocked(func(configPrefix proto.Key) bool {
		return r.ContainsKey(configPrefix)
	})
	return reply, nil
}

// AdminSplit divides the range into into two ranges, using either
// args.SplitKey (if provided) or an internally computed key that aims to
// roughly equipartition the range by size. The split is done inside of
// a distributed txn which writes updated and new range descriptors, and
// updates the range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a split trigger
// carried out as part of the commit of that transaction.
func (r *Replica) AdminSplit(args proto.AdminSplitRequest) (proto.AdminSplitResponse, error) {
	var reply proto.AdminSplitResponse

	// Only allow a single split per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	desc := r.Desc()
	splitKey := proto.Key(args.SplitKey)
	if len(splitKey) == 0 {
		snap := r.rm.NewSnapshot()
		defer snap.Close()
		foundSplitKey, err := engine.MVCCFindSplitKey(snap, desc.RangeID, desc.StartKey, desc.EndKey)
		if err != nil {
			return reply, util.Errorf("unable to determine split key: %s", err)
		}
		splitKey = foundSplitKey
	}
	// First verify this condition so that it will not return
	// proto.NewRangeKeyMismatchError if splitKey equals to desc.EndKey,
	// otherwise it will cause infinite retry loop.
	if splitKey.Equal(desc.StartKey) || splitKey.Equal(desc.EndKey) {
		return reply, util.Errorf("range is already split at key %s", splitKey)
	}
	// Verify some properties of split key.
	if !r.ContainsKey(splitKey) {
		return reply, proto.NewRangeKeyMismatchError(splitKey, splitKey, desc)
	}
	if !engine.IsValidSplitKey(splitKey) {
		return reply, util.Errorf("cannot split range at key %s", splitKey)
	}

	// Create new range descriptor with newly-allocated replica IDs and Range IDs.
	newDesc, err := r.rm.NewRangeDescriptor(splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, util.Errorf("unable to allocate new range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = splitKey

	log.Infof("initiating a split of %s at key %s", r, splitKey)

	if err := r.rm.DB().Txn(func(txn *client.Txn) error {
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
		b.InternalAddCall(proto.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: args.Key},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					SplitTrigger: &proto.SplitTrigger{
						UpdatedDesc: updatedDesc,
						NewDesc:     *newDesc,
					},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Run(b)
	}); err != nil {
		return reply, util.Errorf("split at key %s failed: %s", splitKey, err)
	}

	return reply, nil
}

// splitTrigger is called on a successful commit of an AdminSplit
// transaction. It copies the response cache for the new range and
// recomputes stats for both the existing, updated range and the new
// range.
func (r *Replica) splitTrigger(batch engine.Engine, split *proto.SplitTrigger) error {
	if !bytes.Equal(r.Desc().StartKey, split.UpdatedDesc.StartKey) ||
		!bytes.Equal(r.Desc().EndKey, split.NewDesc.EndKey) {
		return util.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.UpdatedDesc.StartKey, split.UpdatedDesc.EndKey,
			split.NewDesc.StartKey, split.NewDesc.EndKey, r)
	}

	// Copy the GC metadata.
	gcMeta, err := r.GetGCMetadata()
	if err != nil {
		return util.Errorf("unable to fetch GC metadata: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeGCMetadataKey(split.NewDesc.RangeID), proto.ZeroTimestamp, nil, gcMeta); err != nil {
		return util.Errorf("unable to copy GC metadata: %s", err)
	}

	// Copy the last verification timestamp.
	verifyTS, err := r.GetLastVerificationTimestamp()
	if err != nil {
		return util.Errorf("unable to fetch last verification timestamp: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, keys.RangeLastVerificationTimestampKey(split.NewDesc.RangeID), proto.ZeroTimestamp, nil, &verifyTS); err != nil {
		return util.Errorf("unable to copy last verification timestamp: %s", err)
	}

	// Compute stats for updated range.
	now := r.rm.Clock().Timestamp()
	iter := newRangeDataIterator(&split.UpdatedDesc, batch)
	ms, err := engine.MVCCComputeStats(iter, now.WallTime)
	iter.Close()
	if err != nil {
		return util.Errorf("unable to compute stats for updated range after split: %s", err)
	}
	if err := r.stats.SetMVCCStats(batch, ms); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Initialize the new range's response cache by copying the original's.
	if err = r.respCache.CopyInto(batch, split.NewDesc.RangeID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Add the new split replica to the store. This step atomically
	// updates the EndKey of the updated replica and also adds the
	// new replica to the store's replica map.
	newRng, err := NewReplica(&split.NewDesc, r.rm)
	if err != nil {
		return err
	}

	// Compute stats for new range.
	iter = newRangeDataIterator(&split.NewDesc, batch)
	ms, err = engine.MVCCComputeStats(iter, now.WallTime)
	iter.Close()
	if err != nil {
		return util.Errorf("unable to compute stats for new range after split: %s", err)
	}
	if err = newRng.stats.SetMVCCStats(batch, ms); err != nil {
		return util.Errorf("unable to write MVCC stats: %s", err)
	}

	// Copy the timestamp cache into the new range.
	r.Lock()
	r.tsCache.MergeInto(newRng.tsCache, true /* clear */)
	r.Unlock()

	batch.Defer(func() {
		if err := r.rm.SplitRange(r, newRng); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update Store after split: %s", err)
		}
	})

	return nil
}

// AdminMerge extends the range to subsume the range that comes next in
// the key space. The range being subsumed is provided in args.SubsumedRange.
// The EndKey of the subsuming range must equal the start key of the
// range being subsumed. The merge is performed inside of a distributed
// transaction which writes the updated range descriptor for the subsuming range
// and deletes the range descriptor for the subsumed one. It also updates the
// range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a merge trigger
// carried out as part of the commit of that transaction.
// A merge requires that the two ranges are collocate on the same set of replicas.
func (r *Replica) AdminMerge(args proto.AdminMergeRequest) (proto.AdminMergeResponse, error) {
	var reply proto.AdminMergeResponse

	// Only allow a single split/merge per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Lookup subsumed range.
	desc := r.Desc()
	if desc.EndKey.Equal(proto.KeyMax) {
		// Noop.
		return reply, nil
	}
	subsumedRng := r.rm.LookupReplica(desc.EndKey, nil)
	if subsumedRng == nil {
		return reply, util.Errorf("ranges not collocated; migration of ranges in anticipation of merge not yet implemented")
	}
	subsumedDesc := subsumedRng.Desc()

	// Make sure the range being subsumed follows this one.
	if !bytes.Equal(desc.EndKey, subsumedDesc.StartKey) {
		return reply, util.Errorf("Ranges that are not adjacent cannot be merged, %s != %s", desc.EndKey, subsumedDesc.StartKey)
	}

	// Ensure that both ranges are collocate by intersecting the store ids from
	// their replicas.
	if !replicaSetsEqual(subsumedDesc.GetReplicas(), desc.GetReplicas()) {
		return reply, util.Error("The two ranges replicas are not collocate")
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = subsumedDesc.EndKey

	log.Infof("initiating a merge of %s into %s", subsumedRng, r)

	if err := r.rm.DB().Txn(func(txn *client.Txn) error {
		// Update the range descriptor for the receiving range.
		b := &client.Batch{}
		desc1Key := keys.RangeDescriptorKey(updatedDesc.StartKey)
		if err := updateRangeDescriptor(b, desc1Key, desc, &updatedDesc); err != nil {
			return err
		}

		// Remove the range descriptor for the deleted range.
		// TODO(bdarnell): need a conditional delete?
		desc2Key := keys.RangeDescriptorKey(subsumedDesc.StartKey)
		b.Del(desc2Key)

		if err := mergeRangeAddressing(b, desc, &updatedDesc); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		b.InternalAddCall(proto.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: args.Key},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					MergeTrigger: &proto.MergeTrigger{
						UpdatedDesc:     updatedDesc,
						SubsumedRangeID: subsumedDesc.RangeID,
					},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Run(b)
	}); err != nil {
		return reply, util.Errorf("merge of range %d into %d failed: %s", subsumedDesc.RangeID, desc.RangeID, err)
	}

	return reply, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
func (r *Replica) mergeTrigger(batch engine.Engine, merge *proto.MergeTrigger) error {
	if !bytes.Equal(r.Desc().StartKey, merge.UpdatedDesc.StartKey) {
		return util.Errorf("range and updated range start keys do not match: %s != %s",
			r.Desc().StartKey, merge.UpdatedDesc.StartKey)
	}

	if !r.Desc().EndKey.Less(merge.UpdatedDesc.EndKey) {
		return util.Errorf("range end key is not less than the post merge end key: %s >= %s",
			r.Desc().EndKey, merge.UpdatedDesc.EndKey)
	}

	if merge.SubsumedRangeID <= 0 {
		return util.Errorf("subsumed  range ID must be provided: %d", merge.SubsumedRangeID)
	}

	// Copy the subsumed range's response cache to the subsuming one.
	if err := r.respCache.CopyFrom(batch, merge.SubsumedRangeID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Compute stats for updated range.
	now := r.rm.Clock().Timestamp()
	iter := newRangeDataIterator(&merge.UpdatedDesc, batch)
	ms, err := engine.MVCCComputeStats(iter, now.WallTime)
	iter.Close()
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
	r.tsCache.Clear(r.rm.Clock())
	r.Unlock()

	batch.Defer(func() {
		if err := r.rm.MergeRange(r, merge.UpdatedDesc.EndKey, merge.SubsumedRangeID); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf("failed to update store after merging range: %s", err)
		}
	})
	return nil
}

func (r *Replica) changeReplicasTrigger(change *proto.ChangeReplicasTrigger) error {
	cpy := *r.Desc()
	cpy.Replicas = change.UpdatedReplicas
	if err := r.setDesc(&cpy); err != nil {
		return err
	}
	// If we're removing the current replica, add it to the range GC queue.
	if change.ChangeType == proto.REMOVE_REPLICA && r.rm.StoreID() == change.Replica.StoreID {
		if err := r.rm.rangeGCQueue().Add(r, 1.0); err != nil {
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
func (r *Replica) ChangeReplicas(changeType proto.ReplicaChangeType, replica proto.Replica) error {
	// Only allow a single change per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Validate the request and prepare the new descriptor.
	desc := r.Desc()
	updatedDesc := *desc
	updatedDesc.Replicas = append([]proto.Replica{}, desc.Replicas...)
	found := -1       // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas {
		nodeUsed = nodeUsed || existingRep.NodeID == replica.NodeID
		if existingRep.NodeID == replica.NodeID &&
			existingRep.StoreID == replica.StoreID {
			found = i
			break
		}
	}
	if changeType == proto.ADD_REPLICA {
		// If the replica exists on the remote node, no matter in which store,
		// abort the replica add.
		if nodeUsed {
			return util.Errorf("adding replica %v which is already present in range %d",
				replica, desc.RangeID)
		}
		updatedDesc.Replicas = append(updatedDesc.Replicas, replica)
	} else if changeType == proto.REMOVE_REPLICA {
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if found == -1 {
			return util.Errorf("removing replica %v which is not present in range %d",
				replica, desc.RangeID)
		}
		updatedDesc.Replicas[found] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
		updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
	}

	err := r.rm.DB().Txn(func(txn *client.Txn) error {
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
		b.InternalAddCall(proto.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: updatedDesc.StartKey},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					ChangeReplicasTrigger: &proto.ChangeReplicasTrigger{
						NodeID:          replica.NodeID,
						StoreID:         replica.StoreID,
						ChangeType:      changeType,
						Replica:         replica,
						UpdatedReplicas: updatedDesc.Replicas,
					},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Run(b)
	})
	if err != nil {
		return util.Errorf("change replicas of %d failed: %s", desc.RangeID, err)
	}
	return nil
}

// replicaSetsEqual is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func replicaSetsEqual(a, b []proto.Replica) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[proto.StoreID]int)
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
func updateRangeDescriptor(b *client.Batch, descKey proto.Key, oldDesc, newDesc *proto.RangeDescriptor) error {
	var oldValue []byte
	if oldDesc != nil {
		var err error
		if oldValue, err = gogoproto.Marshal(oldDesc); err != nil {
			return err
		}
	}
	newValue, err := gogoproto.Marshal(newDesc)
	if err != nil {
		return err
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}
