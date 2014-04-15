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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/cockroach/util"
)

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
	rangeKey  Key        // The start key for this range.
	engine    Engine     // The underlying key-value store.
	allocator *allocator // Makes allocation decisions.
	mu        sync.Mutex // Protects the pending list.
	pending   *list.List // Not-yet-proposed log entries.
	// TODO(andybons): raft instance goes here.
}

// NewRange initializes the range starting at key. The underlying
// engine is queried for range metadata. Returns an error if the range
// doesn't exist.
func NewRange(rangeKey Key, engine Engine, allocator *allocator) (*Range, error) {
	rng := &Range{
		rangeKey:  rangeKey,
		engine:    engine,
		allocator: allocator,
		pending:   list.New(),
	}
	// TODO(spencer): query underlying engine for metadata.
	return rng, nil
}

// readOnlyCmd executes a read-only command against the store. If this
// server has executed a raft command or heartbeat at a timestamp
// greater than the read timestamp, we can satisfy the read locally
// without further ado. Otherwise, we must contact ceil(N/2) raft
// participants with a ReadQuorum RPC to determine with certainty
// whether our local data is up to date. The requests to other
// participants simply check their in-memory latest-write-cache to
// determine whether the key in question was updated since this node's
// last raft heartbeat, but before this command's read timestamp. In
// the process, the latest-read-cache timestamp for the key being read
// is updated on each participant. If this replica has stale info for
// the key, an error is returned to the client to retry at the replica
// with newer information.
func (r *Range) readOnlyCmd(method string, args, reply interface{}) error {
	if r == nil {
		return util.Errorf("invalid node specification")
	}
	return nil
}

// readWriteCmd executes a read-write command against the store. If
// this node is the raft leader, it proposes the write to the other
// raft participants. Otherwise, the write is forwarded via a
// FollowerPropose RPC to the leader and this replica waits for an ACK
// to execute the command locally and return the result to the
// requesting client.
//
// Commands which mutate the store must be proposed as part of the
// raft consensus write protocol. Only after committed can the command
// be executed. To facilitate this, readWriteCmd returns a channel
// which is signaled upon completion.
func (r *Range) readWriteCmd(method string, args, reply interface{}) <-chan error {
	if r == nil {
		c := make(chan error, 1)
		c <- util.Errorf("invalid node specification")
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	logEntry := &LogEntry{
		Method: method,
		Args:   args,
		Reply:  reply,
		done:   make(chan error, 1),
	}
	r.pending.PushBack(logEntry)

	return logEntry.done
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
func (r *Range) executeCmd(method string, args, reply interface{}) error {
	switch method {
	case "Contains":
		return r.Contains(args.(*ContainsRequest), reply.(*ContainsResponse))
	case "Get":
		return r.Get(args.(*GetRequest), reply.(*GetResponse))
	case "Put":
		return r.Put(args.(*PutRequest), reply.(*PutResponse))
	case "Increment":
		return r.Increment(args.(*IncrementRequest), reply.(*IncrementResponse))
	case "Delete":
		return r.Delete(args.(*DeleteRequest), reply.(*DeleteResponse))
	case "DeleteRange":
		return r.DeleteRange(args.(*DeleteRangeRequest), reply.(*DeleteRangeResponse))
	case "Scan":
		return r.Scan(args.(*ScanRequest), reply.(*ScanResponse))
	case "EndTransaction":
		return r.EndTransaction(args.(*EndTransactionRequest), reply.(*EndTransactionResponse))
	case "AccumulateTS":
		return r.AccumulateTS(args.(*AccumulateTSRequest), reply.(*AccumulateTSResponse))
	case "ReapQueue":
		return r.ReapQueue(args.(*ReapQueueRequest), reply.(*ReapQueueResponse))
	case "EnqueueUpdate":
		return r.EnqueueUpdate(args.(*EnqueueUpdateRequest), reply.(*EnqueueUpdateResponse))
	case "EnqueueMessage":
		return r.EnqueueMessage(args.(*EnqueueMessageRequest), reply.(*EnqueueMessageResponse))
	default:
		return util.Errorf("unrecognized command type: %s", method)
	}
}

// Contains verifies the existence of a key in the key value store.
func (r *Range) Contains(args *ContainsRequest, reply *ContainsResponse) error {
	val, err := r.engine.get(args.Key)
	if err != nil {
		reply.Error = err
	}
	if val.Bytes != nil {
		reply.Exists = true
	}
	return nil
}

// Get returns the value for a specified key.
func (r *Range) Get(args *GetRequest, reply *GetResponse) error {
	val, err := r.engine.get(args.Key)
	if err != nil {
		reply.Error = err
	} else if val.Bytes == nil {
		reply.Error = util.Errorf("key %q not found", args.Key)
	} else {
		reply.Value.Bytes = val.Bytes
	}
	return nil
}

// Put sets the value for a specified key. Conditional puts are supported.
func (r *Range) Put(args *PutRequest, reply *PutResponse) error {
	// Handle conditional put.
	if args.ExpValue != nil {
		// Handle check for non-existence of key.
		val, err := r.engine.get(args.Key)
		if err != nil {
			reply.Error = err
			return nil
		}
		if args.ExpValue.Bytes == nil && val.Bytes != nil {
			reply.Error = util.Errorf("key %q already exists", args.Key)
			return nil
		} else if args.ExpValue != nil {
			// Handle check for existence when there is no key.
			if val.Bytes == nil {
				reply.Error = util.Errorf("key %q does not exist", args.Key)
				return nil
			} else if !bytes.Equal(args.ExpValue.Bytes, val.Bytes) {
				reply.ActualValue.Bytes = val.Bytes
				reply.Error = util.Errorf("key %q does not match existing", args.Key)
				return nil
			}
		}
	}
	if err := r.engine.put(args.Key, args.Value); err != nil {
		reply.Error = err
	}
	return nil
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no
// value exists for the key, zero is incremented.
func (r *Range) Increment(args *IncrementRequest, reply *IncrementResponse) error {
	// First retrieve existing value.
	val, err := r.engine.get(args.Key)
	if err != nil {
		reply.Error = err
		return nil
	}
	var int64Val int64
	// If the value exists, attempt to decode it as a varint.
	if val.Bytes != nil {
		var numBytes int
		int64Val, numBytes = binary.Varint(val.Bytes)
		if numBytes == 0 {
			reply.Error = util.Errorf("key %q cannot be incremented; not varint-encoded", args.Key)
			return nil
		} else if numBytes < 0 {
			reply.Error = util.Errorf("key %q cannot be incremented; integer overflow", args.Key)
			return nil
		}
	}
	int64Val += args.Increment
	encoded := make([]byte, binary.MaxVarintLen64)
	numBytes := binary.PutVarint(encoded, int64Val)
	encoded = encoded[:numBytes]
	if err = r.engine.put(args.Key, Value{Bytes: encoded, Timestamp: args.Timestamp}); err != nil {
		reply.Error = err
	}
	return nil
}

// Delete deletes the key and value specified by key.
func (r *Range) Delete(args *DeleteRequest, reply *DeleteResponse) error {
	if err := r.engine.del(args.Key); err != nil {
		reply.Error = err
	}
	return nil
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Range) DeleteRange(args *DeleteRangeRequest, reply *DeleteRangeResponse) error {
	return util.Error("unimplemented")
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Range) Scan(args *ScanRequest, reply *ScanResponse) error {
	return util.Error("unimplemented")
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Range) EndTransaction(args *EndTransactionRequest, reply *EndTransactionResponse) error {
	return util.Error("unimplemented")
}

// AccumulateTS is used internally to aggregate statistics over key
// ranges throughout the distributed cluster.
func (r *Range) AccumulateTS(args *AccumulateTSRequest, reply *AccumulateTSResponse) error {
	return util.Error("unimplemented")
}

// ReapQueue destructively queries messages from a delivery inbox
// queue. This method must be called from within a transaction.
func (r *Range) ReapQueue(args *ReapQueueRequest, reply *ReapQueueResponse) error {
	return util.Error("unimplemented")
}

// EnqueueUpdate sidelines an update for asynchronous execution.
// AccumulateTS updates are sent this way. Eventually-consistent indexes
// are also built using update queues. Crucially, the enqueue happens
// as part of the caller's transaction, so is guaranteed to be
// executed if the transaction succeeded.
func (r *Range) EnqueueUpdate(args *EnqueueUpdateRequest, reply *EnqueueUpdateResponse) error {
	return util.Error("unimplemented")
}

// EnqueueMessage enqueues a message (Value) for delivery to a
// recipient inbox.
func (r *Range) EnqueueMessage(args *EnqueueMessageRequest, reply *EnqueueMessageResponse) error {
	return util.Error("unimplemented")
}
