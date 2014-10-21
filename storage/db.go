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

package storage

import (
	"bytes"
	"encoding/gob"

	"code.google.com/p/go-uuid/uuid"
	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// A TransactionOptions describes parameters for use with DB.RunTransaction.
type TransactionOptions struct {
	Name         string // Concise desc of txn for debugging
	User         string
	UserPriority int32
	Isolation    proto.IsolationType
	Retry        *util.RetryOptions // Nil for defaults
}

// A DB interface provides asynchronous methods to access a key value
// store. It is defined in storage as Ranges must use it directly. For
// example, to split using distributed txns, push transactions in the
// event commands collide with existing write intents, and process
// message queues.
type DB interface {
	Contains(args *proto.ContainsRequest) <-chan *proto.ContainsResponse
	Get(args *proto.GetRequest) <-chan *proto.GetResponse
	Put(args *proto.PutRequest) <-chan *proto.PutResponse
	ConditionalPut(args *proto.ConditionalPutRequest) <-chan *proto.ConditionalPutResponse
	Increment(args *proto.IncrementRequest) <-chan *proto.IncrementResponse
	Delete(args *proto.DeleteRequest) <-chan *proto.DeleteResponse
	DeleteRange(args *proto.DeleteRangeRequest) <-chan *proto.DeleteRangeResponse
	Scan(args *proto.ScanRequest) <-chan *proto.ScanResponse
	BeginTransaction(args *proto.BeginTransactionRequest) <-chan *proto.BeginTransactionResponse
	EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse
	AccumulateTS(args *proto.AccumulateTSRequest) <-chan *proto.AccumulateTSResponse
	ReapQueue(args *proto.ReapQueueRequest) <-chan *proto.ReapQueueResponse
	EnqueueUpdate(args *proto.EnqueueUpdateRequest) <-chan *proto.EnqueueUpdateResponse
	EnqueueMessage(args *proto.EnqueueMessageRequest) <-chan *proto.EnqueueMessageResponse
	InternalEndTxn(args *proto.InternalEndTxnRequest) <-chan *proto.InternalEndTxnResponse
	InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse
	InternalPushTxn(args *proto.InternalPushTxnRequest) <-chan *proto.InternalPushTxnResponse
	InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse
	InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse
	AdminSplit(args *proto.AdminSplitRequest) <-chan *proto.AdminSplitResponse

	RunTransaction(opts *TransactionOptions, retryable func(db DB) error) error
}

// BaseDB provides a base struct for implementions of the DB
// interface. It provides a ready-made harness for creating reply
// channels and passing args and channel to a configured executor
// function.
type BaseDB struct {
	Executor func(method string, args proto.Request, replyChan interface{})
}

// NewBaseDB creates a new instance initialized with the specified
// command executor.
func NewBaseDB(executor func(method string, args proto.Request, replyChan interface{})) *BaseDB {
	return &BaseDB{
		Executor: executor,
	}
}

// Contains determines whether the KV map contains the specified key.
func (db *BaseDB) Contains(args *proto.ContainsRequest) <-chan *proto.ContainsResponse {
	replyChan := make(chan *proto.ContainsResponse, 1)
	go db.Executor(Contains, args, replyChan)
	return replyChan
}

// Get fetches the value for a key from the KV map, respecting a
// possibly historical timestamp. If the timestamp is 0, returns
// the most recent value.
func (db *BaseDB) Get(args *proto.GetRequest) <-chan *proto.GetResponse {
	replyChan := make(chan *proto.GetResponse, 1)
	go db.Executor(Get, args, replyChan)
	return replyChan
}

// Put sets the value for a key at the specified timestamp. If the
// timestamp is 0, the value is set with the current time as timestamp.
func (db *BaseDB) Put(args *proto.PutRequest) <-chan *proto.PutResponse {
	// Init the value checksum if not set by requesting client.
	args.Value.InitChecksum(args.Key)
	replyChan := make(chan *proto.PutResponse, 1)
	go db.Executor(Put, args, replyChan)
	return replyChan
}

// ConditionalPut sets the value for a key if the existing value
// matches the value specified in the request. Specifying a null value
// for existing means the value must not yet exist.
func (db *BaseDB) ConditionalPut(args *proto.ConditionalPutRequest) <-chan *proto.ConditionalPutResponse {
	// Init the value checksum if not set by requesting client.
	args.Value.InitChecksum(args.Key)
	replyChan := make(chan *proto.ConditionalPutResponse, 1)
	go db.Executor(ConditionalPut, args, replyChan)
	return replyChan
}

// Increment increments the value at the specified key. Once called
// for a key, Put & Get will return errors; only Increment will
// continue to be a valid command. The value must be deleted before
// it can be reset using Put.
func (db *BaseDB) Increment(args *proto.IncrementRequest) <-chan *proto.IncrementResponse {
	replyChan := make(chan *proto.IncrementResponse, 1)
	go db.Executor(Increment, args, replyChan)
	return replyChan
}

// Delete removes the value for the specified key.
func (db *BaseDB) Delete(args *proto.DeleteRequest) <-chan *proto.DeleteResponse {
	replyChan := make(chan *proto.DeleteResponse, 1)
	go db.Executor(Delete, args, replyChan)
	return replyChan
}

// DeleteRange removes all values for keys which fall between
// args.RequestHeader.Key and args.RequestHeader.EndKey.
func (db *BaseDB) DeleteRange(args *proto.DeleteRangeRequest) <-chan *proto.DeleteRangeResponse {
	replyChan := make(chan *proto.DeleteRangeResponse, 1)
	go db.Executor(DeleteRange, args, replyChan)
	return replyChan
}

// Scan fetches the values for all keys which fall between
// args.RequestHeader.Key and args.RequestHeader.EndKey.
func (db *BaseDB) Scan(args *proto.ScanRequest) <-chan *proto.ScanResponse {
	replyChan := make(chan *proto.ScanResponse, 1)
	go db.Executor(Scan, args, replyChan)
	return replyChan
}

// BeginTransaction starts a transaction by initializing a new
// Transaction proto using the contents of the request. Note that this
// method does not call through to the key value interface but instead
// services it directly, as creating a new transaction requires only
// access to the node's clock. Nothing must be read or written.
func (db *BaseDB) BeginTransaction(args *proto.BeginTransactionRequest) <-chan *proto.BeginTransactionResponse {
	replyChan := make(chan *proto.BeginTransactionResponse, 1)
	go db.Executor(BeginTransaction, args, replyChan)
	return replyChan
}

// EndTransaction either commits or aborts an ongoing transaction.
func (db *BaseDB) EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse {
	replyChan := make(chan *proto.EndTransactionResponse, 1)
	go db.Executor(EndTransaction, args, replyChan)
	return replyChan
}

// AccumulateTS is used to efficiently accumulate a time series of
// int64 quantities representing discrete subtimes. For example, a
// key/value might represent a minute of data. Each would contain 60
// int64 counts, each representing a second.
func (db *BaseDB) AccumulateTS(args *proto.AccumulateTSRequest) <-chan *proto.AccumulateTSResponse {
	replyChan := make(chan *proto.AccumulateTSResponse, 1)
	go db.Executor(AccumulateTS, args, replyChan)
	return replyChan
}

// ReapQueue scans and deletes messages from a recipient message
// queue. ReapQueueRequest invocations must be part of an extant
// transaction or they fail. Returns the reaped queue messsages, up to
// the requested maximum. If fewer than the maximum were returned,
// then the queue is empty.
func (db *BaseDB) ReapQueue(args *proto.ReapQueueRequest) <-chan *proto.ReapQueueResponse {
	replyChan := make(chan *proto.ReapQueueResponse, 1)
	go db.Executor(ReapQueue, args, replyChan)
	return replyChan
}

// EnqueueUpdate enqueues an update for eventual execution.
func (db *BaseDB) EnqueueUpdate(args *proto.EnqueueUpdateRequest) <-chan *proto.EnqueueUpdateResponse {
	replyChan := make(chan *proto.EnqueueUpdateResponse, 1)
	go db.Executor(EnqueueUpdate, args, replyChan)
	return replyChan
}

// EnqueueMessage enqueues a message for delivery to an inbox.
func (db *BaseDB) EnqueueMessage(args *proto.EnqueueMessageRequest) <-chan *proto.EnqueueMessageResponse {
	replyChan := make(chan *proto.EnqueueMessageResponse, 1)
	go db.Executor(EnqueueMessage, args, replyChan)
	return replyChan
}

// InternalEndTxn is similar to EndTransaction, except additionally
// provides support for system-specific triggers on successful commit.
func (db *BaseDB) InternalEndTxn(args *proto.InternalEndTxnRequest) <-chan *proto.InternalEndTxnResponse {
	replyChan := make(chan *proto.InternalEndTxnResponse, 1)
	go db.Executor(InternalEndTxn, args, replyChan)
	return replyChan
}

// InternalHeartbeatTxn sends a periodic heartbeat to extant
// transaction rows to indicate the client is still alive and
// the transaction should not be considered abandoned.
func (db *BaseDB) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse {
	replyChan := make(chan *proto.InternalHeartbeatTxnResponse, 1)
	go db.Executor(InternalHeartbeatTxn, args, replyChan)
	return replyChan
}

// InternalPushTxn attempts to resolve read or write conflicts between
// transactions. Both the pusher (args.Txn) and the pushee
// (args.PushTxn) are supplied. However, args.Key should be set to the
// transaction ID of the pushee, as it must be directed to the range
// containing the pushee's transaction record in order to consult the
// most up to date txn state. If the conflict resolution can be
// resolved in favor of the pusher, returns success; otherwise returns
// an error code either indicating the pusher must retry or abort and
// restart the transaction.
func (db *BaseDB) InternalPushTxn(args *proto.InternalPushTxnRequest) <-chan *proto.InternalPushTxnResponse {
	replyChan := make(chan *proto.InternalPushTxnResponse, 1)
	go db.Executor(InternalPushTxn, args, replyChan)
	return replyChan
}

// InternalResolveIntent resolves existing write intents for a key or
// key range.
func (db *BaseDB) InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse {
	replyChan := make(chan *proto.InternalResolveIntentResponse, 1)
	go db.Executor(InternalResolveIntent, args, replyChan)
	return replyChan
}

// InternalSnapshotCopy scans the key range specified by start key through
// end key up to some maximum number of results from the given snapshot_id.
// It will create a snapshot if snapshot_id is empty.
func (db *BaseDB) InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse {
	replyChan := make(chan *proto.InternalSnapshotCopyResponse, 1)
	go db.Executor(InternalSnapshotCopy, args, replyChan)
	return replyChan
}

// AdminSplit is called to coordinate a split of a range.
func (db *BaseDB) AdminSplit(args *proto.AdminSplitRequest) <-chan *proto.AdminSplitResponse {
	replyChan := make(chan *proto.AdminSplitResponse, 1)
	go db.Executor(AdminSplit, args, replyChan)
	return replyChan
}

// RunTransaction executes retryable in the context of a distributed
// transaction. The transaction is automatically aborted if retryable
// returns any error aside from a txnDBError, and is automatically
// committed otherwise. retryable should have no side effects which
// could cause problems in the event it must be run more than
// once. The opts struct contains transaction settings. If limits for
// backoff / retry are enabled through the options and reached during
// transaction execution, TransactionRetryError will be returned.
func (db *BaseDB) RunTransaction(opts *TransactionOptions, retryable func(db DB) error) error {
	return util.Errorf("RunTransaction unimplemented")
}

// GetI fetches the value at the specified key and gob-deserializes it
// into "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value. The first result parameter is "ok": true if a value was
// found for the requested key; false otherwise. An error is returned
// on error fetching from underlying storage or deserializing value.
func GetI(db DB, key proto.Key, iface interface{}) (bool, proto.Timestamp, error) {
	value, err := getInternal(db, key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gob.NewDecoder(bytes.NewBuffer(value.Bytes)).Decode(iface); err != nil {
		return true, *value.Timestamp, err
	}
	return true, *value.Timestamp, nil
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. See comments for GetI for details on
// return values.
func GetProto(db DB, key proto.Key, msg gogoproto.Message) (bool, proto.Timestamp, error) {
	value, err := getInternal(db, key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gogoproto.Unmarshal(value.Bytes, msg); err != nil {
		return true, *value.Timestamp, err
	}
	return true, *value.Timestamp, nil
}

// getInternal fetches the requested key and returns the value.
func getInternal(db DB, key proto.Key) (*proto.Value, error) {
	gr := <-db.Get(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: UserRoot,
		},
	})
	if gr.Error != nil {
		return nil, gr.GoError()
	}
	if gr.Value != nil {
		return gr.Value, gr.Value.Verify(key)
	}
	return nil, nil
}

// PutI sets the given key to the gob-serialized byte string of value.
func PutI(db DB, key proto.Key, iface interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(iface); err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: buf.Bytes()})
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg.
func PutProto(db DB, key proto.Key, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: data})
}

// putInternal writes the specified value to key.
func putInternal(db DB, key proto.Key, value proto.Value) error {
	value.InitChecksum(key)
	pr := <-db.Put(&proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: UserRoot,
		},
		Value: value,
	})
	return pr.GoError()
}

// UpdateRangeAddressing updates the range addressing metadata for the
// range specified by desc.
//
// TODO(spencer): rewrite this to do all writes in parallel.
func UpdateRangeAddressing(db DB, desc *proto.RangeDescriptor) error {
	// First, the case of the range ending with a meta2 prefix. This
	// means the range is full of meta2. We must update the relevant
	// meta1 entry pointing to the end of this range.
	if bytes.HasPrefix(desc.EndKey, engine.KeyMeta2Prefix) {
		if err := PutProto(db, engine.RangeMetaKey(desc.EndKey), desc); err != nil {
			return err
		}
	} else {
		// In this case, the range ends with a normal user key, so we must
		// update the relevant meta2 entry pointing to the end of this range.
		if err := PutProto(db, engine.MakeKey(engine.KeyMeta2Prefix, desc.EndKey), desc); err != nil {
			return err
		}
		// If the range starts with KeyMin, we update the appropriate
		// meta1 entry.
		if bytes.Equal(desc.StartKey, engine.KeyMin) {
			if bytes.HasPrefix(desc.EndKey, engine.KeyMeta2Prefix) {
				if err := PutProto(db, engine.RangeMetaKey(desc.EndKey), desc); err != nil {
					return err
				}
			} else if !bytes.HasPrefix(desc.EndKey, engine.KeyMetaPrefix) {
				if err := PutProto(db, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc); err != nil {
					return err
				}
			} else {
				return util.Errorf("meta1 addressing records cannot be split: %+v", desc)
			}
		} else if bytes.HasPrefix(desc.StartKey, engine.KeyMeta2Prefix) {
			// If the range contains the final set of meta2 addressing
			// records, we update the meta1 entry pointing to KeyMax.
			if err := PutProto(db, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc); err != nil {
				return err
			}
		} else if bytes.HasPrefix(desc.StartKey, engine.KeyMeta1Prefix) {
			return util.Errorf("meta1 addressing records cannot be split: %+v", desc)
		}
	}
	return nil
}

// NewTransaction creates a new transaction. The transaction key is
// composed using the specified baseKey (for locality with data
// affected by the transaction) and a random UUID to guarantee
// uniqueness. The specified user-level priority is combined with
// a randomly chosen value to yield a final priority, used to settle
// write conflicts in a way that avoids starvation of long-running
// transactions (see Range.InternalPushTxn).
func NewTransaction(name string, baseKey proto.Key, userPriority int32,
	isolation proto.IsolationType, clock *hlc.Clock) *proto.Transaction {
	// Compute priority by adjusting based on userPriority factor.
	priority := proto.MakePriority(userPriority)
	// Compute timestamp and max timestamp.
	now := clock.Now()
	max := now
	max.WallTime += clock.MaxOffset().Nanoseconds()

	return &proto.Transaction{
		Name:         name,
		ID:           append(append([]byte(nil), baseKey...), []byte(uuid.New())...),
		Priority:     priority,
		Isolation:    isolation,
		Timestamp:    now,
		MaxTimestamp: max,
	}
}
