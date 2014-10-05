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
	InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse
	InternalPushTxn(args *proto.InternalPushTxnRequest) <-chan *proto.InternalPushTxnResponse
	InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse
	InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse
	InternalSplit(args *proto.InternalSplitRequest) <-chan *proto.InternalSplitResponse

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

// InternalSplit is called by a range leader coordinating a split of its range.
func (db *BaseDB) InternalSplit(args *proto.InternalSplitRequest) <-chan *proto.InternalSplitResponse {
	replyChan := make(chan *proto.InternalSplitResponse, 1)
	go db.Executor(InternalSplit, args, replyChan)
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
func GetI(db DB, key engine.Key, iface interface{}) (bool, proto.Timestamp, error) {
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
func GetProto(db DB, key engine.Key, msg gogoproto.Message) (bool, proto.Timestamp, error) {
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
func getInternal(db DB, key engine.Key) (*proto.Value, error) {
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

// PutI sets the given key to the gob-serialized byte string of value
// and the provided timestamp.
func PutI(db DB, key engine.Key, iface interface{}, timestamp proto.Timestamp) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(iface); err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: buf.Bytes()}, timestamp)
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp.
func PutProto(db DB, key engine.Key, msg gogoproto.Message, timestamp proto.Timestamp) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: data}, timestamp)
}

// putInternal writes the specified value to key.
func putInternal(db DB, key engine.Key, value proto.Value, timestamp proto.Timestamp) error {
	value.InitChecksum(key)
	pr := <-db.Put(&proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			User:      UserRoot,
			Timestamp: timestamp,
		},
		Value: value,
	})
	return pr.GoError()
}

// BootstrapRangeDescriptor sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeDescriptor(db DB, desc *proto.RangeDescriptor, timestamp proto.Timestamp) error {
	// Write meta1.
	if err := PutProto(db, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	// Write meta2.
	if err := PutProto(db, engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	return nil
}

// BootstrapConfigs sets default configurations for accounting,
// permissions, and zones. All configs are specified for the empty key
// prefix, meaning they apply to the entire database. Permissions are
// granted to all users and the zone requires three replicas with no
// other specifications.
func BootstrapConfigs(db DB, timestamp proto.Timestamp) error {
	// Accounting config.
	acctConfig := &proto.AcctConfig{}
	key := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.KeyMin)
	if err := PutProto(db, key, acctConfig, timestamp); err != nil {
		return err
	}
	// Permission config.
	permConfig := &proto.PermConfig{
		Read:  []string{UserRoot}, // root user
		Write: []string{UserRoot}, // root user
	}
	key = engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.KeyMin)
	if err := PutProto(db, key, permConfig, timestamp); err != nil {
		return err
	}
	// Zone config.
	// TODO(spencer): change this when zone specifications change to elect for three
	// replicas with no specific features set.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			proto.Attributes{},
			proto.Attributes{},
			proto.Attributes{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
	}
	key = engine.MakeKey(engine.KeyConfigZonePrefix, engine.KeyMin)
	if err := PutProto(db, key, zoneConfig, timestamp); err != nil {
		return err
	}
	return nil
}

// UpdateRangeDescriptor updates the range locations metadata for the
// range specified by the meta parameter. This always involves a write
// to "meta2", and may require a write to "meta1", in the event that
// meta.EndKey is a "meta2" key (prefixed by KeyMeta2Prefix).
func UpdateRangeDescriptor(db DB, meta proto.RangeMetadata,
	desc *proto.RangeDescriptor, timestamp proto.Timestamp) error {
	// TODO(spencer): a lot more work here to actually implement this.

	// Write meta2.
	key := engine.MakeKey(engine.KeyMeta2Prefix, meta.EndKey)
	if err := PutProto(db, key, desc, timestamp); err != nil {
		return err
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
func NewTransaction(name string, baseKey engine.Key, userPriority int32,
	isolation proto.IsolationType, clock *hlc.Clock) *proto.Transaction {
	// Compute priority by adjusting based on userPriority factor.
	priority := proto.MakePriority(userPriority)
	// Compute timestamp and max timestamp.
	now := clock.Now()
	max := now
	max.WallTime += clock.MaxDrift().Nanoseconds()

	return &proto.Transaction{
		Name:         name,
		ID:           append(append([]byte(nil), baseKey...), []byte(uuid.New())...),
		Priority:     priority,
		Isolation:    isolation,
		Timestamp:    now,
		MaxTimestamp: max,
	}
}
