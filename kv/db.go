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

package kv

import (
	"bytes"
	"encoding/gob"
	"net"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// A DB interface provides asynchronous methods to access a key value store.
type DB interface {
	Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse
	Get(args *storage.GetRequest) <-chan *storage.GetResponse
	Put(args *storage.PutRequest) <-chan *storage.PutResponse
	Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse
	Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse
	DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse
	Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse
	EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse
	AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse
	ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse
	EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse
	EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse
}

// GetI fetches the value at the specified key and deserializes it
// into "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value. The first result parameter is "ok": true if a value was
// found for the requested key; false otherwise. An error is returned
// on error fetching from underlying storage or deserializing value.
func GetI(db DB, key storage.Key, value interface{}) (bool, int64, error) {
	gr := <-db.Get(&storage.GetRequest{Key: key})
	if gr.Error != nil {
		return false, 0, gr.Error
	}
	if len(gr.Value.Bytes) == 0 {
		return false, 0, nil
	}
	if err := gob.NewDecoder(bytes.NewBuffer(gr.Value.Bytes)).Decode(value); err != nil {
		return true, gr.Value.Timestamp, err
	}
	return true, gr.Value.Timestamp, nil
}

// PutI sets the given key to the serialized byte string of the value
// provided. Uses current time and default expiration.
func PutI(db DB, key storage.Key, value interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	pr := <-db.Put(&storage.PutRequest{
		Key: key,
		Value: storage.Value{
			Bytes:     buf.Bytes(),
			Timestamp: time.Now().UnixNano(),
		},
	})
	return pr.Error
}

// BootstrapRangeLocations sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeLocations(db DB, replica storage.Replica) error {
	locations := storage.RangeLocations{
		Replicas: []storage.Replica{replica},
		// TODO(spencer): uncomment when we have hrsht's change.
		//StartKey: storage.KeyMin,
	}
	// Write meta1.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta1Prefix, storage.KeyMax), locations); err != nil {
		return err
	}
	// Write meta2.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta2Prefix, storage.KeyMax), locations); err != nil {
		return err
	}
	return nil
}

// UpdateRangeLocations updates the range locations metadata for the
// range specified by the meta parameter. This always involves a write
// to "meta2", and may require a write to "meta1", in the event that
// meta.EndKey is a "meta2" key (prefixed by KeyMeta2Prefix).
func UpdateRangeLocations(db DB, meta storage.RangeMetadata, locations storage.RangeLocations) error {
	// TODO(spencer): a lot more work here to actually implement this.

	// Write meta2.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta2Prefix, meta.EndKey), locations); err != nil {
		return err
	}
	return nil
}

// A DistDB provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges.
type DistDB struct {
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache util.LRUCache
}

// NewDB returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDB(gossip *gossip.Gossip) *DistDB {
	return &DistDB{gossip: gossip}
}

func (db *DistDB) nodeIDToAddr(nodeID int32) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDGossipKey(nodeID)
	info, err := db.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %v. Error: %v", nodeID, err)
	}
	return info.(net.Addr), nil
}

func (db *DistDB) lookupMetadata(metadataKey storage.Key, replicas []storage.Replica) (*storage.RangeLocations, error) {
	replica := storage.ChooseRandomReplica(replicas)
	if replica == nil {
		return nil, util.Errorf("No replica to choose for metadata key: %q", metadataKey)
	}

	addr, err := db.nodeIDToAddr(replica.NodeID)
	if err != nil {
		// TODO(harshit): May be retry a different replica.
		return nil, err
	}
	client := rpc.NewClient(addr)
	arg := &storage.InternalRangeLookupRequest{
		RequestHeader: storage.RequestHeader{
			Replica: *replica,
		},
		Key: metadataKey,
	}
	var reply storage.InternalRangeLookupResponse
	err = client.Call("Node.InternalRangeLookup", arg, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Error != nil {
		return nil, reply.Error
	}
	return &reply.Locations, nil
}

// TODO(harshit): Consider caching returned metadata info.
func (db *DistDB) lookupMeta1(key storage.Key) (*storage.RangeLocations, error) {
	info, err := db.gossip.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		return nil, err
	}
	metadataKey := storage.MakeKey(storage.KeyMeta1Prefix, key)
	return db.lookupMetadata(metadataKey, info.(storage.RangeLocations).Replicas)
}

func (db *DistDB) lookupMeta2(key storage.Key) (*storage.RangeLocations, error) {
	meta1Val, err := db.lookupMeta1(key)
	if err != nil {
		return nil, err
	}
	metadataKey := storage.MakeKey(storage.KeyMeta2Prefix, key)
	return db.lookupMetadata(metadataKey, meta1Val.Replicas)
}

// getNode gets an RPC client to the node where the requested
// key is located. The range cache may be updated. The bi-level range
// metadata for the cluster is consulted in the event that the local
// cache doesn't contain range metadata corresponding to the specified
// key.
func (db *DistDB) getNode(key storage.Key) (*rpc.Client, *storage.Replica, error) {
	meta2Val, err := db.lookupMeta2(key)
	if err != nil {
		return nil, nil, err
	}
	replica := storage.ChooseRandomReplica(meta2Val.Replicas)
	if replica == nil {
		return nil, nil, util.Errorf("No node found for key: %q", key)
	}
	addr, err := db.nodeIDToAddr(replica.NodeID)
	if err != nil {
		// TODO(harshit): May be retry a different replica.
		return nil, nil, err
	}
	return rpc.NewClient(addr), replica, nil
}

// sendRPC sends the specified RPC asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *DistDB) sendRPC(key storage.Key, method string, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)

	go func() {
		replyVal := reflect.ValueOf(reply)
		node, replica, err := db.getNode(key)
		if err == nil {
			argsHeader := args.(*storage.RequestHeader)
			argsHeader.Replica = *replica
			err = node.Call(method, args, reply)
		}
		if err != nil {
			// TODO(spencer): check error here; we need to clear this
			// segment of range cache and retry getNode() if the range
			// wasn't found.
			reflect.Indirect(replyVal).FieldByName("Error").Set(reflect.ValueOf(err))
		}
		chanVal.Send(replyVal)
	}()

	return chanVal.Interface()
}

// Contains checks for the existence of a key.
func (db *DistDB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	return db.sendRPC(args.Key, "Node.Contains",
		args, &storage.ContainsResponse{}).(chan *storage.ContainsResponse)
}

// Get .
func (db *DistDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.sendRPC(args.Key, "Node.Get",
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put .
func (db *DistDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.sendRPC(args.Key, "Node.Put",
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}

// Increment .
func (db *DistDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.sendRPC(args.Key, "Node.Increment",
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}

// Delete .
func (db *DistDB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	return db.sendRPC(args.Key, "Node.Delete",
		args, &storage.DeleteResponse{}).(chan *storage.DeleteResponse)
}

// DeleteRange .
func (db *DistDB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	// TODO(spencer): range of keys.
	return db.sendRPC(args.StartKey, "Node.DeleteRange",
		args, &storage.DeleteRangeResponse{}).(chan *storage.DeleteRangeResponse)
}

// Scan .
func (db *DistDB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	// TODO(spencer): range of keys.
	return nil
}

// EndTransaction .
func (db *DistDB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	// TODO(spencer): multiple keys here...
	return db.sendRPC(args.Keys[0], "Node.EndTransaction",
		args, &storage.EndTransactionResponse{}).(chan *storage.EndTransactionResponse)
}

// AccumulateTS is used to efficiently accumulate a time series of
// int64 quantities representing discrete subtimes. For example, a
// key/value might represent a minute of data. Each would contain 60
// int64 counts, each representing a second.
func (db *DistDB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	return db.sendRPC(args.Key, "Node.AccumulateTS",
		args, &storage.AccumulateTSResponse{}).(chan *storage.AccumulateTSResponse)
}

// ReapQueue scans and deletes messages from a recipient message
// queue. ReapQueueRequest invocations must be part of an extant
// transaction or they fail. Returns the reaped queue messsages, up to
// the requested maximum. If fewer than the maximum were returned,
// then the queue is empty.
func (db *DistDB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	return db.sendRPC(args.Inbox, "Node.ReapQueue",
		args, &storage.ReapQueueResponse{}).(chan *storage.ReapQueueResponse)
}

// EnqueueUpdate enqueues an update for eventual execution.
func (db *DistDB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	// TODO(spencer): queued updates go to system-reserved keys.
	return nil
}

// EnqueueMessage enqueues a message for delivery to an inbox.
func (db *DistDB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	return db.sendRPC(args.Inbox, "Node.EnqueueMessage",
		args, &storage.EnqueueMessageResponse{}).(chan *storage.EnqueueMessageResponse)
}
