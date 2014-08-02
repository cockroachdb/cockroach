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
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/hlc"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// A DB interface provides asynchronous methods to access a key value store.
type DB interface {
	Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse
	Get(args *storage.GetRequest) <-chan *storage.GetResponse
	Put(args *storage.PutRequest) <-chan *storage.PutResponse
	ConditionalPut(args *storage.ConditionalPutRequest) <-chan *storage.ConditionalPutResponse
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
func GetI(db DB, key storage.Key, value interface{}) (bool, hlc.HLTimestamp, error) {
	gr := <-db.Get(&storage.GetRequest{
		RequestHeader: storage.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	if gr.Error != nil {
		return false, hlc.HLTimestamp{}, gr.Error
	}
	if len(gr.Value.Bytes) == 0 {
		return false, hlc.HLTimestamp{}, nil
	}
	if err := gob.NewDecoder(bytes.NewBuffer(gr.Value.Bytes)).Decode(value); err != nil {
		return true, gr.Value.Timestamp, err
	}
	return true, gr.Value.Timestamp, nil
}

// PutI sets the given key to the serialized byte string of the value
// and the provided timestamp.
func PutI(db DB, key storage.Key, value interface{}, timestamp hlc.HLTimestamp) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	pr := <-db.Put(&storage.PutRequest{
		RequestHeader: storage.RequestHeader{
			Key:       key,
			User:      storage.UserRoot,
			Timestamp: timestamp,
		},
		Value: storage.Value{Bytes: buf.Bytes()},
	})
	return pr.Error
}

// BootstrapRangeDescriptor sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeDescriptor(db DB, desc storage.RangeDescriptor, timestamp hlc.HLTimestamp) error {
	// Write meta1.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta1Prefix, storage.KeyMax), desc, timestamp); err != nil {
		return err
	}
	// Write meta2.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta2Prefix, storage.KeyMax), desc, timestamp); err != nil {
		return err
	}
	return nil
}

// BootstrapConfigs sets default configurations for accounting,
// permissions, and zones. All configs are specified for the empty key
// prefix, meaning they apply to the entire database. Permissions are
// granted to all users and the zone requires three replicas with no
// other specifications.
func BootstrapConfigs(db DB, timestamp hlc.HLTimestamp) error {
	// Accounting config.
	acctConfig := &storage.AcctConfig{}
	key := storage.MakeKey(storage.KeyConfigAccountingPrefix, storage.KeyMin)
	if err := PutI(db, key, acctConfig, timestamp); err != nil {
		return err
	}
	// Permission config.
	permConfig := &storage.PermConfig{
		Read:  []string{storage.UserRoot}, // root user
		Write: []string{storage.UserRoot}, // root user
	}
	key = storage.MakeKey(storage.KeyConfigPermissionPrefix, storage.KeyMin)
	if err := PutI(db, key, permConfig, timestamp); err != nil {
		return err
	}
	// Zone config.
	// TODO(spencer): change this when zone specifications change to elect for three
	// replicas with no specific features set.
	zoneConfig := &storage.ZoneConfig{
		Replicas: []storage.Attributes{
			storage.Attributes{},
			storage.Attributes{},
			storage.Attributes{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
	}
	key = storage.MakeKey(storage.KeyConfigZonePrefix, storage.KeyMin)
	if err := PutI(db, key, zoneConfig, timestamp); err != nil {
		return err
	}
	return nil
}

// UpdateRangeDescriptor updates the range locations metadata for the
// range specified by the meta parameter. This always involves a write
// to "meta2", and may require a write to "meta1", in the event that
// meta.EndKey is a "meta2" key (prefixed by KeyMeta2Prefix).
func UpdateRangeDescriptor(db DB, meta storage.RangeMetadata,
	desc storage.RangeDescriptor, timestamp hlc.HLTimestamp) error {
	// TODO(spencer): a lot more work here to actually implement this.

	// Write meta2.
	key := storage.MakeKey(storage.KeyMeta2Prefix, meta.EndKey)
	if err := PutI(db, key, desc, timestamp); err != nil {
		return err
	}
	return nil
}

// A DistDB provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistDB struct {
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges.
	rangeCache *RangeMetadataCache
}

// Default constants for timeouts.
const (
	defaultSendNextTimeout = 1 * time.Second
	defaultRPCTimeout      = 15 * time.Second
	retryBackoff           = 1 * time.Second
	maxRetryBackoff        = 30 * time.Second
)

// A firstRangeMissingError indicates that the first range has not yet
// been gossipped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct {
	error
}

// CanRetry implements the Retryable interface.
func (f firstRangeMissingError) CanRetry() bool { return true }

// A noNodesAvailError specifies that no node addresses in a replica set
// were available via the gossip network.
type noNodeAddrsAvailError struct {
	error
}

// CanRetry implements the Retryable interface.
func (n noNodeAddrsAvailError) CanRetry() bool { return true }

// NewDB returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDB(gossip *gossip.Gossip) *DistDB {
	db := &DistDB{
		gossip: gossip,
	}
	db.rangeCache = NewRangeMetadataCache(db)
	return db
}

// verifyPermissions verifies that the requesting user (header.User)
// has permission to read/write (capabilities depend on method
// name). In the event that multiple permission configs apply to the
// key range implicated by the command, the lowest common denominator
// for permission. For example, if a scan crosses two permission
// configs, both configs must allow read permissions or the entire
// scan will fail.
func (db *DistDB) verifyPermissions(method string, header *storage.RequestHeader) error {
	// Get permissions map from gossip.
	permMap, err := db.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		return err
	}
	if permMap == nil {
		return util.Errorf("perm configs not available; cannot execute %s", method)
	}
	// Visit PermConfig(s) which apply to the method's key range.
	//   - For each, verify each PermConfig allows reads or writes as method requires.
	end := header.EndKey
	if end == nil {
		end = header.Key
	}
	return permMap.(storage.PrefixConfigMap).VisitPrefixes(
		header.Key, end, func(start, end storage.Key, config interface{}) error {
			perm := config.(*storage.PermConfig)
			if storage.NeedReadPerm(method) && !perm.CanRead(header.User) {
				return util.Errorf("user %q cannot read range %q-%q; permissions: %+v",
					header.User, string(start), string(end), perm)
			}
			if storage.NeedWritePerm(method) && !perm.CanWrite(header.User) {
				return util.Errorf("user %q cannot read range %q-%q; permissions: %+v",
					header.User, string(start), string(end), perm)
			}
			return nil
		})
}

// nodeIDToAddr uses the gossip network to translate from node ID
// to a host:port address pair.
func (db *DistDB) nodeIDToAddr(nodeID int32) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDGossipKey(nodeID)
	info, err := db.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %v. Error: %v", nodeID, err)
	}
	return info.(net.Addr), nil
}

// internalRangeLookup dispatches an InternalRangeLookup request for the given
// metadata key to the replicas of the given range.
func (db *DistDB) internalRangeLookup(key storage.Key,
	info *storage.RangeDescriptor) (storage.Key, *storage.RangeDescriptor, error) {
	args := &storage.InternalRangeLookupRequest{
		RequestHeader: storage.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	}
	replyChan := make(chan *storage.InternalRangeLookupResponse, len(info.Replicas))
	if err := db.sendRPC(info.Replicas, "Node.InternalRangeLookup", args, replyChan); err != nil {
		return nil, nil, err
	}
	reply := <-replyChan
	return reply.EndKey, &reply.Range, nil
}

// LookupRangeMetadata retrieves the range metadata for the range containing the
// given key.  Because range metadata in cockroach is stored using a multi-level
// lookup table, this method may be called recursively to look up higher-level
// ranges.  Recursive lookups are dispatched through the metadata range cache.
func (db *DistDB) LookupRangeMetadata(key storage.Key) (storage.Key, *storage.RangeDescriptor, error) {
	metadataKey := storage.RangeMetaKey(key)
	if len(metadataKey) == 0 {
		// Metadata range is the first range, the description of which is always
		// gossiped.
		infoI, err := db.gossip.GetInfo(gossip.KeyFirstRangeMetadata)
		if err != nil {
			return nil, nil, firstRangeMissingError{err}
		}
		info := infoI.(storage.RangeDescriptor)
		return storage.KeyMin, &info, nil
	}

	// Look up the range containing metadataKey in the cache, which will
	// recursively call into db.LookupRangeMetadata if it is not cached.
	metadataRange, err := db.rangeCache.LookupRangeMetadata(metadataKey)
	if err != nil {
		return nil, nil, err
	}
	return db.internalRangeLookup(metadataKey, metadataRange)
}

// sendRPC sends one or more RPCs to replicas from the supplied
// storage.Replica slice. First, replicas which have gossipped
// addresses are corraled and then sent via rpc.Send, with requirement
// that one RPC to a server must succeed.
func (db *DistDB) sendRPC(replicas []storage.Replica, method string, args, replyChanI interface{}) error {
	if len(replicas) == 0 {
		return util.Errorf("%s: replicas set is empty", method)
	}
	// Build a map from replica address (if gossipped) to args struct
	// with replica set in header.
	argsMap := map[net.Addr]interface{}{}
	for _, replica := range replicas {
		addr, err := db.nodeIDToAddr(replica.NodeID)
		if err != nil {
			glog.V(1).Infof("node %d address is not gossipped", replica.NodeID)
			continue
		}
		// Copy the args value and set the replica in the header.
		argsVal := reflect.New(reflect.TypeOf(args).Elem())
		reflect.Indirect(argsVal).Set(reflect.Indirect(reflect.ValueOf(args)))
		reflect.Indirect(argsVal).FieldByName("Replica").Set(reflect.ValueOf(replica))
		argsMap[addr] = argsVal.Interface()
	}
	if len(argsMap) == 0 {
		return noNodeAddrsAvailError{util.Errorf("%s: no replica node addresses available via gossip", method)}
	}
	rpcOpts := rpc.Options{
		N:               1,
		SendNextTimeout: defaultSendNextTimeout,
		Timeout:         defaultRPCTimeout,
	}
	return rpc.Send(argsMap, method, replyChanI, rpcOpts)
}

// routeRPC verifies permissions and looks up the appropriate range
// based on the supplied key and sends the RPC according to the
// specified options. routeRPC sends asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *DistDB) routeRPC(method string, header *storage.RequestHeader, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)

	// Verify permissions.
	if err := db.verifyPermissions(method, header); err != nil {
		replyVal := reflect.ValueOf(reply)
		reflect.Indirect(replyVal).FieldByName("Error").Set(reflect.ValueOf(err))
		chanVal.Send(replyVal)
		return chanVal.Interface()
	}

	// Retry logic for lookup of range by key and RPCs to range replicas.
	go func() {
		retryOpts := util.RetryOptions{
			Tag:         fmt.Sprintf("routing %s rpc", method),
			Backoff:     retryBackoff,
			MaxBackoff:  maxRetryBackoff,
			Constant:    2,
			MaxAttempts: 0, // retry indefinitely
		}
		err := util.RetryWithBackoff(retryOpts, func() (bool, error) {
			rangeMeta, err := db.rangeCache.LookupRangeMetadata(header.Key)
			if err == nil {
				err = db.sendRPC(rangeMeta.Replicas, method, args, chanVal.Interface())
			}
			if err != nil {
				// Range metadata might be out of date - evict it.
				db.rangeCache.EvictCachedRangeMetadata(header.Key)

				// If retryable, allow outer loop to retry.
				if retryErr, ok := err.(util.Retryable); ok && retryErr.CanRetry() {
					glog.Warningf("failed to invoke %s: %v", method, err)
					return false, nil
				}
				// TODO(mtracy): Make sure that errors that clearly result from
				// a stale metadata cache are retryable.
			}
			return true, err
		})
		if err != nil {
			replyVal := reflect.ValueOf(reply)
			reflect.Indirect(replyVal).FieldByName("Error").Set(reflect.ValueOf(err))
			chanVal.Send(replyVal)
		}
	}()

	return chanVal.Interface()
}

// Contains checks for the existence of a key.
func (db *DistDB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	return db.routeRPC("Node.Contains", &args.RequestHeader,
		args, &storage.ContainsResponse{}).(chan *storage.ContainsResponse)
}

// Get .
func (db *DistDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.routeRPC("Node.Get", &args.RequestHeader,
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put .
func (db *DistDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.routeRPC("Node.Put", &args.RequestHeader,
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}

// ConditionalPut .
func (db *DistDB) ConditionalPut(args *storage.ConditionalPutRequest) <-chan *storage.ConditionalPutResponse {
	return db.routeRPC("Node.ConditionalPut", &args.RequestHeader,
		args, &storage.ConditionalPutResponse{}).(chan *storage.ConditionalPutResponse)
}

// Increment .
func (db *DistDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.routeRPC("Node.Increment", &args.RequestHeader,
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}

// Delete .
func (db *DistDB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	return db.routeRPC("Node.Delete", &args.RequestHeader,
		args, &storage.DeleteResponse{}).(chan *storage.DeleteResponse)
}

// DeleteRange .
func (db *DistDB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	// TODO(spencer): range of keys.
	return db.routeRPC("Node.DeleteRange", &args.RequestHeader,
		args, &storage.DeleteRangeResponse{}).(chan *storage.DeleteRangeResponse)
}

// Scan .
func (db *DistDB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	// TODO(spencer): range of keys.
	return db.routeRPC("Node.Scan", &args.RequestHeader,
		args, &storage.ScanResponse{}).(chan *storage.ScanResponse)
}

// EndTransaction .
func (db *DistDB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	// TODO(spencer): multiple keys here...
	return db.routeRPC("Node.EndTransaction", &args.RequestHeader,
		args, &storage.EndTransactionResponse{}).(chan *storage.EndTransactionResponse)
}

// AccumulateTS is used to efficiently accumulate a time series of
// int64 quantities representing discrete subtimes. For example, a
// key/value might represent a minute of data. Each would contain 60
// int64 counts, each representing a second.
func (db *DistDB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	return db.routeRPC("Node.AccumulateTS", &args.RequestHeader,
		args, &storage.AccumulateTSResponse{}).(chan *storage.AccumulateTSResponse)
}

// ReapQueue scans and deletes messages from a recipient message
// queue. ReapQueueRequest invocations must be part of an extant
// transaction or they fail. Returns the reaped queue messsages, up to
// the requested maximum. If fewer than the maximum were returned,
// then the queue is empty.
func (db *DistDB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	return db.routeRPC("Node.ReapQueue", &args.RequestHeader,
		args, &storage.ReapQueueResponse{}).(chan *storage.ReapQueueResponse)
}

// EnqueueUpdate enqueues an update for eventual execution.
func (db *DistDB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	return db.routeRPC("Node.EnqueueUpdate", &args.RequestHeader,
		args, &storage.EnqueueUpdateResponse{}).(chan *storage.EnqueueUpdateResponse)
}

// EnqueueMessage enqueues a message for delivery to an inbox.
func (db *DistDB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	return db.routeRPC("Node.EnqueueMessage", &args.RequestHeader,
		args, &storage.EnqueueMessageResponse{}).(chan *storage.EnqueueMessageResponse)
}
