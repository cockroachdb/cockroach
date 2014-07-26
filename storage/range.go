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
	"encoding/gob"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// init pre-registers RangeDescriptor and PrefixConfigMap types.
func init() {
	gob.Register(RangeDescriptor{})
	gob.Register(StoreDescriptor{})
	gob.Register(PrefixConfigMap{})
	gob.Register(&AcctConfig{})
	gob.Register(&PermConfig{})
	gob.Register(&ZoneConfig{})
}

// ttlClusterIDGossip is time-to-live for cluster ID. The cluster ID
// serves as the sentinel gossip key which informs a node whether or
// not it's connected to the primary gossip network and not just a
// partition. As such it must expire on a reasonable basis and be
// continually re-gossipped. The replica which is the raft leader of
// the first range gossips it.
const ttlClusterIDGossip = 30 * time.Second

// configPrefixes describes administrative configuration maps
// affecting ranges of the key-value map by key prefix.
var configPrefixes = []struct {
	keyPrefix Key         // Range key prefix
	gossipKey string      // Gossip key
	configI   interface{} // Config struct interface
	dirty     bool        // Info in this config has changed; need to re-init and gossip
}{
	{KeyConfigAccountingPrefix, gossip.KeyConfigAccounting, AcctConfig{}, true},
	{KeyConfigPermissionPrefix, gossip.KeyConfigPermission, PermConfig{}, true},
	{KeyConfigZonePrefix, gossip.KeyConfigZone, ZoneConfig{}, true},
}

// The following are the method names supported by the KV API.
const (
	Contains            = "Contains"
	Get                 = "Get"
	Put                 = "Put"
	ConditionalPut      = "ConditionalPut"
	Increment           = "Increment"
	Scan                = "Scan"
	Delete              = "Delete"
	DeleteRange         = "DeleteRange"
	EndTransaction      = "EndTransaction"
	AccumulateTS        = "AccumulateTS"
	ReapQueue           = "ReapQueue"
	EnqueueUpdate       = "EnqueueUpdate"
	EnqueueMessage      = "EnqueueMessage"
	InternalRangeLookup = "InternalRangeLookup"
)

// readMethods specifies the set of methods which read and return data.
var readMethods = map[string]struct{}{
	Contains:            struct{}{},
	Get:                 struct{}{},
	ConditionalPut:      struct{}{},
	Increment:           struct{}{},
	Scan:                struct{}{},
	ReapQueue:           struct{}{},
	InternalRangeLookup: struct{}{},
}

// writeMethods specifies the set of methods which write data.
var writeMethods = map[string]struct{}{
	Put:            struct{}{},
	ConditionalPut: struct{}{},
	Increment:      struct{}{},
	Delete:         struct{}{},
	DeleteRange:    struct{}{},
	EndTransaction: struct{}{},
	AccumulateTS:   struct{}{},
	ReapQueue:      struct{}{},
	EnqueueUpdate:  struct{}{},
	EnqueueMessage: struct{}{},
}

// NeedReadPerm returns true if the specified method requires read permissions.
func NeedReadPerm(method string) bool {
	_, ok := readMethods[method]
	return ok
}

// NeedWritePerm returns true if the specified method requires write permissions.
func NeedWritePerm(method string) bool {
	_, ok := writeMethods[method]
	return ok
}

// A RangeMetadata holds information about the range, including
// range ID and start and end keys, and replicas slice.
type RangeMetadata struct {
	ClusterID string
	RangeID   int64
	StartKey  Key
	EndKey    Key
	Desc      RangeDescriptor
}

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
	Meta      RangeMetadata
	engine    Engine         // The underlying key-value store
	allocator *allocator     // Makes allocation decisions
	gossip    *gossip.Gossip // Range may gossip based on contents
	pending   chan *Cmd      // Pending commands to propose to Raft
	closer    chan struct{}  // Channel for closing the range
	// TODO(andybons): raft instance goes here.
}

// NewRange initializes the range starting at key.
func NewRange(meta RangeMetadata, engine Engine, allocator *allocator, gossip *gossip.Gossip) *Range {
	r := &Range{
		Meta:      meta,
		engine:    engine,
		allocator: allocator,
		gossip:    gossip,
		pending:   make(chan *Cmd, 10 /* TODO(spencer): what's correct value? */),
		closer:    make(chan struct{}),
	}
	return r
}

// Start begins gossiping and starts the pending log entry processing
// loop in a goroutine.
func (r *Range) Start() {
	r.maybeGossipClusterID()
	r.maybeGossipFirstRange()
	r.maybeGossipConfigs()
	go r.processPending()
	go r.startGossip()
}

// Stop ends the log processing loop.
func (r *Range) Stop() {
	close(r.closer)
}

// IsFirstRange returns true if this is the first range.
func (r *Range) IsFirstRange() bool {
	return bytes.Equal(r.Meta.StartKey, KeyMin)
}

// IsLeader returns true if this range replica is the raft leader.
// TODO(spencer): this is always true for now.
func (r *Range) IsLeader() bool {
	return true
}

// ContainsKey returns whether this range contains the specified key.
func (r *Range) ContainsKey(key Key) bool {
	return bytes.Compare(r.Meta.StartKey, key) <= 0 &&
		bytes.Compare(r.Meta.EndKey, key) > 0
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end Key) bool {
	if len(end) == 0 {
		end = start
	}
	if bytes.Compare(start, end) > 0 {
		glog.Errorf("start key is larger than end key %q > %q", string(start), string(end))
		return false
	}
	return bytes.Compare(r.Meta.StartKey, start) <= 0 &&
		bytes.Compare(r.Meta.EndKey, end) >= 0
}

// EnqueueCmd enqueues a command to the pending queue for Raft
// proposals.
func (r *Range) EnqueueCmd(cmd *Cmd) error {
	r.pending <- cmd
	return <-cmd.done
}

// processPending processes pending read/write commands, sending them
// to other replicas in the set as necessary to achieve consensus.
// This method processes indefinitely or until the Range.Stop() is
// invoked.
//
// TODO(spencer): this is pretty temporary. Just executing commands
// immediately until raft is in place.
func (r *Range) processPending() {
	for {
		select {
		case logEntry := <-r.pending:
			logEntry.done <- r.executeCmd(logEntry.Method, logEntry.Args, logEntry.Reply)
		case <-r.closer:
			return
		}
	}
}

// startGossip periodically gossips the cluster ID if it's the
// first range and the raft leader.
func (r *Range) startGossip() {
	ticker := time.NewTicker(ttlClusterIDGossip / 2)
	for {
		select {
		case <-ticker.C:
			r.maybeGossipClusterID()
		case <-r.closer:
			return
		}
	}
}

// maybeGossipClusterID gossips the cluster ID if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipClusterID() {
	if r.gossip != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.gossip.AddInfo(gossip.KeyClusterID, r.Meta.ClusterID, ttlClusterIDGossip); err != nil {
			glog.Errorf("failed to gossip cluster ID %s: %v", r.Meta.ClusterID, err)
		}
	}
}

// maybeGossipFirstRange gossips the range locations if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipFirstRange() {
	if r.gossip != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.gossip.AddInfo(gossip.KeyFirstRangeMetadata, r.Meta.Desc, 0*time.Second); err != nil {
			glog.Errorf("failed to gossip first range metadata: %v", err)
		}
	}
}

// maybeGossipConfigs gossips configuration maps if their data falls
// within the range, this replica is the raft leader, and their
// contents are marked dirty. Configuration maps include accounting,
// permissions, and zones.
func (r *Range) maybeGossipConfigs() {
	if r.gossip != nil && r.IsLeader() {
		for _, cp := range configPrefixes {
			if cp.dirty && r.ContainsKey(cp.keyPrefix) {
				configMap, err := r.loadConfigMap(cp.keyPrefix, cp.configI)
				if err != nil {
					glog.Errorf("failed loading %s config map: %v", cp.gossipKey, err)
					continue
				} else {
					if err := r.gossip.AddInfo(cp.gossipKey, configMap, 0*time.Second); err != nil {
						glog.Errorf("failed to gossip %s configMap: %v", cp.gossipKey, err)
						continue
					}
				}
				cp.dirty = false
			}
		}
	}
}

// loadConfigMap scans the config entries under keyPrefix and
// instantiates/returns a config map. Prefix configuration maps
// include accounting, permissions, and zones.
func (r *Range) loadConfigMap(keyPrefix Key, configI interface{}) (PrefixConfigMap, error) {
	// TODO(spencer): need to make sure range splitting never
	// crosses a configuration map's key prefix.
	kvs, err := r.engine.scan(keyPrefix, PrefixEndKey(keyPrefix), 0)
	if err != nil {
		return nil, err
	}
	var configs []*PrefixConfig
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// gob encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface()
		if err := gob.NewDecoder(bytes.NewBuffer(kv.Value.Bytes)).Decode(config); err != nil {
			return nil, util.Errorf("unable to unmarshal config key %s: %v", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
	}
	return NewPrefixConfigMap(configs)
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
func (r *Range) executeCmd(method string, args, reply interface{}) error {
	switch method {
	case Contains:
		r.Contains(args.(*ContainsRequest), reply.(*ContainsResponse))
	case Get:
		r.Get(args.(*GetRequest), reply.(*GetResponse))
	case Put:
		r.Put(args.(*PutRequest), reply.(*PutResponse))
	case ConditionalPut:
		r.ConditionalPut(args.(*ConditionalPutRequest), reply.(*ConditionalPutResponse))
	case Increment:
		r.Increment(args.(*IncrementRequest), reply.(*IncrementResponse))
	case Delete:
		r.Delete(args.(*DeleteRequest), reply.(*DeleteResponse))
	case DeleteRange:
		r.DeleteRange(args.(*DeleteRangeRequest), reply.(*DeleteRangeResponse))
	case Scan:
		r.Scan(args.(*ScanRequest), reply.(*ScanResponse))
	case EndTransaction:
		r.EndTransaction(args.(*EndTransactionRequest), reply.(*EndTransactionResponse))
	case AccumulateTS:
		r.AccumulateTS(args.(*AccumulateTSRequest), reply.(*AccumulateTSResponse))
	case ReapQueue:
		r.ReapQueue(args.(*ReapQueueRequest), reply.(*ReapQueueResponse))
	case EnqueueUpdate:
		r.EnqueueUpdate(args.(*EnqueueUpdateRequest), reply.(*EnqueueUpdateResponse))
	case EnqueueMessage:
		r.EnqueueMessage(args.(*EnqueueMessageRequest), reply.(*EnqueueMessageResponse))
	case InternalRangeLookup:
		r.InternalRangeLookup(args.(*InternalRangeLookupRequest), reply.(*InternalRangeLookupResponse))
	default:
		return util.Errorf("unrecognized command type: %s", method)
	}
	// Return the error (if any) set in the reply.
	err := reflect.ValueOf(reply).Elem().FieldByName("Error").Interface()
	if err != nil {
		return err.(error)
	}
	return nil
}

// Contains verifies the existence of a key in the key value store.
func (r *Range) Contains(args *ContainsRequest, reply *ContainsResponse) {
	val, err := r.engine.get(args.Key)
	if err != nil {
		reply.Error = err
		return
	}
	if val.Bytes != nil {
		reply.Exists = true
	}
}

// Get returns the value for a specified key.
func (r *Range) Get(args *GetRequest, reply *GetResponse) {
	reply.Value, reply.Error = r.engine.get(args.Key)
}

// Put sets the value for a specified key.
func (r *Range) Put(args *PutRequest, reply *PutResponse) {
	reply.Error = r.internalPut(args.Key, args.Value)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Range) ConditionalPut(args *ConditionalPutRequest, reply *ConditionalPutResponse) {
	// Handle check for non-existence of key.
	val, err := r.engine.get(args.Key)
	if err != nil {
		reply.Error = err
		return
	}
	if args.ExpValue.Bytes == nil && val.Bytes != nil {
		reply.Error = util.Errorf("key %q already exists", args.Key)
		return
	} else if args.ExpValue.Bytes != nil {
		// Handle check for existence when there is no key.
		if val.Bytes == nil {
			reply.Error = util.Errorf("key %q does not exist", args.Key)
			return
		} else if !bytes.Equal(args.ExpValue.Bytes, val.Bytes) {
			reply.ActualValue = &Value{Bytes: val.Bytes, Timestamp: val.Timestamp}
			reply.Error = util.Errorf("key %q does not match existing", args.Key)
			return
		}
	}

	reply.Error = r.internalPut(args.Key, args.Value)
}

// internalPut is the guts of the put method, called from both Put()
// and ConditionalPut().
func (r *Range) internalPut(key Key, value Value) error {
	// Put the value.
	// TODO(Tobias): Turn this into a writebatch with account stats in a reusable way.
	// This requires use of RocksDB's merge operator to implement increasable counters
	if err := r.engine.put(key, value); err != nil {
		return err
	}
	// Check whether this put has modified a configuration map.
	for _, cp := range configPrefixes {
		if bytes.HasPrefix(key, cp.keyPrefix) {
			cp.dirty = true
			r.maybeGossipConfigs()
			break
		}
	}
	return nil
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Range) Increment(args *IncrementRequest, reply *IncrementResponse) {
	reply.NewValue, reply.Error = increment(r.engine, args.Key, args.Increment, args.Timestamp)
}

// Delete deletes the key and value specified by key.
func (r *Range) Delete(args *DeleteRequest, reply *DeleteResponse) {
	if err := r.engine.del(args.Key); err != nil {
		reply.Error = err
	}
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Range) DeleteRange(args *DeleteRangeRequest, reply *DeleteRangeResponse) {
	reply.Error = util.Error("unimplemented")
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Range) Scan(args *ScanRequest, reply *ScanResponse) {
	reply.Rows, reply.Error = r.engine.scan(args.Key, args.EndKey, args.MaxResults)
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Range) EndTransaction(args *EndTransactionRequest, reply *EndTransactionResponse) {
	reply.Error = util.Error("unimplemented")
}

// AccumulateTS is used internally to aggregate statistics over key
// ranges throughout the distributed cluster.
func (r *Range) AccumulateTS(args *AccumulateTSRequest, reply *AccumulateTSResponse) {
	reply.Error = util.Error("unimplemented")
}

// ReapQueue destructively queries messages from a delivery inbox
// queue. This method must be called from within a transaction.
func (r *Range) ReapQueue(args *ReapQueueRequest, reply *ReapQueueResponse) {
	reply.Error = util.Error("unimplemented")
}

// EnqueueUpdate sidelines an update for asynchronous execution.
// AccumulateTS updates are sent this way. Eventually-consistent indexes
// are also built using update queues. Crucially, the enqueue happens
// as part of the caller's transaction, so is guaranteed to be
// executed if the transaction succeeded.
func (r *Range) EnqueueUpdate(args *EnqueueUpdateRequest, reply *EnqueueUpdateResponse) {
	reply.Error = util.Error("unimplemented")
}

// EnqueueMessage enqueues a message (Value) for delivery to a
// recipient inbox.
func (r *Range) EnqueueMessage(args *EnqueueMessageRequest, reply *EnqueueMessageResponse) {
	reply.Error = util.Error("unimplemented")
}

// InternalRangeLookup looks up the metadata info for the given args.Key.
// args.Key should be a metadata key, which are of the form "\0\0meta[12]<encoded_key>".
func (r *Range) InternalRangeLookup(args *InternalRangeLookupRequest, reply *InternalRangeLookupResponse) {
	if !bytes.HasPrefix(args.Key, KeyMetaPrefix) {
		reply.Error = util.Errorf("invalid metadata key: %q", args.Key)
		return
	}

	// Validate that key is not outside the range. A range ends just
	// before its Meta.EndKey.
	if !args.Key.Less(r.Meta.EndKey) {
		reply.Error = util.Errorf("key outside the range %v with end key %q", r.Meta.RangeID, r.Meta.EndKey)
		return
	}

	// We want to search for the metadata key just greater than args.Key.
	nextKey := NextKey(args.Key)
	kvs, err := r.engine.scan(nextKey, KeyMax, 1)
	if err != nil {
		reply.Error = err
		return
	}
	// We should have gotten the key with the same metadata level prefix as we queried.
	metaPrefix := args.Key[0:len(KeyMeta1Prefix)]
	if len(kvs) != 1 || !bytes.HasPrefix(kvs[0].Key, metaPrefix) {
		reply.Error = util.Errorf("key not found in range %v", r.Meta.RangeID)
		return
	}

	if err = gob.NewDecoder(bytes.NewBuffer(kvs[0].Value.Bytes)).Decode(&reply.Range); err != nil {
		reply.Error = err
		return
	}
	if args.Key.Less(reply.Range.StartKey) {
		// args.Key doesn't belong to this range. We are perhaps searching the wrong node?
		reply.Error = util.Errorf("no range found for key %q in range: %+v", args.Key, r.Meta)
		return
	}
	reply.EndKey = kvs[0].Key
}
