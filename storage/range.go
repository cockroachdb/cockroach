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

package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/golang/glog"
)

// init pre-registers RangeDescriptor, PrefixConfigMap types and Transaction.
func init() {
	gob.Register(StoreDescriptor{})
	gob.Register(PrefixConfigMap{})
	gob.Register(&proto.AcctConfig{})
	gob.Register(&proto.PermConfig{})
	gob.Register(&proto.ZoneConfig{})
	gob.Register(proto.RangeDescriptor{})
	gob.Register(proto.Transaction{})
}

const (
	// DefaultHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 2x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultHeartbeatInterval = 5 * time.Second

	// ttlClusterIDGossip is time-to-live for cluster ID. The cluster ID
	// serves as the sentinel gossip key which informs a node whether or
	// not it's connected to the primary gossip network and not just a
	// partition. As such it must expire on a reasonable basis and be
	// continually re-gossipped. The replica which is the raft leader of
	// the first range gossips it.
	ttlClusterIDGossip = 30 * time.Second
)

// configPrefixes describes administrative configuration maps
// affecting ranges of the key-value map by key prefix.
var configPrefixes = []struct {
	keyPrefix engine.Key  // Range key prefix
	gossipKey string      // Gossip key
	configI   interface{} // Config struct interface
	dirty     bool        // Info in this config has changed; need to re-init and gossip
}{
	{engine.KeyConfigAccountingPrefix, gossip.KeyConfigAccounting, proto.AcctConfig{}, true},
	{engine.KeyConfigPermissionPrefix, gossip.KeyConfigPermission, proto.PermConfig{}, true},
	{engine.KeyConfigZonePrefix, gossip.KeyConfigZone, proto.ZoneConfig{}, true},
}

// The following are the method names supported by the KV API.
const (
	Contains              = "Contains"
	Get                   = "Get"
	Put                   = "Put"
	ConditionalPut        = "ConditionalPut"
	Increment             = "Increment"
	Scan                  = "Scan"
	Delete                = "Delete"
	DeleteRange           = "DeleteRange"
	BeginTransaction      = "BeginTransaction"
	EndTransaction        = "EndTransaction"
	AccumulateTS          = "AccumulateTS"
	ReapQueue             = "ReapQueue"
	EnqueueUpdate         = "EnqueueUpdate"
	EnqueueMessage        = "EnqueueMessage"
	InternalRangeLookup   = "InternalRangeLookup"
	InternalHeartbeatTxn  = "InternalHeartbeatTxn"
	InternalPushTxn       = "InternalPushTxn"
	InternalResolveIntent = "InternalResolveIntent"
	InternalSnapshotCopy  = "InternalSnapshotCopy"
)

// readMethods specifies the set of methods which read and return data.
var readMethods = map[string]struct{}{
	Contains:             struct{}{},
	Get:                  struct{}{},
	ConditionalPut:       struct{}{},
	Increment:            struct{}{},
	Scan:                 struct{}{},
	ReapQueue:            struct{}{},
	InternalRangeLookup:  struct{}{},
	InternalSnapshotCopy: struct{}{},
}

// writeMethods specifies the set of methods which write data.
var writeMethods = map[string]struct{}{
	Put:                   struct{}{},
	ConditionalPut:        struct{}{},
	Increment:             struct{}{},
	Delete:                struct{}{},
	DeleteRange:           struct{}{},
	EndTransaction:        struct{}{},
	AccumulateTS:          struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
}

// ReadMethods lists the read-only methods supported by a range.
var ReadMethods = util.MapKeys(readMethods).([]string)

// WriteMethods lists the methods supported by a range which write data.
var WriteMethods = util.MapKeys(writeMethods).([]string)

// Methods lists all the methods supported by a range.
var Methods = append(ReadMethods, WriteMethods...)

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

// IsReadOnly returns true if the specified method only requires read permissions.
func IsReadOnly(method string) bool {
	return !NeedWritePerm(method)
}

// A Cmd holds method, args, reply and a done channel for a command
// sent to Raft. Once committed to the Raft log, the command is
// executed and the result returned via the done channel.
type Cmd struct {
	Method string
	Args   proto.Request
	Reply  proto.Response
	done   chan error // Used to signal waiting RPC handler
}

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
	Meta      *proto.RangeMetadata
	clock     *hlc.Clock
	mvcc      *engine.MVCC
	engine    engine.Engine  // The underlying key-value store
	allocator *allocator     // Makes allocation decisions
	gossip    *gossip.Gossip // Range may gossip based on contents
	rm        RangeManager   // Makes some store methods available
	raft      chan *Cmd      // Raft commands
	closer    chan struct{}  // Channel for closing the range

	sync.RWMutex                 // Protects readQ, tsCache & respCache.
	readQ        *ReadQueue      // Reads queued behind pending writes
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	respCache    *ResponseCache  // Provides idempotence for retries
}

// NewRange initializes the range using the given metadata. The range will have
// no knowledge of a possible store that contains it and thus all rebalancing
// operations will fail. Use NewRangeFromStore() instead to create ranges
// contained within a store.
//
// TODO(spencer): need to give range just a single instance of Range manager
// to contain clock, mvcc, engine, allocator & gossip. This will reduce
// completely unnecessary memory usage per range.
func NewRange(meta *proto.RangeMetadata, clock *hlc.Clock, eng engine.Engine,
	allocator *allocator, gossip *gossip.Gossip, rm RangeManager) *Range {
	r := &Range{
		Meta:      meta,
		clock:     clock,
		mvcc:      engine.NewMVCC(eng),
		engine:    eng,
		allocator: allocator,
		gossip:    gossip,
		raft:      make(chan *Cmd, 10), // TODO(spencer): remove
		rm:        rm,
		closer:    make(chan struct{}),
		readQ:     NewReadQueue(),
		tsCache:   NewTimestampCache(clock),
		respCache: NewResponseCache(meta.RangeID, eng),
	}
	return r
}

// Start begins gossiping and starts the raft command processing
// loop in a goroutine.
func (r *Range) Start() {
	r.maybeGossipClusterID()
	r.maybeGossipFirstRange()
	r.maybeGossipConfigs()
	go r.processRaft() // TODO(spencer): remove
	// Only start gossiping if this range is the first range.
	if r.IsFirstRange() {
		go r.startGossip()
	}
}

// Stop ends the log processing loop.
func (r *Range) Stop() {
	close(r.closer)
}

// IsFirstRange returns true if this is the first range.
func (r *Range) IsFirstRange() bool {
	return bytes.Equal(r.Meta.StartKey, engine.KeyMin)
}

// IsLeader returns true if this range replica is the raft leader.
// TODO(spencer): this is always true for now.
func (r *Range) IsLeader() bool {
	return true
}

// ContainsKey returns whether this range contains the specified key.
func (r *Range) ContainsKey(key engine.Key) bool {
	return r.Meta.ContainsKey(key)
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end engine.Key) bool {
	return r.Meta.ContainsKeyRange(start, end)
}

// EnqueueCmd enqueues a command to Raft.
func (r *Range) EnqueueCmd(cmd *Cmd) error {
	r.raft <- cmd
	return <-cmd.done
}

// ReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Range) ReadOnlyCmd(method string, args proto.Request, reply proto.Response) error {
	header := args.Header()
	r.Lock()
	r.tsCache.Add(header.Key, header.EndKey, header.Timestamp)
	var wg sync.WaitGroup
	r.readQ.AddRead(header.Key, header.EndKey, &wg)
	r.Unlock()
	wg.Wait()

	// It's possible that arbitrary delays (e.g. major GC, VM
	// de-prioritization, etc.) could cause the execution of this read
	// command to occur AFTER the range replica has lost leadership.
	//
	// There is a chance that we waited on writes, and although they
	// were committed to the log, they weren't successfully applied to
	// this replica's state machine. We re-verify leadership before
	// reading to make sure that all pending writes are persisted.
	//
	// There are some elaborate cases where we might have lost
	// leadership and then regained it during the delay, but this is ok
	// because any writes during that period necessarily had higher
	// timestamps. This is because the read-timestamp-cache prevents it
	// for the active leader and leadership changes force the
	// read-timestamp-cache to reset its high water mark.
	if !r.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		return &proto.NotLeaderError{}
	}
	return r.executeCmd(method, args, reply)
}

// ReadWriteCmd first consults the response cache to determine whether
// this command has already been sent to the range. If a response is
// found, it's returned immediately and not submitted to raft. Next,
// the timestamp cache is checked to determine if any newer accesses to
// this command's affected keys have been made. If so, this command's
// timestamp is moved forward. Finally the keys affected by this
// command are added as pending writes to the read queue and the
// command is submitted to Raft. Upon completion, the write is removed
// from the read queue and the reply is added to the repsonse cache.
func (r *Range) ReadWriteCmd(method string, args proto.Request, reply proto.Response) error {
	// Check the response cache in case this is a replay. This call
	// may block if the same command is already underway.
	header := args.Header()
	if ok, err := r.respCache.GetResponse(header.CmdID, reply); ok || err != nil {
		if ok { // this is a replay! extract error for return
			return reply.Header().GoError()
		}
		// In this case there was an error reading from the response
		// cache. Instead of failing the request just because we can't
		// decode the reply in the response cache, we proceed as though
		// idempotence has expired.
		log.Errorf("unable to read result for %+v from the response cache: %v", args, err)
	}

	// One of the prime invariants of Cockroach is that a mutating command
	// cannot write a key with an earlier timestamp than the most recent
	// read of the same key. So first order of business here is to check
	// the timestamp cache for reads/writes which are more recent than the
	// timestamp of this write. If more recent, we simply update the
	// write's timestamp before enqueuing it for execution. When the write
	// returns, the updated timestamp will inform the final commit
	// timestamp.
	r.Lock() // Protect access to timestamp cache and read queue.
	if ts := r.tsCache.GetMax(header.Key, header.EndKey); header.Timestamp.Less(ts) {
		if glog.V(1) {
			glog.Infof("Overriding existing timestamp %s with %s", header.Timestamp, ts)
		}
		ts.Logical++ // increment logical component by one to differentiate.
		// Update the request timestamp.
		header.Timestamp = ts
	}
	// Just as for reads, we update the timestamp cache with the
	// timestamp of this write. This ensures a strictly higher timestamp
	// for successive writes to the same key or key range.
	r.tsCache.Add(header.Key, header.EndKey, header.Timestamp)

	// The next step is to add the write to the read queue to inform
	// subsequent reads that there is a pending write. Reads which
	// overlap pending writes must wait for those writes to complete.
	wKey := r.readQ.AddWrite(header.Key, header.EndKey)
	r.Unlock()

	// Create command and enqueue for Raft.
	cmd := &Cmd{
		Method: method,
		Args:   args,
		Reply:  reply,
		done:   make(chan error, 1),
	}
	// This waits for the command to complete.
	err := r.EnqueueCmd(cmd)

	// Now that the command has completed, remove the pending write.
	r.Lock()
	r.readQ.RemoveWrite(wKey)
	r.Unlock()

	return err
}

// processRaft processes read/write commands, sending them to the Raft
// consensus algorithm. This method processes indefinitely or until
// Range.Stop() is invoked.
//
// TODO(spencer): this is pretty temporary. Just executing commands
//   immediately until Raft is in place.
//
// TODO(bdarnell): when Raft elects this range replica as the leader,
//   we need to be careful to do the following before the range is
//   allowed to believe it's the leader and begin to accept writes and
//   reads:
//     - Push noop command to raft followers in order to verify the
//       committed entries in the log.
//     - Apply all committed log entries to the state machine.
//     - Signal the range to clear its read timestamp, response caches
//       and pending read queue.
//     - Signal the range that it's now the leader with the duration
//       of its leader lease.
//   If we don't do this, then a read which was previously gated on
//   the former leader waiting for overlapping writes to commit to
//   the underlying state machine, might transit to the new leader
//   and be able to access the new leader's state machine BEFORE
//   the overlapping writes are applied.
func (r *Range) processRaft() {
	for {
		select {
		case cmd := <-r.raft:
			cmd.done <- r.executeCmd(cmd.Method, cmd.Args, cmd.Reply)
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
			r.maybeGossipFirstRange()
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
			log.Errorf("failed to gossip cluster ID %s: %v", r.Meta.ClusterID, err)
		}
	}
}

// maybeGossipFirstRange gossips the range locations if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipFirstRange() {
	if r.gossip != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.gossip.AddInfo(gossip.KeyFirstRangeMetadata, r.Meta.RangeDescriptor, 0*time.Second); err != nil {
			log.Errorf("failed to gossip first range metadata: %v", err)
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
					log.Errorf("failed loading %s config map: %v", cp.gossipKey, err)
					continue
				} else {
					if err := r.gossip.AddInfo(cp.gossipKey, configMap, 0*time.Second); err != nil {
						log.Errorf("failed to gossip %s configMap: %v", cp.gossipKey, err)
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
func (r *Range) loadConfigMap(keyPrefix engine.Key, configI interface{}) (PrefixConfigMap, error) {
	// TODO(spencer): need to make sure range splitting never
	// crosses a configuration map's key prefix.
	kvs, err := r.mvcc.Scan(keyPrefix, engine.PrefixEndKey(keyPrefix), 0, proto.MaxTimestamp, nil)
	if err != nil {
		return nil, err
	}
	var configs []*PrefixConfig
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// gob encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, config); err != nil {
			return nil, util.Errorf("unable to unmarshal config key %s: %v", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
	}
	return NewPrefixConfigMap(configs)
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
func (r *Range) executeCmd(method string, args proto.Request, reply proto.Response) error {
	switch method {
	case Contains:
		r.Contains(args.(*proto.ContainsRequest), reply.(*proto.ContainsResponse))
	case Get:
		r.Get(args.(*proto.GetRequest), reply.(*proto.GetResponse))
	case Put:
		r.Put(args.(*proto.PutRequest), reply.(*proto.PutResponse))
	case ConditionalPut:
		r.ConditionalPut(args.(*proto.ConditionalPutRequest), reply.(*proto.ConditionalPutResponse))
	case Increment:
		r.Increment(args.(*proto.IncrementRequest), reply.(*proto.IncrementResponse))
	case Delete:
		r.Delete(args.(*proto.DeleteRequest), reply.(*proto.DeleteResponse))
	case DeleteRange:
		r.DeleteRange(args.(*proto.DeleteRangeRequest), reply.(*proto.DeleteRangeResponse))
	case Scan:
		r.Scan(args.(*proto.ScanRequest), reply.(*proto.ScanResponse))
	case EndTransaction:
		r.EndTransaction(args.(*proto.EndTransactionRequest), reply.(*proto.EndTransactionResponse))
	case AccumulateTS:
		r.AccumulateTS(args.(*proto.AccumulateTSRequest), reply.(*proto.AccumulateTSResponse))
	case ReapQueue:
		r.ReapQueue(args.(*proto.ReapQueueRequest), reply.(*proto.ReapQueueResponse))
	case EnqueueUpdate:
		r.EnqueueUpdate(args.(*proto.EnqueueUpdateRequest), reply.(*proto.EnqueueUpdateResponse))
	case EnqueueMessage:
		r.EnqueueMessage(args.(*proto.EnqueueMessageRequest), reply.(*proto.EnqueueMessageResponse))
	case InternalRangeLookup:
		r.InternalRangeLookup(args.(*proto.InternalRangeLookupRequest), reply.(*proto.InternalRangeLookupResponse))
	case InternalHeartbeatTxn:
		r.InternalHeartbeatTxn(args.(*proto.InternalHeartbeatTxnRequest), reply.(*proto.InternalHeartbeatTxnResponse))
	case InternalPushTxn:
		r.InternalPushTxn(args.(*proto.InternalPushTxnRequest), reply.(*proto.InternalPushTxnResponse))
	case InternalResolveIntent:
		r.InternalResolveIntent(args.(*proto.InternalResolveIntentRequest), reply.(*proto.InternalResolveIntentResponse))
	case InternalSnapshotCopy:
		r.InternalSnapshotCopy(args.(*proto.InternalSnapshotCopyRequest), reply.(*proto.InternalSnapshotCopyResponse))
	default:
		return util.Errorf("unrecognized command type: %s", method)
	}

	// Propagate the request timestamp (which may have changed).
	reply.Header().Timestamp = args.Header().Timestamp

	// Add this command's result to the response cache if this is a
	// read/write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence when leadership changes.
	if !IsReadOnly(method) {
		if putErr := r.respCache.PutResponse(args.Header().CmdID, reply); putErr != nil {
			log.Errorf("unable to write result of %+v: %+v to the response cache: %v",
				args, reply, putErr)
		}
	}

	// Return the error (if any) set in the reply.
	return reply.Header().GoError()
}

// Contains verifies the existence of a key in the key value store.
func (r *Range) Contains(args *proto.ContainsRequest, reply *proto.ContainsResponse) {
	val, err := r.mvcc.Get(args.Key, args.Timestamp, args.Txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if val != nil {
		reply.Exists = true
	}
}

// Get returns the value for a specified key.
func (r *Range) Get(args *proto.GetRequest, reply *proto.GetResponse) {
	val, err := r.mvcc.Get(args.Key, args.Timestamp, args.Txn)
	reply.Value = val
	reply.SetGoError(err)
}

// Put sets the value for a specified key.
func (r *Range) Put(args *proto.PutRequest, reply *proto.PutResponse) {
	err := r.mvcc.Put(args.Key, args.Timestamp, args.Value, args.Txn)
	if err == nil {
		r.updateGossipConfigs(args.Key)
	}
	reply.SetGoError(err)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Range) ConditionalPut(args *proto.ConditionalPutRequest, reply *proto.ConditionalPutResponse) {
	val, err := r.mvcc.ConditionalPut(args.Key, args.Timestamp, args.Value, args.ExpValue, args.Txn)
	if err == nil {
		r.updateGossipConfigs(args.Key)
	}
	reply.ActualValue = val
	reply.SetGoError(err)
}

// updateGossipConfigs is used to update gossip configs.
func (r *Range) updateGossipConfigs(key engine.Key) {
	// Check whether this put has modified a configuration map.
	for _, cp := range configPrefixes {
		if bytes.HasPrefix(key, cp.keyPrefix) {
			cp.dirty = true
			r.maybeGossipConfigs()
			break
		}
	}
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Range) Increment(args *proto.IncrementRequest, reply *proto.IncrementResponse) {
	val, err := r.mvcc.Increment(args.Key, args.Timestamp, args.Txn, args.Increment)
	reply.NewValue = val
	reply.SetGoError(err)
}

// Delete deletes the key and value specified by key.
func (r *Range) Delete(args *proto.DeleteRequest, reply *proto.DeleteResponse) {
	reply.SetGoError(r.mvcc.Delete(args.Key, args.Timestamp, args.Txn))
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Range) DeleteRange(args *proto.DeleteRangeRequest, reply *proto.DeleteRangeResponse) {
	num, err := r.mvcc.DeleteRange(args.Key, args.EndKey, args.MaxEntriesToDelete, args.Timestamp, args.Txn)
	reply.NumDeleted = num
	reply.SetGoError(err)
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Range) Scan(args *proto.ScanRequest, reply *proto.ScanResponse) {
	kvs, err := r.mvcc.Scan(args.Key, args.EndKey, args.MaxResults, args.Timestamp, args.Txn)
	reply.Rows = kvs
	reply.SetGoError(err)
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Range) EndTransaction(args *proto.EndTransactionRequest, reply *proto.EndTransactionResponse) {
	// Create the actual key to the system-local transaction table.
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.Key)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.GetProto(r.engine, key, existTxn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	// If the transaction record already exists, verify that we can either
	// commit it or abort it (according to args.Commit), and also that the
	// Timestamp and Epoch have not suffered regression.
	if ok {
		// Use the persisted transaction record as final transaction.
		reply.Txn = gogoproto.Clone(existTxn).(*proto.Transaction)

		if existTxn.Status == proto.COMMITTED {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, "already committed"))
			return
		} else if existTxn.Status == proto.ABORTED {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, "already aborted"))
			return
		} else if args.Txn.Epoch < existTxn.Epoch {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("epoch regression: %d", args.Txn.Epoch)))
			return
		} else if existTxn.Timestamp.Less(args.Txn.Timestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the transaction timestamp.
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("timestamp regression: %+v", args.Txn.Timestamp)))
			return
		}
	} else {
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.Txn = gogoproto.Clone(args.Txn).(*proto.Transaction)
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
		if args.Txn.Isolation == proto.SERIALIZABLE && !reply.Txn.Timestamp.Equal(args.Txn.Timestamp) {
			reply.SetGoError(proto.NewTransactionRetryError(reply.Txn))
			return
		}
		reply.Txn.Status = proto.COMMITTED
	} else {
		reply.Txn.Status = proto.ABORTED
	}

	// Persist the transaction record with updated status (& possibly timestmap).
	if err := engine.PutProto(r.engine, key, reply.Txn); err != nil {
		reply.SetGoError(err)
		return
	}
}

// AccumulateTS is used internally to aggregate statistics over key
// ranges throughout the distributed cluster.
func (r *Range) AccumulateTS(args *proto.AccumulateTSRequest, reply *proto.AccumulateTSResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// ReapQueue destructively queries messages from a delivery inbox
// queue. This method must be called from within a transaction.
func (r *Range) ReapQueue(args *proto.ReapQueueRequest, reply *proto.ReapQueueResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// EnqueueUpdate sidelines an update for asynchronous execution.
// AccumulateTS updates are sent this way. Eventually-consistent indexes
// are also built using update queues. Crucially, the enqueue happens
// as part of the caller's transaction, so is guaranteed to be
// executed if the transaction succeeded.
func (r *Range) EnqueueUpdate(args *proto.EnqueueUpdateRequest, reply *proto.EnqueueUpdateResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// EnqueueMessage enqueues a message (Value) for delivery to a
// recipient inbox.
func (r *Range) EnqueueMessage(args *proto.EnqueueMessageRequest, reply *proto.EnqueueMessageResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// InternalRangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix appended. The Range Metadata
// Key for an ordinary key can be generated with the `engine.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and dispatching it to
// InternalRangeLookup.
//
// Note that the Range Metadata Key sent to InternalRangeLookup is NOT the key
// at which the desired RangeDescriptor is stored. Instead, this method returns
// the RangeDescriptor stored at the _lowest_ existing key which is _greater_
// than the given key. The returned RangeDescriptor will thus contain the
// ordinary key which was originally used to generate the Range Metadata Key
// sent to InternalRangeLookup.
//
// This method has an important optimization: instead of just returning the
// request RangeDescriptor, it also returns a slice of additional range
// descriptors immediately consecutive to the desired RangeDescriptor. This is
// intended to serve as a sort of caching pre-fetch, so that the requesting
// nodes can aggressively cache RangeDescriptors which are likely to be desired
// by their current workload.
func (r *Range) InternalRangeLookup(args *proto.InternalRangeLookupRequest, reply *proto.InternalRangeLookupResponse) {
	if err := engine.ValidateRangeMetaKey(args.Key); err != nil {
		reply.SetGoError(err)
		return
	}

	rangeCount := int64(args.MaxRanges)
	if rangeCount < 1 {
		reply.SetGoError(util.Errorf(
			"Range lookup specified invalid maximum range count %d: must be > 0", rangeCount))
		return
	}

	// We want to search for the metadata key just greater than args.Key.  Scan
	// for both the requested key and the keys immediately afterwards, up to
	// MaxRanges.
	metaPrefix := args.Key[:len(engine.KeyMeta1Prefix)]
	nextKey := engine.NextKey(args.Key)
	kvs, err := r.mvcc.Scan(nextKey, engine.PrefixEndKey(metaPrefix), rangeCount, args.Timestamp, args.Txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}

	// The initial key must have the same metadata level prefix as we queried.
	if len(kvs) == 0 {
		// At this point the range has been verified to contain the requested
		// key, but no matching results were returned from the scan. This could
		// indicate a very bad system error, but for now we will just treat it
		// as a retryable Key Mismatch error.
		err := proto.NewRangeKeyMismatchError(args.Key, args.Key, r.Meta)
		reply.SetGoError(err)
		log.Errorf("InternalRangeLookup dispatched to correct range, but no matching RangeDescriptor was found. %s", err)
		return
	}

	// Decode all scanned range descriptors, stopping if a range is encountered
	// which does not have the same metadata prefix as the queried key.
	rds := make([]proto.RangeDescriptor, len(kvs))
	for i := range kvs {
		if err = gogoproto.Unmarshal(kvs[i].Value.Bytes, &rds[i]); err != nil {
			reply.SetGoError(err)
			return
		}
	}

	reply.Ranges = rds
	return
}

// InternalHeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func (r *Range) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest, reply *proto.InternalHeartbeatTxnResponse) {
	// Create the actual key to the system-local transaction table.
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.Key)
	var txn proto.Transaction
	ok, err := engine.GetProto(r.engine, key, &txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	// If no existing transaction record was found, initialize
	// to the transaction in the request header.
	if !ok {
		gogoproto.Merge(&txn, args.Txn)
	}
	if txn.Status == proto.PENDING {
		if txn.LastHeartbeat == nil {
			txn.LastHeartbeat = &proto.Timestamp{}
		}
		if txn.LastHeartbeat.Less(args.Header().Timestamp) {
			*txn.LastHeartbeat = args.Header().Timestamp
		}
		if err := engine.PutProto(r.engine, key, &txn); err != nil {
			reply.SetGoError(err)
			return
		}
	}
	reply.Txn = &txn
}

// InternalPushTxn resolves conflicts between concurrent txns (or
// between a non-transactional reader or writer and a txn) in several
// ways depending on the statuses and priorities of the conflicting
// transactions. The InternalPushTxn operation is invoked by a
// "pusher" (the writer trying to abort a conflicting txn or the
// reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PushTxn -- the pushee txn whose intent(s) caused the
// conflict).
//
// Txn already committed/aborted: If pushee txn is committed, return
// already-committed error. If pushee txn has been aborted, return
// success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use PushTxn.Timestamp as LastHeartbeat. If
// current time - LastHeartbeat > 2 * DefaultHeartbeatInterval, then
// the puhsee txn should be either pushed forward or aborted,
// depending on value of Request.Abort.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of args.Abort. If
// args.Abort is true, set txn.Status to ABORTED, and priority to one
// less than the pusher's priority and return success. If args.Abort
// is false, set txn.Timestamp to pusher's txn.Timestamp + 1.
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionRetryError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Range) InternalPushTxn(args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) {
	if !bytes.Equal(args.Key, args.PushTxn.ID) {
		reply.SetGoError(util.Errorf("push txn not addressed at pushee's txn ID: %q vs. %q", args.Key, args.PushTxn.ID))
		return
	}
	// Create the actual key to the system-local transaction table.
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.Key)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.GetProto(r.engine, key, existTxn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if ok {
		// Start with the persisted transaction record as final transaction.
		reply.PushTxn = gogoproto.Clone(existTxn).(*proto.Transaction)
		// Upgrade the epoch and timestamp as necessary.
		if reply.PushTxn.Epoch < args.PushTxn.Epoch {
			reply.PushTxn.Epoch = args.PushTxn.Epoch
		}
		if reply.PushTxn.Timestamp.Less(args.PushTxn.Timestamp) {
			reply.PushTxn.Timestamp = args.PushTxn.Timestamp
		}
	} else {
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.PushTxn = gogoproto.Clone(&args.PushTxn).(*proto.Transaction)
	}

	// If already committed, return already committed error.
	if reply.PushTxn.Status == proto.COMMITTED {
		reply.SetGoError(proto.NewTransactionStatusError(reply.PushTxn, "already committed"))
		return
	} else if reply.PushTxn.Status == proto.ABORTED {
		// This is a trivial noop.
		return
	}

	// push bool is true in the event the pusher prevails.
	var push bool

	// Check for txn timeout.
	if reply.PushTxn.LastHeartbeat == nil {
		reply.PushTxn.LastHeartbeat = &reply.PushTxn.Timestamp
	}
	// Compute heartbeat expiration.
	expiry := r.clock.Now()
	expiry.WallTime -= 2 * DefaultHeartbeatInterval.Nanoseconds()
	if reply.PushTxn.LastHeartbeat.Less(expiry) {
		log.V(1).Infof("pushing expired txn %+v", reply.PushTxn)
		push = true
	} else if args.PushTxn.Epoch < reply.PushTxn.Epoch {
		// Check for an intent from a prior epoch.
		log.V(1).Infof("pushing intent from previous epoch for txn %+v", reply.PushTxn)
		push = true
	} else if reply.PushTxn.Priority < args.Txn.Priority ||
		(reply.PushTxn.Priority == args.Txn.Priority && args.Txn.Timestamp.Less(reply.PushTxn.Timestamp)) {
		// Finally, choose based on priority; if priorities are equal, order by lower txn timestamp.
		log.V(1).Infof("pushing intent from txn with lower priority %+v vs %+v", reply.PushTxn, args.Txn)
		push = true
	}

	if !push {
		log.V(1).Infof("failed to push intent %+v vs %+v", reply.PushTxn, args.Txn)
		reply.SetGoError(proto.NewTransactionRetryError(reply.PushTxn))
		return
	}

	// If aborting transaction, set new status and return success.
	if args.Abort {
		reply.PushTxn.Status = proto.ABORTED
	} else {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PushTxn.Timestamp = args.Timestamp
		reply.PushTxn.Timestamp.Logical++
	}
	// Persist the pushed transaction.
	if err := engine.PutProto(r.engine, key, reply.PushTxn); err != nil {
		reply.SetGoError(err)
		return
	}
}

// InternalResolveIntent updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator.  The range will return the current status for this
// transaction to the coordinator.
func (r *Range) InternalResolveIntent(args *proto.InternalResolveIntentRequest, reply *proto.InternalResolveIntentResponse) {
	if len(args.EndKey) == 0 || bytes.Equal(args.Key, args.EndKey) {
		reply.SetGoError(r.mvcc.ResolveWriteIntent(args.Key, args.Txn))
	} else {
		_, err := r.mvcc.ResolveWriteIntentRange(args.Key, args.EndKey, 0, args.Txn)
		reply.SetGoError(err)
	}
}

// createSnapshot creates a new snapshot, named using an internal counter.
func (r *Range) createSnapshot() (string, error) {
	candidateID, err := engine.Increment(r.engine, engine.KeyLocalSnapshotIDGenerator, 1)
	if err != nil {
		return "", err
	}
	snapshotID := strconv.FormatInt(candidateID, 10)
	err = r.engine.CreateSnapshot(snapshotID)
	return snapshotID, err
}

// InternalSnapshotCopy scans the key range specified by start key through
// end key up to some maximum number of results from the given snapshot_id.
// It will create a snapshot if snapshot_id is empty.
func (r *Range) InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest, reply *proto.InternalSnapshotCopyResponse) {
	if len(args.SnapshotId) == 0 {
		snapshotID, err := r.createSnapshot()
		if err != nil {
			reply.SetGoError(err)
			return
		}
		args.SnapshotId = snapshotID
	}

	kvs, err := r.engine.ScanSnapshot(args.Key, args.EndKey, args.MaxResults, args.SnapshotId)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if len(kvs) == 0 {
		err = r.engine.ReleaseSnapshot(args.SnapshotId)
	}

	reply.Rows = kvs
	reply.SnapshotId = args.SnapshotId
	reply.SetGoError(err)
}
