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
	InternalSplit         = "InternalSplit"
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
	InternalSplit:         struct{}{},
}

// tsCacheMethods specifies the set of methods which affect the timestamp cache.
var tsCacheMethods = map[string]struct{}{
	Contains:              struct{}{},
	Get:                   struct{}{},
	Put:                   struct{}{},
	ConditionalPut:        struct{}{},
	Increment:             struct{}{},
	Scan:                  struct{}{},
	Delete:                struct{}{},
	DeleteRange:           struct{}{},
	AccumulateTS:          struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
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

// UsesTimestampCache returns true if the method affects or is
// affected by the timestamp cache.
func UsesTimestampCache(method string) bool {
	_, ok := tsCacheMethods[method]
	return ok
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

	sync.RWMutex                 // Protects cmdQ, tsCache & respCache
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
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
		cmdQ:      NewCommandQueue(),
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

// Update must be called after the metadata of the range have been adapted to
// make sure the updates are properly taken into account and persisted.
func (r *Range) Update() error {
	if r.rm == nil {
		return nil
	}
	return r.rm.UpdateRange(r.Meta.RangeID)
}

// Destroy must be called for a range that is being dismantled to make sure it
// is properly handling pending clients and cleans up persisted data.
//
// Destroy() will not Stop() the range; this has to be done manually before.
func (r *Range) Destroy() error {
	if r.rm == nil {
		return nil
	}
	r.respCache.ClearData() // Ignore the result. If it fails, GC should get it.
	return r.rm.DropRange(r.Meta.RangeID)
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
	return r.Meta.ContainsKey(key.Address())
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end engine.Key) bool {
	return r.Meta.ContainsKeyRange(start.Address(), end.Address())
}

// AddCmd adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue. If wait is false, read-write commands are added to
// Raft without waiting for their completion.
func (r *Range) AddCmd(method string, args proto.Request, reply proto.Response, wait bool) error {
	header := args.Header()
	var err error
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		err = proto.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Meta)
	} else if !r.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		err = &proto.NotLeaderError{}
	}
	if err != nil {
		reply.Header().SetGoError(err)
		return err
	}

	// Differentiate between read-only and read-write.
	if IsReadOnly(method) {
		if !wait {
			return util.Errorf("cannot specify !wait for read-only requests")
		}
		return r.addReadOnlyCmd(method, args, reply)
	}
	return r.addReadWriteCmd(method, args, reply, wait)
}

// beginCmd waits for any overlapping, already-executing commands via
// the command queue and adds itself to the queue to gate follow-on
// commands which overlap its key range. This method will block if
// there are any overlapping commands already in the queue. Returns
// the command queue insertion key, to be supplied to subsequent
// invocation of cmdQ.Remove().
func (r *Range) beginCmd(start, end engine.Key, readOnly bool) interface{} {
	r.Lock()
	var wg sync.WaitGroup
	r.cmdQ.GetWait(start, end, readOnly, &wg)
	cmdKey := r.cmdQ.Add(start, end, readOnly)
	r.Unlock()
	wg.Wait()
	return cmdKey
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Range) addReadOnlyCmd(method string, args proto.Request, reply proto.Response) error {
	header := args.Header()

	// Add the read to the command queue to gate subsequent
	// overlapping, commands until this command completes.
	cmdKey := r.beginCmd(header.Key, header.EndKey, true)

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
	// read-timestamp-cache to reset its low water mark.
	if !r.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		return &proto.NotLeaderError{}
	}
	err := r.executeCmd(method, args, reply)

	// Only update the timestamp cache if the command succeeded.
	r.Lock()
	if err == nil && UsesTimestampCache(method) {
		r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, header.Txn.MD5(), true /* readOnly */)
	}
	r.cmdQ.Remove(cmdKey)
	r.Unlock()

	return err
}

// addReadWriteCmd first consults the response cache to determine whether
// this command has already been sent to the range. If a response is
// found, it's returned immediately and not submitted to raft. Next,
// the timestamp cache is checked to determine if any newer accesses to
// this command's affected keys have been made. If so, this command's
// timestamp is moved forward. Finally the keys affected by this
// command are added as pending writes to the read queue and the
// command is submitted to Raft. Upon completion, the write is removed
// from the read queue and the reply is added to the response cache.
// If wait is true, will block until the command is complete.
func (r *Range) addReadWriteCmd(method string, args proto.Request, reply proto.Response, wait bool) error {
	// Check the response cache in case this is a replay. This call
	// may block if the same command is already underway.
	header := args.Header()
	txnMD5 := header.Txn.MD5()
	if ok, err := r.respCache.GetResponse(header.CmdID, reply); ok || err != nil {
		if ok { // this is a replay! extract error for return
			return reply.Header().GoError()
		}
		// In this case there was an error reading from the response
		// cache. Instead of failing the request just because we can't
		// decode the reply in the response cache, we proceed as though
		// idempotence has expired.
		log.Errorf("unable to read result for %+v from the response cache: %s", args, err)
	}

	// Add the write to the command queue to gate subsequent overlapping
	// commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	cmdKey := r.beginCmd(header.Key, header.EndKey, false)

	// Two important invariants of Cockroach: 1) encountering a more
	// recently written value means transaction restart. 2) values must
	// be written with a greater timestamp than the most recent read to
	// the same key. Check the timestamp cache for reads/writes which
	// are at least as recent as the timestamp of this write. For
	// writes, send TransactionRetryError; for reads, update the write's
	// timestamp. When the write returns, the updated timestamp will
	// inform the final commit timestamp.
	if UsesTimestampCache(method) {
		r.Lock()
		rTS, wTS := r.tsCache.GetMax(header.Key, header.EndKey, txnMD5)
		r.Unlock()

		// If there's a newer write timestamp and we're in a txn, set a
		// write too old error in reply. We still go ahead and try the
		// write; afterall, the cause of the higher timestamp may be an
		// intent we can push.
		if !wTS.Less(header.Timestamp) && header.Txn != nil {
			err := &proto.WriteTooOldError{Timestamp: header.Timestamp, ExistingTimestamp: wTS}
			reply.Header().SetGoError(err)
		} else if !wTS.Less(header.Timestamp) || !rTS.Less(header.Timestamp) {
			// Otherwise, make sure we advance the request's timestamp.
			ts := wTS
			if ts.Less(rTS) {
				ts = rTS
			}
			if log.V(1) {
				log.Infof("Overriding existing timestamp %s with %s", header.Timestamp, ts)
			}
			ts.Logical++ // increment logical component by one to differentiate.
			// Update the request timestamp.
			header.Timestamp = ts
		}
	}

	// Create command and enqueue for Raft.
	cmd := &Cmd{
		Method: method,
		Args:   args,
		Reply:  reply,
		done:   make(chan error, 1),
	}
	r.raft <- cmd

	// Create a completion func for mandatory cleanups which we either
	// run synchronously if we're waiting or in a goroutine otherwise.
	completionFunc := func() error {
		err := <-cmd.done

		// As for reads, update timestamp cache with the timestamp
		// of this write on success. This ensures a strictly higher
		// timestamp for successive writes to the same key or key range.
		r.Lock()
		if err == nil && UsesTimestampCache(method) {
			r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, txnMD5, false /* !readOnly */)
		}
		r.cmdQ.Remove(cmdKey)
		r.Unlock()

		// If the original client didn't wait (e.g. resolve write intent),
		// log execution errors so they're surfaced somewhere.
		if !wait && err != nil {
			log.Warningf("non-synchronous execution of %s with %+v failed: %s", cmd.Method, cmd.Args, err)
		}
		return err
	}

	if wait {
		return completionFunc()
	}
	go completionFunc()
	return nil
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
			log.Errorf("failed to gossip cluster ID %s: %s", r.Meta.ClusterID, err)
		}
	}
}

// maybeGossipFirstRange gossips the range locations if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipFirstRange() {
	if r.gossip != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.gossip.AddInfo(gossip.KeyFirstRangeMetadata, r.Meta.RangeDescriptor, 0*time.Second); err != nil {
			log.Errorf("failed to gossip first range metadata: %s", err)
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
					log.Errorf("failed loading %s config map: %s", cp.gossipKey, err)
					continue
				} else {
					if err := r.gossip.AddInfo(cp.gossipKey, configMap, 0*time.Second); err != nil {
						log.Errorf("failed to gossip %s configMap: %s", cp.gossipKey, err)
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
	kvs, err := r.mvcc.Scan(keyPrefix, keyPrefix.PrefixEnd(), 0, proto.MaxTimestamp, nil)
	if err != nil {
		return nil, err
	}
	var configs []*PrefixConfig
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// gob encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, config); err != nil {
			return nil, util.Errorf("unable to unmarshal config key %s: %s", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
	}
	return NewPrefixConfigMap(configs)
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
//
// TODO(Spencer): Differentiate between errors caused by the normal culprits --
// bad inputs from clients, stale information, etc. and errors which might
// cause the range replicas to diverge -- running out of disk space, underlying
// rocksdb corruption, etc. Do a careful code audit to make sure we identify
// errors which should be classified as a ReplicaCorruptionError--when those
// bubble up to the point where we've just tried to execute a Raft command, the
// Raft replica would need to stall itself.
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
	case InternalSplit:
		r.InternalSplit(args.(*proto.InternalSplitRequest), reply.(*proto.InternalSplitResponse))
	default:
		return util.Errorf("unrecognized command type: %s", method)
	}

	// Propagate the request timestamp (which may have changed).
	reply.Header().Timestamp = args.Header().Timestamp

	log.V(1).Infof("executed %s command %+v: %+v", method, args, reply)

	// Add this command's result to the response cache if this is a
	// read/write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence when leadership changes.
	if !IsReadOnly(method) {
		if putErr := r.respCache.PutResponse(args.Header().CmdID, reply); putErr != nil {
			log.Errorf("unable to write result of %+v: %+v to the response cache: %s",
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
	// Encode the key for direct access to/from the engine.
	encKey := engine.Key(args.Key).Encode(nil)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.GetProto(r.engine, encKey, existTxn)
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
			reply.SetGoError(proto.NewTransactionAbortedError(existTxn))
			return
		} else if args.Txn.Epoch < existTxn.Epoch {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("epoch regression: %d", args.Txn.Epoch)))
			return
		} else if existTxn.Timestamp.Less(args.Txn.Timestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the transaction timestamp.
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("timestamp regression: %s", args.Txn.Timestamp)))
			return
		}
		// Take max of requested epoch and existing epoch. The requester
		// may have incremented the epoch on retries.
		if reply.Txn.Epoch < args.Txn.Epoch {
			reply.Txn.Epoch = args.Txn.Epoch
		}
		// Take max of requested priority and existing priority. This isn't
		// terribly useful, but we do it for completeness.
		if reply.Txn.Priority < args.Txn.Priority {
			reply.Txn.Priority = args.Txn.Priority
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
			reply.SetGoError(proto.NewTransactionRetryError(reply.Txn, false /* !Backoff */))
			return
		}
		reply.Txn.Status = proto.COMMITTED
	} else {
		reply.Txn.Status = proto.ABORTED
	}

	// Persist the transaction record with updated status (& possibly timestmap).
	if err := engine.PutProto(r.engine, encKey, reply.Txn); err != nil {
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
	metaPrefix := engine.Key(args.Key[:len(engine.KeyMeta1Prefix)])
	nextKey := engine.Key(args.Key).Next()
	kvs, err := r.mvcc.Scan(nextKey, metaPrefix.PrefixEnd(), rangeCount, args.Timestamp, args.Txn)
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
	// Encode the key for direct access to/from the engine.
	encKey := engine.Key(args.Key).Encode(nil)

	var txn proto.Transaction
	ok, err := engine.GetProto(r.engine, encKey, &txn)
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
		if err := engine.PutProto(r.engine, encKey, &txn); err != nil {
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
// Txn already committed/aborted: If pushee txn is committed or
// aborted return success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use PushTxn.Timestamp as LastHeartbeat. If
// current time - LastHeartbeat > 2 * DefaultHeartbeatInterval, then
// the pushee txn should be either pushed forward or aborted,
// depending on value of Request.Abort.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of args.Abort. If
// args.Abort is true, set txn.Status to ABORTED, and priority to one
// less than the pusher's priority and return success. If args.Abort
// is false, set txn.Timestamp to pusher's Timestamp + 1 (note that
// we use the pusher's Args.Timestamp, not Txn.Timestamp because the
// args timestamp can advance during the txn).
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionRetryError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Range) InternalPushTxn(args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) {
	key := engine.Key(args.Key)
	if !bytes.Equal(key.Address(), args.PusheeTxn.ID) {
		reply.SetGoError(util.Errorf("request key %q should match pushee's txn ID %q",
			key.Address(), args.PusheeTxn.ID))
		return
	}

	// Encode the key for direct access to/from the engine.
	encKey := key.Encode(nil)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.GetProto(r.engine, encKey, existTxn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if ok {
		// Start with the persisted transaction record as final transaction.
		reply.PusheeTxn = gogoproto.Clone(existTxn).(*proto.Transaction)
		// Upgrade the epoch, timestamp and priority as necessary.
		if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
			reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		}
		if reply.PusheeTxn.Timestamp.Less(args.PusheeTxn.Timestamp) {
			reply.PusheeTxn.Timestamp = args.PusheeTxn.Timestamp
		}
		if reply.PusheeTxn.Priority < args.PusheeTxn.Priority {
			reply.PusheeTxn.Priority = args.PusheeTxn.Priority
		}
	} else {
		// Some sanity checks for case where we don't find a transaction record.
		if args.PusheeTxn.LastHeartbeat != nil {
			reply.SetGoError(proto.NewTransactionStatusError(&args.PusheeTxn,
				"no txn persisted, yet intent has heartbeat"))
			return
		} else if args.PusheeTxn.Status != proto.PENDING {
			reply.SetGoError(proto.NewTransactionStatusError(&args.PusheeTxn,
				fmt.Sprintf("no txn persisted, yet intent has status %s", args.PusheeTxn.Status)))
			return
		}
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.PusheeTxn = gogoproto.Clone(&args.PusheeTxn).(*proto.Transaction)
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != proto.PENDING {
		// Trivial noop.
		return
	}
	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if !args.Abort && args.Timestamp.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return
	}

	// pusherWins bool is true in the event the pusher prevails.
	var pusherWins bool

	// If there's no incoming transaction, the pusher is
	// non-transactional. We make a random priority, biased by
	// specified args.Header().UserPriority in this case.
	var priority int32
	if args.Txn != nil {
		priority = args.Txn.Priority
	} else {
		priority = proto.MakePriority(args.GetUserPriority())
	}

	// Check for txn timeout.
	if reply.PusheeTxn.LastHeartbeat == nil {
		reply.PusheeTxn.LastHeartbeat = &reply.PusheeTxn.Timestamp
	}
	// Compute heartbeat expiration.
	expiry := r.clock.Now()
	expiry.WallTime -= 2 * DefaultHeartbeatInterval.Nanoseconds()
	if reply.PusheeTxn.LastHeartbeat.Less(expiry) {
		log.V(1).Infof("pushing expired txn %s", reply.PusheeTxn)
		pusherWins = true
	} else if args.PusheeTxn.Epoch < reply.PusheeTxn.Epoch {
		// Check for an intent from a prior epoch.
		log.V(1).Infof("pushing intent from previous epoch for txn %s", reply.PusheeTxn)
		pusherWins = true
	} else if reply.PusheeTxn.Priority < priority ||
		(reply.PusheeTxn.Priority == priority && args.Txn.Timestamp.Less(reply.PusheeTxn.Timestamp)) {
		// Finally, choose based on priority; if priorities are equal, order by lower txn timestamp.
		log.V(1).Infof("pushing intent from txn with lower priority %s vs %d", reply.PusheeTxn, priority)
		pusherWins = true
	} else if reply.PusheeTxn.Isolation == proto.SNAPSHOT && !args.Abort {
		log.V(1).Infof("pushing timestamp for snapshot isolation txn")
		pusherWins = true
	}

	if !pusherWins {
		log.V(1).Infof("failed to push intent %s vs %s using priority=%d", reply.PusheeTxn, args.Txn, priority)
		reply.SetGoError(proto.NewTransactionRetryError(reply.PusheeTxn, true /* Backoff */))
		return
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.Abort {
		reply.PusheeTxn.Status = proto.ABORTED
	} else {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.Timestamp
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction.
	if err := engine.PutProto(r.engine, encKey, reply.PusheeTxn); err != nil {
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

// InternalSplit shrinks the given range, using args.SplitKey as its new end.
// A new range is created, containing the remaining range from args.SplitKey to
// the original end key of the first range. The split is only carried out if
// args.Key and args.EndKey match precisely the split key and EndKey of the
// original range, respectively.
//
// TODO(Tobias): This version, as is, is provisional. A correct implementation
// is much more subtle and requires a transaction. See:
// https://github.com/cockroachdb/cockroach/pull/64/files#r17667926
func (r *Range) InternalSplit(args *proto.InternalSplitRequest, reply *proto.InternalSplitResponse) {
	var err error
	if r.rm == nil {
		reply.SetGoError(util.Error("cannot split without an underlying store"))
		return
	}
	desc := r.Meta.RangeDescriptor
	// InternalSplit affects the whole range, but in fact only needs to block
	// everything that is to be moved into the new range beginning at the split
	// key. To make sure that the range is protecting those and only those
	// keys, we check that args.Key == splitKey and args.EndKey = desc.EndKey.
	if !bytes.Equal(args.Key, args.SplitKey) || !bytes.Equal(args.EndKey, desc.EndKey) {
		reply.SetGoError(util.Error("key range indicated does not match range"))
		return
	}
	splitKey := args.SplitKey
	if !r.ContainsKey(splitKey) {
		reply.SetGoError(util.Errorf("split key not contained in range"))
		return
	}

	// Lock the range for the duration of the split.
	r.Lock()
	defer r.Unlock()

	// Create a new range, beginning with the split key. The new range is
	// initialized with fresh timestamp cache and read queue, which is the
	// correct behavior. However, the new response cache must know about cached
	// data which now belongs to the new queue. Since caching a little
	// redundant data does not matter, we simply clone the response cache of
	// the old range.

	meta := r.rm.NewRangeMetadata(splitKey, desc.EndKey, desc.Replicas)
	newRange, err := r.rm.CreateRange(meta)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	// Initialize the new range's response cache with a copy of the old one.
	// This may be a little expensive. Note that the new range has not been
	// started yet and that the original range is serving requests for the keys
	// that will remain with it, which is roughly 50%.
	if err = r.respCache.CopyInto(newRange.respCache); err != nil {
		reply.SetGoError(err)
		return
	}
	// TODO(Tobias): Check for possible problems with temporarily having two
	// overlapping ranges.
	newRange.Start()
	// Only now do we lock the range, blocking process on other writes and
	// reads in the original half as well.
	newRange.Lock()
	defer newRange.Unlock()
	// Shrink the existing range and commit to disk.
	oldEndKey := r.Meta.RangeDescriptor.EndKey
	r.Meta.RangeDescriptor.EndKey = splitKey
	if err = r.Update(); err != nil {
		reply.SetGoError(util.Errorf("cannot save range, split aborted: %s", err))
		r.Meta.RangeDescriptor.EndKey = oldEndKey
		newRange.Stop()
		if err = newRange.Destroy(); err != nil {
			log.Errorf("unable to drop obsolete range (error: %s), manual cleanup necessary: %v", err, newRange)
		}
		return
	}
	// TODO(mrtracy): Transactionally update the Meta{1,2} keys so that both
	// ranges have their correct key spaces reflected.
	// Special case: This range contains Meta{1,2} keys (-> possible deadlock)
}
