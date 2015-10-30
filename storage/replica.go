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
	"crypto/sha1"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
	"github.com/gogo/protobuf/proto"
)

const (
	// DefaultHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 2x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultHeartbeatInterval = 5 * time.Second

	// clusterIDGossipTTL is time-to-live for cluster ID. The cluster ID
	// serves as the sentinel gossip key which informs a node whether or
	// not it's connected to the primary gossip network and not just a
	// partition. As such it must expire on a reasonable basis and be
	// continually re-gossiped. The replica which is the raft leader of
	// the first range gossips it.
	clusterIDGossipTTL = 2 * time.Minute
	// clusterIDGossipInterval is the approximate interval at which the
	// sentinel info is gossiped.
	clusterIDGossipInterval = clusterIDGossipTTL / 2

	// configGossipTTL is the time-to-live for configuration maps.
	configGossipTTL = 0 // does not expire
	// configGossipInterval is the interval at which range leaders gossip
	// their config maps. Even if config maps do not expire, we still
	// need a periodic gossip to safeguard against failure of a leader
	// to gossip after performing an update to the map.
	configGossipInterval = 1 * time.Minute
)

// TestingCommandFilter may be set in tests to intercept the handling
// of commands and artificially generate errors. Return nil to continue
// with regular processing or non-nil to terminate processing with the
// returned error. Note that in a multi-replica test this filter will
// be run once for each replica and must produce consistent results
// each time. Should only be used in tests in the storage and
// storage_test packages.
var TestingCommandFilter func(roachpb.Request, roachpb.Header) error

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
// TODO(tschottdorf): Push after removal can happen if txn is not
// single-roundtrip, and pusher will recreate a PENDING entry, getting
// stuck. Since txn gc story is not done yet (#2062), disabled for now.
var txnAutoGC = false

// raftInitialLogIndex is the starting point for the raft log. We bootstrap
// the raft membership by synthesizing a snapshot as if there were some
// discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5

	// DefaultLeaderLeaseDuration is the default duration of the leader lease.
	DefaultLeaderLeaseDuration = time.Second
)

// tsCacheMethods specifies the set of methods which affect the
// timestamp cache. This syntax creates a sparse array with maximum
// index equal to the value of the final Method. Unused indexes
// default to false.
var tsCacheMethods = [...]bool{
	roachpb.Get:                true,
	roachpb.Put:                true,
	roachpb.ConditionalPut:     true,
	roachpb.Increment:          true,
	roachpb.Scan:               true,
	roachpb.ReverseScan:        true,
	roachpb.Delete:             true,
	roachpb.DeleteRange:        true,
	roachpb.ResolveIntent:      true,
	roachpb.ResolveIntentRange: true,
}

// usesTimestampCache returns true if the request affects or is
// affected by the timestamp cache.
func usesTimestampCache(r roachpb.Request) bool {
	m := r.Method()
	if m < 0 || m >= roachpb.Method(len(tsCacheMethods)) {
		return false
	}
	return tsCacheMethods[m]
}

// A pendingCmd holds a done channel for a command sent to Raft. Once
// committed to the Raft log, the command is executed and the result returned
// via the done channel.
type pendingCmd struct {
	ctx  context.Context
	done chan roachpb.ResponseWithError // Used to signal waiting RPC handler
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	desc     unsafe.Pointer // Atomic pointer for *roachpb.RangeDescriptor
	store    *Store
	stats    *rangeStats // Range statistics
	maxBytes int64       // Max bytes before split.
	// Last index persisted to the raft log (not necessarily committed).
	// Updated atomically.
	lastIndex uint64
	// Last index applied to the state machine. Updated atomically.
	appliedIndex uint64
	systemDBHash []byte         // sha1 hash of the system config @ last gossip
	lease        unsafe.Pointer // Information for leader lease, updated atomically
	llMu         sync.Mutex     // Synchronizes readers' requests for leader lease
	respCache    *ResponseCache // Provides idempotence for retries

	// proposeRaftCommandFn can be set to mock out the propose operation.
	proposeRaftCommandFn func(cmdIDKey, roachpb.RaftCommand) <-chan error

	sync.RWMutex                 // Protects the following fields:
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	pendingCmds  map[cmdIDKey]*pendingCmd

	// pendingReplica houses a replica that is not yet in the range
	// descriptor, since we must be able to look up a replica's
	// descriptor in order to add it to the range. It is protected by
	// the RWMutex and once it has taken on a non-zero value it must not
	// be changed until that operation has completed and it has been
	// reset to a zero value. The sync.Cond is signaled whenever
	// pendingReplica.value changes to zero.
	pendingReplica struct {
		*sync.Cond
		value roachpb.ReplicaDescriptor
	}
	truncatedState unsafe.Pointer // *roachpb.RaftTruancatedState
}

var _ client.Sender = &Replica{}

// NewReplica initializes the replica using the given metadata.
func NewReplica(desc *roachpb.RangeDescriptor, rm *Store) (*Replica, error) {
	r := &Replica{
		store:       rm,
		cmdQ:        NewCommandQueue(),
		tsCache:     NewTimestampCache(rm.Clock()),
		respCache:   NewResponseCache(desc.RangeID),
		pendingCmds: map[cmdIDKey]*pendingCmd{},
	}
	r.pendingReplica.Cond = sync.NewCond(r)
	r.setDescWithoutProcessUpdate(desc)

	lastIndex, err := r.loadLastIndex()
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(&r.lastIndex, lastIndex)

	appliedIndex, err := r.loadAppliedIndex(r.store.Engine())
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(&r.appliedIndex, appliedIndex)

	lease, err := loadLeaderLease(r.store.Engine(), desc.RangeID)
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))

	if r.ContainsKey(keys.SystemDBSpan.Key) {
		r.maybeGossipSystemConfig()
	}

	if r.stats, err = newRangeStats(desc.RangeID, rm.Engine()); err != nil {
		return nil, err
	}

	return r, nil
}

// String returns a string representation of the range.
func (r *Replica) String() string {
	desc := r.Desc()
	return fmt.Sprintf("range=%d [%s-%s)", desc.RangeID, desc.StartKey, desc.EndKey)
}

// Destroy cleans up all data associated with this range, leaving a tombstone.
func (r *Replica) Destroy() error {
	desc := r.Desc()
	iter := newReplicaDataIterator(desc, r.store.Engine())
	defer iter.Close()
	batch := r.store.Engine().NewBatch()
	defer batch.Close()
	for ; iter.Valid(); iter.Next() {
		_ = batch.Clear(iter.Key())
	}

	// Save a tombstone. The range cannot be re-replicated onto this
	// node without having a replica ID of at least desc.NextReplicaID.
	tombstoneKey := keys.RaftTombstoneKey(desc.RangeID)
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: desc.NextReplicaID,
	}
	if err := engine.MVCCPutProto(batch, nil, tombstoneKey, roachpb.ZeroTimestamp, nil, tombstone); err != nil {
		return err
	}

	return batch.Commit()
}

// context returns a context which is initialized with information about
// this range. It is only relevant when commands need to be executed
// on this range in the absence of a pre-existing context, such as
// during range scanner operations.
func (r *Replica) context() context.Context {
	return context.WithValue(r.store.Context(nil), log.RangeID, r.Desc().RangeID)
}

// GetMaxBytes atomically gets the range maximum byte limit.
func (r *Replica) GetMaxBytes() int64 {
	return atomic.LoadInt64(&r.maxBytes)
}

// SetMaxBytes atomically sets the maximum byte limit before
// split. This value is cached by the range for efficiency.
func (r *Replica) SetMaxBytes(maxBytes int64) {
	atomic.StoreInt64(&r.maxBytes, maxBytes)
}

// IsFirstRange returns true if this is the first range.
func (r *Replica) IsFirstRange() bool {
	return bytes.Equal(r.Desc().StartKey, roachpb.RKeyMin)
}

func loadLeaderLease(eng engine.Engine, rangeID roachpb.RangeID) (*roachpb.Lease, error) {
	lease := &roachpb.Lease{}
	if _, err := engine.MVCCGetProto(eng, keys.RaftLeaderLeaseKey(rangeID), roachpb.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

// getLease returns the current leader lease.
func (r *Replica) getLease() *roachpb.Lease {
	return (*roachpb.Lease)(atomic.LoadPointer(&r.lease))
}

// newNotLeaderError returns a NotLeaderError initialized with the
// replica for the holder (if any) of the given lease.
func (r *Replica) newNotLeaderError(l *roachpb.Lease, originStoreID roachpb.StoreID) error {
	err := &roachpb.NotLeaderError{}
	if l != nil && l.Replica.ReplicaID != 0 {
		desc := r.Desc()

		err.RangeID = desc.RangeID
		_, err.Replica = desc.FindReplica(originStoreID)
		_, err.Leader = desc.FindReplica(l.Replica.StoreID)
	}
	return err
}

// requestLeaderLease sends a request to obtain or extend a leader lease for
// this replica. Unless an error is returned, the obtained lease will be valid
// for a time interval containing the requested timestamp.
func (r *Replica) requestLeaderLease(timestamp roachpb.Timestamp) error {
	// TODO(Tobias): get duration from configuration, either as a config flag
	// or, later, dynamically adjusted.
	duration := int64(DefaultLeaderLeaseDuration)
	// Prepare a Raft command to get a leader lease for this replica.
	expiration := timestamp.Add(duration, 0)
	desc := r.Desc()
	_, replica := desc.FindReplica(r.store.StoreID())
	if replica == nil {
		return roachpb.NewRangeNotFoundError(desc.RangeID)
	}
	args := &roachpb.LeaderLeaseRequest{
		Span: roachpb.Span{
			Key: desc.StartKey.AsRawKey(),
		},
		Lease: roachpb.Lease{
			Start:      timestamp,
			Expiration: expiration,
			Replica:    *replica,
		},
	}
	ba := roachpb.BatchRequest{}
	ba.RangeID = desc.RangeID
	ba.CmdID = roachpb.ClientCmdID{
		WallTime: r.store.Clock().Now().WallTime,
		Random:   rand.Int63(),
	}
	ba.Add(args)
	// Send lease request directly to raft in order to skip unnecessary
	// checks from normal request machinery, (e.g. the command queue).
	// Note that the command itself isn't traced, but usually the caller
	// waiting for the result has an active Trace.
	errChan, pendingCmd := r.proposeRaftCommand(r.context(), ba)
	if err := <-errChan; err != nil {
		return err
	}
	// Next if the command was committed, wait for the range to apply it.
	return (<-pendingCmd.done).Err
}

// redirectOnOrAcquireLeaderLease checks whether this replica has the
// leader lease at the specified timestamp. If it does, returns
// success. If another replica currently holds the lease, redirects by
// returning NotLeaderError. If the lease is expired, a renewal is
// synchronously requested. This method uses the leader lease mutex
// to guarantee only one request to grant the lease is pending.
//
// TODO(spencer): implement threshold regrants to avoid latency in
//  the presence of read or write pressure sufficiently close to the
//  current lease's expiration.
//
// TODO(spencer): for write commands, don't wait while requesting
//  the leader lease. If the lease acquisition fails, the write cmd
//  will fail as well. If it succeeds, as is likely, then the write
//  will not incur latency waiting for the command to complete.
//  Reads, however, must wait.
func (r *Replica) redirectOnOrAcquireLeaderLease(trace *tracer.Trace, timestamp roachpb.Timestamp) error {
	r.llMu.Lock()
	defer r.llMu.Unlock()

	if lease := r.getLease(); lease.Covers(timestamp) {
		if lease.OwnedBy(r.store.StoreID()) {
			// Happy path: We have an active lease, nothing to do.
			return nil
		}
		// If lease is currently held by another, redirect to holder.
		return r.newNotLeaderError(lease, r.store.StoreID())
	}
	defer trace.Epoch("request leader lease")()
	// Otherwise, no active lease: Request renewal.
	err := r.requestLeaderLease(timestamp)

	// Getting a LeaseRejectedError back means someone else got there first;
	// we can redirect if they cover our timestamp. Note that it can't be us,
	// since we're holding a lock here, and even if it were it would be a rare
	// extra round-trip.
	if _, ok := err.(*roachpb.LeaseRejectedError); ok {
		if lease := r.getLease(); lease.Covers(timestamp) {
			return r.newNotLeaderError(lease, r.store.StoreID())
		}
	}
	return err
}

// isInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) isInitialized() bool {
	return len(r.Desc().EndKey) > 0
}

// Desc atomically returns the range's descriptor.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	return (*roachpb.RangeDescriptor)(atomic.LoadPointer(&r.desc))
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the range manager handle the
// descriptor update.
func (r *Replica) setDesc(desc *roachpb.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.store == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.store.processRangeDescriptorUpdate(r)
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate.
func (r *Replica) setDescWithoutProcessUpdate(desc *roachpb.RangeDescriptor) {
	atomic.StorePointer(&r.desc, unsafe.Pointer(desc))
}

// getCachedTruncatedState atomically returns the range's cached truncated
// state.
func (r *Replica) getCachedTruncatedState() *roachpb.RaftTruncatedState {
	return (*roachpb.RaftTruncatedState)(atomic.LoadPointer(&r.truncatedState))
}

// setCachedTruncatedState atomically sets the range's truncated state.
func (r *Replica) setCachedTruncatedState(state *roachpb.RaftTruncatedState) {
	atomic.StorePointer(&r.truncatedState, unsafe.Pointer(state))
}

// GetReplica returns the replica for this range from the range descriptor.
// Returns nil if the replica is not found.
func (r *Replica) GetReplica() *roachpb.ReplicaDescriptor {
	_, replica := r.Desc().FindReplica(r.store.StoreID())
	return replica
}

// ReplicaDescriptor returns information about the given member of this replica's range.
func (r *Replica) ReplicaDescriptor(replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	r.RLock()
	defer r.RUnlock()

	desc := r.Desc()
	for _, repAddress := range desc.Replicas {
		if repAddress.ReplicaID == replicaID {
			return repAddress, nil
		}
	}
	if r.pendingReplica.value.ReplicaID == replicaID {
		return r.pendingReplica.value, nil
	}
	return roachpb.ReplicaDescriptor{}, util.Errorf("replica %d not found in range %d",
		replicaID, desc.RangeID)
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Replica) GetMVCCStats() engine.MVCCStats {
	return r.stats.GetMVCC()
}

// ContainsKey returns whether this range contains the specified key.
func (r *Replica) ContainsKey(key roachpb.Key) bool {
	return containsKey(*r.Desc(), key)
}

func containsKey(desc roachpb.RangeDescriptor, key roachpb.Key) bool {
	return desc.ContainsKey(keys.Addr(key))
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end roachpb.Key) bool {
	return containsKeyRange(*r.Desc(), start, end)
}

func containsKeyRange(desc roachpb.RangeDescriptor, start, end roachpb.Key) bool {
	return desc.ContainsKeyRange(keys.Addr(start), keys.Addr(end))
}

// GetGCMetadata reads the latest GC metadata for this range.
func (r *Replica) GetGCMetadata() (*roachpb.GCMetadata, error) {
	key := keys.RangeGCMetadataKey(r.Desc().RangeID)
	gcMeta := &roachpb.GCMetadata{}
	_, err := engine.MVCCGetProto(r.store.Engine(), key, roachpb.ZeroTimestamp, true, nil, gcMeta)
	if err != nil {
		return nil, err
	}
	return gcMeta, nil
}

// GetLastVerificationTimestamp reads the timestamp at which the range's
// data was last verified.
func (r *Replica) GetLastVerificationTimestamp() (roachpb.Timestamp, error) {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RangeID)
	timestamp := roachpb.Timestamp{}
	_, err := engine.MVCCGetProto(r.store.Engine(), key, roachpb.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return roachpb.ZeroTimestamp, err
	}
	return timestamp, nil
}

// SetLastVerificationTimestamp writes the timestamp at which the range's
// data was last verified.
func (r *Replica) SetLastVerificationTimestamp(timestamp roachpb.Timestamp) error {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RangeID)
	return engine.MVCCPutProto(r.store.Engine(), nil, key, roachpb.ZeroTimestamp, nil, &timestamp)
}

// Send adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue.
// TODO(tschottdorf): use BatchRequest w/o pointer receiver.
func (r *Replica) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	var br *roachpb.BatchResponse
	var err error

	if err := r.checkBatchRequest(ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	// TODO(tschottdorf) Some (internal) requests go here directly, so they
	// won't be traced.
	trace := tracer.FromCtx(ctx)
	// Differentiate between admin, read-only and write.
	if ba.IsAdmin() {
		defer trace.Epoch("admin path")()
		br, err = r.addAdminCmd(ctx, ba)
	} else if ba.IsReadOnly() {
		defer trace.Epoch("read-only path")()
		br, err = r.addReadOnlyCmd(ctx, ba)
	} else if ba.IsWrite() {
		defer trace.Epoch("read-write path")()
		br, err = r.addWriteCmd(ctx, ba, nil)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		panic("empty batch")
	} else {
		panic(fmt.Sprintf("don't know how to handle command %s", ba))
	}
	if err == multiraft.ErrGroupDeleted {
		// This error needs to be converted appropriately so that
		// clients will retry.
		err = roachpb.NewRangeNotFoundError(r.Desc().RangeID)
	}
	// TODO(tschottdorf): assert nil reply on error.
	if err != nil {
		trace.SetError()
		trace.Event(fmt.Sprintf("error: %s", err))
		return nil, roachpb.NewError(err)
	}
	return br, nil
}

// TODO(tschottdorf): almost obsolete.
func (r *Replica) checkCmdHeader(header *roachpb.Span) error {
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		return roachpb.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc())
	}
	return nil
}

// checkBatchRequest verifies BatchRequest validity requirements. In
// particular, timestamp, user, user priority and transactions must
// all be set to identical values between the batch request header and
// all constituent batch requests. Also, either all requests must be
// read-only, or none.
// TODO(tschottdorf): should check that request is contained in range
// and that EndTransaction only occurs at the very end.
func (r *Replica) checkBatchRequest(ba roachpb.BatchRequest) error {
	if ba.IsReadOnly() {
		if ba.ReadConsistency == roachpb.INCONSISTENT && ba.Txn != nil {
			// Disallow any inconsistent reads within txns.
			return util.Errorf("cannot allow inconsistent reads within a transaction")
		}
		if ba.ReadConsistency == roachpb.CONSENSUS {
			return util.Errorf("consensus reads not implemented")
		}
	} else if ba.ReadConsistency == roachpb.INCONSISTENT {
		return util.Errorf("inconsistent mode is only available to reads")
	}

	return nil
}

// beginCmds waits for any overlapping, already-executing commands via
// the command queue and adds itself to queues based on keys affected by the
// batched commands. This gates subsequent commands with overlapping keys or
// key ranges. This method will block if there are any overlapping commands
// already in the queue. Returns the command queue insertion keys, to be
// supplied to a subsequent invocation of endCmds().
func (r *Replica) beginCmds(ba *roachpb.BatchRequest) ([]interface{}, error) {
	var cmdKeys []interface{}
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		r.Lock()
		var wg sync.WaitGroup
		var spans []roachpb.Span
		readOnly := ba.IsReadOnly()
		for _, union := range ba.Requests {
			h := union.GetInner().Header()
			spans = append(spans, roachpb.Span{Key: h.Key, EndKey: h.EndKey})
		}
		r.cmdQ.GetWait(readOnly, &wg, spans...)
		cmdKeys = append(cmdKeys, r.cmdQ.Add(readOnly, spans...)...)
		r.Unlock()
		wg.Wait()
	}

	// Update the incoming timestamp if unset. Wait until after any
	// preceding command(s) for key range are complete so that the node
	// clock has been updated to the high water mark of any commands
	// which might overlap this one in effect.
	if ba.Timestamp.Equal(roachpb.ZeroTimestamp) {
		if ba.Txn != nil {
			// TODO(tschottdorf): see if this is already done somewhere else.
			ba.Timestamp = ba.Txn.Timestamp
		} else {
			ba.Timestamp = r.store.Clock().Now()
		}
	}

	return cmdKeys, nil
}

// endCmds removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (r *Replica) endCmds(cmdKeys []interface{}, ba roachpb.BatchRequest, err error) {
	r.Lock()
	// Only update the timestamp cache if the command succeeded and we're not
	// doing inconsistent ops (in which case the ops are always read-only).
	if err == nil && ba.ReadConsistency != roachpb.INCONSISTENT {
		for _, union := range ba.Requests {
			args := union.GetInner()
			if usesTimestampCache(args) {
				header := args.Header()
				var txnID []byte
				if ba.Txn != nil {
					txnID = ba.Txn.ID
				}
				r.tsCache.Add(header.Key, header.EndKey, ba.Timestamp, txnID, roachpb.IsReadOnly(args))
			}
		}
	}
	r.cmdQ.Remove(cmdKeys)
	r.Unlock()
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the leader replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) addAdminCmd(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	if len(ba.Requests) != 1 {
		return nil, util.Errorf("only single-element admin batches allowed")
	}
	args := ba.Requests[0].GetInner()

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, err
	}

	// Admin commands always require the leader lease.
	if err := r.redirectOnOrAcquireLeaderLease(tracer.FromCtx(ctx), ba.Timestamp); err != nil {
		return nil, err
	}

	var resp roachpb.Response
	var err error
	switch tArgs := args.(type) {
	case *roachpb.AdminSplitRequest:
		var reply roachpb.AdminSplitResponse
		reply, err = r.AdminSplit(*tArgs, r.Desc())
		resp = &reply
	case *roachpb.AdminMergeRequest:
		var reply roachpb.AdminMergeResponse
		reply, err = r.AdminMerge(*tArgs, r.Desc())
		resp = &reply
	default:
		return nil, util.Errorf("unrecognized admin command: %T", args)
	}

	if err != nil {
		return nil, err
	}
	br := &roachpb.BatchResponse{}
	br.Add(resp)
	br.Timestamp = ba.Timestamp
	br.Txn = resp.Header().Txn
	return br, nil
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Replica) addReadOnlyCmd(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	header := ba.Header
	trace := tracer.FromCtx(ctx)

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	qDone := trace.Epoch("command queue")
	cmdKeys, err := r.beginCmds(&ba)
	qDone()
	if err != nil {
		return nil, err
	}

	// If there are command keys (there might not be if reads are
	// inconsistent), the read requires the leader lease.
	if len(cmdKeys) > 0 {
		if err := r.redirectOnOrAcquireLeaderLease(trace, header.Timestamp); err != nil {
			r.endCmds(cmdKeys, ba, err)
			return nil, err
		}
	}

	// Execute read-only batch command.
	br, intents, err := r.executeBatch(r.store.Engine(), nil, ba)

	r.handleSkippedIntents(intents)

	// Remove keys from command queue.
	r.endCmds(cmdKeys, ba, err)

	if err != nil {
		return nil, err
	}
	return br, nil
}

// addWriteCmd first adds the keys affected by this command as pending writes
// to the command queue. Next, the timestamp cache is checked to determine if
// any newer accesses to this command's affected keys have been made. If so,
// the command's timestamp is moved forward. Finally, the command is submitted
// to Raft. Upon completion, the write is removed from the read queue and any
// error returned. If a WaitGroup is supplied, it is signaled when the command
// enters Raft or the function returns with a preprocessing error, whichever
// happens earlier.
func (r *Replica) addWriteCmd(ctx context.Context, ba roachpb.BatchRequest, wg *sync.WaitGroup) (*roachpb.BatchResponse, error) {
	signal := func() {
		if wg != nil {
			wg.Done()
			wg = nil
		}
	}

	// This happens more eagerly below, but it's important to guarantee that
	// early returns do not skip this.
	defer signal()

	trace := tracer.FromCtx(ctx)

	// Add the write to the command queue to gate subsequent overlapping
	// commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	qDone := trace.Epoch("command queue")
	cmdKeys, err := r.beginCmds(&ba)
	qDone()
	if err != nil {
		return nil, err
	}

	// This replica must have leader lease to process a write.
	if err := r.redirectOnOrAcquireLeaderLease(trace, ba.Timestamp); err != nil {
		r.endCmds(cmdKeys, ba, err)
		return nil, err
	}

	// Two important invariants of Cockroach: 1) encountering a more
	// recently written value means transaction restart. 2) values must
	// be written with a greater timestamp than the most recent read to
	// the same key. Check the timestamp cache for reads/writes which
	// are at least as recent as the timestamp of this write. For
	// writes, send WriteTooOldError; for reads, update the write's
	// timestamp. When the write returns, the updated timestamp will
	// inform the final commit timestamp.
	//
	// Find the maximum timestamp required to satisfy all requests in
	// the batch and then apply that to all requests.
	r.Lock()
	for _, union := range ba.Requests {
		args := union.GetInner()
		if usesTimestampCache(args) {
			header := args.Header()
			var txnID []byte
			if ba.Txn != nil {
				txnID = ba.Txn.ID
			}
			rTS, wTS := r.tsCache.GetMax(header.Key, header.EndKey, txnID)

			// Always push the timestamp forward if there's been a read which
			// occurred after our txn timestamp.
			if !rTS.Less(ba.Timestamp) {
				ba.Timestamp = rTS.Next()
			}
			// If there's a newer write timestamp...
			if !wTS.Less(ba.Timestamp) {
				// If we're in a txn, we still go ahead and try the write since
				// we want to avoid restarting the transaction in the event that
				// there isn't an intent or the intent can be pushed by us.
				//
				// If we're not in a txn, it's trivial to just advance our timestamp.
				if ba.Txn == nil {
					ba.Timestamp = wTS.Next()
				}
			}
		}
	}

	r.Unlock()

	defer trace.Epoch("raft")()

	errChan, pendingCmd := r.proposeRaftCommand(ctx, ba)

	signal()

	// First wait for raft to commit or abort the command.
	var br *roachpb.BatchResponse
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		respWithErr := <-pendingCmd.done
		br, err = respWithErr.Reply, respWithErr.Err
	}

	r.endCmds(cmdKeys, ba, err)
	return br, err
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns the error channel and
// pending command struct for receiving.
func (r *Replica) proposeRaftCommand(ctx context.Context, ba roachpb.BatchRequest) (<-chan error, *pendingCmd) {
	pendingCmd := &pendingCmd{
		ctx:  ctx,
		done: make(chan roachpb.ResponseWithError, 1),
	}
	desc := r.Desc()
	_, replica := desc.FindReplica(r.store.StoreID())
	if replica == nil {
		errChan := make(chan error, 1)
		errChan <- roachpb.NewRangeNotFoundError(desc.RangeID)
		return errChan, pendingCmd
	}
	raftCmd := roachpb.RaftCommand{
		RangeID:       desc.RangeID,
		OriginReplica: *replica,
		Cmd:           ba,
	}
	cmdID := ba.GetOrCreateCmdID(r.store.Clock().PhysicalNow())
	idKey := makeCmdIDKey(cmdID)
	r.Lock()
	r.pendingCmds[idKey] = pendingCmd
	r.Unlock()
	var errChan <-chan error
	if r.proposeRaftCommandFn != nil {
		errChan = r.proposeRaftCommandFn(idKey, raftCmd)
	} else {
		errChan = r.store.ProposeRaftCommand(idKey, raftCmd)
	}

	return errChan, pendingCmd
}

// processRaftCommand processes a raft command by unpacking the command
// struct to get args and reply and then applying the command to the
// state machine via applyRaftCommand(). The error result is sent on
// the command's done channel, if available.
func (r *Replica) processRaftCommand(idKey cmdIDKey, index uint64, raftCmd roachpb.RaftCommand) error {
	if index == 0 {
		log.Fatalc(r.context(), "processRaftCommand requires a non-zero index")
	}

	r.Lock()
	cmd := r.pendingCmds[idKey]
	delete(r.pendingCmds, idKey)
	r.Unlock()

	var ctx context.Context
	if cmd != nil {
		// We initiated this command, so use the caller-supplied context.
		ctx = cmd.ctx
	} else {
		// TODO(tschottdorf): consider the Trace situation here.
		ctx = r.context()
	}

	trace := tracer.FromCtx(ctx)
	execDone := trace.Epoch("applying batch")
	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	br, err := r.applyRaftCommand(ctx, index, raftCmd.OriginReplica, raftCmd.Cmd)
	err = r.maybeSetCorrupt(err)
	execDone()

	if cmd != nil {
		cmd.done <- roachpb.ResponseWithError{Reply: br, Err: err}
	} else if err != nil && log.V(1) {
		log.Errorc(r.context(), "error executing raft command: %s", err)
	}

	return err
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine).
// When certain critical operations fail, a replicaCorruptionError may be
// returned and must be handled by the caller.
func (r *Replica) applyRaftCommand(ctx context.Context, index uint64, originReplica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	if index <= 0 {
		log.Fatalc(ctx, "raft command index is <= 0")
	}

	// If we have an out of order index, there's corruption. No sense in trying
	// to update anything or run the command. Simply return a corruption error.
	if oldIndex := atomic.LoadUint64(&r.appliedIndex); oldIndex >= index {
		return nil, newReplicaCorruptionError(util.Errorf("applied index moved backwards: %d >= %d", oldIndex, index))
	}

	// Call the helper, which returns a batch containing data written
	// during command execution and any associated error.
	ms := engine.MVCCStats{}
	batch, br, intents, rErr := r.applyRaftCommandInBatch(ctx, index, originReplica, ba, &ms)
	defer batch.Close()

	// Advance the last applied index and commit the batch.
	if err := setAppliedIndex(batch, r.Desc().RangeID, index); err != nil {
		log.Fatalc(ctx, "setting applied index in a batch should never fail: %s", err)
	}
	if err := batch.Commit(); err != nil {
		rErr = newReplicaCorruptionError(util.Errorf("could not commit batch"), err, rErr)
	} else {
		// Update cached appliedIndex if we were able to set the applied index on disk.
		atomic.StoreUint64(&r.appliedIndex, index)
		// Invalidate the cache and let raftTruncatedState() read the value the next
		// time it's required.
		if _, ok := ba.GetArg(roachpb.TruncateLog); ok {
			r.setCachedTruncatedState(nil)
		}
	}

	// On successful write commands, flush to event feed, and handle other
	// write-related triggers including splitting and config gossip updates.
	if rErr == nil && ba.IsWrite() {
		// Publish update to event feed.
		// TODO(spencer): we should be sending feed updates for each part
		// of the batch. In particular, stats should be reported per-command.
		r.store.EventFeed().updateRange(r, roachpb.Batch, &ms)
		// If the commit succeeded, potentially add range to split queue.
		r.maybeAddToSplitQueue()
	}

	// On the replica on which this command originated, resolve skipped intents
	// asynchronously - even on failure.
	if originReplica.StoreID == r.store.StoreID() {
		r.handleSkippedIntents(intents)
	}

	return br, rErr
}

// applyRaftCommandInBatch executes the command in a batch engine and
// returns the batch containing the results. The caller is responsible
// for committing the batch, even on error.
func (r *Replica) applyRaftCommandInBatch(ctx context.Context, index uint64, originReplica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest, ms *engine.MVCCStats) (engine.Engine, *roachpb.BatchResponse, []intentsWithArg, error) {
	// Create a new batch for the command to ensure all or nothing semantics.
	btch := r.store.Engine().NewBatch()

	// Check the response cache for this batch to ensure idempotency.
	if ba.IsWrite() {
		if ba.CmdID == roachpb.ZeroCmdID {
			return btch, nil, nil, util.Errorf("write request without CmdID: %s", ba)
		}
		if replyWithErr, readErr := r.respCache.GetResponse(btch, ba.CmdID); readErr != nil {
			return btch, nil, nil, newReplicaCorruptionError(util.Errorf("could not read from response cache"), readErr)
		} else if replyWithErr.Reply != nil {
			// TODO(tschottdorf): this is a hack to avoid wrong replies served
			// back. See #2297. Not 100% correct, only correct enough to get
			// tests passing (multi-range requests going wrong).
			// Treat RangeKeyMismatchError specially. We don't want the correct
			// range to pretend it's mismatching. All other errors have no
			// sanity check as to whether they actually belong to the request,
			// there's no information to deduce that from.
			ok := true
			if _, isMismatch := replyWithErr.Err.(*roachpb.RangeKeyMismatchError); isMismatch {
				ok = false
			}
			if ok && replyWithErr.Err == nil {
				ok = len(replyWithErr.Reply.Responses) == len(ba.Requests)
				for i, union := range replyWithErr.Reply.Responses {
					if !ok {
						break
					}
					args := ba.Requests[i].GetInner()
					reply := union.GetInner()
					ok = fmt.Sprintf("%T", args.CreateReply()) == fmt.Sprintf("%T", reply)
				}
			}
			if ok {
				if log.V(1) {
					log.Infoc(ctx, "found response cache entry for %+v", ba.CmdID)
				}
				// We successfully read from the response cache, so return whatever error
				// was present in the cached entry (if any).
				return btch, replyWithErr.Reply, nil, replyWithErr.Err
			} else if replyWithErr.Err == nil {
				log.Warningf("TODO(tschottdorf): #2297: %s hit cache for: <%s,%T>", ba, replyWithErr.Reply, replyWithErr.Err)
			}
		}
	}

	for _, union := range ba.Requests {
		args := union.GetInner()

		// TODO(tschottdorf): shouldn't be in the loop. Currently is because
		// we haven't cleaned up the timestamp handling fully.
		if lease := r.getLease(); args.Method() != roachpb.LeaderLease &&
			(!lease.OwnedBy(originReplica.StoreID) || !lease.Covers(ba.Timestamp)) {
			// Verify the leader lease is held, unless this command is trying to
			// obtain it. Any other Raft command has had the leader lease held
			// by the replica at proposal time, but this may no longer be the case.
			// Corruption aside, the most likely reason is a leadership change (the
			// most recent leader assumes responsibility for all past timestamps as
			// well). In that case, it's not valid to go ahead with the execution:
			// Writes must be aware of the last time the mutated key was read, and
			// since reads are served locally by the lease holder without going
			// through Raft, a read which was not taken into account may have been
			// served. Hence, we must retry at the current leader.
			//
			// It's crucial that we don't update the response cache for the error
			// returned below since the request is going to be retried with the
			// same ClientCmdID and would get the distributed sender stuck in an
			// infinite loop, retrieving a stale NotLeaderError over and over
			// again, even when proposing at the correct replica.
			return btch, nil, nil, r.newNotLeaderError(lease, originReplica.StoreID)
		}
	}

	// Execute the commands.
	br, intents, err := r.executeBatch(btch, ms, ba)

	// Regardless of error, add result to the response cache if this is
	// a write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence, even if leadership changes.
	if ba.IsWrite() {
		if err == nil {
			// If command was successful, flush the MVCC stats to the batch.
			if err := r.stats.MergeMVCCStats(btch, ms, ba.Timestamp.WallTime); err != nil {
				// TODO(tschottdorf): ReplicaCorruptionError.
				log.Fatalc(ctx, "setting mvcc stats in a batch should never fail: %s", err)
			}
		} else {
			// TODO(tschottdorf): make `nil` acceptable. Corresponds to
			// roachpb.Response{With->Or}Error.
			br = &roachpb.BatchResponse{}
			// Otherwise, reset the batch to clear out partial execution and
			// prepare for the failed response cache entry.
			btch.Close()
			btch = r.store.Engine().NewBatch()
		}
		if err := r.respCache.PutResponse(btch, ba.CmdID,
			roachpb.ResponseWithError{Reply: br, Err: err}); err != nil {
			// TODO(tschottdorf): ReplicaCorruptionError.
			log.Fatalc(ctx, "putting a response cache entry in a batch should never fail: %s", err)
		}
	}

	return btch, br, intents, err
}

type intentsWithArg struct {
	args    roachpb.Request
	intents []roachpb.Intent
}

func (r *Replica) executeBatch(batch engine.Engine, ms *engine.MVCCStats, ba roachpb.BatchRequest) (*roachpb.BatchResponse, []intentsWithArg, error) {
	br := &roachpb.BatchResponse{}
	br.Timestamp = ba.Timestamp
	var intents []intentsWithArg
	// If transactional, we use ba.Txn for each individual command and
	// accumulate updates to it.
	isTxn := ba.Txn != nil

	// Ensure that timestamps on individual requests are offset by an incremental
	// amount of logical ticks from the base timestamp of the batch request (if
	// that is non-zero and the batch is not in a Txn). This has the effect of
	// allowing self-overlapping commands within the batch, which would otherwise
	// result in an endless loop of WriteTooOldErrors.
	// TODO(tschottdorf): discuss self-overlap.
	// TODO(tschottdorf): provisional feature to get back to passing tests.
	// Have to discuss how we go about it.
	fiddleWithTimestamps := ba.Txn == nil && ba.IsWrite()

	// TODO(tschottdorf): provisionals ahead. This loop needs to execute each
	// command and propagate txn and timestamp to the next (and, eventually,
	// to the batch response header). We're currently in an intermediate stage
	// in which requests still contain a header with a lot of redundant info,
	// so the code is fairly clumsy and tries to overwrite as much as it can
	// from the batch. There'll certainly be some more iterations to stream-
	// line the code in this loop.
	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()

		// Set the timestamp for this request.
		ts := ba.Timestamp
		{
			// TODO(tschottdorf): Special exception because of pending
			// self-overlap discussion.
			// TODO(tschottdorf): currently treating PushTxn specially. Otherwise
			// conflict resolution would break because a batch full of pushes of
			// the same Txn for different intents will push the pushee in
			// iteration, bumping up its priority. Unfortunately PushTxn is flagged
			// as a write command (is it really?). Interim solution.
			// Note that being in a txn implies no fiddling.
			if fiddleWithTimestamps && args.Method() != roachpb.PushTxn {
				ts = ba.Timestamp.Add(0, int32(index))
			} else if !isTxn {
				ts = ba.Timestamp
			} else {
				// TODO(tschottdorf): it can happen that the request timestamp
				// is ahead of the Txn timestamp. A few tests do this on pur-
				// pose; should undo that and see if there's a legitimate
				// reason left to keep allowing this behavior.
				ts.Forward(ba.Txn.Timestamp)
			}
		}

		header := ba.Header
		header.Timestamp = ts

		reply, curIntents, err := r.executeCmd(batch, ms, header, args)

		// Collect intents skipped over the course of execution.
		if len(curIntents) > 0 {
			// TODO(tschottdorf): see about refactoring the args away.
			intents = append(intents, intentsWithArg{args: args, intents: curIntents})
		}

		if err != nil {
			if iErr, ok := err.(roachpb.IndexedError); ok {
				iErr.SetErrorIndex(int32(index))
			}
			return nil, intents, err
		}

		// Add the response to the batch, updating the timestamp.
		br.Timestamp.Forward(ts)
		br.Add(reply)
		if isTxn {
			if txn := reply.Header().Txn; txn != nil {
				ba.Txn.Update(txn)
			}
			// The transaction timestamp can't lag behind the actual timestamps
			// we're operating at.
			// TODO(tschottdorf): without this, TestSingleKey fails (since the Txn
			// will commit with its original timestamp). We should have a
			// non-acceptance test verifying this as well.
			// TODO(tschottdorf): wasn't there another place that did this?
			ba.Txn.Timestamp.Forward(br.Timestamp)
		}
	}
	// If transactional, send out the final transaction entry with the reply.
	if isTxn {
		br.Txn = ba.Txn
		// If this is the beginning of the write portion of a transaction,
		// mark the returned transaction as Writing. It's important that
		// we do this only at the end, since updating at the first write
		// could "poison" the transaction on restartable errors, see #2920.
		if _, ok := ba.GetArg(roachpb.BeginTransaction); ok {
			br.Txn.Writing = true
		}
	}
	return br, intents, nil
}

// getLeaseForGossip tries to obtain a leader lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.isInitialized() {
		return false, util.Errorf("no gossip or range not initialized")
	}
	var hasLease bool
	var err error
	if !r.store.Stopper().RunTask(func() {
		timestamp := r.store.Clock().Now()
		// Check for or obtain the lease, if none active.
		err = r.redirectOnOrAcquireLeaderLease(tracer.FromCtx(ctx), timestamp)
		hasLease = err == nil
		if err != nil {
			switch e := err.(type) {
			// NotLeaderError means there is an active lease, leaseRejectedError
			// means we tried to get one but someone beat us to it.
			case *roachpb.NotLeaderError, *roachpb.LeaseRejectedError:
				err = nil
			default:
				// Any other error is worth being logged visibly.
				log.Warningc(ctx, "could not acquire lease for range gossip: %s", e)
			}
		}
	}) {
		err = util.Errorf("node is stopping")
	}
	return hasLease, err
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a leader lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange() error {
	if !r.IsFirstRange() {
		return nil
	}

	ctx := r.context()

	desc := r.Desc()

	// Gossip the cluster ID from all replicas of the first range.
	if log.V(1) {
		log.Infoc(ctx, "gossiping cluster id %s from store %d, range %d", r.store.ClusterID(),
			r.store.StoreID(), r.Desc().RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeyClusterID, []byte(r.store.ClusterID()), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}

	if ok, err := r.getLeaseForGossip(ctx); !ok || err != nil {
		return err
	}
	if log.V(1) {
		log.Infoc(ctx, "gossiping sentinel from store %d, range %d", r.store.StoreID(), desc.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeySentinel, []byte(r.store.ClusterID()), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip sentinel: %s", err)
	}
	if log.V(1) {
		log.Infoc(ctx, "gossiping first range from store %d, range %d", r.store.StoreID(), desc.RangeID)
	}
	if err := r.store.Gossip().AddInfoProto(gossip.KeyFirstRangeDescriptor, desc, configGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip first range metadata: %s", err)
	}
	return nil
}

// maybeGossipSystemConfig scans the entire SystemDB span and gossips it.
// The first call is on NewReplica. Further calls come from the trigger
// on an EndTransactionRequest.
//
// Note that maybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a leader lease
// here since LeaderLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrObtainLeaderLease.
// TODO(tschottdorf): Can possibly simplify.
func (r *Replica) maybeGossipSystemConfig() {
	r.Lock()
	defer r.Unlock()
	r.maybeGossipSystemConfigLocked()
}

func (r *Replica) maybeGossipSystemConfigLocked() {
	if r.store.Gossip() == nil || !r.isInitialized() {
		return
	}

	if lease := r.getLease(); !lease.OwnedBy(r.store.StoreID()) || !lease.Covers(r.store.Clock().Now()) {
		// Do not gossip when a leader lease is not held.
		return
	}

	ctx := r.context()
	// TODO(marc): check for bad split in the middle of the SystemDB span.
	kvs, hash, err := loadSystemDBSpan(r.store.Engine())
	if err != nil {
		log.Errorc(ctx, "could not load SystemDB span: %s", err)
		return
	}
	if bytes.Equal(r.systemDBHash, hash) {
		return
	}

	if log.V(1) {
		log.Infoc(ctx, "gossiping system config from store %d, range %d", r.store.StoreID(), r.Desc().RangeID)
	}

	cfg := &config.SystemConfig{Values: kvs}
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, cfg, 0); err != nil {
		log.Errorc(ctx, "failed to gossip system config: %s", err)
		return
	}

	// Successfully gossiped. Update tracking hash.
	r.systemDBHash = hash
}

func (r *Replica) handleSkippedIntents(intents []intentsWithArg) {
	if len(intents) == 0 {
		return
	}
	now := r.store.Clock().Now()

	for _, item := range intents {
		ctx := r.context()
		stopper := r.store.Stopper()
		// TODO(tschottdorf): avoid data race related to batch unrolling in ExecuteCmd;
		// can probably go again when that provisional code there is gone. Should
		// still be careful though, a retry could happen and race with args.
		args := proto.Clone(item.args).(roachpb.Request)
		stopper.RunAsyncTask(func() {
			h := roachpb.Header{Timestamp: now}
			err := r.store.resolveWriteIntentError(ctx, &roachpb.WriteIntentError{
				Intents: item.intents,
			}, r, args, h, roachpb.CLEANUP_TXN)
			if wiErr, ok := err.(*roachpb.WriteIntentError); !ok || wiErr == nil || !wiErr.Resolved {
				log.Warningc(ctx, "failed to resolve on inconsistent read: %s", err)
			}
		})
	}
}

// TODO(spencerkimball): move to util.
type chainedError struct {
	error
	cause *chainedError
}

// Error implements the error interface, printing the underlying chain of errors.
func (ce *chainedError) Error() string {
	if ce == nil {
		ce = &chainedError{}
	}
	if ce.cause != nil {
		return fmt.Sprintf("%s (caused by %s)", ce.error, ce.cause)
	}
	return ce.error.Error()
}

// newChainedError returns a chainedError made up from the given errors,
// omitting nil values. It returns nil unless at least one of its arguments
// is not nil.
func newChainedError(errs ...error) *chainedError {
	if len(errs) == 0 || (len(errs) == 1 && errs[0] == nil) {
		return nil
	}
	ce := &chainedError{error: errs[0]}
	for _, err := range errs[1:] {
		if err == nil {
			continue
		}
		ce.cause = &chainedError{error: err}
		ce = ce.cause
	}
	return ce
}

// A replicaCorruptionError indicates that the replica has experienced an error
// which puts its integrity at risk.
type replicaCorruptionError struct {
	error
	// processed indicates that the error has been taken into account and
	// necessary steps will be taken. For now, required for testing.
	processed bool
}

// Error implements the error interface.
func (rce *replicaCorruptionError) Error() string {
	if rce == nil {
		rce = newReplicaCorruptionError()
	}
	return fmt.Sprintf("replica corruption (processed=%t): %s", rce.processed, rce.error)
}

// newReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func newReplicaCorruptionError(err ...error) *replicaCorruptionError {
	return &replicaCorruptionError{error: newChainedError(err...)}
}

// maybeSetCorrupt is a stand-in for proper handling of failing replicas. Such a
// failure is indicated by a call to maybeSetCorrupt with a replicaCorruptionError.
// Currently any error is passed through, but prospectively it should stop the
// range from participating in progress, trigger a rebalance operation and
// decide on an error-by-error basis whether the corruption is limited to the
// range, store, node or cluster with corresponding actions taken.
func (r *Replica) maybeSetCorrupt(err error) error {
	if cErr, ok := err.(*replicaCorruptionError); ok && cErr != nil {
		log.Errorc(r.context(), "stalling replica due to: %s", cErr.error)
		cErr.processed = true
		return cErr
	}
	return err
}

// resolveIntents resolves the given intents. For those which are local to the
// range, we submit directly to the range-local Raft instance; the call returns
// as soon as all resolve commands have been **proposed** (not executed). This
// ensures that if a waiting client retries immediately after conflict
// resolution, it will not hit the same intents again. All non-local intents
// are resolved asynchronously in a batch.
// TODO(tschottdorf): once Txn records have a list of possibly open intents,
// resolveIntents should send an RPC to update the transaction(s) as well (for
// those intents with non-pending Txns).
func (r *Replica) resolveIntents(ctx context.Context, intents []roachpb.Intent) {
	trace := tracer.FromCtx(ctx)
	tracer.ToCtx(ctx, nil) // we're doing async stuff below; those need new traces
	trace.Event("resolving intents [async]")

	var reqsRemote []roachpb.Request
	baLocal := roachpb.BatchRequest{}
	baLocal.CmdID = baLocal.GetOrCreateCmdID(r.store.Clock().PhysicalNow())
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		var resolveArgs roachpb.Request
		var local bool // whether this intent lives on this Range
		{
			header := roachpb.Span{
				Key:    intent.Key,
				EndKey: intent.EndKey,
			}

			if len(intent.EndKey) == 0 {
				resolveArgs = &roachpb.ResolveIntentRequest{
					Span:      header,
					IntentTxn: intent.Txn,
				}
				local = r.ContainsKey(intent.Key)
			} else {
				resolveArgs = &roachpb.ResolveIntentRangeRequest{
					Span:      header,
					IntentTxn: intent.Txn,
				}
				local = r.ContainsKeyRange(intent.Key, intent.EndKey)
			}
		}

		// If the intent isn't (completely) local, we'll need to send an external request.
		// We'll batch them all up and send at the end.
		if local {
			baLocal.Add(resolveArgs)
		} else {
			reqsRemote = append(reqsRemote, resolveArgs)
		}
	}

	// The local batch goes directly to Raft.
	var wg sync.WaitGroup
	if len(baLocal.Requests) > 0 {
		action := func() {
			// Trace this under the ID of the intent owner.
			trace := r.store.Tracer().NewTrace(tracer.Node, baLocal)
			defer trace.Finalize()
			ctx := tracer.ToCtx(ctx, trace)
			if _, err := r.addWriteCmd(ctx, baLocal, &wg); err != nil {
				if log.V(1) {
					log.Warningc(ctx, "batch resolve failed: %s", err)
				}
				// At this point, as long as the local Replica accepts the
				// request it should never fail. However, the replica may reject
				// the request in certain cases (for example, if the replica has
				// been removed from its range via a rebalancing a command).
				// Therefore, we inspect the returned error to detect cases
				// where the command was rejected, and can safely ignore those
				// errors.
				if err != multiraft.ErrGroupDeleted {
					switch err.(type) {
					case *roachpb.RangeKeyMismatchError:
					case *roachpb.NotLeaderError:
					case *roachpb.RangeNotFoundError:
					default:
						// TODO(tschottdorf): Does this need to be a panic?
						panic(fmt.Sprintf("intent resolution failed with unexpected error: %s", err))
					}
				}
			}
		}
		wg.Add(1)
		if !r.store.Stopper().RunAsyncTask(action) {
			// Still run the task when draining. Our caller already has a task and
			// going async here again is merely for performance, but some intents
			// need to be resolved because they might block other tasks. See #1684.
			// Note that handleSkippedIntents has a TODO in case #1684 comes back.
			action()
		}
	}

	// Resolve all of the intents which aren't local to the Range.
	if len(reqsRemote) > 0 {
		b := &client.Batch{}
		b.InternalAddRequest(reqsRemote...)
		action := func() {
			// TODO(tschottdorf): no tracing here yet.
			if err := r.store.DB().Run(b); err != nil {
				log.Warningf("unable to resolve intent: %s", err)
			}
		}
		if !r.store.Stopper().RunAsyncTask(action) {
			// As with local intents, try async to not keep the caller waiting, but
			// when draining just go ahead and do it synchronously. See #1684.
			action()
		}
	}

	// Wait until the local ResolveIntents batch has been submitted to
	// raft. No-op if all were non-local.
	wg.Wait()
}

// loadSystemDBSpan scans the entire SystemDB span and returns the full list of
// key/value pairs along with the sha1 checksum of the contents (key and value).
func loadSystemDBSpan(eng engine.Engine) ([]roachpb.KeyValue, []byte, error) {
	// TODO(tschottdorf): Currently this does not handle intents well.
	kvs, _, err := engine.MVCCScan(eng, keys.SystemDBSpan.Key, keys.SystemDBSpan.EndKey,
		0, roachpb.MaxTimestamp, true /* consistent */, nil)
	if err != nil {
		return nil, nil, err
	}
	sha := sha1.New()
	for _, kv := range kvs {
		if _, err := sha.Write(kv.Key); err != nil {
			return nil, nil, err
		}
		// There are all kinds of different types here, so we can't use the
		// typed getters.
		if _, err := sha.Write(kv.Value.RawBytes); err != nil {
			return nil, nil, err
		}
	}
	return kvs, sha.Sum(nil), err
}

// maybeAddToSplitQueue checks whether the current size of the range
// exceeds the max size specified in the zone config. If yes, the
// range is added to the split queue.
func (r *Replica) maybeAddToSplitQueue() {
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && r.stats.KeyBytes+r.stats.ValBytes > maxBytes {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
	}
}
