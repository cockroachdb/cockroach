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
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	gogoproto "github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
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
var TestingCommandFilter func(proto.Request) error

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

// configDescriptor describes administrative configuration maps
// affecting ranges of the key-value map by key prefix.
type configDescriptor struct {
	keyPrefix proto.Key         // Range key prefix
	gossipKey string            // Gossip key
	configI   gogoproto.Message // Config struct interface
}

// configDescriptors is an array containing the accounting, permissions,
// user, and zone configuration descriptors.
var configDescriptors = [...]*configDescriptor{
	{keys.ConfigAccountingPrefix, gossip.KeyConfigAccounting, &config.AcctConfig{}},
	{keys.ConfigPermissionPrefix, gossip.KeyConfigPermission, &config.PermConfig{}},
	{keys.ConfigUserPrefix, gossip.KeyConfigUser, &config.UserConfig{}},
	{keys.ConfigZonePrefix, gossip.KeyConfigZone, &config.ZoneConfig{}},
}

// tsCacheMethods specifies the set of methods which affect the
// timestamp cache.
var tsCacheMethods = [...]bool{
	proto.Get:                true,
	proto.Put:                true,
	proto.ConditionalPut:     true,
	proto.Increment:          true,
	proto.Scan:               true,
	proto.Delete:             true,
	proto.DeleteRange:        true,
	proto.ResolveIntent:      true,
	proto.ResolveIntentRange: true,
}

// usesTimestampCache returns true if the request affects or is
// affected by the timestamp cache.
func usesTimestampCache(r proto.Request) bool {
	m := r.Method()
	if m < 0 || m >= proto.Method(len(tsCacheMethods)) {
		return false
	}
	return tsCacheMethods[m]
}

// A pendingCmd holds a done channel for a command sent to Raft. Once
// committed to the Raft log, the command is executed and the result returned
// via the done channel.
type pendingCmd struct {
	ctx  context.Context
	done chan proto.ResponseWithError // Used to signal waiting RPC handler
}

// A rangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for splitting.
type rangeManager interface {
	// Accessors for shared state.
	ClusterID() string
	StoreID() proto.StoreID
	RaftNodeID() proto.RaftNodeID
	Clock() *hlc.Clock
	Engine() engine.Engine
	DB() *client.DB
	allocator() *allocator
	Gossip() *gossip.Gossip
	splitQueue() *splitQueue
	rangeGCQueue() *rangeGCQueue
	Stopper() *stop.Stopper
	EventFeed() StoreEventFeed
	Context(context.Context) context.Context
	resolveWriteIntentError(context.Context, *proto.WriteIntentError, *Replica, proto.Request, proto.PushTxnType) error

	// Range manipulation methods.
	LookupRange(start, end proto.Key) *Replica
	MergeRange(subsumingRng *Replica, updatedEndKey proto.Key, subsumedRaftID proto.RangeID) error
	NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error)
	NewSnapshot() engine.Engine
	ProposeRaftCommand(cmdIDKey, proto.RaftCommand) <-chan error
	RemoveRange(rng *Replica) error
	Tracer() *tracer.Tracer
	SplitRange(origRng, newRng *Replica) error
	processRangeDescriptorUpdate(rng *Replica) error
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	desc     unsafe.Pointer // Atomic pointer for *proto.RangeDescriptor
	rm       rangeManager   // Makes some store methods available
	stats    *rangeStats    // Range statistics
	maxBytes int64          // Max bytes before split.
	// Held while a split, merge, or replica change is underway.
	metaLock sync.Mutex // TODO(bdarnell): Revisit the metaLock.
	// Last index persisted to the raft log (not necessarily committed).
	// Updated atomically.
	lastIndex uint64
	// Last index applied to the state machine. Updated atomically.
	appliedIndex uint64
	configHashes map[int][]byte // Config map sha256 hashes @ last gossip
	lease        unsafe.Pointer // Information for leader lease, updated atomically
	llMu         sync.Mutex     // Synchronizes readers' requests for leader lease
	respCache    *ResponseCache // Provides idempotence for retries

	sync.RWMutex                 // Protects the following fields:
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	pendingCmds  map[cmdIDKey]*pendingCmd
}

// NewReplica initializes the replica using the given metadata.
func NewReplica(desc *proto.RangeDescriptor, rm rangeManager) (*Replica, error) {
	r := &Replica{
		rm:          rm,
		cmdQ:        NewCommandQueue(),
		tsCache:     NewTimestampCache(rm.Clock()),
		respCache:   NewResponseCache(desc.RangeID),
		pendingCmds: map[cmdIDKey]*pendingCmd{},
	}
	r.setDescWithoutProcessUpdate(desc)

	lastIndex, err := r.loadLastIndex()
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(&r.lastIndex, lastIndex)

	appliedIndex, err := r.loadAppliedIndex(r.rm.Engine())
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(&r.appliedIndex, appliedIndex)

	lease, err := loadLeaderLease(r.rm.Engine(), desc.RangeID)
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))

	// Gossip configs as they might not be gossiped until configs
	// are updated or a leader lease is acquired/extended.
	r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
		return r.ContainsKey(configPrefix)
	})

	if r.stats, err = newRangeStats(desc.RangeID, rm.Engine()); err != nil {
		return nil, err
	}

	return r, nil
}

// String returns a string representation of the range.
func (r *Replica) String() string {
	return fmt.Sprintf("range=%d [%s-%s)", r.Desc().RangeID, r.Desc().StartKey, r.Desc().EndKey)
}

// Destroy cleans up all data associated with this range.
func (r *Replica) Destroy() error {
	iter := newRangeDataIterator(r.Desc(), r.rm.Engine())
	defer iter.Close()
	batch := r.rm.Engine().NewBatch()
	defer batch.Close()
	for ; iter.Valid(); iter.Next() {
		_ = batch.Clear(iter.Key())
	}
	return batch.Commit()
}

// context returns a context which is initialized with information about
// this range. It is only relevant when commands need to be executed
// on this range in the absence of a pre-existing context, such as
// during range scanner operations.
func (r *Replica) context() context.Context {
	return context.WithValue(r.rm.Context(nil), log.RaftID, r.Desc().RangeID)
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
	return bytes.Equal(r.Desc().StartKey, proto.KeyMin)
}

func loadLeaderLease(eng engine.Engine, raftID proto.RangeID) (*proto.Lease, error) {
	lease := &proto.Lease{}
	if _, err := engine.MVCCGetProto(eng, keys.RaftLeaderLeaseKey(raftID), proto.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

// getLease returns the current leader lease.
func (r *Replica) getLease() *proto.Lease {
	return (*proto.Lease)(atomic.LoadPointer(&r.lease))
}

// newNotLeaderError returns a NotLeaderError intialized with the
// replica for the holder (if any) of the given lease.
func (r *Replica) newNotLeaderError(l *proto.Lease, originNode proto.RaftNodeID) error {
	err := &proto.NotLeaderError{}
	if l != nil && l.RaftNodeID != 0 {
		_, originStoreID := proto.DecodeRaftNodeID(originNode)
		_, err.Replica = r.Desc().FindReplica(originStoreID)
		_, storeID := proto.DecodeRaftNodeID(proto.RaftNodeID(l.RaftNodeID))
		_, err.Leader = r.Desc().FindReplica(storeID)
	}
	return err
}

// requestLeaderLease sends a request to obtain or extend a leader lease for
// this replica. Unless an error is returned, the obtained lease will be valid
// for a time interval containing the requested timestamp.
func (r *Replica) requestLeaderLease(timestamp proto.Timestamp) error {
	// TODO(Tobias): get duration from configuration, either as a config flag
	// or, later, dynamically adjusted.
	duration := int64(DefaultLeaderLeaseDuration)
	// Prepare a Raft command to get a leader lease for this replica.
	expiration := timestamp.Add(duration, 0)
	args := &proto.LeaderLeaseRequest{
		RequestHeader: proto.RequestHeader{
			Key:       r.Desc().StartKey,
			Timestamp: timestamp,
			CmdID: proto.ClientCmdID{
				WallTime: r.rm.Clock().Now().WallTime,
				Random:   rand.Int63(),
			},
		},
		Lease: proto.Lease{
			Start:      timestamp,
			Expiration: expiration,
			RaftNodeID: r.rm.RaftNodeID(),
		},
	}
	// Send lease request directly to raft in order to skip unnecessary
	// checks from normal request machinery, (e.g. the command queue).
	// Note that the command itself isn't traced, but usually the caller
	// waiting for the result has an active Trace.
	errChan, pendingCmd := r.proposeRaftCommand(r.context(), args)
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
func (r *Replica) redirectOnOrAcquireLeaderLease(trace *tracer.Trace, timestamp proto.Timestamp) error {
	r.llMu.Lock()
	defer r.llMu.Unlock()

	raftNodeID := r.rm.RaftNodeID()

	if lease := r.getLease(); lease.Covers(timestamp) {
		if lease.OwnedBy(raftNodeID) {
			// Happy path: We have an active lease, nothing to do.
			return nil
		}
		// If lease is currently held by another, redirect to holder.
		return r.newNotLeaderError(lease, raftNodeID)
	}
	defer trace.Epoch("request leader lease")()
	// Otherwise, no active lease: Request renewal.
	err := r.requestLeaderLease(timestamp)

	// Getting a LeaseRejectedError back means someone else got there first;
	// we can redirect if they cover our timestamp. Note that it can't be us,
	// since we're holding a lock here, and even if it were it would be a rare
	// extra round-trip.
	if _, ok := err.(*proto.LeaseRejectedError); ok {
		if lease := r.getLease(); lease.Covers(timestamp) {
			return r.newNotLeaderError(lease, raftNodeID)
		}
	}
	return err
}

// WaitForLeaderLease is used from unittests to wait until this range
// has the leader lease.
func (r *Replica) WaitForLeaderLease(t util.Tester) {
	util.SucceedsWithin(t, 1*time.Second, func() error {
		return r.requestLeaderLease(r.rm.Clock().Now())
	})
}

// isInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) isInitialized() bool {
	return len(r.Desc().EndKey) > 0
}

// Desc atomically returns the range's descriptor.
func (r *Replica) Desc() *proto.RangeDescriptor {
	return (*proto.RangeDescriptor)(atomic.LoadPointer(&r.desc))
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the range manager handle the
// descriptor update.
func (r *Replica) setDesc(desc *proto.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.rm == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.rm.processRangeDescriptorUpdate(r)
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate.
func (r *Replica) setDescWithoutProcessUpdate(desc *proto.RangeDescriptor) {
	atomic.StorePointer(&r.desc, unsafe.Pointer(desc))
}

// GetReplica returns the replica for this range from the range descriptor.
// Returns nil if the replica is not found.
func (r *Replica) GetReplica() *proto.Replica {
	_, replica := r.Desc().FindReplica(r.rm.StoreID())
	return replica
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Replica) GetMVCCStats() engine.MVCCStats {
	return r.stats.GetMVCC()
}

// ContainsKey returns whether this range contains the specified key.
func (r *Replica) ContainsKey(key proto.Key) bool {
	return containsKey(*r.Desc(), key)
}

func containsKey(desc proto.RangeDescriptor, key proto.Key) bool {
	return desc.ContainsKey(keys.KeyAddress(key))
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end proto.Key) bool {
	return containsKeyRange(*r.Desc(), start, end)
}

func containsKeyRange(desc proto.RangeDescriptor, start, end proto.Key) bool {
	return desc.ContainsKeyRange(keys.KeyAddress(start), keys.KeyAddress(end))
}

// GetGCMetadata reads the latest GC metadata for this range.
func (r *Replica) GetGCMetadata() (*proto.GCMetadata, error) {
	key := keys.RangeGCMetadataKey(r.Desc().RangeID)
	gcMeta := &proto.GCMetadata{}
	_, err := engine.MVCCGetProto(r.rm.Engine(), key, proto.ZeroTimestamp, true, nil, gcMeta)
	if err != nil {
		return nil, err
	}
	return gcMeta, nil
}

// GetLastVerificationTimestamp reads the timestamp at which the range's
// data was last verified.
func (r *Replica) GetLastVerificationTimestamp() (proto.Timestamp, error) {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RangeID)
	timestamp := proto.Timestamp{}
	_, err := engine.MVCCGetProto(r.rm.Engine(), key, proto.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return proto.ZeroTimestamp, err
	}
	return timestamp, nil
}

// SetLastVerificationTimestamp writes the timestamp at which the range's
// data was last verified.
func (r *Replica) SetLastVerificationTimestamp(timestamp proto.Timestamp) error {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RangeID)
	return engine.MVCCPutProto(r.rm.Engine(), nil, key, proto.ZeroTimestamp, nil, &timestamp)
}

// AddCmd adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue.
func (r *Replica) AddCmd(ctx context.Context, args proto.Request) (proto.Response, error) {
	// TODO(tschottdorf) Some (internal) requests go here directly, so they
	// won't be traced.
	trace := tracer.FromCtx(ctx)
	// Differentiate between admin, read-only and read-write.
	var reply proto.Response
	var err error
	if proto.IsAdmin(args) {
		defer trace.Epoch("admin path")()
		reply, err = r.addAdminCmd(ctx, args)
	} else if proto.IsReadOnly(args) {
		defer trace.Epoch("read-only path")()
		reply, err = r.addReadOnlyCmd(ctx, args)
	} else if proto.IsWrite(args) {
		defer trace.Epoch("read-write path")()
		reply, err = r.addWriteCmd(ctx, args, nil)
	} else {
		panic(fmt.Sprintf("don't know how to handle command %T", args))
	}

	return reply, err
}

func (r *Replica) checkCmdHeader(header *proto.RequestHeader) error {
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		return proto.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc())
	}
	return nil
}

// beginCmd waits for any overlapping, already-executing commands via
// the command queue and adds itself to the queue to gate follow-on
// commands which overlap its key range. This method will block if
// there are any overlapping commands already in the queue. Returns
// the command queue insertion key, to be supplied to subsequent
// invocation of endCmd().
func (r *Replica) beginCmd(header *proto.RequestHeader, readOnly bool) interface{} {
	r.Lock()
	var wg sync.WaitGroup
	r.cmdQ.GetWait(header.Key, header.EndKey, readOnly, &wg)
	cmdKey := r.cmdQ.Add(header.Key, header.EndKey, readOnly)
	r.Unlock()
	wg.Wait()
	// Update the incoming timestamp if unset. Wait until after any
	// preceding command(s) for key range are complete so that the node
	// clock has been updated to the high water mark of any commands
	// which might overlap this one in effect.
	if header.Timestamp.Equal(proto.ZeroTimestamp) {
		header.Timestamp = r.rm.Clock().Now()
	}
	return cmdKey
}

// endCmd removes a pending command from the command queue.
func (r *Replica) endCmd(cmdKey interface{}, args proto.Request, err error, readOnly bool) {
	r.Lock()
	if err == nil && usesTimestampCache(args) {
		header := args.Header()
		r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, header.Txn.GetID(), readOnly)
	}
	r.cmdQ.Remove(cmdKey)
	r.Unlock()
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the leader replica.
func (r *Replica) addAdminCmd(ctx context.Context, args proto.Request) (proto.Response, error) {
	header := args.Header()

	if err := r.checkCmdHeader(header); err != nil {
		return nil, err
	}

	// Admin commands always require the leader lease.
	if err := r.redirectOnOrAcquireLeaderLease(tracer.FromCtx(ctx), header.Timestamp); err != nil {
		return nil, err
	}

	switch tArgs := args.(type) {
	case *proto.AdminSplitRequest:
		resp, err := r.AdminSplit(*tArgs)
		return &resp, err
	case *proto.AdminMergeRequest:
		resp, err := r.AdminMerge(*tArgs)
		return &resp, err
	default:
		return nil, util.Error("unrecognized admin command")
	}
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Replica) addReadOnlyCmd(ctx context.Context, args proto.Request) (proto.Response, error) {
	header := args.Header()

	if err := r.checkCmdHeader(header); err != nil {
		return nil, err
	}

	// If read-consistency is set to INCONSISTENT, run directly.
	if header.ReadConsistency == proto.INCONSISTENT {
		// But disallow any inconsistent reads within txns.
		if header.Txn != nil {
			return nil, util.Error("cannot allow inconsistent reads within a transaction")
		}
		if header.Timestamp.Equal(proto.ZeroTimestamp) {
			header.Timestamp = r.rm.Clock().Now()
		}
		reply, intents, err := r.executeCmd(r.rm.Engine(), nil, args)
		r.handleSkippedIntents(args, intents) // even on error
		return reply, err
	} else if header.ReadConsistency == proto.CONSENSUS {
		return nil, util.Error("consensus reads not implemented")
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	cmdKey := r.beginCmd(header, true)

	// This replica must have leader lease to process a consistent read.
	if err := r.redirectOnOrAcquireLeaderLease(tracer.FromCtx(ctx), header.Timestamp); err != nil {
		r.endCmd(cmdKey, args, err, true /* readOnly */)
		return nil, err
	}

	// Execute read-only command.
	reply, intents, err := r.executeCmd(r.rm.Engine(), nil, args)

	// Only update the timestamp cache if the command succeeded.
	r.endCmd(cmdKey, args, err, true /* readOnly */)

	r.handleSkippedIntents(args, intents) // even on error
	return reply, err
}

// addWriteCmd first adds the keys affected by this command as pending writes
// to the command queue. Next, the timestamp cache is checked to determine if
// any newer accesses to this command's affected keys have been made. If so,
// the command's timestamp is moved forward. Finally, the command is submitted
// to Raft. Upon completion, the write is removed from the read queue and any
// error returned. If a WaitGroup is supplied, it is signaled when the command
// enters Raft or the function returns with a preprocessing error, whichever
// happens earlier.
func (r *Replica) addWriteCmd(ctx context.Context, args proto.Request, wg *sync.WaitGroup) (proto.Response, error) {
	signal := func() {
		if wg != nil {
			wg.Done()
			wg = nil
		}
	}

	// This happens more eagerly below, but it's important to guarantee that
	// early returns do not skip this.
	defer signal()

	header := args.Header()

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, err
	}

	trace := tracer.FromCtx(ctx)

	// Add the write to the command queue to gate subsequent overlapping
	// Commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	qDone := trace.Epoch("command queue")
	cmdKey := r.beginCmd(header, false)
	qDone()

	// This replica must have leader lease to process a write.
	if err := r.redirectOnOrAcquireLeaderLease(trace, header.Timestamp); err != nil {
		r.endCmd(cmdKey, args, err, false /* !readOnly */)
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
	if usesTimestampCache(args) {
		r.Lock()
		rTS, wTS := r.tsCache.GetMax(header.Key, header.EndKey, header.Txn.GetID())
		r.Unlock()

		// Always push the timestamp forward if there's been a read which
		// occurred after our txn timestamp.
		if !rTS.Less(header.Timestamp) {
			header.Timestamp = rTS.Next()
		}
		// If there's a newer write timestamp...
		if !wTS.Less(header.Timestamp) {
			// If we're in a txn, we still go ahead and try the write since
			// we want to avoid restarting the transaction in the event that
			// there isn't an intent or the intent can be pushed by us.
			//
			// If we're not in a txn, it's trivial to just advance our timestamp.
			if header.Txn == nil {
				header.Timestamp = wTS.Next()
			}
		}
	}

	defer trace.Epoch("raft")()

	errChan, pendingCmd := r.proposeRaftCommand(ctx, args)

	signal()

	// First wait for raft to commit or abort the command.
	var err error
	var reply proto.Response
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		respWithErr := <-pendingCmd.done
		reply, err = respWithErr.Reply, respWithErr.Err
	}

	// As for reads, update timestamp cache with the timestamp
	// of this write on success. This ensures a strictly higher
	// timestamp for successive writes to the same key or key range.
	r.endCmd(cmdKey, args, err, false /* !readOnly */)
	return reply, err
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns the error channel and
// pending command struct for receiving.
func (r *Replica) proposeRaftCommand(ctx context.Context, args proto.Request) (<-chan error, *pendingCmd) {
	pendingCmd := &pendingCmd{
		ctx:  ctx,
		done: make(chan proto.ResponseWithError, 1),
	}
	raftCmd := proto.RaftCommand{
		RangeID:      r.Desc().RangeID,
		OriginNodeID: r.rm.RaftNodeID(),
	}
	cmdID := args.Header().GetOrCreateCmdID(r.rm.Clock().PhysicalNow())
	ok := raftCmd.Cmd.SetValue(args)
	if !ok {
		log.Fatalc(ctx, "unknown command type %T", args)
	}
	idKey := makeCmdIDKey(cmdID)
	r.Lock()
	r.pendingCmds[idKey] = pendingCmd
	r.Unlock()
	errChan := r.rm.ProposeRaftCommand(idKey, raftCmd)

	return errChan, pendingCmd
}

// processRaftCommand processes a raft command by unpacking the command
// struct to get args and reply and then applying the command to the
// state machine via applyRaftCommand(). The error result is sent on
// the command's done channel, if available.
func (r *Replica) processRaftCommand(idKey cmdIDKey, index uint64, raftCmd proto.RaftCommand) error {
	if index == 0 {
		log.Fatalc(r.context(), "processRaftCommand requires a non-zero index")
	}

	r.Lock()
	cmd := r.pendingCmds[idKey]
	delete(r.pendingCmds, idKey)
	r.Unlock()

	args := raftCmd.Cmd.GetValue().(proto.Request)
	var reply proto.Response
	var ctx context.Context
	if cmd != nil {
		// We initiated this command, so use the caller-supplied context.
		ctx = cmd.ctx
	} else {
		// TODO(tschottdorf): consider the Trace situation here.
		ctx = r.context()
	}

	execDone := tracer.FromCtx(ctx).Epoch(fmt.Sprintf("applying %s", args.Method()))
	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	reply, err := r.applyRaftCommand(ctx, index, proto.RaftNodeID(raftCmd.OriginNodeID), args)
	err = r.maybeSetCorrupt(err)
	execDone()

	if cmd != nil {
		cmd.done <- proto.ResponseWithError{Reply: reply, Err: err}
	} else if err != nil && log.V(1) {
		log.Errorc(r.context(), "error executing raft command %s: %s", args.Method(), err)
	}

	return err
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine).
// When certain critical operations fail, a replicaCorruptionError may be
// returned and must be handled by the caller.
func (r *Replica) applyRaftCommand(ctx context.Context, index uint64, originNode proto.RaftNodeID, args proto.Request) (proto.Response, error) {
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
	batch, reply, rErr := r.applyRaftCommandInBatch(ctx, index, originNode, args, &ms)
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
	}

	// On successful write commands, flush to event feed, and handle other
	// write-related triggers including splitting and config gossip updates.
	if rErr == nil && proto.IsWrite(args) {
		// Publish update to event feed.
		r.rm.EventFeed().updateRange(r, args.Method(), &ms)
		// If the commit succeeded, potentially add range to split queue.
		r.maybeAddToSplitQueue()
		// Maybe update gossip configs if the command is not part of a transaction.
		// If the command is part of an uncommitted transaction, we rely on the
		// periodic configGossipInterval loop since we will not see the update
		// until the transaction is committed.
		if key := args.Header().Key; key.Less(keys.SystemMax) && args.Header().Txn == nil {
			r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
				return bytes.HasPrefix(key, configPrefix)
			})
		}
	}

	return reply, rErr
}

// applyRaftCommandInBatch executes the command in a batch engine and
// returns the batch containing the results. The caller is responsible
// for committing the batch, even on error.
func (r *Replica) applyRaftCommandInBatch(ctx context.Context, index uint64, originNode proto.RaftNodeID,
	args proto.Request, ms *engine.MVCCStats) (engine.Engine, proto.Response, error) {
	// Create a new batch for the command to ensure all or nothing semantics.
	batch := r.rm.Engine().NewBatch()

	if lease := r.getLease(); args.Method() != proto.LeaderLease &&
		(!lease.OwnedBy(originNode) || !lease.Covers(args.Header().Timestamp)) {
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
		return batch, nil, r.newNotLeaderError(lease, originNode)
	}

	// Check the response cache to ensure idempotency.
	if proto.IsWrite(args) {
		if replyWithErr, readErr := r.respCache.GetResponse(batch, args.Header().CmdID); readErr != nil {
			return batch, nil, newReplicaCorruptionError(util.Errorf("could not read from response cache"), readErr)
		} else if replyWithErr.Reply != nil {
			if log.V(1) {
				log.Infoc(ctx, "found response cache entry for %+v", args.Header().CmdID)
			}
			// We successfully read from the response cache, so return whatever error
			// was present in the cached entry (if any).
			return batch, replyWithErr.Reply, replyWithErr.Err
		}
	}

	// Execute the command.
	reply, intents, rErr := r.executeCmd(batch, ms, args)
	// Regardless of error, add result to the response cache if this is
	// a write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence, even if leadership changes.
	if proto.IsWrite(args) {
		if rErr == nil {
			// If command was successful, flush the MVCC stats to the batch.
			if err := r.stats.MergeMVCCStats(batch, ms, args.Header().Timestamp.WallTime); err != nil {
				log.Fatalc(ctx, "setting mvcc stats in a batch should never fail: %s", err)
			}
		} else {
			// Otherwise, reset the batch to clear out partial execution and
			// prepare for the failed response cache entry.
			batch.Close()
			batch = r.rm.Engine().NewBatch()
		}
		if reply == nil {
			reply = args.CreateReply()
		}
		if err := r.respCache.PutResponse(batch, args.Header().CmdID,
			proto.ResponseWithError{Reply: reply, Err: rErr}); err != nil {
			log.Fatalc(ctx, "putting a response cache entry in a batch should never fail: %s", err)
		}
	}

	// On the replica on which this command originated, resolve skipped intents
	// asynchronously - even on failure.
	if originNode == r.rm.RaftNodeID() {
		r.handleSkippedIntents(args, intents)
	}

	return batch, reply, rErr
}

// getLeaseForGossip tries to obtain a leader lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.rm.Gossip() == nil || !r.isInitialized() {
		return false, util.Errorf("no gossip or range not initialized")
	}
	var hasLease bool
	var err error
	if !r.rm.Stopper().RunTask(func() {
		timestamp := r.rm.Clock().Now()
		// Check for or obtain the lease, if none active.
		err = r.redirectOnOrAcquireLeaderLease(tracer.FromCtx(ctx), timestamp)
		hasLease = err == nil
		if err != nil {
			switch e := err.(type) {
			// NotLeaderError means there is an active lease, leaseRejectedError
			// means we tried to get one but someone beat us to it.
			case *proto.NotLeaderError, *proto.LeaseRejectedError:
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

	// Gossip the cluster ID from all replicas of the first range.
	log.Infoc(ctx, "gossiping cluster id %s from store %d, range %d", r.rm.ClusterID(),
		r.rm.StoreID(), r.Desc().RangeID)
	if err := r.rm.Gossip().AddInfo(gossip.KeyClusterID, r.rm.ClusterID(), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}

	if ok, err := r.getLeaseForGossip(ctx); !ok || err != nil {
		return err
	}
	log.Infoc(ctx, "gossiping sentinel from store %d, range %d", r.rm.StoreID(), r.Desc().RangeID)
	if err := r.rm.Gossip().AddInfo(gossip.KeySentinel, r.rm.ClusterID(), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}
	log.Infoc(ctx, "gossiping first range from store %d, range %d", r.rm.StoreID(), r.Desc().RangeID)
	if err := r.rm.Gossip().AddInfo(gossip.KeyFirstRangeDescriptor, *r.Desc(), configGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip first range metadata: %s", err)
	}
	return nil
}

// maybeGossipConfigs gossips those configuration maps for which the supplied
// function returns true and whose contents are marked dirty. Configuration
// maps include accounting, permissions, users, and zones. The store is in charge of
// the initial update, and the range itself re-triggers updates following
// writes that may have altered any of the maps.
//
// Note that maybeGossipConfigs gossips information only when the
// lease is actually held. The method does not request a leader lease
// here since LeaderLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrObtainLeaderLease.
// TODO(tschottdorf): Can possibly simplify.
func (r *Replica) maybeGossipConfigs(match func(proto.Key) bool) {
	r.Lock()
	defer r.Unlock()
	r.maybeGossipConfigsLocked(match)
}

func (r *Replica) maybeGossipConfigsLocked(match func(configPrefix proto.Key) bool) {
	if r.rm.Gossip() == nil || !r.isInitialized() {
		return
	}

	if lease := r.getLease(); !lease.OwnedBy(r.rm.RaftNodeID()) || !lease.Covers(r.rm.Clock().Now()) {
		// Do not gossip when a leader lease is not held.
		return
	}

	ctx := r.context()
	for i, cd := range configDescriptors {
		if match(cd.keyPrefix) {
			// Check for a bad range split. This should never happen as ranges
			// cannot be split mid-config.
			if !r.ContainsKey(cd.keyPrefix.PrefixEnd()) {
				// If we ever implement configs that span multiple ranges,
				// we must update store.startGossip accordingly. For the
				// time being, it will only fire the first range.
				log.Fatalc(ctx, "range splits configuration values for %s", cd.keyPrefix)
			}
			configMap, hash, err := loadConfigMap(r.rm.Engine(), cd.keyPrefix, cd.configI)
			if err != nil {
				log.Errorc(ctx, "failed loading %s config map: %s", cd.gossipKey, err)
				continue
			}
			if r.configHashes == nil {
				r.configHashes = map[int][]byte{}
			}
			if prevHash, ok := r.configHashes[i]; !ok || !bytes.Equal(prevHash, hash) {
				r.configHashes[i] = hash
				log.Infoc(ctx, "gossiping %s config from store %d, range %d", cd.gossipKey, r.rm.StoreID(), r.Desc().RangeID)
				if err := r.rm.Gossip().AddInfo(cd.gossipKey, configMap, 0); err != nil {
					log.Errorc(ctx, "failed to gossip %s configMap: %s", cd.gossipKey, err)
				}
			}
		}
	}
}

func (r *Replica) handleSkippedIntents(args proto.Request, intents []proto.Intent) {
	if len(intents) == 0 {
		return
	}

	ctx := r.context()
	stopper := r.rm.Stopper()
	// TODO(tschottdorf): There's a chance that #1684 will make a comeback
	// since intent resolution on commit has since moved to EndTransaction,
	// which returns (some of) them as skipped intents. If so, need to resolve
	// synchronously if we're not allowed to do async (or just launch
	// goroutines).
	stopper.RunAsyncTask(func() {
		err := r.rm.resolveWriteIntentError(ctx, &proto.WriteIntentError{
			Intents: intents,
		}, r, args, proto.CLEANUP_TXN)
		if wiErr, ok := err.(*proto.WriteIntentError); !ok || wiErr == nil || !wiErr.Resolved {
			log.Warningc(ctx, "failed to resolve on inconsistent read: %s", err)
		}
	})
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
func (r *Replica) resolveIntents(ctx context.Context, intents []proto.Intent) {
	trace := tracer.FromCtx(ctx)
	tracer.ToCtx(ctx, nil) // we're doing async stuff below; those need new traces
	trace.Event("resolving intents [async]")
	var wg sync.WaitGroup

	bArgs := &proto.BatchRequest{}
	bArgs.User = security.RootUser
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		var resolveArgs proto.Request
		var local bool // whether this intent lives on this Range
		{
			header := proto.RequestHeader{
				// Use the pushee's timestamp, which might be lower than the
				// pusher's request timestamp. No need to push the intent higher
				// than the pushee's txn!
				Timestamp: intent.Txn.Timestamp,
				Key:       intent.Key,
				EndKey:    intent.EndKey,
				User:      security.RootUser,
				Txn:       &intent.Txn,
			}

			if len(intent.EndKey) == 0 {
				resolveArgs = &proto.ResolveIntentRequest{RequestHeader: header}
				local = r.ContainsKey(intent.Key)
			} else {
				resolveArgs = &proto.ResolveIntentRangeRequest{RequestHeader: header}
				local = r.ContainsKeyRange(intent.Key, intent.EndKey)
			}
		}

		// If the intent isn't (completely) local, we'll need to send an external request.
		// We'll batch them all up and send at the end.
		if !local {
			bArgs.Add(resolveArgs)
			continue
		}

		// If it is local, it goes directly into Raft.
		// TODO(tschottdorf): this may be premature optimization. Consider just
		// treating everything as an external request. This means having to
		// wait for complete execution of the command (whereas now we just wait
		// for proposition) and some more overhead sending things around.
		wg.Add(1)
		action := func() {
			// Trace this under the ID of the intent owner.
			ctx := tracer.ToCtx(ctx, r.rm.Tracer().NewTrace(resolveArgs.Header().Txn))
			if _, err := r.addWriteCmd(ctx, resolveArgs, &wg); err != nil && log.V(1) {
				log.Warningc(ctx, "resolve for key %s failed: %s", intent.Key, err)
			}
		}
		if !r.rm.Stopper().RunAsyncTask(action) {
			// Still run the task. Our caller already has a task and going async
			// here again is merely for performance, but some intents need to
			// be resolved because they might block other tasks. See #1684.
			// Note that handleSkippedIntents has a TODO in case #1684 comes
			// back.
			action()
		}
	}
	// Resolve all of the intents which aren't local to the Range. This is a
	// no-op if all are local.
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: bArgs, Reply: &proto.BatchResponse{}})
	action := func() {
		// TODO(tschottdorf): no tracing here yet. Probably useful at some point,
		// but needs a) the corresponding interface and b) facilities for tracing
		// multiple tracees at the same time (batch full of possibly individual
		// txns).
		if err := r.rm.DB().Run(b); err != nil {
			if log.V(1) {
				log.Infoc(ctx, "%s", err)
			}
		}
	}
	if !r.rm.Stopper().RunAsyncTask(action) {
		// As with local intents, try async to not keep the caller waiting, but
		// when draining just go ahead and do it synchronously. See #1684.
		action()
	}

	// Wait until all the local `ResolveIntent`s have been submitted to raft.
	// No-op if all were external.
	wg.Wait()
}

// loadConfigMap scans the config entries under keyPrefix and
// instantiates/returns a config map and its sha256 hash. Prefix
// configuration maps include accounting, permissions, users, and zones.
func loadConfigMap(eng engine.Engine, keyPrefix proto.Key, configI gogoproto.Message) (config.PrefixConfigMap, []byte, error) {
	// TODO(tschottdorf): Currently this does not handle intents well.
	kvs, _, err := engine.MVCCScan(eng, keyPrefix, keyPrefix.PrefixEnd(), 0, proto.MaxTimestamp, true /* consistent */, nil)
	if err != nil {
		return nil, nil, err
	}
	var cfgs []*config.PrefixConfig
	sha := sha256.New()
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// proto encoded config from the Value into a new instance of configI.
		cfg := reflect.New(reflect.TypeOf(configI).Elem()).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, cfg); err != nil {
			return nil, nil, util.Errorf("unable to unmarshal config key %s: %s", string(kv.Key), err)
		}
		cfgs = append(cfgs, &config.PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: cfg})
		sha.Write(kv.Value.Bytes)
	}
	m, err := config.NewPrefixConfigMap(cfgs)
	return m, sha.Sum(nil), err
}

// maybeAddToSplitQueue checks whether the current size of the range
// exceeds the max size specified in the zone config. If yes, the
// range is added to the split queue.
func (r *Replica) maybeAddToSplitQueue() {
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && r.stats.KeyBytes+r.stats.ValBytes > maxBytes {
		r.rm.splitQueue().MaybeAdd(r, r.rm.Clock().Now())
	}
}
