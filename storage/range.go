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
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
)

// init pre-registers RangeDescriptor, PrefixConfigMap types and Transaction.
func init() {
	gob.Register(proto.StoreDescriptor{})
	gob.Register(PrefixConfigMap{})
	gob.Register(&proto.AcctConfig{})
	gob.Register(&proto.PermConfig{})
	gob.Register(&proto.UserConfig{})
	gob.Register(&proto.ZoneConfig{})
	gob.Register(proto.RangeDescriptor{})
	gob.Register(proto.Transaction{})
}

var (
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
	configGossipTTL = 0 * time.Second // does not expire
	// configGossipInterval is the interval at which range leaders gossip
	// their config maps. Even if config maps do not expire, we still
	// need a periodic gossip to safeguard against failure of a leader
	// to gossip after performing an update to the map.
	configGossipInterval = 1 * time.Minute
)

// TestingCommandFilter may be set in tests to intercept the handling
// of commands and artificially generate errors. Return true to
// terminate processing with the filled-in response, or false to
// continue with regular processing. Note that in a multi-replica test
// this filter will be run once for each replica and must produce
// consistent results each time. Should only be used in tests in the
// storage package but needs to be exported due to circular import
// issues.
var TestingCommandFilter func(proto.Request, proto.Response) bool

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
	keyPrefix proto.Key   // Range key prefix
	gossipKey string      // Gossip key
	configI   interface{} // Config struct interface
}

// configDescriptors is an array containing the accounting, permissions,
// user, and zone configuration descriptors.
var configDescriptors = [...]*configDescriptor{
	{keys.ConfigAccountingPrefix, gossip.KeyConfigAccounting, proto.AcctConfig{}},
	{keys.ConfigPermissionPrefix, gossip.KeyConfigPermission, proto.PermConfig{}},
	{keys.ConfigUserPrefix, gossip.KeyConfigUser, proto.UserConfig{}},
	{keys.ConfigZonePrefix, gossip.KeyConfigZone, proto.ZoneConfig{}},
}

// tsCacheMethods specifies the set of methods which affect the
// timestamp cache.
var tsCacheMethods = [...]bool{
	proto.Get:                        true,
	proto.Put:                        true,
	proto.ConditionalPut:             true,
	proto.Increment:                  true,
	proto.Scan:                       true,
	proto.Delete:                     true,
	proto.DeleteRange:                true,
	proto.InternalResolveIntent:      true,
	proto.InternalResolveIntentRange: true,
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

// A pendingCmd holds the reply buffer and a done channel for a command
// sent to Raft. Once committed to the Raft log, the command is
// executed and the result returned via the done channel.
type pendingCmd struct {
	Reply proto.Response
	ctx   context.Context
	done  chan error // Used to signal waiting RPC handler
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
	Stopper() *util.Stopper
	EventFeed() StoreEventFeed
	Context(context.Context) context.Context
	resolveWriteIntentError(context.Context, *proto.WriteIntentError, *Range, proto.Request, proto.PushTxnType, bool) error

	// Range manipulation methods.
	LookupRange(start, end proto.Key) *Range
	MergeRange(subsumingRng *Range, updatedEndKey proto.Key, subsumedRaftID proto.RaftID) error
	NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error)
	NewSnapshot() engine.Engine
	ProposeRaftCommand(cmdIDKey, proto.InternalRaftCommand) <-chan error
	RemoveRange(rng *Range) error
	SplitRange(origRng, newRng *Range) error
	processRangeDescriptorUpdate(rng *Range) error
}

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
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

	sync.RWMutex                 // Protects the following fields:
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	respCache    *ResponseCache  // Provides idempotence for retries
	pendingCmds  map[cmdIDKey]*pendingCmd
}

// NewRange initializes the range using the given metadata.
func NewRange(desc *proto.RangeDescriptor, rm rangeManager) (*Range, error) {
	r := &Range{
		rm:          rm,
		cmdQ:        NewCommandQueue(),
		tsCache:     NewTimestampCache(rm.Clock()),
		respCache:   NewResponseCache(desc.RaftID),
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

	lease, err := loadLeaderLease(r.rm.Engine(), desc.RaftID)
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))

	if r.stats, err = newRangeStats(desc.RaftID, rm.Engine()); err != nil {
		return nil, err
	}

	return r, nil
}

// String returns a string representation of the range.
func (r *Range) String() string {
	return fmt.Sprintf("range=%d [%s - %s)", r.Desc().RaftID, r.Desc().StartKey, r.Desc().EndKey)
}

// Destroy cleans up all data associated with this range.
func (r *Range) Destroy() error {
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
func (r *Range) context() context.Context {
	return context.WithValue(r.rm.Context(nil), log.RaftID, r.Desc().RaftID)
}

// GetMaxBytes atomically gets the range maximum byte limit.
func (r *Range) GetMaxBytes() int64 {
	return atomic.LoadInt64(&r.maxBytes)
}

// SetMaxBytes atomically sets the maximum byte limit before
// split. This value is cached by the range for efficiency.
func (r *Range) SetMaxBytes(maxBytes int64) {
	atomic.StoreInt64(&r.maxBytes, maxBytes)
}

// IsFirstRange returns true if this is the first range.
func (r *Range) IsFirstRange() bool {
	return bytes.Equal(r.Desc().StartKey, proto.KeyMin)
}

func loadLeaderLease(eng engine.Engine, raftID proto.RaftID) (*proto.Lease, error) {
	lease := &proto.Lease{}
	if _, err := engine.MVCCGetProto(eng, keys.RaftLeaderLeaseKey(raftID), proto.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

// getLease returns the current leader lease.
func (r *Range) getLease() *proto.Lease {
	return (*proto.Lease)(atomic.LoadPointer(&r.lease))
}

// newNotLeaderError returns a NotLeaderError intialized with the
// replica for the holder (if any) of the given lease.
func (r *Range) newNotLeaderError(l *proto.Lease, originNode proto.RaftNodeID) error {
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
func (r *Range) requestLeaderLease(timestamp proto.Timestamp) error {
	// TODO(Tobias): get duration from configuration, either as a config flag
	// or, later, dynamically adjusted.
	duration := int64(DefaultLeaderLeaseDuration)
	// Prepare a Raft command to get a leader lease for this replica.
	expiration := timestamp.Add(duration, 0)
	args := &proto.InternalLeaderLeaseRequest{
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
	errChan, pendingCmd := r.proposeRaftCommand(r.context(), args, &proto.InternalLeaderLeaseResponse{})
	var err error
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		err = <-pendingCmd.done
	}
	return err
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
func (r *Range) redirectOnOrAcquireLeaderLease(timestamp proto.Timestamp) error {
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
func (r *Range) WaitForLeaderLease(t util.Tester) {
	util.SucceedsWithin(t, 1*time.Second, func() error {
		return r.requestLeaderLease(r.rm.Clock().Now())
	})
}

// isInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Range) isInitialized() bool {
	return len(r.Desc().EndKey) > 0
}

// Desc atomically returns the range's descriptor.
func (r *Range) Desc() *proto.RangeDescriptor {
	return (*proto.RangeDescriptor)(atomic.LoadPointer(&r.desc))
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the range manager handle the
// descriptor update.
func (r *Range) setDesc(desc *proto.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.rm == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.rm.processRangeDescriptorUpdate(r)
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate.
func (r *Range) setDescWithoutProcessUpdate(desc *proto.RangeDescriptor) {
	atomic.StorePointer(&r.desc, unsafe.Pointer(desc))
}

// GetReplica returns the replica for this range from the range descriptor.
// Returns nil if the replica is not found.
func (r *Range) GetReplica() *proto.Replica {
	_, replica := r.Desc().FindReplica(r.rm.StoreID())
	return replica
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Range) GetMVCCStats() engine.MVCCStats {
	return r.stats.GetMVCC()
}

// ContainsKey returns whether this range contains the specified key.
func (r *Range) ContainsKey(key proto.Key) bool {
	return r.Desc().ContainsKey(keys.KeyAddress(key))
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end proto.Key) bool {
	return r.Desc().ContainsKeyRange(keys.KeyAddress(start), keys.KeyAddress(end))
}

// GetGCMetadata reads the latest GC metadata for this range.
func (r *Range) GetGCMetadata() (*proto.GCMetadata, error) {
	key := keys.RangeGCMetadataKey(r.Desc().RaftID)
	gcMeta := &proto.GCMetadata{}
	_, err := engine.MVCCGetProto(r.rm.Engine(), key, proto.ZeroTimestamp, true, nil, gcMeta)
	if err != nil {
		return nil, err
	}
	return gcMeta, nil
}

// GetLastVerificationTimestamp reads the timestamp at which the range's
// data was last verified.
func (r *Range) GetLastVerificationTimestamp() (proto.Timestamp, error) {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RaftID)
	timestamp := proto.Timestamp{}
	_, err := engine.MVCCGetProto(r.rm.Engine(), key, proto.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return proto.ZeroTimestamp, err
	}
	return timestamp, nil
}

// SetLastVerificationTimestamp writes the timestamp at which the range's
// data was last verified.
func (r *Range) SetLastVerificationTimestamp(timestamp proto.Timestamp) error {
	key := keys.RangeLastVerificationTimestampKey(r.Desc().RaftID)
	return engine.MVCCPutProto(r.rm.Engine(), nil, key, proto.ZeroTimestamp, nil, &timestamp)
}

// AddCmd adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue. If wait is false, read-write commands are added to
// Raft without waiting for their completion.
func (r *Range) AddCmd(ctx context.Context, call proto.Call, wait bool) error {
	args, reply := call.Args, call.Reply
	header := args.Header()
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		err := proto.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc())
		reply.Header().SetGoError(err)
		return err
	}

	// Differentiate between admin, read-only and read-write.
	if proto.IsAdmin(args) {
		return r.addAdminCmd(ctx, args, reply)
	} else if proto.IsReadOnly(args) {
		return r.addReadOnlyCmd(ctx, args, reply)
	}
	return r.addWriteCmd(ctx, args, reply, wait)
}

// beginCmd waits for any overlapping, already-executing commands via
// the command queue and adds itself to the queue to gate follow-on
// commands which overlap its key range. This method will block if
// there are any overlapping commands already in the queue. Returns
// the command queue insertion key, to be supplied to subsequent
// invocation of endCmd().
func (r *Range) beginCmd(header *proto.RequestHeader, readOnly bool) interface{} {
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
func (r *Range) endCmd(cmdKey interface{}, args proto.Request, err error, readOnly bool) {
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
func (r *Range) addAdminCmd(ctx context.Context, args proto.Request, reply proto.Response) error {
	// Admin commands always require the leader lease.
	if err := r.redirectOnOrAcquireLeaderLease(args.Header().Timestamp); err != nil {
		reply.Header().SetGoError(err)
		return err
	}

	switch args.(type) {
	case *proto.AdminSplitRequest:
		r.AdminSplit(args.(*proto.AdminSplitRequest), reply.(*proto.AdminSplitResponse))
	case *proto.AdminMergeRequest:
		r.AdminMerge(args.(*proto.AdminMergeRequest), reply.(*proto.AdminMergeResponse))
	default:
		return util.Error("unrecognized admin command")
	}
	return reply.Header().GoError()
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Range) addReadOnlyCmd(ctx context.Context, args proto.Request, reply proto.Response) error {
	header := args.Header()

	// If read-consistency is set to INCONSISTENT, run directly.
	if header.ReadConsistency == proto.INCONSISTENT {
		// But disallow any inconsistent reads within txns.
		if header.Txn != nil {
			reply.Header().SetGoError(util.Error("cannot allow inconsistent reads within a transaction"))
			return reply.Header().GoError()
		}
		if header.Timestamp.Equal(proto.ZeroTimestamp) {
			header.Timestamp = r.rm.Clock().Now()
		}
		intents, err := r.executeCmd(r.rm.Engine(), nil, args, reply)
		if err == nil {
			r.handleSkippedIntents(args, intents)
		}
		return err
	} else if header.ReadConsistency == proto.CONSENSUS {
		reply.Header().SetGoError(util.Error("consensus reads not implemented"))
		return reply.Header().GoError()
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	cmdKey := r.beginCmd(header, true)

	// This replica must have leader lease to process a consistent read.
	if err := r.redirectOnOrAcquireLeaderLease(args.Header().Timestamp); err != nil {
		r.endCmd(cmdKey, args, err, true /* readOnly */)
		reply.Header().SetGoError(err)
		return err
	}

	// Execute read-only command.
	intents, err := r.executeCmd(r.rm.Engine(), nil, args, reply)

	// Only update the timestamp cache if the command succeeded.
	r.endCmd(cmdKey, args, err, true /* readOnly */)

	if err == nil {
		r.handleSkippedIntents(args, intents)
	}
	return err
}

// addWriteCmd first adds the keys affected by this command as pending
// writes to the command queue. Next, the timestamp cache is checked to
// determine if any newer accesses to this command's affected keys
// have been made. If so, this command's timestamp is moved forward.
// Finally, the command is submitted to Raft. Upon completion, the
// write is removed from the read queue and the reply is added to the
// response cache.  If wait is true, will block until the command is
// complete.
func (r *Range) addWriteCmd(ctx context.Context, args proto.Request, reply proto.Response, wait bool) error {
	// Check the response cache in case this is a replay. This call
	// may block if the same command is already underway.
	header := args.Header()

	// Add the write to the command queue to gate subsequent overlapping
	// Commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	cmdKey := r.beginCmd(header, false)

	// This replica must have leader lease to process a write.
	if err := r.redirectOnOrAcquireLeaderLease(header.Timestamp); err != nil {
		r.endCmd(cmdKey, args, err, false /* !readOnly */)
		reply.Header().SetGoError(err)
		return err
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

	errChan, pendingCmd := r.proposeRaftCommand(ctx, args, reply)

	// Create a completion func for mandatory cleanups which we either
	// run synchronously if we're waiting or in a goroutine otherwise.
	completionFunc := func() error {
		// First wait for raft to commit or abort the command.
		var err error
		if err = <-errChan; err == nil {
			// Next if the command was committed, wait for the range to apply it.
			err = <-pendingCmd.done
		} else if err == multiraft.ErrGroupDeleted {
			// This error needs to be converted appropriately so that
			// clients will retry.
			err = proto.NewRangeNotFoundError(r.Desc().RaftID)
		}
		// As for reads, update timestamp cache with the timestamp
		// of this write on success. This ensures a strictly higher
		// timestamp for successive writes to the same key or key range.
		r.endCmd(cmdKey, args, err, false /* !readOnly */)
		return err
	}

	if wait {
		return completionFunc()
	}
	go func() {
		// If the original client didn't wait (e.g. resolve write intent),
		// log execution errors so they're surfaced somewhere.
		if err := completionFunc(); err != nil {
			// TODO(tschottdorf): possible security risk to log args.
			log.Warningc(ctx, "async execution of %v failed: %s", args, err)
		}
	}()
	return nil
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns the error channel and
// pending command struct for receiving.
func (r *Range) proposeRaftCommand(ctx context.Context, args proto.Request, reply proto.Response) (<-chan error, *pendingCmd) {
	pendingCmd := &pendingCmd{
		ctx:   ctx,
		Reply: reply,
		done:  make(chan error, 1),
	}
	raftCmd := proto.InternalRaftCommand{
		RaftID:       r.Desc().RaftID,
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
func (r *Range) processRaftCommand(idKey cmdIDKey, index uint64, raftCmd proto.InternalRaftCommand) error {
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
		// We initiated this command, so use the caller-supplied reply.
		reply = cmd.Reply
		ctx = cmd.ctx
	} else {
		// This command originated elsewhere so we must create a new reply buffer.
		reply = args.CreateReply()
		ctx = r.context()
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	err := r.maybeSetCorrupt(
		r.applyRaftCommand(ctx, index, proto.RaftNodeID(raftCmd.OriginNodeID), args, reply),
	)

	if cmd != nil {
		cmd.done <- err
	} else if err != nil && log.V(1) {
		log.Errorc(r.context(), "error executing raft command %s: %s", args.Method(), err)
	}

	return err
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine).
// When certain critical operations fail, a replicaCorruptionError may be
// returned and must be handled by the caller.
func (r *Range) applyRaftCommand(ctx context.Context, index uint64, originNode proto.RaftNodeID, args proto.Request, reply proto.Response) error {
	if index <= 0 {
		log.Fatalc(ctx, "raft command index is <= 0")
	}

	// If we have an out of order index, there's corruption. No sense in trying
	// to update anything or run the command. Simply return a corruption error.
	if oldIndex := atomic.LoadUint64(&r.appliedIndex); oldIndex >= index {
		return newReplicaCorruptionError(util.Errorf("applied index moved backwards: %d >= %d", oldIndex, index))
	}

	// Call the helper, which returns a batch containing data written
	// during command execution and any associated error.
	ms := engine.MVCCStats{}
	batch, rErr := r.applyRaftCommandInBatch(ctx, index, originNode, args, reply, &ms)
	// ALWAYS set the reply header error to the error returned by the
	// helper. This is the definitive result of the execution. The
	// error must be set before saving to the response cache.
	// TODO(tschottdorf,tamird) For #1400, want to refactor executeCmd to not
	// touch the reply header's error field.
	reply.Header().SetGoError(rErr)
	defer batch.Close()

	// Advance the last applied index and commit the batch.
	if err := setAppliedIndex(batch, r.Desc().RaftID, index); err != nil {
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
		// Maybe update gossip configs on a put.
		switch args.(type) {
		case *proto.PutRequest, *proto.DeleteRequest, *proto.DeleteRangeRequest:
			if key := args.Header().Key; key.Less(keys.SystemMax) {
				r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
					return bytes.HasPrefix(key, configPrefix)
				})
			}
		}
	}

	return rErr
}

// applyRaftCommandInBatch executes the command in a batch engine and
// returns the batch containing the results. The caller is responsible
// for committing the batch, even on error.
func (r *Range) applyRaftCommandInBatch(ctx context.Context, index uint64, originNode proto.RaftNodeID,
	args proto.Request, reply proto.Response, ms *engine.MVCCStats) (engine.Engine, error) {
	// Create a new batch for the command to ensure all or nothing semantics.
	batch := r.rm.Engine().NewBatch()

	if lease := r.getLease(); args.Method() != proto.InternalLeaderLease &&
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
		return batch, r.newNotLeaderError(lease, originNode)
	}

	// Check the response cache to ensure idempotency.
	if proto.IsWrite(args) {
		if ok, err := r.respCache.GetResponse(batch, args.Header().CmdID, reply); err != nil {
			// Any error encountered while fetching the response cache entry means corruption.
			return batch, newReplicaCorruptionError(util.Errorf("could not read from response cache"), err)
		} else if ok {
			if log.V(1) {
				log.Infoc(ctx, "found response cache entry for %+v", args.Header().CmdID)
			}
			// We successfully read from the response cache, so return whatever error
			// was present in the cached entry (if any).
			return batch, reply.Header().GoError()
		}
	}

	// Execute the command.
	intents, rErr := r.executeCmd(batch, ms, args, reply)
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
		if err := r.respCache.PutResponse(batch, args.Header().CmdID, reply); err != nil {
			log.Fatalc(ctx, "putting a response cache entry in a batch should never fail: %s", err)
		}
	}

	// If the execution of the command wasn't successful, stop here.
	if rErr != nil {
		return batch, rErr
	}

	// On success and only on the replica on which this command originated,
	// resolve skipped intents asynchronously.
	if originNode == r.rm.RaftNodeID() {
		r.handleSkippedIntents(args, intents)
	}

	return batch, nil
}

// getLeaseForGossip tries to obtain a leader lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Range) getLeaseForGossip(ctx context.Context) (bool, error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.rm.Gossip() == nil || !r.isInitialized() {
		return false, util.Errorf("no gossip or range not initialized")
	}
	if !r.rm.Stopper().StartTask() {
		return false, util.Errorf("system is shutting down")
	}
	defer r.rm.Stopper().FinishTask()

	timestamp := r.rm.Clock().Now()

	// Check for or obtain the lease, if none active.
	err := r.redirectOnOrAcquireLeaderLease(timestamp)
	if err != nil {
		switch e := err.(type) {
		// NotLeaderError means there is an active lease, leaseRejectedError
		// means we tried to get one but someone beat us to it.
		case *proto.NotLeaderError, *proto.LeaseRejectedError:
		default:
			// Any other error is worth being logged visibly.
			log.Warningc(ctx, "could not acquire lease for range gossip: %s", e)
			return false, err
		}
	}
	return err == nil, nil
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a leader lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Range) maybeGossipFirstRange() error {
	if !r.IsFirstRange() {
		return nil
	}

	ctx := r.context()

	// Gossip the cluster ID from all replicas of the first range.
	log.Infoc(ctx, "gossiping cluster id %s from store %d, range %d", r.rm.ClusterID(),
		r.rm.StoreID(), r.Desc().RaftID)
	if err := r.rm.Gossip().AddInfo(gossip.KeyClusterID, r.rm.ClusterID(), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}

	if ok, err := r.getLeaseForGossip(ctx); !ok || err != nil {
		return err
	}
	log.Infoc(ctx, "gossiping sentinel from store %d, range %d", r.rm.StoreID(), r.Desc().RaftID)
	if err := r.rm.Gossip().AddInfo(gossip.KeySentinel, r.rm.ClusterID(), clusterIDGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}
	log.Infoc(ctx, "gossiping first range from store %d, range %d", r.rm.StoreID(), r.Desc().RaftID)
	if err := r.rm.Gossip().AddInfo(gossip.KeyFirstRangeDescriptor, *r.Desc(), configGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip first range metadata: %s", err)
	}
	return nil
}

// maybeGossipConfigs gossips those configuration maps for which the supplied
// function returns true and whose contents are marked dirty. Configuration
// maps include accounting, permissions, and zones. The store is in charge of
// the initial update, and the range itself re-triggers updates following
// writes that may have altered any of the maps.
//
// Note that maybeGossipConfigs does not check the leader lease; it is called
// on only when the lease is actually held.
// TODO(tschottdorf): The main reason this method does not try to get the lease
// is that InternalLeaderLease calls it, which means that we would wind up
// deadlocking in redirectOnOrObtainLeaderLease. Can possibly simplify.
func (r *Range) maybeGossipConfigs(match func(proto.Key) bool) {
	r.Lock()
	defer r.Unlock()
	r.maybeGossipConfigsLocked(match)
}

func (r *Range) maybeGossipConfigsLocked(match func(configPrefix proto.Key) bool) {
	if r.rm.Gossip() == nil || !r.isInitialized() {
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
				log.Infoc(ctx, "gossiping %s config from store %d, range %d", cd.gossipKey, r.rm.StoreID(), r.Desc().RaftID)
				if err := r.rm.Gossip().AddInfo(cd.gossipKey, configMap, 0*time.Second); err != nil {
					log.Errorc(ctx, "failed to gossip %s configMap: %s", cd.gossipKey, err)
				}
			}
		}
	}
}

func (r *Range) handleSkippedIntents(args proto.Request, intents []proto.Intent) {
	if len(intents) == 0 {
		return
	}

	ctx := r.context()
	if stopper := r.rm.Stopper(); stopper.StartTask() {
		go func() {
			err := r.rm.resolveWriteIntentError(ctx, &proto.WriteIntentError{
				Intents: intents,
			}, r, args, proto.CLEANUP_TXN, true /* wait */)
			if wiErr, ok := err.(*proto.WriteIntentError); !ok || wiErr == nil || !wiErr.Resolved {
				log.Warningc(ctx, "failed to resolve on inconsistent read: %s", err)
			}
			stopper.FinishTask()
		}()
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
func (r *Range) maybeSetCorrupt(err error) error {
	if cErr, ok := err.(*replicaCorruptionError); ok && cErr != nil {
		log.Errorc(r.context(), "stalling replica due to: %s", cErr.error)
		cErr.processed = true
		return cErr
	}
	return err
}

// loadConfigMap scans the config entries under keyPrefix and
// instantiates/returns a config map and its sha256 hash. Prefix
// configuration maps include accounting, permissions, and zones.
func loadConfigMap(eng engine.Engine, keyPrefix proto.Key, configI interface{}) (PrefixConfigMap, []byte, error) {
	// TODO(tschottdorf): Currently this does not handle intents well.
	kvs, _, err := engine.MVCCScan(eng, keyPrefix, keyPrefix.PrefixEnd(), 0, proto.MaxTimestamp, true /* consistent */, nil)
	if err != nil {
		return nil, nil, err
	}
	var configs []*PrefixConfig
	sha := sha256.New()
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// proto encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, config); err != nil {
			return nil, nil, util.Errorf("unable to unmarshal config key %s: %s", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
		sha.Write(kv.Value.Bytes)
	}
	m, err := NewPrefixConfigMap(configs)
	return m, sha.Sum(nil), err
}

// maybeAddToSplitQueue checks whether the current size of the range
// exceeds the max size specified in the zone config. If yes, the
// range is added to the split queue.
func (r *Range) maybeAddToSplitQueue() {
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && r.stats.KeyBytes+r.stats.ValBytes > maxBytes {
		r.rm.splitQueue().MaybeAdd(r, r.rm.Clock().Now())
	}
}
