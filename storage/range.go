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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
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

var (
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
	// continually re-gossiped. The replica which is the raft leader of
	// the first range gossips it.
	ttlClusterIDGossip = 30 * time.Second
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
	raftInitialLogIndex        = 10
	raftInitialLogTerm         = 5
	defaultLeaderLeaseDuration = time.Second
)

// configDescriptor describes administrative configuration maps
// affecting ranges of the key-value map by key prefix.
type configDescriptor struct {
	keyPrefix proto.Key   // Range key prefix
	gossipKey string      // Gossip key
	configI   interface{} // Config struct interface
}

// configDescriptors is a slice containing the accounting, permissions
// and zone configuration descriptors.
var configDescriptors = []*configDescriptor{
	{engine.KeyConfigAccountingPrefix, gossip.KeyConfigAccounting, proto.AcctConfig{}},
	{engine.KeyConfigPermissionPrefix, gossip.KeyConfigPermission, proto.PermConfig{}},
	{engine.KeyConfigZonePrefix, gossip.KeyConfigZone, proto.ZoneConfig{}},
}

// tsCacheMethods specifies the set of methods which affect the
// timestamp cache.
var tsCacheMethods = [...]bool{
	proto.Contains:              true,
	proto.Get:                   true,
	proto.Put:                   true,
	proto.ConditionalPut:        true,
	proto.Increment:             true,
	proto.Scan:                  true,
	proto.Delete:                true,
	proto.DeleteRange:           true,
	proto.InternalResolveIntent: true,
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
	done  chan error // Used to signal waiting RPC handler
}

// A RangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for splitting.
type RangeManager interface {
	// Accessors for shared state.
	ClusterID() string
	StoreID() proto.StoreID
	RaftNodeID() multiraft.NodeID
	Clock() *hlc.Clock
	Engine() engine.Engine
	DB() *client.KV
	Allocator() *allocator
	Gossip() *gossip.Gossip
	SplitQueue() *splitQueue
	Stopper() *util.Stopper
	EventFeed() StoreEventFeed

	// Range manipulation methods.
	AddRange(rng *Range) error
	LookupRange(start, end proto.Key) *Range
	MergeRange(subsumingRng *Range, updatedEndKey proto.Key, subsumedRaftID int64) error
	NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error)
	NewSnapshot() engine.Engine
	ProposeRaftCommand(cmdIDKey, proto.InternalRaftCommand) <-chan error
	RemoveRange(rng *Range) error
	SplitRange(origRng, newRng *Range) error
}

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
	desc     unsafe.Pointer // Atomic pointer for *proto.RangeDescriptor
	rm       RangeManager   // Makes some store methods available
	stats    *rangeStats    // Range statistics
	maxBytes int64          // Max bytes before split.
	// Held while a split, merge, or replica change is underway.
	metaLock sync.Mutex
	// Last index persisted to the raft log (not necessarily committed).
	// Updated atomically.
	lastIndex uint64
	// Last index applied to the state machine. Updated atomically.
	appliedIndex uint64
	lease        unsafe.Pointer // Information for leader lease, updated atomically
	llMu         sync.Mutex     // Synchronizes readers' requests for leader lease

	sync.RWMutex                 // Protects the following fields:
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	respCache    *ResponseCache  // Provides idempotence for retries
	pendingCmds  map[cmdIDKey]*pendingCmd
}

// NewRange initializes the range using the given metadata.
func NewRange(desc *proto.RangeDescriptor, rm RangeManager) (*Range, error) {
	r := &Range{
		rm:          rm,
		cmdQ:        NewCommandQueue(),
		tsCache:     NewTimestampCache(rm.Clock()),
		respCache:   NewResponseCache(desc.RaftID, rm.Engine()),
		pendingCmds: map[cmdIDKey]*pendingCmd{},
	}
	r.SetDesc(desc)

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
	return fmt.Sprintf("range=%d (%s-%s)", r.Desc().RaftID, r.Desc().StartKey, r.Desc().EndKey)
}

// start begins by gossiping the configs. It also gossips the sentinel if we're
// the first range (the store will do this regularly, but this is a good time
// to get started quickly).
func (r *Range) start() {
	// Being the first range comes with extra responsibility for the gossip
	// sentinel and first range metadata.
	// TODO(tschottdorf) really want something more streamlined, such as:
	//
	// if r.rm.Stopper().StartTask() {
	// 	go func() {
	// 		r.maybeGossipFirstRangeWithLease()
	// 		r.rm.Stopper().FinishTask()
	// 	}()
	// }
	//
	// but
	// a) in some tests that goroutine gets stuck acquiring the lease, probably
	//    due to Raft losing the command
	// b) other times, the lease fails because the MultiRaft group doesn't exist
	//    yet
	// b) it complicates testing because there's little way of knowing who
	//    will get the lease first, and many tests are too low level to do
	//    the appropriate retries.
	//
	// Instead we do this for now:
	if desc := r.Desc(); desc != nil && len(desc.Replicas) == 1 && r.IsFirstRange() {
		r.gossipFirstRange()
	}

	r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
		return r.ContainsKey(configPrefix)
	})
}

// Destroy cleans up all data associated with this range.
func (r *Range) Destroy() error {
	iter := newRangeDataIterator(r, r.rm.Engine())
	defer iter.Close()
	batch := r.rm.Engine().NewBatch()
	defer batch.Close()
	for ; iter.Valid(); iter.Next() {
		_ = batch.Clear(iter.Key())
	}
	return batch.Commit()
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
	return bytes.Equal(r.Desc().StartKey, engine.KeyMin)
}

// getLease returns the current leader lease.
func (r *Range) getLease() *proto.Lease {
	return (*proto.Lease)(atomic.LoadPointer(&r.lease))
}

// newNotLeaderError returns a NotLeaderError intialized with the
// replica for the last known holder of the leader lease.
func (r *Range) newNotLeaderError() error {
	err := &proto.NotLeaderError{}
	if l := r.getLease(); l.RaftNodeID != 0 {
		_, err.Replica = r.Desc().FindReplica(r.rm.StoreID())
		_, storeID := DecodeRaftNodeID(multiraft.NodeID(l.RaftNodeID))
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
	duration := int64(defaultLeaderLeaseDuration)
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
			RaftNodeID: uint64(r.rm.RaftNodeID()),
		},
	}
	// Send lease request directly to raft in order to skip unnecessary
	// checks from normal request machinery, (e.g. the command queue).
	errChan, pendingCmd := r.proposeRaftCommand(args, &proto.InternalLeaderLeaseResponse{})
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
func (r *Range) redirectOnOrAcquireLeaderLease(args proto.Request) error {
	r.llMu.Lock()
	defer r.llMu.Unlock()
	// If lease is currently held by another, redirect to holder.
	timestamp := args.Header().Timestamp
	if held, expired := r.HasLeaderLease(timestamp); !held && !expired {
		return r.newNotLeaderError()
	} else if !held || expired {
		// Otherwise, if not held by this replica or expired, request renewal.
		if err := r.requestLeaderLease(timestamp); err != nil {
			return util.Errorf("failed to acquire lease: %s", err)
		}
	}
	return nil
}

// verifyLeaderLease checks whether the requesting replica (by raft
// node ID) holds the leader lease covering the specified timestamp.
func (r *Range) verifyLeaderLease(originRaftNodeID multiraft.NodeID, timestamp proto.Timestamp) bool {
	l := r.getLease()
	return uint64(originRaftNodeID) == l.RaftNodeID && timestamp.Less(l.Expiration)
}

// HasLeaderLease returns whether this replica holds or was the last
// holder of the leader lease, and whether the lease has expired.
// Leases may not overlap, and a gap between successive lease holders
// is expected.
func (r *Range) HasLeaderLease(timestamp proto.Timestamp) (bool, bool) {
	if l := r.getLease(); l.RaftNodeID != 0 {
		held := l.RaftNodeID == uint64(r.rm.RaftNodeID())
		expired := !timestamp.Less(l.Expiration)
		return held, expired
	}
	// The lease has never been held.
	return false, true
}

// WaitForLeaderLease is used from unittests to wait until this range
// has the leader lease.
func (r *Range) WaitForLeaderLease(t *testing.T) {
	util.SucceedsWithin(t, 1*time.Second, func() error {
		return r.requestLeaderLease(r.rm.Clock().Now())
	})
}

// isInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Range) isInitialized() bool {
	if desc := r.Desc(); desc != nil && len(desc.EndKey) > 0 {
		return true
	}
	return false
}

// Desc atomically returns the range's descriptor.
func (r *Range) Desc() *proto.RangeDescriptor {
	return (*proto.RangeDescriptor)(atomic.LoadPointer(&r.desc))
}

// SetDesc atomically sets the range's descriptor. This method should
// be called in the context of having metaLock held, as is the case
// for merging, splitting and updating the replica set.
func (r *Range) SetDesc(desc *proto.RangeDescriptor) {
	atomic.StorePointer(&r.desc, unsafe.Pointer(desc))
}

// GetReplica returns the replica for this range from the range descriptor.
// A fatal error occurs if the replica is not found.
func (r *Range) GetReplica() *proto.Replica {
	_, replica := r.Desc().FindReplica(r.rm.StoreID())
	if replica == nil {
		log.Fatalf("own replica missing in range at store %d", r.rm.StoreID())
	}
	return replica
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Range) GetMVCCStats() proto.MVCCStats {
	return r.stats.GetMVCC()
}

// ContainsKey returns whether this range contains the specified key.
// Read-lock the mutex to protect access to Desc, which might be changed
// concurrently via range split.
func (r *Range) ContainsKey(key proto.Key) bool {
	return r.Desc().ContainsKey(engine.KeyAddress(key))
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end proto.Key) bool {
	return r.Desc().ContainsKeyRange(engine.KeyAddress(start), engine.KeyAddress(end))
}

// GetGCMetadata reads the latest GC metadata for this range.
func (r *Range) GetGCMetadata() (*proto.GCMetadata, error) {
	key := engine.RangeGCMetadataKey(r.Desc().RaftID)
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
	key := engine.RangeLastVerificationTimestampKey(r.Desc().RaftID)
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
	key := engine.RangeLastVerificationTimestampKey(r.Desc().RaftID)
	return engine.MVCCPutProto(r.rm.Engine(), nil, key, proto.ZeroTimestamp, nil, &timestamp)
}

// AddCmd adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue. If wait is false, read-write commands are added to
// Raft without waiting for their completion.
func (r *Range) AddCmd(args proto.Request, reply proto.Response, wait bool) error {
	// Differentiate between admin, read-only and read-write.
	if proto.IsAdmin(args) {
		return r.addAdminCmd(args, reply)
	} else if proto.IsReadOnly(args) {
		return r.addReadOnlyCmd(args, reply)
	}
	return r.addReadWriteCmd(args, reply, wait)
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
		r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, header.Txn.MD5(), readOnly)
	}
	r.cmdQ.Remove(cmdKey)
	r.Unlock()
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the leader replica.
func (r *Range) addAdminCmd(args proto.Request, reply proto.Response) error {
	// Admin commands always require the leader lease.
	if err := r.redirectOnOrAcquireLeaderLease(args); err != nil {
		reply.Header().SetGoError(err)
		return err
	}

	switch args.(type) {
	case *proto.AdminSplitRequest:
		r.AdminSplit(args.(*proto.AdminSplitRequest), reply.(*proto.AdminSplitResponse))
	case *proto.AdminMergeRequest:
		r.AdminMerge(args.(*proto.AdminMergeRequest), reply.(*proto.AdminMergeResponse))
	default:
		return util.Errorf("unrecognized admin command type: %s", args.Method())
	}
	return reply.Header().GoError()
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Range) addReadOnlyCmd(args proto.Request, reply proto.Response) error {
	header := args.Header()

	// If read-consistency is set to INCONSISTENT, run directly.
	if header.ReadConsistency == proto.INCONSISTENT {
		// But disallow any inconsistent reads within txns.
		if header.Txn != nil {
			reply.Header().SetGoError(util.Errorf("cannot allow inconsistent reads within a transaction"))
			return reply.Header().GoError()
		}
		if header.Timestamp.Equal(proto.ZeroTimestamp) {
			header.Timestamp = r.rm.Clock().Now()
		}
		return r.executeCmd(r.rm.Engine(), nil, args, reply)
	} else if header.ReadConsistency == proto.CONSENSUS {
		reply.Header().SetGoError(util.Errorf("consensus reads not implemented"))
		return reply.Header().GoError()
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	cmdKey := r.beginCmd(header, true)

	// This replica must have leader lease to process a consistent read.
	if err := r.redirectOnOrAcquireLeaderLease(args); err != nil {
		r.endCmd(cmdKey, args, err, true /* readOnly */)
		reply.Header().SetGoError(err)
		return err
	}

	// Execute read-only command.
	err := r.executeCmd(r.rm.Engine(), nil, args, reply)

	// Only update the timestamp cache if the command succeeded.
	r.endCmd(cmdKey, args, err, true /* readOnly */)

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
func (r *Range) addReadWriteCmd(args proto.Request, reply proto.Response, wait bool) error {
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
	if err := r.redirectOnOrAcquireLeaderLease(args); err != nil {
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
		rTS, wTS := r.tsCache.GetMax(header.Key, header.EndKey, header.Txn.MD5())
		r.Unlock()

		// Always push the timestamp forward if there's been a read which
		// occurred after our txn timestamp.
		if !rTS.Less(header.Timestamp) {
			header.Timestamp = rTS.Next()
		}
		// If there's a newer write timestamp...
		if !wTS.Less(header.Timestamp) {
			// If we're in a txn, set a write too old error in reply. We
			// still go ahead and try the write because we want to avoid
			// restarting the transaction in the event that there isn't an
			// intent or the intent can be pushed by us.
			if header.Txn != nil {
				err := &proto.WriteTooOldError{Timestamp: header.Timestamp, ExistingTimestamp: wTS}
				reply.Header().SetGoError(err)
			} else {
				// Otherwise, make sure we advance the request's timestamp.
				header.Timestamp = wTS.Next()
			}
		}
	}

	errChan, pendingCmd := r.proposeRaftCommand(args, reply)

	// Create a completion func for mandatory cleanups which we either
	// run synchronously if we're waiting or in a goroutine otherwise.
	completionFunc := func() error {
		// First wait for raft to commit or abort the command.
		var err error
		if err = <-errChan; err == nil {
			// Next if the command was committed, wait for the range to apply it.
			err = <-pendingCmd.done
		}
		// As for reads, update timestamp cache with the timestamp
		// of this write on success. This ensures a strictly higher
		// timestamp for successive writes to the same key or key range.
		r.endCmd(cmdKey, args, err, false /* !readOnly */)

		// If the original client didn't wait (e.g. resolve write intent),
		// log execution errors so they're surfaced somewhere.
		if !wait && err != nil {
			log.Warningf("non-synchronous execution of %s with %+v failed: %s",
				args.Method(), args, err)
		}
		return err
	}

	if wait {
		return completionFunc()
	}
	go completionFunc()
	return nil
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns the error channel and
// pending command struct for receiving.
func (r *Range) proposeRaftCommand(args proto.Request, reply proto.Response) (<-chan error, *pendingCmd) {
	pendingCmd := &pendingCmd{
		Reply: reply,
		done:  make(chan error, 1),
	}
	raftCmd := proto.InternalRaftCommand{
		RaftID:       r.Desc().RaftID,
		OriginNodeID: uint64(r.rm.RaftNodeID()),
	}
	cmdID := args.Header().GetOrCreateCmdID(r.rm.Clock().PhysicalNow())
	ok := raftCmd.Cmd.SetValue(args)
	if !ok {
		log.Fatalf("unknown command type %T", args)
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
//
// TODO(spencer): Differentiate between errors caused by the normal culprits --
// bad inputs from clients, stale information, etc. and errors which might
// cause the range replicas to diverge -- running out of disk space, underlying
// rocksdb corruption, etc. Do a careful code audit to make sure we identify
// errors which should be classified as a ReplicaCorruptionError--when those
// bubble up to the point where we've just tried to execute a Raft command, the
// Raft replica would need to stall itself.
func (r *Range) processRaftCommand(idKey cmdIDKey, index uint64, raftCmd proto.InternalRaftCommand) error {
	if index == 0 {
		log.Fatal("processRaftCommand requires a non-zero index")
	}
	r.Lock()
	cmd := r.pendingCmds[idKey]
	delete(r.pendingCmds, idKey)
	r.Unlock()

	args := raftCmd.Cmd.GetValue().(proto.Request)
	var reply proto.Response
	if cmd != nil {
		// We initiated this command, so use the caller-supplied reply.
		reply = cmd.Reply
	} else {
		// This command originated elsewhere so we must create a new reply buffer.
		reply = args.CreateReply()
	}

	err := r.applyRaftCommand(index, multiraft.NodeID(raftCmd.OriginNodeID), args, reply)

	if cmd != nil {
		cmd.done <- err
	} else if err != nil {
		log.Errorf("error executing raft command %s: %s", args.Method(), err)
	}
	return err
}

// applyRaftCommand applies a raft command from the replicated log to
// the underlying state machine (i.e. the engine).
func (r *Range) applyRaftCommand(index uint64, originNodeID multiraft.NodeID, args proto.Request, reply proto.Response) error {
	if index <= 0 {
		log.Fatalf("raft command index is <= 0")
	}

	committed := false
	defer func() {
		if !committed {
			// We didn't commit the batch, but advance the last applied index nonetheless.
			if err := setAppliedIndex(r.rm.Engine(), r.Desc().RaftID, index); err != nil {
				log.Fatalf("could not advance applied index: %s", err)
			}
			atomic.StoreUint64(&r.appliedIndex, index)
		}
	}()

	header := args.Header()

	// Check the response cache to ensure idempotency.
	if proto.IsWrite(args) {
		if ok, err := r.respCache.GetResponse(header.CmdID, reply); ok && err == nil {
			log.V(1).Infof("found response cache entry for %+v", args.Header().CmdID)
			return reply.Header().GoError()
		} else if ok && err != nil {
			newErr := util.Errorf("unable to read result for %+v from the response cache: %s", args, err)
			reply.Header().SetGoError(newErr)
			return newErr
		}
	}

	// Verify the leader lease is held; Note that we don't require the
	// leader lease when trying to grant the leader lease!
	if _, ok := args.(*proto.InternalLeaderLeaseRequest); !ok {
		if !r.verifyLeaderLease(originNodeID, header.Timestamp) {
			err := r.newNotLeaderError()
			reply.Header().SetGoError(err)
			return err
		}
	}

	// Create a new batch for the command to ensure all or nothing semantics.
	batch := r.rm.Engine().NewBatch()
	defer batch.Close()

	// Create an proto.MVCCStats instance.
	ms := proto.MVCCStats{}

	// Execute the command; the error will also be set in the reply header.
	err := r.executeCmd(batch, &ms, args, reply)

	if oldIndex := atomic.LoadUint64(&r.appliedIndex); oldIndex >= index {
		log.Fatalf("applied index moved backwards: %d >= %d", oldIndex, index)
	}

	// Advance the applied index atomically within the batch.
	if e := setAppliedIndex(batch, r.Desc().RaftID, index); e != nil {
		if reply.Header().GoError() == nil {
			reply.Header().SetGoError(e)
		}
		err = e
	}

	if err == nil && proto.IsWrite(args) {
		// On success, flush the MVCC stats to the batch and commit.
		r.stats.MergeMVCCStats(batch, &ms, header.Timestamp.WallTime)
		if err := batch.Commit(); err != nil {
			log.Fatalf("failed to commit batch from Raft command execution: %s", err)
		}
		committed = true
		// Publish update to event feed.
		r.rm.EventFeed().updateRange(r, args.Method(), &ms)
		// After successful commit, update cached stats and appliedIndex value.
		atomic.StoreUint64(&r.appliedIndex, index)
		r.stats.Update(ms)
		// If the commit succeeded, potentially add range to split queue.
		r.maybeAddToSplitQueue()
		// Maybe update gossip configs on a put.
		switch args.(type) {
		case *proto.PutRequest, *proto.DeleteRequest, *proto.DeleteRangeRequest:
			if header.Key.Less(engine.KeySystemMax) {
				r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
					return bytes.HasPrefix(header.Key, configPrefix)
				})
			}
		}
	}

	// Add this command's result to the response cache if this is a
	// read/write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence when leadership changes.
	if proto.IsWrite(args) {
		if putErr := r.respCache.PutResponse(args.Header().CmdID, reply); putErr != nil {
			log.Errorf("unable to write result of %+v: %+v to the response cache: %s",
				args, reply, putErr)
		}
	}

	return reply.Header().GoError()
}

// maybeGossipFirstRangeWithLease is periodically called and gossips the
// sentinel and first range metadata if the range has the lease and is the
// first range. Since one replica in the first range should always gossip this
// information, a lease is acquired if there is no active lease.
func (r *Range) maybeGossipFirstRangeWithLease() {
	// If no Gossip available (some tests) or not first range: nothing to do.
	if r.rm.Gossip() == nil || !r.isInitialized() || !r.IsFirstRange() {
		return
	}
	timestamp := r.rm.Clock().Now()
	held, shouldGetLease := r.HasLeaderLease(timestamp)

	// Someone has an active lease; they'll do the work, not us.
	if !held && !shouldGetLease {
		return
	}

	if shouldGetLease {
		// We don't have an active lease, but we think we should be
		// gossiping. Let's get the lease and then do just that.
		if err := r.requestLeaderLease(timestamp); err != nil {
			log.Infof("could not acquire leader lease for first range gossip: %s", err)
			return
		}
	}

	r.gossipFirstRange()
}

// gossipFirstRange adds the sentinel and first range metadata to gossip.
func (r *Range) gossipFirstRange() {
	if r.rm.Gossip() == nil {
		return
	}
	if err := r.rm.Gossip().AddInfo(gossip.KeyClusterID, r.rm.ClusterID(), ttlClusterIDGossip); err != nil {
		log.Errorf("failed to gossip cluster ID %s: %s", r.rm.ClusterID(), err)
	}

	if err := r.rm.Gossip().AddInfo(gossip.KeyFirstRangeDescriptor, *r.Desc(), 0*time.Second); err != nil {
		log.Errorf("failed to gossip first range metadata: %s", err)
	}
}

// maybeGossipConfigs gossips configuration maps if their data falls
// within the range, and their contents are marked dirty.
// Configuration maps include accounting, permissions, and zones.
func (r *Range) maybeGossipConfigs(match func(configPrefix proto.Key) bool) {
	if r.rm.Gossip() == nil {
		return
	}
	held, expired := r.HasLeaderLease(r.rm.Clock().Now())
	if r.getLease().RaftNodeID == 0 || (held && !expired) {
		for _, cd := range configDescriptors {
			if match(cd.keyPrefix) {
				// Check for a bad range split. This should never happen as ranges
				// cannot be split mid-config.
				if !r.ContainsKey(cd.keyPrefix.PrefixEnd()) {
					log.Fatalf("range splits configuration values for %s", cd.keyPrefix)
				}
				configMap, err := r.loadConfigMap(cd.keyPrefix, cd.configI)
				if err != nil {
					log.Errorf("failed loading %s config map: %s", cd.gossipKey, err)
					continue
				} else {
					if err := r.rm.Gossip().AddInfo(cd.gossipKey, configMap, 0*time.Second); err != nil {
						log.Errorf("failed to gossip %s configMap: %s", cd.gossipKey, err)
						continue
					}
				}
			}
		}
	}
}

// loadConfigMap scans the config entries under keyPrefix and
// instantiates/returns a config map. Prefix configuration maps
// include accounting, permissions, and zones.
func (r *Range) loadConfigMap(keyPrefix proto.Key, configI interface{}) (PrefixConfigMap, error) {
	kvs, err := engine.MVCCScan(r.rm.Engine(), keyPrefix, keyPrefix.PrefixEnd(), 0, proto.MaxTimestamp, true, nil)
	if err != nil {
		return nil, err
	}
	var configs []*PrefixConfig
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// proto encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, config); err != nil {
			return nil, util.Errorf("unable to unmarshal config key %s: %s", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
	}
	return NewPrefixConfigMap(configs)
}

// maybeAddToSplitQueue checks whether the current size of the range
// exceeds the max size specified in the zone config. If yes, the
// range is added to the split queue.
func (r *Range) maybeAddToSplitQueue() {
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && r.stats.KeyBytes+r.stats.ValBytes > maxBytes {
		r.rm.SplitQueue().MaybeAdd(r, r.rm.Clock().Now())
	}
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
func (r *Range) executeCmd(batch engine.Engine, ms *proto.MVCCStats, args proto.Request, reply proto.Response) error {
	// Verify key is contained within range here to catch any range split
	// or merge activity.
	header := args.Header()
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		err := proto.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc())
		reply.Header().SetGoError(err)
		return err
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if TestingCommandFilter != nil && TestingCommandFilter(args, reply) {
		return reply.Header().GoError()
	}

	switch args.(type) {
	case *proto.ContainsRequest:
		r.Contains(batch, args.(*proto.ContainsRequest), reply.(*proto.ContainsResponse))
	case *proto.GetRequest:
		r.Get(batch, args.(*proto.GetRequest), reply.(*proto.GetResponse))
	case *proto.PutRequest:
		r.Put(batch, ms, args.(*proto.PutRequest), reply.(*proto.PutResponse))
	case *proto.ConditionalPutRequest:
		r.ConditionalPut(batch, ms, args.(*proto.ConditionalPutRequest), reply.(*proto.ConditionalPutResponse))
	case *proto.IncrementRequest:
		r.Increment(batch, ms, args.(*proto.IncrementRequest), reply.(*proto.IncrementResponse))
	case *proto.DeleteRequest:
		r.Delete(batch, ms, args.(*proto.DeleteRequest), reply.(*proto.DeleteResponse))
	case *proto.DeleteRangeRequest:
		r.DeleteRange(batch, ms, args.(*proto.DeleteRangeRequest), reply.(*proto.DeleteRangeResponse))
	case *proto.ScanRequest:
		r.Scan(batch, args.(*proto.ScanRequest), reply.(*proto.ScanResponse))
	case *proto.EndTransactionRequest:
		r.EndTransaction(batch, ms, args.(*proto.EndTransactionRequest), reply.(*proto.EndTransactionResponse))
	case *proto.InternalRangeLookupRequest:
		r.InternalRangeLookup(batch, args.(*proto.InternalRangeLookupRequest), reply.(*proto.InternalRangeLookupResponse))
	case *proto.InternalHeartbeatTxnRequest:
		r.InternalHeartbeatTxn(batch, args.(*proto.InternalHeartbeatTxnRequest), reply.(*proto.InternalHeartbeatTxnResponse))
	case *proto.InternalGCRequest:
		r.InternalGC(batch, ms, args.(*proto.InternalGCRequest), reply.(*proto.InternalGCResponse))
	case *proto.InternalPushTxnRequest:
		r.InternalPushTxn(batch, args.(*proto.InternalPushTxnRequest), reply.(*proto.InternalPushTxnResponse))
	case *proto.InternalResolveIntentRequest:
		r.InternalResolveIntent(batch, ms, args.(*proto.InternalResolveIntentRequest), reply.(*proto.InternalResolveIntentResponse))
	case *proto.InternalMergeRequest:
		r.InternalMerge(batch, ms, args.(*proto.InternalMergeRequest), reply.(*proto.InternalMergeResponse))
	case *proto.InternalTruncateLogRequest:
		r.InternalTruncateLog(batch, ms, args.(*proto.InternalTruncateLogRequest), reply.(*proto.InternalTruncateLogResponse))
	case *proto.InternalLeaderLeaseRequest:
		r.InternalLeaderLease(batch, ms, args.(*proto.InternalLeaderLeaseRequest), reply.(*proto.InternalLeaderLeaseResponse))
	default:
		return util.Errorf("unrecognized command %s", args.Method())
	}

	log.V(1).Infof("executed %s command %+v: %+v", args.Method(), args, reply)

	// Update the node clock with the serviced request. This maintains a
	// high water mark for all ops serviced, so that received ops
	// without a timestamp specified are guaranteed one higher than any
	// op already executed for overlapping keys.
	_, err := r.rm.Clock().Update(header.Timestamp)
	if err != nil {
		log.Errorf("executed a command with excessive future clock offset: %s", err)
	}

	// Propagate the request timestamp (which may have changed).
	reply.Header().Timestamp = header.Timestamp

	// A ReadUncertaintyIntervalError contains the timestamp of the value
	// that provoked the conflict. However, we forward the timestamp to the
	// node's time here. The reason is that the caller (which is always
	// transactional when this error occurs) in our implementation wants to
	// use this information to extract a timestamp after which reads from
	// the nodes are causally consistent with the transaction. This allows
	// the node to be classified as without further uncertain reads for the
	// remainder of the transaction.
	// See the comment on proto.Transaction.CertainNodes.
	if err, ok := reply.Header().GoError().(*proto.ReadWithinUncertaintyIntervalError); ok {
		// Note that we can use this node's clock (which may be different from
		// other replicas') because this error attaches the existing timestamp
		// to the node itself when retrying.
		err.ExistingTimestamp.Forward(r.rm.Clock().Now())
	}

	// Return the error (if any) set in the reply.
	return reply.Header().GoError()
}

// Contains verifies the existence of a key in the key value store.
func (r *Range) Contains(batch engine.Engine, args *proto.ContainsRequest, reply *proto.ContainsResponse) {
	val, err := engine.MVCCGet(batch, args.Key, args.Timestamp, args.ReadConsistency == proto.CONSISTENT, args.Txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if val != nil {
		reply.Exists = true
	}
}

// Get returns the value for a specified key.
func (r *Range) Get(batch engine.Engine, args *proto.GetRequest, reply *proto.GetResponse) {
	val, err := engine.MVCCGet(batch, args.Key, args.Timestamp, args.ReadConsistency == proto.CONSISTENT, args.Txn)
	reply.Value = val
	reply.SetGoError(err)
}

// Put sets the value for a specified key.
func (r *Range) Put(batch engine.Engine, ms *proto.MVCCStats, args *proto.PutRequest, reply *proto.PutResponse) {
	err := engine.MVCCPut(batch, ms, args.Key, args.Timestamp, args.Value, args.Txn)
	reply.SetGoError(err)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Range) ConditionalPut(batch engine.Engine, ms *proto.MVCCStats, args *proto.ConditionalPutRequest, reply *proto.ConditionalPutResponse) {
	err := engine.MVCCConditionalPut(batch, ms, args.Key, args.Timestamp, args.Value, args.ExpValue, args.Txn)
	reply.SetGoError(err)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Range) Increment(batch engine.Engine, ms *proto.MVCCStats, args *proto.IncrementRequest, reply *proto.IncrementResponse) {
	val, err := engine.MVCCIncrement(batch, ms, args.Key, args.Timestamp, args.Txn, args.Increment)
	reply.NewValue = val
	reply.SetGoError(err)
}

// Delete deletes the key and value specified by key.
func (r *Range) Delete(batch engine.Engine, ms *proto.MVCCStats, args *proto.DeleteRequest, reply *proto.DeleteResponse) {
	reply.SetGoError(engine.MVCCDelete(batch, ms, args.Key, args.Timestamp, args.Txn))
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Range) DeleteRange(batch engine.Engine, ms *proto.MVCCStats, args *proto.DeleteRangeRequest, reply *proto.DeleteRangeResponse) {
	num, err := engine.MVCCDeleteRange(batch, ms, args.Key, args.EndKey, args.MaxEntriesToDelete, args.Timestamp, args.Txn)
	reply.NumDeleted = num
	reply.SetGoError(err)
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Range) Scan(batch engine.Engine, args *proto.ScanRequest, reply *proto.ScanResponse) {
	kvs, err := engine.MVCCScan(batch, args.Key, args.EndKey, args.MaxResults, args.Timestamp, args.ReadConsistency == proto.CONSISTENT, args.Txn)
	reply.Rows = kvs
	reply.SetGoError(err)
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Range) EndTransaction(batch engine.Engine, ms *proto.MVCCStats, args *proto.EndTransactionRequest, reply *proto.EndTransactionResponse) {
	if args.Txn == nil {
		reply.SetGoError(util.Errorf("no transaction specified to EndTransaction"))
		return
	}
	key := engine.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, true, nil, existTxn)
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
		} else if args.Txn.Epoch == existTxn.Epoch && existTxn.Timestamp.Less(args.Txn.OrigTimestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the original transaction timestamp.
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("timestamp regression: %s", args.Txn.OrigTimestamp)))
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
		if args.Txn.Isolation == proto.SERIALIZABLE && !reply.Txn.Timestamp.Equal(args.Txn.OrigTimestamp) {
			reply.SetGoError(proto.NewTransactionRetryError(reply.Txn))
			return
		}
		reply.Txn.Status = proto.COMMITTED
	} else {
		reply.Txn.Status = proto.ABORTED
	}

	// Persist the transaction record with updated status (& possibly timestamp).
	if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, reply.Txn); err != nil {
		reply.SetGoError(err)
		return
	}

	// Run triggers if successfully committed. Any failures running
	// triggers will set an error and prevent the batch from committing.
	if ct := args.InternalCommitTrigger; ct != nil {
		// Resolve any explicit intents.
		for _, key := range ct.Intents {
			log.V(1).Infof("resolving intent at %s on end transaction [%s]", key, reply.Txn.Status)
			if err := engine.MVCCResolveWriteIntent(batch, ms, key, reply.Txn.Timestamp, reply.Txn); err != nil {
				reply.SetGoError(err)
				return
			}
			reply.Resolved = append(reply.Resolved, key)
		}
		// Run appropriate trigger.
		if reply.Txn.Status == proto.COMMITTED {
			if ct.SplitTrigger != nil {
				reply.SetGoError(r.splitTrigger(batch, ct.SplitTrigger))
			} else if ct.MergeTrigger != nil {
				reply.SetGoError(r.mergeTrigger(batch, ct.MergeTrigger))
			} else if ct.ChangeReplicasTrigger != nil {
				reply.SetGoError(r.changeReplicasTrigger(ct.ChangeReplicasTrigger))
			}
		}
	}
}

// InternalRangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix prepended. The Range Metadata
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
// The "Range Metadata Key" for a range is built by appending the end key of
// the range to the meta[12] prefix because the RocksDB iterator only supports
// a Seek() interface which acts as a Ceil(). Using the start key of the range
// would cause Seek() to find the key after the meta indexing record we're
// looking for, which would result in having to back the iterator up, an option
// which is both less efficient and not available in all cases.
//
// This method has an important optimization: instead of just returning the
// request RangeDescriptor, it also returns a slice of additional range
// descriptors immediately consecutive to the desired RangeDescriptor. This is
// intended to serve as a sort of caching pre-fetch, so that the requesting
// nodes can aggressively cache RangeDescriptors which are likely to be desired
// by their current workload.
func (r *Range) InternalRangeLookup(batch engine.Engine, args *proto.InternalRangeLookupRequest, reply *proto.InternalRangeLookupResponse) {
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

	// We want to search for the metadata key just greater than args.Key. Scan
	// for both the requested key and the keys immediately afterwards, up to
	// MaxRanges.
	metaPrefix := proto.Key(args.Key[:len(engine.KeyMeta1Prefix)])
	nextKey := proto.Key(args.Key).Next()
	// Always false, at least when called from the DistSender.
	consistent := args.ReadConsistency != proto.INCONSISTENT
	kvs, err := engine.MVCCScan(batch, nextKey, metaPrefix.PrefixEnd(), rangeCount, args.Timestamp, consistent, args.Txn)
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
		err := proto.NewRangeKeyMismatchError(args.Key, args.EndKey, r.Desc())
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
func (r *Range) InternalHeartbeatTxn(batch engine.Engine, args *proto.InternalHeartbeatTxnRequest, reply *proto.InternalHeartbeatTxnResponse) {
	key := engine.TransactionKey(args.Txn.Key, args.Txn.ID)

	var txn proto.Transaction
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, true, nil, &txn)
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
		if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, &txn); err != nil {
			reply.SetGoError(err)
			return
		}
	}
	reply.Txn = &txn
}

// InternalGC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func (r *Range) InternalGC(batch engine.Engine, ms *proto.MVCCStats, args *proto.InternalGCRequest, reply *proto.InternalGCResponse) {
	// Garbage collect the specified keys by expiration timestamps.
	if err := engine.MVCCGarbageCollect(batch, ms, args.Keys, args.Timestamp); err != nil {
		reply.SetGoError(err)
		return
	}

	// Store the GC metadata for this range.
	key := engine.RangeGCMetadataKey(r.Desc().RaftID)
	err := engine.MVCCPutProto(batch, ms, key, proto.ZeroTimestamp, nil, &args.GCMeta)
	reply.SetGoError(err)
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
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Range) InternalPushTxn(batch engine.Engine, args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) {
	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		reply.SetGoError(util.Errorf("request key %s should match pushee's txn key %s", args.Key, args.PusheeTxn.Key))
		return
	}
	key := engine.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, true, nil, existTxn)
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

	// If there's no incoming transaction, the pusher is non-transactional.
	// We make a random priority, biased by specified
	// args.Header().UserPriority in this case.
	var priority int32
	if args.Txn != nil {
		priority = args.Txn.Priority
	} else {
		// Make sure we have a deterministic random number when generating
		// a priority for this txn-less request, so all replicas see same priority.
		randGen := rand.New(rand.NewSource(int64(reply.PusheeTxn.Priority) ^ args.Timestamp.WallTime))
		priority = proto.MakePriority(randGen, args.GetUserPriority())
	}

	// Check for txn timeout.
	if reply.PusheeTxn.LastHeartbeat == nil {
		reply.PusheeTxn.LastHeartbeat = &reply.PusheeTxn.Timestamp
	}
	// Compute heartbeat expiration (use args.Timestamp instead of node
	// clock wall time so that all replicas see the same result).
	expiry := args.Timestamp
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
		reply.SetGoError(proto.NewTransactionPushError(args.Txn, reply.PusheeTxn))
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

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, reply.PusheeTxn); err != nil {
		reply.SetGoError(err)
		return
	}
}

// InternalResolveIntent updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. The range will return the current status for this
// transaction to the coordinator.
func (r *Range) InternalResolveIntent(batch engine.Engine, ms *proto.MVCCStats, args *proto.InternalResolveIntentRequest, reply *proto.InternalResolveIntentResponse) {
	if args.Txn == nil {
		reply.SetGoError(util.Errorf("no transaction specified to InternalResolveIntent"))
		return
	}
	if len(args.EndKey) == 0 || bytes.Equal(args.Key, args.EndKey) {
		reply.SetGoError(engine.MVCCResolveWriteIntent(batch, ms, args.Key, args.Timestamp, args.Txn))
	} else {
		_, err := engine.MVCCResolveWriteIntentRange(batch, ms, args.Key, args.EndKey, 0, args.Timestamp, args.Txn)
		reply.SetGoError(err)
	}
}

// InternalMerge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used by
// Cockroach for the efficient accumulation of certain values. Due to the
// difficulty of making these operations transactional, merges are not currently
// exposed directly to clients. Merged values are explicitly not MVCC data.
func (r *Range) InternalMerge(batch engine.Engine, ms *proto.MVCCStats, args *proto.InternalMergeRequest, reply *proto.InternalMergeResponse) {
	err := engine.MVCCMerge(batch, ms, args.Key, args.Value)
	reply.SetGoError(err)
}

// InternalTruncateLog discards a prefix of the raft log.
func (r *Range) InternalTruncateLog(batch engine.Engine, ms *proto.MVCCStats, args *proto.InternalTruncateLogRequest, reply *proto.InternalTruncateLogResponse) {
	// args.Index is the first index to keep.
	term, err := r.Term(args.Index - 1)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	start := engine.RaftLogKey(r.Desc().RaftID, 0)
	end := engine.RaftLogKey(r.Desc().RaftID, args.Index)
	err = batch.Iterate(engine.MVCCEncodeKey(start), engine.MVCCEncodeKey(end),
		func(kv proto.RawKeyValue) (bool, error) {
			err := batch.Clear(kv.Key)
			return false, err
		})
	if err != nil {
		reply.SetGoError(err)
		return
	}
	ts := proto.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}
	err = engine.MVCCPutProto(batch, ms, engine.RaftTruncatedStateKey(r.Desc().RaftID),
		proto.ZeroTimestamp, nil, &ts)
	reply.SetGoError(err)
}

// InternalLeaderLease sets the leader lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). After a lease has been set, calls to
// HasLeaderLease() will return true if this replica is the lease holder and
// the lease has not yet expired. If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range leader are commenced, including
// clearing the command queue and timestamp cache.
func (r *Range) InternalLeaderLease(batch engine.Engine, ms *proto.MVCCStats, args *proto.InternalLeaderLeaseRequest, reply *proto.InternalLeaderLeaseResponse) {
	r.Lock()
	defer r.Unlock()

	oldLease := r.getLease()
	// Verify details of new lease request. The start of this lease must
	// obviously precede its expiration.
	if !args.Lease.Start.Less(args.Lease.Expiration) {
		reply.SetGoError(util.Errorf("lease %s - %s invalid",
			args.Lease.Start, args.Lease.Expiration))
		return
	}

	// Wind the start timestamp back as far to the previous lease's expiration
	// as we can. That'll make sure that when multiple leases are requested out
	// of order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed
	// nevertheless, unless they have a "real" issue with a previous lease.
	// Example: Assuming no previous lease, one request for [5, 15) followed by
	// one for [0, 15) would fail without this optimization. With it, the first
	// request effectively gets the lease for [0, 15), which the second one can
	// commit again (even extending your own lease is possible; see below).
	effectiveStart := oldLease.Expiration
	// If no old lease exists or this is our lease, we don't need to add this
	// extra tick. Unfortunately relevant in practice, especially in tests which
	// tend to have manual clocks and run operations at the zero timestamp.
	if oldLease.RaftNodeID != 0 && oldLease.RaftNodeID != args.Lease.RaftNodeID {
		effectiveStart = effectiveStart.Add(0, 1)
	}

	if oldLease.RaftNodeID == args.Lease.RaftNodeID {
		if effectiveStart.Less(oldLease.Start) {
			reply.SetGoError(util.Errorf("lease renewal %s - [%s] - %s invalid; prior lease %s - %s",
				effectiveStart, args.Lease.Start, args.Lease.Expiration, oldLease.Start, oldLease.Expiration))
			return
		}
		// Note that the lease expiration can be shortened by the holder.
		// This could be used to effect a faster lease handoff.
	} else if effectiveStart.Less(oldLease.Expiration) {
		reply.SetGoError(util.Errorf("lease %s - [%s] - %s invalid; prior lease %s - %s",
			effectiveStart, args.Lease.Start, args.Lease.Expiration, oldLease.Start, oldLease.Expiration))
		return
	}

	args.Lease.Start = effectiveStart

	// Store the lease to disk & in-memory.
	if err := engine.MVCCPutProto(batch, nil, engine.RaftLeaderLeaseKey(r.Desc().RaftID), proto.ZeroTimestamp, nil, &args.Lease); err != nil {
		reply.SetGoError(err)
		return
	}
	atomic.StorePointer(&r.lease, unsafe.Pointer(&args.Lease))

	// If this replica is a new holder of the lease, gossip configs as
	// necessary. Update the low water mark in the timestamp cache. We
	// add the maximum clock offset to account for any difference in
	// clocks between the expiration (set by a remote node) and this
	// node.
	if r.getLease().RaftNodeID == uint64(r.rm.RaftNodeID()) && oldLease.RaftNodeID != r.getLease().RaftNodeID {
		// Gossip configs in the event this range contains config info.
		r.maybeGossipConfigs(func(configPrefix proto.Key) bool {
			return r.ContainsKey(configPrefix)
		})
		r.tsCache.SetLowWater(oldLease.Expiration.Add(int64(r.rm.Clock().MaxOffset()), 0))

		if log.V(1) {
			nodeID, storeID := DecodeRaftNodeID(multiraft.NodeID(args.Lease.RaftNodeID))
			log.Infof("new leader lease for store %d on node %d: %s - %s",
				storeID, nodeID, args.Lease.Start, args.Lease.Expiration)
		}
	}
}

// splitTrigger is called on a successful commit of an AdminSplit
// transaction. It copies the response cache for the new range and
// recomputes stats for both the existing, updated range and the new
// range.
func (r *Range) splitTrigger(batch engine.Engine, split *proto.SplitTrigger) error {
	if !bytes.Equal(r.Desc().StartKey, split.UpdatedDesc.StartKey) ||
		!bytes.Equal(r.Desc().EndKey, split.NewDesc.EndKey) {
		return util.Errorf("range does not match splits: %s-%s + %s-%s != %s-%s", split.UpdatedDesc.StartKey,
			split.UpdatedDesc.EndKey, split.NewDesc.StartKey, split.NewDesc.EndKey, r.Desc().StartKey, r.Desc().EndKey)
	}

	// Copy the GC metadata.
	gcMeta, err := r.GetGCMetadata()
	if err != nil {
		return util.Errorf("unable to fetch GC metadata: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, engine.RangeGCMetadataKey(split.NewDesc.RaftID), proto.ZeroTimestamp, nil, gcMeta); err != nil {
		return util.Errorf("unable to copy GC metadata: %s", err)
	}

	// Copy the last verification timestamp.
	verifyTS, err := r.GetLastVerificationTimestamp()
	if err != nil {
		return util.Errorf("unable to fetch last verification timestamp: %s", err)
	}
	if err := engine.MVCCPutProto(batch, nil, engine.RangeLastVerificationTimestampKey(split.NewDesc.RaftID), proto.ZeroTimestamp, nil, &verifyTS); err != nil {
		return util.Errorf("unable to copy last verification timestamp: %s", err)
	}

	// Compute stats for updated range.
	now := r.rm.Clock().Timestamp()
	ms, err := engine.MVCCComputeStats(r.rm.Engine(), split.UpdatedDesc.StartKey, split.UpdatedDesc.EndKey, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for updated range after split: %s", err)
	}
	r.stats.SetMVCCStats(batch, ms)

	// Initialize the new range's response cache by copying the original's.
	if err = r.respCache.CopyInto(batch, split.NewDesc.RaftID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Add the new split range to the store. This step atomically
	// updates the EndKey of the updated range and also adds the
	// new range to the store's range map.
	newRng, err := NewRange(&split.NewDesc, r.rm)
	if err != nil {
		return err
	}

	// Compute stats for new range.
	ms, err = engine.MVCCComputeStats(r.rm.Engine(), split.NewDesc.StartKey, split.NewDesc.EndKey, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for new range after split: %s", err)
	}
	newRng.stats.SetMVCCStats(batch, ms)

	// Copy the timestamp cache into the new range.
	r.Lock()
	r.tsCache.MergeInto(newRng.tsCache, true /* clear */)
	r.Unlock()

	return r.rm.SplitRange(r, newRng)
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
func (r *Range) mergeTrigger(batch engine.Engine, merge *proto.MergeTrigger) error {
	if !bytes.Equal(r.Desc().StartKey, merge.UpdatedDesc.StartKey) {
		return util.Errorf("range and updated range start keys do not match: %s != %s",
			r.Desc().StartKey, merge.UpdatedDesc.StartKey)
	}

	if !r.Desc().EndKey.Less(merge.UpdatedDesc.EndKey) {
		return util.Errorf("range end key is not less than the post merge end key: %s < %s",
			r.Desc().EndKey, merge.UpdatedDesc.StartKey)
	}

	if merge.SubsumedRaftID <= 0 {
		return util.Errorf("subsumed raft ID must be provided: %d", merge.SubsumedRaftID)
	}

	// Copy the subsumed range's response cache to the subsuming one.
	if err := r.respCache.CopyFrom(batch, merge.SubsumedRaftID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Compute stats for updated range.
	now := r.rm.Clock().Timestamp()
	ms, err := engine.MVCCComputeStats(r.rm.Engine(), merge.UpdatedDesc.StartKey,
		merge.UpdatedDesc.EndKey, now.WallTime)
	if err != nil {
		return util.Errorf("unable to compute stats for the range after merge: %s", err)
	}
	r.stats.SetMVCCStats(batch, ms)

	// Clear the timestamp cache. In the case that this replica and the
	// subsumed replica each held their respective leader leases, we
	// could merge the timestamp caches for efficiency. But it's unlikely
	// and not worth the extra logic and potential for error.
	r.tsCache.Clear(r.rm.Clock())

	return r.rm.MergeRange(r, merge.UpdatedDesc.EndKey, merge.SubsumedRaftID)
}

func (r *Range) changeReplicasTrigger(change *proto.ChangeReplicasTrigger) error {
	copy := *r.Desc()
	copy.Replicas = change.UpdatedReplicas
	r.SetDesc(&copy)
	return nil
}

func loadLeaderLease(eng engine.Engine, raftID int64) (*proto.Lease, error) {
	lease := &proto.Lease{}
	if _, err := engine.MVCCGetProto(eng, engine.RaftLeaderLeaseKey(raftID), proto.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

// AdminSplit divides the range into into two ranges, using either
// args.SplitKey (if provided) or an internally computed key that aims to
// roughly equipartition the range by size. The split is done inside of
// a distributed txn which writes updated and new range descriptors, and
// updates the range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a split trigger
// carried out as part of the commit of that transaction.
func (r *Range) AdminSplit(args *proto.AdminSplitRequest, reply *proto.AdminSplitResponse) {
	// Only allow a single split per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	desc := r.Desc()
	splitKey := proto.Key(args.SplitKey)
	if len(splitKey) == 0 {
		snap := r.rm.NewSnapshot()
		defer snap.Close()
		var err error
		if splitKey, err = engine.MVCCFindSplitKey(snap, desc.RaftID, desc.StartKey, desc.EndKey); err != nil {
			reply.SetGoError(util.Errorf("unable to determine split key: %s", err))
			return
		}
	}

	// Verify some properties of split key.
	if !r.ContainsKey(splitKey) {
		reply.SetGoError(proto.NewRangeKeyMismatchError(splitKey, splitKey, desc))
		return
	}
	if !engine.IsValidSplitKey(splitKey) {
		reply.SetGoError(util.Errorf("cannot split range at key %s", splitKey))
		return
	}
	if splitKey.Equal(desc.StartKey) || splitKey.Equal(desc.EndKey) {
		reply.SetGoError(util.Errorf("range has already been split by key %s", splitKey))
		return
	}

	// Create new range descriptor with newly-allocated replica IDs and Raft IDs.
	newDesc, err := r.rm.NewRangeDescriptor(splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		reply.SetGoError(util.Errorf("unable to allocate new range descriptor: %s", err))
		return
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = splitKey

	log.Infof("initiating a split of range %d %s-%s at key %s", desc.RaftID,
		proto.Key(desc.StartKey), proto.Key(desc.EndKey), splitKey)

	txnOpts := &client.TransactionOptions{
		Name: fmt.Sprintf("split range %d at %s", desc.RaftID, splitKey),
	}
	if err = r.rm.DB().RunTransaction(txnOpts, func(txn *client.Txn) error {
		// Create range descriptor for second half of split.
		// Note that this put must go first in order to locate the
		// transaction record on the correct range.
		desc1Key := engine.RangeDescriptorKey(newDesc.StartKey)
		txn.Prepare(updateRangeDescriptorCall(desc1Key, nil, newDesc))
		// Update existing range descriptor for first half of split.
		desc2Key := engine.RangeDescriptorKey(updatedDesc.StartKey)
		txn.Prepare(updateRangeDescriptorCall(desc2Key, desc, &updatedDesc))
		// Update range descriptor addressing record(s).
		calls, err := SplitRangeAddressing(newDesc, &updatedDesc)
		if err != nil {
			return err
		}
		txn.Prepare(calls...)
		if err := txn.Flush(); err != nil {
			return err
		}
		// Update the RangeTree.
		if err := InsertRange(txn, newDesc.StartKey); err != nil {
			return err
		}
		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		txn.Prepare(client.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: args.Key},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					SplitTrigger: &proto.SplitTrigger{
						UpdatedDesc: updatedDesc,
						NewDesc:     *newDesc,
					},
					Intents: []proto.Key{desc1Key, desc2Key},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Flush()
	}); err != nil {
		reply.SetGoError(util.Errorf("split at key %s failed: %s", splitKey, err))
	}
}

// ReplicaSetsEqual is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func ReplicaSetsEqual(a, b []proto.Replica) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[proto.StoreID]int)
	for _, replica := range a {
		set[replica.StoreID]++
	}

	for _, replica := range b {
		set[replica.StoreID]--
	}

	for _, value := range set {
		if value != 0 {
			return false
		}
	}

	return true
}

// AdminMerge extends the range to subsume the range that comes next in
// the key space. The range being subsumed is provided in args.SubsumedRange.
// The EndKey of the subsuming range must equal the start key of the
// range being subsumed. The merge is performed inside of a distributed
// transaction which writes the updated range descriptor for the subsuming range
// and deletes the range descriptor for the subsumed one. It also updates the
// range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a merge trigger
// carried out as part of the commit of that transaction.
// A merge requires that the two ranges are collocate on the same set of replicas.
func (r *Range) AdminMerge(args *proto.AdminMergeRequest, reply *proto.AdminMergeResponse) {
	// Only allow a single split/merge per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Lookup subsumed range.
	desc := r.Desc()
	if desc.EndKey.Equal(proto.KeyMax) {
		// Noop.
		return
	}
	subsumedRng := r.rm.LookupRange(desc.EndKey, desc.EndKey)
	if subsumedRng == nil {
		reply.SetGoError(util.Errorf("ranges not collocated; migration of ranges in anticipation of merge not yet implemented"))
		return
	}
	subsumedDesc := subsumedRng.Desc()

	// Make sure the range being subsumed follows this one.
	if !bytes.Equal(desc.EndKey, subsumedDesc.StartKey) {
		reply.SetGoError(util.Errorf("Ranges that are not adjacent cannot be merged, %s != %s",
			desc.EndKey, subsumedDesc.StartKey))
		return
	}

	// Ensure that both ranges are collocate by intersecting the store ids from
	// their replicas.
	if !ReplicaSetsEqual(subsumedDesc.GetReplicas(), desc.GetReplicas()) {
		reply.SetGoError(util.Error("The two ranges replicas are not collocate"))
		return
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *desc
	updatedDesc.EndKey = subsumedDesc.EndKey

	log.Infof("initiating a merge of range %d %s-%s into range %d %s-%s",
		subsumedDesc.RaftID, proto.Key(subsumedDesc.StartKey), proto.Key(subsumedDesc.EndKey),
		desc.RaftID, desc.StartKey, desc.EndKey)

	txnOpts := &client.TransactionOptions{
		Name: fmt.Sprintf("merge range %d into %d", subsumedDesc.RaftID, desc.RaftID),
	}
	if err := r.rm.DB().RunTransaction(txnOpts, func(txn *client.Txn) error {
		// Update the range descriptor for the receiving range.
		desc1Key := engine.RangeDescriptorKey(updatedDesc.StartKey)
		txn.Prepare(updateRangeDescriptorCall(desc1Key, desc, &updatedDesc))

		// Remove the range descriptor for the deleted range.
		// TODO(bdarnell): need a conditional delete?
		desc2Key := engine.RangeDescriptorKey(subsumedDesc.StartKey)
		txn.Prepare(client.Delete(desc2Key))

		calls, err := MergeRangeAddressing(desc, &updatedDesc)
		if err != nil {
			return err
		}
		txn.Prepare(calls...)

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		txn.Prepare(client.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: args.Key},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					MergeTrigger: &proto.MergeTrigger{
						UpdatedDesc:    updatedDesc,
						SubsumedRaftID: subsumedDesc.RaftID,
					},
					Intents: []proto.Key{desc1Key, desc2Key},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Flush()
	}); err != nil {
		reply.SetGoError(util.Errorf("merge of range %d into %d failed: %s",
			subsumedDesc.RaftID, desc.RaftID, err))
	}
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
func (r *Range) ChangeReplicas(changeType proto.ReplicaChangeType, replica proto.Replica) error {
	// Only allow a single change per range at a time.
	r.metaLock.Lock()
	defer r.metaLock.Unlock()

	// Validate the request and prepare the new descriptor.
	desc := r.Desc()
	updatedDesc := *desc
	updatedDesc.Replicas = append([]proto.Replica{}, desc.Replicas...)
	found := -1       // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas {
		nodeUsed = nodeUsed || existingRep.NodeID == replica.NodeID
		if existingRep.NodeID == replica.NodeID &&
			existingRep.StoreID == replica.StoreID {
			found = i
			break
		}
	}
	if changeType == proto.ADD_REPLICA {
		// If the replica exists on the remote node, no matter in which store,
		// abort the replica add.
		if nodeUsed {
			return util.Errorf("adding replica %v which is already present in range %d",
				replica, desc.RaftID)
		}
		updatedDesc.Replicas = append(updatedDesc.Replicas, replica)
	} else if changeType == proto.REMOVE_REPLICA {
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if found == -1 {
			return util.Errorf("removing replica %v which is not present in range %d",
				replica, desc.RaftID)
		}
		updatedDesc.Replicas[found] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
		updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
	}

	txnOpts := &client.TransactionOptions{
		Name: fmt.Sprintf("change replicas of %d", desc.RaftID),
	}
	err := r.rm.DB().RunTransaction(txnOpts, func(txn *client.Txn) error {
		// Important: the range descriptor must be the first thing touched in the transaction
		// so the transaction record is co-located with the range being modified.
		descKey := engine.RangeDescriptorKey(updatedDesc.StartKey)

		txn.Prepare(updateRangeDescriptorCall(descKey, desc, &updatedDesc))

		// TODO(bdarnell): call UpdateRangeAddressing

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		txn.Prepare(client.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: updatedDesc.StartKey},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					ChangeReplicasTrigger: &proto.ChangeReplicasTrigger{
						NodeID:          replica.NodeID,
						StoreID:         replica.StoreID,
						ChangeType:      changeType,
						UpdatedReplicas: updatedDesc.Replicas,
					},
					Intents: []proto.Key{descKey},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Flush()
	})
	if err != nil {
		return util.Errorf("change replicas of %d failed: %s", desc.RaftID, err)
	}
	return nil
}

// updateRangeDescriptorCall returns a client.Call to execute a ConditionalPut
// on the range descriptor.
// The conditional put verifies that changes to the range descriptor are made
// in a well-defined order, preventing a scenario where a wayward replica which
// is no longer part of the original Raft group comes back online to form a splinter
// group with a node which was also a former replica, and hijacks the range
// descriptor. This is a last line of defense; other mechanisms should prevent
// rogue replicas from getting this far (see #768).
func updateRangeDescriptorCall(descKey proto.Key, oldDesc, newDesc *proto.RangeDescriptor) client.Call {
	var oldValue *proto.Value
	if oldDesc != nil {
		oldData, err := gogoproto.Marshal(oldDesc)
		if err != nil {
			return client.Call{Err: err}
		}
		oldValue = &proto.Value{Bytes: oldData}
	}

	newData, err := gogoproto.Marshal(newDesc)
	if err != nil {
		return client.Call{Err: err}
	}
	newValue := proto.Value{Bytes: newData}
	newValue.InitChecksum(descKey)

	return client.Call{
		Args: &proto.ConditionalPutRequest{
			RequestHeader: proto.RequestHeader{
				Key: descKey,
			},
			Value:    newValue,
			ExpValue: oldValue,
		},
		Reply: &proto.ConditionalPutResponse{},
	}
}
