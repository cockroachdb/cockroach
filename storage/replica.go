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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// DefaultHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 2x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultHeartbeatInterval = 5 * time.Second

	// sentinelGossipTTL is time-to-live for the gossip sentinel. The
	// sentinel informs a node whether or not it's connected to the
	// primary gossip network and not just a partition. As such it must
	// expire on a reasonable basis and be continually re-gossiped. The
	// replica which is the raft leader of the first range gossips it.
	sentinelGossipTTL = 2 * time.Minute
	// sentinalGossipInterval is the approximate interval at which the
	// sentinel info is gossiped.
	sentinelGossipInterval = sentinelGossipTTL / 2

	// configGossipTTL is the time-to-live for configuration maps.
	configGossipTTL = 0 // does not expire
	// configGossipInterval is the interval at which range leaders gossip
	// their config maps. Even if config maps do not expire, we still
	// need a periodic gossip to safeguard against failure of a leader
	// to gossip after performing an update to the map.
	configGossipInterval = 1 * time.Minute

	opReplica = "replica"
)

// CommandFilter may be used in tests through the StorageTestingMocker to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error. Note that in a multi-replica test this filter will
// be run once for each replica and must produce consistent results each time.
type CommandFilter func(roachpb.StoreID, roachpb.Request, roachpb.Header) error

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var txnAutoGC = true

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
	ctx     context.Context
	idKey   cmdIDKey
	raftCmd roachpb.RaftCommand
	done    chan roachpb.ResponseWithError // Used to signal waiting RPC handler
}

type cmdIDKey string

type replicaChecksum struct {
	// Set to true when the checksum computation is ready. The checksum
	// can be nil indicating an error.
	ok bool
	// Computed checksum. This is set to nil on error.
	checksum []byte
	// GC this checksum after this timestamp. The timestamp is set when
	// ok is set to true.
	gcTimestamp time.Time
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	RangeID      roachpb.RangeID // Should only be set by the constructor.
	store        *Store
	stats        *rangeStats    // Range statistics
	systemDBHash []byte         // sha1 hash of the system config @ last gossip
	sequence     *SequenceCache // Provides txn replay protection

	// Held in read mode during read-only commands. Held in exclusive mode to
	// prevent read-only commands from executing. Acquired before the embedded
	// RWMutex
	readOnlyCmdMu sync.RWMutex

	mu struct {
		sync.Mutex                   // Protects all fields in the mu struct.
		appliedIndex   uint64        // Last index applied to the state machine.
		cmdQ           *CommandQueue // Enforce at most one command is running per key(s).
		desc           *roachpb.RangeDescriptor
		lastIndex      uint64 // Last index persisted to the raft log (not necessarily committed).
		leaderLease    *roachpb.Lease
		maxBytes       int64 // Max bytes before split.
		pendingCmds    map[cmdIDKey]*pendingCmd
		pendingSeq     uint64 // atomic sequence counter for cmdIDKey generation.
		raftGroup      *raft.RawNode
		replicaID      roachpb.ReplicaID
		truncatedState *roachpb.RaftTruncatedState
		tsCache        *TimestampCache       // Most recent timestamps for keys / key ranges
		llChans        []chan *roachpb.Error // Slice of channels to send on after leader lease acquisition
		// proposeRaftCommandFn can be set to mock out the propose operation.
		proposeRaftCommandFn func(cmdIDKey, *pendingCmd) error
		checksums            map[uuid.UUID]replicaChecksum // computed checksum at a snapshot UUID.
		checksumNotify       map[uuid.UUID]chan []byte     // notify of computed checksum.
	}
}

var _ client.Sender = &Replica{}

// NewReplica initializes the replica using the given metadata. If the
// replica is initialized (i.e. desc contains more than a RangeID),
// replicaID should be 0 and the replicaID will be discovered from the
// descriptor.
func NewReplica(desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID) (*Replica, error) {
	r := &Replica{
		store:    store,
		sequence: NewSequenceCache(desc.RangeID),
		RangeID:  desc.RangeID,
	}

	if err := r.newReplicaInner(desc, store.Clock(), replicaID); err != nil {
		return nil, err
	}

	r.maybeGossipSystemConfig()

	var err error
	if r.stats, err = newRangeStats(desc.RangeID, store.Engine()); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Replica) newReplicaInner(desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mu.cmdQ = NewCommandQueue()
	r.mu.tsCache = NewTimestampCache(clock)
	r.mu.pendingCmds = map[cmdIDKey]*pendingCmd{}
	r.mu.checksums = map[uuid.UUID]replicaChecksum{}
	r.mu.checksumNotify = map[uuid.UUID]chan []byte{}
	r.setDescWithoutProcessUpdateLocked(desc)

	var err error
	r.mu.lastIndex, err = r.loadLastIndexLocked()
	if err != nil {
		return err
	}

	r.mu.appliedIndex, err = r.loadAppliedIndexLocked(r.store.Engine())
	if err != nil {
		return err
	}

	r.mu.leaderLease, err = loadLeaderLease(r.store.Engine(), desc.RangeID)
	if err != nil {
		return err
	}

	if r.isInitializedLocked() && replicaID != 0 {
		return util.Errorf("replicaID must be 0 when creating an initialized replica")
	}
	if replicaID == 0 {
		_, repDesc := desc.FindReplica(r.store.StoreID())
		if repDesc == nil {
			return util.Errorf("cannot recreate replica that is not a member of its range (StoreID %s not found in %s)",
				r.store.StoreID(), desc)
		}
		replicaID = repDesc.ReplicaID
	}
	return r.setReplicaIDLocked(replicaID)
}

// String returns a string representation of the range. It acquires mu.Lock in the call to Desc().
func (r *Replica) String() string {
	desc := r.Desc()
	return fmt.Sprintf("range=%d [%s-%s)", desc.RangeID, desc.StartKey, desc.EndKey)
}

// Destroy clears pending command queue by sending each pending
// command an error and cleans up all data associated with this range.
func (r *Replica) Destroy(origDesc roachpb.RangeDescriptor) error {
	desc := r.Desc()
	if _, rd := desc.FindReplica(r.store.StoreID()); rd != nil && rd.ReplicaID >= origDesc.NextReplicaID {
		return util.Errorf("cannot destroy replica %s; replica ID has changed (%s >= %s)",
			r, rd.ReplicaID, origDesc.NextReplicaID)
	}

	// Clear the pending command queue.
	r.mu.Lock()
	for _, p := range r.mu.pendingCmds {
		p.done <- roachpb.ResponseWithError{
			Reply: &roachpb.BatchResponse{},
			Err:   roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID)),
		}
	}
	// Clear the map.
	r.mu.pendingCmds = map[cmdIDKey]*pendingCmd{}
	r.mu.Unlock()

	return r.store.destroyReplicaData(desc)
}

func (r *Replica) setReplicaID(replicaID roachpb.ReplicaID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setReplicaIDLocked(replicaID)
}

// setReplicaIDLocked requires that the replica lock is held.
func (r *Replica) setReplicaIDLocked(replicaID roachpb.ReplicaID) error {
	if r.mu.replicaID == replicaID {
		return nil
	} else if r.mu.replicaID > replicaID {
		return util.Errorf("replicaID cannot move backwards")
	} else if r.mu.replicaID != 0 {
		// TODO(bdarnell): clean up previous raftGroup (cancel pending commands,
		// update peers)
	}

	raftCfg := &raft.Config{
		ID:            uint64(replicaID),
		Applied:       r.mu.appliedIndex,
		ElectionTick:  r.store.ctx.RaftElectionTimeoutTicks,
		HeartbeatTick: r.store.ctx.RaftHeartbeatIntervalTicks,
		Storage:       r,
		// TODO(bdarnell): make these configurable; evaluate defaults.
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          &raftLogger{group: uint64(r.RangeID)},
	}
	raftGroup, err := raft.NewRawNode(raftCfg, nil)
	if err != nil {
		return err
	}
	r.mu.replicaID = replicaID
	r.mu.raftGroup = raftGroup

	// Automatically campaign and elect a leader for this group if there's
	// exactly one known node for this group.
	//
	// A grey area for this being correct happens in the case when we're
	// currently in the progress of adding a second node to the group,
	// with the change committed but not applied.
	// Upon restarting, the node would immediately elect itself and only
	// then apply the config change, where really it should be applying
	// first and then waiting for the majority (which would now require
	// two votes, not only its own).
	// However, in that special case, the second node has no chance to
	// be elected leader while this node restarts (as it's aware of the
	// configuration and knows it needs two votes), so the worst that
	// could happen is both nodes ending up in candidate state, timing
	// out and then voting again. This is expected to be an extremely
	// rare event.
	if len(r.mu.desc.Replicas) == 1 && r.mu.desc.Replicas[0].StoreID == r.store.StoreID() {
		if err := raftGroup.Campaign(); err != nil {
			return err
		}
	}

	return nil
}

// context returns a context which is initialized with information about
// this range. It is only relevant when commands need to be executed
// on this range in the absence of a pre-existing context, such as
// during range scanner operations.
func (r *Replica) context() context.Context {
	return context.WithValue(r.store.Context(nil), log.RangeID, r.RangeID)
}

// GetMaxBytes atomically gets the range maximum byte limit.
func (r *Replica) GetMaxBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.maxBytes
}

// SetMaxBytes atomically sets the maximum byte limit before
// split. This value is cached by the range for efficiency.
func (r *Replica) SetMaxBytes(maxBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.maxBytes = maxBytes
}

// IsFirstRange returns true if this is the first range.
func (r *Replica) IsFirstRange() bool {
	return bytes.Equal(r.Desc().StartKey, roachpb.RKeyMin)
}

func loadLeaderLease(eng engine.Engine, rangeID roachpb.RangeID) (*roachpb.Lease, error) {
	lease := &roachpb.Lease{}
	if _, err := engine.MVCCGetProto(eng, keys.RangeLeaderLeaseKey(rangeID), roachpb.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

// getLeaderLease returns the current leader lease.
func (r *Replica) getLeaderLease() *roachpb.Lease {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.leaderLease
}

// newNotLeaderError returns a NotLeaderError initialized with the
// replica for the holder (if any) of the given lease.
func (r *Replica) newNotLeaderError(l *roachpb.Lease, originStoreID roachpb.StoreID) error {
	err := &roachpb.NotLeaderError{}
	if l != nil && l.Replica.ReplicaID != 0 {
		desc := r.Desc()

		err.RangeID = r.RangeID
		_, err.Replica = desc.FindReplica(originStoreID)
		_, err.Leader = desc.FindReplica(l.Replica.StoreID)
	}
	return err
}

// requestLeaderLease sends a request to obtain or extend a leader
// lease for this replica. Unless an error is returned, the obtained
// lease will be valid for a time interval containing the requested
// timestamp. Only a single lease request may be pending at a time.
func (r *Replica) requestLeaderLease(timestamp roachpb.Timestamp) <-chan *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	llChan := make(chan *roachpb.Error, 1)
	if len(r.mu.llChans) > 0 {
		r.mu.llChans = append(r.mu.llChans, llChan)
		return llChan
	}

	r.store.Stopper().RunWorker(func() {
		pErr := func() *roachpb.Error {
			// TODO(Tobias): get duration from configuration, either as a config flag
			// or, later, dynamically adjusted.
			duration := DefaultLeaderLeaseDuration

			// Prepare a Raft command to get a leader lease for this replica.
			expiration := timestamp.Add(int64(duration), 0)
			desc := r.Desc()
			_, replica := desc.FindReplica(r.store.StoreID())
			if replica == nil {
				return roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
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
			ba.Timestamp = r.store.Clock().Now()
			ba.RangeID = r.RangeID
			ba.Add(args)

			// Send lease request directly to raft in order to skip unnecessary
			// checks from normal request machinery, (e.g. the command queue).
			// Note that the command itself isn't traced, but usually the caller
			// waiting for the result has an active Trace.
			cmd, err := r.proposeRaftCommand(r.context(), ba)
			if err != nil {
				return roachpb.NewError(err)
			}

			// If the command was committed, wait for the range to apply it.
			select {
			case c := <-cmd.done:
				if c.Err != nil {
					if log.V(1) {
						log.Infof("failed to acquire leader lease for replica %s: %s", r.store, c.Err)
					}
				}
				return c.Err
			case <-r.store.Stopper().ShouldStop():
				return roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID()))
			}
		}()

		// Send result of leader lease to all waiter channels.
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, llChan := range r.mu.llChans {
			llChan <- pErr
		}
		r.mu.llChans = r.mu.llChans[:0]
	})

	r.mu.llChans = append(r.mu.llChans, llChan)
	return llChan
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
func (r *Replica) redirectOnOrAcquireLeaderLease(trace opentracing.Span, ctx context.Context) *roachpb.Error {
	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		if lease := r.getLeaderLease(); lease.Covers(timestamp) {
			if lease.OwnedBy(r.store.StoreID()) {
				// Happy path: We have an active lease, nothing to do.
				return nil
			}
			// If lease is currently held by another, redirect to holder.
			return roachpb.NewError(r.newNotLeaderError(lease, r.store.StoreID()))
		}

		// Otherwise, no active lease: Request renewal if a renewal is not already pending.
		trace.LogEvent(fmt.Sprintf("request leader lease (attempt #%d)", attempt))
		llChan := r.requestLeaderLease(timestamp)

		// Wait for the leader lease to finish, or the context to expire.
		select {
		case pErr := <-llChan:
			if pErr != nil {
				// Getting a LeaseRejectedError back means someone else got there
				// first, or the lease request was somehow invalid due to a
				// concurrent change. Convert the error to a NotLeaderError.
				if _, ok := pErr.GetDetail().(*roachpb.LeaseRejectedError); ok {
					lease := r.getLeaderLease()
					if !lease.Covers(r.store.Clock().Now()) {
						lease = nil
					}
					return roachpb.NewError(r.newNotLeaderError(lease, r.store.StoreID()))
				}
				return pErr
			}
			continue
		case <-ctx.Done():
		case <-r.store.Stopper().ShouldStop():
		}
		return roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID()))
	}
}

// IsInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isInitializedLocked()
}

// isInitializedLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedLocked() bool {
	return len(r.mu.desc.EndKey) > 0
}

// Desc returns the range's descriptor.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.desc
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setDescWithoutProcessUpdateLocked(desc)
}

// setDescWithoutProcessUpdateLocked updates the range descriptor without
// calling processRangeDescriptorUpdate.
// setDescWithoutProcessUpdateLocked requires that the replica lock is held.
func (r *Replica) setDescWithoutProcessUpdateLocked(desc *roachpb.RangeDescriptor) {
	if desc.RangeID != r.RangeID {
		panic(fmt.Sprintf("range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID))
	}
	r.mu.desc = desc
}

// GetReplica returns the replica for this range from the range descriptor.
// Returns nil if the replica is not found.
func (r *Replica) GetReplica() *roachpb.ReplicaDescriptor {
	_, replica := r.Desc().FindReplica(r.store.StoreID())
	return replica
}

// ReplicaDescriptor returns information about the given member of
// this replica's range.
func (r *Replica) ReplicaDescriptor(replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desc := r.mu.desc
	for _, repAddress := range desc.Replicas {
		if repAddress.ReplicaID == replicaID {
			return repAddress, nil
		}
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
	if bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return bytes.HasPrefix(key, keys.MakeRangeIDPrefix(desc.RangeID))
	}
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

// getLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for garbage collection.
func (r *Replica) getLastReplicaGCTimestamp() (roachpb.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	timestamp := roachpb.Timestamp{}
	_, err := engine.MVCCGetProto(r.store.Engine(), key, roachpb.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return roachpb.ZeroTimestamp, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(timestamp roachpb.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return engine.MVCCPutProto(r.store.Engine(), nil, key, roachpb.ZeroTimestamp, nil, &timestamp)
}

// getLastVerificationTimestamp reads the timestamp at which the replica's
// data was last verified.
func (r *Replica) getLastVerificationTimestamp() (roachpb.Timestamp, error) {
	key := keys.RangeLastVerificationTimestampKey(r.RangeID)
	timestamp := roachpb.Timestamp{}
	_, err := engine.MVCCGetProto(r.store.Engine(), key, roachpb.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return roachpb.ZeroTimestamp, err
	}
	return timestamp, nil
}

func (r *Replica) setLastVerificationTimestamp(timestamp roachpb.Timestamp) error {
	key := keys.RangeLastVerificationTimestampKey(r.RangeID)
	return engine.MVCCPutProto(r.store.Engine(), nil, key, roachpb.ZeroTimestamp, nil, &timestamp)
}

// RaftStatus returns the current raft status of the replica.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.raftGroup.Status()
}

// Send adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue.
func (r *Replica) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	var br *roachpb.BatchResponse

	if err := r.checkBatchRequest(ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()
	// Differentiate between admin, read-only and write.
	var pErr *roachpb.Error
	if ba.IsAdmin() {
		sp.LogEvent("admin path")
		br, pErr = r.addAdminCmd(ctx, ba)
	} else if ba.IsReadOnly() {
		sp.LogEvent("read-only path")
		br, pErr = r.addReadOnlyCmd(ctx, ba)
	} else if ba.IsWrite() {
		sp.LogEvent("read-write path")
		br, pErr = r.addWriteCmd(ctx, ba, nil)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		panic("empty batch")
	} else {
		panic(fmt.Sprintf("don't know how to handle command %s", ba))
	}
	if _, ok := pErr.GetDetail().(*roachpb.RaftGroupDeletedError); ok {
		// This error needs to be converted appropriately so that
		// clients will retry.
		pErr = roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
	}
	// TODO(tschottdorf): assert nil reply on error.
	if pErr != nil {
		sp.LogEvent(fmt.Sprintf("error: %s", pErr))
		return nil, pErr
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
// already in the queue. Returns a cleanup function to be called when the
// commands are done and can be removed from the queue.
func (r *Replica) beginCmds(ba *roachpb.BatchRequest) func(*roachpb.Error) {
	var cmdKeys []interface{}
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		var spans []roachpb.Span
		readOnly := ba.IsReadOnly()
		for _, union := range ba.Requests {
			h := union.GetInner().Header()
			spans = append(spans, roachpb.Span{Key: h.Key, EndKey: h.EndKey})
		}
		var wg sync.WaitGroup
		r.mu.Lock()
		r.mu.cmdQ.GetWait(readOnly, &wg, spans...)
		cmdKeys = append(cmdKeys, r.mu.cmdQ.Add(readOnly, spans...)...)
		r.mu.Unlock()
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

	return func(pErr *roachpb.Error) {
		r.endCmds(cmdKeys, ba, pErr)
	}
}

// endCmds removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (r *Replica) endCmds(cmdKeys []interface{}, ba *roachpb.BatchRequest, pErr *roachpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Only update the timestamp cache if the command succeeded and we're not
	// doing inconsistent ops (in which case the ops are always read-only).
	if pErr == nil && ba.ReadConsistency != roachpb.INCONSISTENT {
		for _, union := range ba.Requests {
			args := union.GetInner()
			if usesTimestampCache(args) {
				header := args.Header()
				var txnID *uuid.UUID
				if ba.Txn != nil {
					txnID = ba.Txn.ID
				}
				r.mu.tsCache.Add(header.Key, header.EndKey, ba.Timestamp, txnID, roachpb.IsReadOnly(args))
			}
		}
	}
	r.mu.cmdQ.Remove(cmdKeys)
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the leader replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) addAdminCmd(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}
	args := ba.Requests[0].GetInner()

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, roachpb.NewErrorWithTxn(err, ba.Txn)
	}

	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	sp.SetOperationName(reflect.TypeOf(args).String())

	// Admin commands always require the leader lease.
	if pErr := r.redirectOnOrAcquireLeaderLease(sp, ctx); pErr != nil {
		return nil, pErr
	}

	var resp roachpb.Response
	var pErr *roachpb.Error
	switch tArgs := args.(type) {
	case *roachpb.AdminSplitRequest:
		var reply roachpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs, r.Desc())
		resp = &reply
	case *roachpb.AdminMergeRequest:
		var reply roachpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs, r.Desc())
		resp = &reply
	case *roachpb.CheckConsistencyRequest:
		var reply roachpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(*tArgs, r.Desc())
		resp = &reply
	default:
		return nil, roachpb.NewErrorf("unrecognized admin command: %T", args)
	}

	if pErr != nil {
		return nil, pErr
	}
	br := &roachpb.BatchResponse{}
	br.Add(resp)
	br.Txn = resp.Header().Txn
	return br, nil
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Replica) addReadOnlyCmd(ctx context.Context, ba roachpb.BatchRequest) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	// If the read is consistent, the read requires the leader lease.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		if pErr = r.redirectOnOrAcquireLeaderLease(sp, ctx); pErr != nil {
			return nil, pErr
		}
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	sp.LogEvent("command queue")
	endCmdsFunc := r.beginCmds(&ba)

	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we remove the commands from the command queue. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		endCmdsFunc(pErr)
	}()

	// Execute read-only batch command. It checks for matching key range; note
	// that holding readMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	var intents []intentsWithArg
	br, intents, pErr = r.executeBatch(r.store.Engine(), nil, ba)

	if pErr == nil && ba.Txn != nil {
		// Checking the sequence cache on reads makes sure that when our
		// transaction has already been aborted, we don't experience anomalous
		// conditions as described in #2231.
		pErr = r.checkSequenceCache(r.store.Engine(), *ba.Txn)
	}
	r.handleSkippedIntents(intents)
	return br, pErr
}

// addWriteCmd first adds the keys affected by this command as pending writes
// to the command queue. Next, the timestamp cache is checked to determine if
// any newer accesses to this command's affected keys have been made. If so,
// the command's timestamp is moved forward. Finally, the command is submitted
// to Raft. Upon completion, the write is removed from the read queue and any
// error returned. If a WaitGroup is supplied, it is signaled when the command
// enters Raft or the function returns with a preprocessing error, whichever
// happens earlier.
func (r *Replica) addWriteCmd(ctx context.Context, ba roachpb.BatchRequest, wg *sync.WaitGroup) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	signal := func() {
		if wg != nil {
			wg.Done()
			wg = nil
		}
	}

	// This happens more eagerly below, but it's important to guarantee that
	// early returns do not skip this.
	defer signal()

	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	// Add the write to the command queue to gate subsequent overlapping
	// commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	sp.LogEvent("command queue")
	endCmdsFunc := r.beginCmds(&ba)

	// Guarantee we remove the commands from the command queue. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		endCmdsFunc(pErr)
	}()

	// This replica must have leader lease to process a write.
	if pErr = r.redirectOnOrAcquireLeaderLease(sp, ctx); pErr != nil {
		return nil, pErr
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
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, union := range ba.Requests {
			args := union.GetInner()
			if usesTimestampCache(args) {
				header := args.Header()
				var txnID *uuid.UUID
				if ba.Txn != nil {
					txnID = ba.Txn.ID
				}

				// Always push the timestamp forward if there's been a read which
				// occurred after our txn timestamp.
				if rTS := r.mu.tsCache.GetMaxRead(header.Key, header.EndKey, txnID); !rTS.Less(ba.Timestamp) {
					ba.Timestamp = rTS.Next()
				}

				// If we're in a txn, we still go ahead and try the write since
				// we want to avoid restarting the transaction in the event that
				// there isn't an intent or the intent can be pushed by us.
				//
				// If we're not in a txn, it's trivial to just advance our timestamp.
				if ba.Txn == nil {
					// If there's a newer write timestamp...
					if wTS := r.mu.tsCache.GetMaxWrite(header.Key, header.EndKey, txnID); !wTS.Less(ba.Timestamp) {
						ba.Timestamp = wTS.Next()
					}
				}
			}
		}
	}()

	sp.LogEvent("raft")

	pendingCmd, err := r.proposeRaftCommand(ctx, ba)

	signal()

	if err == nil {
		// If the command was accepted by raft, wait for the range to apply it.
		ctxDone := ctx.Done()
		for br == nil && pErr == nil {
			select {
			case respWithErr := <-pendingCmd.done:
				br, pErr = respWithErr.Reply, respWithErr.Err
			case <-ctxDone:
				// Cancellation is somewhat tricky since we can't prevent the
				// Raft command from executing at some point in the future.
				// We try to remove the pending command, but if the processRaft
				// goroutine has already grabbed it (as would typically be the
				// case right as it executes), it's too late and we're still
				// going to have to wait until the command returns (which would
				// typically be right away).
				// A typical outcome of a bug here would be use-after-free of
				// the trace of this client request; we finish it when
				// returning from here, but the Raft execution also uses it.
				ctxDone = nil
				if r.tryAbandon(pendingCmd.idKey) {
					// TODO(tschottdorf): the command will still execute at
					// some process, so maybe this should be a structured error
					// which can be interpreted appropriately upstream.
					pErr = roachpb.NewError(ctx.Err())
				} else {
					log.Warningf("unable to cancel expired Raft command %s", ba)
				}
			}
		}
	} else {
		pErr = roachpb.NewError(err)
	}
	return br, pErr
}

// tryAbandon attempts to remove a pending command from the internal commands
// map. This is possible until execution of the command at the local replica
// has already begun, in which case false is returned and the client needs to
// continue waiting for successful execution.
func (r *Replica) tryAbandon(idKey cmdIDKey) bool {
	r.mu.Lock()
	_, ok := r.mu.pendingCmds[idKey]
	delete(r.mu.pendingCmds, idKey)
	r.mu.Unlock()
	return ok
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns the error channel and
// pending command struct for receiving.
func (r *Replica) proposeRaftCommand(ctx context.Context, ba roachpb.BatchRequest) (*pendingCmd, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, replica := r.mu.desc.FindReplica(r.store.StoreID())
	if replica == nil {
		return nil, roachpb.NewRangeNotFoundError(r.RangeID)
	}
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	idKey := cmdIDKey(idKeyBuf)
	pendingCmd := &pendingCmd{
		ctx:   ctx,
		idKey: idKey,
		done:  make(chan roachpb.ResponseWithError, 1),
		raftCmd: roachpb.RaftCommand{
			RangeID:       r.RangeID,
			OriginReplica: *replica,
			Cmd:           ba,
		},
	}

	if _, ok := r.mu.pendingCmds[idKey]; ok {
		log.Fatalf("pending command already exists for %s", idKey)
	}
	r.mu.pendingCmds[idKey] = pendingCmd

	if err := r.proposePendingCmdLocked(idKey, pendingCmd); err != nil {
		delete(r.mu.pendingCmds, idKey)
		return nil, err
	}
	return pendingCmd, nil
}

// proposePendingCmdLocked proposes or re-proposes a command in r.mu.pendingCmds.
// The replica lock must be held.
func (r *Replica) proposePendingCmdLocked(idKey cmdIDKey, p *pendingCmd) error {
	if r.mu.proposeRaftCommandFn != nil {
		return r.mu.proposeRaftCommandFn(idKey, p)
	}

	if p.raftCmd.Cmd.Timestamp == roachpb.ZeroTimestamp {
		return util.Errorf("can't propose Raft command with zero timestamp")
	}

	data, err := proto.Marshal(&p.raftCmd)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)
	for _, union := range p.raftCmd.Cmd.Requests {
		args := union.GetInner()
		etr, ok := args.(*roachpb.EndTransactionRequest)
		if !ok {
			continue
		}
		if crt := etr.InternalCommitTrigger.GetChangeReplicasTrigger(); crt != nil {
			// EndTransactionRequest with a ChangeReplicasTrigger is special because raft
			// needs to understand it; it cannot simply be an opaque command.
			log.Infof("raft: proposing %s %v for range %d", crt.ChangeType, crt.Replica, p.raftCmd.RangeID)

			ctx := ConfChangeContext{
				CommandID: string(idKey),
				Payload:   data,
				Replica:   crt.Replica,
			}
			encodedCtx, err := ctx.Marshal()
			if err != nil {
				return err
			}

			return r.mu.raftGroup.ProposeConfChange(
				raftpb.ConfChange{
					Type:    changeTypeInternalToRaft[crt.ChangeType],
					NodeID:  uint64(crt.Replica.ReplicaID),
					Context: encodedCtx,
				})
		}
	}
	return r.mu.raftGroup.Propose(encodeRaftCommand(string(idKey), data))
}

func (r *Replica) handleRaftReady() error {
	// TODO(bram): #4562 There is a lot of locking and unlocking of the replica,
	// consider refactoring this.
	r.mu.Lock()
	if !r.mu.raftGroup.HasReady() {
		r.mu.Unlock()
		return nil
	}
	rd := r.mu.raftGroup.Ready()
	lastIndex := r.mu.lastIndex
	r.mu.Unlock()
	logRaftReady(r.store.StoreID(), r.RangeID, rd)

	batch := r.store.Engine().NewBatch()
	defer batch.Close()
	if !raft.IsEmptySnap(rd.Snapshot) {
		var err error
		if lastIndex, err = r.applySnapshot(batch, rd.Snapshot); err != nil {
			return err
		}
		// TODO(bdarnell): update coalesced heartbeat mapping with snapshot info.
	}
	if len(rd.Entries) > 0 {
		var err error
		if lastIndex, err = r.append(batch, lastIndex, rd.Entries); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.setHardState(batch, rd.HardState); err != nil {
			return err
		}
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	r.mu.Lock()
	r.mu.lastIndex = lastIndex
	r.mu.Unlock()

	for _, msg := range rd.Messages {
		r.sendRaftMessage(msg)
	}

	// Process committed entries. etcd raft occasionally adds a nil
	// entry (our own commands are never empty). This happens in two
	// situations: When a new leader is elected, and when a config
	// change is dropped due to the "one at a time" rule. In both
	// cases we may need to resubmit our pending proposals (In the
	// former case we resubmit everything because we proposed them to
	// a former leader that is no longer able to commit them. In the
	// latter case we only need to resubmit pending config changes,
	// but it's hard to distinguish so we resubmit everything
	// anyway). We delay resubmission until after we have processed
	// the entire batch of entries.
	shouldReproposeCmds := false
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				shouldReproposeCmds = true
				continue
			}
			commandID, encodedCommand := DecodeRaftCommand(e.Data)
			var command roachpb.RaftCommand
			if err := command.Unmarshal(encodedCommand); err != nil {
				return err
			}

			// Discard errors from processRaftCommand. The error has been sent
			// to the client that originated it, where it will be handled.
			_ = r.processRaftCommand(cmdIDKey(commandID), e.Index, command)

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return err
			}
			ctx := ConfChangeContext{}
			if err := ctx.Unmarshal(cc.Context); err != nil {
				return err
			}
			var command roachpb.RaftCommand
			if err := command.Unmarshal(ctx.Payload); err != nil {
				return err
			}
			if err := r.processRaftCommand(cmdIDKey(ctx.CommandID), e.Index, command); err != nil {
				// If processRaftCommand failed, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			// TODO(bdarnell): update coalesced heartbeat mapping on success.
			r.mu.Lock()
			r.mu.raftGroup.ApplyConfChange(cc)
			r.mu.Unlock()
		}

	}
	if shouldReproposeCmds {
		r.mu.Lock()
		err := r.reproposePendingCmdsLocked()
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	r.mu.Lock()
	r.mu.raftGroup.Advance(rd)
	r.mu.Unlock()
	return nil
}

func (r *Replica) tick() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.raftGroup.Tick()
	// TODO(tamird/bdarnell): Reproposals should occur less frequently than
	// ticks, but this is acceptable for now.
	// TODO(tamird/bdarnell): Add unit tests.
	err := r.reproposePendingCmdsLocked()
	return err
}

func (r *Replica) reproposePendingCmdsLocked() error {
	if len(r.mu.pendingCmds) > 0 {
		if log.V(1) {
			log.Infof("reproposing %d commands after empty entry", len(r.mu.pendingCmds))
		}
		for idKey, p := range r.mu.pendingCmds {
			if err := r.proposePendingCmdLocked(idKey, p); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Replica) sendRaftMessage(msg raftpb.Message) {
	groupID := r.RangeID

	r.store.mu.Lock()
	toReplica, toErr := r.store.replicaDescriptorLocked(groupID, roachpb.ReplicaID(msg.To))
	fromReplica, fromErr := r.store.replicaDescriptorLocked(groupID, roachpb.ReplicaID(msg.From))
	r.store.mu.Unlock()

	if toErr != nil {
		log.Warningf("failed to lookup recipient replica %d in group %s: %s", msg.To, groupID, toErr)
		return
	}
	if fromErr != nil {
		log.Warningf("failed to lookup sender replica %d in group %s: %s", msg.From, groupID, fromErr)
		return
	}
	err := r.store.ctx.Transport.Send(&RaftMessageRequest{
		GroupID:     groupID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	})
	snapStatus := raft.SnapshotFinish
	if err != nil {
		log.Warningf("group %s on store %s failed to send message to %s: %s", groupID,
			r.store.StoreID(), toReplica.StoreID, err)
		r.mu.Lock()
		r.mu.raftGroup.ReportUnreachable(msg.To)
		r.mu.Unlock()
		snapStatus = raft.SnapshotFailure
	}
	if msg.Type == raftpb.MsgSnap {
		// TODO(bdarnell): add an ack for snapshots and don't report status until
		// ack, error, or timeout.
		r.mu.Lock()
		r.mu.raftGroup.ReportSnapshot(msg.To, snapStatus)
		r.mu.Unlock()
	}
}

// processRaftCommand processes a raft command by unpacking the command
// struct to get args and reply and then applying the command to the
// state machine via applyRaftCommand(). The error result is sent on
// the command's done channel, if available.
func (r *Replica) processRaftCommand(idKey cmdIDKey, index uint64, raftCmd roachpb.RaftCommand) *roachpb.Error {
	if index == 0 {
		log.Fatalc(r.context(), "processRaftCommand requires a non-zero index")
	}

	r.mu.Lock()
	cmd := r.mu.pendingCmds[idKey]
	delete(r.mu.pendingCmds, idKey)
	r.mu.Unlock()

	var ctx context.Context
	if cmd != nil {
		// We initiated this command, so use the caller-supplied context.
		ctx = cmd.ctx
	} else {
		// TODO(tschottdorf): consider the Trace situation here.
		ctx = r.context()
	}

	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	sp.LogEvent("applying batch")
	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	br, err := r.applyRaftCommand(ctx, index, raftCmd.OriginReplica, raftCmd.Cmd)
	err = r.maybeSetCorrupt(err)

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
	ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if index <= 0 {
		log.Fatalc(ctx, "raft command index is <= 0")
	}

	// If we have an out of order index, there's corruption. No sense in trying
	// to update anything or run the command. Simply return a corruption error.
	r.mu.Lock()
	oldIndex := r.mu.appliedIndex
	r.mu.Unlock()
	if oldIndex >= index {
		return nil, roachpb.NewError(newReplicaCorruptionError(util.Errorf("applied index moved backwards: %d >= %d", oldIndex, index)))
	}

	// Call the helper, which returns a batch containing data written
	// during command execution and any associated error.
	ms := engine.MVCCStats{}
	batch, br, intents, rErr := r.applyRaftCommandInBatch(ctx, index, originReplica, ba, &ms)
	defer batch.Close()

	// Advance the last applied index and commit the batch.
	if err := setAppliedIndex(batch, &ms, r.RangeID, index); err != nil {
		log.Fatalc(ctx, "setting applied index in a batch should never fail: %s", err)
	}

	// Flush the MVCC stats to the batch.
	if err := r.stats.MergeMVCCStats(batch, ms); err != nil {
		// TODO(tschottdorf): ReplicaCorruptionError.
		log.Fatalc(ctx, "setting mvcc stats in a batch should never fail: %s", err)
	}
	// Update store-level MVCC stats with merged range stats.
	r.store.metrics.addMVCCStats(ms)

	if err := batch.Commit(); err != nil {
		rErr = roachpb.NewError(newReplicaCorruptionError(util.Errorf("could not commit batch"), err, rErr.GoError()))
	} else {
		r.mu.Lock()
		// Update cached appliedIndex if we were able to set the applied index
		// on disk.
		r.mu.appliedIndex = index
		// Invalidate the cache and let raftTruncatedStateLocked() read the
		// value the next time it's required.
		if _, ok := ba.GetArg(roachpb.TruncateLog); ok {
			r.mu.truncatedState = nil
		}
		r.mu.Unlock()
	}

	// On successful write commands handle write-related triggers including
	// splitting and config gossip updates.
	if rErr == nil && ba.IsWrite() {
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
	ba roachpb.BatchRequest, ms *engine.MVCCStats) (engine.Engine, *roachpb.BatchResponse, []intentsWithArg, *roachpb.Error) {
	// Create a new batch for the command to ensure all or nothing semantics.
	btch := r.store.Engine().NewBatch()

	// Check the sequence for this batch. Only applies to transactional
	// requests which write intents (for example HeartbeatTxn does not get
	// hindered by this). On a cache hit, the transaction is instructed to
	// restart. The epoch check allows us to poison the sequence cache to avoid
	// anomalies when we know that the transaction has been aborted or pushed,
	// but it itself does not.
	if ba.Txn != nil && ba.IsTransactionWrite() {
		if err := r.checkSequenceCache(btch, *ba.Txn); err != nil {
			return btch, nil, nil, err
		}
	}

	for _, union := range ba.Requests {
		args := union.GetInner()

		// TODO(tschottdorf): shouldn't be in the loop. Currently is because
		// we haven't cleaned up the timestamp handling fully.
		if lease := r.getLeaderLease(); args.Method() != roachpb.LeaderLease &&
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
			// It's crucial that we don't update the sequence cache for the
			// error returned below since the request is going to be retried
			// with the same sequence number and would get the distributed
			// sender stuck in an infinite loop, retrieving a stale
			// NotLeaderError over and over again, even when proposing at the
			// correct replica.
			return btch, nil, nil, roachpb.NewError(r.newNotLeaderError(lease, originReplica.StoreID))
		}
	}

	// Execute the commands. If this returns without an error, the batch must
	// be committed (EndTransaction with a CommitTrigger may unlock
	// readOnlyCmdMu via a batch.Defer).
	br, intents, err := r.executeBatch(btch, ms, ba)

	// Regardless of error, add result to the sequence cache if this is
	// a write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence, even if leadership changes.
	if ba.IsWrite() {
		if err != nil {
			// TODO(tschottdorf): make `nil` acceptable. Corresponds to
			// roachpb.Response{With->Or}Error.
			br = &roachpb.BatchResponse{}
			// Otherwise, reset the batch to clear out partial execution and
			// prepare for the failed sequence cache entry.
			btch.Close()
			btch = r.store.Engine().NewBatch()
			*ms = engine.MVCCStats{}
		}

		if ba.Txn != nil {
			// Only transactional requests which leave intents have replay
			// protection. In the particular case of a txn which just ended
			// (via a successful EndTransaction call), it has purged its
			// sequence cache state and would prefer if we didn't write an
			// entry which would then need to be GCed on the slow path. See
			// (*Replica).adjustResponseCache for a more salient explanation
			// of why that is ok.
			_, isEndTxn := ba.GetArg(roachpb.EndTransaction)
			if (!isEndTxn && ba.IsTransactionWrite()) || err != nil {
				if putErr := r.sequence.Put(btch, ms, ba.Txn.ID, ba.Txn.Epoch,
					ba.Txn.Sequence, ba.Txn.Key, ba.Txn.Timestamp, err); putErr != nil {
					// TODO(tschottdorf): ReplicaCorruptionError.
					log.Fatalc(ctx, "putting a sequence cache entry in a batch should never fail: %s", putErr)
				}
			}
		}
	}

	return btch, br, intents, err
}

// checkSequenceCache checks the sequence cache for the given transaction for
// entries newer or of the same age as our current operation's. In case of a
// replay violation or if the transaction has been aborted, transaction retry
// or transaction abort error is returned, respectively.
// checkSequenceCache locks the replica.
func (r *Replica) checkSequenceCache(b engine.Engine, txn roachpb.Transaction) *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var entry roachpb.SequenceCacheEntry
	epoch, sequence, readErr := r.sequence.Get(b, txn.ID, &entry)
	if readErr != nil {
		return roachpb.NewError(newReplicaCorruptionError(util.Errorf("could not read from sequence cache"), readErr))
	}
	var err error
	switch {
	case txn.Epoch > epoch:
		// No cache hit from current or future epoch, continue.
	case sequence == SequencePoisonAbort:
		// We were poisoned, which means that our Transaction has been
		// aborted and learns about that right now.
		err = roachpb.NewTransactionAbortedError()
	case sequence == SequencePoisonRestart:
		// Same, but we got restarted.
		err = roachpb.NewTransactionRetryError()
	case sequence >= txn.Sequence:
		err = roachpb.NewTransactionRetryError()
	}
	if err != nil {
		// We hit the cache, so let the transaction restart.
		if log.V(1) {
			log.Infof("found sequence cache entry for %s@epo=%d,seq=%d: epo=%d,seq=%d", txn.Short(),
				txn.Epoch, txn.Sequence, epoch, sequence)
		}
		newTxn := txn.Clone()
		newTxn.Timestamp.Forward(entry.Timestamp)
		return roachpb.NewErrorWithTxn(err, &newTxn)
	}
	return nil
}

type intentsWithArg struct {
	args    roachpb.Request
	intents []roachpb.Intent
}

func (r *Replica) executeBatch(batch engine.Engine, ms *engine.MVCCStats, ba roachpb.BatchRequest) (*roachpb.BatchResponse, []intentsWithArg, *roachpb.Error) {
	br := &roachpb.BatchResponse{}
	var intents []intentsWithArg
	// If transactional, we use ba.Txn for each individual command and
	// accumulate updates to it.
	isTxn := ba.Txn != nil

	if ba.Requests[0].GetInner().Method() == roachpb.BeginTransaction &&
		(len(ba.Requests) < 2 || !roachpb.IsTransactionWrite(ba.Requests[1].GetInner())) {
		// We rely on this fact for cleaning out sequence cache entries as
		// early as possible. See the implementation of EndTransaction for
		// details.
		return nil, nil, roachpb.NewError(util.Errorf("BeginTransaction must be followed by a transactional write: %s", ba))

	}

	// Ensure that timestamps on individual requests are offset by an incremental
	// amount of logical ticks from the base timestamp of the batch request (if
	// that is non-zero and the batch is not in a Txn). This has the effect of
	// allowing self-overlapping commands within the batch, which would otherwise
	// result in an endless loop of WriteTooOldErrors.
	// TODO(tschottdorf): discuss self-overlap.
	// TODO(tschottdorf): provisional feature to get back to passing tests.
	// Have to discuss how we go about it.
	fiddleWithTimestamps := !isTxn && ba.IsWrite()

	remScanResults := int64(math.MaxInt64)
	if ba.Header.MaxScanResults != 0 {
		// We have a batch of Scan or ReverseScan requests with a limit. We keep track of how many
		// remaining results we can return.
		remScanResults = ba.Header.MaxScanResults
	}

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

		header := ba.Header

		// TODO(tschottdorf): Special exception because of pending
		// self-overlap discussion.
		// TODO(tschottdorf): currently treating PushTxn specially. Otherwise
		// conflict resolution would break because a batch full of pushes of
		// the same Txn for different intents will push the pushee in
		// iteration, bumping up its priority. Unfortunately PushTxn is flagged
		// as a write command. Interim solution.
		// Note that being in a txn implies no fiddling.
		if fiddleWithTimestamps && args.Method() != roachpb.PushTxn {
			header.Timestamp = ba.Timestamp.Add(0, int32(index))
		}

		reply, curIntents, pErr := r.executeCmd(batch, ms, header, remScanResults, args)

		// Collect intents skipped over the course of execution.
		if len(curIntents) > 0 {
			// TODO(tschottdorf): see about refactoring the args away.
			intents = append(intents, intentsWithArg{args: args, intents: curIntents})
		}

		if pErr != nil {
			// Initialize the error index.
			pErr.SetErrorIndex(int32(index))
			return nil, intents, pErr
		}
		if remScanResults != math.MaxInt64 {
			if cReply, ok := reply.(roachpb.Countable); ok {
				retResults := cReply.Count()
				if retResults > remScanResults {
					panic(fmt.Sprintf("received %d results, limit was %d", retResults, remScanResults))
				}
				remScanResults -= retResults
			}
		}

		// Add the response to the batch, updating the timestamp.
		reply.Header().Timestamp.Forward(header.Timestamp)
		br.Add(reply)
		if isTxn {
			if txn := reply.Header().Txn; txn != nil {
				ba.Txn.Update(txn)
			}
		}
	}
	// If transactional, send out the final transaction entry with the reply.
	if isTxn {
		br.Txn = ba.Txn
		br.Txn.Timestamp.Forward(ba.Timestamp)
		// If this is the beginning of the write portion of a transaction,
		// mark the returned transaction as Writing. It's important that
		// we do this only at the end, since updating at the first write
		// could "poison" the transaction on restartable errors, see #2920.
		// TODO(tschottdorf): #4442.
		if _, ok := ba.GetArg(roachpb.BeginTransaction); ok {
			br.Txn.Writing = true
		}
	}
	return br, intents, nil
}

// getLeaseForGossip tries to obtain a leader lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *roachpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, roachpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *roachpb.Error
	if !r.store.Stopper().RunTask(func() {
		// Check for or obtain the lease, if none active.
		sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
		defer cleanupSp()
		pErr = r.redirectOnOrAcquireLeaderLease(sp, ctx)
		hasLease = pErr == nil
		if pErr != nil {
			switch e := pErr.GetDetail().(type) {
			case *roachpb.NotLeaderError:
				// NotLeaderError means there is an active lease, but only if
				// the leader is set; otherwise, it's likely a timeout.
				if e.Leader != nil {
					pErr = nil
				}
			case *roachpb.LeaseRejectedError:
				// leaseRejectedError means we tried to get one but someone
				// beat us to it.
				pErr = nil
			default:
				// Any other error is worth being logged visibly.
				log.Warningc(ctx, "could not acquire lease for range gossip: %s", e)
			}
		}
	}) {
		pErr = roachpb.NewErrorf("node is stopping")
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a leader lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange() *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	ctx := r.context()

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if uuidBytes, err := r.store.Gossip().GetInfo(gossip.KeyClusterID); err == nil {
		if gossipClusterID, err := uuid.FromBytes(uuidBytes); err == nil {
			if *gossipClusterID != r.store.ClusterID() {
				log.Fatalc(ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
					r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
			}
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infoc(ctx, "gossiping cluster id %q from store %d, range %d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeyClusterID, r.store.ClusterID().Bytes(), 0*time.Second); err != nil {
		log.Errorc(ctx, "failed to gossip cluster ID: %s", err)
	}
	if ok, pErr := r.getLeaseForGossip(ctx); !ok || pErr != nil {
		return pErr
	}
	if log.V(1) {
		log.Infoc(ctx, "gossiping sentinel from store %d, range %d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeySentinel, r.store.ClusterID().Bytes(), sentinelGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip sentinel: %s", err)
	}
	if log.V(1) {
		log.Infoc(ctx, "gossiping first range from store %d, range %d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfoProto(gossip.KeyFirstRangeDescriptor, r.Desc(), configGossipTTL); err != nil {
		log.Errorc(ctx, "failed to gossip first range metadata: %s", err)
	}
	return nil
}

// maybeGossipSystemConfig scans the entire SystemConfig span and gossips it.
// The first call is on NewReplica. Further calls come from the trigger on an
// EndTransactionRequest.
//
// Note that maybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a leader lease
// here since LeaderLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrAcquireLeaderLease.
// TODO(tschottdorf): Can possibly simplify.
func (r *Replica) maybeGossipSystemConfig() {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return
	}

	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		return
	}

	if lease := r.getLeaderLease(); !lease.OwnedBy(r.store.StoreID()) || !lease.Covers(r.store.Clock().Now()) {
		// Do not gossip when a leader lease is not held.
		return
	}

	ctx := r.context()
	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	kvs, hash, err := r.loadSystemConfigSpan()
	if err != nil {
		log.Errorc(ctx, "could not load SystemConfig span: %s", err)
		return
	}
	if bytes.Equal(r.systemDBHash, hash) {
		return
	}

	if log.V(1) {
		log.Infoc(ctx, "gossiping system config from store %d, range %d, hash %x",
			r.store.StoreID(), r.RangeID, hash)
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
	ctx := r.context()
	stopper := r.store.Stopper()

	for _, item := range intents {
		// TODO(tschottdorf): avoid data race related to batch unrolling in ExecuteCmd;
		// can probably go again when that provisional code there is gone. Should
		// still be careful though, a retry could happen and race with args.
		args := util.CloneProto(item.args).(roachpb.Request)
		stopper.RunAsyncTask(func() {
			// Everything here is best effort; give up rather than waiting
			// too long (helps avoid deadlocks during test shutdown,
			// although this is imperfect due to the use of an
			// uninterruptible WaitGroup.Wait in beginCmds).
			ctxWithTimeout, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
			defer cancel()
			h := roachpb.Header{Timestamp: now}
			resolveIntents, pErr := r.store.resolveWriteIntentError(ctxWithTimeout, &roachpb.WriteIntentError{
				Intents: item.intents,
			}, r, args, h, roachpb.PUSH_TOUCH)
			if wiErr, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok || wiErr == nil || !wiErr.Resolved {
				log.Warningc(ctxWithTimeout, "failed to push during intent resolution: %s", pErr)
				return
			}
			if pErr := r.resolveIntents(ctxWithTimeout, resolveIntents, true /* wait */, false /* TODO(tschottdorf): #5088 */); pErr != nil {
				log.Warningc(ctxWithTimeout, "failed to resolve intents: %s", pErr)
				return
			}
			// We successfully resolved the intents, so we're able to GC from
			// the txn span directly. Note that the sequence cache was cleared
			// out synchronously with EndTransaction (see comments within for
			// an explanation of why that is kosher).
			//
			// Note that we poisoned the sequence caches on the external ranges
			// above. This may seem counter-intuitive, but it's actually
			// necessary: Assume a transaction has committed here, with two
			// external intents, and assume that we did not poison. Normally,
			// these two intents would be resolved in the same batch, but that
			// is not guaranteed (for example, if DistSender has a stale
			// descriptor after a Merge). When resolved separately, the first
			// ResolveIntent would clear out the sequence cache; an individual
			// write on the second (still present) intent could then be
			// replayed and would resolve to a real value (at least for a
			// window of time unless we delete the local txn entry). That's not
			// OK for non-idempotent commands such as Increment.
			// TODO(tschottdorf): We should have another side effect on
			// MVCCResolveIntent (on commit/abort): If it were able to remove
			// the txn from its corresponding entries in the timestamp cache,
			// no more replays at the same timestamp would be possible. This
			// appears to be a useful performance optimization; we could then
			// not poison on EndTransaction. In fact, the above mechanism
			// could be an effective alternative to sequence-cache based
			// poisoning (or the whole sequence cache?) itself.
			//
			// TODO(tschottdorf): down the road, can probably unclog the system
			// here by batching up a bunch of those GCRequests before proposing.
			if args.Method() == roachpb.EndTransaction {
				var ba roachpb.BatchRequest
				txn := item.intents[0].Txn
				gcArgs := roachpb.GCRequest{
					Span: roachpb.Span{
						Key:    r.Desc().StartKey.AsRawKey(),
						EndKey: r.Desc().EndKey.AsRawKey(),
					},
				}
				gcArgs.Keys = append(gcArgs.Keys, roachpb.GCRequest_GCKey{Key: keys.TransactionKey(txn.Key, txn.ID)})

				ba.Add(&gcArgs)
				if _, pErr := r.addWriteCmd(ctxWithTimeout, ba, nil /* nil */); pErr != nil {
					log.Warningf("could not GC completed transaction: %s", pErr)
				}
			}
		})
	}
}

// newReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func newReplicaCorruptionError(errs ...error) *roachpb.ReplicaCorruptionError {
	var errMsg string
	for i := range errs {
		err := errs[len(errs)-i-1]
		if len(errMsg) == 0 {
			errMsg = err.Error()
		} else {
			errMsg = fmt.Sprintf("%s (caused by %s)", err, errMsg)
		}
	}
	return &roachpb.ReplicaCorruptionError{ErrorMsg: errMsg}
}

// maybeSetCorrupt is a stand-in for proper handling of failing replicas. Such a
// failure is indicated by a call to maybeSetCorrupt with a ReplicaCorruptionError.
// Currently any error is passed through, but prospectively it should stop the
// range from participating in progress, trigger a rebalance operation and
// decide on an error-by-error basis whether the corruption is limited to the
// range, store, node or cluster with corresponding actions taken.
func (r *Replica) maybeSetCorrupt(pErr *roachpb.Error) *roachpb.Error {
	if cErr, ok := pErr.GetDetail().(*roachpb.ReplicaCorruptionError); ok {
		log.Errorc(r.context(), "stalling replica due to: %s", cErr.ErrorMsg)
		cErr.Processed = true
		return roachpb.NewError(cErr)
	}
	return pErr
}

// resolveIntents resolves the given intents. For those which are local to the
// range, we submit directly to the range-local Raft instance; all non-local
// intents are resolved asynchronously in a batch. If `wait` is true, all
// operations are carried out synchronously and an error is returned.
// Otherwise, the call returns without error as soon as all local resolve
// commands have been **proposed** (not executed). This ensures that if a
// waiting client retries immediately after calling this function, it will not
// hit the same intents again.
func (r *Replica) resolveIntents(ctx context.Context, intents []roachpb.Intent, wait bool, poison bool) *roachpb.Error {
	sp, cleanupSp := tracing.SpanFromContext(opReplica, r.store.Tracer(), ctx)
	defer cleanupSp()

	ctx = opentracing.ContextWithSpan(ctx, nil) // we're doing async stuff below; those need new traces
	sp.LogEvent(fmt.Sprintf("resolving intents [wait=%t]", wait))

	var reqsRemote []roachpb.Request
	baLocal := roachpb.BatchRequest{}
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		var resolveArgs roachpb.Request
		var local bool // whether this intent lives on this Range
		{
			if len(intent.EndKey) == 0 {
				resolveArgs = &roachpb.ResolveIntentRequest{
					Span:      intent.Span,
					IntentTxn: intent.Txn,
					Status:    intent.Status,
					Poison:    poison,
				}
				local = r.ContainsKey(intent.Key)
			} else {
				resolveArgs = &roachpb.ResolveIntentRangeRequest{
					Span:      intent.Span,
					IntentTxn: intent.Txn,
					Status:    intent.Status,
					Poison:    poison,
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
		action := func() *roachpb.Error {
			// Trace this under the ID of the intent owner.
			sp := r.store.Tracer().StartSpan("resolve intents")
			defer sp.Finish()
			ctx = opentracing.ContextWithSpan(ctx, sp)
			// Always operate with a timeout when resolving intents: this
			// prevents rare shutdown timeouts in tests.
			ctxWithTimeout, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
			defer cancel()
			_, pErr := r.addWriteCmd(ctxWithTimeout, baLocal, &wg)
			return pErr
		}
		wg.Add(1)
		if wait || !r.store.Stopper().RunAsyncTask(func() {
			if err := action(); err != nil {
				log.Warningf("unable to resolve local intents; %s", err)
			}
		}) {
			// Still run the task when draining. Our caller already has a task and
			// going async here again is merely for performance, but some intents
			// need to be resolved because they might block other tasks. See #1684.
			// Note that handleSkippedIntents has a TODO in case #1684 comes back.
			if err := action(); err != nil {
				return err
			}
		}
	}

	// Resolve all of the intents which aren't local to the Range.
	if len(reqsRemote) > 0 {
		b := &client.Batch{}
		b.InternalAddRequest(reqsRemote...)
		action := func() *roachpb.Error {
			// TODO(tschottdorf): no tracing here yet.
			return r.store.DB().Run(b)
		}
		if wait || !r.store.Stopper().RunAsyncTask(func() {
			if err := action(); err != nil {
				log.Warningf("unable to resolve external intents: %s", err)
			}
		}) {
			// As with local intents, try async to not keep the caller waiting, but
			// when draining just go ahead and do it synchronously. See #1684.
			if err := action(); err != nil {
				return err
			}
		}
	}

	// Wait until the local ResolveIntents batch has been submitted to
	// raft. No-op if all were non-local.
	wg.Wait()
	return nil
}

var errSystemConfigIntent = errors.New("must retry later due to intent on SystemConfigSpan")

// loadSystemConfigSpan scans the entire SystemConfig span and returns the full
// list of key/value pairs along with the sha1 checksum of the contents (key
// and value).
func (r *Replica) loadSystemConfigSpan() ([]roachpb.KeyValue, []byte, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: keys.SystemConfigSpan})
	br, intents, pErr := r.executeBatch(r.store.Engine(), nil, ba)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	if len(intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		r.handleSkippedIntents(intents)
		return nil, nil, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	return kvs, config.SystemConfig{Values: kvs}.Hash(), nil
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
