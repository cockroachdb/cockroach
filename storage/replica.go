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
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/kr/pretty"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// sentinelGossipTTL is time-to-live for the gossip sentinel. The
	// sentinel informs a node whether or not it's connected to the
	// primary gossip network and not just a partition. As such it must
	// expire on a reasonable basis and be continually re-gossiped. The
	// replica which is the lease holder of the first range gossips it.
	sentinelGossipTTL = 2 * time.Minute
	// sentinelGossipInterval is the approximate interval at which the
	// sentinel info is gossiped.
	sentinelGossipInterval = sentinelGossipTTL / 2

	// configGossipTTL is the time-to-live for configuration maps.
	configGossipTTL = 0 // does not expire
	// configGossipInterval is the interval at which range lease holders gossip
	// their config maps. Even if config maps do not expire, we still
	// need a periodic gossip to safeguard against failure of a lease holder
	// to gossip after performing an update to the map.
	configGossipInterval = 1 * time.Minute
	// optimizePutThreshold is the minimum length of a contiguous run
	// of batched puts or conditional puts, after which the constituent
	// put operations will possibly be optimized by determining whether
	// the key space being written is starting out empty.
	optimizePutThreshold = 10

	replicaChangeTxnName = "change-replica"
)

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var txnAutoGC = true

// raftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5
)

// consultsTimestampCacheMethods specifies the set of methods which
// consult the timestamp cache. This syntax creates a sparse array
// with maximum index equal to the value of the final Method. Unused
// indexes default to false.
var consultsTimestampCacheMethods = [...]bool{
	roachpb.Put:              true,
	roachpb.ConditionalPut:   true,
	roachpb.Increment:        true,
	roachpb.Delete:           true,
	roachpb.DeleteRange:      true,
	roachpb.BeginTransaction: true,
}

func consultsTimestampCache(r roachpb.Request) bool {
	m := r.Method()
	if m < 0 || m >= roachpb.Method(len(consultsTimestampCacheMethods)) {
		return false
	}
	return consultsTimestampCacheMethods[m]
}

// updatesTimestampCacheMethods specifies the set of methods which if
// successful will update the timestamp cache.
var updatesTimestampCacheMethods = [...]bool{
	roachpb.Get: true,
	// ConditionalPut effectively reads and may not write, so must
	// update the timestamp cache.
	roachpb.ConditionalPut: true,
	// DeleteRange updates the write timestamp cache as it doesn't leave
	// intents or tombstones for keys which don't yet exist. By updating
	// the write timestamp cache, it forces subsequent writes to get a
	// write-too-old error and avoids the phantom delete anomaly.
	roachpb.DeleteRange: true,
	roachpb.Scan:        true,
	roachpb.ReverseScan: true,
	// EndTransaction updates the write timestamp cache to prevent
	// replays. Replays for the same transaction key and timestamp will
	// have Txn.WriteTooOld=true and must retry on EndTransaction.
	roachpb.EndTransaction: true,
}

func updatesTimestampCache(r roachpb.Request) bool {
	m := r.Method()
	if m < 0 || m >= roachpb.Method(len(updatesTimestampCacheMethods)) {
		return false
	}
	return updatesTimestampCacheMethods[m]
}

var errReplicaTooOld = grpc.Errorf(codes.Aborted, "sender replica too old, discarding message")

// A pendingCmd holds a done channel for a command sent to Raft. Once
// committed to the Raft log, the command is executed and the result returned
// via the done channel.
type pendingCmd struct {
	ctx context.Context
	// TODO(tschottdorf): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, requires adapting tryAbandon.
	idKey           storagebase.CmdIDKey
	proposedAtTicks int
	raftCmd         roachpb.RaftCommand
	done            chan roachpb.ResponseWithError // Used to signal waiting RPC handler
}

type replicaChecksum struct {
	// Computed checksum. This is set to nil on error.
	checksum []byte
	// GC this checksum after this timestamp. The timestamp is valid only
	// after the checksum has been computed.
	gcTimestamp time.Time
	// This channel is closed after the checksum is computed, and is used
	// as a notification.
	notify chan struct{}

	// Some debug output that can be added to the VerifyChecksumRequest.
	snapshot *roachpb.RaftSnapshotData
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	// TODO(tschottdorf): Duplicates r.mu.state.desc.RangeID; revisit that.
	RangeID roachpb.RangeID // Should only be set by the constructor.
	store   *Store
	// sha1 hash of the system config @ last gossip. No synchronized access;
	// must only be accessed from maybeGossipSystemConfig (which in turn is
	// only called from the Raft-processing goroutine).
	systemDBHash []byte
	abortCache   *AbortCache // Avoids anomalous reads after abort
	raftSender   RaftSender

	// creatingReplica is set when a replica is created as uninitialized
	// via a raft message.
	creatingReplica *roachpb.ReplicaDescriptor

	// Held in read mode during read-only commands. Held in exclusive mode to
	// prevent read-only commands from executing. Acquired before the embedded
	// RWMutex.
	readOnlyCmdMu syncutil.RWMutex

	// rangeDesc is a *RangeDescriptor that can be atomically read from in
	// replica.Desc() without needing to acquire the replica.mu lock. All
	// updates to state.Desc should be duplicated here
	rangeDesc atomic.Value

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.Mutex
		// Has the replica been destroyed.
		destroyed error
		// The state of the Raft state machine.
		state storagebase.ReplicaState
		// Counter used for assigning lease indexes for proposals.
		lastAssignedLeaseIndex uint64
		// Enforces at most one command is running per key(s).
		cmdQ *CommandQueue
		// Last index persisted to the raft log (not necessarily committed).
		lastIndex uint64
		// raftLogSize is the approximate size in bytes of the persisted raft log.
		// On server restart, this value is assumed to be zero to avoid costly scans
		// of the raft log. This will be correct when all log entries predating this
		// process have been truncated.
		raftLogSize int64
		// pendingLeaseRequest is used to coalesce RequestLease requests.
		pendingLeaseRequest pendingLeaseRequest
		// Max bytes before split.
		maxBytes int64
		// pendingCmds stores the Raft in-flight commands which
		// originated at this Replica, i.e. all commands for which
		// proposeRaftCommand has been called, but which have not yet
		// applied.
		//
		// TODO(tschottdorf): evaluate whether this should be a list/slice.
		pendingCmds       map[storagebase.CmdIDKey]*pendingCmd
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID roachpb.ReplicaID

		// The last seen replica descriptors from incoming Raft messages. These are
		// stored so that the replica still knows the replica descriptors for itself
		// and for its message recipients in the circumstances when its RangeDescriptor
		// is out of date.
		//
		// Normally, a replica knows about the other replica descriptors for a
		// range via the RangeDescriptor stored in Replica.mu.state.Desc. But that
		// descriptor is only updated during a Split or ChangeReplicas operation.
		// There are periods during a Replica's lifetime when that information is
		// out of date:
		//
		// 1. When a replica is being newly created as the result of an incoming
		// Raft message for it. This is the common case for ChangeReplicas and an
		// uncommon case for Splits. The leader will be sending the replica
		// messages and the replica needs to be able to respond before it can
		// receive an updated range descriptor (via a snapshot,
		// changeReplicasTrigger, or splitTrigger).
		//
		// 2. If the node containing a replica is partitioned or down while the
		// replicas for the range are updated. When the node comes back up, other
		// replicas may begin communicating with it and it needs to be able to
		// respond. Unlike 1 where there is no range descriptor, in this situation
		// the replica has a range descriptor but it is out of date. Note that a
		// replica being removed from a node and then quickly re-added before the
		// replica has been GC'd will also use the last seen descriptors. In
		// effect, this is another path for which the replica's local range
		// descriptor is out of date.
		//
		// The last seen replica descriptors are updated on receipt of every raft
		// message via Replica.setLastReplicaDescriptors (see
		// Store.handleRaftMessage). These last seen descriptors are used when
		// the replica's RangeDescriptor contains missing or out of date descriptors
		// for a replica (see Replica.sendRaftMessage).
		//
		// Removing a replica from Store.mu.replicas is not a problem because
		// when a replica is completely removed, it won't be recreated until
		// there is another event that will repopulate the replicas map in the
		// range descriptor. When it is temporarily dropped and recreated, the
		// newly recreated replica will have a complete range descriptor.
		lastToReplica, lastFromReplica roachpb.ReplicaDescriptor

		// Most recent timestamps for keys / key ranges.
		tsCache *timestampCache
		// proposeRaftCommandFn can be set to mock out the propose operation.
		proposeRaftCommandFn func(*pendingCmd) error
		// Computed checksum at a snapshot UUID.
		checksums map[uuid.UUID]replicaChecksum

		// Set to an open channel while a snapshot is being generated.
		// When no snapshot is in progress, this field may either be nil
		// or a closed channel. If an error occurs during generation,
		// this channel may be closed without producing a result.
		snapshotChan chan raftpb.Snapshot

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int
	}
}

// withRaftGroupLocked calls the supplied function with the (lazily
// initialized) Raft group. It assumes that the Replica lock is held.
func (r *Replica) withRaftGroupLocked(f func(r *raft.RawNode) error) error {
	if r.mu.destroyed != nil {
		// Silently ignore all operations on destroyed replicas. We can't return an
		// error here as all errors returned from this method are considered fatal.
		return nil
	}

	if r.mu.replicaID == 0 {
		// The replica's raft group has not yet been configured (i.e. the replica
		// was created from a preemptive snapshot).
		return nil
	}

	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage(r),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.ctx,
			&raftLogger{stringer: r},
		), nil)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup

		// Automatically campaign and elect a leader for this group if there's
		// exactly one known node for this group.
		//
		// A grey area for this being correct happens in the case when we're
		// currently in the process of adding a second node to the group,
		// with the change committed but not applied.
		// Upon restarting, the first node would immediately elect itself and only
		// then apply the config change, where really it should be applying
		// first and then waiting for the majority (which would now require
		// two votes, not only its own).
		// However, in that special case, the second node has no chance to
		// be elected leader while the first node restarts (as it's aware of the
		// configuration and knows it needs two votes), so the worst that
		// could happen is both nodes ending up in candidate state, timing
		// out and then voting again. This is expected to be an extremely
		// rare event.
		if len(r.mu.state.Desc.Replicas) == 1 && r.mu.state.Desc.Replicas[0].ReplicaID == r.mu.replicaID {
			if err := raftGroup.Campaign(); err != nil {
				return err
			}
		}
	}

	return f(r.mu.internalRaftGroup)
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
func (r *Replica) withRaftGroup(f func(r *raft.RawNode) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(f)
}

var _ client.Sender = &Replica{}

// NewReplica initializes the replica using the given metadata. If the
// replica is initialized (i.e. desc contains more than a RangeID),
// replicaID should be 0 and the replicaID will be discovered from the
// descriptor.
func NewReplica(desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID) (*Replica, error) {
	r := &Replica{
		RangeID:    desc.RangeID,
		store:      store,
		abortCache: NewAbortCache(desc.RangeID),
	}

	r.raftSender = store.ctx.Transport.MakeSender(
		func(err error, toReplica roachpb.ReplicaDescriptor) {
			ctx := context.TODO() // plumb the context from transport
			if grpcutil.ErrorEqual(err, errReplicaTooOld) {
				if err := r.store.Stopper().RunTask(func() {
					r.mu.Lock()
					repID := r.mu.replicaID
					r.mu.Unlock()
					log.Infof(ctx, "%s: replica %d too old, adding to replica GC queue", r, repID)

					if err := r.store.replicaGCQueue.Add(r, 1.0); err != nil {
						log.Errorf(ctx, "%s: unable to add replica %d to GC queue: %s", r, repID, err)
					}
				}); err != nil {
					log.Errorf(ctx, "%s: %s", r, err)
				}
				return
			}
			if err != nil && !grpcutil.IsClosedConnection(err) {
				log.Warningf(ctx,
					"%s: outgoing raft transport stream to %s closed by the remote: %s",
					r, toReplica, err)
			}
		})

	if err := r.newReplicaInner(desc, store.Clock(), replicaID); err != nil {
		return nil, err
	}

	r.maybeGossipSystemConfig()
	return r, nil
}

func (r *Replica) newReplicaInner(desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mu.cmdQ = NewCommandQueue()
	r.mu.tsCache = newTimestampCache(clock)
	r.mu.pendingCmds = map[storagebase.CmdIDKey]*pendingCmd{}
	r.mu.checksums = map[uuid.UUID]replicaChecksum{}

	var err error
	ctx := context.TODO()

	if r.mu.state, err = loadState(ctx, r.store.Engine(), desc); err != nil {
		return err
	}
	r.rangeDesc.Store(r.mu.state.Desc)

	r.mu.lastIndex, err = loadLastIndex(ctx, r.store.Engine(), r.RangeID)
	if err != nil {
		return err
	}

	pErr, err := loadReplicaDestroyedError(ctx, r.store.Engine(), r.RangeID)
	if err != nil {
		return err
	}
	r.mu.destroyed = pErr.GetDetail()

	if r.isInitializedLocked() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}
	if replicaID == 0 {
		repDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID())
		if !ok {
			// This is intentionally not an error and is the code path exercised
			// during preemptive snapshots. The replica ID will be sent when the
			// actual raft replica change occurs.
			return nil
		}
		replicaID = repDesc.ReplicaID
	}
	if err := r.setReplicaIDLocked(replicaID); err != nil {
		return err
	}
	r.assertStateLocked(r.store.Engine())
	return nil
}

// String returns the string representation of the replica using an
// inconsistent copy of the range descriptor. Therefore, String does not
// require a lock and its output may not be atomic with other ongoing work in
// the replica. This is done to prevent deadlocks in logging sites.
func (r *Replica) String() string {
	inconsistentDesc := r.rangeDesc.Load().(*roachpb.RangeDescriptor)
	return fmt.Sprintf("%s range=%d [%s-%s)", r.store,
		inconsistentDesc.RangeID, inconsistentDesc.StartKey, inconsistentDesc.EndKey)
}

// Destroy clears pending command queue by sending each pending
// command an error and cleans up all data associated with this range.
func (r *Replica) Destroy(origDesc roachpb.RangeDescriptor) error {
	desc := r.Desc()
	if repDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID()); ok && repDesc.ReplicaID >= origDesc.NextReplicaID {
		return errors.Errorf("cannot destroy replica %s; replica ID has changed (%s >= %s)",
			r, repDesc.ReplicaID, origDesc.NextReplicaID)
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
	r.mu.pendingCmds = map[storagebase.CmdIDKey]*pendingCmd{}
	r.mu.internalRaftGroup = nil
	r.mu.destroyed = errors.Errorf("replica %d (range %d) was garbage collected",
		r.mu.replicaID, r.RangeID)
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
		return errors.Errorf("replicaID cannot move backwards from %d to %d", r.mu.replicaID, replicaID)
	} else if r.mu.replicaID != 0 {
		// TODO(bdarnell): clean up previous raftGroup (update peers)
	}

	previousReplicaID := r.mu.replicaID
	r.mu.replicaID = replicaID
	// Reset the raft group to force its recreation on next usage.
	r.mu.internalRaftGroup = nil

	// If there was a previous replica, repropose its pending commands under
	// this new incarnation.
	if previousReplicaID != 0 {
		// propose pending commands under new replicaID
		if err := r.refreshPendingCmdsLocked(reasonReplicaIDChanged, 0); err != nil {
			return err
		}
	}

	return nil
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
	return r.RangeID == 1
}

// getLease returns the current lease, and the tentative next one, if a lease
// request initiated by this replica is in progress.
func (r *Replica) getLease() (*roachpb.Lease, *roachpb.Lease) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.state.Lease, r.mu.pendingLeaseRequest.RequestPending()
}

// newNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func newNotLeaseHolderError(
	l *roachpb.Lease,
	originStoreID roachpb.StoreID,
	rangeDesc *roachpb.RangeDescriptor,
) error {
	err := &roachpb.NotLeaseHolderError{
		RangeID: rangeDesc.RangeID,
	}
	err.Replica, _ = rangeDesc.GetReplicaDescriptor(originStoreID)
	if l != nil {
		// Morally, we return the lease-holding Replica here. However, in the
		// case in which a leader removes itself, we want the followers to
		// avoid handing out a misleading clue (which in itself shouldn't be
		// overly disruptive as the lease would expire and then this method
		// shouldn't be called for it any more, but at the very least it
		// could catch tests in a loop, presumably due to manual clocks).
		_, stillMember := rangeDesc.GetReplicaDescriptor(l.Replica.StoreID)
		if stillMember {
			err.LeaseHolder = &l.Replica
		}
	}
	return err
}

// redirectOnOrAcquireLease checks whether this replica has the lease at the
// current timestamp. If it does, returns success. If another replica currently
// holds the lease, redirects by returning NotLeaseHolderError. If the lease is
// expired, a renewal is synchronously requested. This method uses the
// pendingLeaseRequest structure to guarantee only one request to grant the
// lease is pending. Leases are eagerly renewed when a request with a timestamp
// close to the beginning of the stasis period is served.
//
// TODO(spencer): for write commands, don't wait while requesting
//  the range lease. If the lease acquisition fails, the write cmd
//  will fail as well. If it succeeds, as is likely, then the write
//  will not incur latency waiting for the command to complete.
//  Reads, however, must wait.
func (r *Replica) redirectOnOrAcquireLease(ctx context.Context) *roachpb.Error {
	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		llChan, pErr := func() (<-chan *roachpb.Error, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			lease := r.mu.state.Lease
			if lease.Covers(timestamp) {
				if !lease.OwnedBy(r.store.StoreID()) {
					// If lease is currently held by another, redirect to holder.
					return nil, roachpb.NewError(
						newNotLeaseHolderError(lease, r.store.StoreID(), r.mu.state.Desc))
				}
				// Check that we're not in the process of transferring the lease away.
				// If we are transferring the lease away, we can't serve reads or
				// propose Raft commands - see comments on TransferLease.
				// TODO(andrei): If the lease is being transferred, consider returning a
				// new error type so the client backs off until the transfer is
				// completed.
				repDesc, err := r.getReplicaDescriptorLocked()
				if err != nil {
					return nil, roachpb.NewError(err)
				}
				transferLease := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID)
				if transferLease != nil {
					return nil, roachpb.NewError(
						newNotLeaseHolderError(transferLease, r.store.StoreID(), r.mu.state.Desc))
				}

				// Should we extend the lease?
				if (r.mu.pendingLeaseRequest.RequestPending() == nil) &&
					!timestamp.Less(lease.StartStasis.Add(-int64(r.store.ctx.rangeLeaseRenewalDuration), 0)) {
					if log.V(2) {
						log.Warningf(ctx, "%s: extending lease %s at %s", r, lease, timestamp)
					}
					// We had an active lease to begin with, but we want to trigger
					// a lease extension. We don't need to wait for that extension
					// to go through and simply ignore the returned channel (which
					// is buffered).
					_ = r.requestLeaseLocked(timestamp)
				}
				// Return a nil chan to signal that we have a valid lease.
				return nil, nil
			}
			log.Tracef(ctx, "request range lease (attempt #%d)", attempt)

			// No active lease: Request renewal if a renewal is not already pending.
			return r.requestLeaseLocked(timestamp), nil
		}()
		if pErr != nil {
			return pErr
		}
		if llChan == nil {
			// We own a covering lease.
			return nil
		}

		// Wait for the range lease to finish, or the context to expire.
		select {
		case pErr := <-llChan:
			if pErr != nil {
				// Getting a LeaseRejectedError back means someone else got there
				// first, or the lease request was somehow invalid due to a
				// concurrent change. Convert the error to a NotLeaseHolderError.
				if _, ok := pErr.GetDetail().(*roachpb.LeaseRejectedError); ok {
					lease, _ := r.getLease()
					if !lease.Covers(r.store.Clock().Now()) {
						lease = nil
					}
					return roachpb.NewError(newNotLeaseHolderError(lease, r.store.StoreID(), r.Desc()))
				}
				return pErr
			}
			continue
		case <-ctx.Done():
		case <-r.store.Stopper().ShouldStop():
		}
		return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
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
	return r.mu.state.Desc.IsInitialized()
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.state.Desc
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the Store handle the descriptor
// update.
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
		r.panicf("range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}

	// NB: If we used rangeDesc for anything but informational purposes, the
	// order here would be crucial.
	r.rangeDesc.Store(desc)
	r.mu.state.Desc = desc
}

// GetReplicaDescriptor returns the replica for this range from the range
// descriptor. Returns a *RangeNotFoundError if the replica is not found.
// No other errors are returned.
func (r *Replica) GetReplicaDescriptor() (roachpb.ReplicaDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getReplicaDescriptorLocked()
}

// getReplicaDescriptorLocked is like getReplicaDescriptor, but assumes that r.mu is held.
func (r *Replica) getReplicaDescriptorLocked() (roachpb.ReplicaDescriptor, error) {
	repDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(r.store.StoreID())
	if ok {
		return repDesc, nil
	}
	return roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(r.RangeID)
}

// setLastReplicaDescriptors sets the the most recently seen replica descriptors to those
// contained in the *RaftMessageRequest, acquiring r.mu to do so.
func (r *Replica) setLastReplicaDescriptors(req *RaftMessageRequest) {
	r.mu.Lock()
	r.mu.lastFromReplica = req.FromReplica
	r.mu.lastToReplica = req.ToReplica
	r.mu.Unlock()
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Replica) GetMVCCStats() enginepb.MVCCStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.state.Stats
}

// ContainsKey returns whether this range contains the specified key.
func (r *Replica) ContainsKey(key roachpb.Key) bool {
	return containsKey(*r.Desc(), key)
}

func containsKey(desc roachpb.RangeDescriptor, key roachpb.Key) bool {
	if bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return bytes.HasPrefix(key, keys.MakeRangeIDPrefix(desc.RangeID))
	}
	keyAddr, err := keys.Addr(key)
	if err != nil {
		return false
	}
	return desc.ContainsKey(keyAddr)
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end roachpb.Key) bool {
	return containsKeyRange(*r.Desc(), start, end)
}

func containsKeyRange(desc roachpb.RangeDescriptor, start, end roachpb.Key) bool {
	startKeyAddr, err := keys.Addr(start)
	if err != nil {
		return false
	}
	endKeyAddr, err := keys.Addr(end)
	if err != nil {
		return false
	}
	return desc.ContainsKeyRange(startKeyAddr, endKeyAddr)
}

// getLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for garbage collection.
//
// TODO(tschottdorf): we may want to phase this out in favor of using
// gcThreshold.
func (r *Replica) getLastReplicaGCTimestamp() (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	timestamp := hlc.Timestamp{}
	_, err := engine.MVCCGetProto(context.Background(), r.store.Engine(), key, hlc.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return hlc.ZeroTimestamp, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(timestamp hlc.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return engine.MVCCPutProto(context.Background(), r.store.Engine(), nil, key, hlc.ZeroTimestamp, nil, &timestamp)
}

// getLastVerificationTimestamp reads the timestamp at which the replica's
// data was last verified.
func (r *Replica) getLastVerificationTimestamp() (hlc.Timestamp, error) {
	key := keys.RangeLastVerificationTimestampKey(r.RangeID)
	timestamp := hlc.Timestamp{}
	_, err := engine.MVCCGetProto(context.Background(), r.store.Engine(), key, hlc.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return hlc.ZeroTimestamp, err
	}
	return timestamp, nil
}

func (r *Replica) setLastVerificationTimestamp(timestamp hlc.Timestamp) error {
	key := keys.RangeLastVerificationTimestampKey(r.RangeID)
	return engine.MVCCPutProto(context.Background(), r.store.Engine(), nil, key, hlc.ZeroTimestamp, nil, &timestamp)
}

// RaftStatus returns the current raft status of the replica. It returns nil
// if the Raft group has not been initialized yet.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rg := r.mu.internalRaftGroup; rg != nil {
		return rg.Status()
	}
	return nil
}

// State returns a copy of the internal state of the Replica, along with some
// auxiliary information.
func (r *Replica) State() storagebase.RangeInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ri storagebase.RangeInfo
	ri.ReplicaState = *(protoutil.Clone(&r.mu.state)).(*storagebase.ReplicaState)
	ri.LastIndex = r.mu.lastIndex
	ri.NumPending = uint64(len(r.mu.pendingCmds))
	ri.RaftLogSize = r.mu.raftLogSize
	var err error
	if ri.LastVerification, err = r.getLastVerificationTimestamp(); err != nil {
		log.Warningf(context.TODO(), "%s: %v", r, err)
	}
	ri.NumDropped = uint64(r.mu.droppedMessages)

	return ri
}

func (r *Replica) assertState(reader engine.Reader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.assertStateLocked(reader)
}

// assertStateLocked can be called from the Raft goroutine to check that the
// in-memory and on-disk states of the Replica are congruent. See also
// assertState if the replica mutex is not currently held.
//
// TODO(tschottdorf): Consider future removal (for example, when #7224 is resolved).
func (r *Replica) assertStateLocked(reader engine.Reader) {
	diskState, err := loadState(context.TODO(), reader, r.mu.state.Desc)
	if err != nil {
		r.panic(err)
	}
	if !reflect.DeepEqual(diskState, r.mu.state) {
		log.Fatalf(context.TODO(), "%s: on-disk and in-memory state diverged:\n%+v\n%+v", r, diskState, r.mu.state)
	}
}

// Send adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's lease is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue.
func (r *Replica) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	r.assert5725(ba)

	var br *roachpb.BatchResponse

	if err := r.checkBatchRequest(ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	ctx, cleanup := tracing.EnsureContext(ctx, r.store.Tracer())
	defer cleanup()

	// Differentiate between admin, read-only and write.
	var pErr *roachpb.Error
	if ba.IsWrite() {
		log.Trace(ctx, "read-write path")
		br, pErr = r.addWriteCmd(ctx, ba, nil)
	} else if ba.IsReadOnly() {
		log.Trace(ctx, "read-only path")
		br, pErr = r.addReadOnlyCmd(ctx, ba)
	} else if ba.IsAdmin() {
		log.Trace(ctx, "admin path")
		br, pErr = r.addAdminCmd(ctx, ba)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		r.panicf("empty batch")
	} else {
		r.panicf("don't know how to handle command %s", ba)
	}
	if _, ok := pErr.GetDetail().(*roachpb.RaftGroupDeletedError); ok {
		// This error needs to be converted appropriately so that
		// clients will retry.
		pErr = roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
	}
	if pErr != nil {
		log.Tracef(ctx, "error: %s", pErr)
	}
	return br, pErr
}

func (r *Replica) checkCmdHeader(header roachpb.Span) error {
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		mismatchErr := roachpb.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc())
		// Try to suggest the correct range on a key mismatch error where
		// even the start key of the request went to the wrong range.
		if !r.ContainsKey(header.Key) {
			if keyAddr, err := keys.Addr(header.Key); err == nil {
				if repl := r.store.LookupReplica(keyAddr, nil); repl != nil {
					// Only return the correct range descriptor as a hint
					// if we know the current lease holder for that range, which
					// indicates that our knowledge is not stale.
					if lease, _ := repl.getLease(); lease != nil && lease.Covers(r.store.Clock().Now()) {
						mismatchErr.SuggestedRange = repl.Desc()
					}
				}
			}
		}
		return mismatchErr
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
			return errors.Errorf("cannot allow inconsistent reads within a transaction")
		}
		if ba.ReadConsistency == roachpb.CONSENSUS {
			return errors.Errorf("consensus reads not implemented")
		}
	} else if ba.ReadConsistency == roachpb.INCONSISTENT {
		return errors.Errorf("inconsistent mode is only available to reads")
	}

	return nil
}

// beginCmds waits for any overlapping, already-executing commands via
// the command queue and adds itself to queues based on keys affected by the
// batched commands. This gates subsequent commands with overlapping keys or
// key ranges. This method will block if there are any overlapping commands
// already in the queue. Returns a cleanup function to be called when the
// commands are done and can be removed from the queue, and whose returned
// error is to be used in place of the supplied error.
func (r *Replica) beginCmds(ctx context.Context, ba *roachpb.BatchRequest) (func(*roachpb.BatchResponse, *roachpb.Error) *roachpb.Error, error) {
	var cmd *cmd
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		readOnly := ba.IsReadOnly()
		spans := make([]roachpb.Span, len(ba.Requests))
		for i, union := range ba.Requests {
			spans[i] = union.GetInner().Header()
		}
		r.mu.Lock()
		chans := r.mu.cmdQ.getWait(readOnly, spans...)
		cmd = r.mu.cmdQ.add(readOnly, spans...)
		r.mu.Unlock()

		ctxDone := ctx.Done()
		for i, ch := range chans {
			select {
			case <-ch:
			case <-ctxDone:
				err := ctx.Err()
				errStr := fmt.Sprintf("%s while in command queue: %s", err, ba)
				log.Warningf(ctx, "%s: error %v", r, errStr)
				log.Trace(ctx, errStr)
				defer log.Trace(ctx, "removed from command queue")
				// The command is moot, so we don't need to bother executing.
				// However, the command queue assumes that commands don't drop
				// out before their prerequisites, so we still have to wait it
				// out.
				//
				// TODO(tamird): this can be done asynchronously, allowing the
				// caller to return immediately. For now, we're keeping it
				// simple to avoid unexpected surprises.
				for _, ch := range chans[i:] {
					<-ch
				}
				r.mu.Lock()
				r.mu.cmdQ.remove(cmd)
				r.mu.Unlock()
				return nil, err
			}
		}
	}

	// Update the incoming timestamp if unset. Wait until after any
	// preceding command(s) for key range are complete so that the node
	// clock has been updated to the high water mark of any commands
	// which might overlap this one in effect.
	// TODO(spencer,tschottdorf): might remove this, but harder than it looks.
	//   This isn't just unittests (which would require revamping the test
	//   context sender), but also some of the scanner queues place batches
	//   directly into the local range they're servicing.
	if ba.Timestamp.Equal(hlc.ZeroTimestamp) {
		if ba.Txn != nil {
			ba.Timestamp = ba.Txn.OrigTimestamp
		} else {
			ba.Timestamp = r.store.Clock().Now()
		}
	}

	return func(br *roachpb.BatchResponse, pErr *roachpb.Error) *roachpb.Error {
		return r.endCmds(cmd, ba, br, pErr)
	}, nil
}

// endCmds removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
// The returned error replaces the supplied error.
func (r *Replica) endCmds(cmd *cmd, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error) (rErr *roachpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Only update the timestamp cache if the command succeeded and is
	// marked as affecting the cache. Inconsistent reads are excluded.
	if pErr == nil && ba.ReadConsistency != roachpb.INCONSISTENT {
		cr := cacheRequest{
			timestamp: ba.Timestamp,
			txnID:     ba.GetTxnID(),
		}

		for _, union := range ba.Requests {
			args := union.GetInner()
			if updatesTimestampCache(args) {
				header := args.Header()
				switch args.(type) {
				case *roachpb.DeleteRangeRequest:
					// DeleteRange adds to the write timestamp cache to prevent
					// subsequent writes from rewriting history.
					cr.writes = append(cr.writes, header)
				case *roachpb.EndTransactionRequest:
					// EndTransaction adds to the write timestamp cache to ensure replays
					// create a transaction record with WriteTooOld set.
					key := keys.TransactionKey(header.Key, cr.txnID)
					cr.txn = roachpb.Span{Key: key}
				default:
					cr.reads = append(cr.reads, header)
				}
			}
		}

		r.mu.tsCache.AddRequest(cr)
	}
	r.mu.cmdQ.remove(cmd)
	return pErr
}

// applyTimestampCache moves the batch timestamp forward depending on
// the presence of overlapping entries in the timestamp cache. If the
// batch is transactional, the txn timestamp and the txn.WriteTooOld
// bool are updated.
//
// Two important invariants of Cockroach: 1) encountering a more
// recently written value means transaction restart. 2) values must
// be written with a greater timestamp than the most recent read to
// the same key. Check the timestamp cache for reads/writes which
// are at least as recent as the timestamp of this write. The cmd must
// update its timestamp to be greater than more recent values in the
// timestamp cache. When the write returns, the updated timestamp
// will inform the batch response timestamp or batch response txn
// timestamp.
//
// TODO(tschottdorf): find a way not to update the batch txn
//   which should be immutable.
// TODO(tschottdorf): Things look fishy here. We're updating txn.Timestamp in
// multiple places, but there's apparently nothing that forces the remainder of
// request processing to return that updated transaction with a response. In
// effect, we're running in danger of writing at higher timestamps than the
// client (and thus the commit!) are aware of. If there's coverage of these
// code paths, I wonder how it works if not for data races or some brittle
// code path a stack frame higher up. Should really address that previous
// TODO.
func (r *Replica) applyTimestampCache(ba *roachpb.BatchRequest) *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ba.Txn != nil {
		r.mu.tsCache.ExpandRequests(ba.Txn.Timestamp)
	} else {
		r.mu.tsCache.ExpandRequests(ba.Timestamp)
	}

	for _, union := range ba.Requests {
		args := union.GetInner()
		if consultsTimestampCache(args) {
			header := args.Header()
			// BeginTransaction is a special case. We use the transaction
			// key to look for an entry which would indicate this transaction
			// has already been finalized, in which case this is a replay.
			if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
				key := keys.TransactionKey(header.Key, ba.GetTxnID())
				wTS, wOK := r.mu.tsCache.GetMaxWrite(key, nil, nil)
				if wOK {
					return roachpb.NewError(roachpb.NewTransactionReplayError())
				} else if !wTS.Less(ba.Txn.Timestamp) {
					// This is a crucial bit of code. The timestamp cache is
					// reset with the current time + max offset as the low water
					// mark, so if this replica recently obtained the lease,
					// this case will be true for new txns, even if they're not
					// a replay. We move the timestamp forward and return retry.
					// If it's really a replay, it won't retry.
					txn := ba.Txn.Clone()
					txn.Timestamp.Forward(wTS.Next())
					return roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), &txn)
				}
				continue
			}

			// Forward the timestamp if there's been a more recent read.
			rTS, _ := r.mu.tsCache.GetMaxRead(header.Key, header.EndKey, ba.GetTxnID())
			if ba.Txn != nil {
				ba.Txn.Timestamp.Forward(rTS.Next())
			} else {
				ba.Timestamp.Forward(rTS.Next())
			}

			// On more recent writes, forward the timestamp and set the
			// write too old boolean for transactions. Note that currently
			// only EndTransaction and DeleteRange requests update the
			// write timestamp cache.
			wTS, _ := r.mu.tsCache.GetMaxWrite(header.Key, header.EndKey, ba.GetTxnID())
			if ba.Txn != nil {
				if !wTS.Less(ba.Txn.Timestamp) {
					ba.Txn.Timestamp.Forward(wTS.Next())
					ba.Txn.WriteTooOld = true
				}
			} else {
				ba.Timestamp.Forward(wTS.Next())
			}
		}
	}
	return nil
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) addAdminCmd(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}
	args := ba.Requests[0].GetInner()

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, roachpb.NewErrorWithTxn(err, ba.Txn)
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		sp.SetOperationName(reflect.TypeOf(args).String())
	}

	// Admin commands always require the range lease.
	if pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
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
	case *roachpb.AdminTransferLeaseRequest:
		pErr = roachpb.NewError(r.AdminTransferLease(tArgs.Target))
		resp = &roachpb.AdminTransferLeaseResponse{}
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
// clear via the command queue.
func (r *Replica) addReadOnlyCmd(ctx context.Context, ba roachpb.BatchRequest) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is consistent, the read requires the range lease.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		if pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr
		}
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	log.Trace(ctx, "command queue")
	endCmdsFunc, err := r.beginCmds(ctx, &ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we remove the commands from the command queue. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		pErr = endCmdsFunc(br, pErr)
	}()

	// Execute read-only batch command. It checks for matching key range; note
	// that holding readMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	var trigger *PostCommitTrigger
	br, trigger, pErr = r.executeBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), nil, ba)

	if pErr == nil && ba.Txn != nil {
		r.assert5725(ba)
		// Checking whether the transaction has been aborted on reads
		// makes sure that we don't experience anomalous conditions as
		// described in #2231.
		pErr = r.checkIfTxnAborted(ctx, r.store.Engine(), *ba.Txn)
	}
	if trigger != nil && len(trigger.intents) > 0 {
		r.store.intentResolver.processIntentsAsync(r, trigger.intents)
	}
	return br, pErr
}

// TODO(tschottdorf): temporary assertion for #5725, which saw batches with
// a nonempty but incomplete Txn (i.e. &Transaction{})
func (r *Replica) assert5725(ba roachpb.BatchRequest) {
	if ba.Txn != nil && ba.Txn.ID == nil {
		log.Fatalf(context.TODO(), "%s: nontrivial transaction with empty ID: %s\n%s",
			r, ba.Txn, pretty.Sprint(ba))
	}
}

// addWriteCmd first adds the keys affected by this command as pending writes
// to the command queue. Next, the timestamp cache is checked to determine if
// any newer accesses to this command's affected keys have been made. If so,
// the command's timestamp is moved forward. Finally, the command is submitted
// to Raft. Upon completion, the write is removed from the command queue and any
// error returned. If a WaitGroup is supplied, it is signaled when the command
// enters Raft or the function returns with a preprocessing error, whichever
// happens earlier.
func (r *Replica) addWriteCmd(
	ctx context.Context, ba roachpb.BatchRequest, wg *sync.WaitGroup,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	signal := func() {
		if wg != nil {
			wg.Done()
			wg = nil
		}
	}

	// This happens more eagerly below, but it's important to guarantee that
	// early returns do not skip this.
	defer signal()

	// Add the write to the command queue to gate subsequent overlapping
	// commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	log.Trace(ctx, "command queue")
	endCmdsFunc, err := r.beginCmds(ctx, &ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Guarantee we remove the commands from the command queue. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		pErr = endCmdsFunc(br, pErr)
	}()

	// This replica must have range lease to process a write, except when it's
	// an attempt to unfreeze the Range. These are a special case in which any
	// replica will propose it to get back to an active state.
	if pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
		if _, frozen := pErr.GetDetail().(*roachpb.RangeFrozenError); !frozen {
			return nil, pErr
		}
		// Only continue if the batch appears freezing-related.
		if !ba.IsFreeze() {
			return nil, pErr
		}
		pErr = nil
	}

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if pErr := r.applyTimestampCache(&ba); pErr != nil {
		return nil, pErr
	}

	log.Trace(ctx, "raft")

	ch, tryAbandon, err := r.proposeRaftCommand(ctx, ba)

	signal()

	if err == nil {
		// If the command was accepted by raft, wait for the range to apply it.
		ctxDone := ctx.Done()
		for br == nil && pErr == nil {
			select {
			case respWithErr := <-ch:
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
				if tryAbandon() {
					// TODO(tschottdorf): the command will still execute at
					// some process, so maybe this should be a structured error
					// which can be interpreted appropriately upstream.
					pErr = roachpb.NewError(ctx.Err())
				} else {
					log.Warningf(ctx, "%s: unable to cancel expired Raft command %s", r, ba)
				}
			}
		}
	} else {
		pErr = roachpb.NewError(err)
	}
	return br, pErr
}

func (r *Replica) prepareRaftCommandLocked(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	replica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest,
) *pendingCmd {
	if r.mu.lastAssignedLeaseIndex < r.mu.state.LeaseAppliedIndex {
		r.mu.lastAssignedLeaseIndex = r.mu.state.LeaseAppliedIndex
	}
	if !ba.IsLease() {
		r.mu.lastAssignedLeaseIndex++
	}
	if log.V(4) {
		log.Infof(ctx, "%s: prepared command: maxLeaseIndex=%d leaseAppliedIndex=%d",
			r, r.mu.lastAssignedLeaseIndex, r.mu.state.LeaseAppliedIndex)
	}
	return &pendingCmd{
		ctx:   ctx,
		idKey: idKey,
		done:  make(chan roachpb.ResponseWithError, 1),
		raftCmd: roachpb.RaftCommand{
			RangeID:       r.RangeID,
			OriginReplica: replica,
			Cmd:           ba,
			MaxLeaseIndex: r.mu.lastAssignedLeaseIndex,
		},
	}
}

func (r *Replica) insertRaftCommandLocked(pCmd *pendingCmd) {
	idKey := pCmd.idKey
	if _, ok := r.mu.pendingCmds[idKey]; ok {
		log.Fatalf(context.TODO(), "%s: pending command already exists for %s", r, idKey)
	}
	r.mu.pendingCmds[idKey] = pCmd
}

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

// proposeRaftCommand prepares necessary pending command struct and
// initializes a client command ID if one hasn't been. It then
// proposes the command to Raft and returns
// - a channel which receives a response or error upon application
// - a closure used to attempt to abandon the command. When called, it tries to
//   remove the pending command from the internal commands map. This is
//   possible until execution of the command at the local replica has already
//   begun, in which case false is returned and the client needs to continue
//   waiting for successful execution.
// - any error obtained during the creation or proposal of the command, in
//   which case the other returned values are zero.
func (r *Replica) proposeRaftCommand(
	ctx context.Context, ba roachpb.BatchRequest,
) (
	chan roachpb.ResponseWithError, func() bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.destroyed != nil {
		return nil, nil, r.mu.destroyed
	}
	repDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(r.store.StoreID())
	if !ok {
		return nil, nil, roachpb.NewRangeNotFoundError(r.RangeID)
	}
	pCmd := r.prepareRaftCommandLocked(ctx, makeIDKey(), repDesc, ba)
	r.insertRaftCommandLocked(pCmd)

	if err := r.proposePendingCmdLocked(pCmd); err != nil {
		delete(r.mu.pendingCmds, pCmd.idKey)
		return nil, nil, err
	}
	tryAbandon := func() bool {
		r.mu.Lock()
		_, ok := r.mu.pendingCmds[pCmd.idKey]
		delete(r.mu.pendingCmds, pCmd.idKey)
		r.mu.Unlock()
		return ok
	}
	return pCmd.done, tryAbandon, nil
}

// proposePendingCmdLocked proposes or re-proposes a command in r.mu.pendingCmds.
// The replica lock must be held.
func (r *Replica) proposePendingCmdLocked(p *pendingCmd) error {
	p.proposedAtTicks = r.mu.ticks
	if r.mu.proposeRaftCommandFn != nil {
		return r.mu.proposeRaftCommandFn(p)
	}
	return defaultProposeRaftCommandLocked(r, p)
}

func defaultProposeRaftCommandLocked(r *Replica, p *pendingCmd) error {
	if p.raftCmd.Cmd.Timestamp == hlc.ZeroTimestamp {
		return errors.Errorf("can't propose Raft command with zero timestamp")
	}

	data, err := protoutil.Marshal(&p.raftCmd)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)
	if union, ok := p.raftCmd.Cmd.GetArg(roachpb.EndTransaction); ok {
		if crt := union.(*roachpb.EndTransactionRequest).
			InternalCommitTrigger.
			GetChangeReplicasTrigger(); crt != nil {
			// EndTransactionRequest with a ChangeReplicasTrigger is special
			// because raft needs to understand it; it cannot simply be an
			// opaque command.
			log.Infof(context.TODO(), "%s: proposing %s %+v for range %d: %+v", r,
				crt.ChangeType, crt.Replica, p.raftCmd.RangeID, crt.UpdatedReplicas)

			ctx := ConfChangeContext{
				CommandID: string(p.idKey),
				Payload:   data,
				Replica:   crt.Replica,
			}
			encodedCtx, err := protoutil.Marshal(&ctx)
			if err != nil {
				return err
			}

			return r.withRaftGroupLocked(func(raftGroup *raft.RawNode) error {
				return raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    changeTypeInternalToRaft[crt.ChangeType],
					NodeID:  uint64(crt.Replica.ReplicaID),
					Context: encodedCtx,
				})
			})
		}
	}

	return r.withRaftGroupLocked(func(raftGroup *raft.RawNode) error {
		return raftGroup.Propose(encodeRaftCommand(string(p.idKey), data))
	})
}

func (r *Replica) handleRaftReady() error {
	ctx := context.TODO()
	var hasReady bool
	var rd raft.Ready
	r.mu.Lock()
	lastIndex := r.mu.lastIndex // used for append below
	raftLogSize := r.mu.raftLogSize
	err := r.withRaftGroupLocked(func(raftGroup *raft.RawNode) error {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return nil
	})
	r.mu.Unlock()
	if err != nil {
		return err
	}

	if !hasReady {
		return nil
	}

	logRaftReady(ctx, r, rd)

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := r.applySnapshot(ctx, rd.Snapshot, rd.HardState); err != nil {
			return err
		}
		var err error
		if lastIndex, err = loadLastIndex(ctx, r.store.Engine(), r.RangeID); err != nil {
			return err
		}
		// TODO(bdarnell): update coalesced heartbeat mapping with snapshot info.
	}
	batch := r.store.Engine().NewBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		var err error
		if lastIndex, raftLogSize, err = r.append(ctx, writer, lastIndex, raftLogSize, rd.Entries); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := setHardState(ctx, writer, r.RangeID, rd.HardState); err != nil {
			return err
		}
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	// Update last index.
	r.mu.Lock()
	r.mu.lastIndex = lastIndex
	r.mu.raftLogSize = raftLogSize
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

			var commandID string
			var command roachpb.RaftCommand

			if len(e.Data) == 0 {
				shouldReproposeCmds = true
				commandID = "" // special-cased value, command isn't used
			} else {
				var encodedCommand []byte
				commandID, encodedCommand = DecodeRaftCommand(e.Data)
				if err := command.Unmarshal(encodedCommand); err != nil {
					return err
				}
			}

			// Discard errors from processRaftCommand. The error has been sent
			// to the client that originated it, where it will be handled.
			_ = r.processRaftCommand(storagebase.CmdIDKey(commandID), e.Index, command)

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
			if pErr := r.processRaftCommand(storagebase.CmdIDKey(ctx.CommandID), e.Index, command); pErr != nil {
				// If processRaftCommand failed, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			// TODO(bdarnell): update coalesced heartbeat mapping on success.
			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) error {
				raftGroup.ApplyConfChange(cc)
				return nil
			}); err != nil {
				return err
			}
		default:
			log.Fatalf(context.TODO(), "%s: unexpected Raft entry: %v", r, e)
		}
	}
	if shouldReproposeCmds {
		r.mu.Lock()
		err := r.refreshPendingCmdsLocked(reasonNewLeaderOrConfigChange, 0)
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	return r.withRaftGroup(func(raftGroup *raft.RawNode) error {
		raftGroup.Advance(rd)
		return nil
	})
}

func (r *Replica) tick() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the raft group is uninitialized, do not initialize raft groups on
	// tick.
	if r.mu.internalRaftGroup == nil {
		return nil
	}

	r.mu.ticks++
	r.mu.internalRaftGroup.Tick()
	if r.mu.ticks%r.store.ctx.RaftElectionTimeoutTicks == 0 {
		// RaftElectionTimeoutTicks is a reasonable approximation of how long we
		// should wait before deciding that our previous proposal didn't go
		// through. Note that the combination of the above condition and passing
		// RaftElectionTimeoutTicks to refreshPendingCmdsLocked means that commands
		// will be refreshed when they have been pending for 1 to 2 electionc
		// cycles.
		//
		// TODO(tamird/bdarnell): Add unit tests.
		if err := r.refreshPendingCmdsLocked(
			reasonTicks, r.store.ctx.RaftElectionTimeoutTicks); err != nil {
			return err
		}
	}
	return nil
}

// pendingCmdSlice sorts by increasing MaxLeaseIndex.
type pendingCmdSlice []*pendingCmd

func (s pendingCmdSlice) Len() int      { return len(s) }
func (s pendingCmdSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pendingCmdSlice) Less(i, j int) bool {
	return s[i].raftCmd.MaxLeaseIndex < s[j].raftCmd.MaxLeaseIndex
}

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeaderOrConfigChange
	reasonReplicaIDChanged
	reasonTicks
)

func (r *Replica) refreshPendingCmdsLocked(reason refreshRaftReason, refreshAtDelta int) error {
	if len(r.mu.pendingCmds) == 0 {
		return nil
	}

	// Note that we can't use the commit index here (which is typically a
	// little ahead), because a pending command is removed only as it applies.
	// Thus we'd risk reproposing a command that has been committed but not yet
	// applied.
	maxWillRefurbish := r.mu.state.LeaseAppliedIndex // indexes <= will be refurbished
	refreshAtTicks := r.mu.ticks - refreshAtDelta
	refurbished := 0
	var reproposals pendingCmdSlice
	for idKey, p := range r.mu.pendingCmds {
		if p.proposedAtTicks > refreshAtTicks {
			// The command was proposed too recently, don't bother reproprosing or
			// refurbishing it yet. Note that if refreshAtDelta is 0, refreshAtTicks
			// will be r.mu.ticks making the above condition impossible.
			continue
		}
		if p.raftCmd.MaxLeaseIndex > maxWillRefurbish {
			reproposals = append(reproposals, p)
			continue
		}
		delete(r.mu.pendingCmds, idKey)
		// The command can be refurbished.
		if pErr := r.refurbishPendingCmdLocked(p); pErr != nil {
			p.done <- roachpb.ResponseWithError{Err: pErr}
		}
		refurbished++
	}
	if log.V(1) && (refurbished > 0 || len(reproposals) > 0) {
		log.Infof(context.TODO(),
			"%s: pending commands: refurbished %d, reproposing %d (at %d.%d); %s",
			r, refurbished, len(reproposals), r.mu.state.RaftAppliedIndex,
			r.mu.state.LeaseAppliedIndex, reason)
	}

	// Reproposals are those commands which we weren't able to refurbish (since
	// we're not sure that another copy of them could apply at the "correct"
	// index).
	// For reproposals, it's generally pretty unlikely that they can make it in
	// the right place. Reproposing in order is definitely required, however.
	sort.Sort(reproposals)
	for _, p := range reproposals {
		if err := r.proposePendingCmdLocked(p); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replica) getReplicaDescriptorByIDLocked(
	replicaID roachpb.ReplicaID,
	fallback roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, error) {
	if repDesc, ok := r.mu.state.Desc.GetReplicaDescriptorByID(replicaID); ok {
		return repDesc, nil
	}
	if fallback.ReplicaID == replicaID {
		return fallback, nil
	}
	return roachpb.ReplicaDescriptor{},
		errors.Errorf("replica %d not present in %v, %v", replicaID, fallback, r.mu.state.Desc.Replicas)
}

func (r *Replica) sendRaftMessage(msg raftpb.Message) {
	rangeID := r.RangeID

	r.mu.Lock()
	fromReplica, fromErr := r.getReplicaDescriptorByIDLocked(roachpb.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDLocked(roachpb.ReplicaID(msg.To), r.mu.lastFromReplica)
	r.mu.Unlock()

	if fromErr != nil {
		log.Warningf(context.TODO(),
			"failed to look up sender replica %d in range %d while sending %s: %s", msg.From, rangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		log.Warningf(context.TODO(),
			"failed to look up recipient replica %d in range %d while sending %s: %s", msg.To, rangeID, msg.Type, toErr)
		return
	}

	if !r.raftSender.SendAsync(&RaftMessageRequest{
		RangeID:     rangeID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	}) {
		r.mu.Lock()
		r.mu.droppedMessages++
		r.mu.Unlock()

		if err := r.withRaftGroup(func(raftGroup *raft.RawNode) error {
			raftGroup.ReportUnreachable(msg.To)
			return nil
		}); err != nil {
			r.panic(err)
		}
	}
}

func (r *Replica) reportSnapshotStatus(to uint64, snapErr error) {
	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) error {
		raftGroup.ReportSnapshot(to, snapStatus)
		return nil
	}); err != nil {
		r.panic(err)
	}
}

// refurbishPendingCmdLocked takes a pendingCmd which was discovered to apply
// at a log position other than the one at which it was originally proposed
// (this can happen when the range lease held by a raft follower, who must
// forward MsgProp messages to the raft leader without guaranteed ordering).
// It inserts and proposes a new command, returning an error if that fails.
// The passed command must have been deleted from r.mu.pendingCmds.
func (r *Replica) refurbishPendingCmdLocked(cmd *pendingCmd) *roachpb.Error {
	// Note that the new command has the same idKey (which matters since we
	// leaked that to the pending client).
	newPCmd := r.prepareRaftCommandLocked(cmd.ctx, cmd.idKey,
		cmd.raftCmd.OriginReplica, cmd.raftCmd.Cmd)
	newPCmd.done = cmd.done
	r.insertRaftCommandLocked(newPCmd)
	if err := r.proposePendingCmdLocked(newPCmd); err != nil {
		delete(r.mu.pendingCmds, newPCmd.idKey)
		return roachpb.NewError(err)
	}
	return nil
}

// processRaftCommand processes a raft command by unpacking the command
// struct to get args and reply and then applying the command to the
// state machine via applyRaftCommand(). The error result is sent on
// the command's done channel, if available.
// As a special case, the zero idKey signifies an empty Raft command,
// which will apply as a no-op (without accessing raftCmd, via an error),
// updating only the applied index.
func (r *Replica) processRaftCommand(
	idKey storagebase.CmdIDKey, index uint64, raftCmd roachpb.RaftCommand,
) *roachpb.Error {
	if index == 0 {
		log.Fatalf(context.TODO(), "%s: processRaftCommand requires a non-zero index", r)
	}

	if log.V(4) {
		log.Infof(context.TODO(), "%s: processing command: maxLeaseIndex=%d",
			r, raftCmd.MaxLeaseIndex)
	}

	r.mu.Lock()
	cmd := r.mu.pendingCmds[idKey]

	isLeaseError := func() bool {
		l, ba, origin := r.mu.state.Lease, raftCmd.Cmd, raftCmd.OriginReplica
		if l.Replica != origin && !ba.IsLease() {
			return true
		}
		notCovered := !l.OwnedBy(origin.StoreID) || !l.Covers(ba.Timestamp)
		if notCovered && !ba.IsFreeze() && !ba.IsLease() {
			// Verify the range lease is held, unless this command is trying
			// to obtain it or is a freeze change (which can be proposed by any
			// Replica). Any other Raft command has had the range lease held
			// by the replica at proposal time, but this may no longer be the
			// case. Corruption aside, the most likely reason is a lease
			// change (the most recent lease holder assumes responsibility for all
			// past timestamps as well). In that case, it's not valid to go
			// ahead with the execution: Writes must be aware of the last time
			// the mutated key was read, and since reads are served locally by
			// the lease holder without going through Raft, a read which was
			// not taken into account may have been served. Hence, we must
			// retry at the current lease holder.
			return true
		}
		return false
	}

	// TODO(tschottdorf): consider the Trace situation here.
	ctx := context.Background()
	if cmd != nil {
		// We initiated this command, so use the caller-supplied context.
		ctx = cmd.ctx
		delete(r.mu.pendingCmds, idKey)
	}
	leaseIndex := r.mu.state.LeaseAppliedIndex

	var forcedErr *roachpb.Error
	if idKey == "" {
		// This is an empty Raft command (which is sent by Raft after elections
		// to trigger reproposals or during concurrent configuration changes).
		// Nothing to do here except making sure that the corresponding batch
		// (which is bogus) doesn't get executed (for it is empty and so
		// properties like key range are undefined).
		forcedErr = roachpb.NewErrorf("no-op on empty Raft entry")
	} else if isLeaseError() {
		if log.V(1) {
			log.Warningf(context.TODO(), "%s: command proposed from replica %+v (lease at %v): %s",
				r, raftCmd.OriginReplica, r.mu.state.Lease.Replica, raftCmd.Cmd)
		}
		forcedErr = roachpb.NewError(newNotLeaseHolderError(
			r.mu.state.Lease, raftCmd.OriginReplica.StoreID, r.mu.state.Desc))
	} else if raftCmd.Cmd.IsLease() {
		// Lease commands are ignored by the counter (and their MaxLeaseIndex
		// is ignored). This makes sense since lease commands are proposed by
		// anyone, so we can't expect a coherent MaxLeaseIndex. Also, lease
		// proposals are often replayed, so not making them update the counter
		// makes sense from a testing perspective.
	} else if r.mu.state.LeaseAppliedIndex < raftCmd.MaxLeaseIndex {
		// The happy case: the command is applying at or ahead of the minimal
		// permissible index. It's ok if it skips a few slots (as can happen
		// during rearrangement); this command will apply, but later ones which
		// were proposed at lower indexes may not. Overall though, this is more
		// stable and simpler than requiring commands to apply at their exact
		// lease index: Handling the case in which MaxLeaseIndex > oldIndex+1
		// is otherwise tricky since a refurbishment is not allowed
		// (reproposals could exist and may apply at the right index, leading
		// to a replay), and assigning the required index would be tedious
		// seeing that it would have to rewind sometimes.
		leaseIndex = raftCmd.MaxLeaseIndex
	} else {
		// The command is trying to apply at a past log position. That's
		// unfortunate and hopefully rare; we will refurbish on the proposer.
		// Note that in this situation, the leaseIndex does not advance.
		forcedErr = roachpb.NewErrorf("command observed at lease index %d, "+
			"but required < %d", leaseIndex, raftCmd.MaxLeaseIndex)

		if cmd != nil {
			// Only refurbish when no earlier incarnation of this command has
			// already gone through the trouble of doing so (which would have
			// changed our local copy of the pending command). We want to error
			// out, but keep the pending command (i.e. not tell the client)
			// so that the future incarnation can apply and notify the it.
			// Note that we keep the context to avoid hiding these internal
			// cycles from traces.
			if localMaxLeaseIndex := cmd.raftCmd.MaxLeaseIndex; localMaxLeaseIndex <= raftCmd.MaxLeaseIndex {
				if log.V(1) {
					log.Infof(context.TODO(), "%s: refurbishing command for <= %d observed at %d",
						r, raftCmd.MaxLeaseIndex, leaseIndex)
				}

				if pErr := r.refurbishPendingCmdLocked(cmd); pErr == nil {
					cmd.done = make(chan roachpb.ResponseWithError, 1)
				} else {
					// We could try to send the error to the client instead,
					// but to avoid even the appearance of Replica divergence,
					// let's not.
					log.Warningf(context.TODO(), "%s: unable to refurbish: %s", r, pErr)
				}
			} else {
				// The refurbishment is already in flight, so we better get cmd back
				// into pendingCmds (the alternative, not deleting it in this case
				// in the first place, leads to less legible code here). This code
				// path is rare.
				r.insertRaftCommandLocked(cmd)
				// The client should get the actual execution, not this one. We
				// keep the context (which is fine since the client will only
				// finish it when the "real" incarnation applies).
				cmd = nil
			}
		}
	}
	r.mu.Unlock()

	log.Trace(ctx, "applying batch")
	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if log.V(1) && forcedErr != nil {
		log.Infof(context.TODO(), "%s: applying command with forced error: %v", r, forcedErr)
	}

	br, propResult, pErr := r.applyRaftCommand(idKey, ctx, index, leaseIndex,
		raftCmd.OriginReplica, raftCmd.Cmd, forcedErr)
	pErr = r.maybeSetCorrupt(ctx, pErr)

	// Handle all returned side effects. This must happen after commit but
	// before returning to the client.

	// Update store-level MVCC stats with merged range stats.
	r.store.metrics.addMVCCStats(propResult.delta)

	// Handle commit triggers.
	//
	// TODO(tschottdorf): we currently propagate *PostCommitTrigger. Consider
	// using PostCommitTrigger instead.
	if trigger := propResult.PostCommitTrigger; trigger != nil {
		r.handleTrigger(ctx, raftCmd.OriginReplica, *trigger)
		// Assert that the on-disk state doesn't diverge from the in-memory
		// state as a result of the trigger.
		r.assertState(r.store.Engine())
	}

	// On successful write commands handle write-related triggers including
	// splitting and raft log truncation.
	if pErr == nil && raftCmd.Cmd.IsWrite() {
		r.maybeAddToSplitQueue()
		r.maybeAddToRaftLogQueue(index)
	}

	if cmd != nil {
		cmd.done <- roachpb.ResponseWithError{Reply: br, Err: pErr}
		close(cmd.done)
	} else if pErr != nil && log.V(1) {
		log.Errorf(context.TODO(), "%s: error executing raft command: %s", r, pErr)
	}

	return pErr
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine).
// When certain critical operations fail, a replicaCorruptionError may be
// returned and must be handled by the caller.
func (r *Replica) applyRaftCommand(
	idKey storagebase.CmdIDKey,
	ctx context.Context,
	index, leaseIndex uint64,
	originReplica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest,
	forcedError *roachpb.Error,
) (*roachpb.BatchResponse, proposalResult, *roachpb.Error) {
	if index <= 0 {
		log.Fatalf(ctx, "raft command index is <= 0")
	}

	r.mu.Lock()
	oldIndex := r.mu.state.RaftAppliedIndex
	// When frozen, the Range only applies freeze-related requests. Overrides
	// any forcedError.
	if mayApply := !r.mu.state.Frozen || ba.IsFreeze(); !mayApply {
		forcedError = roachpb.NewError(roachpb.NewRangeFrozenError(*r.mu.state.Desc))
	}
	r.mu.Unlock()

	if index != oldIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return nil, proposalResult{}, roachpb.NewError(NewReplicaCorruptionError(errors.Errorf("applied index jumped from %d to %d", oldIndex, index)))
	}

	// Call the helper, which returns a batch containing data written
	// during command execution and any associated error.
	var batch engine.Batch
	var br *roachpb.BatchResponse
	// TODO(tschottdorf): With proposer-eval'ed KV, this will be returned
	// along with the batch representation and, together with it, must
	// contain everything necessary for Replicas to apply the command.
	var propResult proposalResult
	var rErr *roachpb.Error

	if forcedError != nil {
		batch = r.store.Engine().NewBatch()
		br, rErr = nil, forcedError
	} else {
		batch, propResult.delta, br, propResult.PostCommitTrigger, rErr =
			r.applyRaftCommandInBatch(ctx, idKey, originReplica, ba)
	}

	// TODO(tschottdorf): remove when #7224 is cleared.
	if ba.Txn != nil && ba.Txn.Name == replicaChangeTxnName {
		log.Infof(ctx, "%s: applied part of replica change txn: %s, pErr=%v",
			r, ba, rErr)
	}

	defer batch.Close()

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch. Currently the only
	// remaining writes are the raft applied index and the updated MVCC stats.
	//
	writer := batch.Distinct()

	// Advance the last applied index.
	if err := setAppliedIndex(ctx, writer, &propResult.delta, r.RangeID, index, leaseIndex); err != nil {
		log.Fatalf(ctx, "setting applied index in a batch should never fail: %s", err)
	}

	// Flush the MVCC stats to the batch. Note that we need to grab the previous
	// stats now, for they might've been changed during triggers due to side
	// effects.
	// TODO(tschottdorf): refactor that for clarity.
	newMS := r.GetMVCCStats()
	newMS.Add(propResult.delta)
	if err := setMVCCStats(ctx, writer, r.RangeID, newMS); err != nil {
		log.Fatalf(ctx, "setting mvcc stats in a batch should never fail: %s", err)
	}

	// TODO(petermattis): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	// TODO(tschottdorf): with proposer-eval'ed KV, the batch would not be
	// committed at this point. Instead, it would be added to propResult.
	if err := batch.Commit(); err != nil {
		if rErr != nil {
			err = errors.Wrap(rErr.GoError(), err.Error())
		}
		rErr = roachpb.NewError(NewReplicaCorruptionError(errors.Wrap(err, "could not commit batch")))
	} else {
		r.mu.Lock()
		// Update cached appliedIndex if we were able to set the applied index
		// on disk.
		// TODO(tschottdorf): with proposer-eval'ed KV, the lease applied index
		// can be read from the WriteBatch, but there may be reasons to pass
		// it with propResult. We'll see.
		r.mu.state.RaftAppliedIndex = index
		r.mu.state.LeaseAppliedIndex = leaseIndex
		r.mu.state.Stats = newMS
		if forcedError != nil {
			// We only assert when there's a forced error since it might be
			// a little expensive to do on *every* Raft command, seeing that
			// disk i/o could be involved.
			r.assertStateLocked(r.store.Engine())
		}
		r.mu.Unlock()
	}

	return br, propResult, rErr
}

// applyRaftCommandInBatch executes the command in a batch engine and
// returns the batch containing the results. The caller is responsible
// for committing the batch, even on error.
func (r *Replica) applyRaftCommandInBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	originReplica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest,
) (
	engine.Batch,
	enginepb.MVCCStats,
	*roachpb.BatchResponse,
	*PostCommitTrigger, *roachpb.Error,
) {
	// Check whether this txn has been aborted. Only applies to transactional
	// requests which write intents (for example HeartbeatTxn does not get
	// hindered by this).
	if ba.Txn != nil && ba.IsTransactionWrite() {
		r.assert5725(ba)
		if pErr := r.checkIfTxnAborted(ctx, r.store.Engine(), *ba.Txn); pErr != nil {
			return r.store.Engine().NewBatch(), enginepb.MVCCStats{}, nil, nil, pErr
		}
	}

	// Keep track of original txn Writing state to sanitize txn
	// reported with any error except TransactionRetryError.
	wasWriting := ba.Txn != nil && ba.Txn.Writing

	// Execute the commands. If this returns without an error, the batch should
	// be committed.
	btch, ms, br, trigger, pErr := r.executeWriteBatch(ctx, idKey, ba)

	if ba.IsWrite() {
		if pErr != nil {
			// If the batch failed with a TransactionRetryError, any
			// preceding mutations in the batch engine should still be
			// applied so that intents are laid down in preparation for
			// the retry.
			if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
				// TODO(tschottdorf): make `nil` acceptable. Corresponds to
				// roachpb.Response{With->Or}Error.
				br = &roachpb.BatchResponse{}
				// Otherwise, reset the batch to clear out partial execution and
				// prepare for the failed sequence cache entry.
				btch.Close()
				btch = r.store.Engine().NewBatch()
				ms = enginepb.MVCCStats{}
				// Restore the original txn's Writing bool if pErr specifies a transaction.
				if txn := pErr.GetTxn(); txn != nil && txn.Equal(ba.Txn) {
					txn.Writing = wasWriting
				}
			}
		}
	}

	return btch, ms, br, trigger, pErr
}

// checkIfTxnAborted checks the txn abort cache for the given
// transaction. In case the transaction has been aborted, return a
// transaction abort error. Locks the replica.
func (r *Replica) checkIfTxnAborted(
	ctx context.Context, b engine.Reader, txn roachpb.Transaction,
) *roachpb.Error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var entry roachpb.AbortCacheEntry
	aborted, err := r.abortCache.Get(ctx, b, txn.ID, &entry)
	if err != nil {
		return roachpb.NewError(NewReplicaCorruptionError(errors.Wrap(err, "could not read from abort cache")))
	}
	if aborted {
		// We hit the cache, so let the transaction restart.
		if log.V(1) {
			log.Infof(ctx, "%s: found abort cache entry for %s with priority %d",
				r, txn.ID.Short(), entry.Priority)
		}
		newTxn := txn.Clone()
		if entry.Priority > newTxn.Priority {
			newTxn.Priority = entry.Priority
		}
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &newTxn)
	}
	return nil
}

type intentsWithArg struct {
	args    roachpb.Request
	intents []roachpb.Intent
}

// executeWriteBatch attempts to execute transactional batches on the
// 1-phase-commit path as just an atomic, non-transactional batch of
// write commands. One phase commit batches contain transactional
// writes sandwiched by BeginTransaction and EndTransaction requests.
//
// If the batch is transactional, and there's nothing to suggest that
// the transaction will require retry or restart, the batch's txn is
// stripped and it's executed as a normal batch write. If the writes
// cannot all be completed at the intended timestamp, the batch's
// txn is restored and it's re-executed as transactional.
func (r *Replica) executeWriteBatch(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest) (
	engine.Batch,
	enginepb.MVCCStats,
	*roachpb.BatchResponse,
	*PostCommitTrigger, *roachpb.Error,
) {
	batch := r.store.Engine().NewBatch()
	ms := enginepb.MVCCStats{}
	// If not transactional or there are indications that the batch's txn
	// will require restart or retry, execute as normal.
	if r.store.TestingKnobs().DisableOnePhaseCommits || !isOnePhaseCommit(ba) {
		br, trigger, pErr := r.executeBatch(ctx, idKey, batch, &ms, ba)
		return batch, ms, br, trigger, pErr
	}

	// Try executing with transaction stripped.
	strippedBa := ba
	strippedBa.Txn = nil
	strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs

	// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
	br, trigger, pErr := r.executeBatch(ctx, idKey, batch, &ms, strippedBa)
	if pErr == nil && ba.Timestamp == br.Timestamp {
		clonedTxn := ba.Txn.Clone()
		clonedTxn.Writing = true
		clonedTxn.Status = roachpb.COMMITTED

		// If the end transaction is not committed, clear the batch and mark the status aborted.
		arg, _ := ba.GetArg(roachpb.EndTransaction)
		etArg := arg.(*roachpb.EndTransactionRequest)
		if !etArg.Commit {
			clonedTxn.Status = roachpb.ABORTED
			batch.Close()
			batch = r.store.Engine().NewBatch()
			ms = enginepb.MVCCStats{}
		} else {
			// Run commit trigger manually.
			var err error
			if trigger, err = r.runCommitTrigger(ctx, batch, &ms, *etArg, &clonedTxn); err != nil {
				return batch, ms, br, trigger, roachpb.NewErrorf("failed to run commit trigger: %s", err)
			}
		}

		br.Txn = &clonedTxn
		// Add placeholder responses for begin & end transaction requests.
		br.Responses = append([]roachpb.ResponseUnion{{BeginTransaction: &roachpb.BeginTransactionResponse{}}}, br.Responses...)
		br.Responses = append(br.Responses, roachpb.ResponseUnion{EndTransaction: &roachpb.EndTransactionResponse{OnePhaseCommit: true}})
		return batch, ms, br, trigger, nil
	}

	// Otherwise, re-execute with the original, transactional batch.
	batch.Close()
	batch = r.store.Engine().NewBatch()
	ms = enginepb.MVCCStats{}
	br, trigger, pErr = r.executeBatch(ctx, idKey, batch, &ms, ba)
	return batch, ms, br, trigger, pErr
}

// isOnePhaseCommit returns true iff the BatchRequest contains all
// commands in the transaction, starting with BeginTransaction and
// ending with EndTransaction. One phase commits are disallowed if (1) the
// transaction has already been flagged with a write too old error or
// (2) if isolation is serializable and the commit timestamp has been
// forwarded, or (3) the transaction exceeded its deadline.
func isOnePhaseCommit(ba roachpb.BatchRequest) bool {
	if ba.Txn == nil || isEndTransactionTriggeringRetryError(ba.Txn, ba.Txn) {
		return false
	}
	if _, hasBegin := ba.GetArg(roachpb.BeginTransaction); !hasBegin {
		return false
	}
	arg, hasEnd := ba.GetArg(roachpb.EndTransaction)
	if !hasEnd {
		return false
	}
	etArg := arg.(*roachpb.EndTransactionRequest)
	return !isEndTransactionExceedingDeadline(ba.Header.Timestamp, *etArg)
}

// optimizePuts searches for contiguous runs of Put & CPut commands in
// the supplied request union. Any run which exceeds a minimum length
// threshold employs a full order iterator to determine whether the
// range of keys being written is empty. If so, then the run can be
// set to put "blindly", meaning no iterator need be used to read
// existing values during the MVCC write.
func optimizePuts(batch engine.ReadWriter, reqs []roachpb.RequestUnion, distinctSpans bool) {
	var minKey, maxKey roachpb.Key
	var unique map[string]struct{}
	if !distinctSpans {
		unique = make(map[string]struct{}, len(reqs))
	}
	// Returns false on occurrence of a duplicate key.
	maybeAddPut := func(key roachpb.Key) bool {
		// Note that casting the byte slice key to a string does not allocate.
		if unique != nil {
			if _, ok := unique[string(key)]; ok {
				return false
			}
			unique[string(key)] = struct{}{}
		}
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
		return true
	}

	for i, r := range reqs {
		switch t := r.GetInner().(type) {
		case *roachpb.PutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.ConditionalPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		}
		reqs = reqs[:i]
		break
	}

	if len(reqs) < optimizePutThreshold { // don't bother if below this threshold
		return
	}
	iter := batch.NewIterator(false /* total order iterator */)
	defer iter.Close()

	// If there are enough puts in the run to justify calling seek,
	// we can determine whether any part of the range being written
	// is "virgin" and set the puts to write blindly.
	// Find the first non-empty key in the run.
	iter.Seek(engine.MakeMVCCMetadataKey(minKey))
	var iterKey roachpb.Key
	if iter.Valid() && bytes.Compare(iter.Key().Key, maxKey) <= 0 {
		iterKey = iter.Key().Key
	}
	// Set the prefix of the run which is being written to virgin
	// keyspace to "blindly" put values.
	for _, r := range reqs {
		if iterKey == nil || bytes.Compare(iterKey, r.GetInner().Header().Key) > 0 {
			switch t := r.GetInner().(type) {
			case *roachpb.PutRequest:
				t.Blind = true
			case *roachpb.ConditionalPutRequest:
				t.Blind = true
			default:
				panic(fmt.Sprintf("unexpected non-put request: %s", t))
			}
		}
	}
}

// TODO(tschottdorf): Reliance on mutating `ba.Txn` should be dealt with.
func (r *Replica) executeBatch(
	ctx context.Context, idKey storagebase.CmdIDKey,
	batch engine.ReadWriter, ms *enginepb.MVCCStats, ba roachpb.BatchRequest) (
	*roachpb.BatchResponse, *PostCommitTrigger, *roachpb.Error) {
	br := ba.CreateReply()
	var trigger *PostCommitTrigger

	r.mu.Lock()
	threshold := r.mu.state.GCThreshold
	r.mu.Unlock()
	if !threshold.Less(ba.Timestamp) {
		return nil, nil, roachpb.NewError(fmt.Errorf("batch timestamp %v must be after replica GC threshold %v", ba.Timestamp, threshold))
	}

	maxKeys := int64(math.MaxInt64)
	if ba.Header.MaxSpanRequestKeys != 0 {
		// We have a batch of requests with a limit. We keep track of how many
		// remaining keys we can touch.
		maxKeys = ba.Header.MaxSpanRequestKeys
	}

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(ba.Requests) >= optimizePutThreshold {
		optimizePuts(batch, ba.Requests, ba.Header.DistinctSpans)
	}

	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()
		if ba.Txn != nil {
			ba.Txn.BatchIndex = int32(index)
		}
		reply := br.Responses[index].GetInner()
		curTrigger, pErr := r.executeCmd(ctx, idKey, index, batch, ms, ba.Header, maxKeys, args, reply)

		trigger = updateTrigger(trigger, curTrigger)

		if pErr != nil {
			switch tErr := pErr.GetDetail().(type) {
			case *roachpb.WriteTooOldError:
				// WriteTooOldErrors may be the product of raft replays. If
				// timestamp of the request matches exactly with the existing
				// value, maybe propagate the WriteTooOldError to let client
				// retry at a higher timestamp. Keep in mind that this replay
				// protection is best effort only. If replays come out of
				// order, we'd expect them to succeed as the timestamps which
				// would match on a successive replay won't match if the replay
				// is delivered only after another raft command has been applied
				// to the same key.
				if ba.Timestamp.Next().Equal(tErr.ActualTimestamp) {
					// If in a txn, propagate WriteTooOldError immediately. In
					// a txn, intents from earlier commands in the same batch
					// won't return a WriteTooOldError.
					if ba.Txn != nil {
						return nil, trigger, pErr
					}
					// If not in a txn, need to make sure we don't propagate the
					// error unless there are no earlier commands in the batch
					// which might have written the same key.
					var overlap bool
					if ba.Txn == nil {
						for _, union := range ba.Requests[:index] {
							if union.GetInner().Header().Overlaps(args.Header()) {
								overlap = true
								break
							}
						}
					}
					if !overlap {
						return nil, trigger, pErr
					}
				}
				// On WriteTooOldError, we've written a new value or an intent
				// at a too-high timestamp and we must forward the batch txn or
				// timestamp as appropriate so that it's returned.
				if ba.Txn != nil {
					ba.Txn.Timestamp.Forward(tErr.ActualTimestamp)
					ba.Txn.WriteTooOld = true
				} else {
					ba.Timestamp.Forward(tErr.ActualTimestamp)
				}
				// Clear the WriteTooOldError; we're done processing it by having
				// moved the batch or txn timestamps forward and set WriteTooOld
				// if this is a transactional write.
				pErr = nil
			default:
				// Initialize the error index.
				pErr.SetErrorIndex(int32(index))
				return nil, trigger, pErr
			}
		}

		if maxKeys != math.MaxInt64 {
			if cReply, ok := reply.(roachpb.Countable); ok {
				retResults := cReply.Count()
				if retResults > maxKeys {
					r.panicf("received %d results, limit was %d",
						retResults, maxKeys)
				}
				maxKeys -= retResults
			}
		}

		// If transactional, we use ba.Txn for each individual command and
		// accumulate updates to it.
		// TODO(spencer,tschottdorf): need copy-on-write behavior for the
		//   updated batch transaction / timestamp.
		if ba.Txn != nil {
			if txn := reply.Header().Txn; txn != nil {
				ba.Txn.Update(txn)
			}
		}
	}

	if ba.Txn != nil {
		// If transactional, send out the final transaction entry with the reply.
		br.Txn = ba.Txn
	} else {
		// When non-transactional, use the timestamp field.
		br.Timestamp.Forward(ba.Timestamp)
	}

	return br, trigger, nil
}

// getLeaseForGossip tries to obtain a range lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *roachpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, roachpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *roachpb.Error
	if err := r.store.Stopper().RunTask(func() {
		// Check for or obtain the lease, if none active.
		pErr = r.redirectOnOrAcquireLease(ctx)
		hasLease = pErr == nil
		if pErr != nil {
			switch e := pErr.GetDetail().(type) {
			case *roachpb.NotLeaseHolderError:
				// NotLeaseHolderError means there is an active lease, but only if
				// the lease holder is set; otherwise, it's likely a timeout.
				if e.LeaseHolder != nil {
					pErr = nil
				}
			case *roachpb.RangeFrozenError:
				storeID := r.store.StoreID()
				// Let the replica with the smallest StoreID gossip.
				// TODO(tschottdorf): this is silly and hopefully not necessary
				// after #6722 (which prevents Raft reproposals from spuriously
				// re-freezing ranges unfrozen at node startup)
				hasLease = true
				for _, replica := range r.Desc().Replicas {
					if storeID < replica.StoreID {
						hasLease = false
						break
					}
				}
				if hasLease {
					pErr = nil
				}
			default:
				// Any other error is worth being logged visibly.
				log.Warningf(ctx, "could not acquire lease for range gossip: %s", e)
			}
		}
	}); err != nil {
		pErr = roachpb.NewError(err)
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a range lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange() *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	ctx := context.Background()

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if uuidBytes, err := r.store.Gossip().GetInfo(gossip.KeyClusterID); err == nil {
		if gossipClusterID, err := uuid.FromBytes(uuidBytes); err == nil {
			if *gossipClusterID != r.store.ClusterID() {
				log.Fatalf(ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
					r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
			}
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster id %q from store %d, range %d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeyClusterID, r.store.ClusterID().GetBytes(), 0*time.Second); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %s", err)
	}
	if hasLease, pErr := r.getLeaseForGossip(ctx); hasLease {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.gossipFirstRangeLocked(ctx)
	} else {
		return pErr
	}
	return nil
}

func (r *Replica) gossipFirstRangeLocked(ctx context.Context) {
	// Gossip is not provided for the bootstrap store and for some tests.
	if r.store.Gossip() == nil {
		return
	}
	log.Trace(ctx, "gossiping sentinel and first range")
	if log.V(1) {
		log.Infof(ctx, "gossiping sentinel from store %d, range %d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeySentinel, r.store.ClusterID().GetBytes(), sentinelGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip sentinel: %s", err)
	}
	if log.V(1) {
		log.Infof(ctx, "gossiping first range from store %d, range %d: %s",
			r.store.StoreID(), r.RangeID, r.mu.state.Desc.Replicas)
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.mu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %s", err)
	}
}

// maybeGossipSystemConfig scans the entire SystemConfig span and gossips it.
// The first call is on NewReplica. Further calls come from the trigger on
// EndTransaction or range lease acquisition.
//
// Note that maybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a range lease
// here since RequestLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrAcquireLease.
//
// maybeGossipSystemConfig must only be called from Raft commands
// (which provide the necessary serialization to avoid data races).
func (r *Replica) maybeGossipSystemConfig() {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return
	}

	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		return
	}

	if lease, _ := r.getLease(); !lease.OwnedBy(r.store.StoreID()) || !lease.Covers(r.store.Clock().Now()) {
		// Do not gossip when a range lease is not held.
		return
	}

	ctx := context.Background()
	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	kvs, hash, err := r.loadSystemConfigSpan()
	if err != nil {
		log.Errorf(ctx, "could not load SystemConfig span: %s", err)
		return
	}
	if bytes.Equal(r.systemDBHash, hash) {
		return
	}

	if log.V(2) {
		log.Infof(ctx, "gossiping system config from store %d, range %d, hash %x",
			r.store.StoreID(), r.RangeID, hash)
	}

	cfg := &config.SystemConfig{Values: kvs}
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, cfg, 0); err != nil {
		log.Errorf(ctx, "failed to gossip system config: %s", err)
		return
	}

	// Successfully gossiped. Update tracking hash.
	r.systemDBHash = hash
}

// NewReplicaCorruptionError creates a new error indicating a corrupt replica,
// with the supplied list of errors given as history.
func NewReplicaCorruptionError(err error) *roachpb.ReplicaCorruptionError {
	return &roachpb.ReplicaCorruptionError{ErrorMsg: err.Error()}
}

// maybeSetCorrupt is a stand-in for proper handling of failing replicas. Such a
// failure is indicated by a call to maybeSetCorrupt with a ReplicaCorruptionError.
// Currently any error is passed through, but prospectively it should stop the
// range from participating in progress, trigger a rebalance operation and
// decide on an error-by-error basis whether the corruption is limited to the
// range, store, node or cluster with corresponding actions taken.
//
// TODO(d4l3k): when marking a Replica corrupt, must subtract its stats from
// r.store.metrics. Errors which happen between committing a batch and sending
// a stats delta from the store are going to be particularly tricky and the
// best bet is to not have any of those.
// @bdarnell remarks: Corruption errors should be rare so we may want the store
// to just recompute its stats in the background when one occurs.
func (r *Replica) maybeSetCorrupt(ctx context.Context, pErr *roachpb.Error) *roachpb.Error {
	if cErr, ok := pErr.GetDetail().(*roachpb.ReplicaCorruptionError); ok {
		r.mu.Lock()
		defer r.mu.Unlock()

		log.Errorf(ctx, "%s: stalling replica due to: %s", r, cErr.ErrorMsg)
		cErr.Processed = true
		r.mu.destroyed = cErr
		pErr = roachpb.NewError(cErr)

		// Try to persist the destroyed error message. If the underlying store is
		// corrupted the error won't be processed and a panic will occur.
		if err := setReplicaDestroyedError(ctx, r.store.Engine(), r.RangeID, pErr); err != nil {
			cErr.Processed = false
			return roachpb.NewError(cErr)
		}
	}
	return pErr
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
	br, trigger, pErr :=
		r.executeBatch(context.Background(), storagebase.CmdIDKey(""), r.store.Engine(), nil, ba)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	if trigger != nil && len(trigger.intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		r.store.intentResolver.processIntentsAsync(r, trigger.intents)
		return nil, nil, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	return kvs, config.SystemConfig{Values: kvs}.Hash(), nil
}

// needsSplitBySize returns true if the size of the range requires it
// to be split.
func (r *Replica) needsSplitBySize() bool {
	r.mu.Lock()
	maxBytes := r.mu.maxBytes
	size := r.mu.state.Stats.Total()
	r.mu.Unlock()
	return maxBytes > 0 && size > maxBytes
}

func (r *Replica) exceedsDoubleSplitSizeLocked() bool {
	maxBytes := r.mu.maxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && size > maxBytes*2
}

// maybeAddToSplitQueue checks whether the current size of the range
// exceeds the max size specified in the zone config. If yes, the
// range is added to the split queue.
func (r *Replica) maybeAddToSplitQueue() {
	if r.needsSplitBySize() {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
	}
}

// maybeAddToRaftLogQueue checks whether the raft log is a candidate for
// truncation. If yes, the range is added to the raft log queue.
func (r *Replica) maybeAddToRaftLogQueue(appliedIndex uint64) {
	const raftLogCheckFrequency = 1 + RaftLogQueueStaleThreshold/4
	if appliedIndex%raftLogCheckFrequency == 0 {
		r.store.raftLogQueue.MaybeAdd(r, r.store.Clock().Now())
	}
}

func (r *Replica) panic(err error) {
	panic(r.String() + ": " + err.Error())
}

func (r *Replica) panicf(format string, vals ...interface{}) {
	panic(r.String() + ": " + fmt.Sprintf(format, vals...))
}
