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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/google/btree"
	"github.com/kr/pretty"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	// optimizePutThreshold is the minimum length of a contiguous run
	// of batched puts or conditional puts, after which the constituent
	// put operations will possibly be optimized by determining whether
	// the key space being written is starting out empty.
	optimizePutThreshold = 10

	replicaChangeTxnName = "change-replica"
	splitTxnName         = "split"
	mergeTxnName         = "merge"

	defaultReplicaRaftMuWarnThreshold = 500 * time.Millisecond
)

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var txnAutoGC = true

var tickQuiesced = envutil.EnvOrDefaultBool("COCKROACH_TICK_QUIESCED", true)

// TODO(peter): Off by default until we can figure out the performance
// degradation see on test clusters.
var syncRaftLog = settings.RegisterBoolSetting(
	"kv.raft_log.synchronize",
	"set to true to synchronize on Raft log writes to persistent storage",
	true)

var maxCommandSize = settings.RegisterByteSizeSetting(
	"kv.raft.command.max_size",
	"maximum size of a raft command",
	64<<20)

// raftInitialLog{Index,Term} are the starting points for the raft log. We
// bootstrap the raft membership by synthesizing a snapshot as if there were
// some discarded prefix to the log, so we must begin the log at an arbitrary
// index greater than 1.
const (
	raftInitialLogIndex = 10
	raftInitialLogTerm  = 5
)

type proposalRetryReason int

const (
	proposalNoRetry proposalRetryReason = iota
	// proposalIllegalLeaseIndex indicates the proposal failed to apply at
	// a Lease index it was not legal for. The command should be retried.
	proposalIllegalLeaseIndex
	// proposalAmbiguousShouldBeReevaluated indicates that it's ambiguous whether
	// the command was committed (and possibly even applied) or not. The command
	// should be retried. However, the original proposal may have succeeded, so if
	// the retry does not succeed, care must be taken to correctly inform the
	// caller via an AmbiguousResultError.
	proposalAmbiguousShouldBeReevaluated
	// proposalErrorReproposing indicates that re-proposal
	// failed. Because the original proposal may have succeeded, an
	// AmbiguousResultError must be returned. The command should not be
	// retried.
	proposalErrorReproposing
	// proposalRangeNoLongerExists indicates the proposal was for a
	// range that no longer exists. Because the original proposal may
	// have succeeded, an AmbiguousResultError must be returned. The
	// command should not be retried.
	proposalRangeNoLongerExists
)

// proposalResult indicates the result of a proposal. Exactly one of
// Reply, Err and ProposalRetry is set, and it represents the result of
// the proposal.
type proposalResult struct {
	Reply         *roachpb.BatchResponse
	Err           *roachpb.Error
	ProposalRetry proposalRetryReason
	Intents       []intentsWithArg
}

type replicaChecksum struct {
	// started is true if the checksum computation has started.
	started bool
	// Computed checksum. This is set to nil on error.
	checksum []byte
	// If gcTimestamp is nonzero, GC this checksum after gcTimestamp. gcTimestamp
	// is zero if and only if the checksum computation is in progress.
	gcTimestamp time.Time
	// This channel is closed after the checksum is computed, and is used
	// as a notification.
	notify chan struct{}
	// Some debug output that can be added to the CollectChecksumResponse.
	snapshot *roachpb.RaftSnapshotData
}

type atomicDescString struct {
	strPtr unsafe.Pointer
}

// store atomically updates d.strPtr with the string representation of desc.
func (d *atomicDescString) store(replicaID roachpb.ReplicaID, desc *roachpb.RangeDescriptor) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d/", desc.RangeID)
	if replicaID == 0 {
		fmt.Fprintf(&buf, "?:")
	} else {
		fmt.Fprintf(&buf, "%d:", replicaID)
	}

	if !desc.IsInitialized() {
		buf.WriteString("{-}")
	} else {
		const maxRangeChars = 30
		rngStr := keys.PrettyPrintRange(roachpb.Key(desc.StartKey), roachpb.Key(desc.EndKey), maxRangeChars)
		buf.WriteString(rngStr)
	}

	str := buf.String()
	atomic.StorePointer(&d.strPtr, unsafe.Pointer(&str))
}

// String returns the string representation of the range; since we are not
// using a lock, the copy might be inconsistent.
func (d *atomicDescString) String() string {
	return *(*string)(atomic.LoadPointer(&d.strPtr))
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	log.AmbientContext

	// TODO(tschottdorf): Duplicates r.mu.state.desc.RangeID; revisit that.
	RangeID roachpb.RangeID // Should only be set by the constructor.

	store        *Store
	abortCache   *AbortCache   // Avoids anomalous reads after abort
	pushTxnQueue *pushTxnQueue // Queues push txn attempts by txn ID

	stats *replicaStats

	// creatingReplica is set when a replica is created as uninitialized
	// via a raft message.
	creatingReplica *roachpb.ReplicaDescriptor

	// Held in read mode during read-only commands. Held in exclusive mode to
	// prevent read-only commands from executing. Acquired before the embedded
	// RWMutex.
	readOnlyCmdMu syncutil.RWMutex

	// rangeStr is a string representation of a RangeDescriptor that can be
	// atomically read and updated without needing to acquire the replica.mu lock.
	// All updates to state.Desc should be duplicated here.
	rangeStr atomicDescString

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	//
	// TODO(peter): evaluate runtime overhead of the timed mutex.
	raftMu timedMutex

	// stateLoader provides facilities for loading and saving on-disk replica
	// state. Requires Replica.raftMu is held.
	stateLoader replicaStateLoader

	// Contains the lease history when enabled.
	leaseHistory *leaseHistory

	cmdQMu struct {
		// Protects all fields in the cmdQMu struct.
		//
		// Locking notes: Replica.mu < Replica.cmdQMu
		syncutil.Mutex
		// Enforces at most one command is running per key(s). The global
		// component tracks user writes (i.e. all keys for which keys.Addr is
		// the identity), the local component the rest (e.g. RangeDescriptor,
		// transaction record, Lease, ...).
		global, local *CommandQueue
	}

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// Has the replica been destroyed.
		destroyed error
		// Corrupted persistently (across process restarts) indicates whether the
		// replica has been corrupted.
		//
		// TODO(tschottdorf): remove/refactor this field.
		corrupted bool
		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool
		// The state of the Raft state machine.
		state storagebase.ReplicaState
		// Counter used for assigning lease indexes for proposals.
		lastAssignedLeaseIndex uint64
		// Last index persisted to the raft log (not necessarily committed).
		lastIndex uint64
		// The most recent commit index seen in a message from the leader. Used by
		// the follower to estimate the number of Raft log entries it is
		// behind. This field is only valid when the Replica is a follower.
		estimatedCommitIndex uint64
		// The raft log index of a pending preemptive snapshot. Used to prohibit
		// raft log truncation while a preemptive snapshot is in flight. A value of
		// 0 indicates that there is no pending snapshot.
		pendingSnapshotIndex uint64
		// raftLogSize is the approximate size in bytes of the persisted raft log.
		// On server restart, this value is assumed to be zero to avoid costly scans
		// of the raft log. This will be correct when all log entries predating this
		// process have been truncated.
		raftLogSize int64
		// pendingLeaseRequest is used to coalesce RequestLease requests.
		pendingLeaseRequest pendingLeaseRequest
		// minLeaseProposedTS is the minimum acceptable lease.ProposedTS; only
		// leases proposed after this timestamp can be used for proposing commands.
		// This is used to protect against several hazards:
		// - leases held (or even proposed) before a restart cannot be used after a
		// restart. This is because:
		// 		a) the command queue is wiped during the restart; there might be
		// 		writes in flight that are not reflected in the new command queue. So,
		// 		we need to synchronize all new reads with those old in-flight writes.
		// 		Forcing acquisition of a new lease essentially flushes all the
		// 		previous raft commands.
		// 		b) a lease transfer might have been in progress at the time of the
		// 		restart. Using the existing lease after the restart would break the
		// 		transfer proposer's promise to not use the existing lease.
		// - a lease cannot be used after a transfer is initiated. Moreover, even
		// lease extension that were in flight at the time of the transfer cannot be
		// used, if they eventually apply.
		minLeaseProposedTS hlc.Timestamp
		// Max bytes before split.
		maxBytes int64
		// proposals stores the Raft in-flight commands which
		// originated at this Replica, i.e. all commands for which
		// propose has been called, but which have not yet
		// applied.
		//
		// The *ProposalData in the map are "owned" by it. Elements from the
		// map must only be referenced while Replica.mu is held, except if the
		// element is removed from the map first.
		proposals         map[storagebase.CmdIDKey]*ProposalData
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. May be 0 if the replica has
		// been created from a preemptive snapshot (i.e. before being added to the
		// Raft group). The replica ID will be non-zero whenever the replica is
		// part of a Raft group.
		replicaID roachpb.ReplicaID
		// The minimum allowed ID for this replica. Initialized from
		// RaftTombstone.NextReplicaID.
		minReplicaID roachpb.ReplicaID
		// The ID of the leader replica within the Raft group. Used to determine
		// when the leadership changes.
		leaderID roachpb.ReplicaID

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
		// Store.HandleRaftRequest). These last seen descriptors are used when
		// the replica's RangeDescriptor contains missing or out of date descriptors
		// for a replica (see Replica.sendRaftMessage).
		//
		// Removing a replica from Store.mu.replicas is not a problem because
		// when a replica is completely removed, it won't be recreated until
		// there is another event that will repopulate the replicas map in the
		// range descriptor. When it is temporarily dropped and recreated, the
		// newly recreated replica will have a complete range descriptor.
		lastToReplica, lastFromReplica roachpb.ReplicaDescriptor

		// submitProposalFn can be set to mock out the propose operation.
		submitProposalFn func(*ProposalData) error
		// Computed checksum at a snapshot UUID.
		checksums map[uuid.UUID]replicaChecksum

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int
	}

	unreachablesMu struct {
		syncutil.Mutex
		remotes map[roachpb.ReplicaID]struct{}
	}
}

// KeyRange is an interface type for the replicasByKey BTree, to compare
// Replica and ReplicaPlaceholder.
type KeyRange interface {
	Desc() *roachpb.RangeDescriptor
	rangeKeyItem
	btree.Item
	fmt.Stringer
}

var _ KeyRange = &Replica{}

// withRaftGroupLocked calls the supplied function with the (lazily
// initialized) Raft group. The supplied function should return true for the
// unquiesceAndWakeLeader argument if the replica should be unquiesced (and the
// leader awoken). See handleRaftReady for an instance of where this value
// varies. The shouldCampaignOnCreation argument indicates whether a new raft group
// should be campaigned upon creation and is used to eagerly campaign idle
// replicas.
//
// Requires that both Replica.mu and Replica.raftMu are held.
func (r *Replica) withRaftGroupLocked(
	shouldCampaignOnCreation bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
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

	if shouldCampaignOnCreation {
		// Special handling of idle replicas: we campaign their Raft group upon
		// creation if we gossiped our store descriptor more than the election
		// timeout in the past.
		shouldCampaignOnCreation = (r.mu.internalRaftGroup == nil) && r.store.canCampaignIdleReplica()
	}

	ctx := r.AnnotateCtx(context.TODO())

	if r.mu.internalRaftGroup == nil {
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage(r),
			uint64(r.mu.replicaID),
			r.mu.state.RaftAppliedIndex,
			r.store.cfg,
			&raftLogger{ctx: ctx},
		), nil)
		if err != nil {
			return err
		}
		r.mu.internalRaftGroup = raftGroup

		if !shouldCampaignOnCreation {
			// Automatically campaign and elect a leader for this group if there's
			// exactly one known node for this group.
			//
			// A grey area for this being correct happens in the case when we're
			// currently in the process of adding a second node to the group, with
			// the change committed but not applied.
			//
			// Upon restarting, the first node would immediately elect itself and
			// only then apply the config change, where really it should be applying
			// first and then waiting for the majority (which would now require two
			// votes, not only its own).
			//
			// However, in that special case, the second node has no chance to be
			// elected leader while the first node restarts (as it's aware of the
			// configuration and knows it needs two votes), so the worst that could
			// happen is both nodes ending up in candidate state, timing out and then
			// voting again. This is expected to be an extremely rare event.
			//
			// TODO(peter): It would be more natural for this campaigning to only be
			// done when proposing a command (see defaultProposeRaftCommandLocked).
			// Unfortunately, we enqueue the right hand side of a split for Raft
			// ready processing if the range only has a single replica (see
			// splitPostApply). Doing so implies we need to be campaigning
			// that right hand side range when raft ready processing is
			// performed. Perhaps we should move the logic for campaigning single
			// replica ranges there so that normally we only eagerly campaign when
			// proposing.
			shouldCampaignOnCreation = r.isSoloReplicaRLocked()
		}
		if shouldCampaignOnCreation {
			log.VEventf(ctx, 3, "campaigning")
			if err := raftGroup.Campaign(); err != nil {
				return err
			}
			if fn := r.store.cfg.TestingKnobs.OnCampaign; fn != nil {
				fn(r)
			}
		}
	}

	unquiesce, err := f(r.mu.internalRaftGroup)
	if unquiesce {
		r.unquiesceAndWakeLeaderLocked()
	}
	return err
}

// withRaftGroup calls the supplied function with the (lazily initialized)
// Raft group. It acquires and releases the Replica lock, so r.mu must not be
// held (or acquired by the supplied function).
//
// Requires that Replica.raftMu is held.
func (r *Replica) withRaftGroup(
	f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withRaftGroupLocked(false, f)
}

var _ client.Sender = &Replica{}

func newReplica(rangeID roachpb.RangeID, store *Store) *Replica {
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		stateLoader:    makeReplicaStateLoader(rangeID),
		store:          store,
		abortCache:     NewAbortCache(rangeID),
		pushTxnQueue:   newPushTxnQueue(store),
	}
	if leaseHistoryMaxEntries > 0 {
		r.leaseHistory = newLeaseHistory()
	}
	if store.cfg.StorePool != nil {
		r.stats = newReplicaStats(store.Clock(), store.cfg.StorePool.getNodeLocalityString)
	}

	// Init rangeStr with the range ID.
	r.rangeStr.store(0, &roachpb.RangeDescriptor{RangeID: rangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTagStr("@", fmt.Sprintf("%x", unsafe.Pointer(r)))

	raftMuLogger := thresholdLogger(
		r.AnnotateCtx(context.Background()),
		defaultReplicaRaftMuWarnThreshold,
		func(ctx context.Context, msg string, args ...interface{}) {
			log.Warningf(ctx, "raftMu: "+msg, args...)
		},
	)
	r.raftMu = makeTimedMutex(raftMuLogger)
	return r
}

// NewReplica initializes the replica using the given metadata. If the
// replica is initialized (i.e. desc contains more than a RangeID),
// replicaID should be 0 and the replicaID will be discovered from the
// descriptor.
func NewReplica(
	desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	r := newReplica(desc.RangeID, store)
	return r, r.init(desc, store.Clock(), replicaID)
}

func (r *Replica) init(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.initLocked(desc, clock, replicaID)
}

func (r *Replica) initLocked(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	}
	if desc.IsInitialized() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}

	r.cmdQMu.Lock()
	r.cmdQMu.global = NewCommandQueue(true /* optimizeOverlap */)
	r.cmdQMu.local = NewCommandQueue(false /* !optimizeOverlap */)
	r.cmdQMu.Unlock()

	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]replicaChecksum{}
	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil
	// Init the minLeaseProposedTS such that we won't use an existing lease (if
	// any). This is so that, after a restart, we don't propose under old leases.
	// If the replica is being created through a split, this value will be
	// overridden.
	if !r.store.cfg.TestingKnobs.DontPreventUseOfOldLeaseOnStart {
		r.mu.minLeaseProposedTS = clock.Now()
	}

	var err error

	if r.mu.state, err = r.stateLoader.load(ctx, r.store.Engine(), desc); err != nil {
		return err
	}
	r.rangeStr.store(0, r.mu.state.Desc)

	r.mu.lastIndex, err = r.stateLoader.loadLastIndex(ctx, r.store.Engine())
	if err != nil {
		return err
	}

	pErr, err := r.stateLoader.loadReplicaDestroyedError(ctx, r.store.Engine())
	if err != nil {
		return err
	}
	r.mu.destroyed = pErr.GetDetail()
	r.mu.corrupted = r.mu.destroyed != nil

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
	r.rangeStr.store(replicaID, r.mu.state.Desc)
	if err := r.setReplicaIDLocked(replicaID); err != nil {
		return err
	}
	r.assertStateRLocked(r.store.Engine())
	return nil
}

// String returns the string representation of the replica using an
// inconsistent copy of the range descriptor. Therefore, String does not
// require a lock and its output may not be atomic with other ongoing work in
// the replica. This is done to prevent deadlocks in logging sites.
func (r *Replica) String() string {
	return fmt.Sprintf("[n%d,s%d,r%s]", r.store.Ident.NodeID, r.store.Ident.StoreID, &r.rangeStr)
}

// destroyData deletes all data associated with a replica, leaving a
// tombstone. Requires that Replica.raftMu is held.
func (r *Replica) destroyDataRaftMuLocked(
	ctx context.Context, consistentDesc roachpb.RangeDescriptor,
) error {
	startTime := timeutil.Now()

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// NB: this uses the local descriptor instead of the consistent one to match
	// the data on disk.
	if err := clearRangeData(r.Desc(), r.store.Engine(), batch); err != nil {
		return err
	}
	clearTime := timeutil.Now()

	// Save a tombstone to ensure that replica IDs never get reused.
	if err := r.setTombstoneKey(ctx, batch, &consistentDesc); err != nil {
		return err
	}
	if err := batch.Commit(false); err != nil {
		return err
	}
	commitTime := timeutil.Now()

	ms := r.GetMVCCStats()
	log.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
		ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
		commitTime.Sub(startTime).Seconds()*1000,
		clearTime.Sub(startTime).Seconds()*1000,
		commitTime.Sub(clearTime).Seconds()*1000)
	return nil
}

func (r *Replica) cancelPendingCommandsLocked() {
	r.mu.AssertHeld()
	for _, p := range r.mu.proposals {
		resp := proposalResult{
			Reply:         &roachpb.BatchResponse{},
			Err:           roachpb.NewError(roachpb.NewAmbiguousResultError("removing replica")),
			ProposalRetry: proposalRangeNoLongerExists,
		}
		p.finishRaftApplication(resp)
	}
	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
}

// setTombstoneKey writes a tombstone to disk to ensure that replica IDs never
// get reused. It determines what the minimum next replica ID can be using
// the provided RangeDescriptor and the Replica's own ID.
//
// We have to be careful to set the right key, since a replica can be using an
// ID that it hasn't yet received a RangeDescriptor for if it receives raft
// requests for that replica ID (as seen in #14231).
func (r *Replica) setTombstoneKey(
	ctx context.Context, eng engine.ReadWriter, desc *roachpb.RangeDescriptor,
) error {
	r.mu.Lock()
	nextReplicaID := r.nextReplicaIDLocked(desc)
	r.mu.minReplicaID = nextReplicaID
	r.mu.Unlock()
	tombstoneKey := keys.RaftTombstoneKey(desc.RangeID)
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: nextReplicaID,
	}
	return engine.MVCCPutProto(ctx, eng, nil, tombstoneKey,
		hlc.Timestamp{}, nil, tombstone)
}

// nextReplicaIDLocked returns the minimum ID that a new replica can be created
// with for this replica's range. We have to be very careful to ensure that
// replica IDs never get re-used because that can cause panics.
//
// The externalDesc parameter is an optional way to provide an additional
// descriptor for the range that was looked up outside the replica code.
func (r *Replica) nextReplicaIDLocked(externalDesc *roachpb.RangeDescriptor) roachpb.ReplicaID {
	result := r.mu.state.Desc.NextReplicaID
	if externalDesc != nil && result < externalDesc.NextReplicaID {
		result = externalDesc.NextReplicaID
	}
	if result < r.mu.minReplicaID {
		result = r.mu.minReplicaID
	}
	return result
}

func (r *Replica) setReplicaID(replicaID roachpb.ReplicaID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setReplicaIDLocked(replicaID)
}

// setReplicaIDLocked requires that the replica lock is held.
func (r *Replica) setReplicaIDLocked(replicaID roachpb.ReplicaID) error {
	if r.mu.replicaID == replicaID {
		// The common case: the replica ID is unchanged.
		return nil
	}
	if replicaID == 0 {
		// If the incoming message does not have a new replica ID it is a
		// preemptive snapshot. We'll update minReplicaID if the snapshot is
		// accepted.
		return nil
	}
	if replicaID < r.mu.minReplicaID {
		return &roachpb.RaftGroupDeletedError{}
	}
	if r.mu.replicaID > replicaID {
		return errors.Errorf("replicaID cannot move backwards from %d to %d", r.mu.replicaID, replicaID)
	}
	// if r.mu.replicaID != 0 {
	// 	// TODO(bdarnell): clean up previous raftGroup (update peers)
	// }

	previousReplicaID := r.mu.replicaID
	r.mu.replicaID = replicaID
	if replicaID >= r.mu.minReplicaID {
		r.mu.minReplicaID = replicaID + 1
	}
	// Reset the raft group to force its recreation on next usage.
	r.mu.internalRaftGroup = nil

	// If there was a previous replica, repropose its pending commands under
	// this new incarnation.
	if previousReplicaID != 0 {
		if log.V(1) {
			log.Infof(r.AnnotateCtx(context.TODO()), "changed replica ID from %d to %d",
				previousReplicaID, replicaID)
		}
		// repropose all pending commands under new replicaID.
		r.refreshProposalsLocked(0, reasonReplicaIDChanged)
	}

	return nil
}

func (r *Replica) setEstimatedCommitIndexLocked(commit uint64) {
	// The estimated commit index only ratchets up to account for Raft messages
	// arriving out of order.
	if r.mu.estimatedCommitIndex < commit {
		r.mu.estimatedCommitIndex = commit
	}
}

// getEstimatedBehindCountRLocked returns an estimate of how far this replica is
// behind. A return value of 0 indicates that the replica is up to date.
func (r *Replica) getEstimatedBehindCountRLocked(raftStatus *raft.Status) int64 {
	if r.mu.quiescent {
		// The range is quiescent, so it is up to date.
		return 0
	}
	if raftStatus != nil && roachpb.ReplicaID(raftStatus.SoftState.Lead) == r.mu.replicaID {
		// We're the leader, so we can't be behind.
		return 0
	}
	if !hasRaftLeader(raftStatus) && r.store.canCampaignIdleReplica() {
		// The Raft group is idle or there is no Raft leader. This can be a
		// temporary situation due to an in progress election or because the group
		// can't achieve quorum. In either case, assume we're up to date.
		return 0
	}
	if r.mu.estimatedCommitIndex == 0 {
		// We haven't heard from the leader, assume we're far behind. This is the
		// case that is commonly hit when a node restarts. In particular, we hit
		// this case until an election timeout passes and canCampaignIdleReplica
		// starts to return true. The results it that a restarted node will
		// consider its replicas far behind initially which will in turn cause it
		// to reject rebalances.
		return prohibitRebalancesBehindThreshold
	}
	if r.mu.estimatedCommitIndex >= r.mu.state.RaftAppliedIndex {
		return int64(r.mu.estimatedCommitIndex - r.mu.state.RaftAppliedIndex)
	}
	return 0
}

// GetMaxBytes atomically gets the range maximum byte limit.
func (r *Replica) GetMaxBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
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

// IsDestroyed returns a non-nil error if the replica has been destroyed.
func (r *Replica) IsDestroyed() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.destroyed
}

// getLease returns the current lease, and the tentative next one, if a lease
// request initiated by this replica is in progress.
func (r *Replica) getLease() (*roachpb.Lease, *roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLeaseRLocked()
}

func (r *Replica) getLeaseRLocked() (*roachpb.Lease, *roachpb.Lease) {
	lease := *r.mu.state.Lease
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return &lease, &nextLease
	}
	return &lease, nil
}

// ownsValidLease returns whether this replica is the current valid
// leaseholder. Note that this method does not check to see if a transfer is
// pending, but returns the status of the current lease and ownership at the
// specified point in time.
func (r *Replica) ownsValidLease(ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Lease.OwnedBy(r.store.StoreID()) &&
		r.leaseStatus(r.mu.state.Lease, ts, r.mu.minLeaseProposedTS).state == leaseValid
}

// IsLeaseValid returns true if the replica's lease is owned by this
// replica and is valid (not expired, not in stasis).
func (r *Replica) IsLeaseValid(lease *roachpb.Lease, ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeaseValidRLocked(lease, ts)
}

func (r *Replica) isLeaseValidRLocked(lease *roachpb.Lease, ts hlc.Timestamp) bool {
	return r.leaseStatus(lease, ts, r.mu.minLeaseProposedTS).state == leaseValid
}

// newNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func newNotLeaseHolderError(
	l *roachpb.Lease, proposerStoreID roachpb.StoreID, rangeDesc *roachpb.RangeDescriptor,
) *roachpb.NotLeaseHolderError {
	err := &roachpb.NotLeaseHolderError{
		RangeID: rangeDesc.RangeID,
	}
	err.Replica, _ = rangeDesc.GetReplicaDescriptor(proposerStoreID)
	if l != nil {
		// Normally, we return the lease-holding Replica here. However, in the
		// case in which a leader removes itself, we want the followers to
		// avoid handing out a misleading clue (which in itself shouldn't be
		// overly disruptive as the lease would expire and then this method
		// shouldn't be called for it any more, but at the very least it
		// could catch tests in a loop, presumably due to manual clocks).
		_, stillMember := rangeDesc.GetReplicaDescriptor(l.Replica.StoreID)
		if stillMember {
			err.LeaseHolder = &l.Replica
			err.Lease = l
		}
	}
	return err
}

// leaseGoodToGo is a fast-path for lease checks which verifies that an
// existing lease is valid and owned by the current store. This method should
// not be called directly. Use redirectOnOrAcquireLease instead.
func (r *Replica) leaseGoodToGo(ctx context.Context) (LeaseStatus, bool) {
	timestamp := r.store.Clock().Now()
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.requiresExpiringLeaseRLocked() {
		// Slow-path for expiration-based leases.
		return LeaseStatus{}, false
	}

	status := r.leaseStatus(r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
	switch status.state {
	case leaseValid:
		if status.lease.OwnedBy(r.store.StoreID()) {
			// We own the lease...
			if repDesc, err := r.getReplicaDescriptorRLocked(); err == nil {
				if _, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); !ok {
					// ...and there is no transfer pending.
					return status, true
				}
			}
		}
	}
	return LeaseStatus{}, false
}

// redirectOnOrAcquireLease checks whether this replica has the lease
// at the current timestamp. If it does, returns success. If another
// replica currently holds the lease, redirects by returning
// NotLeaseHolderError. If the lease is expired, a renewal is
// synchronously requested. Leases are eagerly renewed when a request
// with a timestamp within rangeLeaseRenewalDuration of the lease
// expiration is served.
//
// TODO(spencer): for write commands, don't wait while requesting
//  the range lease. If the lease acquisition fails, the write cmd
//  will fail as well. If it succeeds, as is likely, then the write
//  will not incur latency waiting for the command to complete.
//  Reads, however, must wait.
func (r *Replica) redirectOnOrAcquireLease(ctx context.Context) (LeaseStatus, *roachpb.Error) {
	if status, ok := r.leaseGoodToGo(ctx); ok {
		return status, nil
	}

	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	var status LeaseStatus
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		llChan, pErr := func() (<-chan *roachpb.Error, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			status = r.leaseStatus(r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
			switch status.state {
			case leaseError:
				// Lease state couldn't be determined.
				log.VEventf(ctx, 2, "lease state couldn't be determined")
				return nil, roachpb.NewError(
					newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))

			case leaseValid, leaseStasis:
				if !status.lease.OwnedBy(r.store.StoreID()) {
					_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(status.lease.Replica.StoreID)
					if !stillMember {
						log.Fatalf(ctx, "lease %s owned by replica %+v that no longer exists",
							status.lease, status.lease.Replica)
					}
					// Otherwise, if the lease is currently held by another replica, redirect
					// to the holder.
					return nil, roachpb.NewError(
						newNotLeaseHolderError(status.lease, r.store.StoreID(), r.mu.state.Desc))
				}
				// Check that we're not in the process of transferring the lease away.
				// If we are transferring the lease away, we can't serve reads or
				// propose Raft commands - see comments on TransferLease.
				// TODO(andrei): If the lease is being transferred, consider returning a
				// new error type so the client backs off until the transfer is
				// completed.
				repDesc, err := r.getReplicaDescriptorRLocked()
				if err != nil {
					return nil, roachpb.NewError(err)
				}
				if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(
					repDesc.ReplicaID); ok {
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))
				}

				// If the lease is in stasis, we can't serve requests until we've
				// renewed the lease, so we return the channel to block on renewal.
				// Otherwise, we don't need to wait for the extension and simply
				// ignore the returned channel (which is buffered) and continue.
				if status.state == leaseStasis {
					return r.requestLeaseLocked(ctx, status), nil
				}

				// Extend the lease if this range uses expiration-based
				// leases, the lease is in need of renewal, and there's not
				// already an extension pending.
				_, requestPending := r.mu.pendingLeaseRequest.RequestPending()
				if !requestPending && r.requiresExpiringLeaseRLocked() {
					renewal := status.lease.Expiration.Add(-int64(r.store.cfg.RangeLeaseRenewalDuration), 0)
					if !timestamp.Less(renewal) {
						if log.V(2) {
							log.Infof(ctx, "extending lease %s at %s", status.lease, timestamp)
						}
						// We had an active lease to begin with, but we want to trigger
						// a lease extension. We explicitly ignore the returned channel
						// as we won't block on it.
						_ = r.requestLeaseLocked(ctx, status)
					}
				}

			case leaseExpired:
				// No active lease: Request renewal if a renewal is not already pending.
				log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
				return r.requestLeaseLocked(ctx, status), nil

			case leaseProscribed:
				// Lease proposed timestamp is earlier than the min proposed
				// timestamp limit this replica must observe. If this store
				// owns the lease, re-request. Otherwise, redirect.
				if status.lease.OwnedBy(r.store.StoreID()) {
					log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
					return r.requestLeaseLocked(ctx, status), nil
				}
				// If lease is currently held by another, redirect to holder.
				return nil, roachpb.NewError(
					newNotLeaseHolderError(status.lease, r.store.StoreID(), r.mu.state.Desc))
			}

			// Return a nil chan to signal that we have a valid lease.
			return nil, nil
		}()
		if pErr != nil {
			return LeaseStatus{}, pErr
		}
		if llChan == nil {
			// We own a valid lease.
			return status, nil
		}

		// Wait for the range lease to finish, or the context to expire.
		pErr = func() *roachpb.Error {
			slowTimer := timeutil.NewTimer()
			defer slowTimer.Stop()
			slowTimer.Reset(base.SlowRequestThreshold)
			for {
				select {
				case pErr = <-llChan:
					if pErr != nil {
						switch tErr := pErr.GetDetail().(type) {
						case *roachpb.AmbiguousResultError:
							// This can happen if the RequestLease command we sent has been
							// applied locally through a snapshot: the RequestLeaseRequest
							// cannot be reproposed so we get this ambiguity.
							// We'll just loop around.
							return nil
						case *roachpb.LeaseRejectedError:
							if tErr.Existing.OwnedBy(r.store.StoreID()) {
								// The RequestLease command we sent was rejected because another
								// lease was applied in the meantime, but we own that other
								// lease. So, loop until the current node becomes aware that
								// it's the leaseholder.
								return nil
							}

							// Getting a LeaseRejectedError back means someone else got there
							// first, or the lease request was somehow invalid due to a
							// to a concurrent change. That concurrent change could have been
							// that this replica was removed, so check for that case before
							// falling back to a NotLeaseHolderError.
							var err error
							if _, descErr := r.GetReplicaDescriptor(); descErr != nil {
								err = descErr
							} else if lease, _ := r.getLease(); !r.IsLeaseValid(lease, r.store.Clock().Now()) {
								err = newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc())
							} else {
								err = newNotLeaseHolderError(lease, r.store.StoreID(), r.Desc())
							}
							pErr = roachpb.NewError(err)
						}
						return pErr
					}
					log.Eventf(ctx, "lease acquisition succeeded: %+v", status.lease)
					return nil
				case <-slowTimer.C:
					log.Warningf(ctx, "have been waiting %s attempting to acquire lease",
						base.SlowRequestThreshold)
					r.store.metrics.SlowLeaseRequests.Inc(1)
					defer r.store.metrics.SlowLeaseRequests.Dec(1)
				case <-ctx.Done():
					log.ErrEventf(ctx, "lease acquisition failed: %s", ctx.Err())
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				case <-r.store.Stopper().ShouldStop():
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				}
			}
		}()
		if pErr != nil {
			return LeaseStatus{}, pErr
		}
	}
}

// IsInitialized is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInitializedRLocked()
}

// isInitializedRLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedRLocked() bool {
	return r.mu.state.Desc.IsInitialized()
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

// NodeID returns the ID of the node this replica belongs to.
func (r *Replica) NodeID() roachpb.NodeID {
	return r.store.nodeDesc.NodeID
}

// setDesc atomically sets the range's descriptor. This method calls
// processRangeDescriptorUpdate() to make the Store handle the descriptor
// update. Requires raftMu to be locked.
func (r *Replica) setDesc(desc *roachpb.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.store == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.store.processRangeDescriptorUpdate(r.AnnotateCtx(context.TODO()), r)
}

// setDescWithoutProcessUpdate updates the range descriptor without calling
// processRangeDescriptorUpdate. Requires raftMu to be locked.
func (r *Replica) setDescWithoutProcessUpdate(desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if desc.RangeID != r.RangeID {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}

	r.rangeStr.store(r.mu.replicaID, desc)
	r.mu.state.Desc = desc
}

// GetReplicaDescriptor returns the replica for this range from the range
// descriptor. Returns a *RangeNotFoundError if the replica is not found.
// No other errors are returned.
func (r *Replica) GetReplicaDescriptor() (roachpb.ReplicaDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getReplicaDescriptorRLocked()
}

// getReplicaDescriptorRLocked is like getReplicaDescriptor, but assumes that
// r.mu is held for either reading or writing.
func (r *Replica) getReplicaDescriptorRLocked() (roachpb.ReplicaDescriptor, error) {
	repDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(r.store.StoreID())
	if ok {
		return repDesc, nil
	}
	return roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(r.RangeID)
}

// setLastReplicaDescriptors sets the the most recently seen replica
// descriptors to those contained in the *RaftMessageRequest, acquiring r.mu
// to do so.
func (r *Replica) setLastReplicaDescriptors(req *RaftMessageRequest) {
	r.mu.Lock()
	r.mu.lastFromReplica = req.FromReplica
	r.mu.lastToReplica = req.ToReplica
	r.mu.Unlock()
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
func (r *Replica) GetMVCCStats() enginepb.MVCCStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Stats
}

// ContainsKey returns whether this range contains the specified key.
//
// TODO(bdarnell): This is not the same as RangeDescriptor.ContainsKey.
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
// last checked for removal by the replica gc queue.
func (r *Replica) getLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	var timestamp hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, true, nil, &timestamp)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(ctx context.Context, timestamp hlc.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return engine.MVCCPutProto(ctx, r.store.Engine(), nil, key, hlc.Timestamp{}, nil, &timestamp)
}

// getQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) getQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	key := keys.QueueLastProcessedKey(r.Desc().StartKey, queue)
	var timestamp hlc.Timestamp
	if r.store != nil {
		_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, true, nil, &timestamp)
		if err != nil {
			return hlc.Timestamp{}, err
		}
	}
	return timestamp, nil
}

// setQueueLastProcessed writes the last processed timestamp for the
// specified queue.
func (r *Replica) setQueueLastProcessed(
	ctx context.Context, queue string, timestamp hlc.Timestamp,
) error {
	key := keys.QueueLastProcessedKey(r.Desc().StartKey, queue)
	return r.store.DB().PutInline(ctx, key, &timestamp)
}

// RaftStatus returns the current raft status of the replica. It returns nil
// if the Raft group has not been initialized yet.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftStatusRLocked()
}

func (r *Replica) raftStatusRLocked() *raft.Status {
	if rg := r.mu.internalRaftGroup; rg != nil {
		return rg.Status()
	}
	return nil
}

// State returns a copy of the internal state of the Replica, along with some
// auxiliary information.
func (r *Replica) State() storagebase.RangeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ri storagebase.RangeInfo
	ri.ReplicaState = *(protoutil.Clone(&r.mu.state)).(*storagebase.ReplicaState)
	ri.LastIndex = r.mu.lastIndex
	ri.NumPending = uint64(len(r.mu.proposals))
	ri.RaftLogSize = r.mu.raftLogSize
	ri.NumDropped = uint64(r.mu.droppedMessages)

	return ri
}

func (r *Replica) assertState(reader engine.Reader) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.assertStateRLocked(reader)
}

// assertStateRLocked can be called from the Raft goroutine to check that the
// in-memory and on-disk states of the Replica are congruent. See also
// assertState if the replica mutex is not currently held.
//
// TODO(tschottdorf): Consider future removal (for example, when #7224 is resolved).
func (r *Replica) assertStateRLocked(reader engine.Reader) {
	ctx := r.AnnotateCtx(context.TODO())
	diskState, err := r.stateLoader.load(ctx, reader, r.mu.state.Desc)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if !reflect.DeepEqual(diskState, r.mu.state) {
		// The roundabout way of printing here is to expose this information in sentry.io.
		//
		// TODO(dt): expose properly once #15892 is addressed.
		log.Errorf(ctx, "on-disk and in-memory state diverged:\n%s", pretty.Diff(diskState, r.mu.state))
		r.mu.state.Desc, diskState.Desc = nil, nil
		panic(log.Safe{V: fmt.Sprintf("on-disk and in-memory state diverged: %s", pretty.Diff(diskState, r.mu.state))})
	}
}

// maybeInitializeRaftGroup check whether the internal Raft group has
// not yet been initialized. If not, it is created and set to campaign
// if this replica is the most recent owner of the range lease.
func (r *Replica) maybeInitializeRaftGroup(ctx context.Context) {
	r.mu.RLock()
	// If this replica hasn't initialized the Raft group, create it and
	// unquiesce and wake the leader to ensure the replica comes up to date.
	initialized := r.mu.internalRaftGroup != nil
	r.mu.RUnlock()
	if initialized {
		return
	}

	// Acquire raftMu, but need to maintain lock ordering (raftMu then mu).
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	// Campaign if this replica is the current lease holder to avoid
	// an election storm after a recent split. If no replica is the
	// lease holder, all replicas must campaign to avoid waiting for
	// an election timeout to acquire the lease. In the latter case,
	// there's less chance of an election storm because this replica
	// will only campaign if it's been idle for >= election timeout,
	// so there's most likely been no traffic to the range.
	shouldCampaignOnCreation := r.mu.state.Lease.OwnedBy(r.store.StoreID()) ||
		r.leaseStatus(r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS).state != leaseValid
	if err := r.withRaftGroupLocked(shouldCampaignOnCreation, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil {
		log.ErrEventf(ctx, "unable to initialize raft group: %s", err)
	}
}

// Send executes a command on this range, dispatching it to the
// read-only, read-write, or admin execution path as appropriate.
// ctx should contain the log tags from the store (and up).
func (r *Replica) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	var br *roachpb.BatchResponse

	if r.stats != nil && ba.Header.GatewayNodeID != 0 {
		r.stats.record(ba.Header.GatewayNodeID)
	}

	if err := r.checkBatchRequest(ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureContext(ctx, r.AmbientContext.Tracer, "replica send")
	defer cleanup()

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	// Differentiate between admin, read-only and write.
	var pErr *roachpb.Error
	if ba.IsWrite() {
		log.Event(ctx, "read-write path")
		br, pErr = r.executeWriteBatch(ctx, ba)
	} else if ba.IsReadOnly() {
		log.Event(ctx, "read-only path")
		br, pErr = r.executeReadOnlyBatch(ctx, ba)
	} else if ba.IsAdmin() {
		log.Event(ctx, "admin path")
		br, pErr = r.executeAdminBatch(ctx, ba)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		log.Fatalf(ctx, "empty batch")
	} else {
		log.Fatalf(ctx, "don't know how to handle command %s", ba)
	}
	if _, ok := pErr.GetDetail().(*roachpb.RaftGroupDeletedError); ok {
		// This error needs to be converted appropriately so that
		// clients will retry.
		pErr = roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
	}
	if pErr != nil {
		log.Eventf(ctx, "replica.Send got error: %s", pErr)
	} else {
		if filter := r.store.cfg.TestingKnobs.TestingResponseFilter; filter != nil {
			pErr = filter(ba, br)
		}
	}
	return br, pErr
}

// requestCanProceed returns an error if a request (identified by its
// key span and timestamp) can proceed. It may be called multiple
// times during the processing of the request (i.e. during both
// proposal and application for write commands).
//
// This is called downstream of raft and therefore should be changed
// only with extreme care. It also accesses replica state that is not
// declared in the SpanSet; this is OK because it can never change the
// evaluation of a batch, only allow or disallow it.
func (r *Replica) requestCanProceed(rspan roachpb.RSpan, ts hlc.Timestamp) error {
	r.mu.Lock()
	desc := r.mu.state.Desc
	threshold := r.mu.state.GCThreshold
	r.mu.Unlock()
	if !threshold.Less(ts) {
		return errors.Errorf("batch timestamp %v must be after GC threshold %v", ts, threshold)
	}

	if desc.ContainsKeyRange(rspan.Key, rspan.EndKey) {
		return nil
	}

	mismatchErr := roachpb.NewRangeKeyMismatchError(
		rspan.Key.AsRawKey(), rspan.EndKey.AsRawKey(), desc)
	// Try to suggest the correct range on a key mismatch error where
	// even the start key of the request went to the wrong range.
	if !desc.ContainsKey(rspan.Key) {
		if repl := r.store.LookupReplica(rspan.Key, nil); repl != nil {
			// Only return the correct range descriptor as a hint
			// if we know the current lease holder for that range, which
			// indicates that our knowledge is not stale.
			if lease, _ := repl.getLease(); repl.IsLeaseValid(lease, r.store.Clock().Now()) {
				mismatchErr.SuggestedRange = repl.Desc()
			}
		}
	}
	return mismatchErr
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

// endCmds holds necessary information to end a batch after Raft
// command processing.
type endCmds struct {
	repl *Replica
	cmds [numSpanAccess]struct {
		global, local *cmd
	}
	ba roachpb.BatchRequest
}

// done removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
func (ec *endCmds) done(br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalRetryReason) {
	// Update the timestamp cache if the command succeeded and is not
	// being retried. Each request is considered in turn; only those
	// marked as affecting the cache are processed. Inconsistent reads
	// are excluded.
	if pErr == nil && retry == proposalNoRetry && ec.ba.ReadConsistency != roachpb.INCONSISTENT {
		span, err := keys.Range(ec.ba)
		if err != nil {
			// This can't happen because we've already called keys.Range before
			// evaluating the request.
			log.Fatal(context.Background(), err)
		}
		creq := makeCacheRequest(&ec.ba, br, span)
		ec.repl.store.tsCacheMu.Lock()
		ec.repl.store.tsCacheMu.cache.AddRequest(creq)
		ec.repl.store.tsCacheMu.Unlock()
	}

	ec.repl.cmdQMu.Lock()
	for i := range ec.cmds {
		ec.repl.cmdQMu.global.remove(ec.cmds[i].global)
		ec.repl.cmdQMu.local.remove(ec.cmds[i].local)
	}
	ec.repl.cmdQMu.Unlock()
}

func makeCacheRequest(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse, span roachpb.RSpan,
) cacheRequest {
	cr := cacheRequest{
		span:      span,
		timestamp: ba.Timestamp,
		txnID:     ba.GetTxnID(),
	}

	for i, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.UpdatesTimestampCache(args) {
			header := args.Header()
			switch args.(type) {
			case *roachpb.DeleteRangeRequest:
				// DeleteRange adds to the write timestamp cache to prevent
				// subsequent writes from rewriting history.
				cr.writes = append(cr.writes, header)
			case *roachpb.EndTransactionRequest:
				// EndTransaction adds to the write timestamp cache to ensure replays
				// create a transaction record with WriteTooOld set.
				//
				// Note that TimestampCache.ExpandRequests lazily creates the
				// transaction key from the request key.
				cr.txn = roachpb.Span{Key: header.Key}
			case *roachpb.ScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
				if ba.Header.MaxSpanRequestKeys != 0 &&
					ba.Header.MaxSpanRequestKeys == int64(len(resp.Rows)) {
					// If the scan requested a limited number of results and we hit the
					// limit, truncate the span of keys to add to the timestamp cache
					// to those that were returned.
					//
					// NB: We need to include any key read by the operation, even if
					// not returned, in the span added to the timestamp cache in order
					// to prevent phantom read anomalies. That means we can only
					// perform this truncate if the scan requested a limited number of
					// results and we hit that limit.

					// Make a copy of the key to avoid holding on to the large buffer the
					// key was allocated from for the response.
					src := resp.Rows[len(resp.Rows)-1].Key
					// Size the new key to be exactly what we need for key.Next() to not
					// allocate.
					key := make(roachpb.Key, len(src), len(src)+1)
					copy(key, src)
					header.EndKey = key.Next()
				}
				cr.reads = append(cr.reads, header)
			case *roachpb.ReverseScanRequest:
				resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
				if ba.Header.MaxSpanRequestKeys != 0 &&
					ba.Header.MaxSpanRequestKeys == int64(len(resp.Rows)) {
					// See comment in the ScanRequest case. For revert scans, results
					// are returned in reverse order and we truncate the start key of
					// the span.

					// Make a copy of the key to avoid holding on to the large buffer the
					// key was allocated from for the response.
					src := resp.Rows[len(resp.Rows)-1].Key
					key := make(roachpb.Key, len(src))
					copy(key, src)
					header.Key = key
				}
				cr.reads = append(cr.reads, header)
			default:
				cr.reads = append(cr.reads, header)
			}
		}
	}

	return cr
}

func collectSpans(desc roachpb.RangeDescriptor, ba *roachpb.BatchRequest) (*SpanSet, error) {
	spans := &SpanSet{}
	// TODO(bdarnell): need to make this less global when the local
	// command queue is used more heavily. For example, a split will
	// have a large read-only span but also a write (see #10084).
	// Currently local spans are the exception, so preallocate for the
	// common case in which all are global. We rarely mix read and
	// write commands, so preallocate for writes if there are any
	// writes present in the batch.
	//
	// TODO(bdarnell): revisit as the local portion gets its appropriate
	// use.
	if ba.IsReadOnly() {
		spans.reserve(SpanReadOnly, spanGlobal, len(ba.Requests))
	} else {
		spans.reserve(SpanReadWrite, spanGlobal, len(ba.Requests))
	}

	for _, union := range ba.Requests {
		inner := union.GetInner()
		if _, ok := inner.(*roachpb.NoopRequest); ok {
			continue
		}
		if cmd, ok := commands[inner.Method()]; ok {
			cmd.DeclareKeys(desc, ba.Header, inner, spans)
		} else {
			return nil, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// If any command gave us spans that are invalid, bail out early
	// (before passing them to the command queue, which may panic).
	if err := spans.validate(); err != nil {
		return nil, err
	}
	return spans, nil
}

// beginCmds waits for any overlapping, already-executing commands via
// the command queue and adds itself to queues based on keys affected by the
// batched commands. This gates subsequent commands with overlapping keys or
// key ranges. This method will block if there are any overlapping commands
// already in the queue. Returns a cleanup function to be called when the
// commands are done and can be removed from the queue, and whose returned
// error is to be used in place of the supplied error.
func (r *Replica) beginCmds(
	ctx context.Context, ba *roachpb.BatchRequest, spans *SpanSet,
) (*endCmds, error) {
	var cmds [numSpanAccess]struct {
		global, local *cmd
	}
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency != roachpb.INCONSISTENT {

		// Check for context cancellation before inserting into the
		// command queue (and check again afterward). Once we're in the
		// command queue we're committed to waiting on our prerequisites
		// (which costs a goroutine, and slightly increases the cost of
		// other commands that might wait on our keys), so it's good to
		// bail out early if we can.
		select {
		case <-ctx.Done():
			err := ctx.Err()
			errStr := fmt.Sprintf("%s before command queue: %s", err, ba)
			log.Warning(ctx, errStr)
			log.ErrEvent(ctx, errStr)
			return nil, err
		default:
		}

		// Get the requested timestamp. This is used for non-interference
		// of earlier reads with later writes, but only for the global
		// command queue. Reads and writes to local keys are specified as
		// having a zero timestamp which will cause them to always
		// interfere. This is done to avoid confusion with local keys
		// declared as part of proposer evaluated KV.
		reqGlobalTS := ba.Timestamp
		if txn := ba.Txn; txn != nil {
			reqGlobalTS = txn.OrigTimestamp
		}
		var reqLocalTS hlc.Timestamp

		r.cmdQMu.Lock()
		var chans []<-chan struct{}
		// Collect all the channels to wait on before adding this batch to the
		// command queue.
		for i := SpanAccess(0); i < numSpanAccess; i++ {
			readOnly := i == SpanReadOnly
			chans = append(chans, r.cmdQMu.global.getWait(readOnly, reqGlobalTS, spans.getSpans(i, spanGlobal))...)
			chans = append(chans, r.cmdQMu.local.getWait(readOnly, reqLocalTS, spans.getSpans(i, spanLocal))...)
		}
		for i := SpanAccess(0); i < numSpanAccess; i++ {
			readOnly := i == SpanReadOnly
			cmds[i].global = r.cmdQMu.global.add(readOnly, reqGlobalTS, spans.getSpans(i, spanGlobal))
			cmds[i].local = r.cmdQMu.local.add(readOnly, reqLocalTS, spans.getSpans(i, spanLocal))
		}
		r.cmdQMu.Unlock()

		beforeWait := timeutil.Now()
		ctxDone := ctx.Done()
		numChans := len(chans)
		if numChans > 0 {
			log.Eventf(ctx, "waiting for %d overlapping requests", len(chans))
		}
		for _, ch := range chans {
			select {
			case <-ch:
			case <-ctxDone:
				err := ctx.Err()
				errStr := fmt.Sprintf("%s while in command queue: %s", err, ba)
				log.Warning(ctx, errStr)
				log.ErrEvent(ctx, errStr)
				// TODO(tamird): This should use r.store.stopper.RunAsyncTask, though
				// be careful about such a change as the code paths here are not well
				// tested.
				go func() {
					// The command is moot, so we don't need to bother executing.
					// However, the command queue assumes that commands don't drop
					// out before their prerequisites, so we still have to wait it
					// out.
					slowTimer := timeutil.NewTimer()
					defer slowTimer.Stop()
					slowTimer.Reset(base.SlowRequestThreshold)
					for _, ch := range chans {
						select {
						case <-ch:
						case <-slowTimer.C:
							r.cmdQMu.Lock()
							g := r.cmdQMu.global.String()
							l := r.cmdQMu.local.String()
							r.cmdQMu.Unlock()
							log.Warningf(r.AnnotateCtx(context.Background()),
								"have been waiting %s for dependencies: cmds:\n%+v\nglobal:\n%s\n"+
									"local:%s\n",
								base.SlowRequestThreshold, cmds, g, l)
							r.store.metrics.SlowCommandQueueRequests.Inc(1)
							defer r.store.metrics.SlowCommandQueueRequests.Dec(1)
						case <-r.store.stopper.ShouldQuiesce():
							// If the process is shutting down, we return without
							// removing this command from queue. This avoids
							// dropping out before prerequisites.
							return
						}
					}
					r.cmdQMu.Lock()
					for i := range cmds {
						r.cmdQMu.global.remove(cmds[i].global)
						r.cmdQMu.local.remove(cmds[i].local)
					}
					r.cmdQMu.Unlock()
				}()
				return nil, err
			case <-r.store.stopper.ShouldQuiesce():
				// While shutting down, commands may have been added to the
				// command queue that will never finish.
				return nil, &roachpb.NodeUnavailableError{}
			}
		}
		if numChans > 0 {
			log.Eventf(ctx, "waited %s for overlapping requests", timeutil.Since(beforeWait))
		}
	} else {
		log.Event(ctx, "operation accepts inconsistent results")
	}

	// Update the incoming timestamp if unset. Wait until after any
	// preceding command(s) for key range are complete so that the node
	// clock has been updated to the high water mark of any commands
	// which might overlap this one in effect.
	// TODO(spencer,tschottdorf): might remove this, but harder than it looks.
	//   This isn't just unittests (which would require revamping the test
	//   context sender), but also some of the scanner queues place batches
	//   directly into the local range they're servicing.
	if ba.Timestamp == (hlc.Timestamp{}) {
		if txn := ba.Txn; txn != nil {
			ba.Timestamp = txn.OrigTimestamp
		} else {
			ba.Timestamp = r.store.Clock().Now()
		}
	}

	ec := &endCmds{
		repl: r,
		cmds: cmds,
		ba:   *ba,
	}
	return ec, nil
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
func (r *Replica) applyTimestampCache(ba *roachpb.BatchRequest) (bool, *roachpb.Error) {
	span, err := keys.Range(*ba)
	if err != nil {
		return false, roachpb.NewError(err)
	}

	// TODO(peter): We only need to hold a write lock during the ExpandRequests
	// calls. Investigate whether using a RWMutex here reduces lock contention.
	r.store.tsCacheMu.Lock()
	defer r.store.tsCacheMu.Unlock()

	if ba.Txn != nil {
		r.store.tsCacheMu.cache.ExpandRequests(ba.Txn.Timestamp, span)
	} else {
		r.store.tsCacheMu.cache.ExpandRequests(ba.Timestamp, span)
	}

	var bumped bool
	for _, union := range ba.Requests {
		args := union.GetInner()
		if roachpb.ConsultsTimestampCache(args) {
			header := args.Header()
			// BeginTransaction is a special case. We use the transaction
			// key to look for an entry which would indicate this transaction
			// has already been finalized, in which case this is a replay.
			if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
				key := keys.TransactionKey(header.Key, *ba.GetTxnID())
				wTS, _, wOK := r.store.tsCacheMu.cache.GetMaxWrite(key, nil)
				if wOK {
					return bumped, roachpb.NewError(roachpb.NewTransactionReplayError())
				} else if !wTS.Less(ba.Txn.Timestamp) {
					// This is a crucial bit of code. The timestamp cache is
					// reset with the current time as the low-water
					// mark, so if this replica recently obtained the lease,
					// this case will be true for new txns, even if they're not
					// a replay. We move the timestamp forward and return retry.
					// If it's really a replay, it won't retry.
					txn := ba.Txn.Clone()
					bumped = txn.Timestamp.Forward(wTS.Next()) || bumped
					err = roachpb.NewTransactionRetryError(roachpb.RETRY_POSSIBLE_REPLAY)
					return bumped, roachpb.NewErrorWithTxn(err, &txn)
				}
				continue
			}

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID, _ := r.store.tsCacheMu.cache.GetMaxRead(header.Key, header.EndKey)
			if ba.Txn != nil {
				if rTxnID == nil || *ba.Txn.ID != *rTxnID {
					nextTS := rTS.Next()
					if ba.Txn.Timestamp.Less(nextTS) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(nextTS) || bumped
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(rTS.Next()) || bumped
			}

			// On more recent writes, forward the timestamp and set the
			// write too old boolean for transactions. Note that currently
			// only EndTransaction and DeleteRange requests update the
			// write timestamp cache.
			wTS, wTxnID, _ := r.store.tsCacheMu.cache.GetMaxWrite(header.Key, header.EndKey)
			if ba.Txn != nil {
				if wTxnID == nil || *ba.Txn.ID != *wTxnID {
					if !wTS.Less(ba.Txn.Timestamp) {
						txn := ba.Txn.Clone()
						bumped = txn.Timestamp.Forward(wTS.Next()) || bumped
						txn.WriteTooOld = true
						ba.Txn = &txn
					}
				}
			} else {
				bumped = ba.Timestamp.Forward(wTS.Next()) || bumped
			}
		}
	}
	return bumped, nil
}

// executeAdminBatch executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) executeAdminBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, roachpb.NewError(err)
	}

	args := ba.Requests[0].GetInner()
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		sp.SetOperationName(reflect.TypeOf(args).String())
	}

	// Admin commands always require the range lease.
	_, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return nil, pErr
	}

	var resp roachpb.Response
	switch tArgs := args.(type) {
	case *roachpb.AdminSplitRequest:
		var reply roachpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs)
		resp = &reply

	case *roachpb.AdminMergeRequest:
		var reply roachpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs)
		resp = &reply

	case *roachpb.AdminTransferLeaseRequest:
		pErr = roachpb.NewError(r.AdminTransferLease(ctx, tArgs.Target))
		resp = &roachpb.AdminTransferLeaseResponse{}

	case *roachpb.AdminChangeReplicasRequest:
		var err error
		for _, target := range tArgs.Targets {
			err = r.ChangeReplicas(ctx, tArgs.ChangeType, target, r.Desc())
			if err != nil {
				break
			}
		}
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminChangeReplicasResponse{}

	case *roachpb.CheckConsistencyRequest:
		var reply roachpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *roachpb.ImportRequest:
		cArgs := CommandArgs{
			EvalCtx: ReplicaEvalContext{r, nil},
			Header:  ba.Header,
			Args:    args,
		}
		var err error
		resp, err = importCmdFn(ctx, cArgs)
		pErr = roachpb.NewError(err)

	case *roachpb.AdminScatterRequest:
		reply, err := r.adminScatter(ctx, *tArgs)
		pErr = roachpb.NewError(err)
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

// executeReadOnlyBatch updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the command queue.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is consistent, the read requires the range lease.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		if _, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr
		}
	}

	spans, err := collectSpans(*r.Desc(), &ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Add the read to the command queue to gate subsequent
	// overlapping commands until this command completes.
	log.Event(ctx, "command queue")
	endCmds, err := r.beginCmds(ctx, &ba, spans)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we remove the commands from the command queue. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		endCmds.done(br, pErr, proposalNoRetry)
	}()

	if err := r.IsDestroyed(); err != nil {
		return nil, roachpb.NewError(err)
	}

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Evaluate read-only batch command. It checks for matching key range; note
	// that holding readOnlyCmdMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	var result EvalResult
	rec := ReplicaEvalContext{r, spans}
	br, result, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), rec, nil, ba)

	if intents := result.Local.detachIntents(); len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		r.store.intentResolver.processIntentsAsync(r, intents)
	}
	if pErr != nil {
		log.ErrEvent(ctx, pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, pErr
}

// executeWriteBatch is the entry point for client requests which may mutate the
// range's replicated state. Requests taking this path are ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Internally, multiple iterations of the above process may take place
// due to the Raft proposal failing retryably, possibly due to proposal
// reordering or re-proposals.
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	var ambiguousResult bool
	for count := 0; ; count++ {
		br, pErr, retry := r.tryExecuteWriteBatch(ctx, ba)
		switch retry {
		case proposalIllegalLeaseIndex:
			continue // retry
		case proposalAmbiguousShouldBeReevaluated:
			ambiguousResult = true
			continue // retry
		case proposalRangeNoLongerExists, proposalErrorReproposing:
			ambiguousResult = true
		}
		if pErr != nil {
			// If this is a transactional request but doesn't include an
			// EndTransaction with commit=true, return error immediately.
			if etArg, ok := ba.GetArg(roachpb.EndTransaction); ba.Txn != nil &&
				(!ok || !etArg.(*roachpb.EndTransactionRequest).Commit) {
				return nil, pErr
			}
			// If we've gotten an indication of possible ambiguous result,
			// we must return an AmbiguousResultError to prevent callers
			// from retrying thinking this batch could not have succeeded.
			//
			// TODO(spencer): add metrics for how often the re-proposed
			// commands succeed and how often we return errors.
			if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok && ambiguousResult {
				log.VEventf(ctx, 2, "received error after %d retries; returning ambiguous result: %s",
					count, pErr)
				are := roachpb.NewAmbiguousResultError(
					fmt.Sprintf("Raft re-proposal failed: %s", pErr))
				are.WrappedErr = pErr
				return nil, roachpb.NewError(are)
			}
		}
		return br, pErr
	}
}

// tryExecuteWriteBatch is invoked by executeWriteBatch, which will
// call this method until it returns a non-retryable result. Retries
// may happen if either the proposal was submitted to Raft but did not
// end up in a legal log position, or the proposal was submitted to
// Raft and then was re-proposed. On re-proposals, the proposal may
// have applied successfully and so the caller must be careful to
// indicate an ambiguous result to the caller in the event
// proposalReproposed is returned.
//
// Concretely,
//
// - The keys affected by the command are added to the command queue (i.e.
//   tracked as in-flight mutations).
// - Wait until the command queue promises that no overlapping mutations are
//   in flight.
// - The timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - A RaftCommand is constructed. If proposer-evaluated KV is active,
//   the request is evaluated and the EvalResult is placed in the
//   RaftCommand. If not, the request itself is added to the command.
// - The proposal is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - The result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, removed from the command queue, and
//   its result (which could be an error) returned to the client.
//
// TODO(tschottdorf): take special care with "special" commands and their
// reorderings. For example, a batch of writes and a split could be in flight
// in parallel without overlap, but if the writes hit the RHS, something must
// prevent them from writing outside of the key range when they apply.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) tryExecuteWriteBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error, retry proposalRetryReason) {
	startTime := timeutil.Now()

	spans, err := collectSpans(*r.Desc(), &ba)
	if err != nil {
		return nil, roachpb.NewError(err), proposalNoRetry
	}

	var endCmds *endCmds
	if !ba.IsLeaseRequest() {
		// Add the write to the command queue to gate subsequent overlapping
		// commands until this command completes. Note that this must be
		// done before getting the max timestamp for the key(s), as
		// timestamp cache is only updated after preceding commands have
		// been run to successful completion.
		log.Event(ctx, "command queue")
		var err error
		endCmds, err = r.beginCmds(ctx, &ba, spans)
		if err != nil {
			return nil, roachpb.NewError(err), proposalNoRetry
		}
	}

	// Guarantee we remove the commands from the command queue. This is
	// wrapped to delay pErr evaluation to its value when returning.
	defer func() {
		if endCmds != nil {
			endCmds.done(br, pErr, retry)
		}
	}()

	var lease *roachpb.Lease
	// For lease commands, use the provided previous lease for verification.
	if ba.IsSingleSkipLeaseCheckRequest() {
		lease = ba.GetPrevLeaseForLeaseRequest()
	} else {
		// Other write commands require that this replica has the range
		// lease.
		var status LeaseStatus
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr, proposalNoRetry
		}
		lease = status.lease
	}

	// Examine the read and write timestamp caches for preceding
	// commands which require this command to move its timestamp
	// forward. Or, in the case of a transactional write, the txn
	// timestamp and possible write-too-old bool.
	if bumped, pErr := r.applyTimestampCache(&ba); pErr != nil {
		return nil, pErr, proposalNoRetry
	} else if bumped {
		// If we bump the transaction's timestamp, we must absolutely
		// tell the client in a response transaction (for otherwise it
		// doesn't know about the incremented timestamp). Response
		// transactions are set far away from this code, but at the time
		// of writing, they always seem to be set. Since that is a
		// likely target of future micro-optimization, this assertion is
		// meant to protect against future correctness anomalies.
		defer func() {
			if br != nil && ba.Txn != nil && br.Txn == nil {
				log.Fatalf(ctx, "assertion failed: transaction updated by "+
					"timestamp cache, but transaction returned in response; "+
					"updated timestamp would have been lost (recovered): "+
					"%s in batch %s", ba.Txn, ba,
				)
			}
		}()
	}

	log.Event(ctx, "raft")

	ch, tryAbandon, err := r.propose(ctx, lease, ba, endCmds, spans)
	if err != nil {
		return nil, roachpb.NewError(err), proposalNoRetry
	}

	// After the command is proposed to Raft, invoking endCmds.done is now the
	// responsibility of processRaftCommand.
	endCmds = nil

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		select {
		case propResult := <-ch:
			if len(propResult.Intents) > 0 {
				// Semi-synchronously process any intents that need resolving here in
				// order to apply back pressure on the client which generated them. The
				// resolution is semi-synchronous in that there is a limited number of
				// outstanding asynchronous resolution tasks allowed after which
				// further calls will block.
				//
				// TODO(peter): Re-proposed and cancelled (but executed) commands can
				// both leave intents to GC that don't hit this code path. No good
				// solution presents itself at the moment and such intents will be
				// resolved on reads.
				r.store.intentResolver.processIntentsAsync(r, propResult.Intents)
			}
			return propResult.Reply, propResult.Err, propResult.ProposalRetry
		case <-slowTimer.C:
			log.Warningf(ctx, "have been waiting %s for proposing command %s",
				base.SlowRequestThreshold, ba)
			r.store.metrics.SlowRaftRequests.Inc(1)
			defer r.store.metrics.SlowRaftRequests.Dec(1)

		case <-ctxDone:
			// If our context was cancelled, return an AmbiguousResultError
			// if the command isn't already being executed and using our
			// context, in which case we expect it to finish soon. The
			// AmbiguousResultError indicates to caller that the command may
			// have executed.
			if tryAbandon() {
				log.Warningf(ctx, "context cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error())), proposalNoRetry
			}
			ctxDone = nil
		case <-shouldQuiesce:
			// If shutting down, return an AmbiguousResultError if the
			// command isn't already being executed and using our context,
			// in which case we expect it to finish soon. AmbiguousResultError
			// indicates to caller that the command may have executed. If
			// tryAbandon fails, we iterate through the loop again to wait
			// for the command to finish.
			if tryAbandon() {
				log.Warningf(ctx, "shutdown cancellation after %0.1fs of attempting command %s",
					timeutil.Since(startTime).Seconds(), ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown")), proposalNoRetry
			}
			shouldQuiesce = nil
		}
	}
}

// requestToProposal converts a BatchRequest into a ProposalData, by
// evaluating it. The returned ProposalData is partially valid even
// on a non-nil *roachpb.Error.
func (r *Replica) requestToProposal(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	ba roachpb.BatchRequest,
	endCmds *endCmds,
	spans *SpanSet,
) (*ProposalData, *roachpb.Error) {
	proposal := &ProposalData{
		ctx:     ctx,
		idKey:   idKey,
		endCmds: endCmds,
		doneCh:  make(chan proposalResult, 1),
		Request: &ba,
	}
	var pErr *roachpb.Error
	var result *EvalResult
	result, pErr = r.evaluateProposal(ctx, idKey, ba, spans)
	// Fill out the local results even if pErr != nil; we'll return the error below.
	proposal.Local = &result.Local
	proposal.command = storagebase.RaftCommand{
		ReplicatedEvalResult: &result.Replicated,
		WriteBatch:           result.WriteBatch,
	}
	if r.store.TestingKnobs().TestingEvalFilter != nil {
		// For backwards compatibility, tests that use TestingEvalFilter
		// need the original request to be preserved. See #10493
		proposal.command.BatchRequest = &ba
	}
	return proposal, pErr
}

// evaluateProposal generates an EvalResult from the given request by
// evaluating it, returning both state which is held only on the
// proposer and that which is to be replicated through Raft. The
// return value is ready to be inserted into Replica's proposal map
// and subsequently passed to submitProposalLocked.
//
// If an *Error is returned, the proposal should fail fast, i.e. be
// sent directly back to the client without going through Raft, but
// while still handling LocalEvalResult. Note that even if this method
// returns a nil error, the command could still return an error to the
// client, but only after going through raft (e.g. to lay down
// intents).
//
// Replica.mu must not be held.
func (r *Replica) evaluateProposal(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *SpanSet,
) (*EvalResult, *roachpb.Error) {
	// Note that we don't hold any locks at this point. This is important
	// since evaluating a proposal is expensive (at least under proposer-
	// evaluated KV).
	var result EvalResult

	if ba.Timestamp == (hlc.Timestamp{}) {
		return nil, roachpb.NewErrorf("can't propose Raft command with zero timestamp")
	}

	result = r.evaluateProposalInner(ctx, idKey, ba, spans)

	if result.Local.Err != nil {
		// Failed proposals (whether they're failfast or not) can't have any
		// EvalResult except what's whitelisted here.
		result.Local = LocalEvalResult{
			intents:            result.Local.intents,
			Err:                r.maybeSetCorrupt(ctx, result.Local.Err),
			leaseMetricsResult: result.Local.leaseMetricsResult,
		}
		if result.WriteBatch == nil {
			result.Replicated.Reset()
		}
	}

	result.Replicated.IsLeaseRequest = ba.IsLeaseRequest()
	result.Replicated.Timestamp = ba.Timestamp
	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	result.Replicated.StartKey = rSpan.Key
	result.Replicated.EndKey = rSpan.EndKey

	if result.WriteBatch == nil {
		if result.Local.Err == nil {
			log.Fatalf(ctx, "proposal must fail fast with an error: %+v", ba)
		}
		return &result, result.Local.Err
	}

	// If there is an error, it will be returned to the client when the
	// proposal (and thus WriteBatch) applies.
	return &result, nil
}

// insertProposalLocked assigns a MaxLeaseIndex to a proposal and adds
// it to the pending map.
func (r *Replica) insertProposalLocked(
	proposal *ProposalData, proposerReplica roachpb.ReplicaDescriptor, proposerLease *roachpb.Lease,
) {
	// Assign a lease index. Note that we do this as late as possible
	// to make sure (to the extent that we can) that we don't assign
	// (=predict) the index differently from the order in which commands are
	// proposed (and thus likely applied).
	if r.mu.lastAssignedLeaseIndex < r.mu.state.LeaseAppliedIndex {
		r.mu.lastAssignedLeaseIndex = r.mu.state.LeaseAppliedIndex
	}
	if !proposal.Request.IsLeaseRequest() {
		r.mu.lastAssignedLeaseIndex++
	}
	proposal.command.MaxLeaseIndex = r.mu.lastAssignedLeaseIndex
	proposal.command.ProposerReplica = proposerReplica
	proposal.command.ProposerLease = proposerLease
	if log.V(4) {
		log.Infof(proposal.ctx, "submitting proposal %x: maxLeaseIndex=%d",
			proposal.idKey, proposal.command.MaxLeaseIndex)
	}

	if _, ok := r.mu.proposals[proposal.idKey]; ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "pending command already exists for %s", proposal.idKey)
	}
	r.mu.proposals[proposal.idKey] = proposal
}

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

// propose prepares the necessary pending command struct and initializes a
// client command ID if one hasn't been. A verified lease is supplied
// as a parameter if the command requires a lease; nil otherwise. It
// then proposes the command to Raft and returns
// - a channel which receives a response or error upon application
// - a closure used to attempt to abandon the command. When called, it tries to
//   remove the pending command from the internal commands map. This is
//   possible until execution of the command at the local replica has already
//   begun, in which case false is returned and the client needs to continue
//   waiting for successful execution.
// - any error obtained during the creation or proposal of the command, in
//   which case the other returned values are zero.
func (r *Replica) propose(
	ctx context.Context,
	lease *roachpb.Lease,
	ba roachpb.BatchRequest,
	endCmds *endCmds,
	spans *SpanSet,
) (chan proposalResult, func() bool, error) {
	if err := r.IsDestroyed(); err != nil {
		return nil, nil, err
	}

	// submitProposalLocked calls withRaftGroupLocked which requires that
	// raftMu is held. In order to maintain our lock ordering we need to lock
	// Replica.raftMu here before locking Replica.mu.
	//
	// TODO(peter): It appears that we only need to hold Replica.raftMu when
	// calling raft.NewRawNode. We could get fancier with the locking here to
	// optimize for the common case where Replica.mu.internalRaftGroup is
	// non-nil, but that doesn't currently seem worth it. Always locking raftMu
	// has a tiny (~1%) performance hit for single-node block_writer testing.
	//
	// TODO(tschottdorf): holding raftMu during evaluation limits concurrency
	// at the range level and is something we will eventually need to address.
	// See #10084.
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, nil, err
	}

	// Must check that the request is in bounds at proposal time in
	// addition to application time because some evaluation functions
	// (especially EndTransaction with SplitTrigger) fail (with a
	// replicaCorruptionError) if called when out of bounds. This is not
	// synchronized with anything else, but in cases where it matters
	// the command is also registered in the command queue for the range
	// descriptor key.
	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, nil, err
	}

	idKey := makeIDKey()
	proposal, pErr := r.requestToProposal(ctx, idKey, ba, endCmds, spans)
	// An error here corresponds to a failfast-proposal: The command resulted
	// in an error and did not need to commit a batch (the common error case).
	if pErr != nil {
		if proposal.Local == nil || proposal.command.ReplicatedEvalResult == nil {
			return nil, nil, errors.Errorf(
				"requestToProposal returned error %s without eval results", pErr)
		}
		intents := proposal.Local.detachIntents()
		r.handleEvalResult(ctx, proposal.Local, proposal.command.ReplicatedEvalResult)
		if endCmds != nil {
			endCmds.done(nil, pErr, proposalNoRetry)
		}
		ch := make(chan proposalResult, 1)
		ch <- proposalResult{Err: pErr, Intents: intents}
		close(ch)
		return ch, func() bool { return false }, nil
	}

	if proposal.command.Size() > int(maxCommandSize.Get()) {
		// Once a command is written to the raft log, it must be loaded
		// into memory and replayed on all replicas. If a command is
		// too big, stop it here.
		return nil, nil, errors.Errorf("command is too large: %d bytes (max: %d)",
			proposal.command.Size(), maxCommandSize.Get())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// NB: We need to check Replica.mu.destroyed again in case the Replica has
	// been destroyed between the initial check at the beginning of this method
	// and the acquisition of Replica.mu. Failure to do so will leave pending
	// proposals that never get cleared.
	if err := r.mu.destroyed; err != nil {
		return nil, nil, err
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return nil, nil, err
	}
	r.insertProposalLocked(proposal, repDesc, lease)

	if err := r.submitProposalLocked(proposal); err != nil {
		delete(r.mu.proposals, proposal.idKey)
		return nil, nil, err
	}
	// Must not use `proposal` in the closure below as a proposal which is not
	// present in r.mu.proposals is no longer protected by the mutex. Abandoning
	// a command only abandons the associated context. As soon as we propose a
	// command to Raft ownership passes to the "below Raft" machinery. In
	// particular, endCmds will be invoked when the command is applied. There are
	// a handful of cases where the command may not be applied (or even
	// processed): the process crashes or the local replica is removed from the
	// range.
	tryAbandon := func() bool {
		r.mu.Lock()
		p, ok := r.mu.proposals[idKey]
		if ok {
			// TODO(radu): Should this context be created via tracer.ForkCtxSpan?
			// We'd need to make sure the span is finished eventually.
			p.ctx = r.AnnotateCtx(context.TODO())
		}
		r.mu.Unlock()
		return ok
	}
	return proposal.doneCh, tryAbandon, nil
}

// submitProposalLocked proposes or re-proposes a command in r.mu.proposals.
// The replica lock must be held.
func (r *Replica) submitProposalLocked(p *ProposalData) error {
	p.proposedAtTicks = r.mu.ticks

	if r.mu.submitProposalFn != nil {
		return r.mu.submitProposalFn(p)
	}
	return defaultSubmitProposalLocked(r, p)
}

func (r *Replica) isSoloReplicaRLocked() bool {
	return len(r.mu.state.Desc.Replicas) == 1 &&
		r.mu.state.Desc.Replicas[0].ReplicaID == r.mu.replicaID
}

func defaultSubmitProposalLocked(r *Replica, p *ProposalData) error {
	ctx := r.AnnotateCtx(context.TODO())

	data, err := protoutil.Marshal(&p.command)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)

	var changeReplicas *storagebase.ChangeReplicas
	if p.command.ReplicatedEvalResult != nil {
		changeReplicas = p.command.ReplicatedEvalResult.ChangeReplicas
	} else {
		if union, ok := p.Request.GetArg(roachpb.EndTransaction); ok {
			ict := union.(*roachpb.EndTransactionRequest).InternalCommitTrigger
			if tr := ict.GetChangeReplicasTrigger(); tr != nil {
				changeReplicas = &storagebase.ChangeReplicas{
					ChangeReplicasTrigger: *tr,
				}
			}
		}
	}

	if crt := changeReplicas; crt != nil {
		// EndTransactionRequest with a ChangeReplicasTrigger is special
		// because raft needs to understand it; it cannot simply be an
		// opaque command.
		log.Infof(ctx, "proposing %s %+v: %+v",
			crt.ChangeType, crt.Replica, crt.UpdatedReplicas)

		// Ensure that we aren't trying to remove ourselves from the range without
		// having previously given up our lease, since the range won't be able
		// to make progress while the lease is owned by a removed replica (and
		// leases can stay in such a state for a very long time when using epoch-
		// based range leases). This shouldn't happen often, but has been seen
		// before (#12591).
		if crt.ChangeType == roachpb.REMOVE_REPLICA &&
			crt.Replica.ReplicaID == r.mu.replicaID {
			log.Errorf(ctx, "received invalid ChangeReplicasTrigger %+v to remove leaseholder replica %+v",
				crt, r.mu.state)
			return errors.Errorf("%s: invalid ChangeReplicasTrigger %+v to remove leaseholder replica",
				r, crt)
		}

		confChangeCtx := ConfChangeContext{
			CommandID: string(p.idKey),
			Payload:   data,
			Replica:   crt.Replica,
		}
		encodedCtx, err := protoutil.Marshal(&confChangeCtx)
		if err != nil {
			return err
		}

		return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
			// We're proposing a command here so there is no need to wake the
			// leader if we were quiesced.
			r.unquiesceLocked()
			return false, /* !unquiesceAndWakeLeader */
				raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    changeTypeInternalToRaft[crt.ChangeType],
					NodeID:  uint64(crt.Replica.ReplicaID),
					Context: encodedCtx,
				})
		})
	}

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		if log.V(4) {
			log.Infof(ctx, "proposing command %x: %s", p.idKey, p.Request.Summary())
		}
		// We're proposing a command so there is no need to wake the leader if we
		// were quiesced.
		r.unquiesceLocked()
		return false /* !unquiesceAndWakeLeader */, raftGroup.Propose(encodeRaftCommand(p.idKey, data))
	})
}

// mark the replica as quiesced. Returns true if the Replica is successfully
// quiesced and false otherwise.
func (r *Replica) quiesce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.quiesceLocked()
}

func (r *Replica) quiesceLocked() bool {
	ctx := r.AnnotateCtx(context.TODO())
	if len(r.mu.proposals) != 0 {
		log.Infof(ctx, "not quiescing: %d pending commands", len(r.mu.proposals))
		return false
	}
	if !r.mu.quiescent {
		if log.V(3) {
			log.Infof(ctx, "quiescing")
		}
		r.mu.quiescent = true
	} else if log.V(4) {
		log.Infof(ctx, "already quiesced")
	}
	return true
}

func (r *Replica) unquiesceLocked() {
	if r.mu.quiescent {
		if log.V(3) {
			ctx := r.AnnotateCtx(context.TODO())
			log.Infof(ctx, "unquiescing")
		}
		r.mu.quiescent = false
	}
}

func (r *Replica) unquiesceAndWakeLeaderLocked() {
	if r.mu.quiescent {
		if log.V(3) {
			ctx := r.AnnotateCtx(context.TODO())
			log.Infof(ctx, "unquiescing: waking leader")
		}
		r.mu.quiescent = false
		// Propose an empty command which will wake the leader.
		_ = r.mu.internalRaftGroup.Propose(encodeRaftCommand(makeIDKey(), nil))
	}
}

type handleRaftReadyStats struct {
	processed int
}

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
func (r *Replica) handleRaftReady(inSnap IncomingSnapshot) (handleRaftReadyStats, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
func (r *Replica) handleRaftReadyRaftMuLocked(
	inSnap IncomingSnapshot,
) (handleRaftReadyStats, error) {
	var stats handleRaftReadyStats

	ctx := r.AnnotateCtx(context.TODO())
	var hasReady bool
	var rd raft.Ready
	r.mu.Lock()

	lastIndex := r.mu.lastIndex // used for append below
	raftLogSize := r.mu.raftLogSize
	leaderID := r.mu.leaderID
	err := r.withRaftGroupLocked(false, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	r.mu.Unlock()
	if err != nil {
		return stats, err
	}

	if !hasReady {
		return stats, nil
	}

	logRaftReady(ctx, rd)

	refreshReason := noReason
	if rd.SoftState != nil && leaderID != roachpb.ReplicaID(rd.SoftState.Lead) {
		// Refresh pending commands if the Raft leader has changed. This is usually
		// the first indication we have of a new leader on a restarted node.
		//
		// TODO(peter): Re-proposing commands when SoftState.Lead changes can lead
		// to wasteful multiple-reproposals when we later see an empty Raft command
		// indicating a newly elected leader or a conf change. Replay protection
		// prevents any corruption, so the waste is only a performance issue.
		if log.V(3) {
			log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, rd.SoftState.Lead)
		}
		if !r.store.TestingKnobs().DisableRefreshReasonNewLeader {
			refreshReason = reasonNewLeader
		}
		leaderID = roachpb.ReplicaID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		if err != nil {
			return stats, errors.Wrap(err, "invalid snapshot id")
		}
		if inSnap.SnapUUID == (uuid.UUID{}) {
			log.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
		}
		if snapUUID != inSnap.SnapUUID {
			log.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", snapUUID, inSnap.SnapUUID)
		}

		if err := r.applySnapshot(ctx, inSnap, rd.Snapshot, rd.HardState); err != nil {
			return stats, err
		}

		if err := func() error {
			r.store.mu.Lock()
			defer r.store.mu.Unlock()

			if r.store.removePlaceholderLocked(r.RangeID) {
				atomic.AddInt32(&r.store.counts.filledPlaceholders, 1)
			}
			if err := r.store.processRangeDescriptorUpdateLocked(ctx, r); err != nil {
				return errors.Wrap(err, "could not processRangeDescriptorUpdate after applySnapshot")
			}
			return nil
		}(); err != nil {
			return stats, err
		}

		if lastIndex, err = r.stateLoader.loadLastIndex(ctx, r.store.Engine()); err != nil {
			return stats, err
		}
		// We refresh pending commands after applying a snapshot because this
		// replica may have been temporarily partitioned from the Raft group and
		// missed leadership changes that occurred. Suppose node A is the leader,
		// and then node C gets partitioned away from the others. Leadership passes
		// back and forth between A and B during the partition, but when the
		// partition is healed node A is leader again.
		if !r.store.TestingKnobs().DisableRefreshReasonSnapshotApplied &&
			refreshReason == noReason {
			refreshReason = reasonSnapshotApplied
		}
	}

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		var err error
		if lastIndex, raftLogSize, err = r.append(ctx, writer, lastIndex, raftLogSize, rd.Entries); err != nil {
			return stats, err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.stateLoader.setHardState(ctx, writer, rd.HardState); err != nil {
			return stats, err
		}
	}
	writer.Close()
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	start := timeutil.Now()
	if err := batch.Commit(syncRaftLog.Get() && rd.MustSync); err != nil {
		return stats, err
	}
	elapsed := timeutil.Since(start)
	r.store.metrics.RaftLogCommitLatency.RecordValue(elapsed.Nanoseconds())

	// Update protected state (last index, raft log size and raft leader
	// ID) and set raft log entry cache. We clear any older, uncommitted
	// log entries and cache the latest ones.
	r.mu.Lock()
	r.store.raftEntryCache.addEntries(r.RangeID, rd.Entries)
	r.mu.lastIndex = lastIndex
	r.mu.raftLogSize = raftLogSize
	r.mu.leaderID = leaderID
	r.mu.Unlock()

	for _, msg := range rd.Messages {
		r.sendRaftMessage(ctx, msg)
	}

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:

			var commandID storagebase.CmdIDKey
			var command storagebase.RaftCommand

			// Process committed entries. etcd raft occasionally adds a nil entry
			// (our own commands are never empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				refreshReason = reasonNewLeaderOrConfigChange
				commandID = "" // special-cased value, command isn't used
			} else {
				var encodedCommand []byte
				commandID, encodedCommand = DecodeRaftCommand(e.Data)
				// An empty command is used to unquiesce a range and wake the
				// leader. Clear commandID so it's ignored for processing.
				if len(encodedCommand) == 0 {
					commandID = ""
				} else if err := command.Unmarshal(encodedCommand); err != nil {
					return stats, err
				}
			}

			if changedRepl := r.processRaftCommand(ctx, commandID, e.Index, command); changedRepl {
				log.Fatalf(ctx, "unexpected replication change from command %s", &command)
			}
			r.store.metrics.RaftCommandsApplied.Inc(1)
			stats.processed++

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return stats, err
			}
			var ccCtx ConfChangeContext
			if err := ccCtx.Unmarshal(cc.Context); err != nil {
				return stats, err
			}
			var command storagebase.RaftCommand
			if err := command.Unmarshal(ccCtx.Payload); err != nil {
				return stats, err
			}
			if changedRepl := r.processRaftCommand(
				ctx, storagebase.CmdIDKey(ccCtx.CommandID), e.Index, command,
			); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			stats.processed++
			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				return stats, err
			}
		default:
			log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		}
	}
	if refreshReason != noReason {
		r.mu.Lock()
		r.refreshProposalsLocked(0, refreshReason)
		r.mu.Unlock()
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	return stats, r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.Advance(rd)
		return true, nil
	})
}

// tick the Raft group, returning any error and true if the raft group exists
// and false otherwise.
func (r *Replica) tick() (bool, error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.tickRaftMuLocked()
}

// tickRaftMuLocked requires that raftMu is held, but not replicaMu.
func (r *Replica) tickRaftMuLocked() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.unreachablesMu.Lock()
	remotes := r.unreachablesMu.remotes
	r.unreachablesMu.remotes = nil
	r.unreachablesMu.Unlock()

	// If the raft group is uninitialized, do not initialize raft groups on
	// tick.
	if r.mu.internalRaftGroup == nil {
		return false, nil
	}

	for remoteReplica := range remotes {
		r.mu.internalRaftGroup.ReportUnreachable(uint64(remoteReplica))
	}

	if r.mu.quiescent {
		// While a replica is quiesced we still advance its logical clock. This is
		// necessary to avoid a scenario where the leader quiesces and a follower
		// does not. The follower calls an election but the election fails because
		// the leader and other follower believe that no time in the current term
		// has passed. The Raft group is then in a state where one member has a
		// term that is advanced which will then cause subsequent heartbeats from
		// the existing leader to be rejected in a way that the leader will step
		// down. This situation is caused by an interaction between quiescence and
		// the Raft CheckQuorum feature which relies on the logical clock ticking
		// at roughly the same rate on all members of the group.
		//
		// By ticking the logical clock (incrementing an integer) we avoid this
		// situation. If one of the followers does not quiesce it will call an
		// election but the election will succeed. Note that while we expect such
		// elections from quiesced followers to be extremely rare, it is very
		// difficult to completely eliminate them so we want to minimize the
		// disruption when they do occur.
		//
		// For more details, see #9372.
		// TODO(bdarnell): remove this once we have fully switched to PreVote
		if tickQuiesced {
			r.mu.internalRaftGroup.TickQuiesced()
		}
		return false, nil
	}
	if r.maybeQuiesceLocked() {
		return false, nil
	}

	r.mu.ticks++
	r.mu.internalRaftGroup.Tick()
	if !r.store.TestingKnobs().DisableRefreshReasonTicks &&
		r.mu.ticks%r.store.cfg.RaftElectionTimeoutTicks == 0 {
		// RaftElectionTimeoutTicks is a reasonable approximation of how long we
		// should wait before deciding that our previous proposal didn't go
		// through. Note that the combination of the above condition and passing
		// RaftElectionTimeoutTicks to refreshProposalsLocked means that commands
		// will be refreshed when they have been pending for 1 to 2 election
		// cycles.
		r.refreshProposalsLocked(
			r.store.cfg.RaftElectionTimeoutTicks, reasonTicks,
		)
	}
	return true, nil
}

// maybeQuiesceLocked checks to see if the replica is quiescable and initiates
// quiescence if it is. Returns true if the replica has been quiesced and false
// otherwise.
//
// A quiesced range is not ticked and thus doesn't create MsgHeartbeat requests
// or cause elections. The Raft leader for a range checks various
// pre-conditions: no pending raft commands, no pending raft ready, all of the
// followers are up to date, etc. Quiescence is initiated by a special
// MsgHeartbeat that is tagged as Quiesce. Upon receipt (see
// Store.processRaftRequest), the follower checks to see if the term/commit
// matches and marks the local replica as quiescent. If the term/commit do not
// match the MsgHeartbeat is passed through to Raft which will generate a
// MsgHeartbeatResp that will unquiesce the sender.
//
// Any Raft operation on the local replica will unquiesce the Replica. For
// example, a Raft operation initiated on a follower will unquiesce the
// follower which will send a MsgProp to the leader that will unquiesce the
// leader. If the leader of a quiesced range dies, followers will not notice,
// though any request directed to the range will eventually end up on a
// follower which will unquiesce the follower and lead to an election. When a
// follower unquiesces for a reason other than receiving a raft message or
// proposing a raft command (for example the concurrent enqueuing of a tick),
// it wakes the leader by sending an empty message proposal. This avoids
// unnecessary elections due to bugs in which a follower is left unquiesced
// while the leader is quiesced.
//
// Note that both the quiesce and wake-the-leader messages can be dropped or
// reordered by the transport. The wake-the-leader message is termless so it
// won't affect elections and, while it triggers reproprosals that won't cause
// problems on reordering. If the wake-the-leader message is dropped the leader
// won't be woken and the follower will eventually call an election.
//
// If the quiesce message is dropped the follower which missed it will not
// quiesce and will eventually cause an election. The quiesce message is tagged
// with the current term and commit index. If the quiesce message is reordered
// it will either still apply to the recipient or the recipient will have moved
// forward and the quiesce message will fall back to being a heartbeat.
//
// TODO(peter): There remains a scenario in which a follower is left unquiesced
// while the leader is quiesced: the follower's receive queue is full and the
// "quiesce" message is dropped. This seems very very unlikely because if the
// follower isn't keeping up with raft messages it is unlikely that the leader
// would quiesce. The fallout from this situation are undesirable raft
// elections which will cause throughput hiccups to the range, but not
// correctness issues.
//
// TODO(peter): When a node goes down, any range which has a replica on the
// down node will not quiesce. This could be a significant performance
// impact. Additionally, when the node comes back up we want to bring any
// replicas it contains back up to date. Right now this will be handled because
// those ranges never quiesce. One thought for handling both these scenarios is
// to hook into the StorePool and its notion of "down" nodes. But that might
// not be sensitive enough.
func (r *Replica) maybeQuiesceLocked() bool {
	ctx := r.AnnotateCtx(context.TODO())
	if len(r.mu.proposals) != 0 {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d pending commands", len(r.mu.proposals))
		}
		return false
	}
	status := r.mu.internalRaftGroup.Status()
	if status.SoftState.RaftState != raft.StateLeader {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leader")
		}
		return false
	}
	// Only quiesce if this replica is the leaseholder as well;
	// otherwise the replica which is the valid leaseholder may have
	// pending commands which it's waiting on this leader to propose.
	if l := r.mu.state.Lease; !l.OwnedBy(r.store.StoreID()) &&
		r.isLeaseValidRLocked(l, r.store.Clock().Now()) {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: not leaseholder")
		}
		// Try to correct leader-not-leaseholder condition, if encountered,
		// assuming the leaseholder is caught up to the commit index.
		if pr, ok := status.Progress[uint64(l.Replica.ReplicaID)]; ok && pr.Match >= status.Commit {
			log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", l.Replica.ReplicaID)
			r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
			r.mu.internalRaftGroup.TransferLeader(uint64(l.Replica.ReplicaID))
		}
		return false
	}
	// We need all of Applied, Commit, LastIndex and Progress.Match indexes to be
	// equal in order to quiesce.
	if status.Applied != status.Commit {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: applied (%d) != commit (%d)",
				status.Applied, status.Commit)
		}
		return false
	}
	if status.Commit != r.mu.lastIndex {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: commit (%d) != last-index (%d)",
				status.Commit, r.mu.lastIndex)
		}
		return false
	}
	var foundSelf bool
	for id, progress := range status.Progress {
		if roachpb.ReplicaID(id) == r.mu.replicaID {
			foundSelf = true
		}
		if progress.Match != status.Applied {
			if log.V(4) {
				log.Infof(ctx, "not quiescing: replica %d match (%d) != applied (%d)",
					id, progress.Match, status.Applied)
			}
			return false
		}
	}
	if !foundSelf {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: %d not found in progress: %+v",
				r.mu.replicaID, status.Progress)
		}
		return false
	}
	if r.mu.internalRaftGroup.HasReady() {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: raft ready")
		}
		return false
	}
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(r.mu.replicaID, r.mu.lastToReplica)
	if fromErr != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: cannot find from replica (%d)", r.mu.replicaID)
		}
		return false
	}

	r.quiesceLocked()
	for id := range status.Progress {
		if roachpb.ReplicaID(id) == r.mu.replicaID {
			continue
		}
		toReplica, toErr := r.getReplicaDescriptorByIDRLocked(
			roachpb.ReplicaID(id), r.mu.lastFromReplica)
		if toErr != nil {
			if log.V(4) {
				log.Infof(ctx, "failed to quiesce: cannot find to replica (%d)", id)
			}
			r.unquiesceLocked()
			return false
		}
		msg := raftpb.Message{
			From:   uint64(r.mu.replicaID),
			To:     id,
			Type:   raftpb.MsgHeartbeat,
			Term:   status.Term,
			Commit: status.Commit,
		}

		if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, true) {
			continue
		}

		req := &RaftMessageRequest{
			RangeID:     r.RangeID,
			ToReplica:   toReplica,
			FromReplica: fromReplica,
			Message:     msg,
			Quiesce:     true,
		}
		if !r.sendRaftMessageRequest(ctx, req) {
			r.unquiesceLocked()
			r.mu.droppedMessages++
			r.mu.internalRaftGroup.ReportUnreachable(id)
			return false
		}
	}
	return true
}

// pendingCmdSlice sorts by increasing MaxLeaseIndex.
type pendingCmdSlice []*ProposalData

func (s pendingCmdSlice) Len() int      { return len(s) }
func (s pendingCmdSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s pendingCmdSlice) Less(i, j int) bool {
	return s[i].command.MaxLeaseIndex < s[j].command.MaxLeaseIndex
}

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeader
	reasonNewLeaderOrConfigChange
	reasonSnapshotApplied
	reasonReplicaIDChanged
	reasonTicks
)

// refreshProposalsLocked goes through the pending proposals, notifying
// proposers whose proposals need to be retried, and resubmitting proposals
// which were likely dropped (but may still apply at a legal Lease index) -
// ensuring that the proposer will eventually get a reply on the channel it's
// waiting on.
// mu must be held.
//
// refreshAtDelta only applies for reasonTicks and specifies how old (in ticks)
// a command must be for it to be inspected; the usual value is the number of
// ticks of an election timeout (affect only proposals that have had ample time
// to apply but didn't).
func (r *Replica) refreshProposalsLocked(refreshAtDelta int, reason refreshRaftReason) {
	if refreshAtDelta != 0 && reason != reasonTicks {
		log.Fatalf(context.TODO(), "refreshAtDelta specified for reason %s != reasonTicks", reason)
	}

	numShouldRetry := 0
	var reproposals pendingCmdSlice
	for idKey, p := range r.mu.proposals {
		if p.command.MaxLeaseIndex == 0 {
			// Commands without a MaxLeaseIndex cannot be reproposed, as they might
			// apply twice. We also don't want to ask the proposer to retry these
			// special commands.
			delete(r.mu.proposals, idKey)
			log.VEventf(p.ctx, 2, "refresh (reason: %s) returning AmbiguousResultError for command "+
				"without MaxLeaseIndex: %v", reason, p.command)
			p.finishRaftApplication(proposalResult{Err: roachpb.NewError(
				roachpb.NewAmbiguousResultError(
					fmt.Sprintf("unknown status for command without MaxLeaseIndex "+
						"at refreshProposalsLocked time (refresh reason: %s)", reason)))})
		} else if p.command.MaxLeaseIndex <= r.mu.state.LeaseAppliedIndex {
			// The command's designated lease index slot was filled up. We got to
			// LeaseAppliedIndex and p is still pending in r.mu.proposals; generally
			// this means that proposal p didn't commit, and it will be sent back to
			// the proposer for a retry - the request needs to be re-evaluated and the
			// command re-proposed with a new MaxLeaseIndex.
			//
			// An exception is the case when we're refreshing because of
			// reasonSnapshotApplied - in that case we don't know if p or some other
			// command filled the p.command.MaxLeaseIndex slot (i.e. p might have been
			// applied, but we're not watching for every proposal when applying a
			// snapshot, so nobody removed p from r.mu.proposals). In this ambiguous
			// case, we'll also send the command back to the proposer for a retry, but
			// the proposer needs to be aware that, if the retry fails, an
			// AmbiguousResultError needs to be returned to the higher layers.
			// We're relying on the fact that all commands are either idempotent
			// (generally achieved through the wonders of MVCC) or, if they aren't,
			// the 2nd application will somehow fail to apply (e.g. a command
			// resulting from a RequestLease is not idempotent, but the 2nd
			// application will be rejected by the ProposerLease verification).
			//
			// Note that we can't use the commit index here (which is typically a
			// little ahead), because a pending command is removed only as it
			// applies. Thus we'd risk reproposing a command that has been committed
			// but not yet applied.
			delete(r.mu.proposals, idKey)
			log.Eventf(p.ctx, "retry proposal %x: %s", p.idKey, reason)
			if reason == reasonSnapshotApplied {
				p.finishRaftApplication(proposalResult{ProposalRetry: proposalAmbiguousShouldBeReevaluated})
			} else {
				p.finishRaftApplication(proposalResult{ProposalRetry: proposalIllegalLeaseIndex})
			}
			numShouldRetry++
		} else if reason == reasonTicks && p.proposedAtTicks > r.mu.ticks-refreshAtDelta {
			// The command was proposed too recently, don't bother reproprosing it
			// yet.
		} else {
			// The proposal can still apply according to its MaxLeaseIndex. But it's
			// likely that the proposal was dropped, so we're going to repropose it
			// directly; it's cheaper than asking the proposer to re-evaluate and
			// re-propose. Note that we don't need to worry about receiving ambiguous
			// responses for this reproposal - we'll only be handling one response
			// across the original proposal and the reproposal because we're
			// reproposing with the same MaxLeaseIndex and the same idKey as the
			// original (a possible second response for the same idKey would be
			// ignored).
			//
			// This situation happens when we've proposed commands that never made it
			// to the leader - usually because we didn't know who the leader is. When
			// we do discover the leader we need to repropose the command. In local
			// testing, by far the most common reason for these reproposals is the
			// initial leader election after a split.  Specifically, when a range is
			// split when the next command is proposed to the RHS the leader is
			// elected. After the election completes the command is reproposed for
			// both reasonNewLeader and reasonNewLeaderOrConfigChange.
			reproposals = append(reproposals, p)
		}
	}
	if log.V(1) && (numShouldRetry > 0 || len(reproposals) > 0) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Infof(ctx,
			"pending commands: sent %d back to client, reproposing %d (at %d.%d) %s",
			numShouldRetry, len(reproposals), r.mu.state.RaftAppliedIndex,
			r.mu.state.LeaseAppliedIndex, reason)
	}

	// Reproposals are those commands which we weren't able to send back to the
	// client (since we're not sure that another copy of them could apply at
	// the "correct" index). For reproposals, it's generally pretty unlikely
	// that they can make it in the right place. Reproposing in order is
	// definitely required, however.
	//
	// TODO(tschottdorf): evaluate whether `r.mu.proposals` should be a list/slice.
	sort.Sort(reproposals)
	for _, p := range reproposals {
		log.Eventf(p.ctx, "re-submitting command %x to Raft: %s", p.idKey, reason)
		if err := r.submitProposalLocked(p); err != nil {
			delete(r.mu.proposals, p.idKey)
			p.finishRaftApplication(proposalResult{Err: roachpb.NewError(err), ProposalRetry: proposalErrorReproposing})
		}
	}
}

func (r *Replica) getReplicaDescriptorByIDRLocked(
	replicaID roachpb.ReplicaID, fallback roachpb.ReplicaDescriptor,
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

// maybeCoalesceHeartbeat returns true if the heartbeat was coalesced and added
// to the appropriate queue.
func (r *Replica) maybeCoalesceHeartbeat(
	ctx context.Context,
	msg raftpb.Message,
	toReplica, fromReplica roachpb.ReplicaDescriptor,
	quiesce bool,
) bool {
	var hbMap map[roachpb.StoreIdent][]RaftHeartbeat
	switch msg.Type {
	case raftpb.MsgHeartbeat:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		r.store.coalescedMu.Lock()
		hbMap = r.store.coalescedMu.heartbeatResponses
	default:
		return false
	}
	beat := RaftHeartbeat{
		RangeID:       r.RangeID,
		ToReplicaID:   toReplica.ReplicaID,
		FromReplicaID: fromReplica.ReplicaID,
		Term:          msg.Term,
		Commit:        msg.Commit,
		Quiesce:       quiesce,
	}
	if log.V(4) {
		log.Infof(ctx, "coalescing beat: %+v", beat)
	}
	toStore := roachpb.StoreIdent{
		StoreID: toReplica.StoreID,
		NodeID:  toReplica.NodeID,
	}
	hbMap[toStore] = append(hbMap[toStore], beat)
	r.store.coalescedMu.Unlock()
	return true
}

func (r *Replica) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	r.mu.Lock()
	fromReplica, fromErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDRLocked(roachpb.ReplicaID(msg.To), r.mu.lastFromReplica)
	r.mu.Unlock()

	if fromErr != nil {
		log.Warningf(ctx, "failed to look up sender replica %d in r%d while sending %s: %s",
			msg.From, r.RangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		log.Warningf(ctx, "failed to look up recipient replica %d in r%d while sending %s: %s",
			msg.To, r.RangeID, msg.Type, toErr)
		return
	}

	// Raft-initiated snapshots are handled by the Raft snapshot queue.
	if msg.Type == raftpb.MsgSnap {
		if _, err := r.store.raftSnapshotQueue.Add(r, raftSnapshotPriority); err != nil {
			log.Errorf(ctx, "unable to add replica to Raft repair queue: %s", err)
		}
		return
	}

	r.store.cfg.Transport.mu.Lock()
	var queuedMsgs int64
	for _, queue := range r.store.cfg.Transport.mu.queues {
		queuedMsgs += int64(len(queue))
	}
	r.store.cfg.Transport.mu.Unlock()
	r.store.metrics.RaftEnqueuedPending.Update(queuedMsgs)

	if r.maybeCoalesceHeartbeat(ctx, msg, toReplica, fromReplica, false) {
		return
	}

	if !r.sendRaftMessageRequest(ctx, &RaftMessageRequest{
		RangeID:     r.RangeID,
		ToReplica:   toReplica,
		FromReplica: fromReplica,
		Message:     msg,
	}) {
		if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
			r.mu.droppedMessages++
			raftGroup.ReportUnreachable(msg.To)
			return true, nil
		}); err != nil {
			log.Fatal(ctx, err)
		}
	}
}

// addUnreachableRemoteReplica adds the given remote ReplicaID to be reported
// as unreachable on the next tick.
func (r *Replica) addUnreachableRemoteReplica(remoteReplica roachpb.ReplicaID) {
	r.unreachablesMu.Lock()
	if r.unreachablesMu.remotes == nil {
		r.unreachablesMu.remotes = make(map[roachpb.ReplicaID]struct{})
	}
	r.unreachablesMu.remotes[remoteReplica] = struct{}{}
	r.unreachablesMu.Unlock()
}

// sendRaftMessageRequest sends a raft message, returning false if the message
// was dropped. It is the caller's responsibility to call ReportUnreachable on
// the Raft group.
func (r *Replica) sendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) bool {
	r.store.cfg.Transport.mu.Lock()
	var queuedMsgs int64
	for _, queue := range r.store.cfg.Transport.mu.queues {
		queuedMsgs += int64(len(queue))
	}
	r.store.cfg.Transport.mu.Unlock()
	r.store.metrics.RaftEnqueuedPending.Update(queuedMsgs)

	if log.V(4) {
		log.Infof(ctx, "sending raft request %+v", req)
	}

	return r.store.cfg.Transport.SendAsync(req)
}

func (r *Replica) reportSnapshotStatus(to uint64, snapErr error) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	snapStatus := raft.SnapshotFinish
	if snapErr != nil {
		snapStatus = raft.SnapshotFailure
	}

	if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportSnapshot(to, snapStatus)
		return true, nil
	}); err != nil {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatal(ctx, err)
	}
}

// processRaftCommand processes a raft command by unpacking the
// command struct to get args and reply and then applying the command
// to the state machine via applyRaftCommand(). The result is sent on
// the command's done channel, if available. As a special case, the
// zero idKey signifies an empty Raft command, which will apply as a
// no-op (without accessing raftCmd), updating only the applied index.
//
// This method returns true if the command successfully applied a
// replica change.
//
// TODO(tschottdorf): once we properly check leases and lease requests etc,
// make sure that the error returned from this method is always populated in
// those cases, as one of the callers uses it to abort replica changes.
func (r *Replica) processRaftCommand(
	ctx context.Context, idKey storagebase.CmdIDKey, index uint64, raftCmd storagebase.RaftCommand,
) bool {
	if index == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}

	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", idKey, raftCmd.MaxLeaseIndex)
	}

	var isLeaseRequest bool
	var requestedLease roachpb.Lease
	var ts hlc.Timestamp
	var rSpan roachpb.RSpan
	if raftCmd.ReplicatedEvalResult != nil {
		isLeaseRequest = raftCmd.ReplicatedEvalResult.IsLeaseRequest
		if isLeaseRequest {
			if raftCmd.ReplicatedEvalResult.State.Lease == nil {
				log.Fatalf(ctx, "isLeaseRequest but no lease. eval state: %v", raftCmd.ReplicatedEvalResult.State)
			}
			requestedLease = *raftCmd.ReplicatedEvalResult.State.Lease
		}
		ts = raftCmd.ReplicatedEvalResult.Timestamp
		rSpan = roachpb.RSpan{
			Key:    raftCmd.ReplicatedEvalResult.StartKey,
			EndKey: raftCmd.ReplicatedEvalResult.EndKey,
		}
	} else if idKey != "" {
		isLeaseRequest = raftCmd.BatchRequest.IsLeaseRequest()
		if isLeaseRequest {
			rl, ok := raftCmd.BatchRequest.GetArg(roachpb.RequestLease)
			if !ok {
				log.Fatalf(ctx, "isLeaseRequest but no lease: %s", raftCmd.BatchRequest)
			}
			requestedLease = rl.(*roachpb.RequestLeaseRequest).Lease
		}
		ts = raftCmd.BatchRequest.Timestamp
		var err error
		rSpan, err = keys.Range(*raftCmd.BatchRequest)
		if err != nil {
			// TODO(bdarnell): This should really use forcedErr but I don't
			// want to do that much refactoring for this code path that will
			// be deleted soon.
			log.Fatalf(ctx, "failed to compute range for BatchRequest: %s", err)
		}
	}

	r.mu.Lock()
	proposal, proposedLocally := r.mu.proposals[idKey]

	// Verify the lease matches the proposer's expectation. We rely on
	// the proposer's determination of whether the existing lease is
	// held, and can be used, or is expired, and can be replaced.
	// Verify checks that the lease has not been modified since proposal
	// due to Raft delays / reorderings.
	// To understand why this lease verification is necessary, see comments on the
	// proposer_lease field in the proto.
	//
	// TODO(spencer): remove the special-casing for the pre-epoch range
	// leases.
	verifyLease := func() error {
		// Handle the case of pre-epoch-based-leases command.
		if raftCmd.ProposerLease == nil {
			// Skip verification for lease commands for legacy case.
			if raftCmd.BatchRequest.IsSingleSkipLeaseCheckRequest() {
				return nil
			}
			l, proposer := r.mu.state.Lease, raftCmd.ProposerReplica
			if l.OwnedBy(proposer.StoreID) && ts.Less(l.DeprecatedStartStasis) {
				return nil
			}
			return errors.Errorf("lease %s not held", l)
		}
		return raftCmd.ProposerLease.Equivalent(*r.mu.state.Lease)
	}

	// TODO(tschottdorf): consider the Trace situation here.
	if proposedLocally {
		// We initiated this command, so use the caller-supplied context.
		ctx = proposal.ctx
		proposal.ctx = nil // avoid confusion
		delete(r.mu.proposals, idKey)
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
	} else if err := verifyLease(); err != nil {
		log.VEventf(
			ctx, 1,
			"command %s proposed from replica %+v: %s",
			raftCmd.BatchRequest, raftCmd.ProposerReplica, err,
		)
		if !isLeaseRequest {
			// We return a NotLeaseHolderError so that the DistSender retries.
			nlhe := newNotLeaseHolderError(
				r.mu.state.Lease, raftCmd.ProposerReplica.StoreID, r.mu.state.Desc)
			nlhe.CustomMsg = fmt.Sprintf(
				"stale proposal: command was proposed under lease %s but is being applied "+
					"under lease: %s", raftCmd.ProposerLease, r.mu.state.Lease)
			forcedErr = roachpb.NewError(nlhe)
		} else {
			// For lease requests we return a special error that
			// replica.RedirectOnOrAcquireLease() understands. Note that these
			// requests don't go through the DistSender.
			forcedErr = roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *r.mu.state.Lease,
				Requested: requestedLease,
			})
		}
	} else if isLeaseRequest {
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
		// is otherwise tricky since we can't tell the client to try again
		// (reproposals could exist and may apply at the right index, leading
		// to a replay), and assigning the required index would be tedious
		// seeing that it would have to rewind sometimes.
		leaseIndex = raftCmd.MaxLeaseIndex
	} else {
		// The command is trying to apply at a past log position. That's
		// unfortunate and hopefully rare; the client on the proposer will try
		// again. Note that in this situation, the leaseIndex does not advance.
		forcedErr = roachpb.NewErrorf(
			"command observed at lease index %d, but required < %d", leaseIndex, raftCmd.MaxLeaseIndex,
		)

		if proposedLocally {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required <= %d",
				proposal.idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			// Send to the client only at the end of this invocation. We can't
			// use the context any more once we signal the client, so we make
			// sure we signal it at the end of this method, when the context
			// has been fully used.
			copyProposal := *proposal
			// Clear the endCmds and doneCh so that ProposalData.finish()
			// is a noop when invoked below.
			proposal.endCmds = nil
			proposal.doneCh = make(chan proposalResult, 1)
			defer func() {
				// Assert against another defer trying to use the context after
				// the client has been signaled.
				ctx = nil
				copyProposal.finishRaftApplication(proposalResult{ProposalRetry: proposalIllegalLeaseIndex})
			}()
		}
	}
	r.mu.Unlock()

	if forcedErr == nil {
		forcedErr = roachpb.NewError(r.requestCanProceed(rSpan, ts))
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock := r.maybeAcquireSplitMergeLock(raftCmd); splitMergeUnlock != nil {
			// Close over raftCmd to capture its value at execution time; we clear
			// ReplicatedEvalResult on certain errors.
			defer func() {
				splitMergeUnlock(*raftCmd.ReplicatedEvalResult)
			}()
		}
	}

	var response proposalResult
	var writeBatch *storagebase.WriteBatch
	{
		if raftCmd.ReplicatedEvalResult == nil && forcedErr == nil {
			panic("non-proposer-evaluated command found in raft log. Cannot upgrade directly from versions older than beta-20170413 to beta-20170420 or newer.")
		}

		if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; forcedErr == nil && filter != nil && raftCmd.ReplicatedEvalResult != nil {
			forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: *raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
		}

		if forcedErr != nil {
			// Apply an empty entry.
			raftCmd.ReplicatedEvalResult = &storagebase.ReplicatedEvalResult{}
			raftCmd.WriteBatch = nil
		}
		raftCmd.ReplicatedEvalResult.State.RaftAppliedIndex = index
		raftCmd.ReplicatedEvalResult.State.LeaseAppliedIndex = leaseIndex

		// Update the node clock with the serviced request. This maintains
		// a high water mark for all ops serviced, so that received ops without
		// a timestamp specified are guaranteed one higher than any op already
		// executed for overlapping keys.
		r.store.Clock().Update(ts)

		var pErr *roachpb.Error
		if raftCmd.WriteBatch != nil {
			writeBatch = raftCmd.WriteBatch
		}
		raftCmd.ReplicatedEvalResult.Delta, pErr = r.applyRaftCommand(
			ctx, idKey, *raftCmd.ReplicatedEvalResult, writeBatch)

		if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; pErr == nil && filter != nil {
			pErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                idKey,
				ReplicatedEvalResult: *raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
		}

		pErr = r.maybeSetCorrupt(ctx, pErr)
		if pErr == nil {
			pErr = forcedErr
		}

		var lResult *LocalEvalResult
		if proposedLocally {
			if pErr != nil {
				// A forced error was set (i.e. we did not apply the proposal,
				// for instance due to its log position) or the Replica is now
				// corrupted.
				response.Err = pErr
			} else if proposal.Local.Err != nil {
				// Everything went as expected, but this proposal should return
				// an error to the client.
				response.Err = proposal.Local.Err
			} else if proposal.Local.Reply != nil {
				response.Reply = proposal.Local.Reply
			} else {
				log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", proposal)
			}
			response.Intents = proposal.Local.detachIntents()
			lResult = proposal.Local
		}

		// Handle the EvalResult, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		r.handleEvalResult(ctx, lResult, raftCmd.ReplicatedEvalResult)
	}

	if proposedLocally {
		proposal.finishRaftApplication(response)
	} else if response.Err != nil {
		log.VEventf(ctx, 1, "error executing raft command %s: %s", raftCmd.BatchRequest, response.Err)
	}

	return raftCmd.ReplicatedEvalResult.ChangeReplicas != nil
}

// maybeAcquireSplitMergeLock examines the given raftCmd (which need
// not be evaluated yet) and acquires the split or merge lock if
// necessary (in addition to other preparation). It returns a function
// which will release any lock acquired (or nil) and use the result of
// applying the command to perform any necessary cleanup.
func (r *Replica) maybeAcquireSplitMergeLock(
	raftCmd storagebase.RaftCommand,
) func(storagebase.ReplicatedEvalResult) {
	var split *storagebase.Split
	var merge *storagebase.Merge
	if raftCmd.ReplicatedEvalResult != nil {
		split = raftCmd.ReplicatedEvalResult.Split
		merge = raftCmd.ReplicatedEvalResult.Merge
	} else {
		if union, ok := raftCmd.BatchRequest.GetArg(roachpb.EndTransaction); ok {
			ict := union.(*roachpb.EndTransactionRequest).InternalCommitTrigger
			if tr := ict.GetSplitTrigger(); tr != nil {
				split = &storagebase.Split{
					SplitTrigger: *tr,
				}
			}
			if tr := ict.GetMergeTrigger(); tr != nil {
				merge = &storagebase.Merge{
					MergeTrigger: *tr,
				}
			}

		}
	}

	if split != nil {
		return r.acquireSplitLock(&split.SplitTrigger)
	} else if merge != nil {
		return r.acquireMergeLock(&merge.MergeTrigger)
	}
	return nil
}

func (r *Replica) acquireSplitLock(
	split *roachpb.SplitTrigger,
) func(storagebase.ReplicatedEvalResult) {
	rightRng, created, err := r.store.getOrCreateReplica(split.RightDesc.RangeID, 0, nil)
	if err != nil {
		return nil
	}

	// It would be nice to assert that rightRng is not initialized
	// here. Unfortunately, due to reproposals and retries we might be executing
	// a reproposal for a split trigger that was already executed via a
	// retry. The reproposed command will not succeed (the transaction has
	// already committed).
	//
	// TODO(peter): It might be okay to return an error here, but it is more
	// conservative to hit the exact same error paths that we would hit for other
	// commands that have reproposals interacting with retries (i.e. we don't
	// treat splits differently).

	return func(rResult storagebase.ReplicatedEvalResult) {
		if rResult.Split == nil && created && !rightRng.IsInitialized() {
			// An error occurred during processing of the split and the RHS is still
			// uninitialized. Mark the RHS destroyed and remove it from the replica's
			// map as it is likely detritus. One reason this can occur is when
			// concurrent splits on the same key are executed. Only one of the splits
			// will succeed while the other will allocate a range ID, but fail to
			// commit.
			//
			// We condition this removal on whether the RHS was newly created in
			// order to be conservative. If a Raft message had created the Replica
			// then presumably it was alive for some reason other than a concurrent
			// split and shouldn't be destroyed.
			rightRng.mu.Lock()
			rightRng.mu.destroyed = errors.Errorf("%s: failed to initialize", rightRng)
			rightRng.mu.Unlock()
			r.store.mu.Lock()
			delete(r.store.mu.replicas, rightRng.RangeID)
			delete(r.store.mu.replicaQueues, rightRng.RangeID)
			delete(r.store.mu.uninitReplicas, rightRng.RangeID)
			r.store.mu.Unlock()
		}
		rightRng.raftMu.Unlock()
	}
}

func (r *Replica) acquireMergeLock(
	merge *roachpb.MergeTrigger,
) func(storagebase.ReplicatedEvalResult) {
	rightRng, err := r.store.GetReplica(merge.RightDesc.RangeID)
	if err != nil {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "unable to find merge RHS replica: %s", err)
	}

	// TODO(peter,tschottdorf): This is necessary but likely not sufficient. The
	// right hand side of the merge can still race on reads. See #8630.
	rightRng.raftMu.Lock()
	return func(storagebase.ReplicatedEvalResult) {
		rightRng.raftMu.Unlock()
	}
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine). When the state machine can not
// be updated, an error (which is likely a ReplicaCorruptionError) is returned
// and must be handled by the caller.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rResult storagebase.ReplicatedEvalResult,
	writeBatch *storagebase.WriteBatch,
) (enginepb.MVCCStats, *roachpb.Error) {
	if rResult.State.RaftAppliedIndex <= 0 {
		log.Fatalf(ctx, "raft command index is <= 0")
	}

	r.mu.Lock()
	oldRaftAppliedIndex := r.mu.state.RaftAppliedIndex
	oldLeaseAppliedIndex := r.mu.state.LeaseAppliedIndex
	ms := r.mu.state.Stats
	r.mu.Unlock()

	if rResult.State.RaftAppliedIndex != oldRaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return enginepb.MVCCStats{}, roachpb.NewError(NewReplicaCorruptionError(
			errors.Errorf("applied index jumped from %d to %d",
				oldRaftAppliedIndex, rResult.State.RaftAppliedIndex)))
	}

	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return enginepb.MVCCStats{}, roachpb.NewError(NewReplicaCorruptionError(
				errors.Wrap(err, "unable to apply WriteBatch")))
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch. Currently the only
	// remaining writes are the raft applied index and the updated MVCC stats.
	writer := batch.Distinct()

	// Advance the last applied index. We use a blind write in order to avoid
	// reading the previous applied index keys on every write operation. This
	// requires a little additional work in order maintain the MVCC stats.
	var appliedIndexNewMS enginepb.MVCCStats
	if err := r.stateLoader.setAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
		rResult.State.RaftAppliedIndex, rResult.State.LeaseAppliedIndex); err != nil {
		return enginepb.MVCCStats{}, roachpb.NewError(NewReplicaCorruptionError(
			errors.Wrap(err, "unable to set applied index")))
	}
	rResult.Delta.SysBytes += appliedIndexNewMS.SysBytes -
		r.stateLoader.calcAppliedIndexSysBytes(oldRaftAppliedIndex, oldLeaseAppliedIndex)

	// Special-cased MVCC stats handling to exploit commutativity of stats
	// delta upgrades. Thanks to commutativity, the command queue does not
	// have to serialize on the stats key.
	ms.Add(rResult.Delta)
	if err := r.stateLoader.setMVCCStats(ctx, writer, &ms); err != nil {
		return enginepb.MVCCStats{}, roachpb.NewError(NewReplicaCorruptionError(
			errors.Wrap(err, "unable to update MVCCStats")))
	}

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	start := timeutil.Now()
	if err := batch.Commit(false); err != nil {
		return enginepb.MVCCStats{}, roachpb.NewError(NewReplicaCorruptionError(
			errors.Wrap(err, "could not commit batch")))
	}
	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	return rResult.Delta, nil
}

// evaluateProposalInner executes the command in a batch engine and returns
// the batch containing the results. If the return value contains a non-nil
// WriteBatch, the caller should go ahead with the proposal (eventually
// committing the data contained in the batch), even when the Err field is set
// (which is then the result sent to the client).
//
// TODO(tschottdorf): the setting of WriteTooOld does not work. With
// proposer-evaluated KV, TestStoreResolveWriteIntentPushOnRead fails in the
// SNAPSHOT case since the transactional write in that test *always* catches
// a WriteTooOldError. With proposer-evaluated KV disabled the same happens,
// but the resulting WriteTooOld flag on the transaction is lost, letting the
// test pass erroneously.
//
// TODO(bdarnell): merge evaluateProposal and evaluateProposalInner. There
// is no longer a clear distinction between them.
func (r *Replica) evaluateProposalInner(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *SpanSet,
) EvalResult {
	// Keep track of original txn Writing state to sanitize txn
	// reported with any error except TransactionRetryError.
	wasWriting := ba.Txn != nil && ba.Txn.Writing

	// Evaluate the commands. If this returns without an error, the batch should
	// be committed.
	var result EvalResult
	var batch engine.Batch
	{
		// TODO(tschottdorf): absorb all returned values in `pd` below this point
		// in the call stack as well.
		var pErr *roachpb.Error
		var ms enginepb.MVCCStats
		var br *roachpb.BatchResponse
		batch, ms, br, result, pErr = r.evaluateTxnWriteBatch(ctx, idKey, ba, spans)
		result.Replicated.Delta = ms
		result.Local.Reply = br
		result.Local.Err = pErr
		if batch == nil {
			return result
		}
	}

	if result.Local.Err != nil && ba.IsWrite() {
		if _, ok := result.Local.Err.GetDetail().(*roachpb.TransactionRetryError); !ok {
			// TODO(tschottdorf): make `nil` acceptable. Corresponds to
			// roachpb.Response{With->Or}Error.
			result.Local.Reply = &roachpb.BatchResponse{}
			// Reset the batch to clear out partial execution. Don't set
			// a WriteBatch to signal to the caller that we fail-fast this
			// proposal.
			batch.Close()
			batch = nil
			// Restore the original txn's Writing bool if pd.Err specifies
			// a transaction.
			if txn := result.Local.Err.GetTxn(); txn != nil && txn.Equal(ba.Txn) {
				txn.Writing = wasWriting
			}
			return result
		}
		// If the batch failed with a TransactionRetryError, any preceding
		// mutations in the batch engine should still be applied so that
		// intents are laid down in preparation for the retry. However,
		// no reply is sent back.
		result.Local.Reply = nil
	}

	result.WriteBatch = &storagebase.WriteBatch{
		Data: batch.Repr(),
	}
	// TODO(tschottdorf): could keep this open and commit as the proposal
	// applies, saving work on the proposer. Take care to discard batches
	// properly whenever the command leaves `r.mu.proposals` without coming
	// back.
	batch.Close()
	return result
}

// checkIfTxnAborted checks the txn abort cache for the given
// transaction. In case the transaction has been aborted, return a
// transaction abort error.
func checkIfTxnAborted(
	ctx context.Context, rec ReplicaEvalContext, b engine.Reader, txn roachpb.Transaction,
) *roachpb.Error {
	var entry roachpb.AbortCacheEntry
	aborted, err := rec.AbortCache().Get(ctx, b, *txn.ID, &entry)
	if err != nil {
		return roachpb.NewError(NewReplicaCorruptionError(errors.Wrap(err, "could not read from abort cache")))
	}
	if aborted {
		// We hit the cache, so let the transaction restart.
		if log.V(1) {
			log.Infof(ctx, "found abort cache entry for %s with priority %d",
				txn.ID.Short(), entry.Priority)
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

// evaluateTxnWriteBatch attempts to execute transactional batches on
// the 1-phase-commit path as just an atomic, non-transactional batch
// of write commands. One phase commit batches contain transactional
// writes sandwiched by BeginTransaction and EndTransaction requests.
//
// If the batch is transactional, and there's nothing to suggest that
// the transaction will require retry or restart, the batch's txn is
// stripped and it's executed as a normal batch write. If the writes
// cannot all be completed at the intended timestamp, the batch's txn
// is restored and it's re-executed as transactional. This allows it
// to lay down intents and return an appropriate retryable error.
func (r *Replica) evaluateTxnWriteBatch(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *SpanSet,
) (engine.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, EvalResult, *roachpb.Error) {
	ms := enginepb.MVCCStats{}
	// If not transactional or there are indications that the batch's txn will
	// require restart or retry, execute as normal.
	if !r.store.TestingKnobs().DisableOnePhaseCommits && isOnePhaseCommit(ba) {
		arg, _ := ba.GetArg(roachpb.EndTransaction)
		etArg := arg.(*roachpb.EndTransactionRequest)

		// Try executing with transaction stripped. We use the transaction timestamp
		// to write any values as it may have been advanced by the timestamp cache.
		strippedBa := ba
		strippedBa.Timestamp = strippedBa.Txn.Timestamp
		strippedBa.Txn = nil
		strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs

		// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
		batch := r.store.Engine().NewBatch()
		if raceEnabled && spans != nil {
			batch = makeSpanSetBatch(batch, spans)
		}
		rec := ReplicaEvalContext{r, spans}
		br, result, pErr := evaluateBatch(ctx, idKey, batch, rec, &ms, strippedBa)
		if pErr == nil && ba.Timestamp == br.Timestamp {
			clonedTxn := ba.Txn.Clone()
			clonedTxn.Writing = true
			clonedTxn.Status = roachpb.COMMITTED

			// If the end transaction is not committed, clear the batch and mark the status aborted.
			if !etArg.Commit {
				clonedTxn.Status = roachpb.ABORTED
				batch.Close()
				batch = r.store.Engine().NewBatch()
				ms = enginepb.MVCCStats{}
			} else {
				// Run commit trigger manually.
				innerResult, err := runCommitTrigger(ctx, rec, batch, &ms, *etArg, &clonedTxn)
				if err != nil {
					return batch, ms, br, result, roachpb.NewErrorf("failed to run commit trigger: %s", err)
				}
				if err := result.MergeAndDestroy(innerResult); err != nil {
					return batch, ms, br, result, roachpb.NewError(err)
				}
			}

			br.Txn = &clonedTxn
			// Add placeholder responses for begin & end transaction requests.
			br.Responses = append([]roachpb.ResponseUnion{{BeginTransaction: &roachpb.BeginTransactionResponse{}}}, br.Responses...)
			br.Responses = append(br.Responses, roachpb.ResponseUnion{EndTransaction: &roachpb.EndTransactionResponse{OnePhaseCommit: true}})
			return batch, ms, br, result, nil
		}

		batch.Close()
		ms = enginepb.MVCCStats{}

		// Handle the case of a required one phase commit transaction.
		if etArg.Require1PC {
			if pErr != nil {
				return nil, ms, nil, EvalResult{}, pErr
			} else if ba.Timestamp != br.Timestamp {
				err := roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN)
				return nil, ms, nil, EvalResult{}, roachpb.NewError(err)
			}
			panic("unreachable")
		}

		log.VEventf(ctx, 2, "1PC execution failed, reverting to regular execution for batch")
	}

	batch := r.store.Engine().NewBatch()
	if raceEnabled && spans != nil {
		batch = makeSpanSetBatch(batch, spans)
	}
	rec := ReplicaEvalContext{r, spans}
	br, result, pErr := evaluateBatch(ctx, idKey, batch, rec, &ms, ba)
	return batch, ms, br, result, pErr
}

// isOnePhaseCommit returns true iff the BatchRequest contains all
// commands in the transaction, starting with BeginTransaction and
// ending with EndTransaction. One phase commits are disallowed if (1) the
// transaction has already been flagged with a write too old error or
// (2) if isolation is serializable and the commit timestamp has been
// forwarded, or (3) the transaction exceeded its deadline.
func isOnePhaseCommit(ba roachpb.BatchRequest) bool {
	if ba.Txn == nil {
		return false
	}
	if retry, _ := isEndTransactionTriggeringRetryError(ba.Txn, ba.Txn); retry {
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
// The caller should use the returned slice (which is either equal to
// the input slice, or has been shallow-copied appropriately to avoid
// mutating the original requests).
func optimizePuts(
	batch engine.ReadWriter, origReqs []roachpb.RequestUnion, distinctSpans bool,
) []roachpb.RequestUnion {
	var minKey, maxKey roachpb.Key
	var unique map[string]struct{}
	if !distinctSpans {
		unique = make(map[string]struct{}, len(origReqs))
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

	firstUnoptimizedIndex := len(origReqs)
	for i, r := range origReqs {
		switch t := r.GetInner().(type) {
		case *roachpb.PutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.ConditionalPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		case *roachpb.InitPutRequest:
			if maybeAddPut(t.Key) {
				continue
			}
		}
		firstUnoptimizedIndex = i
		break
	}

	if firstUnoptimizedIndex < optimizePutThreshold { // don't bother if below this threshold
		return origReqs
	}
	iter := batch.NewIterator(false /* total order iterator */)
	defer iter.Close()

	// If there are enough puts in the run to justify calling seek,
	// we can determine whether any part of the range being written
	// is "virgin" and set the puts to write blindly.
	// Find the first non-empty key in the run.
	iter.Seek(engine.MakeMVCCMetadataKey(minKey))
	var iterKey roachpb.Key
	if ok, err := iter.Valid(); err != nil {
		// TODO(bdarnell): return an error here instead of silently
		// running without the optimization?
		log.Errorf(context.TODO(), "Seek returned error; disabling blind-put optimization: %s", err)
		return origReqs
	} else if ok && bytes.Compare(iter.Key().Key, maxKey) <= 0 {
		iterKey = iter.Key().Key
	}
	// Set the prefix of the run which is being written to virgin
	// keyspace to "blindly" put values.
	reqs := append([]roachpb.RequestUnion(nil), origReqs...)
	for i := range reqs[:firstUnoptimizedIndex] {
		inner := reqs[i].GetInner()
		if iterKey == nil || bytes.Compare(iterKey, inner.Header().Key) > 0 {
			switch t := inner.(type) {
			case *roachpb.PutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.ConditionalPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			case *roachpb.InitPutRequest:
				shallow := *t
				shallow.Blind = true
				reqs[i].MustSetInner(&shallow)
			default:
				log.Fatalf(context.TODO(), "unexpected non-put request: %s", t)
			}
		}
	}
	return reqs
}

// evaluateBatch evaluates a batch request by splitting it up into its
// individual commands, passing them to evaluateCommand, and combining
// the results.
func evaluateBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	batch engine.ReadWriter,
	rec ReplicaEvalContext,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, EvalResult, *roachpb.Error) {
	br := ba.CreateReply()

	maxKeys := int64(math.MaxInt64)
	if ba.Header.MaxSpanRequestKeys != 0 {
		// We have a batch of requests with a limit. We keep track of how many
		// remaining keys we can touch.
		maxKeys = ba.Header.MaxSpanRequestKeys
	}

	// Optimize any contiguous sequences of put and conditional put ops.
	if len(ba.Requests) >= optimizePutThreshold {
		ba.Requests = optimizePuts(batch, ba.Requests, ba.Header.DistinctSpans)
	}

	// Create a shallow clone of the transaction. We only modify a few
	// non-pointer fields (BatchIndex, WriteTooOld, Timestamp), so this saves
	// a few allocs.
	if ba.Txn != nil {
		txnShallow := *ba.Txn
		ba.Txn = &txnShallow

		// Check whether this transaction has been aborted, if applicable.
		// This applies to writes that leave intents (the use of the
		// IsTransactionWrite flag excludes operations like HeartbeatTxn),
		// and reads that occur in a transaction that has already written
		// (see #2231 for more about why we check for aborted transactions
		// on reads). Note that 1PC transactions have had their
		// transaction field cleared by this point so we do not execute
		// this check in that case.
		if ba.IsTransactionWrite() || ba.Txn.Writing {
			if pErr := checkIfTxnAborted(ctx, rec, batch, *ba.Txn); pErr != nil {
				return nil, EvalResult{}, pErr
			}
		}
	}

	var result EvalResult
	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()
		if ba.Txn != nil {
			ba.Txn.BatchIndex = int32(index)
		}
		// Note that responses are populated even when an error is returned.
		// TODO(tschottdorf): Change that. IIRC there is nontrivial use of it currently.
		reply := br.Responses[index].GetInner()
		curResult, pErr := evaluateCommand(ctx, idKey, index, batch, rec, ms, ba.Header, maxKeys, args, reply)

		if err := result.MergeAndDestroy(curResult); err != nil {
			// TODO(tschottdorf): see whether we really need to pass nontrivial
			// EvalResult up on error and if so, formalize that.
			log.Fatalf(
				ctx,
				"unable to absorb EvalResult: %s\ndiff(new, old): %s",
				err, pretty.Diff(curResult, result),
			)
		}

		if pErr != nil {
			switch tErr := pErr.GetDetail().(type) {
			case *roachpb.WriteTooOldError:
				// We got a WriteTooOldError. In case this is a transactional request,
				// we want to run the other commands in the batch and let them lay
				// intents so that other concurrent overlapping transactions are forced
				// through intent resolution and the chances of this batch succeeding
				// when it will be retried are increased.

				if ba.Txn == nil {
					return nil, result, pErr
				}
				// On WriteTooOldError, we've written a new value or an intent
				// at a too-high timestamp and we must forward the batch txn or
				// timestamp as appropriate so that it's returned.
				ba.Txn.Timestamp.Forward(tErr.ActualTimestamp)
				ba.Txn.WriteTooOld = true
				// Clear the WriteTooOldError; we're done processing it by having
				// moved the batch or txn timestamps forward and set WriteTooOld
				// if this is a transactional write. The EndTransaction will detect this
				// pushed timestamp and return a TransactionRetryError.
				pErr = nil
			default:
				// Initialize the error index.
				pErr.SetErrorIndex(int32(index))
				return nil, result, pErr
			}
		}

		if maxKeys != math.MaxInt64 {
			retResults := reply.Header().NumKeys
			if retResults > maxKeys {
				log.Fatalf(ctx, "received %d results, limit was %d", retResults, maxKeys)
			}
			maxKeys -= retResults
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

	return br, result, nil
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
	if err := r.store.Stopper().RunTask(ctx, func(ctx context.Context) {
		// Check for or obtain the lease, if none active.
		_, pErr = r.redirectOnOrAcquireLease(ctx)
		hasLease = pErr == nil
		if pErr != nil {
			switch e := pErr.GetDetail().(type) {
			case *roachpb.NotLeaseHolderError:
				// NotLeaseHolderError means there is an active lease, but only if
				// the lease holder is set; otherwise, it's likely a timeout.
				if e.LeaseHolder != nil {
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
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if uuidBytes, err := r.store.Gossip().GetInfo(gossip.KeyClusterID); err == nil {
		if gossipClusterID, err := uuid.FromBytes(uuidBytes); err == nil {
			if gossipClusterID != r.store.ClusterID() {
				log.Fatalf(
					ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
					r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
			}
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster id %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(
		gossip.KeyClusterID, r.store.ClusterID().GetBytes(), 0*time.Second,
	); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %s", err)
	}

	if r.store.cfg.TestingKnobs.DisablePeriodicGossips {
		return nil
	}

	hasLease, pErr := r.getLeaseForGossip(ctx)
	if pErr != nil {
		return pErr
	} else if !hasLease {
		return nil
	}
	r.gossipFirstRange(ctx)
	return nil
}

func (r *Replica) gossipFirstRange(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Gossip is not provided for the bootstrap store and for some tests.
	if r.store.Gossip() == nil {
		return
	}
	log.Event(ctx, "gossiping sentinel and first range")
	if log.V(1) {
		log.Infof(ctx, "gossiping sentinel from store %d, r%d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(gossip.KeySentinel, r.store.ClusterID().GetBytes(), sentinelGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip sentinel: %s", err)
	}
	if log.V(1) {
		log.Infof(ctx, "gossiping first range from store %d, r%d: %s",
			r.store.StoreID(), r.RangeID, r.mu.state.Desc.Replicas)
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.mu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %s", err)
	}
}

// shouldGossip returns true if this replica should be gossiping. Gossip is
// inherently inconsistent and asynchronous, we're using the lease as a way to
// ensure that only one node gossips at a time.
func (r *Replica) shouldGossip() bool {
	return r.ownsValidLease(r.store.Clock().Now())
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
func (r *Replica) maybeGossipSystemConfig(ctx context.Context) error {
	if r.store.Gossip() == nil {
		log.VEventf(ctx, 2, "not gossiping system config because gossip isn't initialized")
		return nil
	}
	if !r.IsInitialized() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica isn't initialized")
		return nil
	}
	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		log.VEventf(ctx, 2,
			"not gossiping system config because the replica doesn't contain the system config's start key")
		return nil
	}
	if !r.shouldGossip() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica doesn't hold the lease")
		return nil
	}

	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	loadedCfg, err := r.loadSystemConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "could not load SystemConfig span")
	}

	if gossipedCfg, ok := r.store.Gossip().GetSystemConfig(); ok && gossipedCfg.Equal(loadedCfg) {
		log.VEventf(ctx, 2, "not gossiping unchanged system config")
		return nil
	}

	log.VEventf(ctx, 2, "gossiping system config")
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, &loadedCfg, 0); err != nil {
		return errors.Wrap(err, "failed to gossip system config")
	}
	return nil
}

// maybeGossipNodeLiveness gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica
// must hold the lease to a range which contains some or all of the
// node liveness records. After scanning the records, it checks
// against what's already in gossip and only gossips records which
// are out of date.
func (r *Replica) maybeGossipNodeLiveness(ctx context.Context, span roachpb.Span) error {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return nil
	}

	if !r.ContainsKeyRange(span.Key, span.EndKey) || !r.shouldGossip() {
		return nil
	}

	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: span})
	// Call evaluateBatch instead of Send to avoid command queue reentrance.
	rec := ReplicaEvalContext{r, nil}
	br, result, pErr :=
		evaluateBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), rec, nil, ba)
	if pErr != nil {
		return errors.Wrapf(pErr.GoError(), "couldn't scan node liveness records in span %s", span)
	}
	if result.Local.intents != nil && len(*result.Local.intents) > 0 {
		return errors.Errorf("unexpected intents on node liveness span %s: %+v", span, *result.Local.intents)
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	log.VEventf(ctx, 2, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	for _, kv := range kvs {
		var liveness, exLiveness Liveness
		if err := kv.Value.GetProto(&liveness); err != nil {
			return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
		}
		key := gossip.MakeNodeLivenessKey(liveness.NodeID)
		// Look up liveness from gossip; skip gossiping anew if unchanged.
		if err := r.store.Gossip().GetInfoProto(key, &exLiveness); err == nil {
			if exLiveness == liveness {
				continue
			}
		}
		if err := r.store.Gossip().AddInfoProto(key, &liveness, 0); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness (%+v)", liveness)
		}
	}
	return nil
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

		log.Errorf(ctx, "stalling replica due to: %s", cErr.ErrorMsg)
		cErr.Processed = true
		r.mu.destroyed = cErr
		r.mu.corrupted = true
		pErr = roachpb.NewError(cErr)

		// Try to persist the destroyed error message. If the underlying store is
		// corrupted the error won't be processed and a panic will occur.
		if err := r.stateLoader.setReplicaDestroyedError(ctx, r.store.Engine(), pErr); err != nil {
			cErr.Processed = false
			return roachpb.NewError(cErr)
		}
	}
	return pErr
}

var errSystemConfigIntent = errors.New("must retry later due to intent on SystemConfigSpan")

// loadSystemConfig scans the system config span and returns the system
// config.
func (r *Replica) loadSystemConfig(ctx context.Context) (config.SystemConfig, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: keys.SystemConfigSpan})
	// Call evaluateBatch instead of Send to avoid command queue reentrance.
	rec := ReplicaEvalContext{r, nil}
	br, result, pErr := evaluateBatch(
		ctx, storagebase.CmdIDKey(""), r.store.Engine(), rec, nil, ba,
	)
	if pErr != nil {
		return config.SystemConfig{}, pErr.GoError()
	}
	if intents := result.Local.detachIntents(); len(intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		r.store.intentResolver.processIntentsAsync(r, intents)
		return config.SystemConfig{}, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	return config.SystemConfig{Values: kvs}, nil
}

// needsSplitBySize returns true if the size of the range requires it
// to be split.
func (r *Replica) needsSplitBySize() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.needsSplitBySizeRLocked()
}

func (r *Replica) needsSplitBySizeRLocked() bool {
	maxBytes := r.mu.maxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && size > maxBytes
}

func (r *Replica) exceedsDoubleSplitSizeRLocked() bool {
	maxBytes := r.mu.maxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && size > maxBytes*2
}

func (r *Replica) setPendingSnapshotIndex(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We allow the pendingSnapshotIndex to change from 0 to 1 and then from 1 to
	// a value greater than 1. Any other change indicates 2 current preemptive
	// snapshots on the same replica which is disallowed.
	if (index == 1 && r.mu.pendingSnapshotIndex != 0) ||
		(index > 1 && r.mu.pendingSnapshotIndex != 1) {
		return errors.Errorf("%s: pending snapshot already present: %d", r, r.mu.pendingSnapshotIndex)
	}
	r.mu.pendingSnapshotIndex = index
	return nil
}

func (r *Replica) clearPendingSnapshotIndex() {
	r.mu.Lock()
	r.mu.pendingSnapshotIndex = 0
	r.mu.Unlock()
}

func (r *Replica) endKey() roachpb.RKey {
	return r.Desc().EndKey
}

// Less implements the btree.Item interface.
func (r *Replica) Less(i btree.Item) bool {
	return r.endKey().Less(i.(rangeKeyItem).endKey())
}

// ReplicaMetrics contains details on the current status of the replica.
type ReplicaMetrics struct {
	Leader      bool
	LeaseValid  bool
	Leaseholder bool
	LeaseType   roachpb.LeaseType
	Quiescent   bool
	// Is this the replica which collects per-range metrics? This is done either
	// on the leader or, if there is no leader, on the largest live replica ID.
	RangeCounter    bool
	Unavailable     bool
	Underreplicated bool
	BehindCount     int64
	SelfBehindCount int64
}

// Metrics returns the current metrics for the replica.
func (r *Replica) Metrics(
	ctx context.Context,
	now hlc.Timestamp,
	cfg config.SystemConfig,
	livenessMap map[roachpb.NodeID]bool,
) ReplicaMetrics {
	r.mu.RLock()
	raftStatus := r.raftStatusRLocked()
	status := r.leaseStatus(r.mu.state.Lease, now, r.mu.minLeaseProposedTS)
	quiescent := r.mu.quiescent || r.mu.internalRaftGroup == nil
	desc := r.mu.state.Desc
	selfBehindCount := r.getEstimatedBehindCountRLocked(raftStatus)
	r.mu.RUnlock()

	return calcReplicaMetrics(ctx, now, cfg, livenessMap, desc,
		raftStatus, status, r.store.StoreID(), quiescent, selfBehindCount)
}

func isRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.RaftState == raft.StateLeader
}

func hasRaftLeader(raftStatus *raft.Status) bool {
	return raftStatus != nil && raftStatus.SoftState.Lead != 0
}

func calcReplicaMetrics(
	ctx context.Context,
	now hlc.Timestamp,
	cfg config.SystemConfig,
	livenessMap map[roachpb.NodeID]bool,
	desc *roachpb.RangeDescriptor,
	raftStatus *raft.Status,
	status LeaseStatus,
	storeID roachpb.StoreID,
	quiescent bool,
	selfBehindCount int64,
) ReplicaMetrics {
	var m ReplicaMetrics

	var leaseOwner bool
	if status.state == leaseValid {
		m.LeaseValid = true
		leaseOwner = status.lease.OwnedBy(storeID)
		m.LeaseType = status.lease.Type()
	}
	m.Leaseholder = m.LeaseValid && leaseOwner
	m.Leader = isRaftLeader(raftStatus)
	m.Quiescent = quiescent
	if !m.Leader {
		m.SelfBehindCount = selfBehindCount
	}

	// We gather per-range stats on either the leader or, if there is no leader,
	// the first live replica in the descriptor. Note that the first live replica
	// is an arbitrary choice. We want to select one live replica to do the
	// counting that all replicas can agree on.
	//
	// Note that the current heuristics can double count. If the first live
	// replica is on a node that is partitioned from the other replicas in the
	// range it may not know the leader even though there is one. This scenario
	// seems rare as it requires the partitioned node to be alive enough to be
	// performing liveness heartbeats.
	if !hasRaftLeader(raftStatus) {
		// The range doesn't have a leader or we don't know who the leader is.
		for _, rd := range desc.Replicas {
			if livenessMap[rd.NodeID] {
				m.RangeCounter = rd.StoreID == storeID
				break
			}
		}
	} else {
		m.RangeCounter = m.Leader
	}

	if m.RangeCounter {
		var goodReplicas int
		goodReplicas, m.BehindCount = calcGoodReplicas(raftStatus, desc, livenessMap)
		if goodReplicas < computeQuorum(len(desc.Replicas)) {
			m.Unavailable = true
		}
		if zoneConfig, err := cfg.GetZoneConfigForKey(desc.StartKey); err != nil {
			log.Error(ctx, err)
		} else if int32(goodReplicas) < zoneConfig.NumReplicas {
			m.Underreplicated = true
		}
	}

	return m
}

// calcGoodReplicas returns a count of the "good" replicas and a count of the
// number of log entries the replicas are behind. The log entry count is only
// returned if the local replica is the leader. A "good" replica must be on a
// live node and, if there is a leader, not too far behind.
func calcGoodReplicas(
	raftStatus *raft.Status, desc *roachpb.RangeDescriptor, livenessMap map[roachpb.NodeID]bool,
) (int, int64) {
	leader := isRaftLeader(raftStatus)
	var goodReplicas int
	var behindCount int64
	for _, rd := range desc.Replicas {
		live := livenessMap[rd.NodeID]
		if !leader {
			if live {
				goodReplicas++
			}
			continue
		}

		if progress, ok := raftStatus.Progress[uint64(rd.ReplicaID)]; ok {
			if live {
				// A single range can process thousands of ops/sec, so a replica that
				// is 10 Raft log entries behind is fairly current. Setting this value
				// to 0 makes the under-replicated metric overly sensitive. Setting
				// this value too high makes the metric not show all of the
				// under-replicated replicas.
				const behindThreshold = 10
				// TODO(peter): progress.Match will be 0 if this node recently
				// became the leader. Presume such replicas are up to date until we
				// hear otherwise. This is a bit of a hack.
				if progress.Match == 0 ||
					progress.Match+behindThreshold >= raftStatus.Commit {
					goodReplicas++
				}
			}
			if progress.Match > 0 &&
				progress.Match < raftStatus.Commit {
				behindCount += int64(raftStatus.Commit) - int64(progress.Match)
			}
		}
	}
	return goodReplicas, behindCount
}

// GetTempPrefix proxies Store.GetTempPrefix.
func (r *Replica) GetTempPrefix() string {
	return r.store.GetTempPrefix()
}

// GetLeaseHistory returns the lease history stored on this replica.
func (r *Replica) GetLeaseHistory() []roachpb.Lease {
	if r.leaseHistory == nil {
		return nil
	}
	return r.leaseHistory.get()
}

// EnableLeaseHistory turns on the lease history for testing purposes. Returns
// a function to return it to its original state that can be deferred.
func EnableLeaseHistory(maxEntries int) func() {
	originalValue := leaseHistoryMaxEntries
	leaseHistoryMaxEntries = maxEntries
	return func() {
		leaseHistoryMaxEntries = originalValue
	}
}
