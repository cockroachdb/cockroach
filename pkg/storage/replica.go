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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	defaultReplicaRaftMuWarnThreshold = 500 * time.Millisecond
	defaultReplicaMuWarnThreshold     = 500 * time.Millisecond
)

// This flag controls whether Transaction entries are automatically gc'ed
// upon EndTransaction if they only have local intents (which can be
// resolved synchronously with EndTransaction). Certain tests become
// simpler with this being turned off.
var txnAutoGC = true

var tickQuiesced = envutil.EnvOrDefaultBool("COCKROACH_TICK_QUIESCED", true)

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

// proposalResult indicates the result of a proposal with the following semantics:
// - If ShouldRetry is set, the proposal applied at a Lease index it was not
//   legal for. The command should be retried.
// - Otherwise, exactly one of the BatchResponse or the Error are set and
//   represent the result of the proposal.
type proposalResult struct {
	Reply       *roachpb.BatchResponse
	Err         *roachpb.Error
	ShouldRetry bool
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
		keys.PrettyPrintRange(&buf, roachpb.Key(desc.StartKey), roachpb.Key(desc.EndKey), maxRangeChars)
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

	store *Store
	// sha1 hash of the system config @ last gossip. No synchronized access;
	// must only be accessed from maybeGossipSystemConfig (which in turn is
	// only called from the Raft-processing goroutine).
	systemDBHash []byte
	abortCache   *AbortCache // Avoids anomalous reads after abort

	// creatingReplica is set when a replica is created as uninitialized
	// via a raft message.
	creatingReplica *roachpb.ReplicaDescriptor

	// Held in read mode during read-only commands. Held in exclusive mode to
	// prevent read-only commands from executing. Acquired before the embedded
	// RWMutex.
	readOnlyCmdMu syncutil.RWMutex

	// rangeStr is a string representation of a RangeDescriptor that cam be
	// atomically read and updated without needing to acquire the replica.mu lock.
	// All updates to state.Desc should be duplicated here.
	rangeStr atomicDescString

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	//
	// TODO(peter): evaluate runtime overhead the timed mutex.
	raftMu syncutil.TimedMutex

	cmdQMu struct {
		// Protects all fields in the cmdQMu struct.
		//
		// Locking notes: Replica.mu < Replica.cmdQMu
		syncutil.TimedMutex
		// Enforces at most one command is running per key(s). The global
		// component tracks user writes (i.e. all keys for which keys.Addr is
		// the identity), the local component the rest (e.g. RangeDescriptor,
		// transaction record, Lease, ...).
		global, local *CommandQueue
	}

	mu struct {
		// Protects all fields in the mu struct.
		//
		// TODO(peter): evaluate runtime overhead the timed mutex.
		syncutil.TimedMutex
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

		// Most recent timestamps for keys / key ranges.
		tsCache *timestampCache
		// submitProposalFn can be set to mock out the propose operation.
		submitProposalFn func(*ProposalData) error
		// Computed checksum at a snapshot UUID.
		checksums map[uuid.UUID]replicaChecksum

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int

		// When closed, indicates that this replica has finished sending
		// an outgoing snapshot. Nothing is sent on this channel.
		outSnapDone chan struct{}

		// The pending outgoing snapshot if there is one.
		outSnap OutgoingSnapshot
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
// varies. The shouldCampaign argument indicates whether a new raft group
// should be campaigned upon creation and is used to eagerly campaign idle
// replicas.
//
// Requires that both Replica.mu and Replica.raftMu are held.
func (r *Replica) withRaftGroupLocked(
	shouldCampaign bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
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

	if shouldCampaign {
		// Special handling of idle replicas: we campaign their Raft group upon
		// creation if we gossiped our store descriptor more than the election
		// timeout in the past.
		shouldCampaign = (r.mu.internalRaftGroup == nil) && r.store.canCampaignIdleReplica()
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

		if !shouldCampaign {
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
			shouldCampaign = r.isSoloReplicaLocked()
		}
		if shouldCampaign {
			if log.V(3) {
				log.Infof(ctx, "campaigning")
			}
			if err := raftGroup.Campaign(); err != nil {
				return err
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

var initialOutSnapDone = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

func newReplica(rangeID roachpb.RangeID, store *Store) *Replica {
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		store:          store,
		abortCache:     NewAbortCache(rangeID),
	}

	// Init rangeStr with the range ID.
	r.rangeStr.store(0, &roachpb.RangeDescriptor{RangeID: rangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)

	raftMuLogger := syncutil.ThresholdLogger(
		r.AnnotateCtx(context.Background()),
		defaultReplicaRaftMuWarnThreshold,
		func(ctx context.Context, msg string, args ...interface{}) {
			log.Warningf(ctx, "raftMu: "+msg, args...)
		},
		func(t time.Duration) {
			r.store.metrics.MuRaftNanos.RecordValue(t.Nanoseconds())
		},
	)
	r.raftMu = syncutil.MakeTimedMutex(raftMuLogger)

	replicaMuLogger := syncutil.ThresholdLogger(
		r.AnnotateCtx(context.Background()),
		defaultReplicaMuWarnThreshold,
		func(ctx context.Context, msg string, args ...interface{}) {
			log.Warningf(ctx, "replicaMu: "+msg, args...)
		},
		func(t time.Duration) {
			r.store.metrics.MuReplicaNanos.RecordValue(t.Nanoseconds())
		},
	)
	r.mu.TimedMutex = syncutil.MakeTimedMutex(replicaMuLogger)
	r.mu.outSnapDone = initialOutSnapDone

	cmdQMuLogger := syncutil.ThresholdLogger(
		r.AnnotateCtx(context.Background()),
		defaultReplicaMuWarnThreshold,
		func(ctx context.Context, msg string, args ...interface{}) {
			log.Warningf(ctx, "cmdQMu: "+msg, args...)
		},
		func(t time.Duration) {
			r.store.metrics.MuCommandQueueNanos.RecordValue(t.Nanoseconds())
		},
	)
	r.cmdQMu.TimedMutex = syncutil.MakeTimedMutex(cmdQMuLogger)
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

	if err := r.init(desc, store.Clock(), replicaID); err != nil {
		return nil, err
	}

	r.maybeGossipSystemConfig()
	r.maybeGossipNodeLiveness(keys.NodeLivenessSpan)
	return r, nil
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
	if r.mu.state.Desc != nil && r.isInitializedLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	}
	if desc.IsInitialized() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}

	r.cmdQMu.Lock()
	r.cmdQMu.global = NewCommandQueue(true /* optimizeOverlap */)
	r.cmdQMu.local = NewCommandQueue(false /* !optimizeOverlap */)
	r.cmdQMu.Unlock()

	r.mu.tsCache = newTimestampCache(clock)
	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]replicaChecksum{}
	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	var err error

	if r.mu.state, err = loadState(ctx, r.store.Engine(), desc); err != nil {
		return err
	}
	r.rangeStr.store(0, r.mu.state.Desc)

	r.mu.lastIndex, err = loadLastIndex(ctx, r.store.Engine(), r.RangeID)
	if err != nil {
		return err
	}

	pErr, err := loadReplicaDestroyedError(ctx, r.store.Engine(), r.RangeID)
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
	r.assertStateLocked(r.store.Engine())
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
func (r *Replica) destroyDataRaftMuLocked() error {
	desc := r.Desc()
	iter := NewReplicaDataIterator(desc, r.store.Engine(), false /* !replicatedOnly */)
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
	ctx := r.AnnotateCtx(context.TODO())
	if err := engine.MVCCPutProto(ctx, batch, nil, tombstoneKey, hlc.ZeroTimestamp, nil, tombstone); err != nil {
		return err
	}

	return batch.Commit()
}

func (r *Replica) setReplicaID(replicaID roachpb.ReplicaID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setReplicaIDLocked(replicaID)
}

// setReplicaIDLocked requires that the replica lock is held.
func (r *Replica) setReplicaIDLocked(replicaID roachpb.ReplicaID) error {
	if replicaID == 0 {
		// If the incoming message didn't give us a new replica ID,
		// there's nothing to do (this is only expected for preemptive snapshots).
		return nil
	}
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
		// repropose all pending commands under new replicaID.
		r.refreshProposalsLocked(0, reasonReplicaIDChanged)
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
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return r.mu.state.Lease, &nextLease
	}
	return r.mu.state.Lease, nil
}

// newNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func newNotLeaseHolderError(
	l *roachpb.Lease, originStoreID roachpb.StoreID, rangeDesc *roachpb.RangeDescriptor,
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
			err.Lease = l
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
				if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(
					repDesc.ReplicaID); ok {
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))
				}

				// Should we extend the lease?
				if _, ok := r.mu.pendingLeaseRequest.RequestPending(); !ok &&
					!timestamp.Less(lease.StartStasis.Add(-int64(r.store.cfg.RangeLeaseRenewalDuration), 0)) {
					if log.V(2) {
						log.Warningf(ctx, "extending lease %s at %s", lease, timestamp)
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
			log.Eventf(ctx, "request range lease (attempt #%d)", attempt)

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
			log.Event(ctx, "lease acquisition succeeded")
			continue
		case <-ctx.Done():
			log.ErrEventf(ctx, "lease acquisition failed: %s", ctx.Err())
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
// update. Requires raftMu to be locked.
func (r *Replica) setDesc(desc *roachpb.RangeDescriptor) error {
	r.setDescWithoutProcessUpdate(desc)
	if r.store == nil {
		// r.rm is null in some tests.
		return nil
	}
	return r.store.processRangeDescriptorUpdate(r)
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
func (r *Replica) getLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	timestamp := hlc.Timestamp{}
	_, err := engine.MVCCGetProto(ctx, r.store.Engine(), key, hlc.ZeroTimestamp, true, nil, &timestamp)
	if err != nil {
		return hlc.ZeroTimestamp, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(ctx context.Context, timestamp hlc.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return engine.MVCCPutProto(ctx, r.store.Engine(), nil, key, hlc.ZeroTimestamp, nil, &timestamp)
}

// RaftStatus returns the current raft status of the replica. It returns nil
// if the Raft group has not been initialized yet.
func (r *Replica) RaftStatus() *raft.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftStatusLocked()
}

func (r *Replica) raftStatusLocked() *raft.Status {
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
	ri.NumPending = uint64(len(r.mu.proposals))
	ri.RaftLogSize = r.mu.raftLogSize
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
	ctx := r.AnnotateCtx(context.TODO())
	diskState, err := loadState(ctx, reader, r.mu.state.Desc)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if !reflect.DeepEqual(diskState, r.mu.state) {
		log.Fatalf(ctx, "on-disk and in-memory state diverged:\n%s", pretty.Diff(diskState, r.mu.state))
	}
}

// Send adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's lease is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue.
// ctx should contain the log tags from the store (and up).
func (r *Replica) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	r.assert5725(ba)

	var br *roachpb.BatchResponse

	if err := r.checkBatchRequest(ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)
	ctx, cleanup := tracing.EnsureContext(ctx, r.AmbientContext.Tracer)
	defer cleanup()

	// Differentiate between admin, read-only and write.
	var pErr *roachpb.Error
	if ba.IsWrite() {
		log.Event(ctx, "read-write path")
		br, pErr = r.addWriteCmd(ctx, ba)
	} else if ba.IsReadOnly() {
		log.Event(ctx, "read-only path")
		br, pErr = r.addReadOnlyCmd(ctx, ba)
	} else if ba.IsAdmin() {
		log.Event(ctx, "admin path")
		br, pErr = r.addAdminCmd(ctx, ba)
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

func (r *Replica) checkBatchRange(ba roachpb.BatchRequest) error {
	rspan, err := keys.Range(ba)
	if err != nil {
		return err
	}

	desc := r.Desc()
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
			if lease, _ := repl.getLease(); lease != nil && lease.Covers(r.store.Clock().Now()) {
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

// beginCmds waits for any overlapping, already-executing commands via
// the command queue and adds itself to queues based on keys affected by the
// batched commands. This gates subsequent commands with overlapping keys or
// key ranges. This method will block if there are any overlapping commands
// already in the queue. Returns a cleanup function to be called when the
// commands are done and can be removed from the queue, and whose returned
// error is to be used in place of the supplied error.
func (r *Replica) beginCmds(
	ctx context.Context, ba *roachpb.BatchRequest,
) (func(*roachpb.BatchResponse, *roachpb.Error) *roachpb.Error, error) {
	var cmdGlobal *cmd
	var cmdLocal *cmd
	// Don't use the command queue for inconsistent reads.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		// Currently local spans are the exception, so preallocate for the
		// common case in which all are global.
		//
		// TODO(tschottdorf): revisit as the local portion gets its appropriate
		// use.
		spansGlobal := make([]roachpb.Span, 0, len(ba.Requests))
		var spansLocal []roachpb.Span

		for _, union := range ba.Requests {
			header := union.GetInner().Header()
			if keys.IsLocal(header.Key) {
				spansLocal = append(spansLocal, header)
			} else {
				spansGlobal = append(spansGlobal, header)
			}
		}

		// TODO(tschottdorf): need to make this less global when the local
		// command queue is used more heavily. For example, a split will have
		// a large read-only span but also a write (see #10084).
		readOnly := ba.IsReadOnly()

		r.cmdQMu.Lock()
		chans := r.cmdQMu.global.getWait(readOnly, spansGlobal...)
		chans = append(chans, r.cmdQMu.global.getWait(readOnly, spansLocal...)...)
		cmdGlobal = r.cmdQMu.global.add(readOnly, spansGlobal...)
		cmdLocal = r.cmdQMu.local.add(readOnly, spansLocal...)
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
				go func() {
					// The command is moot, so we don't need to bother executing.
					// However, the command queue assumes that commands don't drop
					// out before their prerequisites, so we still have to wait it
					// out.
					for _, ch := range chans {
						<-ch
					}
					r.cmdQMu.Lock()
					r.cmdQMu.global.remove(cmdGlobal)
					r.cmdQMu.local.remove(cmdLocal)
					r.cmdQMu.Unlock()
				}()
				return nil, err
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
	if ba.Timestamp.Equal(hlc.ZeroTimestamp) {
		if ba.Txn != nil {
			ba.Timestamp = ba.Txn.OrigTimestamp
		} else {
			ba.Timestamp = r.store.Clock().Now()
		}
	}

	return func(br *roachpb.BatchResponse, pErr *roachpb.Error) *roachpb.Error {
		return r.endCmds(cmdGlobal, cmdLocal, ba, br, pErr)
	}, nil
}

// endCmds removes pending commands from the command queue and updates
// the timestamp cache using the final timestamp of each command.
// The returned error replaces the supplied error.
func (r *Replica) endCmds(
	cmdGlobal *cmd,
	cmdLocal *cmd,
	ba *roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) (rErr *roachpb.Error) {
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

		r.mu.Lock()
		r.mu.tsCache.AddRequest(cr)
		r.mu.Unlock()
	}

	r.cmdQMu.Lock()
	r.cmdQMu.global.remove(cmdGlobal)
	r.cmdQMu.local.remove(cmdLocal)
	r.cmdQMu.Unlock()
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
func (r *Replica) applyTimestampCache(ba *roachpb.BatchRequest) (bumped bool, _ *roachpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var origTS hlc.Timestamp
	if ba.Txn != nil {
		r.mu.tsCache.ExpandRequests(ba.Txn.Timestamp)
		origTS = ba.Txn.Timestamp
	} else {
		r.mu.tsCache.ExpandRequests(ba.Timestamp)
		origTS = ba.Timestamp
	}
	defer func() {
		// Let the caller know whether we did anything.
		if ba.Txn != nil {
			bumped = origTS != ba.Txn.Timestamp
		} else {
			bumped = origTS != ba.Timestamp
		}
	}()

	for _, union := range ba.Requests {
		args := union.GetInner()
		if consultsTimestampCache(args) {
			header := args.Header()
			// BeginTransaction is a special case. We use the transaction
			// key to look for an entry which would indicate this transaction
			// has already been finalized, in which case this is a replay.
			if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
				key := keys.TransactionKey(header.Key, ba.GetTxnID())
				wTS, _, wOK := r.mu.tsCache.GetMaxWrite(key, nil)
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
					txn.Timestamp.Forward(wTS.Next())
					return bumped, roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(), &txn)
				}
				continue
			}

			// Forward the timestamp if there's been a more recent read (by someone else).
			rTS, rTxnID, _ := r.mu.tsCache.GetMaxRead(header.Key, header.EndKey)
			if ba.Txn != nil {
				if rTxnID == nil || *ba.Txn.ID != *rTxnID {
					nextTS := rTS.Next()
					if ba.Txn.Timestamp.Less(nextTS) {
						txn := ba.Txn.Clone()
						txn.Timestamp.Forward(rTS.Next())
						ba.Txn = &txn
					}
				}
			} else {
				ba.Timestamp.Forward(rTS.Next())
			}

			// On more recent writes, forward the timestamp and set the
			// write too old boolean for transactions. Note that currently
			// only EndTransaction and DeleteRange requests update the
			// write timestamp cache.
			wTS, wTxnID, _ := r.mu.tsCache.GetMaxWrite(header.Key, header.EndKey)
			if ba.Txn != nil {
				if wTxnID == nil || *ba.Txn.ID != *wTxnID {
					if !wTS.Less(ba.Txn.Timestamp) {
						txn := ba.Txn.Clone()
						txn.Timestamp.Forward(wTS.Next())
						txn.WriteTooOld = true
						ba.Txn = &txn
					}
				}
			} else {
				ba.Timestamp.Forward(wTS.Next())
			}
		}
	}
	return bumped, nil
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) addAdminCmd(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}

	if err := r.checkBatchRange(ba); err != nil {
		return nil, roachpb.NewErrorWithTxn(err, ba.Txn)
	}

	args := ba.Requests[0].GetInner()
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
		reply, pErr = r.CheckConsistency(ctx, *tArgs, r.Desc())
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
func (r *Replica) addReadOnlyCmd(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is consistent, the read requires the range lease.
	if ba.ReadConsistency != roachpb.INCONSISTENT {
		if pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			return nil, pErr
		}
	}

	var endCmdsFunc func(*roachpb.BatchResponse, *roachpb.Error) *roachpb.Error
	if !ba.IsNonKV() {
		// Add the read to the command queue to gate subsequent
		// overlapping commands until this command completes.
		log.Event(ctx, "command queue")
		var err error
		endCmdsFunc, err = r.beginCmds(ctx, &ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
	} else {
		endCmdsFunc = func(
			br *roachpb.BatchResponse, pErr *roachpb.Error,
		) *roachpb.Error {
			return pErr
		}
	}

	log.Event(ctx, "waiting for read lock")
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
	var pd ProposalData
	br, pd, pErr = r.executeBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), nil, ba)

	if pErr == nil && ba.Txn != nil {
		r.assert5725(ba)
		// Checking whether the transaction has been aborted on reads
		// makes sure that we don't experience anomalous conditions as
		// described in #2231.
		pErr = r.checkIfTxnAborted(ctx, r.store.Engine(), *ba.Txn)
	}
	if pd.intents != nil && len(*pd.intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(*pd.intents))
		r.store.intentResolver.processIntentsAsync(r, *pd.intents)
	}
	if pErr != nil {
		log.ErrEvent(ctx, pErr.String())
	} else {
		log.Event(ctx, "read completed")
	}
	return br, pErr
}

// TODO(tschottdorf): temporary assertion for #5725, which saw batches with
// a nonempty but incomplete Txn (i.e. &Transaction{})
func (r *Replica) assert5725(ba roachpb.BatchRequest) {
	if ba.Txn != nil && ba.Txn.ID == nil {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "nontrivial transaction with empty ID: %s\n%s",
			ba.Txn, pretty.Sprint(ba))
	}
}

// addWriteCmd is the entry point for client requests which may mutate the
// Range's replicated state. Requests taking this path are ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative (submitting requests to Raft one after another, paying massive
// latency) is only taken for commands whose effects may overlap.
//
// Concretely,
//
// - the keys affected by the command are added to the command queue (i.e.
//   tracked as in-flight mutations).
// - wait until the command queue promises that no overlapping mutations are
//   in flight.
// - the timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - the command is evaluated, resulting in a ProposalData. As proposer-eval'ed
//   KV isn't implemented yet, this means that the BatchRequest is added to
//   the resulting ProposalData object. With proposer-evaluated KV, a
//   WriteBatch containing the raw effects of the command's application is
//   added along with some auxiliary data.
// - the ProposalData is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - the result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, removed from the command queue, and
//   its result (which could be an error) returned to the client.
//
// Internally, multiple iterations of the above process are may take place due
// to the (rare) need to the Raft proposal failing retryably (usually due to
// proposal reordering).
//
// TODO(tschottdorf): take special care with "special" commands and their
// reorderings. For example, a batch of writes and a split could be in flight
// in parallel without overlap, but if the writes hit the RHS, something must
// prevent them from writing outside of the key range when they apply.
// Similarly, a command proposed under lease A must not apply under lease B.
func (r *Replica) addWriteCmd(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for {
		br, pErr, shouldRetry := r.tryAddWriteCmd(ctx, ba)
		if !shouldRetry {
			return br, pErr
		}
	}
}

// tryAddWriteCmd implements the logic outlined in its caller addWriteCmd, who
// will call this method until the returned boolean is false, in which case
// a result of the proposal (which is either an error or a successful response)
// is returned. If the boolean is true, a proposal was submitted to Raft but
// did not end up in a legal log position; it is guaranteed that the proposal
// will never apply successfully and so the caller may and should retry the
// same invocation of tryAddWriteCmd.
func (r *Replica) tryAddWriteCmd(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error, shouldRetry bool) {
	isNonKV := ba.IsNonKV()
	if !isNonKV {
		// Add the write to the command queue to gate subsequent overlapping
		// commands until this command completes. Note that this must be
		// done before getting the max timestamp for the key(s), as
		// timestamp cache is only updated after preceding commands have
		// been run to successful completion.
		log.Event(ctx, "command queue")
		endCmdsFunc, err := r.beginCmds(ctx, &ba)
		if err != nil {
			return nil, roachpb.NewError(err), false
		}

		// Guarantee we remove the commands from the command queue. This is
		// wrapped to delay pErr evaluation to its value when returning.
		defer func() {
			pErr = endCmdsFunc(br, pErr)
		}()
	}

	if !ba.IsSingleSkipLeaseCheckRequest() {
		// This replica must have range lease to process a write, except when it's
		// an attempt to unfreeze the Range. These are a special case in which any
		// replica will propose it to get back to an active state.
		if pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
			if _, frozen := pErr.GetDetail().(*roachpb.RangeFrozenError); !frozen {
				return nil, pErr, false
			}
			// Only continue if the batch appears freezing-related.
			if !ba.IsFreeze() {
				return nil, pErr, false
			}
		}
	}

	if !isNonKV {
		// Examine the read and write timestamp caches for preceding
		// commands which require this command to move its timestamp
		// forward. Or, in the case of a transactional write, the txn
		// timestamp and possible write-too-old bool.
		if bumped, pErr := r.applyTimestampCache(&ba); pErr != nil {
			return nil, pErr, false
		} else if bumped {
			// There is brittleness built into this system. If we bump the
			// transaction's timestamp, we must absolutely tell the client in
			// a response transaction (for otherwise it doesn't know about the
			// incremented timestamp; it might commit with the old one, which
			// either resolves intents to a lower timestamp than they were
			// written at, or discards them; whatever it would be in practice,
			// there's no way to do "the right thing" at that point).
			// Response transactions are set far away from this code, but at
			// the time of writing, they always seem to be set. Since that is
			// a likely target of future micro-optimization, this assertion
			// is likely to avoid a future correctness anomaly.
			defer func() {
				if br != nil && ba.Txn != nil && br.Txn == nil {
					txn := ba.Txn.Clone()
					br.Txn = &txn
					// TODO(tschottdorf): this actually fails in tests during
					// AdminSplit transactions. Very bad.
					//
					// See #10401.
					log.Warningf(ctx, "assertion failed: transaction updated by "+
						"timestamp cache, but transaction returned in response; "+
						"updated timestamp would have been lost (recovered): "+
						"%s in batch %s", ba.Txn, ba,
					)
				}
			}()
		}
	}

	log.Event(ctx, "raft")

	ch, tryAbandon, err := r.propose(ctx, ba)
	if err != nil {
		return nil, roachpb.NewError(err), false
	}

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	for {
		select {
		case respWithErr := <-ch:
			return respWithErr.Reply, respWithErr.Err, respWithErr.ShouldRetry
		case <-ctxDone:
			// We ignore cancelled contexts for running commands in order to
			// properly update the timestamp and command queue. Exiting early,
			// before knowing the final disposition of the command would make
			// those updates impossible, leading to potential inconsistencies.
			// TODO(spencer): move updates to the timestamp cache to raft
			// command execution.
			log.Warningf(ctx, "ignoring cancelled context for command %s", ba)
			ctxDone = nil
		case <-shouldQuiesce:
			// If we're shutting down, return an AmbiguousResultError to
			// indicate to caller that the command may have executed.
			// However, we proceed only if the command isn't already being
			// executed and using our context, in which case we expect it to
			// finish soon.
			if tryAbandon() {
				log.Warningf(ctx, "shutdown cancellation of command %s", ba)
				return nil, roachpb.NewError(roachpb.NewAmbiguousResultError()), false
			}
			shouldQuiesce = nil
		}
	}
}

// evaluateProposal generates ProposalData from the given request by evaluating
// it, returning both state which is held only on the proposer and that which
// is to be replicated through Raft. The return value is ready to be inserted
// into Replica's proposal map and subsequently passed to submitProposalLocked.
//
// Replica.mu must not be held.
//
// TODO(tschottdorf): with proposer-evaluated KV, a WriteBatch will be prepared
// in this method.
func (r *Replica) evaluateProposal(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	replica roachpb.ReplicaDescriptor,
	ba roachpb.BatchRequest,
) *ProposalData {
	// Note that we don't hold any locks at this point. This is important
	// since evaluating a proposal is expensive (at least under proposer-
	// evaluated KV).
	var pd ProposalData
	pd.ReplicatedProposalData = storagebase.ReplicatedProposalData{
		RangeID:       r.RangeID,
		OriginReplica: replica,
		Cmd:           &ba,
	}
	pd.ctx = ctx
	pd.idKey = idKey
	pd.done = make(chan proposalResult, 1)
	return &pd
}

func (r *Replica) insertProposalLocked(pd *ProposalData) {
	// Assign a lease index. Note that we do this as late as possible
	// to make sure (to the extent that we can) that we don't assign
	// (=predict) the index differently from the order in which commands are
	// proposed (and thus likely applied).
	if r.mu.lastAssignedLeaseIndex < r.mu.state.LeaseAppliedIndex {
		r.mu.lastAssignedLeaseIndex = r.mu.state.LeaseAppliedIndex
	}
	if !pd.Cmd.IsLeaseRequest() {
		r.mu.lastAssignedLeaseIndex++
	}
	pd.MaxLeaseIndex = r.mu.lastAssignedLeaseIndex
	if log.V(4) {
		log.Infof(pd.ctx, "submitting proposal %x: maxLeaseIndex=%d",
			pd.idKey, pd.MaxLeaseIndex)
	}

	if _, ok := r.mu.proposals[pd.idKey]; ok {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "pending command already exists for %s", pd.idKey)
	}
	r.mu.proposals[pd.idKey] = pd
}

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

// propose prepares necessary pending command struct and
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
func (r *Replica) propose(
	ctx context.Context, ba roachpb.BatchRequest,
) (chan proposalResult, func() bool, error) {
	r.mu.Lock()
	if r.mu.destroyed != nil {
		r.mu.Unlock()
		return nil, nil, r.mu.destroyed
	}
	repDesc, err := r.getReplicaDescriptorLocked()
	if err != nil {
		r.mu.Unlock()
		return nil, nil, err
	}
	r.mu.Unlock()

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

	pCmd := r.evaluateProposal(ctx, makeIDKey(), repDesc, ba)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.insertProposalLocked(pCmd)

	if err := r.submitProposalLocked(pCmd); err != nil {
		delete(r.mu.proposals, pCmd.idKey)
		return nil, nil, err
	}
	tryAbandon := func() bool {
		r.mu.Lock()
		_, ok := r.mu.proposals[pCmd.idKey]
		delete(r.mu.proposals, pCmd.idKey)
		r.mu.Unlock()
		return ok
	}
	return pCmd.done, tryAbandon, nil
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

func (r *Replica) isSoloReplicaLocked() bool {
	return len(r.mu.state.Desc.Replicas) == 1 &&
		r.mu.state.Desc.Replicas[0].ReplicaID == r.mu.replicaID
}

func defaultSubmitProposalLocked(r *Replica, p *ProposalData) error {
	if p.Cmd.Timestamp == hlc.ZeroTimestamp {
		return errors.Errorf("can't propose Raft command with zero timestamp")
	}

	ctx := r.AnnotateCtx(context.TODO())

	data, err := protoutil.Marshal(&p.ReplicatedProposalData)
	if err != nil {
		return err
	}
	defer r.store.enqueueRaftUpdateCheck(r.RangeID)

	if union, ok := p.Cmd.GetArg(roachpb.EndTransaction); ok {
		ict := union.(*roachpb.EndTransactionRequest).InternalCommitTrigger
		if crt := ict.GetChangeReplicasTrigger(); crt != nil {
			// EndTransactionRequest with a ChangeReplicasTrigger is special
			// because raft needs to understand it; it cannot simply be an
			// opaque command.
			log.Infof(ctx, "proposing %s %+v for range %d: %+v",
				crt.ChangeType, crt.Replica, p.RangeID, crt.UpdatedReplicas)

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
	}

	return r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		if log.V(4) {
			log.Infof(ctx, "proposing command %x", p.idKey)
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
		// Send an empty proposal which will wake the leader. Empty proposals also
		// trigger reproposal of pending commands, but this is expected to be a
		// very rare situation.
		_ = r.mu.internalRaftGroup.Propose(nil)
	}
}

func (r *Replica) maybeAbandonSnapshot(ctx context.Context) {
	r.mu.Lock()
	doneChan := r.mu.outSnapDone
	claimed := r.mu.outSnap.claimed
	snapUUID := r.mu.outSnap.SnapUUID
	r.mu.Unlock()

	if !claimed {
		select {
		// We can read from this without the replica lock because we're holding the
		// raft lock, which protects modification of the snapshot data.
		case <-doneChan:
		default:
			// If we're blocking on outSnapDone but not sending a snapshot, we
			// have a leaked/abandoned snapshot and need to clean it up.
			log.Warningf(ctx, "abandoning unsent snapshot %s", snapUUID)
			r.CloseOutSnap()
		}
	}
}

// handleRaftReady processes a raft.Ready containing entries and messages that
// are ready to read, be saved to stable storage, committed or sent to other
// peers. It takes a non-empty IncomingSnapshot to indicate that it is
// about to process a snapshot.
func (r *Replica) handleRaftReady(inSnap IncomingSnapshot) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.handleRaftReadyRaftMuLocked(inSnap)
}

// handleRaftReadyLocked is the same as handleRaftReady but requires that the
// replica's raftMu be held.
func (r *Replica) handleRaftReadyRaftMuLocked(inSnap IncomingSnapshot) error {
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
		return err
	}

	if !hasReady {
		return nil
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
			return errors.Wrap(err, "invalid snapshot id")
		}
		if inSnap.SnapUUID == *uuid.EmptyUUID {
			log.Fatalf(ctx, "programming error: a snapshot application was attempted outside of the streaming snapshot codepath")
		}
		if *snapUUID != inSnap.SnapUUID {
			log.Fatalf(ctx, "incoming snapshot id doesn't match raft snapshot id: %s != %s", *snapUUID, inSnap.SnapUUID)
		}

		if err := r.applySnapshot(ctx, inSnap, rd.Snapshot, rd.HardState); err != nil {
			return err
		}

		// handleRaftReady is called under the processRaftMu lock, so it is
		// safe to lock the store here.
		if err := func() error {
			r.store.mu.Lock()
			defer r.store.mu.Unlock()

			if r.store.removePlaceholderLocked(r.RangeID) {
				atomic.AddInt32(&r.store.counts.filledPlaceholders, 1)
			}
			if err := r.store.processRangeDescriptorUpdateLocked(r); err != nil {
				return errors.Wrapf(err, "could not processRangeDescriptorUpdate after applySnapshot")
			}
			return nil
		}(); err != nil {
			return err
		}

		if lastIndex, err = loadLastIndex(ctx, r.store.Engine(), r.RangeID); err != nil {
			return err
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

	// Update protected state (last index, raft log size and raft leader
	// ID) and set raft log entry cache. We clear any older, uncommitted
	// log entries and cache the latest ones.
	r.mu.Lock()
	r.store.raftEntryCache.addEntries(r.RangeID, rd.Entries)
	r.mu.lastIndex = lastIndex
	r.mu.raftLogSize = raftLogSize
	r.mu.leaderID = leaderID
	r.mu.Unlock()

	sendingSnapshot := false
	for _, msg := range rd.Messages {
		if !raft.IsEmptySnap(msg.Snapshot) {
			sendingSnapshot = true
		}
		r.sendRaftMessage(ctx, msg)
	}
	if !sendingSnapshot {
		r.maybeAbandonSnapshot(ctx)
	}

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:

			var commandID storagebase.CmdIDKey
			var command storagebase.ReplicatedProposalData

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
				if refreshReason == noReason {
					refreshReason = reasonNewLeaderOrConfigChange
				}
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
			_ = r.processRaftCommand(ctx, commandID, e.Index, command)

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return err
			}
			var ccCtx ConfChangeContext
			if err := ccCtx.Unmarshal(cc.Context); err != nil {
				return err
			}
			var command storagebase.ReplicatedProposalData
			if err := command.Unmarshal(ccCtx.Payload); err != nil {
				return err
			}
			if pErr := r.processRaftCommand(
				ctx, storagebase.CmdIDKey(ccCtx.CommandID), e.Index, command,
			); pErr != nil {
				// If processRaftCommand failed, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				return err
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
	return r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
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

	for remoteReplica := range remotes {
		r.mu.internalRaftGroup.ReportUnreachable(uint64(remoteReplica))
	}

	// If the raft group is uninitialized, do not initialize raft groups on
	// tick.
	if r.mu.internalRaftGroup == nil {
		return false, nil
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
		// RaftElectionTimeoutTicks to refreshPendingCmdsLocked means that commands
		// will be refreshed when they have been pending for 1 to 2 election
		// cycles.
		r.refreshProposalsLocked(
			r.store.cfg.RaftElectionTimeoutTicks, reasonTicks,
		)
	}
	return true, nil
}

var enableQuiescence = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_QUIESCENCE", true)

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
// problems on reorderin. If the wake-the-leader message is dropped the leader
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
	if !enableQuiescence {
		return false
	}
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
	fromReplica, fromErr := r.getReplicaDescriptorByIDLocked(r.mu.replicaID, r.mu.lastToReplica)
	if fromErr != nil {
		if log.V(4) {
			log.Infof(ctx, "not quiescing: cannot find from replica (%d)", r.mu.replicaID)
		}
		return false
	}
	select {
	case <-r.mu.outSnapDone:
	default:
		if log.V(4) {
			log.Infof(ctx, "not quiescing: replica %d has an in-progress snapshot", r.mu.replicaID)
		}
		return false
	}

	r.quiesceLocked()
	for id := range status.Progress {
		if roachpb.ReplicaID(id) == r.mu.replicaID {
			continue
		}
		toReplica, toErr := r.getReplicaDescriptorByIDLocked(
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
	return s[i].MaxLeaseIndex < s[j].MaxLeaseIndex
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
// which were likely dropped (but may still apply at a legal Lease index).
// mu must be held.
//
// refreshAtDelta specifies how old (in ticks) a command must be for it to be
// inspected; usually this is called with zero (affect everything) or the
// number of ticks of an election timeout (affect only proposals that have had
// ample time to apply but didn't).
func (r *Replica) refreshProposalsLocked(refreshAtDelta int, reason refreshRaftReason) {
	// Note that we can't use the commit index here (which is typically a
	// little ahead), because a pending command is removed only as it applies.
	// Thus we'd risk reproposing a command that has been committed but not yet
	// applied.
	maxMustRetryCommandIndex := r.mu.state.LeaseAppliedIndex // indexes <= are given up on
	refreshAtTicks := r.mu.ticks - refreshAtDelta
	numShouldRetry := 0
	var reproposals pendingCmdSlice
	for idKey, p := range r.mu.proposals {
		if p.MaxLeaseIndex > maxMustRetryCommandIndex {
			if p.proposedAtTicks > refreshAtTicks {
				// The command was proposed too recently, don't bother reproprosing
				// it yet. Note that if refreshAtDelta is 0, refreshAtTicks will be
				// r.mu.ticks making the above condition impossible.
				continue
			}
			reproposals = append(reproposals, p)
			continue
		}
		delete(r.mu.proposals, idKey)
		// The command's designated lease index range was filled up, so send it
		// back to the proposer for a retry.
		log.Eventf(p.ctx, "retry proposal %x: %s", p.idKey, reason)
		p.done <- proposalResult{ShouldRetry: true}
		close(p.done)
		numShouldRetry++
	}
	if log.V(1) && (numShouldRetry > 0 || len(reproposals) > 0) {
		ctx := r.AnnotateCtx(context.TODO())
		log.Infof(ctx,
			"pending commands: sent %d back to client, reproposing %d (at %d.%d)k %s",
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
			p.done <- proposalResult{Err: roachpb.NewError(err)}
			close(p.done)
		}
	}
}

func (r *Replica) getReplicaDescriptorByIDLocked(
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
	if !r.store.cfg.EnableCoalescedHeartbeats {
		return false
	}
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
	fromReplica, fromErr := r.getReplicaDescriptorByIDLocked(roachpb.ReplicaID(msg.From), r.mu.lastToReplica)
	toReplica, toErr := r.getReplicaDescriptorByIDLocked(roachpb.ReplicaID(msg.To), r.mu.lastFromReplica)
	snap := &r.mu.outSnap
	snap.claimed = true
	r.mu.Unlock()

	hasSnapshot := !raft.IsEmptySnap(msg.Snapshot)

	var beganStreaming bool
	if hasSnapshot {
		defer func() {
			if !beganStreaming {
				r.CloseOutSnap()
			}
		}()
	}

	if fromErr != nil {
		log.Warningf(ctx, "failed to look up sender replica %d in range %d while sending %s: %s",
			msg.From, r.RangeID, msg.Type, fromErr)
		return
	}
	if toErr != nil {
		log.Warningf(ctx, "failed to look up recipient replica %d in range %d while sending %s: %s",
			msg.To, r.RangeID, msg.Type, toErr)
		return
	}

	if hasSnapshot {
		msgUUID, err := uuid.FromBytes(msg.Snapshot.Data)
		if err != nil {
			log.Fatalf(ctx, "invalid snapshot: couldn't parse UUID from data: %s", err)
		}
		if *msgUUID != snap.SnapUUID {
			log.Fatalf(ctx, "programming error: snapshot message from Raft.Ready %s doesn't match outgoing snapshot UUID %s.",
				msgUUID.Short(), snap.SnapUUID.Short())
		}
		// Asynchronously stream the snapshot to the recipient.
		if err := r.store.Stopper().RunTask(func() {
			beganStreaming = true
			r.store.Stopper().RunWorker(func() {
				defer r.CloseOutSnap()
				if err := r.store.cfg.Transport.SendSnapshot(
					ctx,
					r.store.allocator.storePool,
					SnapshotRequest_Header{
						RangeDescriptor: *r.Desc(),
						RaftMessageRequest: RaftMessageRequest{
							RangeID:     r.RangeID,
							FromReplica: fromReplica,
							ToReplica:   toReplica,
							Message:     msg,
							Quiesce:     false,
						},
						RangeSize:  r.GetMVCCStats().Total(),
						CanDecline: false,
					}, snap, r.store.Engine().NewBatch); err != nil {
					log.Warningf(ctx, "failed to send snapshot: %s", err)
				}
				// Report the snapshot status to Raft, which expects us to do this once
				// we finish attempting to send the snapshot.
				r.reportSnapshotStatus(msg.To, err)
			})
		}); err != nil {
			log.Warningf(ctx, "failed to send snapshot: %s", err)
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

// processRaftCommand processes a raft command by unpacking the command
// struct to get args and reply and then applying the command to the
// state machine via applyRaftCommand(). The error result is sent on
// the command's done channel, if available.
// As a special case, the zero idKey signifies an empty Raft command,
// which will apply as a no-op (without accessing raftCmd, via an error),
// updating only the applied index.
func (r *Replica) processRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	index uint64,
	raftCmd storagebase.ReplicatedProposalData,
) (pErr *roachpb.Error) {
	if index == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}

	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", idKey, raftCmd.MaxLeaseIndex)
	}

	r.mu.Lock()
	cmd, cmdProposedLocally := r.mu.proposals[idKey]

	isLeaseError := func() bool {
		l, ba, origin := r.mu.state.Lease, raftCmd.Cmd, raftCmd.OriginReplica
		if l.Replica != origin && !ba.IsLeaseRequest() {
			return true
		}
		notCovered := !l.OwnedBy(origin.StoreID) || !l.Covers(ba.Timestamp)
		if notCovered && !ba.IsFreeze() && !ba.IsLeaseRequest() {
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
	if cmdProposedLocally {
		// We initiated this command, so use the caller-supplied context.
		ctx = cmd.ctx
		delete(r.mu.proposals, idKey)
	}
	leaseIndex := r.mu.state.LeaseAppliedIndex
	sendToClient := cmdProposedLocally

	var forcedErr *roachpb.Error
	if idKey == "" {
		// This is an empty Raft command (which is sent by Raft after elections
		// to trigger reproposals or during concurrent configuration changes).
		// Nothing to do here except making sure that the corresponding batch
		// (which is bogus) doesn't get executed (for it is empty and so
		// properties like key range are undefined).
		forcedErr = roachpb.NewErrorf("no-op on empty Raft entry")
	} else if isLeaseError() {
		log.VEventf(
			ctx, 1, "command proposed from replica %+v (lease at %v): %s",
			raftCmd.OriginReplica, r.mu.state.Lease.Replica, raftCmd.Cmd,
		)
		forcedErr = roachpb.NewError(newNotLeaseHolderError(
			r.mu.state.Lease, raftCmd.OriginReplica.StoreID, r.mu.state.Desc))
	} else if raftCmd.Cmd.IsLeaseRequest() {
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

		if cmdProposedLocally {
			log.VEventf(
				ctx, 1,
				"retry proposal %x: applied at lease index %d, required <= %d",
				cmd.idKey, leaseIndex, raftCmd.MaxLeaseIndex,
			)
			// Send to the client only at the end of this invocation. We can't
			// use the context any more once we signal the client, so we make
			// sure we signal it at the end of this method, when the context
			// has been fully used.
			defer func(ch chan proposalResult) {
				// Assert against another defer trying to use the context after
				// the client has been signaled.
				ctx = nil
				cmd.ctx = nil

				ch <- proposalResult{ShouldRetry: true}
				close(ch)
			}(cmd.done)
			cmd.done = make(chan proposalResult, 1)
		}
	}
	r.mu.Unlock()

	// TODO(tschottdorf): not all commands have a BatchRequest (for example,
	// empty appends). Be more careful with this in proposer-eval'ed KV.
	if raftCmd.Cmd == nil {
		raftCmd.Cmd = &roachpb.BatchRequest{}
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock := r.maybeAcquireSplitMergeLock(*raftCmd.Cmd); splitMergeUnlock != nil {
			defer func() {
				splitMergeUnlock(pErr)
			}()
		}

	}
	var response proposalResult
	{
		pd := r.applyRaftCommand(ctx, idKey, index, leaseIndex, *raftCmd.Cmd, forcedErr)
		pd.Err = r.maybeSetCorrupt(ctx, pd.Err)

		// TODO(tschottdorf): this field should be zeroed earlier.
		pd.Batch = nil

		// Save the response and zero out the field so that handleProposalData
		// knows it wasn't forgotten.
		response = proposalResult{Err: pd.Err, Reply: pd.Reply}
		pd.Err, pd.Reply = nil, nil

		// Handle the ProposalData, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		//
		// Note also that ProposalData can be returned on error. For example,
		// a failed commit might still send intents up for resolution.
		//
		// TODO(tschottdorf): make that more formal and then remove the ~4 copies
		// of this TODO which are scattered across the code.
		r.handleProposalData(ctx, raftCmd.OriginReplica, pd)
	}

	// On successful write commands handle write-related triggers including
	// splitting and raft log truncation.
	if response.Err == nil && raftCmd.Cmd.IsWrite() {
		if r.needsSplitBySize() {
			r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
		}
		const raftLogCheckFrequency = 1 + RaftLogQueueStaleThreshold/4
		if index%raftLogCheckFrequency == 0 {
			r.store.raftLogQueue.MaybeAdd(r, r.store.Clock().Now())
		}
	}

	if sendToClient {
		cmd.done <- response
		close(cmd.done)
	} else if response.Err != nil {
		log.VEventf(ctx, 1, "error executing raft command %s: %s", raftCmd.Cmd, response.Err)
	}

	return response.Err
}

func (r *Replica) maybeAcquireSplitMergeLock(ba roachpb.BatchRequest) func(pErr *roachpb.Error) {
	arg, ok := ba.GetArg(roachpb.EndTransaction)
	if !ok {
		return nil
	}
	ict := arg.(*roachpb.EndTransactionRequest).InternalCommitTrigger
	if split := ict.GetSplitTrigger(); split != nil {
		return r.acquireSplitLock(split)
	}
	if merge := ict.GetMergeTrigger(); merge != nil {
		return r.acquireMergeLock(merge)
	}
	return nil
}

func (r *Replica) acquireSplitLock(split *roachpb.SplitTrigger) func(pErr *roachpb.Error) {
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

	return func(pErr *roachpb.Error) {
		if pErr != nil && created && !rightRng.IsInitialized() {
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

func (r *Replica) acquireMergeLock(merge *roachpb.MergeTrigger) func(pErr *roachpb.Error) {
	rightRng, err := r.store.GetReplica(merge.RightDesc.RangeID)
	if err != nil {
		ctx := r.AnnotateCtx(context.TODO())
		log.Fatalf(ctx, "unable to find merge RHS replica: %s", err)
	}

	// TODO(peter,tschottdorf): This is necessary but likely not sufficient. The
	// right hand side of the merge can still race on reads. See #8630.
	rightRng.raftMu.Lock()
	return func(_ *roachpb.Error) {
		rightRng.raftMu.Unlock()
	}
}

// applyRaftCommand applies a raft command from the replicated log to the
// underlying state machine (i.e. the engine).
// When certain critical operations fail, a replicaCorruptionError may be
// returned and must be handled by the caller.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	index, leaseIndex uint64,
	ba roachpb.BatchRequest,
	forcedError *roachpb.Error,
) ProposalData {
	if index <= 0 {
		log.Fatalf(ctx, "raft command index is <= 0")
	}

	r.mu.Lock()
	oldIndex := r.mu.state.RaftAppliedIndex
	// When frozen, the Range only applies freeze- and consistency-related
	// requests. Overrides any forcedError.
	if mayApply := !r.mu.state.IsFrozen() || ba.IsFreeze() || ba.IsConsistencyRelated(); !mayApply {
		forcedError = roachpb.NewError(roachpb.NewRangeFrozenError(*r.mu.state.Desc))
	}
	ms := r.mu.state.Stats
	r.mu.Unlock()

	if index != oldIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		var pd ProposalData
		pd.Err = roachpb.NewError(NewReplicaCorruptionError(errors.Errorf("applied index jumped from %d to %d", oldIndex, index)))
		return pd
	}

	// TODO(tschottdorf): With proposer-eval'ed KV, this will be returned
	// along with the batch representation and, together with it, must
	// contain everything necessary for Replicas to apply the command.
	var pd ProposalData
	if forcedError != nil {
		pd.Batch = r.store.Engine().NewBatch()
		pd.Err = forcedError
	} else {
		pd = r.applyRaftCommandInBatch(ctx, idKey, ba)
	}
	// TODO(tschottdorf): remove when #7224 is cleared.
	if ba.Txn != nil && ba.Txn.Name == replicaChangeTxnName && log.V(1) {
		log.Infof(ctx, "applied part of replica change txn: %s, pErr=%v",
			ba, pd.Err)
	}

	defer func() {
		pd.Batch.Close()
		pd.Batch = nil
	}()

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch. Currently the only
	// remaining writes are the raft applied index and the updated MVCC stats.
	//
	writer := pd.Batch.Distinct()

	// Advance the last applied index.
	if err := setAppliedIndex(ctx, writer, &pd.delta, r.RangeID, index, leaseIndex); err != nil {
		log.Fatalf(ctx, "setting applied index in a batch should never fail: %s", err)
	}

	// Flush the MVCC stats to the batch. Note that they must not have changed
	// since we only evaluated a command but did not update in-memory state
	// yet.
	//
	// TODO(tschottdorf): this assertion should never fire (as most assertions)
	// and it should be removed after 2016-12-01.
	if nowMS := r.GetMVCCStats(); nowMS != ms {
		log.Fatalf(
			ctx,
			"MVCCStats changed during Raft command application: had %+v, now have %+v",
			ms, nowMS,
		)
	}
	ms.Add(pd.delta)
	if err := setMVCCStats(ctx, writer, r.RangeID, ms); err != nil {
		log.Fatalf(ctx, "setting mvcc stats in a batch should never fail: %s", err)
	}

	// TODO(tschottdorf): we could also not send this along and compute it
	// from the new stats (which are contained in the write batch). See about
	// a potential performance penalty (reads forcing an index to be built for
	// what is initially a slim Go batch) in doing so.
	//
	// We are interested in this delta only to report it to the Store, which
	// keeps a running total of all of its Replicas' stats.
	pd.State.Stats = ms
	// TODO(tschottdorf): return delta up the stack as separate variable.
	// It's used by the caller.

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	// TODO(tschottdorf): with proposer-eval'ed KV, the batch would not be
	// committed at this point. Instead, it would be added to propResult.
	if err := pd.Batch.Commit(); err != nil {
		if pd.Err != nil {
			err = errors.Wrap(pd.Err.GoError(), err.Error())
		}
		pd.Err = roachpb.NewError(NewReplicaCorruptionError(errors.Wrap(err, "could not commit batch")))
	} else {
		r.mu.Lock()
		// Update cached appliedIndex if we were able to set the applied index
		// on disk.
		// TODO(tschottdorf): with proposer-eval'ed KV, the lease applied index
		// can be read from the WriteBatch, but there may be reasons to pass
		// it with propResult. We'll see.
		pd.State.RaftAppliedIndex = index
		pd.State.LeaseAppliedIndex = leaseIndex
		r.mu.Unlock()
	}
	return pd
}

// applyRaftCommandInBatch executes the command in a batch engine and
// returns the batch containing the results. The caller is responsible
// for committing the batch, even on error.
func (r *Replica) applyRaftCommandInBatch(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest,
) ProposalData {
	// Check whether this txn has been aborted. Only applies to transactional
	// requests which write intents (for example HeartbeatTxn does not get
	// hindered by this).
	if ba.Txn != nil && ba.IsTransactionWrite() {
		r.assert5725(ba)
		if pErr := r.checkIfTxnAborted(ctx, r.store.Engine(), *ba.Txn); pErr != nil {
			var pd ProposalData
			pd.Batch = r.store.Engine().NewBatch()
			pd.Err = pErr
			return pd
		}
	}

	// Keep track of original txn Writing state to sanitize txn
	// reported with any error except TransactionRetryError.
	wasWriting := ba.Txn != nil && ba.Txn.Writing

	// Execute the commands. If this returns without an error, the batch should
	// be committed.
	var pd ProposalData
	{
		// TODO(tschottdorf): absorb all returned values in `pd` below this point
		// in the call stack as well.
		var pErr *roachpb.Error
		var ms enginepb.MVCCStats
		var br *roachpb.BatchResponse
		var btch engine.Batch
		btch, ms, br, pd, pErr = r.executeWriteBatch(ctx, idKey, ba)
		if (pd.delta != enginepb.MVCCStats{}) {
			log.Fatalf(ctx, "unexpected nonempty MVCC delta in ProposalData: %+v", pd)
		}

		pd.delta = ms
		pd.Batch = btch
		pd.Reply = br
		pd.Err = pErr
	}

	if ba.IsWrite() {
		if pd.Err != nil {
			// If the batch failed with a TransactionRetryError, any
			// preceding mutations in the batch engine should still be
			// applied so that intents are laid down in preparation for
			// the retry.
			if _, ok := pd.Err.GetDetail().(*roachpb.TransactionRetryError); !ok {
				// TODO(tschottdorf): make `nil` acceptable. Corresponds to
				// roachpb.Response{With->Or}Error.
				pd.Reply = &roachpb.BatchResponse{}
				// Otherwise, reset the batch to clear out partial execution and
				// prepare for the failed sequence cache entry.
				pd.Batch.Close()
				pd.Batch = r.store.Engine().NewBatch()
				pd.delta = enginepb.MVCCStats{}
				// Restore the original txn's Writing bool if pd.Err specifies
				// a transaction.
				if txn := pd.Err.GetTxn(); txn != nil && txn.Equal(ba.Txn) {
					txn.Writing = wasWriting
				}
			}
		}
	}
	return pd
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
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest,
) (engine.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, ProposalData, *roachpb.Error) {
	batch := r.store.Engine().NewBatch()
	ms := enginepb.MVCCStats{}
	// If not transactional or there are indications that the batch's txn
	// will require restart or retry, execute as normal.
	if r.store.TestingKnobs().DisableOnePhaseCommits || !isOnePhaseCommit(ba) {
		br, pd, pErr := r.executeBatch(ctx, idKey, batch, &ms, ba)
		return batch, ms, br, pd, pErr
	}

	// Try executing with transaction stripped.
	strippedBa := ba
	strippedBa.Txn = nil
	strippedBa.Requests = ba.Requests[1 : len(ba.Requests)-1] // strip begin/end txn reqs

	// If all writes occurred at the intended timestamp, we've succeeded on the fast path.
	br, pd, pErr := r.executeBatch(ctx, idKey, batch, &ms, strippedBa)
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
			if pd, err = r.runCommitTrigger(ctx, batch, &ms, *etArg, &clonedTxn); err != nil {
				return batch, ms, br, pd, roachpb.NewErrorf("failed to run commit trigger: %s", err)
			}
		}

		br.Txn = &clonedTxn
		// Add placeholder responses for begin & end transaction requests.
		br.Responses = append([]roachpb.ResponseUnion{{BeginTransaction: &roachpb.BeginTransactionResponse{}}}, br.Responses...)
		br.Responses = append(br.Responses, roachpb.ResponseUnion{EndTransaction: &roachpb.EndTransactionResponse{OnePhaseCommit: true}})
		return batch, ms, br, pd, nil
	}

	// Otherwise, re-execute with the original, transactional batch.
	batch.Close()
	batch = r.store.Engine().NewBatch()
	ms = enginepb.MVCCStats{}
	br, pd, pErr = r.executeBatch(ctx, idKey, batch, &ms, ba)
	return batch, ms, br, pd, pErr
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
				log.Fatalf(context.TODO(), "unexpected non-put request: %s", t)
			}
		}
	}
}

func (r *Replica) executeBatch(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, ProposalData, *roachpb.Error) {
	br := ba.CreateReply()
	if ba.Txn != nil {
		txnShallow := *ba.Txn // intentionally not a clone; only modify BatchIndex
		ba.Txn = &txnShallow
	}

	r.mu.Lock()
	threshold := r.mu.state.GCThreshold
	r.mu.Unlock()
	if !threshold.Less(ba.Timestamp) {
		return nil, ProposalData{}, roachpb.NewError(fmt.Errorf("batch timestamp %v must be after replica GC threshold %v", ba.Timestamp, threshold))
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

	// Update the node clock with the serviced request. This maintains a high
	// water mark for all ops serviced, so that received ops without a timestamp
	// specified are guaranteed one higher than any op already executed for
	// overlapping keys.
	r.store.Clock().Update(ba.Timestamp)

	if err := r.checkBatchRange(ba); err != nil {
		return nil, ProposalData{}, roachpb.NewErrorWithTxn(err, ba.Header.Txn)
	}

	var pd ProposalData
	for index, union := range ba.Requests {
		// Execute the command.
		args := union.GetInner()
		if ba.Txn != nil {
			ba.Txn.BatchIndex = int32(index)
		}
		reply := br.Responses[index].GetInner()
		curPD, pErr := r.executeCmd(ctx, idKey, index, batch, ms, ba.Header, maxKeys, args, reply)

		if err := pd.MergeAndDestroy(curPD); err != nil {
			// TODO(tschottdorf): see whether we really need to pass nontrivial
			// ProposalData up on error and if so, formalize that.
			log.Fatalf(
				ctx,
				"unable to absorb ProposalData: %s\ndiff(new, old): %s",
				err, pretty.Diff(curPD, pd),
			)
		}

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
						return nil, pd, pErr
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
						return nil, pd, pErr
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
				return nil, pd, pErr
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

	return br, pd, nil
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
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *roachpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if uuidBytes, err := r.store.Gossip().GetInfo(gossip.KeyClusterID); err == nil {
		if gossipClusterID, err := uuid.FromBytes(uuidBytes); err == nil {
			if *gossipClusterID != r.store.ClusterID() {
				log.Fatalf(
					ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
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
	if err := r.store.Gossip().AddInfo(
		gossip.KeyClusterID, r.store.ClusterID().GetBytes(), 0*time.Second,
	); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %s", err)
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

	ctx := r.AnnotateCtx(context.TODO())

	if lease, _ := r.getLease(); !lease.OwnedBy(r.store.StoreID()) || !lease.Covers(r.store.Clock().Now()) {
		// Do not gossip when a range lease is not held.
		return
	}

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

// maybeGossipNodeLiveness gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica
// must hold the lease to a range which contains some or all of the
// node liveness records. After scanning the records, it checks
// against what's already in gossip and only gossips records which
// are out of date.
func (r *Replica) maybeGossipNodeLiveness(span roachpb.Span) {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return
	}

	if !r.ContainsKeyRange(span.Key, span.EndKey) {
		return
	}

	if lease, _ := r.getLease(); !lease.OwnedBy(r.store.StoreID()) || !lease.Covers(r.store.Clock().Now()) {
		// Do not gossip when a range lease is not held.
		return
	}

	ctx := r.AnnotateCtx(context.TODO())

	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: span})
	// Call executeBatch instead of Send to avoid command queue reentrance.
	br, pd, pErr :=
		r.executeBatch(ctx, storagebase.CmdIDKey(""), r.store.Engine(), nil, ba)
	if pErr != nil {
		log.Errorf(ctx, "couldn't scan node liveness records in span %s: %s", span, pErr.GoError())
		return
	}
	if pd.intents != nil && len(*pd.intents) > 0 {
		log.Errorf(ctx, "unexpected intents on node liveness span %s: %+v", span, *pd.intents)
		return
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	log.VEventf(ctx, 1, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	for _, kv := range kvs {
		var liveness, exLiveness Liveness
		if err := kv.Value.GetProto(&liveness); err != nil {
			log.Errorf(ctx, "failed to unmarshal liveness value %s: %s", kv.Key, err)
			continue
		}
		key := gossip.MakeNodeLivenessKey(liveness.NodeID)
		// Look up liveness from gossip; skip gossiping anew if unchanged.
		if err := r.store.Gossip().GetInfoProto(key, &exLiveness); err == nil {
			if exLiveness == liveness {
				continue
			}
		}
		if err := r.store.Gossip().AddInfoProto(key, &liveness, 0); err != nil {
			log.Errorf(ctx, "failed to gossip node liveness (%+v): %s", liveness, err)
			continue
		}
	}
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
	ctx := r.AnnotateCtx(context.TODO())
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{Span: keys.SystemConfigSpan})
	// Call executeBatch instead of Send to avoid command queue reentrance.
	br, pd, pErr := r.executeBatch(
		ctx, storagebase.CmdIDKey(""), r.store.Engine(), nil, ba,
	)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	if pd.intents != nil && len(*pd.intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		r.store.intentResolver.processIntentsAsync(r, *pd.intents)
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
