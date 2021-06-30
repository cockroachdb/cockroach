// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/kr/pretty"
	"go.etcd.io/etcd/raft/v3"
)

const (
	// configGossipTTL is the time-to-live for configuration maps.

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

var testingDisableQuiescence = envutil.EnvOrDefaultBool("COCKROACH_DISABLE_QUIESCENCE", false)

var disableSyncRaftLog = settings.RegisterBoolSetting(
	"kv.raft_log.disable_synchronization_unsafe",
	"set to true to disable synchronization on Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on server crashes. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	false,
)

// MaxCommandSizeFloor is the minimum allowed value for the MaxCommandSize
// cluster setting.
const MaxCommandSizeFloor = 4 << 20 // 4MB

// MaxCommandSize wraps "kv.raft.command.max_size".
var MaxCommandSize = settings.RegisterByteSizeSetting(
	"kv.raft.command.max_size",
	"maximum size of a raft command",
	64<<20,
	func(size int64) error {
		if size < MaxCommandSizeFloor {
			return fmt.Errorf("max_size must be greater than %s", humanizeutil.IBytes(MaxCommandSizeFloor))
		}
		return nil
	},
)

// StrictGCEnforcement controls whether requests are rejected based on the GC
// threshold and the current GC TTL (true) or just based on the GC threshold
// (false).
var StrictGCEnforcement = settings.RegisterBoolSetting(
	"kv.gc_ttl.strict_enforcement.enabled",
	"if true, fail to serve requests at timestamps below the TTL even if the data still exists",
	true,
)

type proposalReevaluationReason int

const (
	proposalNoReevaluation proposalReevaluationReason = iota
	// proposalIllegalLeaseIndex indicates the proposal failed to apply at
	// a Lease index it was not legal for. The command should be re-evaluated.
	proposalIllegalLeaseIndex
)

type atomicDescString struct {
	strPtr unsafe.Pointer
}

// store atomically updates d.strPtr with the string representation of desc.
func (d *atomicDescString) store(replicaID roachpb.ReplicaID, desc *roachpb.RangeDescriptor) {
	str := redact.Sprintfn(func(w redact.SafePrinter) {
		w.Printf("%d/", desc.RangeID)
		if replicaID == 0 {
			w.SafeString("?:")
		} else {
			w.Printf("%d:", replicaID)
		}

		if !desc.IsInitialized() {
			w.SafeString("{-}")
		} else {
			const maxRangeChars = 30
			rngStr := keys.PrettyPrintRange(roachpb.Key(desc.StartKey), roachpb.Key(desc.EndKey), maxRangeChars)
			w.UnsafeString(rngStr)
		}
	})

	atomic.StorePointer(&d.strPtr, unsafe.Pointer(&str))
}

// String returns the string representation of the range; since we are not
// using a lock, the copy might be inconsistent.
func (d *atomicDescString) String() string {
	return d.get().StripMarkers()
}

// SafeFormat renders the string safely.
func (d *atomicDescString) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(d.get())
}

// Get returns the string representation of the range; since we are not
// using a lock, the copy might be inconsistent.
func (d *atomicDescString) get() redact.RedactableString {
	return *(*redact.RedactableString)(atomic.LoadPointer(&d.strPtr))
}

// atomicConnectionClass stores an rpc.ConnectionClass atomically.
type atomicConnectionClass uint32

// get reads the current value of the ConnectionClass.
func (c *atomicConnectionClass) get() rpc.ConnectionClass {
	return rpc.ConnectionClass(atomic.LoadUint32((*uint32)(c)))
}

// set updates the current value of the ConnectionClass.
func (c *atomicConnectionClass) set(cc rpc.ConnectionClass) {
	atomic.StoreUint32((*uint32)(c), uint32(cc))
}

// A Replica is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Replica struct {
	log.AmbientContext

	RangeID roachpb.RangeID // Only set by the constructor
	// The start key of a Range remains constant throughout its lifetime (it does
	// not change through splits or merges). This field carries a copy of
	// r.mu.state.Desc.StartKey (and nil if the replica is not initialized). The
	// copy is maintained to allow inserting locked Replicas into
	// Store.mu.replicasByKey (keyed on start key) without the risk of deadlock.
	// The synchronization for this field works as follows:
	//
	// - the field must not be accessed for uninitialized replicas, except:
	// - when setting the field (i.e. when initializing the replica), under `mu`.
	//
	// Due to the first rule, any access to the field is preceded by an
	// acquisition of `mu` (Replica.IsInitialized) which serializes the write and
	// any subsequent reads of the field.
	//
	// The writes to this key happen in Replica.setStartKeyLocked.
	startKey roachpb.RKey

	store     *Store
	abortSpan *abortspan.AbortSpan // Avoids anomalous reads after abort

	// leaseholderStats tracks all incoming BatchRequests to the replica and which
	// localities they come from in order to aid in lease rebalancing decisions.
	leaseholderStats *replicaStats
	// writeStats tracks the number of keys written by applied raft commands
	// in order to aid in replica rebalancing decisions.
	writeStats *replicaStats

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

	// connectionClass controls the ConnectionClass used to send raft messages.
	connectionClass atomicConnectionClass

	// schedulerCtx is a cached instance of an annotated Raft scheduler context.
	schedulerCtx atomic.Value // context.Context

	// raftMu protects Raft processing the replica.
	//
	// Locking notes: Replica.raftMu < Replica.mu
	raftMu struct {
		syncutil.Mutex

		// Note that there are two StateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader stateloader.StateLoader
		// on-disk storage for sideloaded SSTables. nil when there's no ReplicaID.
		sideloaded SideloadStorage
		// stateMachine is used to apply committed raft entries.
		stateMachine replicaStateMachine
		// decoder is used to decode committed raft entries.
		decoder replicaDecoder
	}

	// Contains the lease history when enabled.
	leaseHistory *leaseHistory

	// concMgr sequences incoming requests and provides isolation between
	// requests that intend to perform conflicting operations. It is the
	// centerpiece of transaction contention handling.
	concMgr concurrency.Manager

	// tenantLimiter rate limits requests on a per-tenant basis and accumulates
	// metrics about it.
	tenantLimiter tenantrate.Limiter

	// sideTransportClosedTimestamp encapsulates state related to the closed
	// timestamp's information about the range. Note that the
	// sideTransportClosedTimestamp does not incorporate the closed timestamp
	// information carried by Raft commands. That can be found in
	// r.mu.state.RaftClosedTimestamp. Generally, the Raft state should be queried
	// in parallel with the side transport state to determine an up to date closed
	// timestamp (i.e. the maximum across the two). For a given LAI, the side
	// transport closed timestamp will always lead the Raft closed timestamp.
	// Across LAIs, the larger LAI will always include the larger closed
	// timestamp, independent of the source.
	sideTransportClosedTimestamp sidetransportAccess

	mu struct {
		// Protects all fields in the mu struct.
		syncutil.RWMutex
		// The destroyed status of a replica indicating if it's alive, corrupt,
		// scheduled for destruction or has been GCed.
		// destroyStatus should only be set while also holding the raftMu and
		// readOnlyCmdMu.
		destroyStatus
		// Is the range quiescent? Quiescent ranges are not Tick()'d and unquiesce
		// whenever a Raft operation is performed.
		quiescent bool
		// laggingFollowersOnQuiesce is the set of dead replicas that are not
		// up-to-date with the rest of the quiescent Raft group. Nil if !quiescent.
		laggingFollowersOnQuiesce laggingReplicaSet
		// TODO(nvanbenschoten): remove in v21.1.
		laggingFollowersOnQuiesceAccurate bool
		// mergeComplete is non-nil if a merge is in-progress, in which case any
		// requests should be held until the completion of the merge is signaled by
		// the closing of the channel.
		mergeComplete chan struct{}
		// mergeTxnID contains the ID of the in-progress merge transaction, if a
		// merge is currently in progress. Otherwise, the ID is empty.
		mergeTxnID uuid.UUID
		// freezeStart indicates the subsumption time of this range when it is the
		// right-hand range in an ongoing merge. This range will allow read-only
		// traffic below this timestamp, while blocking everything else, until the
		// merge completes.
		// TODO(nvanbenschoten): get rid of this. It seemed like a good idea at
		// the time (b192bba), but in retrospect, it's a premature optimization
		// that prevents us from being more optimal about the read summary we
		// ship to the LHS during a merge. Serving reads below the closed
		// timestamp seems fine because that can't advance after the range is
		// frozen, but serving reads above the closed timestamp but below the
		// freeze start time is dangerous.
		freezeStart hlc.Timestamp
		// The state of the Raft state machine.
		state kvserverpb.ReplicaState
		// Last index/term persisted to the raft log (not necessarily
		// committed). Note that lastTerm may be 0 (and thus invalid) even when
		// lastIndex is known, in which case the term will have to be retrieved
		// from the Raft log entry. Use the invalidLastTerm constant for this
		// case.
		lastIndex, lastTerm uint64
		// A map of raft log index of pending snapshots to deadlines.
		// Used to prohibit raft log truncations that would leave a gap between
		// the snapshot and the new first index. The map entry has a zero
		// deadline while the snapshot is being sent and turns nonzero when the
		// snapshot has completed, preventing truncation for a grace period
		// (since there is a race between the snapshot completing and its being
		// reflected in the raft status used to make truncation decisions).
		//
		// NB: If we kept only one value, we could end up in situations in which
		// we're either giving some snapshots no grace period, or keep an
		// already finished snapshot "pending" for extended periods of time
		// (preventing log truncation).
		snapshotLogTruncationConstraints map[uuid.UUID]snapTruncationInfo
		// raftLogSize is the approximate size in bytes of the persisted raft
		// log, including sideloaded entries' payloads. The value itself is not
		// persisted and is computed lazily, paced by the raft log truncation
		// queue which will recompute the log size when it finds it
		// uninitialized. This recomputation mechanism isn't relevant for ranges
		// which see regular write activity (for those the log size will deviate
		// from zero quickly, and so it won't be recomputed but will undercount
		// until the first truncation is carried out), but it prevents a large
		// dormant Raft log from sitting around forever, which has caused problems
		// in the past.
		raftLogSize int64
		// If raftLogSizeTrusted is false, don't trust the above raftLogSize until
		// it has been recomputed.
		raftLogSizeTrusted bool
		// raftLogLastCheckSize is the value of raftLogSize the last time the Raft
		// log was checked for truncation or at the time of the last Raft log
		// truncation.
		raftLogLastCheckSize int64
		// pendingLeaseRequest is used to coalesce RequestLease requests.
		pendingLeaseRequest pendingLeaseRequest
		// minLeaseProposedTS is the minimum acceptable lease.ProposedTS; only
		// leases proposed after this timestamp can be used for proposing commands.
		// This is used to protect against several hazards:
		// - leases held (or even proposed) before a restart cannot be used after a
		// restart. This is because:
		// 		a) the spanlatch manager is wiped during the restart; there might be
		// 		writes in flight that do not have the latches they held reflected. So,
		// 		we need to synchronize all new reads with those old in-flight writes.
		// 		Forcing acquisition of a new lease essentially flushes all the
		// 		previous raft commands.
		// 		b) a lease transfer might have been in progress at the time of the
		// 		restart. Using the existing lease after the restart would break the
		// 		transfer proposer's promise to not use the existing lease.
		// - a lease cannot be used after a transfer is initiated. Moreover, even
		// lease extension that were in flight at the time of the transfer cannot be
		// used, if they eventually apply.
		minLeaseProposedTS hlc.ClockTimestamp
		// A pointer to the zone config for this replica.
		zone *zonepb.ZoneConfig
		// proposalBuf buffers Raft commands as they are passed to the Raft
		// replication subsystem. The buffer is populated by requests after
		// evaluation and is consumed by the Raft processing thread. Once
		// consumed, commands are proposed through Raft and moved to the
		// proposals map.
		//
		// The propBuf is the one closing timestamps, so evaluating writes must be
		// registered with the propBuf through TrackEvaluatingRequest before their
		// write timestamp is decided.
		//
		// Access to proposalBuf must occur *without* holding the mutex.
		// Instead, the buffer internally holds a reference to mu and will use
		// it appropriately.
		proposalBuf propBuf
		// proposals stores the Raft in-flight commands which originated at
		// this Replica, i.e. all commands for which propose has been called,
		// but which have not yet applied.
		//
		// The *ProposalData in the map are "owned" by it. Elements from the
		// map must only be referenced while the Replica.mu is held, except
		// if the element is removed from the map first. Modifying the proposal
		// itself may require holding the raftMu as fields can be accessed
		// underneath raft. See comments on ProposalData fields for synchronization
		// requirements.
		//
		// Due to Raft reproposals, multiple in-flight Raft entries can have
		// the same CmdIDKey, all corresponding to the same KV request. However,
		// not all Raft entries with a given command ID will correspond directly
		// to the *RaftCommand contained in its associated *ProposalData. This
		// is because the *RaftCommand can be mutated during reproposals by
		// Replica.tryReproposeWithNewLeaseIndex.
		//
		// TODO(ajwerner): move the proposal map and ProposalData entirely under
		// the raftMu.
		proposals map[kvserverbase.CmdIDKey]*ProposalData
		// Indicates that the replica is in the process of applying log entries.
		// Updated to true in handleRaftReady before entries are removed from
		// the proposals map and set to false after they are applied. Useful in
		// conjunction with len(proposals) to check for any in-flight proposals
		// whose effects have not yet taken hold without synchronizing with
		// raftMu and the entire handleRaftReady loop. Not needed if raftMu is
		// already held.
		applyingEntries bool
		// The replica's Raft group "node".
		internalRaftGroup *raft.RawNode
		// The ID of the replica within the Raft group. This value may never be 0.
		replicaID roachpb.ReplicaID
		// The minimum allowed ID for this replica. Initialized from
		// RangeTombstone.NextReplicaID.
		tombstoneMinReplicaID roachpb.ReplicaID

		// The ID of the leader replica within the Raft group. Used to determine
		// when the leadership changes.
		leaderID roachpb.ReplicaID
		// The most recently added replica for the range and when it was added.
		// Used to determine whether a replica is new enough that we shouldn't
		// penalize it for being slightly behind. These field gets cleared out once
		// we know that the replica has caught up.
		lastReplicaAdded     roachpb.ReplicaID
		lastReplicaAddedTime time.Time
		// initialMaxClosed is the initial maxClosed timestamp for the replica as known
		// from its left-hand-side upon creation.
		initialMaxClosed hlc.Timestamp

		// The most recently updated time for each follower of this range. This is updated
		// every time a Raft message is received from a peer.
		// Note that superficially it seems that similar information is contained in the
		// Progress of a RaftStatus, which has a RecentActive field. However, that field
		// is always true unless CheckQuorum is active, which at the time of writing in
		// CockroachDB is not the case.
		//
		// The lastUpdateTimes map is also updated when a leaseholder steps up
		// (making the assumption that all followers are live at that point),
		// and when the range unquiesces (marking all replicating followers as
		// live).
		//
		// TODO(tschottdorf): keeping a map on each replica seems to be
		// overdoing it. We should map the replicaID to a NodeID and then use
		// node liveness (or any sensible measure of the peer being around).
		// The danger in doing so is that a single stuck replica on an otherwise
		// functioning node could fill up the quota pool. We are already taking
		// this kind of risk though: a replica that gets stuck on an otherwise
		// live node will not lose leaseholdership.
		lastUpdateTimes lastUpdateTimesMap

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

		// Computed checksum at a snapshot UUID.
		checksums map[uuid.UUID]ReplicaChecksum

		// proposalQuota is the quota pool maintained by the lease holder where
		// incoming writes acquire quota from a fixed quota pool before going
		// through. If there is no quota available, the write is throttled
		// until quota is made available to the pool.
		// Acquired quota for a given command is only released when all the
		// replicas have persisted the corresponding entry into their logs.
		proposalQuota *quotapool.IntPool

		// The base index is the index up to (including) which quota was already
		// released. That is, the first element in quotaReleaseQueue below is
		// released as the base index moves up by one, etc.
		proposalQuotaBaseIndex uint64

		// Once the leader observes a proposal come 'out of Raft', we add the size
		// of the associated command to a queue of quotas we have yet to release
		// back to the quota pool. At that point ownership of the quota is
		// transferred from r.mu.proposals to this queue.
		// We'll release the respective quota once all replicas have persisted the
		// corresponding entry into their logs (or once we give up waiting on some
		// replica because it looks like it's dead).
		quotaReleaseQueue []*quotapool.IntAlloc

		// Counts calls to Replica.tick()
		ticks int

		// Counts Raft messages refused due to queue congestion.
		droppedMessages int

		// Note that there are two replicaStateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader stateloader.StateLoader

		// cachedProtectedTS provides the state of the protected timestamp
		// subsystem as used on the request serving path to determine the effective
		// gc threshold given the current TTL when using strict GC enforcement.
		//
		// It would be too expensive to go read from the protected timestamp cache
		// for every request. Instead, if clients want to ensure that their request
		// will see the effect of a protected timestamp record, they need to verify
		// the request. See the comment on the struct for more details.
		cachedProtectedTS cachedProtectedTimestampState

		// largestPreviousMaxRangeSizeBytes tracks a previous zone.RangeMaxBytes
		// which exceeded the current zone.RangeMaxBytes to help defeat the range
		// backpressure mechanism in cases where a user reduces the configured range
		// size. It is set when the zone config changes to a smaller value and the
		// current range size exceeds the new value. It is cleared after the range's
		// size drops below its current zone.MaxRangeBytes or if the
		// zone.MaxRangeBytes increases to surpass the current value.
		largestPreviousMaxRangeSizeBytes int64

		// failureToGossipSystemConfig is set to true when the leaseholder of the
		// range containing the system config span fails to gossip due to an
		// outstanding intent (see MaybeGossipSystemConfig). It is reset when the
		// system config is successfully gossiped or when the Replica loses the
		// lease. It is read when handling a MaybeGossipSystemConfigIfHaveFailure
		// local result trigger. That trigger is set when an EndTransaction with an
		// ABORTED status is evaluated on a range containing the system config span.
		//
		// While the gossipping of the system config span is best-effort, the sql
		// schema leasing mechanism degrades dramatically if changes are not
		// gossiped. This degradation is due to the fact that schema changes, after
		// writing intents, often need to ensure that there aren't outstanding
		// leases on old versions and if there are, roll back and wait until there
		// are not. The problem is that this waiting may take a long time if the
		// current leaseholders are not notified. We deal with this by detecting the
		// abort of a transaction which might have blocked the system config from
		// being gossiped and attempting to gossip again.
		failureToGossipSystemConfig bool

		tenantID roachpb.TenantID // Set when first initialized, not modified after

		// Historical information about the command that set the closed timestamp.
		closedTimestampSetter closedTimestampSetterInfo
	}

	rangefeedMu struct {
		syncutil.RWMutex
		// proc is an instance of a rangefeed Processor that is capable of
		// routing rangefeed events to a set of subscribers. Will be nil if no
		// subscribers are registered.
		//
		// Requires Replica.rangefeedMu be held when mutating the pointer.
		// Requires Replica.raftMu be held when providing logical ops and
		//  informing the processor of closed timestamp updates. This properly
		//  synchronizes updates that are linearized and driven by the Raft log.
		proc *rangefeed.Processor
		// opFilter is a best-effort filter that informs the raft processing
		// goroutine of which logical operations the rangefeed processor is
		// interested in based on the processor's current registrations.
		//
		// The filter is allowed to return false positives, but not false
		// negatives. False negatives are avoided by updating (expanding) the
		// filter while holding the Replica.raftMu when adding new registrations
		// after flushing the rangefeed.Processor event channel. This ensures
		// that no events that were filtered before the new registration was
		// added will be observed by the new registration and all events after
		// the new registration will respect the updated filter.
		//
		// Requires Replica.rangefeedMu be held when mutating the pointer.
		opFilter *rangefeed.Filter
	}

	// Throttle how often we offer this Replica to the split and merge queues.
	// We have triggers downstream of Raft that do so based on limited
	// information and without explicit throttling some replicas will offer once
	// per applied Raft command, which is silly and also clogs up the queues'
	// semaphores.
	splitQueueThrottle, mergeQueueThrottle util.EveryN

	// loadBasedSplitter keeps information about load-based splitting.
	loadBasedSplitter split.Decider

	unreachablesMu struct {
		syncutil.Mutex
		remotes map[roachpb.ReplicaID]struct{}
	}

	// r.mu < r.protectedTimestampMu
	protectedTimestampMu struct {
		syncutil.Mutex

		// minStateReadTimestamp is a lower bound on the timestamp of the cached
		// protected timestamp state which may be used when updating
		// pendingGCThreshold. This field acts to eliminate races between
		// verification of protected timestamp records and the setting of a new
		// GC threshold
		minStateReadTimestamp hlc.Timestamp

		// pendingGCThreshold holds a timestamp which is being proposed as a new
		// GC threshold for the range.
		pendingGCThreshold hlc.Timestamp
	}
}

var _ batcheval.EvalContext = &Replica{}

// String returns the string representation of the replica using an
// inconsistent copy of the range descriptor. Therefore, String does not
// require a lock and its output may not be atomic with other ongoing work in
// the replica. This is done to prevent deadlocks in logging sites.
func (r *Replica) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r *Replica) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[n%d,s%d,r%s]",
		r.store.Ident.NodeID, r.store.Ident.StoreID, r.rangeStr.get())
}

// ReplicaID returns the ID for the Replica. It may be zero if the replica does
// not know its ID. Once a Replica has a non-zero ReplicaID it will never change.
func (r *Replica) ReplicaID() roachpb.ReplicaID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.replicaID
}

// cleanupFailedProposal cleans up after a proposal that has failed. It
// clears any references to the proposal and releases associated quota.
// It requires that both Replica.mu and Replica.raftMu are exclusively held.
func (r *Replica) cleanupFailedProposalLocked(p *ProposalData) {
	r.raftMu.AssertHeld()
	r.mu.AssertHeld()
	delete(r.mu.proposals, p.idKey)
	p.releaseQuota()
}

// GetMinBytes gets the replica's minimum byte threshold.
func (r *Replica) GetMinBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.zone.RangeMinBytes
}

// GetMaxBytes gets the replica's maximum byte threshold.
func (r *Replica) GetMaxBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.zone.RangeMaxBytes
}

// SetZoneConfig sets the replica's zone config.
func (r *Replica) SetZoneConfig(zone *zonepb.ZoneConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isInitializedRLocked() &&
		r.mu.zone != nil &&
		zone != nil {
		total := r.mu.state.Stats.Total()

		// Set largestPreviousMaxRangeSizeBytes if the current range size is above
		// the new limit and we don't already have a larger value. Reset it if
		// the new limit is larger than the current largest we're aware of.
		if total > *zone.RangeMaxBytes &&
			*zone.RangeMaxBytes < *r.mu.zone.RangeMaxBytes &&
			r.mu.largestPreviousMaxRangeSizeBytes < *r.mu.zone.RangeMaxBytes &&
			// Check to make sure that we're replacing a real zone config. Otherwise
			// the default value would prevent backpressure until the range was
			// larger than the default value. When the store starts up it sets the
			// zone for the replica to this default value; later on it overwrites it
			// with a new instance even if the value is the same as the default.
			r.mu.zone != r.store.cfg.DefaultZoneConfig &&
			r.mu.zone != r.store.cfg.DefaultSystemZoneConfig {

			r.mu.largestPreviousMaxRangeSizeBytes = *r.mu.zone.RangeMaxBytes
		} else if r.mu.largestPreviousMaxRangeSizeBytes > 0 &&
			r.mu.largestPreviousMaxRangeSizeBytes < *zone.RangeMaxBytes {

			r.mu.largestPreviousMaxRangeSizeBytes = 0
		}
	}
	r.mu.zone = zone
}

// IsFirstRange returns true if this is the first range.
func (r *Replica) IsFirstRange() bool {
	return r.RangeID == 1
}

// IsDestroyed returns a non-nil error if the replica has been destroyed
// and the reason if it has.
func (r *Replica) IsDestroyed() (DestroyReason, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isDestroyedRLocked()
}

func (r *Replica) isDestroyedRLocked() (DestroyReason, error) {
	return r.mu.destroyStatus.reason, r.mu.destroyStatus.err
}

// IsQuiescent returns whether the replica is quiescent or not.
func (r *Replica) IsQuiescent() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.quiescent
}

// DescAndZone returns the authoritative range descriptor as well
// as the zone config for the replica.
func (r *Replica) DescAndZone() (*roachpb.RangeDescriptor, *zonepb.ZoneConfig) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc, r.mu.zone
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (r *Replica) Desc() *roachpb.RangeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.Desc
}

func (r *Replica) descRLocked() *roachpb.RangeDescriptor {
	r.mu.AssertRHeld()
	return r.mu.state.Desc
}

// closedTimestampPolicyRLocked returns the closed timestamp policy of the
// range, which is updated asynchronously through gossip of zone configurations.
// NOTE: an exported version of this method which does not require the replica
// lock exists in helpers_test.go. Move here if needed.
func (r *Replica) closedTimestampPolicyRLocked() roachpb.RangeClosedTimestampPolicy {
	if r.mu.zone.GlobalReads != nil && *r.mu.zone.GlobalReads {
		if !r.mu.state.Desc.ContainsKey(roachpb.RKey(keys.NodeLivenessPrefix)) {
			return roachpb.LEAD_FOR_GLOBAL_READS
		}
		// The node liveness range ignores zone configs and always uses a
		// LAG_BY_CLUSTER_SETTING closed timestamp policy. If it was to begin
		// closing timestamps in the future, it would break liveness updates,
		// which perform a 1PC transaction with a commit trigger and can not
		// tolerate being pushed into the future.
	}
	return roachpb.LAG_BY_CLUSTER_SETTING
}

// NodeID returns the ID of the node this replica belongs to.
func (r *Replica) NodeID() roachpb.NodeID {
	return r.store.NodeID()
}

// GetNodeLocality returns the locality of the node this replica belongs to.
func (r *Replica) GetNodeLocality() roachpb.Locality {
	return r.store.nodeDesc.Locality
}

// ClusterSettings returns the node's ClusterSettings.
func (r *Replica) ClusterSettings() *cluster.Settings {
	return r.store.cfg.Settings
}

// StoreID returns the Replica's StoreID.
func (r *Replica) StoreID() roachpb.StoreID {
	return r.store.StoreID()
}

// EvalKnobs returns the EvalContext's Knobs.
func (r *Replica) EvalKnobs() kvserverbase.BatchEvalTestingKnobs {
	return r.store.cfg.TestingKnobs.EvalKnobs
}

// Clock returns the hlc clock shared by this replica.
func (r *Replica) Clock() *hlc.Clock {
	return r.store.Clock()
}

// Engine returns the Replica's underlying Engine. In most cases the
// evaluation Batch should be used instead.
func (r *Replica) Engine() storage.Engine {
	return r.store.Engine()
}

// AbortSpan returns the Replica's AbortSpan.
func (r *Replica) AbortSpan() *abortspan.AbortSpan {
	// Despite its name, the AbortSpan doesn't hold on-disk data in
	// memory. It just provides methods that take a Batch, so SpanSet
	// declarations are enforced there.
	return r.abortSpan
}

// GetConcurrencyManager returns the Replica's concurrency.Manager.
func (r *Replica) GetConcurrencyManager() concurrency.Manager {
	return r.concMgr
}

// GetTerm returns the term of the given index in the raft log.
func (r *Replica) GetTerm(i uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftTermRLocked(i)
}

// GetRangeID returns the Range ID.
func (r *Replica) GetRangeID() roachpb.RangeID {
	return r.RangeID
}

// GetGCThreshold returns the GC threshold.
func (r *Replica) GetGCThreshold() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.GCThreshold
}

// Version returns the replica version.
func (r *Replica) Version() roachpb.Version {
	if r.mu.state.Version == nil {
		// We introduced replica versions in v21.1 to service long-running
		// migrations. For replicas that were instantiated pre-21.1, it's
		// possible that the replica version is unset (but not for too long!).
		//
		// In the 21.1 cycle we introduced below-raft migrations that install a
		// replica version on all replicas currently part of a raft group. What
		// the migrations don't (directly) do is ensure that the versions are
		// also installed on replicas slated to be GC-ed soon. For that purpose
		// the migrations infrastructure makes use of PurgeOutdatedReplicas.
		//
		// All that is to say that in 21.1, it's possible we're dealing with
		// unset replica versions.
		//
		// TODO(irfansharif): Remove this in 21.2; we'll have migrated into 21.1
		// and purged all outdated replicas by then, and thus guaranteed to
		// always have replica versions.
		return roachpb.Version{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.Version
}

// GetRangeInfo atomically reads the range's current range info.
func (r *Replica) GetRangeInfo(ctx context.Context) roachpb.RangeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	desc := r.descRLocked()
	l, _ /* nextLease */ := r.getLeaseRLocked()
	closedts := r.closedTimestampPolicyRLocked()

	// Sanity check the lease.
	if !l.Empty() {
		if _, ok := desc.GetReplicaDescriptorByID(l.Replica.ReplicaID); !ok {
			// I wish this could be a Fatal, but unfortunately it's possible for the
			// lease to be incoherent with the descriptor after a leaseholder was
			// brutally removed through `cockroach debug unsafe-remove-dead-replicas`.
			log.Errorf(ctx, "leaseholder replica not in descriptor; desc: %s, lease: %s", desc, l)
			// Let's not return an incoherent lease; for example if we end up
			// returning it to a client through a br.RangeInfos, the client will freak
			// out.
			l = roachpb.Lease{}
		}
	}

	return roachpb.RangeInfo{
		Desc:                  *desc,
		Lease:                 l,
		ClosedTimestampPolicy: closedts,
	}
}

// getImpliedGCThresholdRLocked returns the gc threshold of the replica which
// should be used to determine the validity of commands. The returned timestamp
// may be newer than the replica's true GC threshold if strict enforcement
// is enabled and the TTL has passed. If this is an admin command or this range
// contains data outside of the user keyspace, we return the true GC threshold.
func (r *Replica) getImpliedGCThresholdRLocked(
	st kvserverpb.LeaseStatus, isAdmin bool,
) hlc.Timestamp {
	threshold := *r.mu.state.GCThreshold

	// The GC threshold is the oldest value we can return here.
	if isAdmin || !StrictGCEnforcement.Get(&r.store.ClusterSettings().SV) ||
		r.isSystemRangeRLocked() {
		return threshold
	}

	// In order to make this check inexpensive, we keep a copy of the reading of
	// protected timestamp state in the replica. This state may be stale, may not
	// exist, or may be unusable given the current lease status. In those cases we
	// must return the GC threshold. On the one hand this seems like a big deal,
	// after a lease transfer, for minutes, users will be able to read data that
	// has technically expired. Fortunately this strict enforcement is merely a
	// user experience win; it's always safe to allow reads to continue so long
	// as they are after the GC threshold.
	c := r.mu.cachedProtectedTS
	if st.State != kvserverpb.LeaseState_VALID || c.readAt.Less(st.Lease.Start.ToTimestamp()) {
		return threshold
	}

	impliedThreshold := gc.CalculateThreshold(st.Now.ToTimestamp(), *r.mu.zone.GC)
	threshold.Forward(impliedThreshold)

	// If we have a protected timestamp record which precedes the implied
	// threshold, use the threshold it implies instead.
	if c.earliestRecord != nil && c.earliestRecord.Timestamp.Less(threshold) {
		threshold = c.earliestRecord.Timestamp.Prev()
	}
	return threshold
}

// isSystemRange returns true if r's key range precedes the start of user
// structured data (SQL keys) for the range's tenant keyspace.
func (r *Replica) isSystemRange() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isSystemRangeRLocked()
}

func (r *Replica) isSystemRangeRLocked() bool {
	rem, _, err := keys.DecodeTenantPrefix(r.mu.state.Desc.StartKey.AsRawKey())
	return err == nil && roachpb.Key(rem).Compare(keys.UserTableDataMin) < 0
}

// maxReplicaIDOfAny returns the maximum ReplicaID of any replica, including
// voters and learners.
func maxReplicaIDOfAny(desc *roachpb.RangeDescriptor) roachpb.ReplicaID {
	if desc == nil || !desc.IsInitialized() {
		return 0
	}
	var maxID roachpb.ReplicaID
	for _, repl := range desc.Replicas().Descriptors() {
		if repl.ReplicaID > maxID {
			maxID = repl.ReplicaID
		}
	}
	return maxID
}

// LastReplicaAdded returns the ID of the most recently added replica and the
// time at which it was added.
func (r *Replica) LastReplicaAdded() (roachpb.ReplicaID, time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastReplicaAdded, r.mu.lastReplicaAddedTime
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
	return roachpb.ReplicaDescriptor{}, roachpb.NewRangeNotFoundError(r.RangeID, r.store.StoreID())
}

func (r *Replica) getMergeCompleteCh() chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getMergeCompleteChRLocked()
}

func (r *Replica) getMergeCompleteChRLocked() chan struct{} {
	return r.mu.mergeComplete
}

func (r *Replica) mergeInProgressRLocked() bool {
	return r.mu.mergeComplete != nil
}

// setLastReplicaDescriptors sets the most recently seen replica
// descriptors to those contained in the *RaftMessageRequest, acquiring r.mu
// to do so.
func (r *Replica) setLastReplicaDescriptors(req *RaftMessageRequest) {
	r.mu.Lock()
	r.mu.lastFromReplica = req.FromReplica
	r.mu.lastToReplica = req.ToReplica
	r.mu.Unlock()
}

// GetMVCCStats returns a copy of the MVCC stats object for this range.
// This accessor is thread-safe, but provides no guarantees about its
// synchronization with any concurrent writes.
func (r *Replica) GetMVCCStats() enginepb.MVCCStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.mu.state.Stats
}

// GetMaxSplitQPS returns the Replica's maximum queries/s request rate over a
// configured measurement period. If the Replica has not been recording QPS for
// at least an entire measurement period, the method will return false.
//
// NOTE: This should only be used for load based splitting, only
// works when the load based splitting cluster setting is enabled.
//
// Use QueriesPerSecond() for current QPS stats for all other purposes.
func (r *Replica) GetMaxSplitQPS() (float64, bool) {
	return r.loadBasedSplitter.MaxQPS(r.Clock().PhysicalTime())
}

// GetLastSplitQPS returns the Replica's most recent queries/s request rate.
//
// NOTE: This should only be used for load based splitting, only
// works when the load based splitting cluster setting is enabled.
//
// Use QueriesPerSecond() for current QPS stats for all other purposes.
func (r *Replica) GetLastSplitQPS() float64 {
	return r.loadBasedSplitter.LastQPS(r.Clock().PhysicalTime())
}

// ContainsKey returns whether this range contains the specified key.
//
// TODO(bdarnell): This is not the same as RangeDescriptor.ContainsKey.
func (r *Replica) ContainsKey(key roachpb.Key) bool {
	return kvserverbase.ContainsKey(r.Desc(), key)
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Replica) ContainsKeyRange(start, end roachpb.Key) bool {
	return kvserverbase.ContainsKeyRange(r.Desc(), start, end)
}

// GetLastReplicaGCTimestamp reads the timestamp at which the replica was
// last checked for removal by the replica gc queue.
func (r *Replica) GetLastReplicaGCTimestamp(ctx context.Context) (hlc.Timestamp, error) {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	var timestamp hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, &timestamp,
		storage.MVCCGetOptions{})
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return timestamp, nil
}

func (r *Replica) setLastReplicaGCTimestamp(ctx context.Context, timestamp hlc.Timestamp) error {
	key := keys.RangeLastReplicaGCTimestampKey(r.RangeID)
	return storage.MVCCPutProto(ctx, r.store.Engine(), nil, key, hlc.Timestamp{}, nil, &timestamp)
}

// getQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) getQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	key := keys.QueueLastProcessedKey(r.Desc().StartKey, queue)
	var timestamp hlc.Timestamp
	if r.store != nil {
		_, err := storage.MVCCGetProto(ctx, r.store.Engine(), key, hlc.Timestamp{}, &timestamp,
			storage.MVCCGetOptions{})
		if err != nil {
			log.VErrEventf(ctx, 2, "last processed timestamp unavailable: %s", err)
			return hlc.Timestamp{}, err
		}
	}
	log.VEventf(ctx, 2, "last processed timestamp: %s", timestamp)
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
		s := rg.Status()
		return &s
	}
	return nil
}

func (r *Replica) raftBasicStatusRLocked() raft.BasicStatus {
	if rg := r.mu.internalRaftGroup; rg != nil {
		return rg.BasicStatus()
	}
	return raft.BasicStatus{}
}

// State returns a copy of the internal state of the Replica, along with some
// auxiliary information.
func (r *Replica) State(ctx context.Context) kvserverpb.RangeInfo {
	var ri kvserverpb.RangeInfo

	// NB: this acquires an RLock(). Reentrant RLocks are deadlock prone, so do
	// this first before RLocking below. Performance of this extra lock
	// acquisition is not a concern.
	ri.ActiveClosedTimestamp, _ = r.maxClosed(context.Background())

	// NB: numRangefeedRegistrations doesn't require Replica.mu to be locked.
	// However, it does require coordination between multiple goroutines, so
	// it's best to keep it out of the Replica.mu critical section.
	ri.RangefeedRegistrations = int64(r.numRangefeedRegistrations())

	ri.LockTable = r.concMgr.LockTableDebug()

	r.mu.RLock()
	defer r.mu.RUnlock()
	ri.ReplicaState = *(protoutil.Clone(&r.mu.state)).(*kvserverpb.ReplicaState)
	ri.LastIndex = r.mu.lastIndex
	ri.NumPending = uint64(r.numPendingProposalsRLocked())
	ri.RaftLogSize = r.mu.raftLogSize
	ri.RaftLogSizeTrusted = r.mu.raftLogSizeTrusted
	ri.NumDropped = uint64(r.mu.droppedMessages)
	if r.mu.proposalQuota != nil {
		ri.ApproximateProposalQuota = int64(r.mu.proposalQuota.ApproximateQuota())
		ri.ProposalQuotaBaseIndex = int64(r.mu.proposalQuotaBaseIndex)
		ri.ProposalQuotaReleaseQueue = make([]int64, len(r.mu.quotaReleaseQueue))
		for i, a := range r.mu.quotaReleaseQueue {
			if a != nil {
				ri.ProposalQuotaReleaseQueue[i] = int64(a.Acquired())
			}
		}
	}
	ri.RangeMaxBytes = *r.mu.zone.RangeMaxBytes
	if desc := ri.ReplicaState.Desc; desc != nil {
		// Learner replicas don't serve follower reads, but they still receive
		// closed timestamp updates, so include them here.
		allReplicas := desc.Replicas().Descriptors()
		for i := range allReplicas {
			replDesc := &allReplicas[i]
			r.store.cfg.ClosedTimestamp.Storage.VisitDescending(replDesc.NodeID, func(e ctpb.Entry) (done bool) {
				mlai, found := e.MLAI[r.RangeID]
				if !found {
					return false // not done
				}
				if ri.NewestClosedTimestamp.ClosedTimestamp.Less(e.ClosedTimestamp) {
					ri.NewestClosedTimestamp.NodeID = replDesc.NodeID
					ri.NewestClosedTimestamp.ClosedTimestamp = e.ClosedTimestamp
					ri.NewestClosedTimestamp.MLAI = int64(mlai)
					ri.NewestClosedTimestamp.Epoch = int64(e.Epoch)
				}
				return true // done
			})
		}

		if r.mu.tenantID != (roachpb.TenantID{}) {
			ri.TenantID = r.mu.tenantID.ToUint64()
		}
	}
	ri.ClosedTimestampPolicy = r.closedTimestampPolicyRLocked()
	r.sideTransportClosedTimestamp.mu.Lock()
	ri.ClosedTimestampSideTransportInfo.ReplicaClosed = r.sideTransportClosedTimestamp.mu.closedTimestamp
	ri.ClosedTimestampSideTransportInfo.ReplicaLAI = r.sideTransportClosedTimestamp.mu.lai
	r.sideTransportClosedTimestamp.mu.Unlock()
	centralClosed, centralLAI := r.store.cfg.ClosedTimestampReceiver.GetClosedTimestamp(
		ctx, r.RangeID, r.mu.state.Lease.Replica.NodeID)
	ri.ClosedTimestampSideTransportInfo.CentralClosed = centralClosed
	ri.ClosedTimestampSideTransportInfo.CentralLAI = centralLAI

	return ri
}

// assertStateRaftMuLockedReplicaMuRLocked can be called from the Raft goroutine
// to check that the in-memory and on-disk states of the Replica are congruent.
// Requires that r.raftMu is locked and r.mu is read locked.
func (r *Replica) assertStateRaftMuLockedReplicaMuRLocked(
	ctx context.Context, reader storage.Reader,
) {
	diskState, err := r.mu.stateLoader.Load(ctx, reader, r.mu.state.Desc)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	if !diskState.Equal(r.mu.state) {
		// The roundabout way of printing here is to expose this information in sentry.io.
		//
		// TODO(dt): expose properly once #15892 is addressed.
		log.Errorf(ctx, "on-disk and in-memory state diverged:\n%s",
			pretty.Diff(diskState, r.mu.state))
		r.mu.state.Desc, diskState.Desc = nil, nil
		log.Fatalf(ctx, "on-disk and in-memory state diverged: %s",
			log.Safe(pretty.Diff(diskState, r.mu.state)))
	}
	if r.isInitializedRLocked() {
		if !r.startKey.Equal(r.mu.state.Desc.StartKey) {
			log.Fatalf(ctx, "denormalized start key %s diverged from %s", r.startKey, r.mu.state.Desc.StartKey)
		}
	}
}

// TODO(nvanbenschoten): move the following 5 methods to replica_send.go.

// checkExecutionCanProceed returns an error if a batch request cannot be
// executed by the Replica. An error indicates that the Replica is not live and
// able to serve traffic or that the request is not compatible with the state of
// the Range due to the range's key bounds, the range's lease, or the ranges GC
// threshold.
//
// The method accepts a concurrency Guard, which is used to indicate whether the
// caller has acquired latches. When this condition is false, the batch request
// will not wait for a pending merge to conclude before proceeding. Callers
// might be ok with this if they know that they will end up checking for a
// pending merge at some later time.
func (r *Replica) checkExecutionCanProceed(
	ctx context.Context, ba *roachpb.BatchRequest, g *concurrency.Guard,
) (kvserverpb.LeaseStatus, error) {
	var st kvserverpb.LeaseStatus
	var shouldExtend bool
	defer func() {
		if shouldExtend {
			r.maybeExtendLeaseAsync(ctx, st)
		}
	}()
	now := r.Clock().NowAsClockTimestamp()
	rSpan, err := keys.Range(ba.Requests)
	if err != nil {
		return kvserverpb.LeaseStatus{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Has the replica been initialized?
	// NB: this should have already been checked in Store.Send, so we don't need
	// to handle this case particularly well, but if we do reach here (as some
	// tests that call directly into Replica.Send have), it's better to return
	// an error than to panic in checkSpanInRangeRLocked.
	if !r.isInitializedRLocked() {
		return kvserverpb.LeaseStatus{}, errors.Errorf("%s not initialized", r)
	}

	// Is the replica destroyed?
	if _, err := r.isDestroyedRLocked(); err != nil {
		return kvserverpb.LeaseStatus{}, err
	}

	// Is the request fully contained in the range?
	// NB: we only need to check that the request is in the Range's key bounds
	// at evaluation time, not at application time, because the spanlatch manager
	// will synchronize all requests (notably EndTxn with SplitTrigger) that may
	// cause this condition to change.
	if err := r.checkSpanInRangeRLocked(ctx, rSpan); err != nil {
		return kvserverpb.LeaseStatus{}, err
	}

	// Is the lease valid?
	if !ba.ReadConsistency.RequiresReadLease() {
		// For INCONSISTENT requests, we don't need the lease.
		st = kvserverpb.LeaseStatus{
			Now: now,
		}
	} else if ba.IsSingleSkipLeaseCheckRequest() {
		// For lease commands, use the provided previous lease for verification.
		st = kvserverpb.LeaseStatus{
			Lease: ba.GetPrevLeaseForLeaseRequest(),
			Now:   now,
		}
	} else {
		// If the request is a write or a consistent read, it requires the
		// replica serving it to hold the range lease. We pass the write
		// timestamp of the request because this is the maximum timestamp that
		// the request will operate at, ignoring the uncertainty interval, which
		// is already accounted for in LeaseStatus's stasis period handling.
		st, shouldExtend, err = r.leaseGoodToGoRLocked(ctx, now, ba.WriteTimestamp())
		if err != nil {
			// If not, can we serve this request on a follower?
			// TODO(nvanbenschoten): once we make this check cheaper
			// than leaseGoodToGoRLocked, invert these checks.
			if !r.canServeFollowerReadRLocked(ctx, ba, err) {
				return st, err
			}
			err = nil                     // ignore error
			st = kvserverpb.LeaseStatus{} // already empty for follower reads, but be explicit
		}
	}

	// Is the request below the GC threshold?
	if err := r.checkTSAboveGCThresholdRLocked(ba.EarliestActiveTimestamp(), st, ba.IsAdmin()); err != nil {
		return kvserverpb.LeaseStatus{}, err
	}

	// Is there a merge in progress?
	if r.mergeInProgressRLocked() && g.HoldingLatches() {
		// We only check for a merge if we are holding latches. In practice,
		// this means that any request where concurrency.shouldAcquireLatches()
		// is false (e.g. RequestLeaseRequests) will not wait for a pending
		// merge before executing and, as such, can execute while a range is in
		// a merge's critical phase (i.e. while the RHS of the merge is
		// subsumed).
		if err := r.shouldWaitForPendingMergeRLocked(ctx, ba); err != nil {
			// TODO(nvanbenschoten): we should still be able to serve reads
			// below the closed timestamp in this case.
			return kvserverpb.LeaseStatus{}, err
		}
	}

	return st, nil
}

// checkExecutionCanProceedForRangeFeed returns an error if a rangefeed request
// cannot be executed by the Replica.
func (r *Replica) checkExecutionCanProceedForRangeFeed(
	ctx context.Context, rSpan roachpb.RSpan, ts hlc.Timestamp,
) error {
	now := r.Clock().NowAsClockTimestamp()
	r.mu.RLock()
	defer r.mu.RUnlock()
	status := r.leaseStatusForRequestRLocked(ctx, now, ts)
	if _, err := r.isDestroyedRLocked(); err != nil {
		return err
	} else if err := r.checkSpanInRangeRLocked(ctx, rSpan); err != nil {
		return err
	} else if err := r.checkTSAboveGCThresholdRLocked(ts, status, false /* isAdmin */); err != nil {
		return err
	} else if r.requiresExpiringLeaseRLocked() {
		// Ensure that the range does not require an expiration-based lease. If it
		// does, it will never get closed timestamp updates and the rangefeed will
		// never be able to advance its resolved timestamp.
		return errors.New("expiration-based leases are incompatible with rangefeeds")
	}
	return nil
}

// checkSpanInRangeRLocked returns an error if a request (identified by its
// key span) can not be run on the replica.
func (r *Replica) checkSpanInRangeRLocked(ctx context.Context, rspan roachpb.RSpan) error {
	desc := r.mu.state.Desc
	if desc.ContainsKeyRange(rspan.Key, rspan.EndKey) {
		return nil
	}
	return roachpb.NewRangeKeyMismatchError(
		ctx, rspan.Key.AsRawKey(), rspan.EndKey.AsRawKey(), desc, r.mu.state.Lease,
	)
}

// checkTSAboveGCThresholdRLocked returns an error if a request (identified by
// its read timestamp) wants to read below the range's GC threshold.
func (r *Replica) checkTSAboveGCThresholdRLocked(
	ts hlc.Timestamp, st kvserverpb.LeaseStatus, isAdmin bool,
) error {
	threshold := r.getImpliedGCThresholdRLocked(st, isAdmin)
	if threshold.Less(ts) {
		return nil
	}
	return &roachpb.BatchTimestampBeforeGCError{
		Timestamp: ts,
		Threshold: threshold,
	}
}

// shouldWaitForPendingMergeRLocked determines whether the given batch request
// should wait for an on-going merge to conclude before being allowed to proceed.
// If not, an error is returned to prevent the request from proceeding until the
// merge completes.
func (r *Replica) shouldWaitForPendingMergeRLocked(
	ctx context.Context, ba *roachpb.BatchRequest,
) error {
	if !r.mergeInProgressRLocked() {
		log.Fatal(ctx, "programming error: shouldWaitForPendingMergeRLocked should"+
			" only be called when a range merge is in progress")
		return nil
	}

	// The replica is being merged into its left-hand neighbor. This request
	// cannot proceed until the merge completes, signaled by the closing of the
	// channel.
	//
	// It is very important that this check occur after we have acquired latches
	// from the spanlatch manager. Only after we acquire these latches are we
	// guaranteed that we're not racing with a Subsume command. (Subsume
	// commands declare a conflict with all other commands.) It is also
	// important that this check occur after we have verified that this replica
	// is the leaseholder. Only the leaseholder will have its merge complete
	// channel set.

	// However, we do permit exactly two forms of requests when a range is in
	// the process of being merged into its left-hand neighbor.
	//
	// The first request type that we allow on the RHS of a merge after it has
	// entered its critical phase is a Subsume request. This sounds backwards,
	// but it is necessary to avoid deadlock. While normally a Subsume request
	// will trigger the installation of a mergeComplete channel after it is
	// executed, it may sometimes execute after the mergeComplete channel has
	// been installed. Consider the case where the RHS replica acquires a new
	// lease after the merge transaction deletes its local range descriptor but
	// before the Subsume command is sent. The lease acquisition request will
	// notice the intent on the local range descriptor and install a
	// mergeComplete channel. If the forthcoming Subsume blocked on that
	// channel, the merge transaction would deadlock.
	//
	// This exclusion admits a small race condition. If a Subsume request is
	// sent to the right-hand side of a merge, outside of a merge transaction,
	// after the merge has committed but before the RHS has noticed that the
	// merge has committed, the request may return stale data. Since the merge
	// has committed, the LHS may have processed writes to the keyspace
	// previously owned by the RHS that the RHS is unaware of. This window
	// closes quickly, as the RHS will soon notice the merge transaction has
	// committed and mark itself as destroyed, which prevents it from serving
	// all traffic, including Subsume requests.
	//
	// In our current, careful usage of Subsume, this race condition is
	// irrelevant. Subsume is only sent from within a merge transaction, and
	// merge transactions read the RHS descriptor at the beginning of the
	// transaction to verify that it has not already been merged away.
	if ba.IsSingleSubsumeRequest() {
		return nil
	}
	// The second request type that we allow on the RHS of a merge after it has
	// entered its critical phase is a Refresh request, but only one issued by
	// the active range merge transaction itself, targeting the RHS's local
	// range descriptor. This is necessary to allow the merge transaction to
	// have its write timestamp be bumped and still commit without retrying. In
	// such cases, the transaction must refresh its reads, including its
	// original read on the RHS's local range descriptor. If we were to block
	// this refresh on the frozen RHS range, the merge would deadlock.
	//
	// On the surface, it seems unsafe to permit Refresh requests on an already
	// subsumed RHS range, because the Refresh's effect on the timestamp cache
	// will never make it to the LHS leaseholder. This risks the future joint
	// range serving a write that invalidates the Refresh. However, in this
	// specific situation, we can be sure that such a serializability violation
	// will not occur because the Range merge also writes to (deletes) this key.
	// This means that if the Range merge transaction commits, its intent on the
	// key will be resolved to the timestamp of the refresh and no future write
	// will ever be able to violate the refresh. Conversely, if the Range merge
	// transaction does not commit, then the merge will fail and the update to
	// the RHS's timestamp cache will not be lost (not that this particularly
	// matters in cases of aborted transactions).
	//
	// The same line of reasoning as the one above has motivated us to explore
	// removing keys from a transaction's refresh spans when they are written to
	// by the transaction, as the intents written by the transaction act as a
	// form of pessimistic lock that obviate the need for the optimistic
	// refresh. Such an improvement would eliminate the need for this special
	// case, but until we generalize the mechanism to prune refresh spans based
	// on intent spans, we're forced to live with this.
	if ba.Txn != nil && ba.Txn.ID == r.mu.mergeTxnID {
		if ba.IsSingleRefreshRequest() {
			desc := r.descRLocked()
			descKey := keys.RangeDescriptorKey(desc.StartKey)
			if ba.Requests[0].GetRefresh().Key.Equal(descKey) {
				return nil
			}
		}
	}

	// Otherwise, the request must wait. We can't wait for the merge to complete
	// here, though. The replica might need to respond to a Subsume request in
	// order for the merge to complete, and blocking here would force that
	// Subsume request to sit in hold its latches forever, deadlocking the
	// merge. Instead, we release the latches we acquired above and return a
	// MergeInProgressError. The store will catch that error and resubmit the
	// request after mergeCompleteCh closes. See #27442 for the full context.
	return &roachpb.MergeInProgressError{}
}

// isNewerThanSplit is a helper used in split(Pre|Post)Apply to
// determine whether the Replica on the right hand side of the split must
// have been removed from this store after the split. There is one
// false negative where false will be returned but the hard state may
// be due to a newer replica which is outlined below. It should be safe.
//
// TODO(ajwerner): Ideally if this store had ever learned that the replica
// created by the split were removed it would not forget that fact.
// There exists one edge case where the store may learn that it should house
// a replica of the same range with a higher replica ID and then forget.
// If the first raft message this store ever receives for the this range
// contains a replica ID higher than the replica ID in the split trigger
// then an in-memory replica at that higher replica ID will be created and
// no tombstone at a lower replica ID will be written. If the server then
// crashes it will forget that it had ever been the higher replica ID. The
// server may then proceed to process the split and initialize a replica at
// the replica ID implied by the split. This is potentially problematic as
// the replica may have voted as this higher replica ID and when it rediscovers
// the higher replica ID it will delete all of the state corresponding to the
// older replica ID including its hard state which may have been synthesized
// with votes as the newer replica ID. This case tends to be handled safely
// in practice because the replica should only be receiving messages as the
// newer replica ID after it has been added to the range. Prior to learner
// replicas we would only add a store to a range after we've successfully
// applied a pre-emptive snapshot. If the store were to split between the
// preemptive snapshot and the addition then the addition would fail due to
// the conditional put logic. If the store were to then enable learners then
// we're still okay because we won't promote a learner unless we succeed in
// sending a learner snapshot. If we fail to send the replica never becomes
// a voter then its votes don't matter and are safe to discard.
//
// Despite the safety due to the change replicas protocol explained above
// it'd be good to know for sure that a replica ID for a range on a store
// is always monotonically increasing, even across restarts.
//
// See TestProcessSplitAfterRightHandSideHasBeenRemoved.
func (r *Replica) isNewerThanSplit(split *roachpb.SplitTrigger) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isNewerThanSplitRLocked(split)
}

func (r *Replica) isNewerThanSplitRLocked(split *roachpb.SplitTrigger) bool {
	rightDesc, _ := split.RightDesc.GetReplicaDescriptor(r.StoreID())
	// If we have written a tombstone for this range then we know that the RHS
	// must have already been removed at the split replica ID.
	return r.mu.tombstoneMinReplicaID != 0 ||
		// If the first raft message we received for the RHS range was for a replica
		// ID which is above the replica ID of the split then we would not have
		// written a tombstone but we will have a replica ID that will exceed the
		// split replica ID.
		r.mu.replicaID > rightDesc.ReplicaID
}

// WatchForMerge is like maybeWatchForMergeLocked, except it expects a merge to
// be in progress and returns an error if one is not.
//
// See docs/tech-notes/range-merges.md.
func (r *Replica) WatchForMerge(ctx context.Context) error {
	ok, err := r.maybeWatchForMerge(ctx)
	if err != nil {
		return err
	} else if !ok {
		return errors.AssertionFailedf("range merge unexpectedly not in-progress")
	}
	return nil
}

// maybeWatchForMergeLocked checks whether a merge of this replica into its left
// neighbor is in its critical phase and, if so, arranges to block all requests
// until the merge completes. Returns a boolean indicating whether a merge was
// found to be in progress.
//
// See docs/tech-notes/range-merges.md.
func (r *Replica) maybeWatchForMerge(ctx context.Context) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maybeWatchForMergeLocked(ctx)
}

func (r *Replica) maybeWatchForMergeLocked(ctx context.Context) (bool, error) {
	// Checking for a deletion intent on the local range descriptor, which
	// indicates that a merge is in progress and this range is currently in its
	// critical phase of being subsumed by its left-hand side neighbor. Read
	// inconsistently at the maximum timestamp to ensure that we see an intent
	// if one exists, regardless of what timestamp it is written at.
	desc := r.descRLocked()
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	_, intent, err := storage.MVCCGet(ctx, r.Engine(), descKey, hlc.MaxTimestamp,
		storage.MVCCGetOptions{Inconsistent: true})
	if err != nil {
		return false, err
	} else if intent == nil {
		return false, nil
	}
	val, _, err := storage.MVCCGetAsTxn(
		ctx, r.Engine(), descKey, intent.Txn.WriteTimestamp, intent.Txn)
	if err != nil {
		return false, err
	} else if val != nil {
		return false, nil
	}

	// At this point, we know we have a deletion intent on our range descriptor.
	// That means a merge is in progress. Block all commands until we can
	// retrieve an updated range descriptor from meta2, which will indicate
	// whether the merge succeeded or not.

	mergeCompleteCh := make(chan struct{})
	if r.mu.mergeComplete != nil {
		// Another request already noticed the merge, installed a mergeComplete
		// channel, and launched a goroutine to watch for the merge's completion.
		// Nothing more to do.
		return true, nil
	}
	r.mu.mergeComplete = mergeCompleteCh
	r.mu.mergeTxnID = intent.Txn.ID
	// The RHS of a merge is not permitted to quiesce while a mergeComplete
	// channel is installed. (If the RHS is quiescent when the merge commits, any
	// orphaned followers would fail to queue themselves for GC.) Unquiesce the
	// range in case it managed to quiesce between when the Subsume request
	// arrived and now, which is rare but entirely legal.
	r.unquiesceLocked()

	taskCtx := r.AnnotateCtx(context.Background())
	err = r.store.stopper.RunAsyncTask(taskCtx, "wait-for-merge", func(ctx context.Context) {
		var pushTxnRes *roachpb.PushTxnResponse
		for retry := retry.Start(base.DefaultRetryOptions()); retry.Next(); {
			// Wait for the merge transaction to complete by attempting to push it. We
			// don't want to accidentally abort the merge transaction, so we use the
			// minimum transaction priority. Note that a push type of
			// roachpb.PUSH_TOUCH, though it might appear more semantically correct,
			// returns immediately and causes us to spin hot, whereas
			// roachpb.PUSH_ABORT efficiently blocks until the transaction completes.
			b := &kv.Batch{}
			b.Header.Timestamp = r.Clock().Now()
			b.AddRawRequest(&roachpb.PushTxnRequest{
				RequestHeader: roachpb.RequestHeader{Key: intent.Txn.Key},
				PusherTxn: roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{Priority: enginepb.MinTxnPriority},
				},
				PusheeTxn: intent.Txn,
				PushType:  roachpb.PUSH_ABORT,
			})
			if err := r.store.DB().Run(ctx, b); err != nil {
				select {
				case <-r.store.stopper.ShouldQuiesce():
					// The server is shutting down. The error while pushing the
					// transaction was probably caused by the shutdown, so ignore it.
					return
				default:
					log.Warningf(ctx, "error while watching for merge to complete: PushTxn: %+v", err)
					// We can't safely unblock traffic until we can prove that the merge
					// transaction is committed or aborted. Nothing to do but try again.
					continue
				}
			}
			pushTxnRes = b.RawResponse().Responses[0].GetInner().(*roachpb.PushTxnResponse)
			break
		}

		var mergeCommitted bool
		switch pushTxnRes.PusheeTxn.Status {
		case roachpb.PENDING, roachpb.STAGING:
			log.Fatalf(ctx, "PushTxn returned while merge transaction %s was still %s",
				intent.Txn.ID.Short(), pushTxnRes.PusheeTxn.Status)
		case roachpb.COMMITTED:
			// If PushTxn claims that the transaction committed, then the transaction
			// definitely committed.
			mergeCommitted = true
		case roachpb.ABORTED:
			// If PushTxn claims that the transaction aborted, it's not a guarantee
			// that the transaction actually aborted. It could also mean that the
			// transaction completed, resolved its intents, and GC'd its transaction
			// record before our PushTxn arrived. To figure out what happened, we
			// need to look in meta2.
			var getRes *roachpb.GetResponse
			for retry := retry.Start(base.DefaultRetryOptions()); retry.Next(); {
				metaKey := keys.RangeMetaKey(desc.EndKey)
				res, pErr := kv.SendWrappedWith(ctx, r.store.DB().NonTransactionalSender(), roachpb.Header{
					// Use READ_UNCOMMITTED to avoid trying to resolve intents, since
					// resolving those intents might involve sending requests to this
					// range, and that could deadlock. See the comment on
					// TestStoreRangeMergeConcurrentSplit for details.
					ReadConsistency: roachpb.READ_UNCOMMITTED,
				}, &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: metaKey.AsRawKey()},
				})
				if pErr != nil {
					select {
					case <-r.store.stopper.ShouldQuiesce():
						// The server is shutting down. The error while fetching the range
						// descriptor was probably caused by the shutdown, so ignore it.
						return
					default:
						log.Warningf(ctx, "error while watching for merge to complete: Get %s: %s", metaKey, pErr)
						// We can't safely unblock traffic until we can prove that the merge
						// transaction is committed or aborted. Nothing to do but try again.
						continue
					}
				}
				getRes = res.(*roachpb.GetResponse)
				break
			}
			if getRes.Value == nil {
				// A range descriptor with our end key is no longer present in meta2, so
				// the merge must have committed.
				mergeCommitted = true
			} else {
				// A range descriptor with our end key is still present in meta2. The
				// merge committed iff that range descriptor has a different range ID.
				var meta2Desc roachpb.RangeDescriptor
				if err := getRes.Value.GetProto(&meta2Desc); err != nil {
					log.Fatalf(ctx, "error while watching for merge to complete: "+
						"unmarshaling meta2 range descriptor: %s", err)
				}
				if meta2Desc.RangeID != r.RangeID {
					mergeCommitted = true
				}
			}
		}
		r.raftMu.Lock()
		r.readOnlyCmdMu.Lock()
		r.mu.Lock()
		if mergeCommitted && r.mu.destroyStatus.IsAlive() {
			// The merge committed but the left-hand replica on this store hasn't
			// subsumed this replica yet. Mark this replica as destroyed so it
			// doesn't serve requests when we close the mergeCompleteCh below.
			r.mu.destroyStatus.Set(roachpb.NewRangeNotFoundError(r.RangeID, r.store.StoreID()), destroyReasonMergePending)
		}
		// Unblock pending requests. If the merge committed, the requests will
		// notice that the replica has been destroyed and return an appropriate
		// error. If the merge aborted, the requests will be handled normally.
		r.mu.mergeComplete = nil
		r.mu.mergeTxnID = uuid.UUID{}
		close(mergeCompleteCh)
		r.mu.Unlock()
		r.readOnlyCmdMu.Unlock()
		r.raftMu.Unlock()
	})
	if errors.Is(err, stop.ErrUnavailable) {
		// We weren't able to launch a goroutine to watch for the merge's completion
		// because the server is shutting down. Normally failing to launch the
		// watcher goroutine would wedge pending requests on the replica's
		// mergeComplete channel forever, but since we're shutting down those
		// requests will get dropped and retried on another node. Suppress the error.
		err = nil
	}
	return true, err
}

// maybeTransferRaftLeadershipToLeaseholderLocked attempts to transfer the
// leadership away from this node to the leaseholder, if this node is the
// current raft leader but not the leaseholder. We don't attempt to transfer
// leadership if the leaseholder is behind on applying the log.
//
// We like it when leases and raft leadership are collocated because that
// facilitates quick command application (requests generally need to make it to
// both the lease holder and the raft leader before being applied by other
// replicas).
func (r *Replica) maybeTransferRaftLeadershipToLeaseholderLocked(ctx context.Context) {
	if r.store.TestingKnobs().DisableLeaderFollowsLeaseholder {
		return
	}
	status := r.leaseStatusAtRLocked(ctx, r.Clock().NowAsClockTimestamp())
	if !status.IsValid() || status.OwnedBy(r.StoreID()) {
		return
	}
	raftStatus := r.raftStatusRLocked()
	if raftStatus == nil || raftStatus.RaftState != raft.StateLeader {
		return
	}
	lhReplicaID := uint64(status.Lease.Replica.ReplicaID)
	lhProgress, ok := raftStatus.Progress[lhReplicaID]
	if (ok && lhProgress.Match >= raftStatus.Commit) || r.store.IsDraining() {
		log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", lhReplicaID)
		r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
		r.mu.internalRaftGroup.TransferLeader(lhReplicaID)
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
		errors.Errorf("replica %d not present in %v, %v",
			replicaID, fallback, r.mu.state.Desc.Replicas())
}

// checkIfTxnAborted checks the txn AbortSpan for the given
// transaction. In case the transaction has been aborted, return a
// transaction abort error.
func checkIfTxnAborted(
	ctx context.Context, rec batcheval.EvalContext, reader storage.Reader, txn roachpb.Transaction,
) *roachpb.Error {
	var entry roachpb.AbortSpanEntry
	aborted, err := rec.AbortSpan().Get(ctx, reader, txn.ID, &entry)
	if err != nil {
		return roachpb.NewError(roachpb.NewReplicaCorruptionError(
			errors.Wrap(err, "could not read from AbortSpan")))
	}
	if aborted {
		// We hit the cache, so let the transaction restart.
		log.VEventf(ctx, 1, "found AbortSpan entry for %s with priority %d",
			txn.ID.Short(), entry.Priority)
		newTxn := txn.Clone()
		if entry.Priority > newTxn.Priority {
			newTxn.Priority = entry.Priority
		}
		newTxn.Status = roachpb.ABORTED
		return roachpb.NewErrorWithTxn(
			roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_ABORT_SPAN), newTxn)
	}
	return nil
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

// GetExternalStorage returns an ExternalStorage object, based on
// information parsed from a URI, stored in `dest`.
func (r *Replica) GetExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	return r.store.cfg.ExternalStorage(ctx, dest)
}

// GetExternalStorageFromURI returns an ExternalStorage object, based on the given URI.
func (r *Replica) GetExternalStorageFromURI(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	return r.store.cfg.ExternalStorageFromURI(ctx, uri, user)
}

func (r *Replica) markSystemConfigGossipSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.failureToGossipSystemConfig = false
}

func (r *Replica) markSystemConfigGossipFailed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.failureToGossipSystemConfig = true
}

func init() {
	tracing.RegisterTagRemapping("r", "range")
}
