// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/tracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.etcd.io/etcd/raft/v3"
)

const (
	splitQueueThrottleDuration = 5 * time.Second
	mergeQueueThrottleDuration = 5 * time.Second
)

// newReplica constructs a new Replica. If the desc is initialized, the store
// must be present in it and the corresponding replica descriptor must have
// replicaID as its ReplicaID.
func newReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	repl := newUnloadedReplica(ctx, desc, store, replicaID)
	repl.raftMu.Lock()
	defer repl.raftMu.Unlock()
	repl.mu.Lock()
	defer repl.mu.Unlock()
	if err := repl.loadRaftMuLockedReplicaMuLocked(desc); err != nil {
		return nil, err
	}
	return repl, nil
}

// newUnloadedReplica partially constructs a replica. The primary reason this
// function exists separately from Replica.loadRaftMuLockedReplicaMuLocked() is
// to avoid attempting to fully constructing a Replica prior to proving that it
// can exist during the delicate synchronization dance that occurs in
// Store.tryGetOrCreateReplica(). A Replica returned from this function must not
// be used in any way until it's load() method has been called.
func newUnloadedReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, store *Store, replicaID roachpb.ReplicaID,
) *Replica {
	if replicaID == 0 {
		log.Fatalf(ctx, "cannot construct a replica for range %d with a 0 replica ID", desc.RangeID)
	}
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        desc.RangeID,
		replicaID:      replicaID,
		creationTime:   timeutil.Now(),
		store:          store,
		abortSpan:      abortspan.New(desc.RangeID),
		concMgr: concurrency.NewManager(concurrency.Config{
			NodeDesc:          store.nodeDesc,
			RangeDesc:         desc,
			Settings:          store.ClusterSettings(),
			DB:                store.DB(),
			Clock:             store.Clock(),
			Stopper:           store.Stopper(),
			IntentResolver:    store.intentResolver,
			TxnWaitMetrics:    store.txnWaitMetrics,
			SlowLatchGauge:    store.metrics.SlowLatchRequests,
			DisableTxnPushing: store.TestingKnobs().DontPushOnWriteIntentError,
			TxnWaitKnobs:      store.TestingKnobs().TxnWaitKnobs,
		}),
	}
	r.mu.pendingLeaseRequest = makePendingLeaseRequest(r)
	r.mu.stateLoader = stateloader.Make(desc.RangeID)
	r.mu.quiescent = true
	r.mu.conf = store.cfg.DefaultSpanConfig
	split.Init(&r.loadBasedSplitter, rand.Intn, func() float64 {
		return float64(SplitByLoadQPSThreshold.Get(&store.cfg.Settings.SV))
	}, func() time.Duration {
		return kvserverbase.SplitByLoadMergeDelay.Get(&store.cfg.Settings.SV)
	})
	r.mu.proposals = map[kvserverbase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]replicaChecksum{}
	r.mu.proposalBuf.Init((*replicaProposer)(r), tracker.NewLockfreeTracker(), r.Clock(), r.ClusterSettings())
	r.mu.proposalBuf.testing.allowLeaseProposalWhenNotLeader = store.cfg.TestingKnobs.AllowLeaseRequestProposalsWhenNotLeader
	r.mu.proposalBuf.testing.dontCloseTimestamps = store.cfg.TestingKnobs.DontCloseTimestamps
	r.mu.proposalBuf.testing.submitProposalFilter = store.cfg.TestingKnobs.TestingProposalSubmitFilter

	if leaseHistoryMaxEntries > 0 {
		r.leaseHistory = newLeaseHistory()
	}
	if store.cfg.StorePool != nil {
		r.leaseholderStats = newReplicaStats(store.Clock(), store.cfg.StorePool.getNodeLocalityString)
	}
	// Pass nil for the localityOracle because we intentionally don't track the
	// origin locality of write load.
	r.writeStats = newReplicaStats(store.Clock(), nil)

	// Init rangeStr with the range ID.
	r.rangeStr.store(replicaID, &roachpb.RangeDescriptor{RangeID: desc.RangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	r.raftCtx = logtags.AddTag(r.AnnotateCtx(context.Background()), "raft", nil /* value */)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTag("@", fmt.Sprintf("%x", unsafe.Pointer(r)))
	r.raftMu.stateLoader = stateloader.Make(desc.RangeID)

	r.splitQueueThrottle = util.Every(splitQueueThrottleDuration)
	r.mergeQueueThrottle = util.Every(mergeQueueThrottleDuration)

	onTrip := func() {
		telemetry.Inc(telemetryTripAsync)
		r.store.Metrics().ReplicaCircuitBreakerCumTripped.Inc(1)
		store.Metrics().ReplicaCircuitBreakerCurTripped.Inc(1)
	}
	onReset := func() {
		store.Metrics().ReplicaCircuitBreakerCurTripped.Dec(1)
	}
	r.breaker = newReplicaCircuitBreaker(
		store.cfg.Settings, store.stopper, r.AmbientContext, r, onTrip, onReset,
	)
	return r
}

// setStartKeyLocked sets r.startKey. Note that this field has special semantics
// described on its comment. Callers to this method are initializing an
// uninitialized Replica and hold Replica.mu.
func (r *Replica) setStartKeyLocked(startKey roachpb.RKey) {
	r.mu.AssertHeld()
	if r.startKey != nil {
		log.Fatalf(
			r.AnnotateCtx(context.Background()),
			"start key written twice: was %s, now %s", r.startKey, startKey,
		)
	}
	r.startKey = startKey
}

// loadRaftMuLockedReplicaMuLocked will load the state of the replica from disk.
// If desc is initialized, the Replica will be initialized when this method
// returns. An initialized Replica may not be reloaded. If this method is called
// with an uninitialized desc it may be called again later with an initialized
// desc.
//
// This method is called in three places:
//
//  1) newReplica - used when the store is initializing and during testing
//  2) tryGetOrCreateReplica - see newUnloadedReplica
//  3) splitPostApply - this call initializes a previously uninitialized Replica.
//
func (r *Replica) loadRaftMuLockedReplicaMuLocked(desc *roachpb.RangeDescriptor) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	} else if r.replicaID == 0 {
		// NB: This is just a defensive check as r.mu.replicaID should never be 0.
		log.Fatalf(ctx, "r%d: cannot initialize replica without a replicaID", desc.RangeID)
	}
	if desc.IsInitialized() {
		r.setStartKeyLocked(desc.StartKey)
	}

	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	var err error
	if r.mu.state, err = r.mu.stateLoader.Load(ctx, r.Engine(), desc); err != nil {
		return err
	}
	r.mu.lastIndex, err = r.mu.stateLoader.LoadLastIndex(ctx, r.Engine())
	if err != nil {
		return err
	}
	r.mu.lastTerm = invalidLastTerm

	// Ensure that we're not trying to load a replica with a different ID than
	// was used to construct this Replica.
	replicaID := r.replicaID
	if replicaDesc, found := r.mu.state.Desc.GetReplicaDescriptor(r.StoreID()); found {
		replicaID = replicaDesc.ReplicaID
	} else if desc.IsInitialized() {
		log.Fatalf(ctx, "r%d: cannot initialize replica which is not in descriptor %v", desc.RangeID, desc)
	}
	if r.replicaID != replicaID {
		log.Fatalf(ctx, "attempting to initialize a replica which has ID %d with ID %d",
			r.replicaID, replicaID)
	}

	r.setDescLockedRaftMuLocked(ctx, desc)

	// Only do this if there was a previous lease. This shouldn't be important
	// to do but consider that the first lease which is obtained is back-dated
	// to a zero start timestamp (and this de-flakes some tests). If we set the
	// min proposed TS here, this lease could not be renewed (by the semantics
	// of minLeaseProposedTS); and since minLeaseProposedTS is copied on splits,
	// this problem would multiply to a number of replicas at cluster bootstrap.
	// Instead, we make the first lease special (which is OK) and the problem
	// disappears.
	if r.mu.state.Lease.Sequence > 0 {
		r.mu.minLeaseProposedTS = r.Clock().NowAsClockTimestamp()
	}

	ssBase := r.Engine().GetAuxiliaryDir()
	if r.raftMu.sideloaded, err = newDiskSideloadStorage(
		r.store.cfg.Settings,
		desc.RangeID,
		replicaID,
		ssBase,
		r.store.limiters.BulkIOWriteRate,
		r.store.engine,
	); err != nil {
		return errors.Wrap(err, "while initializing sideloaded storage")
	}
	r.assertStateRaftMuLockedReplicaMuRLocked(ctx, r.store.Engine())

	r.sideTransportClosedTimestamp.init(r.store.cfg.ClosedTimestampReceiver, desc.RangeID)

	return nil
}

// IsInitialized is true if we know the metadata of this replica's range, either
// because we created it or we have received an initial snapshot from another
// node. It is false when a replica has been created in response to an incoming
// message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isInitializedRLocked()
}

// TenantID returns the associated tenant ID and a boolean to indicate that it
// is valid. It will be invalid only if the replica is not initialized.
func (r *Replica) TenantID() (roachpb.TenantID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getTenantIDRLocked()
}

func (r *Replica) getTenantIDRLocked() (roachpb.TenantID, bool) {
	return r.mu.tenantID, r.mu.tenantID != (roachpb.TenantID{})
}

// isInitializedRLocked is true if we know the metadata of this range, either
// because we created it or we have received an initial snapshot from
// another node. It is false when a range has been created in response
// to an incoming message but we are waiting for our initial snapshot.
// isInitializedLocked requires that the replica lock is held.
func (r *Replica) isInitializedRLocked() bool {
	return r.mu.state.Desc.IsInitialized()
}

// maybeInitializeRaftGroup check whether the internal Raft group has
// not yet been initialized. If not, it is created and set to campaign
// if this replica is the most recent owner of the range lease.
func (r *Replica) maybeInitializeRaftGroup(ctx context.Context) {
	r.mu.RLock()
	// If this replica hasn't initialized the Raft group, create it and
	// unquiesce and wake the leader to ensure the replica comes up to date.
	initialized := r.mu.internalRaftGroup != nil
	// If this replica has been removed or is in the process of being removed
	// then it'll never handle any raft events so there's no reason to initialize
	// it now.
	removed := !r.mu.destroyStatus.IsAlive()
	r.mu.RUnlock()
	if initialized || removed {
		return
	}

	// Acquire raftMu, but need to maintain lock ordering (raftMu then mu).
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we raced on checking the destroyStatus above that's fine as
	// the below withRaftGroupLocked will no-op.
	if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil && !errors.Is(err, errRemoved) {
		log.VErrEventf(ctx, 1, "unable to initialize raft group: %s", err)
	}
}

// setDescRaftMuLocked atomically sets the replica's descriptor. It requires raftMu to be
// locked.
func (r *Replica) setDescRaftMuLocked(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setDescLockedRaftMuLocked(ctx, desc)
}

func (r *Replica) setDescLockedRaftMuLocked(ctx context.Context, desc *roachpb.RangeDescriptor) {
	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}
	if r.mu.state.Desc.IsInitialized() &&
		!r.mu.state.Desc.StartKey.Equal(desc.StartKey) {
		log.Fatalf(ctx, "attempted to change replica's start key from %s to %s",
			r.mu.state.Desc.StartKey, desc.StartKey)
	}

	// NB: It might be nice to assert that the current replica exists in desc
	// however we allow it to not be present for three reasons:
	//
	//   1) When removing the current replica we update the descriptor to the point
	//      of removal even though we will delete the Replica's data in the same
	//      batch. We could avoid setting the local descriptor in this case.
	//   2) When the DisableEagerReplicaRemoval testing knob is enabled. We
	//      could remove all tests which utilize this behavior now that there's
	//      no other mechanism for range state which does not contain the current
	//      store to exist on disk.
	//   3) Various unit tests do not provide a valid descriptor.
	replDesc, found := desc.GetReplicaDescriptor(r.StoreID())
	if found && replDesc.ReplicaID != r.replicaID {
		log.Fatalf(ctx, "attempted to change replica's ID from %d to %d",
			r.replicaID, replDesc.ReplicaID)
	}

	// Initialize the tenant. The must be the first time that the descriptor has
	// been initialized. Note that the desc.StartKey never changes throughout the
	// life of a range.
	if desc.IsInitialized() && r.mu.tenantID == (roachpb.TenantID{}) {
		_, tenantID, err := keys.DecodeTenantPrefix(desc.StartKey.AsRawKey())
		if err != nil {
			log.Fatalf(ctx, "failed to decode tenant prefix from key for "+
				"replica %v: %v", r, err)
		}
		r.mu.tenantID = tenantID
		r.tenantMetricsRef = r.store.metrics.acquireTenant(tenantID)
		if tenantID != roachpb.SystemTenantID {
			r.tenantLimiter = r.store.tenantRateLimiters.GetTenant(ctx, tenantID, r.store.stopper.ShouldQuiesce())
		}
	}

	// Determine if a new replica was added. This is true if the new max replica
	// ID is greater than the old max replica ID.
	oldMaxID := maxReplicaIDOfAny(r.mu.state.Desc)
	newMaxID := maxReplicaIDOfAny(desc)
	if newMaxID > oldMaxID {
		r.mu.lastReplicaAdded = newMaxID
		r.mu.lastReplicaAddedTime = timeutil.Now()
	} else if r.mu.lastReplicaAdded > newMaxID {
		// The last replica added was removed.
		r.mu.lastReplicaAdded = 0
		r.mu.lastReplicaAddedTime = time.Time{}
	}

	r.rangeStr.store(r.replicaID, desc)
	r.connectionClass.set(rpc.ConnectionClassForKey(desc.StartKey))
	r.concMgr.OnRangeDescUpdated(desc)
	r.mu.state.Desc = desc

	// Prioritize the NodeLiveness Range in the Raft scheduler above all other
	// Ranges to ensure that liveness never sees high Raft scheduler latency.
	if bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix) {
		r.store.scheduler.SetPriorityID(desc.RangeID)
	}
}
