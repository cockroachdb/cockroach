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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/tracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
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
	"go.etcd.io/raft/v3"
)

const (
	splitQueueThrottleDuration = 5 * time.Second
	mergeQueueThrottleDuration = 5 * time.Second
)

// loadInitializedReplicaForTesting loads and constructs an initialized Replica,
// after checking its invariants.
func loadInitializedReplicaForTesting(
	ctx context.Context, store *Store, desc *roachpb.RangeDescriptor, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	if !desc.IsInitialized() {
		return nil, errors.AssertionFailedf("can not load with uninitialized descriptor: %s", desc)
	}
	state, err := kvstorage.LoadReplicaState(ctx, store.TODOEngine(), store.StoreID(), desc, replicaID)
	if err != nil {
		return nil, err
	}
	return newInitializedReplica(store, state)
}

// newInitializedReplica creates an initialized Replica from its loaded state.
func newInitializedReplica(store *Store, loaded kvstorage.LoadedReplicaState) (*Replica, error) {
	r := newUninitializedReplica(store, loaded.ReplState.Desc.RangeID, loaded.ReplicaID)
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.initRaftMuLockedReplicaMuLocked(loaded); err != nil {
		return nil, err
	}
	return r, nil
}

// newUninitializedReplica constructs an uninitialized Replica with the given
// range/replica ID. The returned replica remains uninitialized until
// Replica.loadRaftMuLockedReplicaMuLocked() is called.
//
// TODO(#94912): we actually have another initialization path which should be
// refactored: Replica.initFromSnapshotLockedRaftMuLocked().
func newUninitializedReplica(
	store *Store, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID,
) *Replica {
	uninitState := stateloader.UninitializedReplicaState(rangeID)
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		replicaID:      replicaID,
		creationTime:   timeutil.Now(),
		store:          store,
		abortSpan:      abortspan.New(rangeID),
		concMgr: concurrency.NewManager(concurrency.Config{
			NodeDesc:          store.nodeDesc,
			RangeDesc:         uninitState.Desc,
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
	r.sideTransportClosedTimestamp.init(store.cfg.ClosedTimestampReceiver, rangeID)

	r.mu.pendingLeaseRequest = makePendingLeaseRequest(r)
	r.mu.stateLoader = stateloader.Make(rangeID)
	r.mu.quiescent = true
	r.mu.conf = store.cfg.DefaultSpanConfig

	r.mu.proposals = map[kvserverbase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]*replicaChecksum{}
	r.mu.proposalBuf.Init((*replicaProposer)(r), tracker.NewLockfreeTracker(), r.Clock(), r.ClusterSettings())
	r.mu.proposalBuf.testing.allowLeaseProposalWhenNotLeader = store.cfg.TestingKnobs.AllowLeaseRequestProposalsWhenNotLeader
	r.mu.proposalBuf.testing.dontCloseTimestamps = store.cfg.TestingKnobs.DontCloseTimestamps
	r.mu.proposalBuf.testing.submitProposalFilter = store.cfg.TestingKnobs.TestingProposalSubmitFilter

	if leaseHistoryMaxEntries > 0 {
		r.leaseHistory = newLeaseHistory()
	}

	if store.cfg.StorePool != nil {
		r.loadStats = load.NewReplicaLoad(store.Clock(), store.cfg.StorePool.GetNodeLocalityString)
		split.Init(
			&r.loadBasedSplitter,
			newReplicaSplitConfig(store.ClusterSettings()),
			store.metrics.LoadSplitterMetrics,
			store.rebalanceObjManager.Objective().ToSplitObjective(),
		)
	}

	// NB: the state will be loaded when the replica gets initialized.
	r.mu.state = uninitState
	r.rangeStr.store(replicaID, uninitState.Desc)
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	r.raftCtx = logtags.AddTag(r.AnnotateCtx(context.Background()), "raft", nil /* value */)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTag("@", fmt.Sprintf("%x", unsafe.Pointer(r)))

	r.raftMu.stateLoader = stateloader.Make(rangeID)
	r.raftMu.sideloaded = logstore.NewDiskSideloadStorage(
		store.cfg.Settings,
		rangeID,
		store.TODOEngine().GetAuxiliaryDir(),
		store.limiters.BulkIOWriteRate,
		store.TODOEngine(),
	)

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

// initRaftMuLockedReplicaMuLocked initializes the Replica using the state
// loaded from storage. Must not be called more than once on a Replica.
//
// This method is called in:
// - loadInitializedReplicaForTesting, to finalize creating an initialized replica;
// - splitPostApply, to initialize a previously uninitialized replica.
func (r *Replica) initRaftMuLockedReplicaMuLocked(s kvstorage.LoadedReplicaState) error {
	desc := s.ReplState.Desc
	// Ensure that the loaded state corresponds to the same replica.
	if desc.RangeID != r.RangeID || s.ReplicaID != r.replicaID {
		return errors.AssertionFailedf(
			"%s: trying to init with other replica's state r%d/%d", r, desc.RangeID, s.ReplicaID)
	}
	// Ensure that we transition to initialized replica, and do it only once.
	if !desc.IsInitialized() {
		return errors.AssertionFailedf("%s: cannot init replica with uninitialized descriptor", r)
	} else if r.IsInitialized() {
		return errors.AssertionFailedf("%s: cannot reinitialize an initialized replica", r)
	}

	r.setStartKeyLocked(desc.StartKey)

	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	r.mu.state = s.ReplState
	r.mu.lastIndexNotDurable = s.LastIndex
	r.mu.lastTermNotDurable = invalidLastTerm

	r.setDescLockedRaftMuLocked(r.AnnotateCtx(context.TODO()), desc)

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

	return nil
}

// initFromSnapshotLockedRaftMuLocked initializes a Replica when applying the
// initial snapshot.
func (r *Replica) initFromSnapshotLockedRaftMuLocked(
	ctx context.Context, desc *roachpb.RangeDescriptor,
) error {
	if !desc.IsInitialized() {
		return errors.AssertionFailedf("initializing replica with uninitialized desc: %s", desc)
	}

	r.setDescLockedRaftMuLocked(ctx, desc)
	r.setStartKeyLocked(desc.StartKey)

	// Unquiesce the replica. We don't allow uninitialized replicas to unquiesce,
	// but now that the replica has been initialized, we unquiesce it as soon as
	// possible. This replica was initialized in response to the reception of a
	// snapshot from another replica. This means that the other replica is not
	// quiesced, so we don't need to campaign or wake the leader. We just want
	// to start ticking.
	//
	// NOTE: The fact that this replica is being initialized in response to the
	// receipt of a snapshot means that its r.mu.internalRaftGroup must not be
	// nil.
	//
	// NOTE: Unquiescing the replica here is not strictly necessary. As of the
	// time of writing, this function is only ever called below handleRaftReady,
	// which will always unquiesce any eligible replicas before completing. So in
	// marking this replica as initialized, we have made it eligible to unquiesce.
	// However, there is still a benefit to unquiecing here instead of letting
	// handleRaftReady do it for us. The benefit is that handleRaftReady cannot
	// make assumptions about the state of the other replicas in the range when it
	// unquieces a replica, so when it does so, it also instructs the replica to
	// campaign and to wake the leader (see maybeUnquiesceAndWakeLeaderLocked).
	// We have more information here (see "This means that the other replica ..."
	// above) and can make assumptions about the state of the other replicas in
	// the range, so we can unquiesce without campaigning or waking the leader.
	if !r.maybeUnquiesceWithOptionsLocked(false /* campaignOnWake */) {
		return errors.AssertionFailedf("expected replica %s to unquiesce after initialization", desc)
	}

	return nil
}

// IsInitialized is true if we know the metadata of this replica's range, either
// because we created it or we have received an initial snapshot from another
// node. It is false when a replica has been created in response to an incoming
// message but we are waiting for our initial snapshot.
func (r *Replica) IsInitialized() bool {
	return r.isInitialized.Get()
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
	r.isInitialized.Set(desc.IsInitialized())
	r.connectionClass.set(rpc.ConnectionClassForKey(desc.StartKey))
	r.concMgr.OnRangeDescUpdated(desc)
	r.mu.state.Desc = desc

	// Prioritize the NodeLiveness Range in the Raft scheduler above all other
	// Ranges to ensure that liveness never sees high Raft scheduler latency.
	if bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix) {
		r.store.scheduler.SetPriorityID(desc.RangeID)
	}
}
