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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	splitQueueThrottleDuration = 5 * time.Second
	mergeQueueThrottleDuration = 5 * time.Second
)

// loadedReplicaState represents the state of a Replica loaded from storage, and
// is used to initialize the in-memory Replica instance.
type loadedReplicaState struct {
	replicaID roachpb.ReplicaID
	hardState raftpb.HardState
	lastIndex uint64
	replState kvserverpb.ReplicaState
}

// loadReplicaState loads the state necessary to create a Replica from storage.
func loadReplicaState(
	ctx context.Context,
	eng storage.Reader,
	desc *roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
) (loadedReplicaState, error) {
	sl := stateloader.Make(desc.RangeID)
	id, found, err := sl.LoadRaftReplicaID(ctx, eng)
	if err != nil {
		return loadedReplicaState{}, err
	} else if !found {
		return loadedReplicaState{}, errors.AssertionFailedf(
			"r%d: RaftReplicaID not found", desc.RangeID)
	} else if loaded := id.ReplicaID; loaded != replicaID {
		return loadedReplicaState{}, errors.AssertionFailedf(
			"r%d: loaded RaftReplicaID %d does not match %d", desc.RangeID, loaded, replicaID)
	}

	ls := loadedReplicaState{replicaID: replicaID}
	if ls.hardState, err = sl.LoadHardState(ctx, eng); err != nil {
		return loadedReplicaState{}, err
	}
	if ls.lastIndex, err = sl.LoadLastIndex(ctx, eng); err != nil {
		return loadedReplicaState{}, err
	}
	if ls.replState, err = sl.Load(ctx, eng, desc); err != nil {
		return loadedReplicaState{}, err
	}
	return ls, nil
}

// check makes sure that the replica invariants hold for the loaded state.
func (r loadedReplicaState) check(storeID roachpb.StoreID) error {
	desc := r.replState.Desc
	if id := r.replicaID; id == 0 {
		return errors.AssertionFailedf("r%d: replicaID is 0", desc.RangeID)
	}

	if !desc.IsInitialized() {
		// An uninitialized replica must have an empty HardState.Commit at all
		// times. Failure to maintain this invariant indicates corruption. And yet,
		// we have observed this in the wild. See #40213.
		if hs := r.hardState; hs.Commit != 0 {
			return errors.AssertionFailedf(
				"r%d/%d: non-zero HardState.Commit on uninitialized replica: %+v", desc.RangeID, r.replicaID, hs)
		}
		return nil
	}
	// desc.IsInitialized() == true

	if replDesc, ok := desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf("%+v does not contain local store s%d", desc, storeID)
	} else if replDesc.ReplicaID != r.replicaID {
		return errors.AssertionFailedf(
			"%+v does not contain replicaID %d for local store s%d", desc, r.replicaID, storeID)
	}
	return nil
}

// loadReplica loads and constructs a Replica, after checking its invariants.
func loadReplica(
	ctx context.Context, store *Store, desc *roachpb.RangeDescriptor, replicaID roachpb.ReplicaID,
) (*Replica, error) {
	state, err := loadReplicaState(ctx, store.engine, desc, replicaID)
	if err != nil {
		return nil, err
	}
	if err := state.check(store.StoreID()); err != nil {
		return nil, err
	}
	return newReplica(ctx, store, state), nil
}

// newReplica constructs a Replica using its state loaded from storage. It may
// return either an initialized, or an uninitialized Replica, depending on the
// loaded state. If the returned replica is uninitialized, it remains such until
// Replica.initRaftMuLockedReplicaMuLocked() is called.
func newReplica(ctx context.Context, store *Store, loaded loadedReplicaState) *Replica {
	state := loaded.replState
	rangeID := state.Desc.RangeID
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		replicaID:      loaded.replicaID,
		startKey:       state.Desc.StartKey,
		creationTime:   timeutil.Now(),
		store:          store,
		abortSpan:      abortspan.New(rangeID),
		concMgr: concurrency.NewManager(concurrency.Config{
			NodeDesc:          store.nodeDesc,
			RangeDesc:         state.Desc,
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
	split.Init(&r.loadBasedSplitter, store.cfg.Settings, split.GlobalRandSource(), func() float64 {
		return float64(SplitByLoadQPSThreshold.Get(&store.cfg.Settings.SV))
	}, func() time.Duration {
		return kvserverbase.SplitByLoadMergeDelay.Get(&store.cfg.Settings.SV)
	}, store.metrics.LoadSplitterMetrics)
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
	}

	r.mu.state = state
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
	r.mu.lastIndex = loaded.lastIndex
	r.mu.lastTerm = invalidLastTerm
	// NB: mutexes don't need to be locked on Replica struct initialization.
	r.setDescLockedRaftMuLocked(r.AnnotateCtx(ctx), state.Desc)

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
		store.engine.GetAuxiliaryDir(),
		store.limiters.BulkIOWriteRate,
		store.engine,
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
// This method is called only in splitPostApply to initialize a previously
// uninitialized Replica.
func (r *Replica) initRaftMuLockedReplicaMuLocked(s loadedReplicaState) error {
	ctx := r.AnnotateCtx(context.TODO())
	desc := s.replState.Desc

	if !desc.IsInitialized() {
		return errors.AssertionFailedf("r%d: cannot load an uninitialized replica", desc.RangeID)
	}
	if r.IsInitialized() {
		return errors.AssertionFailedf("r%d: cannot reinitialize an initialized replica", desc.RangeID)
	} else if r.replicaID == 0 {
		// NB: This is just a defensive check as r.mu.replicaID should never be 0.
		return errors.AssertionFailedf("r%d: cannot initialize replica without a replicaID",
			desc.RangeID)
	}
	r.setStartKeyLocked(desc.StartKey)

	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil

	r.mu.state = s.replState
	r.mu.lastIndex = s.lastIndex
	r.mu.lastTerm = invalidLastTerm

	// Ensure that we're not trying to load a replica with a different ID than
	// was used to construct this Replica.
	var replicaID roachpb.ReplicaID
	if replicaDesc, found := r.mu.state.Desc.GetReplicaDescriptor(r.StoreID()); found {
		replicaID = replicaDesc.ReplicaID
	} else {
		return errors.AssertionFailedf("r%d: cannot initialize replica which is not in descriptor %v",
			desc.RangeID, desc)
	}
	if r.replicaID != replicaID {
		return errors.AssertionFailedf("attempting to initialize a replica which has ID %d with ID %d",
			r.replicaID, replicaID)
	}

	r.setDescLockedRaftMuLocked(ctx, desc)
	r.assertStateRaftMuLockedReplicaMuRLocked(ctx, r.store.Engine())

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
