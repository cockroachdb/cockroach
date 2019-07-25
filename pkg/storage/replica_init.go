// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/split"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

const (
	splitQueueThrottleDuration = 5 * time.Second
	mergeQueueThrottleDuration = 5 * time.Second
)

func newReplica(rangeID roachpb.RangeID, store *Store) *Replica {
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        rangeID,
		store:          store,
		abortSpan:      abortspan.New(rangeID),
		txnWaitQueue:   txnwait.NewQueue(store),
	}
	r.mu.pendingLeaseRequest = makePendingLeaseRequest(r)
	r.mu.stateLoader = stateloader.Make(rangeID)
	r.mu.quiescent = true
	r.mu.zone = store.cfg.DefaultZoneConfig
	split.Init(&r.loadBasedSplitter, rand.Intn, func() float64 {
		return float64(SplitByLoadQPSThreshold.Get(&store.cfg.Settings.SV))
	})

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
	r.rangeStr.store(0, &roachpb.RangeDescriptor{RangeID: rangeID})
	// Add replica log tag - the value is rangeStr.String().
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	// Add replica pointer value. NB: this was historically useful for debugging
	// replica GC issues, but is a distraction at the moment.
	// r.AmbientContext.AddLogTag("@", fmt.Sprintf("%x", unsafe.Pointer(r)))
	r.raftMu.stateLoader = stateloader.Make(rangeID)

	r.splitQueueThrottle = util.Every(splitQueueThrottleDuration)
	r.mergeQueueThrottle = util.Every(mergeQueueThrottleDuration)
	return r
}

func (r *Replica) init(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.initRaftMuLockedReplicaMuLocked(desc, clock, replicaID)
}

func (r *Replica) initRaftMuLockedReplicaMuLocked(
	desc *roachpb.RangeDescriptor, clock *hlc.Clock, replicaID roachpb.ReplicaID,
) error {
	ctx := r.AnnotateCtx(context.TODO())
	if r.mu.state.Desc != nil && r.isInitializedRLocked() {
		log.Fatalf(ctx, "r%d: cannot reinitialize an initialized replica", desc.RangeID)
	}
	if desc.IsInitialized() && replicaID != 0 {
		return errors.Errorf("replicaID must be 0 when creating an initialized replica")
	}

	r.latchMgr = spanlatch.Make(r.store.stopper, r.store.metrics.SlowLatchRequests)
	r.mu.proposals = map[storagebase.CmdIDKey]*ProposalData{}
	r.mu.checksums = map[uuid.UUID]ReplicaChecksum{}
	// Clear the internal raft group in case we're being reset. Since we're
	// reloading the raft state below, it isn't safe to use the existing raft
	// group.
	r.mu.internalRaftGroup = nil
	r.mu.proposalBuf.Init((*replicaProposer)(r))

	var err error
	if r.mu.state, err = r.mu.stateLoader.Load(ctx, r.store.Engine(), desc); err != nil {
		return err
	}

	// Init the minLeaseProposedTS such that we won't use an existing lease (if
	// any). This is so that, after a restart, we don't propose under old leases.
	// If the replica is being created through a split, this value will be
	// overridden.
	if !r.store.cfg.TestingKnobs.DontPreventUseOfOldLeaseOnStart {
		// Only do this if there was a previous lease. This shouldn't be important
		// to do but consider that the first lease which is obtained is back-dated
		// to a zero start timestamp (and this de-flakes some tests). If we set the
		// min proposed TS here, this lease could not be renewed (by the semantics
		// of minLeaseProposedTS); and since minLeaseProposedTS is copied on splits,
		// this problem would multiply to a number of replicas at cluster bootstrap.
		// Instead, we make the first lease special (which is OK) and the problem
		// disappears.
		if r.mu.state.Lease.Sequence > 0 {
			r.mu.minLeaseProposedTS = clock.Now()
		}
	}

	r.rangeStr.store(0, r.mu.state.Desc)

	r.mu.lastIndex, err = r.mu.stateLoader.LoadLastIndex(ctx, r.store.Engine())
	if err != nil {
		return err
	}
	r.mu.lastTerm = invalidLastTerm

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
	if err := r.setReplicaIDRaftMuLockedMuLocked(replicaID); err != nil {
		return err
	}

	r.assertStateLocked(ctx, r.store.Engine())
	return nil
}

func (r *Replica) setReplicaID(replicaID roachpb.ReplicaID) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setReplicaIDRaftMuLockedMuLocked(replicaID)
}

func (r *Replica) setReplicaIDRaftMuLockedMuLocked(replicaID roachpb.ReplicaID) error {
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

	if r.mu.destroyStatus.reason == destroyReasonRemovalPending {
		// An earlier incarnation of this replica was removed, but apparently it has been re-added
		// now, so reset the status.
		r.mu.destroyStatus.Reset()
	}

	// if r.mu.replicaID != 0 {
	// 	// TODO(bdarnell): clean up previous raftGroup (update peers)
	// }

	// Initialize or update the sideloaded storage. If the sideloaded storage
	// already exists (which is iff the previous replicaID was non-zero), then
	// we have to move the contained files over (this corresponds to the case in
	// which our replica is removed and re-added to the range, without having
	// the replica GC'ed in the meantime).
	//
	// Note that we can't race with a concurrent replicaGC here because both that
	// and this is under raftMu.
	ssBase := r.store.Engine().GetAuxiliaryDir()
	rangeID := r.mu.state.Desc.RangeID
	if err := moveSideloadedData(r.raftMu.sideloaded, ssBase, rangeID, replicaID); err != nil {
		return err
	}

	var err error
	if r.raftMu.sideloaded, err = newDiskSideloadStorage(
		r.store.cfg.Settings,
		rangeID,
		replicaID,
		ssBase,
		r.store.limiters.BulkIOWriteRate,
		r.store.engine,
	); err != nil {
		return errors.Wrap(err, "while initializing sideloaded storage")
	}

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

	if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		return true, nil
	}); err != nil {
		log.VErrEventf(ctx, 1, "unable to initialize raft group: %s", err)
	}
}

// setDesc atomically sets the replica's descriptor. It requires raftMu to be
// locked.
func (r *Replica) setDesc(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "range descriptor ID (%d) does not match replica's range ID (%d)",
			desc.RangeID, r.RangeID)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		(desc == nil || !desc.IsInitialized()) {
		log.Fatalf(ctx, "cannot replace initialized descriptor with uninitialized one: %+v -> %+v",
			r.mu.state.Desc, desc)
	}
	if r.mu.state.Desc != nil && r.mu.state.Desc.IsInitialized() &&
		!r.mu.state.Desc.StartKey.Equal(desc.StartKey) {
		log.Fatalf(ctx, "attempted to change replica's start key from %s to %s",
			r.mu.state.Desc.StartKey, desc.StartKey)
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

	r.rangeStr.store(r.mu.replicaID, desc)
	r.connectionClass.set(rpc.ConnectionClassForKey(desc.StartKey))
	r.mu.state.Desc = desc
}
