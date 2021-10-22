// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type storeIDSet map[roachpb.StoreID]struct{}

// CollectReplicaInfo captures states of all replicas in all stores for the sake of quorum recovery.
func CollectReplicaInfo(
	ctx context.Context, stores []storage.Engine,
) (loqrecoverypb.NodeReplicaInfo, error) {
	if len(stores) == 0 {
		return loqrecoverypb.NodeReplicaInfo{}, errors.New("no stores were provided for info collection")
	}

	var replicas []loqrecoverypb.ReplicaInfo
	for _, db := range stores {
		storeIdent, err := kvserver.ReadStoreIdent(ctx, db)
		if err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
		err = kvserver.IterateRangeDescriptorsFromDisk(ctx, db, func(desc roachpb.RangeDescriptor) error {
			rsl := stateloader.Make(desc.RangeID)
			rstate, err := rsl.Load(ctx, db, &desc)
			if err != nil {
				return err
			}
			hstate, err := rsl.LoadHardState(ctx, db)
			if err != nil {
				return err
			}
			replicaData := loqrecoverypb.ReplicaInfo{
				StoreID:            storeIdent.StoreID,
				NodeID:             storeIdent.NodeID,
				Desc:               desc,
				RaftAppliedIndex:   rstate.RaftAppliedIndex,
				RaftCommittedIndex: hstate.Commit,
				// TODO(oleg): #73282 Track presence of uncommitted descriptors
				HasUncommittedDescriptors: false,
			}
			replicas = append(replicas, replicaData)
			return nil
		})
		if err != nil {
			return loqrecoverypb.NodeReplicaInfo{}, err
		}
	}
	return loqrecoverypb.NodeReplicaInfo{Replicas: replicas}, nil
}

type updateLocationsMap map[roachpb.NodeID]storeIDSet

func (m updateLocationsMap) Add(node roachpb.NodeID, store roachpb.StoreID) {
	var set storeIDSet
	var ok bool
	if set, ok = m[node]; !ok {
		set = make(storeIDSet)
		m[node] = set
	}
	set[store] = struct{}{}
}

func (m updateLocationsMap) AsMapOfLists() map[roachpb.NodeID][]roachpb.StoreID {
	newMap := make(map[roachpb.NodeID][]roachpb.StoreID)
	for k, v := range m {
		newMap[k] = storeListFromSet(v)
	}
	return newMap
}

// PlanningReport provides aggregate stats and details of replica updates that could
// be used for user confirmation.
type PlanningReport struct {
	// Totals for the planning process.
	TotalReplicas    int
	HealthyReplicas  int
	LiveNonSurvivors int
	// TODO(oleg): track total analyzed range count in subsequent version

	// Deduced store availability info.
	AvailableStores []roachpb.StoreID
	MissingStores   []roachpb.StoreID

	// Detailed update info.
	PlannedUpdates []PlannedReplicaReport
	// Updates about nodes where plan must be applied.
	UpdatedNodes map[roachpb.NodeID][]roachpb.StoreID
}

// PlannedReplicaReport contains detailed info about changes planned for particular replicas.
// Provides information which replica exactly is promoted and which ones are discarded.
type PlannedReplicaReport struct {
	RangeID           roachpb.RangeID
	StartKey          roachpb.RKey
	OldReplicaID      roachpb.ReplicaID
	ReplicaID         roachpb.ReplicaID
	StoreID           roachpb.StoreID
	DiscardedReplicas roachpb.ReplicaSet
}

// PlanReplicas analyzes captured replica information to determine which replicas could serve
// as dedicated survivors in ranges where quorum was lost.
// Devised plan doesn't guarantee data consistency after the recovery, only the fact that ranges
// could progress and subsequently perform up-replication.
func PlanReplicas(
	ctx context.Context, nodes []loqrecoverypb.NodeReplicaInfo, deadStores []roachpb.StoreID,
) (loqrecoverypb.ReplicaUpdatePlan, PlanningReport, error) {
	var report PlanningReport
	updateLocations := make(updateLocationsMap)
	var replicas []loqrecoverypb.ReplicaInfo
	for _, node := range nodes {
		replicas = append(replicas, node.Replicas...)
	}
	availableStoreIDs, missingStores, err := validateReplicaSets(replicas, deadStores)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}
	report.AvailableStores = storeListFromSet(availableStoreIDs)
	report.MissingStores = storeListFromSet(missingStores)

	var plan []loqrecoverypb.ReplicaUpdate
	// Find ranges that lost quorum and create updates for them.
	// This approach is only using local info stored in each replica independently.
	// It is inherited in unchanged form from existing recovery operation
	// `debug unsafe-remove-dead-replicas`.
	// TODO(oleg): #73662 Use additional field and information about all replicas of
	// range to determine winner.
	for _, rangeDesc := range replicas {
		report.TotalReplicas++
		numDeadPeers := 0
		desc := rangeDesc.Desc
		allReplicas := desc.Replicas().Descriptors()
		maxLiveVoter := roachpb.StoreID(-1)
		var winningReplicaID roachpb.ReplicaID
		for _, rep := range allReplicas {
			if _, ok := availableStoreIDs[rep.StoreID]; !ok {
				numDeadPeers++
				continue
			}
			// The designated survivor will be the voter with the highest storeID.
			// Note that an outgoing voter cannot be designated, as the only
			// replication change it could make is to turn itself into a learner, at
			// which point the range is completely messed up.
			//
			// Note: a better heuristic might be to choose the leaseholder store, not
			// the largest store, as this avoids the problem of requests still hanging
			// after running the tool in a rolling-restart fashion (when the lease-
			// holder is under a valid epoch and was ont chosen as designated
			// survivor). However, this choice is less deterministic, as leaseholders
			// are more likely to change than replication configs. The hanging would
			// independently be fixed by the below issue, so staying with largest store
			// is likely the right choice. See:
			//
			// https://github.com/cockroachdb/cockroach/issues/33007
			if rep.IsVoterNewConfig() && rep.StoreID > maxLiveVoter {
				maxLiveVoter = rep.StoreID
				winningReplicaID = rep.ReplicaID
			}
		}

		// If there's no dead peer in this group (so can't hope to fix
		// anything by rewriting the descriptor) or the current store is not the
		// one we want to turn into the sole voter, don't do anything.
		if numDeadPeers == 0 {
			report.HealthyReplicas++
			continue
		}

		// The replica thinks it can make progress anyway, so we leave it alone.
		if desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
			_, ok := availableStoreIDs[rep.StoreID]
			return ok
		}) {
			log.Infof(ctx, "Replica has not lost quorum, skipping: %s", desc)
			report.HealthyReplicas++
			continue
		}

		if rangeDesc.StoreID != maxLiveVoter {
			log.Infof(ctx, "Not designated survivor, skipping: %s", desc)
			report.LiveNonSurvivors++
			continue
		}

		// We're the designated survivor and the range needs to be recovered.
		//
		// Rewrite the range as having a single replica. The winning replica is
		// picked arbitrarily: the one with the highest store ID. This is not always
		// the best option: it may lose writes that were committed on another
		// surviving replica that had applied more of the raft log. However, in
		// practice when we have multiple surviving replicas but still need this
		// tool (because the replication factor was 4 or higher), we see that the
		// logs are nearly always in sync and the choice doesn't matter. Correctly
		// picking the replica with the longer log would complicate the use of this
		// tool.
		// Rewrite the replicas list. Bump the replica ID so that in case there are
		// other surviving nodes that were members of the old incarnation of the
		// range, they no longer recognize this revived replica (because they are
		// not in sync with it).
		update := loqrecoverypb.ReplicaUpdate{
			NodeID:       rangeDesc.NodeID,
			StoreID:      rangeDesc.StoreID,
			RangeID:      rangeDesc.Desc.RangeID,
			StartKey:     loqrecoverypb.RecoveryKey(desc.StartKey),
			OldReplicaID: winningReplicaID,
			NewReplica: &roachpb.ReplicaDescriptor{
				NodeID:    rangeDesc.NodeID,
				StoreID:   rangeDesc.StoreID,
				ReplicaID: desc.NextReplicaID,
			},
			NextReplicaID: desc.NextReplicaID + 1,
		}
		log.Infof(ctx, "Replica has lost quorum, recovering: %s -> %s", desc, update)
		plan = append(plan, update)

		discarded := desc.Replicas().DeepCopy()
		discarded.RemoveReplica(rangeDesc.NodeID, rangeDesc.StoreID)
		report.PlannedUpdates = append(report.PlannedUpdates, PlannedReplicaReport{
			RangeID:           desc.RangeID,
			StartKey:          desc.StartKey,
			OldReplicaID:      winningReplicaID,
			ReplicaID:         desc.NextReplicaID,
			StoreID:           rangeDesc.StoreID,
			DiscardedReplicas: discarded,
		})
		updateLocations.Add(rangeDesc.NodeID, rangeDesc.StoreID)
	}
	report.UpdatedNodes = updateLocations.AsMapOfLists()
	return loqrecoverypb.ReplicaUpdatePlan{Updates: plan}, report, nil
}

// validateReplicaSets evaluates provided set of replicas and an optional deadStoreIDs
// request and produces consistency info containing:
// availableStores  - all storeIDs for which info was collected, i.e. (barring operator
//                    error) the conclusive list of all remaining stores in the cluster.
// missingStores    - all dead stores (stores that are referenced by replicas, but not
//                    present in any of descriptors)
// If inconsistency is found e.g. no info was provided for a store but it is not present
// in explicit deadStoreIDs list, error is returned.
func validateReplicaSets(
	replicas []loqrecoverypb.ReplicaInfo, deadStores []roachpb.StoreID,
) (availableStoreIDs, missingStoreIDs storeIDSet, _ error) {
	// Populate availableStoreIDs with all StoreIDs from which we collected info and,
	// populate missingStoreIDs with all StoreIDs referenced in replica descriptors for
	// which no information was collected.
	availableStoreIDs = make(storeIDSet)
	missingStoreIDs = make(storeIDSet)
	for _, replicaDescriptor := range replicas {
		availableStoreIDs[replicaDescriptor.StoreID] = struct{}{}
		for _, replicaDesc := range replicaDescriptor.Desc.InternalReplicas {
			missingStoreIDs[replicaDesc.StoreID] = struct{}{}
		}
	}
	// The difference between all referenced StoreIDs (missingStoreIDs) and the present
	// StoreIDs (presentStoreIDs) should exactly equal the user-provided list of dead
	// stores (deadStores), and the former must be a superset of the latter (since each
	// descriptor found on a store references that store). Verify all of these conditions
	// and error out if one of them does not hold.
	for id := range availableStoreIDs {
		delete(missingStoreIDs, id)
	}
	// Stores that doesn't have info, but are not in explicit list.
	missingButNotDeadStoreIDs := make(storeIDSet)
	for id := range missingStoreIDs {
		missingButNotDeadStoreIDs[id] = struct{}{}
	}
	// Suspicious are available, but requested dead.
	suspiciousStoreIDs := make(storeIDSet)
	for _, id := range deadStores {
		delete(missingButNotDeadStoreIDs, id)
		if _, ok := availableStoreIDs[id]; ok {
			suspiciousStoreIDs[id] = struct{}{}
		}
	}
	if len(suspiciousStoreIDs) > 0 {
		return nil, nil, errors.Errorf(
			"stores %s are listed as dead, but replica info is provided for them",
			joinStoreIDs(suspiciousStoreIDs))
	}
	if len(deadStores) > 0 && len(missingButNotDeadStoreIDs) > 0 {
		// We can't proceed with this if dead nodes were explicitly provided.
		return nil, nil, errors.Errorf(
			"information about stores %s were not provided, nor they are listed as dead",
			joinStoreIDs(missingButNotDeadStoreIDs))
	}
	return availableStoreIDs, missingStoreIDs, nil
}

// MarshalAndWriteResults serializes plan and write into file or stdout.
func MarshalAndWriteResults(results protoutil.Message, writer io.Writer) error {
	jsonpb := protoutil.JSONPb{Indent: "  "}
	var out []byte
	var err error
	if out, err = jsonpb.Marshal(results); err != nil {
		return errors.Wrap(err, "failed to marshal results")
	}
	if _, err = writer.Write(out); err != nil {
		return errors.Wrap(err, "failed to write results")
	}
	return nil
}

// PrepareStoreReport contains information about all prepared changes for the
// stores. Its purpose is to inform user about actions to be performed.
type PrepareStoreReport struct {
	// MissingStores contains a set of stores that node on which this update is performed
	// is expected to have according to update plan, but are not provided in config.
	MissingStores []roachpb.StoreID
	// Replicas contain update info about all replicas that are planned for update.
	UpdatedReplicas []PrepareReplicaReport
	// Replicas identified as ones that doesn't need updating (currently because update was
	// already done).
	SkippedReplicas []PrepareReplicaReport
}

// UpdatableStore is a wrapper around storage.Engine providing ability
// to lazily create batches and perform lifecycle management of the store
// and the batch.
type UpdatableStore struct {
	storage storage.Engine
	batch   storage.Batch
}

// NewUpdatableStore creates new UpdatableStore from storage.Engine
func NewUpdatableStore(storage storage.Engine) *UpdatableStore {
	return &UpdatableStore{storage: storage}
}

// GetStoreID reads roachpb.StoreIdent from underlying storage.
// Hides underlying operation details from loqrecovery users.
func (u *UpdatableStore) GetStoreID(ctx context.Context) (roachpb.StoreIdent, error) {
	return kvserver.ReadStoreIdent(ctx, u.storage)
}

// Batch creates or returns existing batch to update storage.
func (u *UpdatableStore) Batch() storage.Batch {
	if u.batch == nil {
		u.batch = u.storage.NewBatch()
	}
	return u.batch
}

// Close closes storage and corresponding update batch if it was
// open.
func (u *UpdatableStore) Close() {
	if u.batch != nil {
		u.batch.Close()
	}
	u.storage.Close()
}

func (u *UpdatableStore) unchanged() bool {
	return u.batch == nil || u.batch.Empty()
}

// PrepareReplicaReport contains information about prepared change for a replica.
// Its purpose is to inform user about actions to be performed.
type PrepareReplicaReport struct {
	// Replica identification data.
	RangeID      roachpb.RangeID
	StartKey     roachpb.RKey
	ReplicaID    roachpb.ReplicaID
	OldReplicaID roachpb.ReplicaID

	// Update status.
	AlreadyUpdated bool

	// Update details.
	RemovedReplicas roachpb.ReplicaSet

	// Fields indicating if descriptor change intent was found and removed as a part
	// or recovery preparation.
	AbortedTransaction   bool
	AbortedTransactionID uuid.UUID
}

// PrepareUpdateReplicas creates batch updates from plan to provided stores.
// All found discrepancies are collected into list of warnings.
// This is the first step in applying recovery plan to stores. After this step
// all stores are still intact but changes are prepared successfully.
func PrepareUpdateReplicas(
	ctx context.Context,
	plan loqrecoverypb.ReplicaUpdatePlan,
	nodeID roachpb.NodeID,
	stores map[roachpb.StoreID]*UpdatableStore,
) (PrepareStoreReport, error) {
	var report PrepareStoreReport
	// TODO(oleg): #73281 Track attempts to apply changes and also fill into audit log
	// Make a pre-check for all found stores, so we could confirm action.
	// Map contains a set of store names that were found in plan for this node, but were not
	// configured in this command invocation.
	missing := make(map[roachpb.StoreID]struct{})
	for _, update := range plan.Updates {
		if nodeID != update.NodeID {
			continue
		}
		if store, ok := stores[update.StoreID]; !ok {
			missing[update.StoreID] = struct{}{}
			continue
		} else {
			replicaReport, err := applyReplicaUpdate(ctx, store.Batch(), update)
			if err != nil {
				return PrepareStoreReport{}, errors.Wrapf(err,
					"failed to prepare update replica for range r%v on store s%d", update.RangeID, update.StoreID)
			}
			if !replicaReport.AlreadyUpdated {
				report.UpdatedReplicas = append(report.UpdatedReplicas, replicaReport)
			} else {
				report.SkippedReplicas = append(report.SkippedReplicas, replicaReport)
			}
		}
	}

	if len(missing) > 0 {
		report.MissingStores = storeListFromSet(missing)
	}
	return report, nil
}

func applyReplicaUpdate(
	ctx context.Context, readWriter storage.ReadWriter, update loqrecoverypb.ReplicaUpdate,
) (PrepareReplicaReport, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)
	report := PrepareReplicaReport{
		RangeID:      update.RangeID,
		ReplicaID:    update.NewReplica.ReplicaID,
		OldReplicaID: update.OldReplicaID,
		StartKey:     update.StartKey.AsRKey(),
	}

	// Write the rewritten descriptor to the range-local descriptor
	// key. We do not update the meta copies of the descriptor.
	// Instead, we leave them in a temporarily inconsistent state and
	// they will be overwritten when the cluster recovers and
	// up-replicates this range from its single copy to multiple
	// copies. We rely on the fact that all range descriptor updates
	// start with a CPut on the range-local copy followed by a blind
	// Put to the meta copy.
	//
	// For example, if we have replicas on s1-s4 but s3 and s4 are
	// dead, we will rewrite the replica on s2 to have s2 as its only
	// member only. When the cluster is restarted (and the dead nodes
	// remain dead), the rewritten replica will be the only one able
	// to make progress. It will elect itself leader and upreplicate.
	//
	// The old replica on s1 is untouched by this process. It will
	// eventually either be overwritten by a new replica when s2
	// upreplicates, or it will be destroyed by the replica GC queue
	// after upreplication has happened and s1 is no longer a member.
	// (Note that in the latter case, consistency between s1 and s2 no
	// longer matters; the consistency checker will only run on nodes
	// that the new leader believes are members of the range).
	//
	// Note that this tool does not guarantee fully consistent
	// results; the most recent writes to the raft log may have been
	// lost. In the most unfortunate cases, this means that we would
	// be "winding back" a split or a merge, which is almost certainly
	// to result in irrecoverable corruption (for example, not only
	// will individual values stored in the meta ranges diverge, but
	// there will be keys not represented by any ranges or vice
	// versa).
	key := keys.RangeDescriptorKey(update.StartKey.AsRKey())
	value, intent, err := storage.MVCCGet(
		ctx, readWriter, key, clock.Now(), storage.MVCCGetOptions{Inconsistent: true})
	if value == nil {
		return PrepareReplicaReport{}, errors.Errorf(
			"failed to find a range descriptor for range %v", key)
	}
	if err != nil {
		return PrepareReplicaReport{}, err
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return PrepareReplicaReport{}, err
	}
	// Sanity check that this is indeed the right range.
	if desc.RangeID != update.RangeID {
		return PrepareReplicaReport{}, errors.Errorf(
			"unexpected range ID at key: expected r%d but found r%d", update.RangeID, desc.RangeID)
	}
	// Check if replica is in a fixed state already if we already applied the change.
	if len(desc.InternalReplicas) == 1 &&
		desc.InternalReplicas[0].ReplicaID == update.NewReplica.ReplicaID &&
		desc.NextReplicaID == update.NextReplicaID {
		report.AlreadyUpdated = true
		return report, nil
	}

	sl := stateloader.Make(desc.RangeID)
	ms, err := sl.LoadMVCCStats(ctx, readWriter)
	if err != nil {
		return PrepareReplicaReport{}, errors.Wrap(err, "loading MVCCStats")
	}

	if intent != nil {
		// We rely on the property that transactions involving the range
		// descriptor always start on the range-local descriptor's key. When there
		// is an intent, this means that it is likely that the transaction did not
		// commit, so we abort the intent.
		//
		// However, this is not guaranteed. For one, applying a command is not
		// synced to disk, so in theory whichever store becomes the designated
		// survivor may temporarily have "forgotten" that the transaction
		// committed in its applied state (it would still have the committed log
		// entry, as this is durable state, so it would come back once the node
		// was running, but we don't see that materialized state in
		// unsafe-remove-dead-replicas). This is unlikely to be a problem in
		// practice, since we assume that the store was shut down gracefully and
		// besides, the write likely had plenty of time to make it to durable
		// storage. More troubling is the fact that the designated survivor may
		// simply not yet have learned that the transaction committed; it may not
		// have been in the quorum and could've been slow to catch up on the log.
		// It may not even have the intent; in theory the remaining replica could
		// have missed any number of transactions on the range descriptor (even if
		// they are in the log, they may not yet be applied, and the replica may
		// not yet have learned that they are committed). This is particularly
		// troubling when we miss a split, as the right-hand side of the split
		// will exist in the meta ranges and could even be able to make progress.
		// For yet another thing to worry about, note that the determinism (across
		// different nodes) assumed in this tool can easily break down in similar
		// ways (not all stores are going to have the same view of what the
		// descriptors are), and so multiple replicas of a range may declare
		// themselves the designated survivor. Long story short, use of this tool
		// with or without the presence of an intent can - in theory - really
		// tear the cluster apart.
		//
		// A solution to this would require a global view, where in a first step
		// we collect from each store in the cluster the replicas present and
		// compute from that a "recovery plan", i.e. set of replicas that will
		// form the recovered keyspace. We may then find that no such recovery
		// plan is trivially achievable, due to any of the above problems. But
		// in the common case, we do expect one to exist.
		report.AbortedTransaction = true
		report.AbortedTransactionID = intent.Txn.ID

		// A crude form of the intent resolution process: abort the
		// transaction by deleting its record.
		txnKey := keys.TransactionKey(intent.Txn.Key, intent.Txn.ID)
		if err := storage.MVCCDelete(ctx, readWriter, &ms, txnKey, hlc.Timestamp{}, nil); err != nil {
			return PrepareReplicaReport{}, err
		}
		update := roachpb.LockUpdate{
			Span:   roachpb.Span{Key: intent.Key},
			Txn:    intent.Txn,
			Status: roachpb.ABORTED,
		}
		if _, err := storage.MVCCResolveWriteIntent(ctx, readWriter, &ms, update); err != nil {
			return PrepareReplicaReport{}, err
		}
		report.AbortedTransaction = true
		report.AbortedTransactionID = intent.Txn.ID
	}
	newDesc := desc
	replicas := []roachpb.ReplicaDescriptor{{
		NodeID:    update.NewReplica.NodeID,
		StoreID:   update.NewReplica.StoreID,
		ReplicaID: update.NewReplica.ReplicaID,
		Type:      update.NewReplica.Type,
	}}
	newDesc.SetReplicas(roachpb.MakeReplicaSet(replicas))
	newDesc.NextReplicaID = update.NextReplicaID

	// Write back
	if err := storage.MVCCPutProto(ctx, readWriter, &ms, key, clock.Now(),
		nil /* txn */, &newDesc); err != nil {
		return PrepareReplicaReport{}, err
	}
	report.RemovedReplicas = desc.Replicas()
	report.RemovedReplicas.RemoveReplica(update.NewReplica.NodeID, update.NewReplica.StoreID)

	// Refresh stats
	if err := sl.SetMVCCStats(ctx, readWriter, &ms); err != nil {
		return PrepareReplicaReport{}, errors.Wrap(err, "updating MVCCStats")
	}

	return report, nil
}

// ApplyUpdateReport contains info about recovery changes applied to stores.
type ApplyUpdateReport struct {
	// IDs of successfully updated stores.
	UpdatedStores []roachpb.StoreID
}

// CommitReplicaChanges saves content storage batches into stores. This is the second step
// of applying recovery plan.
func CommitReplicaChanges(stores map[roachpb.StoreID]*UpdatableStore) (ApplyUpdateReport, error) {
	var report ApplyUpdateReport
	failed := false
	var updateErrors []string
	// Since we reached here we updated all stores
	for id, store := range stores {
		// We can have empty batches if we try to apply the same change multiple times.
		// This is fine if we have to retry if previous invocation of apply-plan command
		// failed after updating only some stores.
		if !store.unchanged() {
			if err := store.Batch().Commit(true); err != nil {
				// If we fail here, we can only try to run the whole process from scratch as this store is somehow broken.
				updateErrors = append(updateErrors, fmt.Sprintf("failed to update store s%d: %v", id, err))
				failed = true
			} else {
				report.UpdatedStores = append(report.UpdatedStores, id)
			}
		} else {
			fmt.Printf("Unchanged s%d\n", id)
		}
	}
	if failed {
		return ApplyUpdateReport{}, errors.Errorf("failed to commit update to one or more stores: %s", strings.Join(updateErrors, "; "))
	}
	return report, nil
}

// storeListFromSet unwraps map to a sorted list of StoreIDs.
func storeListFromSet(set storeIDSet) []roachpb.StoreID {
	storeIDs := make([]roachpb.StoreID, 0, len(set))
	for k := range set {
		storeIDs = append(storeIDs, k)
	}
	sort.Slice(storeIDs, func(i, j int) bool {
		return storeIDs[i] < storeIDs[j]
	})
	return storeIDs
}

// Make a string of stores 'set' in ascending order.
func joinStoreIDs(storeIDs storeIDSet) string {
	storeNames := make([]string, 0, len(storeIDs))
	for _, id := range storeListFromSet(storeIDs) {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}
