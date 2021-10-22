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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// nextReplicaIDIncrement defines how much forward we want to advance an ID of
// the designated winning replica to avoid any potential replicaID conflicts in
// case we've picked not the most up-to-date replica.
const nextReplicaIDIncrement = 10

// updatedLocationsMap is tracking which stores we plan to update with a plan.
// This information is used to present a user with a list of nodes where update
// must run to apply this plan saving them from running update on _every_ node
// in the cluster.
type updatedLocationsMap map[roachpb.NodeID]storeIDSet

func (m updatedLocationsMap) add(node roachpb.NodeID, store roachpb.StoreID) {
	var set storeIDSet
	var ok bool
	if set, ok = m[node]; !ok {
		set = make(storeIDSet)
		m[node] = set
	}
	set[store] = struct{}{}
}

func (m updatedLocationsMap) asMapOfSlices() map[roachpb.NodeID][]roachpb.StoreID {
	newMap := make(map[roachpb.NodeID][]roachpb.StoreID)
	for k, v := range m {
		newMap[k] = storeListFromSet(v)
	}
	return newMap
}

// PlanningReport provides aggregate stats and details of replica updates that is used
// for user confirmation.
type PlanningReport struct {
	// TotalReplicas is the number of replicas that were found on nodes present in the
	// cluster
	TotalReplicas int
	// DiscardedNonSurvivors is the number of replicas from ranges that lost quorum that
	// we decided not to use according to selection criteria used by planner.
	DiscardedNonSurvivors int
	// TODO(oleg): track total analyzed range count in subsequent version

	// PresentStores is deduced list of stores that we collected replica info from. This
	// set is filled from analyzed descriptors and may not strictly match stores on which
	// collection was run if some stores are empty.
	PresentStores []roachpb.StoreID
	// MissingStores is a deduced list of stores that were found in replica descriptors
	// but were not found in range descriptors e.g. collection was not run on those stores
	// because they are dead or because of human error.
	MissingStores []roachpb.StoreID

	// PlannedUpdates contains detailed update info about each planned update. This
	// information is presented to user for action confirmation and can contain details
	// that are not needed for actual plan application.
	PlannedUpdates []ReplicaUpdateReport
	// UpdatedNodes contains information about nodes with their stores where plan
	// needs to be applied. Stores are sorted in ascending order.
	UpdatedNodes map[roachpb.NodeID][]roachpb.StoreID
}

// ReplicaUpdateReport contains detailed info about changes planned for particular replica
// that was chosen as a designated survivor for the range.
// This information is more detailed than update plan and collected for reporting purposes.
// While information in update plan is meant for loqrecovery components, Report is meant for
// cli interaction to keep user informed of changes.
type ReplicaUpdateReport struct {
	RangeID           roachpb.RangeID
	StartKey          roachpb.RKey
	Replica           roachpb.ReplicaDescriptor
	OldReplica        roachpb.ReplicaDescriptor
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
	updatedLocations := make(updatedLocationsMap)
	var replicas []loqrecoverypb.ReplicaInfo
	for _, node := range nodes {
		replicas = append(replicas, node.Replicas...)
	}
	availableStoreIDs, missingStores, err := validateReplicaSets(replicas, deadStores)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}
	report.PresentStores = storeListFromSet(availableStoreIDs)
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
		var winningReplica roachpb.ReplicaDescriptor
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
				winningReplica = rep
			}
		}

		// If there's no dead peer in this group (so can't hope to fix
		// anything by rewriting the descriptor) or the current store is not the
		// one we want to turn into the sole voter, don't do anything.
		if numDeadPeers == 0 {
			continue
		}

		// The replica thinks it can make progress anyway, so we leave it alone.
		if desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
			_, ok := availableStoreIDs[rep.StoreID]
			return ok
		}) {
			log.Infof(ctx, "Replica has not lost quorum, skipping: %s", desc)
			continue
		}

		if rangeDesc.StoreID != maxLiveVoter {
			log.Infof(ctx, "Not designated survivor, skipping: %s", desc)
			report.DiscardedNonSurvivors++
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
			RangeID:      rangeDesc.Desc.RangeID,
			StartKey:     loqrecoverypb.RecoveryKey(desc.StartKey),
			OldReplicaID: winningReplica.ReplicaID,
			NewReplica: &roachpb.ReplicaDescriptor{
				NodeID:    rangeDesc.NodeID,
				StoreID:   rangeDesc.StoreID,
				ReplicaID: desc.NextReplicaID + nextReplicaIDIncrement,
			},
			NextReplicaID: desc.NextReplicaID + nextReplicaIDIncrement + 1,
		}
		log.Infof(ctx, "Replica has lost quorum, recovering: %s -> %s", desc, update)
		plan = append(plan, update)

		discarded := desc.Replicas().DeepCopy()
		discarded.RemoveReplica(rangeDesc.NodeID, rangeDesc.StoreID)
		report.PlannedUpdates = append(report.PlannedUpdates, ReplicaUpdateReport{
			RangeID:           desc.RangeID,
			StartKey:          desc.StartKey,
			Replica:           *update.NewReplica,
			OldReplica:        winningReplica,
			StoreID:           rangeDesc.StoreID,
			DiscardedReplicas: discarded,
		})
		updatedLocations.add(rangeDesc.NodeID, rangeDesc.StoreID)
	}
	report.UpdatedNodes = updatedLocations.asMapOfSlices()
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
