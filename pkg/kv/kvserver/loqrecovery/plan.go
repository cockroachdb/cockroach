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
	"sort"
	"strings"

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

func (r *PlanningReport) addUpdate(rr ReplicaUpdateReport) {
	r.PlannedUpdates = append(r.PlannedUpdates, rr)
}

// ReplicaUpdateReport contains detailed info about changes planned for particular replica
// that was chosen as a designated survivor for the range.
// This information is more detailed than update plan and collected for reporting purposes.
// While information in update plan is meant for loqrecovery components, Report is meant for
// cli interaction to keep user informed of changes.
type ReplicaUpdateReport struct {
	RangeID                    roachpb.RangeID
	StartKey                   roachpb.RKey
	Replica                    roachpb.ReplicaDescriptor
	OldReplica                 roachpb.ReplicaDescriptor
	StoreID                    roachpb.StoreID
	DiscardedAvailableReplicas roachpb.ReplicaSet
	DiscardedDeadReplicas      roachpb.ReplicaSet
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

	replicasByRangeID := groupReplicasByRangeID(replicas)
	// proposedWinners contain decisions for all ranges in keyspace. it contains ranges
	// that lost quorum as well as the ones that didn't.
	var proposedWinners []proposedDecision
	for _, rangeReplicas := range replicasByRangeID {
		proposedWinners = append(proposedWinners, pickPotentialWinningReplica(rangeReplicas))
	}
	if err = checkRangeCompleteness(proposedWinners); err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}

	var plan []loqrecoverypb.ReplicaUpdate
	for _, p := range proposedWinners {
		report.TotalReplicas += len(p.others) + 1
		u, ok := makeReplicaUpdateIfNeeded(ctx, p, availableStoreIDs)
		if ok {
			plan = append(plan, u)
			report.DiscardedNonSurvivors += len(p.others)
			report.addUpdate(makeReplicaUpdateReport(p, u))
			updatedLocations.add(u.NodeID(), u.StoreID())
			log.Infof(ctx, "Replica has lost quorum, recovering: %s -> %s", p.winner.Desc, u)
		} else {
			log.Infof(ctx, "Range r%d didn't lose quorum", &p.winner.Desc.RangeID)
		}
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

func groupReplicasByRangeID(
	descriptors []loqrecoverypb.ReplicaInfo,
) map[roachpb.RangeID][]loqrecoverypb.ReplicaInfo {
	groupedRanges := make(map[roachpb.RangeID][]loqrecoverypb.ReplicaInfo)
	for _, descriptor := range descriptors {
		groupedRanges[descriptor.Desc.RangeID] = append(groupedRanges[descriptor.Desc.RangeID], descriptor)
	}
	return groupedRanges
}

// proposedDecision contains replica resolution details e.g. preferred replica as
// well as extra info to produce a report of the planned action.
type proposedDecision struct {
	winner loqrecoverypb.ReplicaInfo
	others []loqrecoverypb.ReplicaInfo
}

func (p *proposedDecision) StartKey() roachpb.RKey {
	return p.winner.Desc.StartKey
}

func (p *proposedDecision) EndKey() roachpb.RKey {
	return p.winner.Desc.EndKey
}

func (p *proposedDecision) RangeID() roachpb.RangeID {
	return p.winner.Desc.RangeID
}

// pickPotentialWinningReplica given a slice of replicas for the range from all live stores, pick one to survive recovery.
// if progress can be made, still pick one replica so that it could be used to do key completeness validation.
// Note that descriptors argument would be sorted in process of picking a winner
func pickPotentialWinningReplica(replicas []loqrecoverypb.ReplicaInfo) proposedDecision {
	// Maybe that's too expensive to compute multiple times per replica?
	isVoter := func(desc loqrecoverypb.ReplicaInfo) int {
		for _, replica := range desc.Desc.InternalReplicas {
			if replica.StoreID == desc.StoreID {
				if replica.IsVoterNewConfig() {
					return 1
				}
				return 0
			}
		}
		// This is suspicious, our descriptor is not in replicas. Panic maybe?
		return 0
	}
	sort.Slice(replicas, func(i, j int) bool {
		// When finding the best suitable replica evaluate 3 conditions in order:
		//  - replica is a voter
		//  - replica has the higher range committed index
		//  - replica has the higher store id
		//
		// Note: that an outgoing voter cannot be designated, as the only
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
		voterI := isVoter(replicas[i])
		voterJ := isVoter(replicas[j])
		return voterI > voterJ ||
			(voterI == voterJ &&
				(replicas[i].RaftAppliedIndex > replicas[j].RaftAppliedIndex ||
					(replicas[i].RaftAppliedIndex == replicas[j].RaftAppliedIndex && replicas[i].StoreID > replicas[j].StoreID)))
	})
	// We could be returning a non-voting replica if all voters are gone. Is this acceptable or we need to filter?
	return proposedDecision{winner: replicas[0], others: replicas[1:]}
}

type anomaly struct {
	span    roachpb.Span
	range1  roachpb.RangeID
	range2  roachpb.RangeID
	overlap bool
}

func (i anomaly) String() string {
	if i.overlap {
		return fmt.Sprintf("range overlap %v between ranges r%d and r%d", i.span, i.range1, i.range2)
	}
	return fmt.Sprintf("range gap %v between ranges r%d and r%d", i.span, i.range1, i.range2)
}

// checkRangeCompleteness given slice of all survivor ranges, checks that full keyspace is covered.
// Note that slice would be sorted in process of the check.
func checkRangeCompleteness(replicas []proposedDecision) error {
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].StartKey().Less(replicas[j].StartKey())
	})
	var anomalies []anomaly
	lastKey := roachpb.RKeyMin
	prevRange := roachpb.RangeID(0)
	// We validate that first range starts at min key, last range ends at max key and that for
	// every range start key is equal to end key of previous range. If any of those conditions
	// fail, we record this as anomaly to indicate there's a gap between ranges or an overlap
	// between two or more ranges.
	for _, desc := range replicas {
		switch {
		case desc.StartKey().Less(lastKey):
			anomalies = append(anomalies, anomaly{
				span:    roachpb.Span{Key: roachpb.Key(desc.StartKey()), EndKey: roachpb.Key(lastKey)},
				range1:  prevRange,
				range2:  desc.RangeID(),
				overlap: true,
			})
			if lastKey.Less(desc.EndKey()) {
				prevRange = desc.RangeID()
				lastKey = desc.EndKey()
			}
		case lastKey.Less(desc.StartKey()):
			anomalies = append(anomalies, anomaly{
				span:    roachpb.Span{Key: roachpb.Key(lastKey), EndKey: roachpb.Key(desc.StartKey())},
				range1:  prevRange,
				range2:  desc.RangeID(),
				overlap: false,
			})
			prevRange = desc.RangeID()
			lastKey = desc.EndKey()
		default:
			prevRange = desc.RangeID()
			lastKey = desc.EndKey()
		}
	}
	if !lastKey.Equal(roachpb.RKeyMax) {
		anomalies = append(anomalies, anomaly{
			span:    roachpb.Span{Key: roachpb.Key(lastKey), EndKey: roachpb.Key(roachpb.RKeyMax)},
			range1:  prevRange,
			range2:  roachpb.RangeID(0),
			overlap: false,
		})
	}

	if len(anomalies) > 0 {
		descriptions := make([]string, 0, len(anomalies))
		for _, id := range anomalies {
			descriptions = append(descriptions, fmt.Sprintf("%v", id))
		}
		return errors.Newf("key range is incomplete. discovered inconsistencies: %s", strings.Join(descriptions, ", "))
	}
	return nil
}

// makeReplicaUpdateIfNeeded if candidate range can't make progress, create an update using preferred
// replica.
// Returns a replica update and a flag indicating if update needs to be performed.
// For replicas that can make progress return empty update and false to exclude range from update plan.
func makeReplicaUpdateIfNeeded(
	ctx context.Context, p proposedDecision, liveStoreIDs storeIDSet,
) (loqrecoverypb.ReplicaUpdate, bool) {
	if !p.winner.Desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
		_, ok := liveStoreIDs[rep.StoreID]
		return ok
	}) {
		// We want to have replicaID which is greater or equal nextReplicaID across
		// all available replicas. We want to ensure that there would be no conflict
		// without bumping it by arbitrary number.
		nextReplicaID := p.winner.Desc.NextReplicaID
		for _, r := range p.others {
			if r.Desc.NextReplicaID > nextReplicaID {
				nextReplicaID = r.Desc.NextReplicaID
			}
		}

		replica, err := p.winner.Replica()
		if err != nil {
			log.Fatalf(ctx, "replica descriptor doesn't contain replica for its own store: %s", p.winner.Desc)
		}

		// The range needs to be recovered and this replica is a designated survivor.
		// To recover the range rewrite it as having a single replica:
		// - Rewrite the replicas list.
		// - Bump the replica ID so that in case there are other surviving nodes that
		//   were members of the old incarnation of the range, they no longer recognize
		//   this revived replica (because they are not in sync with it).
		return loqrecoverypb.ReplicaUpdate{
			RangeID:      p.winner.Desc.RangeID,
			StartKey:     loqrecoverypb.RecoveryKey(p.winner.Desc.StartKey),
			OldReplicaID: replica.ReplicaID,
			NewReplica: &roachpb.ReplicaDescriptor{
				NodeID:    p.winner.NodeID,
				StoreID:   p.winner.StoreID,
				ReplicaID: nextReplicaID + nextReplicaIDIncrement,
			},
			NextReplicaID: nextReplicaID + nextReplicaIDIncrement + 1,
		}, true
	}
	return loqrecoverypb.ReplicaUpdate{}, false
}

// makeReplicaUpdateReport creates a detailed report of changes that needs to
// be performed on range. It uses decision as well as information about all replicas
// of range to provide information about what is being discarded and how new replica
// would be configured.
func makeReplicaUpdateReport(
	p proposedDecision, update loqrecoverypb.ReplicaUpdate,
) ReplicaUpdateReport {
	oldReplica, _ := p.winner.Replica()

	// Replicas that belonged to unavailable nodes based on winning range descriptor.
	discardedDead := p.winner.Desc.Replicas()
	discardedDead.RemoveReplica(update.NodeID(), update.StoreID())
	// Replicas that we collected info about for the range, but decided they are not
	// preferred choice.
	discardedAvailable := roachpb.ReplicaSet{}
	for _, replica := range p.others {
		discardedDead.RemoveReplica(replica.NodeID, replica.StoreID)
		r, _ := replica.Desc.GetReplicaDescriptor(replica.StoreID)
		discardedAvailable.AddReplica(r)
	}

	return ReplicaUpdateReport{
		RangeID:                    p.winner.Desc.RangeID,
		StartKey:                   p.winner.Desc.StartKey,
		OldReplica:                 oldReplica,
		Replica:                    *update.NewReplica,
		StoreID:                    p.winner.StoreID,
		DiscardedDeadReplicas:      discardedDead,
		DiscardedAvailableReplicas: discardedAvailable,
	}
}
