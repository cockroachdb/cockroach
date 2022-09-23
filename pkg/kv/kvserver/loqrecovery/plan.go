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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// nextReplicaIDIncrement defines how much forward we want to advance an ID of
// the designated surviving replica to avoid any potential replicaID conflicts
// in case we've picked not the most up-to-date replica.
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
		newMap[k] = storeSliceFromSet(v)
	}
	return newMap
}

// PlanningReport provides aggregate stats and details of replica updates that
// is used for user confirmation.
type PlanningReport struct {
	// TotalReplicas is the number of replicas that were found on nodes present
	// in the cluster
	TotalReplicas int
	// DiscardedNonSurvivors is the number of replicas from ranges that lost
	// quorum that
	// we decided not to use according to selection criteria used by planner.
	DiscardedNonSurvivors int
	// TODO(oleg): track total analyzed range count in subsequent version

	// PresentStores is deduced list of stores that we collected replica info
	// from. This set is filled from analyzed descriptors and may not strictly
	// match stores on which collection was run if some stores are empty.
	PresentStores []roachpb.StoreID
	// MissingStores is a deduced list of stores that were found in replica
	// descriptors but were not found in range descriptors e.g. collection was not
	// run on those stores because they are dead or because of human error.
	MissingStores []roachpb.StoreID

	// PlannedUpdates contains detailed update info about each planned update.
	// This information is presented to user for action confirmation and can
	// contain details that are not needed for actual plan application.
	PlannedUpdates []ReplicaUpdateReport
	// UpdatedNodes contains information about nodes with their stores where plan
	// needs to be applied. Stores are sorted in ascending order.
	UpdatedNodes map[roachpb.NodeID][]roachpb.StoreID

	// Problems contains any keyspace coverage problems
	Problems []Problem
}

// Error returns error if there are problems with the cluster that could make
// recovery "unsafe". Those errors could be ignored in dire situation and
// produce cluster that is partially unblocked but can have inconsistencies.
func (p PlanningReport) Error() error {
	if len(p.Problems) > 0 {
		return &RecoveryError{p.Problems}
	}
	return nil
}

// ReplicaUpdateReport contains detailed info about changes planned for
// particular replica that was chosen as a designated survivor for the range.
// This information is more detailed than update plan and collected for
// reporting purposes.
// While information in update plan is meant for loqrecovery components, Report
// is meant for cli interaction to keep user informed of changes.
type ReplicaUpdateReport struct {
	RangeID                    roachpb.RangeID
	StartKey                   roachpb.RKey
	Replica                    roachpb.ReplicaDescriptor
	OldReplica                 roachpb.ReplicaDescriptor
	StoreID                    roachpb.StoreID
	DiscardedAvailableReplicas roachpb.ReplicaSet
	DiscardedDeadReplicas      roachpb.ReplicaSet
}

// PlanReplicas analyzes captured replica information to determine which
// replicas could serve as dedicated survivors in ranges where quorum was
// lost.
// Devised plan doesn't guarantee data consistency after the recovery, only
// the fact that ranges could progress and subsequently perform up-replication.
// Moreover, if we discover conflicts in the range coverage or range descriptors
// they would be returned in the report, but that would not prevent us from
// making an "unsafe" recovery plan.
// An error is returned in case of unrecoverable error in the collected data
// that prevents creation of any sane plan or correctable user error.
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
	report.PresentStores = storeSliceFromSet(availableStoreIDs)
	report.MissingStores = storeSliceFromSet(missingStores)

	replicasByRangeID := groupReplicasByRangeID(replicas)
	// proposedSurvivors contain decisions for all ranges in keyspace. it
	// contains ranges that lost quorum as well as the ones that didn't.
	var proposedSurvivors []rankedReplicas
	for _, rangeReplicas := range replicasByRangeID {
		proposedSurvivors = append(proposedSurvivors, rankReplicasBySurvivability(rangeReplicas))
	}
	problems, err := checkKeyspaceCovering(proposedSurvivors)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}

	var plan []loqrecoverypb.ReplicaUpdate
	for _, p := range proposedSurvivors {
		report.TotalReplicas += len(p)
		u, ok := makeReplicaUpdateIfNeeded(ctx, p, availableStoreIDs)
		if ok {
			problems = append(problems, checkDescriptor(p)...)
			plan = append(plan, u)
			report.DiscardedNonSurvivors += len(p) - 1
			report.PlannedUpdates = append(report.PlannedUpdates, makeReplicaUpdateReport(ctx, p, u))
			updatedLocations.add(u.NodeID(), u.StoreID())
			log.Infof(ctx, "replica has lost quorum, recovering: %s -> %s", p.survivor().Desc, u)
		} else {
			log.Infof(ctx, "range r%d didn't lose quorum", p.rangeID())
		}
	}

	sort.Slice(problems, func(i, j int) bool {
		return problems[i].Span().Key.Compare(problems[j].Span().Key) < 0
	})
	report.Problems = problems
	report.UpdatedNodes = updatedLocations.asMapOfSlices()
	return loqrecoverypb.ReplicaUpdatePlan{Updates: plan}, report, nil
}

// validateReplicaSets evaluates provided set of replicas and an optional
// deadStoreIDs request and produces consistency info containing:
// availableStores  - all storeIDs for which info was collected, i.e.
//
//	(barring operator error) the conclusive list of all
//	remaining stores in the cluster.
//
// missingStores    - all dead stores (stores that are referenced by replicas,
//
//	but not present in any of descriptors)
//
// If inconsistency is found e.g. no info was provided for a store but it is
// not present in explicit deadStoreIDs list, error is returned.
func validateReplicaSets(
	replicas []loqrecoverypb.ReplicaInfo, deadStores []roachpb.StoreID,
) (availableStoreIDs, missingStoreIDs storeIDSet, _ error) {
	// Populate availableStoreIDs with all StoreIDs from which we collected info
	// and, populate missingStoreIDs with all StoreIDs referenced in replica
	// descriptors for which no information was collected.
	availableStoreIDs = make(storeIDSet)
	missingStoreIDs = make(storeIDSet)
	for _, replicaDescriptor := range replicas {
		availableStoreIDs[replicaDescriptor.StoreID] = struct{}{}
		for _, replicaDesc := range replicaDescriptor.Desc.InternalReplicas {
			missingStoreIDs[replicaDesc.StoreID] = struct{}{}
		}
	}
	// The difference between all referenced StoreIDs (missingStoreIDs) and the
	// present StoreIDs (presentStoreIDs) should exactly equal the user-provided
	// list of dead stores (deadStores), and the former must be a superset of the
	// latter (since each descriptor found on a store references that store).
	// Verify all of these conditions and error out if one of them does not hold.
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
		groupedRanges[descriptor.Desc.RangeID] = append(
			groupedRanges[descriptor.Desc.RangeID], descriptor)
	}
	return groupedRanges
}

// rankedReplicas contains replica resolution details e.g. preferred replica as
// well as extra info to produce a report of the planned action.
type rankedReplicas []loqrecoverypb.ReplicaInfo

func (p rankedReplicas) startKey() roachpb.RKey {
	return p[0].Desc.StartKey
}

func (p rankedReplicas) endKey() roachpb.RKey {
	return p[0].Desc.EndKey
}

func (p rankedReplicas) span() roachpb.Span {
	return roachpb.Span{Key: roachpb.Key(p[0].Desc.StartKey), EndKey: roachpb.Key(p[0].Desc.EndKey)}
}

func (p rankedReplicas) rangeID() roachpb.RangeID {
	return p[0].Desc.RangeID
}

func (p rankedReplicas) nodeID() roachpb.NodeID {
	return p[0].NodeID
}

func (p rankedReplicas) storeID() roachpb.StoreID {
	return p[0].StoreID
}

func (p rankedReplicas) survivor() *loqrecoverypb.ReplicaInfo {
	return &p[0]
}

// rankReplicasBySurvivability given a slice of replicas for the range from
// all live stores, pick one to survive recovery. if progress can be made,
// still pick one replica so that it could be used to do key covering
// validation.
// Note that replicas argument would be sorted in process of picking a
// survivor
func rankReplicasBySurvivability(replicas []loqrecoverypb.ReplicaInfo) rankedReplicas {
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
		if voterI > voterJ {
			return true
		}
		if voterI < voterJ {
			return false
		}
		if replicas[i].RaftAppliedIndex > replicas[j].RaftAppliedIndex {
			return true
		}
		if replicas[i].RaftAppliedIndex < replicas[j].RaftAppliedIndex {
			return false
		}
		return replicas[i].StoreID > replicas[j].StoreID
	})
	return replicas
}

// checkKeyspaceCovering given slice of all survivor ranges, checks that full
// keyspace is covered.
// Note that slice would be sorted in process of the check.
func checkKeyspaceCovering(replicas []rankedReplicas) ([]Problem, error) {
	sort.Slice(replicas, func(i, j int) bool {
		// We only need to sort replicas in key order to detect
		// key collisions or gaps, but if we have matching keys
		// sort becomes unstable which makes it produce different
		// errors on different runs on the same data. To address
		// that, we also add RangeID as a sorting criteria as a
		// second level key to add stability.
		if replicas[i].startKey().Less(replicas[j].startKey()) {
			return true
		}
		if replicas[i].startKey().Equal(replicas[j].startKey()) {
			return replicas[i].rangeID() < replicas[j].rangeID()
		}
		return false
	})
	var problems []Problem
	prevDesc := rankedReplicas{{Desc: roachpb.RangeDescriptor{}}}
	// We validate that first range starts at min key, last range ends at max key
	// and that for every range start key is equal to end key of previous range.
	// If any of those conditions fail, we record this as a problem to indicate
	// there's a gap between ranges or an overlap between two or more ranges.
	for _, rankedDescriptors := range replicas {
		// We need to take special care of the case where the survivor replica is
		// outgoing voter. It cannot be designated, as the only replication change
		// it could make is to turn itself into a learner, at which point the range
		// is completely messed up. If it is not a stale replica of some sorts,
		// then that would be a gap in keyspace coverage.
		r, err := rankedDescriptors.survivor().Replica()
		if err != nil {
			return nil, err
		}
		if !r.IsVoterNewConfig() {
			continue
		}
		switch {
		case rankedDescriptors.startKey().Less(prevDesc.endKey()):
			start := keyMax(rankedDescriptors.startKey(), prevDesc.startKey())
			end := keyMin(rankedDescriptors.endKey(), prevDesc.endKey())
			problems = append(problems, keyspaceOverlap{
				span:       roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)},
				range1:     prevDesc.rangeID(),
				range1Span: prevDesc.span(),
				range2:     rankedDescriptors.rangeID(),
				range2Span: rankedDescriptors.span(),
			})
		case prevDesc.endKey().Less(rankedDescriptors.startKey()):
			problems = append(problems, keyspaceGap{
				span: roachpb.Span{
					Key:    roachpb.Key(prevDesc.endKey()),
					EndKey: roachpb.Key(rankedDescriptors.startKey()),
				},
				range1:     prevDesc.rangeID(),
				range1Span: prevDesc.span(),
				range2:     rankedDescriptors.rangeID(),
				range2Span: rankedDescriptors.span(),
			})
		}
		// We want to advance previous range details only when new range will
		// advance upper bound. This is not always the case as theoretically ranges
		// could be "nested" or range could be an earlier version encompassing LHS
		// and RHS parts.
		if prevDesc.endKey().Less(rankedDescriptors.endKey()) {
			prevDesc = rankedDescriptors
		}
	}
	if !prevDesc.endKey().Equal(roachpb.RKeyMax) {
		problems = append(problems, keyspaceGap{
			span:       roachpb.Span{Key: roachpb.Key(prevDesc.endKey()), EndKey: roachpb.KeyMax},
			range1:     prevDesc.rangeID(),
			range1Span: prevDesc.span(),
			range2:     roachpb.RangeID(0),
			range2Span: roachpb.Span{Key: roachpb.KeyMax, EndKey: roachpb.KeyMax},
		})
	}

	return problems, nil
}

// makeReplicaUpdateIfNeeded if candidate range can't make progress, create an
// update using preferred replica.
// Returns a replica update and a flag indicating if update needs to be
// performed.
// For replicas that can make progress return empty update and false to exclude
// range from update plan.
func makeReplicaUpdateIfNeeded(
	ctx context.Context, p rankedReplicas, liveStoreIDs storeIDSet,
) (loqrecoverypb.ReplicaUpdate, bool) {
	if p.survivor().Desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
		_, ok := liveStoreIDs[rep.StoreID]
		return ok
	}) {
		return loqrecoverypb.ReplicaUpdate{}, false
	}

	// We want to have replicaID which is greater or equal nextReplicaID across
	// all available replicas. We'll use that as a base to bump it by arbitrary
	// number to avoid potential conflicts with other replicas applying
	// uncommitted raft log with descriptor updates.
	nextReplicaID := p.survivor().Desc.NextReplicaID
	for _, r := range p[1:] {
		if r.Desc.NextReplicaID > nextReplicaID {
			nextReplicaID = r.Desc.NextReplicaID
		}
	}

	replica, err := p.survivor().Replica()
	if err != nil {
		// We don't expect invalid replicas reaching this stage because we will err
		// out on earlier stages. This is covered by invalid input tests and if we
		// ended up here that means tests are not run, or code changed sufficiently
		// and both checks and tests were lost.
		log.Fatalf(ctx, "unexpected invalid replica info while making recovery plan, "+
			"we should never have unvalidated descriptors at planning stage, they must be detected "+
			"while performing keyspace coverage check: %s", err)
	}

	// The range needs to be recovered and this replica is a designated survivor.
	// To recover the range rewrite it as having a single replica:
	// - Rewrite the replicas list.
	// - Bump the replica ID so that in case there are other surviving nodes that
	//   were members of the old incarnation of the range, they no longer
	//   recognize this revived replica (because they are not in sync with it).
	return loqrecoverypb.ReplicaUpdate{
		RangeID:      p.rangeID(),
		StartKey:     loqrecoverypb.RecoveryKey(p.startKey()),
		OldReplicaID: replica.ReplicaID,
		NewReplica: roachpb.ReplicaDescriptor{
			NodeID:    p.nodeID(),
			StoreID:   p.storeID(),
			ReplicaID: nextReplicaID + nextReplicaIDIncrement,
		},
		NextReplicaID: nextReplicaID + nextReplicaIDIncrement + 1,
	}, true
}

// checkDescriptor analyses descriptor and raft log of surviving replica to find
// if its state is safe to perform recovery from. Currently only unapplied
// descriptor changes that either remove replica, or change KeySpan (splits or
// merges) are treated as unsafe.
func checkDescriptor(rankedDescriptors rankedReplicas) (problems []Problem) {
	// We now need to analyze if range is unsafe to recover due to pending
	// changes for the range descriptor in the raft log, but we only want to
	// do that if range needs to be recovered.
	for _, change := range rankedDescriptors.survivor().RaftLogDescriptorChanges {
		switch change.ChangeType {
		case loqrecoverypb.DescriptorChangeType_Split:
			problems = append(problems, rangeSplit{
				rangeID:    rankedDescriptors.rangeID(),
				span:       rankedDescriptors.span(),
				rHSRangeID: change.OtherDesc.RangeID,
				rHSRangeSpan: roachpb.Span{
					Key:    roachpb.Key(change.OtherDesc.StartKey),
					EndKey: roachpb.Key(change.OtherDesc.EndKey),
				},
			})
		case loqrecoverypb.DescriptorChangeType_Merge:
			problems = append(problems, rangeMerge{
				rangeID:    rankedDescriptors.rangeID(),
				span:       rankedDescriptors.span(),
				rHSRangeID: change.OtherDesc.RangeID,
				rHSRangeSpan: roachpb.Span{
					Key:    roachpb.Key(change.OtherDesc.StartKey),
					EndKey: roachpb.Key(change.OtherDesc.EndKey),
				},
			})
		case loqrecoverypb.DescriptorChangeType_ReplicaChange:
			// Check if our own replica is being removed as part of descriptor
			// change.
			_, ok := change.Desc.GetReplicaDescriptor(rankedDescriptors.storeID())
			if !ok {
				problems = append(problems, rangeReplicaRemoval{
					rangeID: rankedDescriptors.rangeID(),
					span:    rankedDescriptors.span(),
				})
			}
		}
	}
	return
}

// makeReplicaUpdateReport creates a detailed report of changes that needs to
// be performed on range. It uses decision as well as information about all
// replicas of range to provide information about what is being discarded and
// how new replica would be configured.
func makeReplicaUpdateReport(
	ctx context.Context, p rankedReplicas, update loqrecoverypb.ReplicaUpdate,
) ReplicaUpdateReport {
	oldReplica, err := p.survivor().Replica()
	if err != nil {
		// We don't expect invalid replicas reaching this stage because we will err
		// out on earlier stages. This is covered by invalid input tests and if we
		// ended up here that means tests are not run, or code changed sufficiently
		// and both checks and tests were lost.
		log.Fatalf(ctx, "unexpected invalid replica info while making recovery plan: %s", err)
	}

	// Replicas that belonged to unavailable nodes based on surviving range
	// descriptor.
	discardedDead := p.survivor().Desc.Replicas()
	discardedDead.RemoveReplica(update.NodeID(), update.StoreID())
	// Replicas that we collected info about for the range, but decided they are
	// not preferred choice.
	discardedAvailable := roachpb.ReplicaSet{}
	for _, replica := range p[1:] {
		discardedDead.RemoveReplica(replica.NodeID, replica.StoreID)
		r, _ := replica.Desc.GetReplicaDescriptor(replica.StoreID)
		discardedAvailable.AddReplica(r)
	}

	return ReplicaUpdateReport{
		RangeID:                    p.rangeID(),
		StartKey:                   p.startKey(),
		OldReplica:                 oldReplica,
		Replica:                    update.NewReplica,
		StoreID:                    p.storeID(),
		DiscardedDeadReplicas:      discardedDead,
		DiscardedAvailableReplicas: discardedAvailable,
	}
}
