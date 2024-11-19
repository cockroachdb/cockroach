// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// nextReplicaIDIncrement defines how much forward we want to advance an ID of
// the designated surviving replica to avoid any potential replicaID conflicts
// in case we've picked not the most up-to-date replica.
const nextReplicaIDIncrement = 10

type NodeStores struct {
	NodeID   roachpb.NodeID
	StoreIDs []roachpb.StoreID
}

// PlanningReport provides aggregate stats and details of replica updates that
// is used for user confirmation.
type PlanningReport struct {
	// TotalReplicas is the number of replicas that were found on nodes present
	// in the cluster.
	TotalReplicas int
	// DiscardedNonSurvivors is the number of replicas from ranges that lost
	// quorum that we decided not to use according to selection criteria used by
	// planner.
	DiscardedNonSurvivors int
	// TODO(oleg): track total analyzed range count in subsequent version

	// PresentStores is deduced list of stores that we collected replica info
	// from. This set is filled from analyzed descriptors and may not strictly
	// match stores on which collection was run if some stores are empty.
	PresentStores []roachpb.StoreID
	// MissingNodes is a sorted slice of missing nodes and their corresponding
	// stores. This list is deduced from stores found in range descriptors of
	// replicas, but not found in actual replicas. e.g. no collection was run
	// on those stores or nodes.
	MissingNodes []NodeStores

	// PlannedUpdates contains detailed update info about each planned update.
	// This information is presented to user for action confirmation and can
	// contain details that are not needed for actual plan application.
	PlannedUpdates []PlannedReplicaUpdate
	// UpdatedNodes contains information about nodes with their stores where plan
	// needs to be applied. Nodes and stores are sorted in ascending order.
	UpdatedNodes []NodeStores

	// Problems contains any keyspace coverage problems or range descriptor
	// inconsistency between storage info and range metadata.
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

// PlannedReplicaUpdate contains detailed info about changes planned for
// particular replica that was chosen as a designated survivor for the range.
type PlannedReplicaUpdate struct {
	RangeID    roachpb.RangeID
	StartKey   roachpb.RKey
	NewReplica roachpb.ReplicaDescriptor
	OldReplica roachpb.ReplicaDescriptor
	StoreID    roachpb.StoreID
	// Discarded replicas based on survivor descriptor content.
	DiscardedAvailableReplicas roachpb.ReplicaSet
	DiscardedDeadReplicas      roachpb.ReplicaSet
	DiscardedLeaseholders      roachpb.ReplicaSet
	// Discarded actual replicas from stores (including stale).
	DiscardedReplicasCount int
	NextReplicaID          roachpb.ReplicaID
}

func (u PlannedReplicaUpdate) asReplicaUpdate() loqrecoverypb.ReplicaUpdate {
	return loqrecoverypb.ReplicaUpdate{
		RangeID:       u.RangeID,
		StartKey:      loqrecoverypb.RecoveryKey(u.StartKey),
		OldReplicaID:  u.OldReplica.ReplicaID,
		NewReplica:    u.NewReplica,
		NextReplicaID: u.NextReplicaID,
	}
}

// PlanReplicas analyzes captured replica information to determine which
// replicas could serve as dedicated survivors in ranges where quorum was
// lost.
// Devised plan doesn't guarantee data consistency after the recovery, only
// the fact that ranges could progress and subsequently perform up-replication.
// Moreover, if we discover conflicts in the range coverage or range descriptors
// they would be returned in the report, but that would not prevent us from
// making an "unsafe" recovery plan.
// clusterInfo contains information about replicas that survived and optional
// range descriptors collected from meta rages. If meta information is available
// because meta ranges didn't lose quorum, then we can avoid a lot of complexity
// in figuring out obsolete replicas and inconsistencies stemming from replica
// information not being atomically captured.
// An error is returned in case of unrecoverable error in the collected data
// that prevents creation of any sane plan or correctable user error.
// Note on versions in clusterInfo.
// Plan expects info collection and info reading functions to ensure that
// provided clusterInfo is compatible with current binary.
// It is planners responsibility to only use features supported by cluster
// version specified in the clusterInfo. Newer features must not be used as it
// may break the cluster if they are not backward compatible. Target versions
// is copied into resulting plan so that cluster could reject higher versions
// of plans.
func PlanReplicas(
	ctx context.Context,
	clusterInfo loqrecoverypb.ClusterReplicaInfo,
	deadStoreIDs []roachpb.StoreID,
	deadNodeIDs []roachpb.NodeID,
	uuidGen uuid.Generator,
) (loqrecoverypb.ReplicaUpdatePlan, PlanningReport, error) {
	planID := uuidGen.NewV4()
	var replicas []loqrecoverypb.ReplicaInfo
	for _, node := range clusterInfo.LocalInfo {
		replicas = append(replicas, node.Replicas...)
	}
	availableStoreIDs, deadNodes, err := validateReplicaSets(replicas, deadStoreIDs, deadNodeIDs)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}
	replicasByRangeID := groupReplicasByRangeID(replicas)

	var report PlanningReport
	report.MissingNodes = deadNodes.asSortedSlice()
	report.PresentStores = availableStoreIDs.storeSliceFromSet()
	report.TotalReplicas = len(replicas)

	if rangeDescriptorsComplete(clusterInfo.Descriptors) {
		report.PlannedUpdates, report.Problems, err = planReplicasWithMeta(ctx, clusterInfo.Descriptors,
			replicasByRangeID, availableStoreIDs)
	} else {
		report.PlannedUpdates, report.Problems, err = planReplicasWithoutMeta(ctx, replicasByRangeID,
			availableStoreIDs)
	}
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, PlanningReport{}, err
	}

	updates := make([]loqrecoverypb.ReplicaUpdate, len(report.PlannedUpdates))
	updatedLocations := make(locationsMap)
	nodesWithDiscardedLeaseholders := make(map[roachpb.NodeID]interface{})
	for i, u := range report.PlannedUpdates {
		updates[i] = u.asReplicaUpdate()
		updatedLocations.add(u.NewReplica.NodeID, u.NewReplica.StoreID)
		report.DiscardedNonSurvivors += u.DiscardedReplicasCount
		for _, r := range u.DiscardedLeaseholders.Descriptors() {
			nodesWithDiscardedLeaseholders[r.NodeID] = struct{}{}
		}
	}
	report.UpdatedNodes = updatedLocations.asSortedSlice()

	var decommissionNodeIDs []roachpb.NodeID
	for id := range deadNodes {
		decommissionNodeIDs = append(decommissionNodeIDs, id)
	}
	slices.Sort(decommissionNodeIDs)

	var staleLeaseholderNodes []roachpb.NodeID
	for node := range nodesWithDiscardedLeaseholders {
		if _, ok := updatedLocations[node]; !ok {
			staleLeaseholderNodes = append(staleLeaseholderNodes, node)
		}
	}
	slices.Sort(staleLeaseholderNodes)

	return loqrecoverypb.ReplicaUpdatePlan{
		Updates:                 updates,
		PlanID:                  planID,
		DecommissionedNodeIDs:   decommissionNodeIDs,
		ClusterID:               clusterInfo.ClusterID,
		StaleLeaseholderNodeIDs: staleLeaseholderNodes,
		Version:                 clusterInfo.Version,
	}, report, err
}

func planReplicasWithMeta(
	ctx context.Context,
	descriptors []roachpb.RangeDescriptor,
	replicasByRangeID map[roachpb.RangeID][]loqrecoverypb.ReplicaInfo,
	availableStoreIDs storeIDSet,
) ([]PlannedReplicaUpdate, []Problem, error) {
	var updates []PlannedReplicaUpdate
	var problems []Problem
	for _, d := range descriptors {
		if !d.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
			_, ok := availableStoreIDs[rep.StoreID]
			return ok
		}) {
			remainingReplicas, replicasFound := replicasByRangeID[d.RangeID]
			if !replicasFound {
				problems = append(problems, allReplicasLost{
					rangeID: d.RangeID,
					span:    d.KeySpan().AsRawSpanWithNoLocals(),
				})
				continue
			}
			rankedRangeReplicas := rankReplicasBySurvivability(remainingReplicas)
			if !d.StartKey.Equal(rankedRangeReplicas.startKey()) || !d.EndKey.Equal(rankedRangeReplicas.endKey()) {
				problems = append(problems, rangeMetaMismatch{
					rangeID:  0,
					span:     rankedRangeReplicas.span(),
					metaSpan: d.KeySpan().AsRawSpanWithNoLocals(),
				})
				continue
			}
			u, ok := makeReplicaUpdateIfNeeded(ctx, rankedRangeReplicas, availableStoreIDs)
			if !ok {
				continue
			}
			problems = append(problems, checkDescriptor(rankedRangeReplicas)...)
			updates = append(updates, u)
			log.Infof(ctx, "replica has lost quorum, recovering: %s -> %s",
				rankedRangeReplicas.survivor().Desc, u.NewReplica)
		}
	}

	slices.SortFunc(problems, func(a, b Problem) int {
		return a.Span().Key.Compare(b.Span().Key)
	})
	return updates, problems, nil
}

func planReplicasWithoutMeta(
	ctx context.Context,
	replicasByRangeID map[roachpb.RangeID][]loqrecoverypb.ReplicaInfo,
	availableStoreIDs storeIDSet,
) ([]PlannedReplicaUpdate, []Problem, error) {
	// proposedSurvivors contain decisions for all ranges in keyspace. it
	// contains ranges that lost quorum as well as the ones that didn't.
	var proposedSurvivors []rankedReplicas
	for _, rangeReplicas := range replicasByRangeID {
		proposedSurvivors = append(proposedSurvivors, rankReplicasBySurvivability(rangeReplicas))
	}
	problems, err := checkKeyspaceCovering(proposedSurvivors)
	if err != nil {
		return nil, nil, err
	}

	var updates []PlannedReplicaUpdate
	for _, p := range proposedSurvivors {
		u, ok := makeReplicaUpdateIfNeeded(ctx, p, availableStoreIDs)
		if !ok {
			continue
		}
		problems = append(problems, checkDescriptor(p)...)
		updates = append(updates, u)
	}

	slices.SortFunc(problems, func(a, b Problem) int {
		return a.Span().Key.Compare(b.Span().Key)
	})
	return updates, problems, nil
}

// rangeDescriptorsComplete verifies that descriptor info covers keyspace
// completely. We can have partial coverage if scan failed mid-way and the tail
// of meta range is unavailable.
func rangeDescriptorsComplete(descriptors []roachpb.RangeDescriptor) bool {
	if len(descriptors) < 1 {
		return false
	}
	prevKey := roachpb.RKeyMin
	for _, d := range descriptors {
		if !d.StartKey.Equal(prevKey) {
			return false
		}
		prevKey = d.EndKey
	}
	return prevKey.Equal(keys.MaxKey)
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
	replicas []loqrecoverypb.ReplicaInfo,
	deadStoreIDs []roachpb.StoreID,
	deadNodeIDs []roachpb.NodeID,
) (availableStoreIDs storeIDSet, deadNodes locationsMap, _ error) {
	allNodes := make(locationsMap)
	availableNodes := make(locationsMap)

	// Populate availableStoreIDs with all StoreIDs from which we collected info
	// and, populate missingStoreIDs with all StoreIDs referenced in replica
	// descriptors for which no information was collected.
	for _, replicaDescriptor := range replicas {
		availableNodes.add(replicaDescriptor.NodeID, replicaDescriptor.StoreID)
		for _, replicaDesc := range replicaDescriptor.Desc.InternalReplicas {
			allNodes.add(replicaDesc.NodeID, replicaDesc.StoreID)
		}
	}

	availableStoreIDs = availableNodes.stores()
	deadNodes = allNodes.diff(availableNodes)

	// If dead stores were provided check that no replicas are provided for those
	// stores and that all detected dead stores are present.
	if len(deadStoreIDs) > 0 {
		ds := idSetFromSlice(deadStoreIDs)
		if susp := availableStoreIDs.intersect(ds); len(susp) > 0 {
			return nil, nil, errors.Errorf(
				"stores %s are listed as dead, but replica info is provided for them",
				susp.joinStoreIDs())
		}
		dds := deadNodes.stores()
		if nonList := dds.diff(ds); len(nonList) > 0 {
			return nil, nil, errors.Errorf(
				"information about stores %s were not provided, nor they are listed as dead",
				nonList.joinStoreIDs())
		}
	}

	// If dead nodes were provided check that no replicas are provided for those
	// nodes and that all detected dead nodes are present.
	if len(deadNodeIDs) > 0 {
		dn := locationsFromSlice(deadNodeIDs)
		if susp := availableNodes.intersect(dn); len(susp) > 0 {
			return nil, nil, errors.Errorf(
				"nodes %s are listed as dead, but replica info is provided for them",
				susp.joinNodeIDs())
		}
		if nonList := deadNodes.diff(dn); len(nonList) > 0 {
			return nil, nil, errors.Errorf(
				"information about nodes %s were not provided, nor they are listed as dead",
				nonList.joinNodeIDs())
		}
	}

	return availableStoreIDs, deadNodes, nil
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
	isVoter := func(desc loqrecoverypb.ReplicaInfo) bool {
		for _, replica := range desc.Desc.InternalReplicas {
			if replica.StoreID == desc.StoreID {
				return replica.IsVoterNewConfig()
			}
		}
		// This is suspicious, our descriptor is not in replicas. Panic maybe?
		return false
	}
	b2i := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	slices.SortFunc(replicas, func(a, b loqrecoverypb.ReplicaInfo) int {
		// When finding the best suitable replica evaluate 3 conditions in order:
		//  - replica is a voter
		//  - replica has the higher range committed index
		//  - replica thinks it is the leaseholder
		//  - replica has the higher store id
		//
		// Note: that an outgoing voter cannot be designated, as the only
		// replication change it could make is to turn itself into a learner, at
		// which point the range is completely messed up.
		return -cmp.Or(
			cmp.Compare(b2i(isVoter(a)), b2i(isVoter(b))),
			cmp.Compare(a.RaftAppliedIndex, b.RaftAppliedIndex),
			cmp.Compare(b2i(a.LocalAssumesLeaseholder), b2i(b.LocalAssumesLeaseholder)),
			cmp.Compare(a.StoreID, b.StoreID),
		)
	})
	return replicas
}

// checkKeyspaceCovering given slice of all survivor ranges, checks that full
// keyspace is covered.
// Note that slice would be sorted in process of the check.
func checkKeyspaceCovering(replicas []rankedReplicas) ([]Problem, error) {
	slices.SortFunc(replicas, func(a, b rankedReplicas) int {
		// We only need to sort replicas in key order to detect
		// key collisions or gaps, but if we have matching keys
		// sort becomes unstable which makes it produce different
		// errors on different runs on the same data. To address
		// that, we also add RangeID as a sorting criteria as a
		// second level key to add stability.
		return cmp.Or(
			a.startKey().Compare(b.startKey()),
			cmp.Compare(a.rangeID(), b.rangeID()),
		)
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
) (PlannedReplicaUpdate, bool) {
	if p.survivor().Desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
		_, ok := liveStoreIDs[rep.StoreID]
		return ok
	}) {
		return PlannedReplicaUpdate{}, false
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

	// Replicas that belonged to unavailable nodes based on surviving range
	// descriptor.
	discardedDead := p.survivor().Desc.Replicas().DeepCopy()
	discardedDead.RemoveReplica(replica.NodeID, replica.StoreID)
	// Replicas that belong to available nodes, but discarded as they are
	// not preferred choice.
	discardedAvailable := roachpb.ReplicaSet{}
	discardedLeaseholders := roachpb.ReplicaSet{}
	for _, storeReplica := range p[1:] {
		discardedDead.RemoveReplica(storeReplica.NodeID, storeReplica.StoreID)
		r, _ := storeReplica.Desc.GetReplicaDescriptor(storeReplica.StoreID)
		discardedAvailable.AddReplica(r)
		if storeReplica.LocalAssumesLeaseholder {
			discardedLeaseholders.AddReplica(r)
		}
	}

	// The range needs to be recovered and this replica is a designated survivor.
	// To recover the range rewrite it as having a single replica:
	// - Rewrite the replicas list.
	// - Bump the replica ID so that in case there are other surviving nodes that
	//   were members of the old incarnation of the range, they no longer
	//   recognize this revived replica (because they are not in sync with it).
	return PlannedReplicaUpdate{
		RangeID:  p.rangeID(),
		StartKey: p.startKey(),
		NewReplica: roachpb.ReplicaDescriptor{
			NodeID:    p.nodeID(),
			StoreID:   p.storeID(),
			ReplicaID: nextReplicaID + nextReplicaIDIncrement,
		},
		OldReplica:                 replica,
		StoreID:                    p.storeID(),
		DiscardedAvailableReplicas: discardedAvailable,
		DiscardedDeadReplicas:      discardedDead,
		DiscardedLeaseholders:      discardedLeaseholders,
		DiscardedReplicasCount:     len(p) - 1,
		NextReplicaID:              nextReplicaID + nextReplicaIDIncrement + 1,
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
			// Any change of descriptor even if it doesn't change current replica
			// is not safe to apply if we change replica id.
			// Until we have a way to remove this change, we should treat this as
			// a problem.
			problems = append(problems, rangeReplicaChange{
				rangeID: rankedDescriptors.rangeID(),
				span:    rankedDescriptors.span(),
			})
		}
	}
	return
}
