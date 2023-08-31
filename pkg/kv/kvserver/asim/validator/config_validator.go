// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// processClusterInfo handles region data and returns: 1. A map of zone names to
// their respective region names 2. A map of zone names to the number of
// available nodes in the zones 3. A map of region names to the number of
// available nodes in the regions
func processClusterInfo(
	regions []state.Region,
) (map[string]string, map[string]int, map[string]int, int) {
	zone := map[string]int{}
	region := map[string]int{}
	total := 0
	zoneToRegion := map[string]string{}

	for _, r := range regions {
		for _, z := range r.Zones {
			zoneToRegion[z.Name] = r.Name
			zone[z.Name] += z.NodeCount
			region[r.Name] += z.NodeCount
			total += z.NodeCount
		}
	}
	return zoneToRegion, zone, region, total
}

type allocationDetailsAtEachLevel struct {
	unassigned        int
	assignedVoters    int
	assignedNonVoters int
}

// tryToAddVoters attempts to assign numOfVoters from the available nodes as
// voters. It returns true if there are sufficient available nodes to be
// assigned as voters, and false otherwise.
func (a *allocationDetailsAtEachLevel) tryToAddVoters(numOfVoters int) (success bool) {
	if a.unassigned < numOfVoters {
		return false
	}
	a.unassigned -= numOfVoters
	a.assignedVoters += numOfVoters
	return true
}

// tryToAddNonVoters attempts to assign numOfNonVoters from the available nodes
// as nonvoters. It returns true if there are sufficient available nodes to be
// assigned as voters, and false otherwise.
func (a *allocationDetailsAtEachLevel) tryToAddNonVoters(numOfNonVoters int) (success bool) {
	if a.unassigned < numOfNonVoters {
		return false
	}
	a.unassigned -= numOfNonVoters
	a.assignedNonVoters += numOfNonVoters
	return true
}

// promoteNonVoters promotes numOfNonVotersToPromote of nonvoters to voters.
func (a *allocationDetailsAtEachLevel) promoteNonVoters(numOfNonVotersToPromote int) {
	if a.assignedNonVoters < numOfNonVotersToPromote {
		panic("insufficient non-voters for promotion. This is unexpected as computeNecessaryChanges " +
			"should calculate number of non-voters for promotion correctly.")
	}
	a.assignedNonVoters -= numOfNonVotersToPromote
	a.assignedVoters += numOfNonVotersToPromote
}

type mockAllocator struct {
	zoneToRegion map[string]string
	zone         map[string]allocationDetailsAtEachLevel
	region       map[string]allocationDetailsAtEachLevel
	cluster      allocationDetailsAtEachLevel
}

// newMockAllocator creates a mock allocator based on the provided cluster
// setup. mockAllocator is designed to determine if a config can be satisfied by
// trying to assign replicas in a way that meet the constraints. Note that since
// isSatisfiable directly alters mockAlloactor fields, a new mock allocator
// should be initialized for each isSatisfiable call.
func newMockAllocator(
	zoneToRegion map[string]string, zone map[string]int, region map[string]int, total int,
) mockAllocator {
	m := mockAllocator{
		zoneToRegion: zoneToRegion,
		zone:         map[string]allocationDetailsAtEachLevel{},
		region:       map[string]allocationDetailsAtEachLevel{},
		cluster: allocationDetailsAtEachLevel{
			unassigned: total,
		},
	}

	for k, v := range zone {
		m.zone[k] = allocationDetailsAtEachLevel{
			unassigned: v,
		}
	}

	for k, v := range region {
		m.region[k] = allocationDetailsAtEachLevel{
			unassigned: v,
		}
	}
	return m
}

type constraint struct {
	requiredReplicas int
	requiredVoters   int
}

// validateConstraint returns nil if the constraint is feasible and error
// (not `nil`) otherwise.
func (m *mockAllocator) validateConstraint(c roachpb.Constraint) error {
	if c.Type == roachpb.Constraint_PROHIBITED {
		return errors.New("constraints marked as Constraint_PROHIBITED are unsupported")
	}
	switch c.Key {
	case "zone":
		_, ok := m.zone[c.Value]
		if !ok {
			return errors.Newf("zone constraint value %s is not found in the cluster set up", c.Value)
		}
	case "region":
		_, ok := m.region[c.Value]
		if !ok {
			return errors.Newf("region constraint value %s is not found in the cluster set up", c.Value)
		}
	default:
		return errors.New("only zone and region constraint keys are supported")
	}
	return nil
}

// processConstraintsHelper is a helper function for processConstraint to handle
// the processing logic for both replica and voter constraints. It centralizes
// the validation and updating of the given zoneConstraints and
// regionConstraints. If all constraints are feasible, it returns nil.
// Otherwise, it returns error (not `nil`).
func (m *mockAllocator) processConstraintsHelper(
	constraintsConjunction []roachpb.ConstraintsConjunction,
	isVoterConstraint bool,
	totalNumOfVotersOrReplicas int,
	zoneConstraints map[string]constraint,
	regionConstraints map[string]constraint,
) error {
	for _, cc := range constraintsConjunction {
		required := int(cc.NumReplicas)
		if cc.NumReplicas == 0 {
			// If NumReplicas is zero, the constraints will be applied to all voters /
			// replicas.
			required = totalNumOfVotersOrReplicas
		}
		for _, c := range cc.Constraints {
			if err := m.validateConstraint(c); err != nil {
				return err
			}
			if c.Key == "zone" {
				zc := zoneConstraints[c.Value]
				if isVoterConstraint {
					zc.requiredVoters = required
				} else {
					zc.requiredReplicas = required
				}
				zoneConstraints[c.Value] = zc
			} else if c.Key == "region" {
				rc := regionConstraints[c.Value]
				if isVoterConstraint {
					rc.requiredVoters = required
				} else {
					rc.requiredReplicas = required
				}
				regionConstraints[c.Value] = rc
			}
		}
	}
	return nil
}

// processConstraints validates and extracts region and zone-specific replica
// and voter constraints, storing them in two separate maps. If certain
// constraints fail the validation, they are considered as infeasible. In such
// cases, error(not `nil`) will be returned.
func (m *mockAllocator) processConstraints(
	config roachpb.SpanConfig,
) (map[string]constraint, map[string]constraint, error) {
	zoneConstraints := map[string]constraint{}
	regionConstraints := map[string]constraint{}
	totalVoters := int(config.GetNumVoters())
	totalReplicas := int(config.NumReplicas)
	if err := m.processConstraintsHelper(
		config.VoterConstraints, true /*isVoterConstraint*/, totalVoters, /*totalNumOfVotersOrReplicas*/
		zoneConstraints, regionConstraints); err != nil {
		return map[string]constraint{}, map[string]constraint{}, err
	}
	if err := m.processConstraintsHelper(
		config.Constraints, false /*isVoterConstraint*/, totalReplicas, /*totalNumOfVotersOrReplicas*/
		zoneConstraints, regionConstraints); err != nil {
		return map[string]constraint{}, map[string]constraint{}, err
	}
	return zoneConstraints, regionConstraints, nil
}

// computeNecessaryChanges computes the necessary minimal changes needed for a
// level to satisfy the constraints, considering the existing number of voters
// and non-voters, as well as the required number of voters and replicas.
func computeNecessaryChanges(
	existingVoters int, existingNonVoters int, requiredVoters int, requiredReplicas int,
) (int, int, int) {
	// Note that having more than required (having unconstrained
	// replicas/voters) is fine and simply means no more additional voters or
	// replicas need to be added.

	// numOfVotersNeeded will be satisfied by promoting non-voters or adding
	// voters. Try to promote as many existing nonvoters to voters as possible
	// first to satisfy voter constraints (so that we require minimal voters or
	// replicas to be added).
	numOfVotersNeeded := int(math.Max(0, float64(requiredVoters-existingVoters)))

	// Step 1: find out number of nonvoters needed to be promoted
	nonVotersToPromote := int(math.Min(float64(existingNonVoters), float64(numOfVotersNeeded)))
	existingVotersAfterPromotion := existingVoters + nonVotersToPromote
	existingNonVotersAfterPromotion := existingNonVoters - nonVotersToPromote

	// Step 2: find out number of voters needed to be added
	votersToAdd := int(math.Max(0, float64(requiredVoters-existingVotersAfterPromotion)))
	existingVotersAfterPromotionAndVoterAddition := existingVotersAfterPromotion + votersToAdd
	existingNonVotersAfterPromotionAndVoterAddition := existingNonVotersAfterPromotion // no changes

	// Step 3: find out number of nonvoters needed to be added
	nonVotersToAdd := int(math.Max(0, float64(requiredReplicas-existingVotersAfterPromotionAndVoterAddition-existingNonVotersAfterPromotionAndVoterAddition)))
	return nonVotersToPromote, votersToAdd, nonVotersToAdd
}

// applyAtRegionLevel attempts to apply the desired changes (nonVotersToPromote,
// votersToAdd, nonVotersToAdd) at the provided region (specified by
// regionName). If enough nodes are available, it makes the changes and returns
// true. Otherwise, it returns false.
func (m *mockAllocator) applyAtRegionLevel(
	regionName string, nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := m.region[regionName]
	if !ok {
		panic("unknown region name in the region constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}

	existing.promoteNonVoters(nonVotersToPromote)
	success := existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd)
	m.region[regionName] = existing
	return success
}

// applyAtClusterLevel attempts to apply the desired changes
// (nonVotersToPromote, votersToAdd, nonVotersToAdd) at the cluster level. If
// enough nodes are available, it makes the changes and returns true. Otherwise,
// it returns false.
func (m *mockAllocator) applyAtClusterLevel(
	nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	m.cluster.promoteNonVoters(nonVotersToPromote)
	return m.cluster.tryToAddVoters(votersToAdd) && m.cluster.tryToAddNonVoters(nonVotersToAdd)
}

// applyAtZoneLevel attempts to apply the desired changes (nonVotersToPromote,
// votersToAdd, nonVotersToAdd) at the provided zone (specified by zoneName). If
// enough nodes are available, it makes the changes and returns true. Otherwise,
// it returns false.
func (m *mockAllocator) applyAtZoneLevel(
	zoneName string, nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := m.zone[zoneName]
	if !ok {
		panic("unknown zone name in the zone constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	existing.promoteNonVoters(nonVotersToPromote)
	success := existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd)
	m.zone[zoneName] = existing
	return success
}

// tryToSatisfyRegionConstraint checks whether the allocator can assign voters
// and replicas in a manner that meets the specified required voters and
// replicas for the region. If possible, it makes the necessary assignment,
// updates the allocator, and returns true. Otherwise, it returns false.
func (m *mockAllocator) tryToSatisfyRegionConstraint(
	regionName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := m.region[regionName]
	if !ok {
		panic("unknown region name in the region constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.assignedVoters, existing.assignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to region and cluster.
	return m.applyAtRegionLevel(regionName, nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		m.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
}

// tryToSatisfyZoneConstraint checks whether the allocator can assign voters and
// replicas in a manner that meets the specified required voters and replicas
// for the zone. If possible, it makes the necessary assignment, updates the
// allocator, and returns true. Otherwise, it returns false.
func (m *mockAllocator) tryToSatisfyZoneConstraint(
	zoneName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := m.zone[zoneName]
	if !ok {
		panic("unknown zone name in the zone constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.assignedVoters, existing.assignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to zone, region and cluster.
	return m.applyAtZoneLevel(zoneName, nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		m.applyAtRegionLevel(m.zoneToRegion[zoneName], nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		m.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
}

// tryToSatisfyClusterConstraint checks whether the allocator can assign voters
// and replicas in a manner that meets the specified required voters and
// replicas for the cluster. If possible, it makes the necessary assignment,
// updates the allocator, and returns true. Otherwise, it returns false.
func (m *mockAllocator) tryToSatisfyClusterConstraint(
	requiredVoters int, requiredReplicas int,
) bool {
	existing := m.cluster
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.assignedVoters, existing.assignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to cluster.
	success := m.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
	if m.cluster.assignedVoters != requiredVoters || m.cluster.assignedNonVoters+m.cluster.assignedVoters != requiredReplicas {
		// Since having unconstrained replicas or voters do not lead to error in
		// earlier process, we check for exact bound cluster constraint here.
		return false
	}
	return success
}

// isSatisfiable is a method that assesses whether a given configuration is
// satisfiable within the cluster used to initialize the mockAllocator. It
// returns (true, nil) for satisfiable configurations and (false, reason) for
// unsatisfiable configurations. mockAllocator tries to allocate voters and
// nonvoters across nodes in a manner that satisfies the constraints. If no such
// allocation can be found, the constraint is considered unsatisfiable. The
// allocation is found through the following process:
// 1. Preprocess the config constraints to store replica and voter constraints
// specific to the zone and region in two maps.
// 2. Try to satisfy zone constraints first, region constraints next, and
// cluster constraints in the end. As we allocate replicas for zone constraints,
// some region constraints are also satisfied.
// 3. While trying to satisfy constraints at each hierarchical level, we
// allocate voters or replicas specific to the zone or region only when
// necessary. It first promotes non-voters to voters when possible as voters are
// also replicas and can satisfy both constraints. Additional voters and
// non-voters are then assigned as needed. If any zones or regions lack
// available nodes for assignment, the constraint is considered as
// unsatisfiable.
//
// Limitation:
// - leaseholder preference are not checked and treated as satisfiable. -
// constraints with a key other than zone and region are unsatisfiable. -
// constraints with a value that does not correspond to a known zone or region
// in the cluster setup are unsatisfiable.
// - constraints labeled as Constraint_PROHIBITED are considered unsatisfiable.
func (m *mockAllocator) isSatisfiable(config roachpb.SpanConfig) (success bool, err error) {
	zoneConstraints, regionConstraints, err := m.processConstraints(config)
	if err != nil {
		return false, err
	}

	for zoneName, zc := range zoneConstraints {
		if !m.tryToSatisfyZoneConstraint(zoneName, zc.requiredVoters, zc.requiredReplicas) {
			return false, errors.Newf("failed to satisfy constraints for zone %s", zoneName)
		}
	}

	for regionName, rc := range regionConstraints {
		if !m.tryToSatisfyRegionConstraint(regionName, rc.requiredVoters, rc.requiredReplicas) {
			return false, errors.Newf("failed to satisfy constraints for region %s", regionName)
		}
	}

	if !m.tryToSatisfyClusterConstraint(int(config.GetNumVoters()), int(config.NumReplicas)) {
		return false, errors.Newf("failed to satisfy constraints for cluster")
	}
	return true, nil
}
