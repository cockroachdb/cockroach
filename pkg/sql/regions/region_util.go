// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/errors"
)

// ZoneConfigForMultiRegionPartition generates a ZoneConfig stub for a partition
// that belongs to a regional by row table in a multi-region database.
//
// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
func ZoneConfigForMultiRegionPartition(
	partitionRegion catpb.RegionName, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := *zonepb.NewZoneConfig()

	numVoters, numReplicas := GetNumVotersAndNumReplicas(regionConfig)
	zc.NumVoters = &numVoters

	if regionConfig.IsMemberOfExplicitSuperRegion(partitionRegion) {
		err := AddConstraintsForSuperRegion(&zc, regionConfig, partitionRegion)
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
	} else if !regionConfig.RegionalInTablesInheritDatabaseConstraints(partitionRegion) {
		// If the database constraints can't be inherited to serve as the
		// constraints for this partition, define the constraints ourselves.
		zc.NumReplicas = &numReplicas

		constraints, err := SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
		zc.Constraints = constraints
		zc.InheritedConstraints = false
	}

	voterConstraints, err := SynthesizeVoterConstraints(partitionRegion, regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	zc.VoterConstraints = voterConstraints
	zc.NullVoterConstraintsIsEmpty = true
	zc.LeasePreferences = SynthesizeLeasePreferences(partitionRegion, regionConfig.SecondaryRegion())
	zc.InheritedLeasePreferences = false

	return regionConfig.ExtendZoneConfigWithRegionalIn(zc, partitionRegion)
}

// IsPlaceholderZoneConfigForMultiRegion returns whether a given zone config
// should be marked as a placeholder config for a multi-region object.
// See zonepb.IsSubzonePlaceholder for why this is necessary.
func IsPlaceholderZoneConfigForMultiRegion(zc zonepb.ZoneConfig) bool {
	// Placeholders must have at least 1 subzone.
	if len(zc.Subzones) == 0 {
		return false
	}
	// Strip Subzones / SubzoneSpans, as these may contain items if migrating
	// from one REGIONAL BY ROW table to another.
	strippedZC := zc
	strippedZC.Subzones, strippedZC.SubzoneSpans = nil, nil
	return strippedZC.Equal(zonepb.NewZoneConfig())
}

// SynthesizeLeasePreferences generates a LeasePreferences
// clause representing the `lease_preferences` field to be set for the primary
// region and secondary region of a multi-region database or the home region of
// a table in such a database.
func SynthesizeLeasePreferences(
	region catpb.RegionName, secondaryRegion catpb.RegionName,
) []zonepb.LeasePreference {
	ret := []zonepb.LeasePreference{
		{Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)}},
	}
	if secondaryRegion != "" && secondaryRegion != region {
		ret = append(ret, zonepb.LeasePreference{
			Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(secondaryRegion)},
		})
	}
	return ret
}

// SynthesizeReplicaConstraints generates a ConstraintsConjunction clause
// representing the `constraints` field to be set for a multi-region database.
func SynthesizeReplicaConstraints(
	regions catpb.RegionNames, placement descpb.DataPlacement,
) ([]zonepb.ConstraintsConjunction, error) {
	switch placement {
	case descpb.DataPlacement_DEFAULT:
		constraints := make([]zonepb.ConstraintsConjunction, len(regions))
		for i, region := range regions {
			// Constrain at least 1 (voting or non-voting) replica per region.
			constraints[i] = zonepb.ConstraintsConjunction{
				NumReplicas: 1,
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			}
		}
		return constraints, nil
	case descpb.DataPlacement_RESTRICTED:
		// In a RESTRICTED placement policy, the database zone config has no
		// non-voters so that REGIONAL BY [TABLE | ROW] can inherit the RESTRICTED
		// placement. Voter placement will be set at the table/partition level to
		// the table/partition region.

		// NB: When setting empty constraints, use nil as opposed to []. When
		// constraints are deserialized from the database, empty constraints are
		// always deserialized as nil. Therefore, if constraints are set as [] here,
		// the database will have a difference in its expected constraints vs the
		// actual constraints when comparing using the multi-region validation
		// builtins.
		return nil, nil
	default:
		return nil, errors.AssertionFailedf("unknown data placement: %v", placement)
	}
}

// SynthesizeVoterConstraints generates a ConstraintsConjunction clause
// representing the `voter_constraints` field to be set for the primary region
// of a multi-region database or the home region of a table/partition in such a
// database.
//
// Under zone survivability, we will constrain all voting replicas to be inside
// the primary/home region.
//
// Under region survivability, we will constrain exactly <quorum - 1> voting
// replicas in the primary/home region.
func SynthesizeVoterConstraints(
	region catpb.RegionName, regionConfig multiregion.RegionConfig,
) ([]zonepb.ConstraintsConjunction, error) {
	switch regionConfig.SurvivalGoal() {
	case descpb.SurvivalGoal_ZONE_FAILURE:
		return []zonepb.ConstraintsConjunction{
			{
				// We don't specify `NumReplicas` here to indicate that we want _all_
				// voting replicas to be constrained to this one region.
				//
				// Constraining all voting replicas to be inside the primary/home region
				// is necessary and sufficient to ensure zone survivability, even though
				// it might appear that these zone configs don't seem to spell out the
				// requirement of being resilient to zone failures. This is because, by
				// default, the allocator (see kv/kvserver/allocator.go) will maximize
				// survivability due to it's diversity heuristic (see
				// Locality.DiversityScore()) by spreading the replicas of a range
				// across nodes with the most mutual difference in their locality
				// hierarchies.
				//
				// For instance, in a 2 region deployment, each with 3 AZs, this is
				// expected to result in a configuration like the following:
				//
				// +---- Region A -----+      +---- Region B -----+
				// |                   |      |                   |
				// |   +------------+  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// |   +------------+  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  | NON-VOTER  |   |
				// |   +------------+  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// |   |            |  |      |  +------------+   |
				// |   |   VOTER    |  |      |  |            |   |
				// |   |            |  |      |  |            |   |
				// |   +------------+  |      |  +------------+   |
				// +-------------------+      +-------------------+
				//
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			},
		}, nil
	case descpb.SurvivalGoal_REGION_FAILURE:
		// We constrain <quorum - 1> voting replicas to the primary region and
		// allow the rest to "float" around. This allows the allocator inside KV
		// to make dynamic placement decisions for the voting replicas that lie
		// outside the primary/home region.
		//
		// It might appear that constraining just <quorum - 1> voting replicas
		// to the primary region leaves open the possibility of a majority
		// quorum coalescing inside of some other region. However, similar to
		// the case above, the diversity heuristic in the allocator prevents
		// this from happening as it will spread the unconstrained replicas out
		// across nodes with the most diverse locality hierarchies.
		//
		// For instance, in a 3 region deployment (minimum for a database with
		// "region" survivability), each with 3 AZs, we'd expect to see a
		// configuration like the following:
		//
		// +---- Region A ------+   +---- Region B -----+    +----- Region C -----+
		// |                    |   |                   |    |                    |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// |   |   VOTER    |   |   |  |   VOTER    |   |    |   |            |   |
		// |   |            |   |   |  |            |   |    |   |            |   |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// |   |            |   |   |  |   VOTER    |   |    |   |   VOTER    |   |
		// |   |            |   |   |  |            |   |    |   |            |   |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// |   |   VOTER    |   |   |  |            |   |    |   |            |   |
		// |   |            |   |   |  |            |   |    |   |            |   |
		// |   +------------+   |   |  +------------+   |    |   +------------+   |
		// +--------------------+   +-------------------+    +--------------------+
		//
		numVoters, _ := GetNumVotersAndNumReplicas(regionConfig)
		ret := []zonepb.ConstraintsConjunction{
			{
				NumReplicas: MaxFailuresBeforeUnavailability(numVoters),
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			},
		}
		if regionConfig.HasSecondaryRegion() && regionConfig.SecondaryRegion() != region {
			ret = append(ret, zonepb.ConstraintsConjunction{
				NumReplicas: MaxFailuresBeforeUnavailability(numVoters),
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(regionConfig.SecondaryRegion())},
			})
		}
		return ret, nil
	default:
		return nil, errors.AssertionFailedf("unknown survival goal: %v", regionConfig.SurvivalGoal())
	}
}

// MaxFailuresBeforeUnavailability returns the maximum number of individual
// failures that can be tolerated, among `numVoters` voting replicas, before a
// given range is unavailable.
func MaxFailuresBeforeUnavailability(numVoters int32) int32 {
	return ((numVoters + 1) / 2) - 1
}

// GetNumVotersAndNumReplicas computes the number of voters and the total number
// of replicas needed for a given region config.
func GetNumVotersAndNumReplicas(
	regionConfig multiregion.RegionConfig,
) (numVoters, numReplicas int32) {
	const numVotersForZoneSurvival = 3
	// Under region survivability, we use 5 voting replicas to allow for a
	// theoretical (2-2-1) voting replica configuration, where the primary region
	// has 2 voting replicas and the next closest region has another 2. This
	// allows for stable read/write latencies even under single node failures.
	//
	// TODO(aayush): Until we add allocator heuristics to coalesce voting replicas
	// together based on their relative latencies to the leaseholder, we can't
	// actually ensure that the region closest to the leaseholder has 2 voting
	// replicas.
	//
	// Until the above TODO is addressed, the non-leaseholder voting replicas will
	// be allowed to "float" around among the other regions in the database. They
	// may or may not be placed geographically close to the leaseholder replica.
	const numVotersForRegionSurvival = 5

	numRegions := int32(len(regionConfig.Regions()))
	switch regionConfig.SurvivalGoal() {
	// NB: See mega-comment inside `SynthesizeVoterConstraints()` for why these
	// are set the way they are.
	case descpb.SurvivalGoal_ZONE_FAILURE:
		numVoters = numVotersForZoneSurvival
		switch regionConfig.Placement() {
		case descpb.DataPlacement_DEFAULT:
			// <numVoters in the home region> + <1 replica for every other region>
			numReplicas = (numVotersForZoneSurvival) + (numRegions - 1)
		case descpb.DataPlacement_RESTRICTED:
			numReplicas = numVoters
		default:
			panic(errors.AssertionFailedf("unknown data placement: %v", regionConfig.Placement()))
		}
	case descpb.SurvivalGoal_REGION_FAILURE:
		// The primary and secondary region each have two voters.
		// MaxFailuresBeforeUnavailability(numVotersForRegionSurvival) = 2.
		// We have 5 voters for survival mode region failure such that we can
		// get quorum with 2 voters in the primary region + one voter outside.
		// Every other region has one replica.
		numVoters = numVotersForRegionSurvival

		// There are always 2 (i.e. MaxFailuresBeforeUnavailability) replicas in the
		// primary region, and 1 replica in every other region.
		numReplicas = MaxFailuresBeforeUnavailability(numVotersForRegionSurvival) + (numRegions - 1)
		if regionConfig.HasSecondaryRegion() {
			// If there is a secondary region, it gets an additional replica.
			numReplicas++
		}
		if numReplicas < numVoters {
			// NumReplicas cannot be less than NumVoters. If we have <= 4 regions, all
			// replicas will be voting replicas.
			numReplicas = numVoters
		}
	}
	return numVoters, numReplicas
}

func MakeRequiredConstraintForRegion(r catpb.RegionName) zonepb.Constraint {
	return zonepb.Constraint{
		Type:  zonepb.Constraint_REQUIRED,
		Key:   "region",
		Value: string(r),
	}
}

// AddConstraintsForSuperRegion updates the ZoneConfig.Constraints field such
// that every replica is guaranteed to be constrained to a region within the
// super region.
// If !regionConfig.IsMemberOfExplicitSuperRegion(affinityRegion), and error
// will be returned.
func AddConstraintsForSuperRegion(
	zc *zonepb.ZoneConfig, regionConfig multiregion.RegionConfig, affinityRegion catpb.RegionName,
) error {
	regions, ok := regionConfig.GetSuperRegionRegionsForRegion(affinityRegion)
	if !ok {
		return errors.AssertionFailedf("region %s is not part of a super region", affinityRegion)
	}
	_, numReplicas := GetNumVotersAndNumReplicas(regionConfig.WithRegions(regions))

	zc.NumReplicas = &numReplicas
	zc.Constraints = nil
	zc.InheritedConstraints = false

	switch regionConfig.SurvivalGoal() {
	case descpb.SurvivalGoal_ZONE_FAILURE:
		for _, region := range regions {
			zc.Constraints = append(zc.Constraints, zonepb.ConstraintsConjunction{
				NumReplicas: 1,
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			})
		}
		return nil
	case descpb.SurvivalGoal_REGION_FAILURE:
		// There is a special case where we have 3 regions under survival goal
		// region failure where we have to constrain an extra replica to any
		// region within the super region to guarantee that all replicas are
		// accounted for. In our case, we assign it to the first non-primary region
		// in sorted order.
		// This happens because we have 5 replicas and 3 regions. 2 voters are
		// constrained to the primary region, the other 2 regions each are given a
		// replica, the last non-voting replica is not guaranteed to be constrained
		// anywhere.
		// If we have more than 3 regions, all replicas are accounted for and
		// constrained within the super region.
		// See: https://github.com/cockroachdb/cockroach/issues/63617 for more.
		extraReplicaToConstrain := len(regions) == 3
		for _, region := range regions {
			n := int32(1)
			if region != affinityRegion && extraReplicaToConstrain {
				n = 2
				extraReplicaToConstrain = false
			}
			zc.Constraints = append(zc.Constraints, zonepb.ConstraintsConjunction{
				NumReplicas: n,
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			})
		}
		return nil
	default:
		return errors.AssertionFailedf("unknown survival goal: %v", regionConfig.SurvivalGoal())
	}
}
