// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// ZoneConfigForMultiRegionTable generates a ZoneConfig stub for a
// regional-by-table or global table in a multi-region database.
//
// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
//
// This function can return a nil zonepb.ZoneConfig, meaning no table level zone
// configuration is required.
//
// Relevant multi-region configured fields (as defined in
// `zonepb.MultiRegionZoneConfigFields`) will be overwritten by the calling function
// into an existing ZoneConfig.
func ZoneConfigForMultiRegionTable(
	localityConfig catpb.LocalityConfig, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := *zonepb.NewZoneConfig()

	switch l := localityConfig.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		// Enable non-blocking transactions.
		zc.GlobalReads = proto.Bool(true)

		if !regionConfig.GlobalTablesInheritDatabaseConstraints() {
			// For GLOBAL tables, we want non-voters in all regions for fast reads, so
			// we always use a DEFAULT placement config, even if the database is using
			// RESTRICTED placement.
			regionConfig = regionConfig.WithPlacementDefault()

			numVoters, numReplicas := GetNumVotersAndNumReplicas(regionConfig)
			zc.NumVoters = &numVoters
			zc.NumReplicas = &numReplicas

			constraints, err := SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.Constraints = constraints
			zc.InheritedConstraints = false

			voterConstraints, err := SynthesizeVoterConstraints(regionConfig.PrimaryRegion(), regionConfig)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.VoterConstraints = voterConstraints
			zc.NullVoterConstraintsIsEmpty = true
			zc.LeasePreferences = SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), "" /* secondaryRegion */)
			zc.InheritedLeasePreferences = false

			zc, err = regionConfig.ExtendZoneConfigWithGlobal(zc)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
		}
		// Inherit lease preference from the database. We do
		// nothing here because `NewZoneConfig()` already marks the field as
		// 'inherited'.
		return zc, nil
	case *catpb.LocalityConfig_RegionalByTable_:
		affinityRegion := regionConfig.PrimaryRegion()
		if l.RegionalByTable.Region != nil {
			affinityRegion = *l.RegionalByTable.Region
		}
		if l.RegionalByTable.Region == nil && !regionConfig.IsMemberOfSuperRegion(affinityRegion) {
			// If we don't have an explicit affinity region, use the same
			// configuration as the database and return a blank zcfg here.
			return zc, nil
		}

		numVoters, numReplicas := GetNumVotersAndNumReplicas(regionConfig)
		zc.NumVoters = &numVoters

		if regionConfig.IsMemberOfSuperRegion(affinityRegion) {
			err := AddConstraintsForSuperRegion(&zc, regionConfig, affinityRegion)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
		} else if !regionConfig.RegionalInTablesInheritDatabaseConstraints(affinityRegion) {
			// If the database constraints can't be inherited to serve as the
			// constraints for this table, define the constraints ourselves.
			zc.NumReplicas = &numReplicas

			constraints, err := SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.Constraints = constraints
			zc.InheritedConstraints = false
		}

		// If the table has a user-specified affinity region, use it.
		voterConstraints, err := SynthesizeVoterConstraints(affinityRegion, regionConfig)
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
		zc.VoterConstraints = voterConstraints
		zc.NullVoterConstraintsIsEmpty = true
		zc.LeasePreferences = SynthesizeLeasePreferences(affinityRegion, "" /* secondaryRegion */)
		zc.InheritedLeasePreferences = false

		return regionConfig.ExtendZoneConfigWithRegionalIn(zc, affinityRegion)

	case *catpb.LocalityConfig_RegionalByRow_:
		// We purposely do not set anything here at table level - this should be done at
		// partition level instead.
		return zc, nil
	default:
		return zonepb.ZoneConfig{}, errors.AssertionFailedf(
			"unexpected unknown locality type %T", localityConfig.Locality)
	}
}

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

	if regionConfig.IsMemberOfSuperRegion(partitionRegion) {
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

// ZoneConfigForMultiRegionValidator is an interface that both schema changers
// must implement to provide error reporting and state information during
// multi-region zone config validation.
type ZoneConfigForMultiRegionValidator interface {
	// TransitioningRegions returns the regions currently being added or removed
	TransitioningRegions() catpb.RegionNames

	// NewMismatchFieldError creates an error for when a zone config field doesn't match
	NewMismatchFieldError(descType, name string, mismatch zonepb.DiffWithZoneMismatch) error

	// NewMissingSubzoneError creates an error for when an expected subzone is missing
	NewMissingSubzoneError(descType, name string, mismatch zonepb.DiffWithZoneMismatch) error

	// NewExtraSubzoneError creates an error for when an unexpected subzone is present
	NewExtraSubzoneError(descType, name string, mismatch zonepb.DiffWithZoneMismatch) error
}

// MultiRegionTableValidatorData provides an abstraction for reading table metadata
// that works with both legacy and declarative schema changers.
type MultiRegionTableValidatorData interface {
	// GetNonDropIndexes returns a map of index ID to index name for all non-drop indexes
	GetNonDropIndexes() map[uint32]tree.Name

	// GetTransitioningRBRIndexes returns index IDs that are transitioning to/from RBR
	// (only relevant for legacy schema changer with mutations)
	GetTransitioningRBRIndexes() map[uint32]struct{}

	// GetTableLocalitySecondaryRegion returns the secondary region for REGIONAL BY TABLE
	// Returns nil if not a secondary region table
	GetTableLocalitySecondaryRegion() *catpb.RegionName

	// GetDatabasePrimaryRegion returns the database primary region name
	GetDatabasePrimaryRegion() catpb.RegionName

	// GetDatabaseSecondaryRegion returns the database secondary region name
	GetDatabaseSecondaryRegion() catpb.RegionName
}

// ValidateZoneConfigForMultiRegionTable validates that the multi-region fields
// of a table's zone configuration match what is expected.
// This is the shared core validation logic used by both schema changers.
func ValidateZoneConfigForMultiRegionTable(
	tableName tree.Name,
	currentZoneConfig *zonepb.ZoneConfig,
	expectedZoneConfig zonepb.ZoneConfig,
	tableData MultiRegionTableValidatorData,
	validator ZoneConfigForMultiRegionValidator,
) error {
	if currentZoneConfig == nil {
		currentZoneConfig = zonepb.NewZoneConfig()
	}

	regionalByRowNewIndexes := tableData.GetTransitioningRBRIndexes()
	subzoneIndexIDsToDiff := tableData.GetNonDropIndexes()

	// Build transitioning regions map
	// Do not compare partitioning for these regions, as they may be in a
	// transitioning state.
	transitioningRegions := make(map[string]struct{}, len(validator.TransitioningRegions()))
	for _, region := range validator.TransitioningRegions() {
		transitioningRegions[string(region)] = struct{}{}
	}

	// We only want to compare against the list of subzones on active indexes
	// and partitions, so filter the subzone list based on the
	// subzoneIndexIDsToDiff computed above.
	// No need to pass regionalByRowNewIndexes to filter because they are
	// already excluded from subzoneIndexIDsToDiff.
	filteredCurrentSubzones := filterSubzones(
		currentZoneConfig.Subzones,
		subzoneIndexIDsToDiff,
		nil, /* skipRBRIndexes */
		transitioningRegions,
	)
	currentZoneConfig.Subzones = filteredCurrentSubzones

	// Strip placeholder if no subzones
	if len(filteredCurrentSubzones) == 0 && currentZoneConfig.IsSubzonePlaceholder() {
		currentZoneConfig.NumReplicas = nil
	}

	// Remove regional by row new indexes and transitioning partitions from the expected zone config.
	// These will be incorrect as ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes
	// will apply the existing locality config on them instead of the new locality config.
	filteredExpectedSubzones := filterSubzones(
		expectedZoneConfig.Subzones,
		nil,                     /* validIndexIDs */
		regionalByRowNewIndexes, // don't filter RBR indexes from expected
		transitioningRegions,
	)
	expectedZoneConfig.Subzones = filteredExpectedSubzones

	// Mark the expected NumReplicas as 0 if we have a placeholder
	// and the current zone config is also a placeholder.
	// The latter check is required as in cases where non-multiregion fields
	// are set on the current zone config, the expected zone config needs
	// the placeholder marked so that DiffWithZone does not error when
	// num_replicas is expectedly different.
	// e.g. if current zone config has gc.ttlseconds set, then we
	// do not fudge num replicas to be equal to 0 -- otherwise the
	// check fails when num_replicas is different, but that is
	// expected as the current zone config is no longer a placeholder.
	if currentZoneConfig.IsSubzonePlaceholder() && IsPlaceholderZoneConfigForMultiRegion(expectedZoneConfig) {
		expectedZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Synthesize lease preferences if secondary region exists
	if tableData.GetDatabaseSecondaryRegion() != "" {
		expectedZoneConfig.LeasePreferences = synthesizeLeasePreferencesForTable(
			tableData,
		)
	}

	// Compare the two zone configs to see if anything is amiss.
	same, mismatch, err := currentZoneConfig.DiffWithZone(
		expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}

	if !same {
		return buildValidationError(
			tableName,
			mismatch,
			subzoneIndexIDsToDiff,
			validator,
		)
	}

	return nil
}

// filterSubzones filters subzones based on index IDs and regions
func filterSubzones(
	subzones []zonepb.Subzone,
	validIndexIDs map[uint32]tree.Name,
	skipRBRIndexes map[uint32]struct{},
	skipRegions map[string]struct{},
) []zonepb.Subzone {
	filtered := subzones[:0]
	for _, subzone := range subzones {
		// Skip transitioning regions
		if subzone.PartitionName != "" {
			if _, skip := skipRegions[subzone.PartitionName]; skip {
				continue
			}
		}
		// Skip RBR transitioning indexes
		if skipRBRIndexes != nil {
			if _, skip := skipRBRIndexes[subzone.IndexID]; skip {
				continue
			}
		}
		// Skip indexes not in valid set
		if validIndexIDs != nil {
			if _, valid := validIndexIDs[subzone.IndexID]; !valid {
				continue
			}
		}
		filtered = append(filtered, subzone)
	}
	return filtered
}

// buildValidationError constructs the appropriate error based on mismatch type
func buildValidationError(
	tableName tree.Name,
	mismatch zonepb.DiffWithZoneMismatch,
	indexMap map[uint32]tree.Name,
	validator ZoneConfigForMultiRegionValidator,
) error {
	descType := "table"
	name := tableName.String()

	if mismatch.IndexID != 0 {
		indexName, ok := indexMap[mismatch.IndexID]
		if !ok {
			return errors.AssertionFailedf(
				"unexpected unknown index id %d on table %s (mismatch %#v)",
				mismatch.IndexID,
				tableName,
				mismatch,
			)
		}

		if mismatch.PartitionName != "" {
			descType = "partition"
			partitionName := tree.Name(mismatch.PartitionName)
			name = fmt.Sprintf("%s of %s@%s",
				partitionName.String(),
				tableName.String(),
				indexName.String(),
			)
		} else {
			descType = "index"
			name = fmt.Sprintf("%s@%s", tableName.String(), indexName.String())
		}
	}

	if mismatch.IsMissingSubzone {
		return validator.NewMissingSubzoneError(descType, name, mismatch)
	}
	if mismatch.IsExtraSubzone {
		return validator.NewExtraSubzoneError(descType, name, mismatch)
	}
	return validator.NewMismatchFieldError(descType, name, mismatch)
}

func synthesizeLeasePreferencesForTable(
	tableData MultiRegionTableValidatorData,
) []zonepb.LeasePreference {
	if rbtSecondaryRegion := tableData.GetTableLocalitySecondaryRegion(); rbtSecondaryRegion != nil {
		return SynthesizeLeasePreferences(*rbtSecondaryRegion, tableData.GetDatabaseSecondaryRegion())
	}
	return SynthesizeLeasePreferences(tableData.GetDatabasePrimaryRegion(), tableData.GetDatabaseSecondaryRegion())
}

// distributeReplicasAcrossRegions distributes numReplicas across the given
// regions, ensuring all replicas are constrained within the region set. The
// affinityRegion receives priority for any extra replicas beyond an even
// distribution. The affinityRegion must be a member of the regions slice.
func distributeReplicasAcrossRegions(
	numReplicas int32, regions catpb.RegionNames, affinityRegion catpb.RegionName,
) ([]zonepb.ConstraintsConjunction, error) {
	numRegions := int32(len(regions))
	if numRegions == 0 {
		return nil, errors.AssertionFailedf("cannot distribute replicas across empty region set")
	}
	if !regions.Contains(affinityRegion) {
		return nil, errors.AssertionFailedf("affinity region %s must be a member of the region set %v", affinityRegion, regions)
	}
	base := numReplicas / numRegions
	extra := numReplicas % numRegions

	// Determine which regions get an extra replica. The affinity region gets
	// first priority, then remaining extras go to other regions in their
	// original order.
	getsExtra := make(map[catpb.RegionName]bool)
	remaining := extra
	if remaining > 0 {
		getsExtra[affinityRegion] = true
		remaining--
	}
	for _, r := range regions {
		if remaining == 0 {
			break
		}
		if r != affinityRegion {
			getsExtra[r] = true
			remaining--
		}
	}

	// Emit constraints in original region order so that zone configs are
	// deterministic and predictable across runs.
	var constraints []zonepb.ConstraintsConjunction
	for _, region := range regions {
		n := base
		if getsExtra[region] {
			n++
		}
		if n > 0 {
			constraints = append(constraints, zonepb.ConstraintsConjunction{
				NumReplicas: n,
				Constraints: []zonepb.Constraint{MakeRequiredConstraintForRegion(region)},
			})
		}
	}
	return constraints, nil
}

// AddConstraintsForSuperRegion updates the ZoneConfig.Constraints field such
// that every replica is guaranteed to be constrained to a region within the
// super region. Replicas are distributed evenly across regions with the
// affinity region receiving priority for any remainder. An error is returned
// if the affinity region is not a member of a super region.
func AddConstraintsForSuperRegion(
	zc *zonepb.ZoneConfig, regionConfig multiregion.RegionConfig, affinityRegion catpb.RegionName,
) error {
	regions, ok := regionConfig.GetSuperRegionRegionsForRegion(affinityRegion)
	if !ok {
		return errors.AssertionFailedf("region %s is not part of a super region", affinityRegion)
	}
	_, numReplicas := GetNumVotersAndNumReplicas(regionConfig.WithRegions(regions))

	zc.NumReplicas = &numReplicas
	constraints, err := distributeReplicasAcrossRegions(numReplicas, regions, affinityRegion)
	if err != nil {
		return err
	}
	zc.Constraints = constraints
	zc.InheritedConstraints = false
	return nil
}
