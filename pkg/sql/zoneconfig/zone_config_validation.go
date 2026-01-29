// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package zoneconfig

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// pkg/sql/regions/zone_config_validation.go

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

	// GetDatabasePrimaryRegion returns the database secondary region name
	GetDatabasePrimaryRegion() catpb.RegionName

	// GetDatabaseSecondaryRegion returns the database primary region name
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
	validator regions.ZoneConfigForMultiRegionValidator,
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
		nil,
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
		nil,
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
	if currentZoneConfig.IsSubzonePlaceholder() && regions.IsPlaceholderZoneConfigForMultiRegion(expectedZoneConfig) {
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
	validator regions.ZoneConfigForMultiRegionValidator,
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

func synthesizeLeasePreferencesForTable(tableData MultiRegionTableValidatorData) []zonepb.LeasePreference {
	if rbtSecondaryRegion := tableData.GetTableLocalitySecondaryRegion(); rbtSecondaryRegion != nil {
		return regions.SynthesizeLeasePreferences(*rbtSecondaryRegion, tableData.GetDatabaseSecondaryRegion())
	}
	return regions.SynthesizeLeasePreferences(tableData.GetDatabasePrimaryRegion(), tableData.GetDatabaseSecondaryRegion())
}
