// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package multiregion

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// minNumRegionsForSurviveRegionalGoal is the minimum number of regions that a
// a database must have to survive a REGION failure.
const minNumRegionsForSurviveRegionGoal = 3

// RegionConfig represents the user configured state of a multi-region database.
type RegionConfig struct {
	survivalGoal  descpb.SurvivalGoal
	regions       descpb.RegionNames
	primaryRegion descpb.RegionName
}

// SurvivalGoal returns the survival goal configured on the RegionConfig.
func (r *RegionConfig) SurvivalGoal() descpb.SurvivalGoal {
	return r.survivalGoal
}

// PrimaryRegion returns the primary region configured on the RegionConfig.
func (r *RegionConfig) PrimaryRegion() descpb.RegionName {
	return r.primaryRegion
}

// Regions returns the list of regions added to the RegionConfig.
func (r *RegionConfig) Regions() descpb.RegionNames {
	return r.regions
}

// IsValidRegionNameString implements the tree.DatabaseRegionConfig interface.
func (r *RegionConfig) IsValidRegionNameString(regionStr string) bool {
	if r == nil {
		return false
	}
	for _, region := range r.Regions() {
		if string(region) == regionStr {
			return true
		}
	}
	return false
}

// PrimaryRegionString implements the tree.DatabaseRegionConfig interface.
func (r *RegionConfig) PrimaryRegionString() string {
	if r == nil {
		return ""
	}
	return string(r.PrimaryRegion())
}

// NewRegionConfig constructs a RegionConfig.
func NewRegionConfig(
	regions descpb.RegionNames, primaryRegion descpb.RegionName, survivalGoal descpb.SurvivalGoal,
) *RegionConfig {
	return &RegionConfig{
		regions:       regions,
		primaryRegion: primaryRegion,
		survivalGoal:  survivalGoal,
	}
}

// EnsureSurvivalGoalIsSatisfiable returns an error if the survivability goal is
// unsatisfiable.
func (r *RegionConfig) EnsureSurvivalGoalIsSatisfiable() error {
	// If we're changing to survive a region failure, validate that we have enough
	// regions in the database.
	if r.SurvivalGoal() == descpb.SurvivalGoal_REGION_FAILURE {
		if len(r.Regions()) < minNumRegionsForSurviveRegionGoal {
			return errors.WithHintf(
				pgerror.Newf(pgcode.InvalidName,
					"at least %d regions are required for surviving a region failure",
					minNumRegionsForSurviveRegionGoal,
				),
				"you must add additional regions to the database using "+
					"ALTER DATABASE <db_name> ADD REGION <region_name>",
			)
		}
	}
	return nil
}

// InitializationRegionConfig is a wrapper around RegionConfig containing
// additional fields which are only required during initialization.
type InitializationRegionConfig struct {
	RegionConfig

	regionEnumID descpb.ID
}

// NewInitializationRegionConfig constructs a region config capable of
// initializing a multi-region database.
func NewInitializationRegionConfig(
	regions descpb.RegionNames,
	primaryRegion descpb.RegionName,
	survivalGoal descpb.SurvivalGoal,
	regionEnumID descpb.ID,
) *InitializationRegionConfig {
	regionConfig := NewRegionConfig(regions, primaryRegion, survivalGoal)

	return &InitializationRegionConfig{
		RegionConfig: *regionConfig,
		regionEnumID: regionEnumID,
	}
}

// RegionEnumID returns the multi-region enum ID.
func (i *InitializationRegionConfig) RegionEnumID() descpb.ID {
	return i.regionEnumID
}

// ValidateInitializationRegionConfig validates that the given
// InitializationRegionConfig is valid.
func ValidateInitializationRegionConfig(config *InitializationRegionConfig) error {
	if config.regionEnumID == descpb.InvalidID {
		return errors.AssertionFailedf("expected a valid multi-region enum ID to be initialized")
	}
	if len(config.regions) == 0 {
		return errors.AssertionFailedf("expected > 0 number of regions in the region config")
	}
	if config.survivalGoal == descpb.SurvivalGoal_REGION_FAILURE &&
		len(config.regions) < minNumRegionsForSurviveRegionGoal {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"at least %d regions are required for surviving a region failure",
			minNumRegionsForSurviveRegionGoal,
		)
	}
	return nil
}
