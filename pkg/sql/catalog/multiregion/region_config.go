// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package multiregion provides functions and structs for interacting with the
// static multi-region state configured by users on their databases.
package multiregion

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// minNumRegionsForSurviveRegionGoal is the minimum number of regions that a
// a database must have to survive a REGION failure.
const minNumRegionsForSurviveRegionGoal = 3

// RegionConfig represents the user configured state of a multi-region database.
// RegionConfig is intended to be a READ-ONLY struct and as such all members
// are private. Any modifications to the underlying type desc / db desc that
// inform be a RegionConfig must be made directly on those structs and a new
// RegionConfig must be synthesized to pick up those changes.
type RegionConfig struct {
	survivalGoal         descpb.SurvivalGoal
	regions              descpb.RegionNames
	transitioningRegions descpb.RegionNames
	primaryRegion        descpb.RegionName
	regionEnumID         descpb.ID
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
func (r RegionConfig) IsValidRegionNameString(regionStr string) bool {
	for _, region := range r.Regions() {
		if string(region) == regionStr {
			return true
		}
	}
	return false
}

// PrimaryRegionString implements the tree.DatabaseRegionConfig interface.
func (r RegionConfig) PrimaryRegionString() string {
	return string(r.PrimaryRegion())
}

// TransitioningRegions returns all the regions which are currently transitioning
// from or to being PUBLIC.
func (r RegionConfig) TransitioningRegions() descpb.RegionNames {
	return r.transitioningRegions
}

// RegionEnumID returns the multi-region enum ID.
func (r *RegionConfig) RegionEnumID() descpb.ID {
	return r.regionEnumID
}

// MakeRegionConfigOption is an option for MakeRegionConfig
type MakeRegionConfigOption func(r *RegionConfig)

// WithTransitioningRegions is an option to include transitioning
// regions into MakeRegionConfig.
func WithTransitioningRegions(transitioningRegions descpb.RegionNames) MakeRegionConfigOption {
	return func(r *RegionConfig) {
		r.transitioningRegions = transitioningRegions
	}
}

// MakeRegionConfig constructs a RegionConfig.
func MakeRegionConfig(
	regions descpb.RegionNames,
	primaryRegion descpb.RegionName,
	survivalGoal descpb.SurvivalGoal,
	regionEnumID descpb.ID,
	opts ...MakeRegionConfigOption,
) RegionConfig {
	ret := RegionConfig{
		regions:       regions,
		primaryRegion: primaryRegion,
		survivalGoal:  survivalGoal,
		regionEnumID:  regionEnumID,
	}
	for _, opt := range opts {
		opt(&ret)
	}
	return ret
}

// canSatisfySurvivalGoal returns true if the survival goal is satisfiable by
// the given region config.
func canSatisfySurvivalGoal(survivalGoal descpb.SurvivalGoal, numRegions int) error {
	if survivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		if numRegions < minNumRegionsForSurviveRegionGoal {
			return errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidParameterValue,
					"at least %d regions are required for surviving a region failure",
					minNumRegionsForSurviveRegionGoal,
				),
				"you must add additional regions to the database or "+
					"change the survivability goal",
			)
		}
	}
	return nil
}

// ValidateRegionConfig validates that the given RegionConfig is valid.
func ValidateRegionConfig(config RegionConfig) error {
	if config.regionEnumID == descpb.InvalidID {
		return errors.AssertionFailedf("expected a valid multi-region enum ID to be initialized")
	}
	if len(config.regions) == 0 {
		return errors.AssertionFailedf("expected > 0 number of regions in the region config")
	}
	return canSatisfySurvivalGoal(config.survivalGoal, len(config.regions))
}

// CanDropRegion returns an error if the survival goal doesn't allow for
// removing regions.
func CanDropRegion(config RegionConfig) error {
	return canSatisfySurvivalGoal(config.survivalGoal, len(config.regions)-1)
}
