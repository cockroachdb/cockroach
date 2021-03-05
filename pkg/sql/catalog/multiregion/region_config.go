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

// MinNumRegionsForSurviveRegionalGoal is the minimum number of regions that a
// a database must have to survive a REGION failure.
const MinNumRegionsForSurviveRegionGoal = 3

// RegionConfig represents the user configured state of a multi-region database.
type RegionConfig struct {
	survivalGoal  descpb.SurvivalGoal
	regions       descpb.RegionNames
	primaryRegion descpb.RegionName
	regionEnumID  descpb.ID
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
	for _, region := range r.Regions() {
		if string(region) == regionStr {
			return true
		}
	}
	return false
}

// PrimaryRegionString implements the tree.DatabaseRegionConfig interface.
func (r *RegionConfig) PrimaryRegionString() string {
	return string(r.PrimaryRegion())
}

// RegionEnumID returns the multi-region enum ID.
func (r *RegionConfig) RegionEnumID() descpb.ID {
	return r.regionEnumID
}

// NewRegionConfig constructs a RegionConfig.
func NewRegionConfig(
	regions descpb.RegionNames,
	primaryRegion descpb.RegionName,
	survivalGoal descpb.SurvivalGoal,
	regionEnumID descpb.ID,
) *RegionConfig {
	return &RegionConfig{
		regions:       regions,
		primaryRegion: primaryRegion,
		survivalGoal:  survivalGoal,
		regionEnumID:  regionEnumID,
	}
}

// CanSatisfySurvivalGoal returns true if the survival goal is satisfiable by
// the given region config.
func CanSatisfySurvivalGoal(r *RegionConfig) bool {
	if r.survivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		if len(r.Regions()) < MinNumRegionsForSurviveRegionGoal {
			return false
		}
	}
	return true
}

// ValidateRegionConfig validates that the given RegionConfig is valid.
func ValidateRegionConfig(config *RegionConfig) error {
	if config.regionEnumID == descpb.InvalidID {
		return errors.AssertionFailedf("expected a valid multi-region enum ID to be initialized")
	}
	if len(config.regions) == 0 {
		return errors.AssertionFailedf("expected > 0 number of regions in the region config")
	}
	if !CanSatisfySurvivalGoal(config) {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"at least %d regions are required for surviving a region failure",
			MinNumRegionsForSurviveRegionGoal,
		)
	}
	return nil
}
