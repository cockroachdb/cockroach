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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
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
	regions              catpb.RegionNames
	transitioningRegions catpb.RegionNames
	primaryRegion        catpb.RegionName
	regionEnumID         descpb.ID
	placement            descpb.DataPlacement
	superRegions         []descpb.SuperRegion
}

// SurvivalGoal returns the survival goal configured on the RegionConfig.
func (r *RegionConfig) SurvivalGoal() descpb.SurvivalGoal {
	return r.survivalGoal
}

// PrimaryRegion returns the primary region configured on the RegionConfig.
func (r *RegionConfig) PrimaryRegion() catpb.RegionName {
	return r.primaryRegion
}

// Regions returns the list of regions added to the RegionConfig.
func (r *RegionConfig) Regions() catpb.RegionNames {
	return r.regions
}

// IsMemberOfExplicitSuperRegion returns whether t the region is an explicit
// member of a super region.
func (r *RegionConfig) IsMemberOfExplicitSuperRegion(region catpb.RegionName) bool {
	for _, superRegion := range r.SuperRegions() {
		for _, regionOfSuperRegion := range superRegion.Regions {
			if region == regionOfSuperRegion {
				return true
			}
		}
	}

	return false
}

// GetSuperRegionRegionsForRegion returns the members of the super region the
// specified region is part of.
// If the region is not a member of any super regions, we return all
// the regions on the RegionConfig.
func (r *RegionConfig) GetSuperRegionRegionsForRegion(region catpb.RegionName) catpb.RegionNames {
	for _, superRegion := range r.SuperRegions() {
		for _, regionOfSuperRegion := range superRegion.Regions {
			if region == regionOfSuperRegion {
				return superRegion.Regions
			}
		}
	}

	return r.Regions()
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
func (r RegionConfig) TransitioningRegions() catpb.RegionNames {
	return r.transitioningRegions
}

// RegionEnumID returns the multi-region enum ID.
func (r *RegionConfig) RegionEnumID() descpb.ID {
	return r.regionEnumID
}

// Placement returns the data placement strategy for the region config.
func (r *RegionConfig) Placement() descpb.DataPlacement {
	return r.placement
}

// IsPlacementRestricted returns true if the database is in restricted
// placement, false otherwise.
func (r *RegionConfig) IsPlacementRestricted() bool {
	return r.placement == descpb.DataPlacement_RESTRICTED
}

// SuperRegions returns the list of super regions in the database.
func (r *RegionConfig) SuperRegions() []descpb.SuperRegion {
	return r.superRegions
}

// MakeRegionConfigOption is an option for MakeRegionConfig
type MakeRegionConfigOption func(r *RegionConfig)

// WithTransitioningRegions is an option to include transitioning
// regions into MakeRegionConfig.
func WithTransitioningRegions(transitioningRegions catpb.RegionNames) MakeRegionConfigOption {
	return func(r *RegionConfig) {
		r.transitioningRegions = transitioningRegions
	}
}

// MakeRegionConfig constructs a RegionConfig.
func MakeRegionConfig(
	regions catpb.RegionNames,
	primaryRegion catpb.RegionName,
	survivalGoal descpb.SurvivalGoal,
	regionEnumID descpb.ID,
	placement descpb.DataPlacement,
	superRegions []descpb.SuperRegion,
	opts ...MakeRegionConfigOption,
) RegionConfig {
	ret := RegionConfig{
		regions:       regions,
		primaryRegion: primaryRegion,
		survivalGoal:  survivalGoal,
		regionEnumID:  regionEnumID,
		placement:     placement,
		superRegions:  superRegions,
	}
	for _, opt := range opts {
		opt(&ret)
	}
	return ret
}

// CanSatisfySurvivalGoal returns true if the survival goal is satisfiable by
// the given region config.
func CanSatisfySurvivalGoal(survivalGoal descpb.SurvivalGoal, numRegions int) error {
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
	if config.placement == descpb.DataPlacement_RESTRICTED &&
		config.survivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		return errors.AssertionFailedf(
			"cannot have a database with restricted placement that is also region survivable")
	}

	err := ValidateSuperRegions(config.SuperRegions(), config.SurvivalGoal(), config.Regions(), func(err error) error {
		return err
	})
	if err != nil {
		return err
	}

	return CanSatisfySurvivalGoal(config.survivalGoal, len(config.regions))
}

// ValidateSuperRegions validates that:
//   1. Region names are unique within a super region and are sorted.
//   2. All region within a super region map to a region on the RegionConfig.
//   3. Super region names are unique.
//   4. Each region can only belong to one super region.
func ValidateSuperRegions(
	superRegions []descpb.SuperRegion,
	survivalGoal descpb.SurvivalGoal,
	regionNames catpb.RegionNames,
	errorHandler func(error) error,
) error {
	seenRegions := make(map[catpb.RegionName]struct{})
	superRegionNames := make(map[string]struct{})

	// Ensure that the super region names are in sorted order.
	if !sort.SliceIsSorted(superRegions, func(i, j int) bool {
		return superRegions[i].SuperRegionName < superRegions[j].SuperRegionName
	}) {
		err := errors.AssertionFailedf("super regions are not in sorted order based on the super region name %v", superRegions)
		if err := errorHandler(err); err != nil {
			return err
		}
	}

	for _, superRegion := range superRegions {
		if len(superRegion.Regions) == 0 {
			err := errors.AssertionFailedf("no regions found within super region %s", superRegion.SuperRegionName)
			if err := errorHandler(err); err != nil {
				return err
			}
		}

		if err := CanSatisfySurvivalGoal(survivalGoal, len(superRegion.Regions)); err != nil {
			err := errors.HandleAsAssertionFailure(errors.Wrapf(err, "super region %s only has %d regions", superRegion.SuperRegionName, len(superRegion.Regions)))
			if err := errorHandler(err); err != nil {
				return err
			}
		}

		_, found := superRegionNames[superRegion.SuperRegionName]
		if found {
			err := errors.AssertionFailedf("duplicate super regions with name %s found", superRegion.SuperRegionName)
			if err := errorHandler(err); err != nil {
				return err
			}
		}
		superRegionNames[superRegion.SuperRegionName] = struct{}{}

		// Ensure that regions within a super region are sorted.
		if !sort.SliceIsSorted(superRegion.Regions, func(i, j int) bool {
			return superRegion.Regions[i] < superRegion.Regions[j]
		}) {
			err := errors.AssertionFailedf("the regions within super region %s were not in a sorted order", superRegion.SuperRegionName)
			if err := errorHandler(err); err != nil {
				return err
			}
		}

		seenRegionsInCurrentSuperRegion := make(map[catpb.RegionName]struct{})
		for _, region := range superRegion.Regions {
			_, found := seenRegionsInCurrentSuperRegion[region]
			if found {
				err := errors.AssertionFailedf("duplicate region %s found in super region %s", region, superRegion.SuperRegionName)
				if err := errorHandler(err); err != nil {
					return err
				}
			}
			seenRegionsInCurrentSuperRegion[region] = struct{}{}
			_, found = seenRegions[region]
			if found {
				return errors.AssertionFailedf("region %s found in multiple super regions", region)
			}
			seenRegions[region] = struct{}{}

			// Ensure that the region actually maps to a region on the regionConfig.
			found = false
			for _, regionName := range regionNames {
				if region == regionName {
					found = true
				}
			}
			if !found {
				err := errors.Newf("region %s not part of database", region)
				if err := errorHandler(err); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// IsMemberOfSuperRegion returns a boolean representing if the region is part
// of a super region and the name of the super region.
func IsMemberOfSuperRegion(name catpb.RegionName, config RegionConfig) (bool, string) {
	for _, superRegion := range config.SuperRegions() {
		for _, region := range superRegion.Regions {
			if region == name {
				return true, superRegion.SuperRegionName
			}
		}
	}

	return false, ""
}

// CanDropRegion returns an error if the survival goal doesn't allow for
// removing regions or if the region is part of a super region.
func CanDropRegion(name catpb.RegionName, config RegionConfig) error {
	isMember, superRegion := IsMemberOfSuperRegion(name, config)
	if isMember {
		return errors.WithHintf(
			pgerror.Newf(pgcode.DependentObjectsStillExist, "region %s is part of super region %s", name, superRegion),
			"you must first drop super region %s before you can drop the region %s", superRegion, name,
		)
	}
	return CanSatisfySurvivalGoal(config.survivalGoal, len(config.regions)-1)
}
