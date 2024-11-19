// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func init() {
	sql.InitializeMultiRegionMetadataCCL = initializeMultiRegionMetadata
	sql.GetMultiRegionEnumAddValuePlacementCCL = getMultiRegionEnumAddValuePlacement
}

func initializeMultiRegionMetadata(
	ctx context.Context,
	descIDGenerator eval.DescIDGenerator,
	settings *cluster.Settings,
	liveRegions sql.LiveClusterRegions,
	goal tree.SurvivalGoal,
	primaryRegion catpb.RegionName,
	regions []tree.Name,
	dataPlacement tree.DataPlacement,
	secondaryRegion catpb.RegionName,
) (*multiregion.RegionConfig, error) {
	if err := CheckClusterSupportsMultiRegion(
		settings,
	); err != nil {
		return nil, err
	}

	survivalGoal, err := sql.TranslateSurvivalGoal(goal)
	if err != nil {
		return nil, err
	}
	placement, err := sql.TranslateDataPlacement(dataPlacement)
	if err != nil {
		return nil, err
	}

	if primaryRegion != catpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
		if err := sql.CheckClusterRegionIsLive(liveRegions, primaryRegion); err != nil {
			return nil, err
		}
	}
	regionNames := make(catpb.RegionNames, 0, len(regions)+1)
	seenRegions := make(map[catpb.RegionName]struct{}, len(regions)+1)
	if len(regions) > 0 {
		if primaryRegion == catpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
			return nil, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"PRIMARY REGION must be specified if REGIONS are specified",
			)
		}
		for _, r := range regions {
			region := catpb.RegionName(r)
			if err := sql.CheckClusterRegionIsLive(liveRegions, region); err != nil {
				return nil, err
			}

			if _, ok := seenRegions[region]; ok {
				return nil, pgerror.Newf(
					pgcode.InvalidName,
					"region %q defined multiple times",
					region,
				)
			}
			seenRegions[region] = struct{}{}
			regionNames = append(regionNames, region)
		}
	}
	// If PRIMARY REGION is not in REGIONS, add it implicitly.
	if _, ok := seenRegions[primaryRegion]; !ok {
		regionNames = append(regionNames, primaryRegion)
	}

	if secondaryRegion != catpb.RegionName(tree.SecondaryRegionNotSpecifiedName) {
		if _, ok := seenRegions[secondaryRegion]; !ok {
			regionNames = append(regionNames, secondaryRegion)
		}
	}

	sort.SliceStable(regionNames, func(i, j int) bool {
		return regionNames[i] < regionNames[j]
	})

	// Generate a unique ID for the multi-region enum type descriptor here as
	// well.
	regionEnumID, err := descIDGenerator.GenerateUniqueDescID(ctx)
	if err != nil {
		return nil, err
	}
	regionConfig := multiregion.MakeRegionConfig(
		regionNames,
		primaryRegion,
		survivalGoal,
		regionEnumID,
		placement,
		nil,
		descpb.ZoneConfigExtensions{},
		multiregion.WithSecondaryRegion(secondaryRegion),
	)
	if err := multiregion.ValidateRegionConfig(regionConfig, false); err != nil {
		return nil, err
	}

	return &regionConfig, nil
}

// CheckClusterSupportsMultiRegion returns whether the current cluster supports
// multi-region features.
func CheckClusterSupportsMultiRegion(settings *cluster.Settings) error {
	return utilccl.CheckEnterpriseEnabled(
		settings,
		"multi-region features",
	)
}

func getMultiRegionEnumAddValuePlacement(
	execCfg *sql.ExecutorConfig, typeDesc *typedesc.Mutable, region tree.Name,
) (tree.AlterTypeAddValue, error) {
	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings,
		"ADD REGION",
	); err != nil {
		return tree.AlterTypeAddValue{}, err
	}

	// Find the location in the enum where we should insert the new value. We much search
	// for the location (and not append to the end), as we want to keep the values in sorted
	// order.
	loc := sort.Search(
		len(typeDesc.EnumMembers),
		func(i int) bool {
			return string(region) < typeDesc.EnumMembers[i].LogicalRepresentation
		},
	)

	// If the above search couldn't find a value greater than the region being added, add the
	// new region at the end of the enum.
	before := true
	if loc == len(typeDesc.EnumMembers) {
		before = false
		loc = len(typeDesc.EnumMembers) - 1
	}

	return tree.AlterTypeAddValue{
		IfNotExists: false,
		NewVal:      tree.EnumValue(region),
		Placement: &tree.AlterTypeAddValuePlacement{
			Before:      before,
			ExistingVal: tree.EnumValue(typeDesc.EnumMembers[loc].LogicalRepresentation),
		},
	}, nil
}
