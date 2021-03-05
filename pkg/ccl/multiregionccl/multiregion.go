// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func init() {
	sql.InitializeMultiRegionMetadataCCL = initializeMultiRegionMetadata
	sql.GetMultiRegionEnumAddValuePlacementCCL = getMultiRegionEnumAddValuePlacement
}

func initializeMultiRegionMetadata(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	execCfg *sql.ExecutorConfig,
	liveRegions sql.LiveClusterRegions,
	goal tree.SurvivalGoal,
	primaryRegion descpb.RegionName,
	regions []tree.Name,
) (*multiregion.RegionConfig, error) {
	if err := checkClusterSupportsMultiRegion(evalCtx); err != nil {
		return nil, err
	}

	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings,
		execCfg.ClusterID(),
		execCfg.Organization(),
		"multi-region features",
	); err != nil {
		return nil, err
	}

	survivalGoal, err := sql.TranslateSurvivalGoal(goal)
	if err != nil {
		return nil, err
	}

	if primaryRegion != descpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
		if err := sql.CheckClusterRegionIsLive(liveRegions, primaryRegion); err != nil {
			return nil, err
		}
	}
	regionNames := make(descpb.RegionNames, 0, len(regions)+1)
	seenRegions := make(map[descpb.RegionName]struct{}, len(regions)+1)
	if len(regions) > 0 {
		if primaryRegion == descpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
			return nil, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"PRIMARY REGION must be specified if REGIONS are specified",
			)
		}
		for _, r := range regions {
			region := descpb.RegionName(r)
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

	sort.SliceStable(regionNames, func(i, j int) bool {
		return regionNames[i] < regionNames[j]
	})

	// Generate a unique ID for the multi-region enum type descriptor here as
	// well.
	regionEnumID, err := catalogkv.GenerateUniqueDescID(ctx, execCfg.DB, execCfg.Codec)
	if err != nil {
		return nil, err
	}
	regionConfig := multiregion.MakeRegionConfig(
		regionNames,
		primaryRegion,
		survivalGoal,
		regionEnumID,
	)
	if err := multiregion.ValidateRegionConfig(regionConfig); err != nil {
		return nil, err
	}

	return &regionConfig, nil
}

func checkClusterSupportsMultiRegion(evalCtx *tree.EvalContext) error {
	if !evalCtx.Settings.Version.IsActive(evalCtx.Context, clusterversion.MultiRegionFeatures) {
		return pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			`cannot add regions to a database until the cluster upgrade is finalized`,
		)
	}
	return nil
}

func getMultiRegionEnumAddValuePlacement(
	execCfg *sql.ExecutorConfig, typeDesc *typedesc.Mutable, region tree.Name,
) (tree.AlterTypeAddValue, error) {
	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings,
		execCfg.ClusterID(),
		execCfg.Organization(),
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
