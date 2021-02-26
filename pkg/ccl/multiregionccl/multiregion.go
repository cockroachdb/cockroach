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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func init() {
	sql.CreateRegionConfigCCL = createRegionConfig
	sql.GetMultiRegionEnumAddValuePlacementCCL = getMultiRegionEnumAddValuePlacement
}

func createRegionConfig(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	execCfg *sql.ExecutorConfig,
	liveRegions sql.LiveClusterRegions,
	survivalGoal tree.SurvivalGoal,
	primaryRegion tree.Name,
	regions []tree.Name,
) (descpb.DatabaseDescriptor_RegionConfig, error) {
	if err := checkClusterSupportsMultiRegion(evalCtx); err != nil {
		return descpb.DatabaseDescriptor_RegionConfig{}, err
	}

	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings,
		execCfg.ClusterID(),
		execCfg.Organization(),
		"multi-region features",
	); err != nil {
		return descpb.DatabaseDescriptor_RegionConfig{}, err
	}

	var regionConfig descpb.DatabaseDescriptor_RegionConfig
	var err error
	regionConfig.SurvivalGoal, err = sql.TranslateSurvivalGoal(survivalGoal)
	if err != nil {
		return descpb.DatabaseDescriptor_RegionConfig{}, err
	}

	regionConfig.PrimaryRegion = descpb.RegionName(primaryRegion)
	if regionConfig.PrimaryRegion != descpb.RegionName(tree.PrimaryRegionLocalityName) {
		if err := sql.CheckLiveClusterRegion(liveRegions, regionConfig.PrimaryRegion); err != nil {
			return descpb.DatabaseDescriptor_RegionConfig{}, err
		}
	}
	if len(regions) > 0 {
		if regionConfig.PrimaryRegion == descpb.RegionName(tree.PrimaryRegionLocalityName) {
			return descpb.DatabaseDescriptor_RegionConfig{}, pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"PRIMARY REGION must be specified if REGIONS are specified",
			)
		}
		regionConfig.Regions = make([]descpb.DatabaseDescriptor_RegionConfig_Region, 0, len(regions)+1)
		seenRegions := make(map[descpb.RegionName]struct{}, len(regions)+1)
		for _, r := range regions {
			region := descpb.RegionName(r)
			if err := sql.CheckLiveClusterRegion(liveRegions, region); err != nil {
				return descpb.DatabaseDescriptor_RegionConfig{}, err
			}

			if _, ok := seenRegions[region]; ok {
				return descpb.DatabaseDescriptor_RegionConfig{}, pgerror.Newf(
					pgcode.InvalidName,
					"region %q defined multiple times",
					region,
				)
			}
			seenRegions[region] = struct{}{}
			regionConfig.Regions = append(
				regionConfig.Regions,
				descpb.DatabaseDescriptor_RegionConfig_Region{
					Name: region,
				},
			)
		}
		// If PRIMARY REGION is not in REGIONS, add it implicitly.
		if _, ok := seenRegions[regionConfig.PrimaryRegion]; !ok {
			regionConfig.Regions = append(
				regionConfig.Regions,
				descpb.DatabaseDescriptor_RegionConfig_Region{
					Name: regionConfig.PrimaryRegion,
				},
			)
		}
		sort.SliceStable(regionConfig.Regions, func(i, j int) bool {
			return regionConfig.Regions[i].Name < regionConfig.Regions[j].Name
		})
	} else {
		regionConfig.Regions = []descpb.DatabaseDescriptor_RegionConfig_Region{
			{Name: regionConfig.PrimaryRegion},
		}
	}

	// Generate a unique ID for the multi-region enum type descriptor here as
	// well.
	id, err := catalogkv.GenerateUniqueDescID(ctx, execCfg.DB, execCfg.Codec)
	if err != nil {
		return descpb.DatabaseDescriptor_RegionConfig{}, err
	}
	regionConfig.RegionEnumID = id
	return regionConfig, nil
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
