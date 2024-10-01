// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateRegionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const validRegionEnumID = 100

	testCases := []struct {
		err          string
		regionConfig multiregion.RegionConfig
	}{
		{
			err: "expected a valid multi-region enum ID",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			err: "3 regions are required for surviving a region failure",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			err: "expected > 0 number of regions in the region config",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			err: "cannot have a database with restricted placement that is also region survivable",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_RESTRICTED,
				nil,
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			err: "region region_d has REGIONAL IN zone config extension, but is not a region in the database",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_b": {NumReplicas: proto.Int32(7)},
						"region_d": {NumReplicas: proto.Int32(8)},
					},
				},
			),
		},
	}

	for _, tc := range testCases {
		err := multiregion.ValidateRegionConfig(tc.regionConfig, false)

		require.Error(t, err)
		require.True(
			t,
			testutils.IsError(err, tc.err),
			"expected err %v, got %v",
			tc.err,
			err,
		)
	}
}

func TestValidateSuperRegionConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const validRegionEnumID = 100

	testCases := []struct {
		testName     string
		err          string
		regionConfig multiregion.RegionConfig
	}{
		{
			testName: "region names within a super region should be sorted",
			err:      "the regions within super region sr1 were not in a sorted order",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_b", "region_a"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "regions should be unique within a super region",
			err:      "duplicate region region_b found in super region sr1",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_b", "region_b"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "regions within a super region should map to a valid region on the database",
			err:      "region region_d not part of database",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_d"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "super region names should be sorted",
			err:      "super regions are not in sorted order based on the super region name",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr2",
						Regions:         []catpb.RegionName{"region_a"},
					},
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_b"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "a region can only appear in one super region",
			err:      "region region_a found in multiple super regions",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_a"},
					},
					{
						SuperRegionName: "sr2",
						Regions:         []catpb.RegionName{"region_a"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "super region names must be unique",
			err:      "duplicate super regions with name sr1 found",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_a"},
					},
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_a"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "a super region should have at least one region",
			err:      "no regions found within super region sr1",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
		{
			testName: "a super region should have at least three regions if the survival mode is region failure",
			err:      "super region sr1 only has 2 region(s): at least 3 regions are required for surviving a region failure",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_a", "region_b"},
					},
				},
				descpb.ZoneConfigExtensions{},
			),
		},
	}

	for _, tc := range testCases {
		err := multiregion.ValidateRegionConfig(tc.regionConfig, false)

		require.Error(t, err)
		require.True(
			t,
			testutils.IsError(err, tc.err),
			"test %s: expected err %v, got %v",
			tc.testName,
			tc.err,
			err,
		)
	}
}
