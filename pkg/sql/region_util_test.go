// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestZoneConfigForMultiRegionDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc         string
		regionConfig multiregion.RegionConfig
		expected     zonepb.ZoneConfig
	}{
		{
			desc: "one region, zone survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc: "two regions, zone survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_a",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(4),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc: "three regions, zone survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
		{
			desc: "three regions, region survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
		{
			desc: "four regions, zone survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(6),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
		{
			desc: "four regions, region survival",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
		{
			desc: "one region, restricted placement",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints:                 nil,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc: "four regions, restricted placement",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
					"region_d",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints:                 nil,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			// NOTE: this test case uses zone config extensions to mimic placement
			// restricted.
			desc: "four regions, zone survival, zone config extensions",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(6),
						Constraints:               nil,
						InheritedLeasePreferences: true,
					},
					// Unused. Testing that this doesn't cause issues.
					Global: &zonepb.ZoneConfig{
						LeasePreferences: []zonepb.LeasePreference{
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
								},
							},
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
								},
							},
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(6),
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints:                 nil,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			// NOTE: this test case uses zone config extensions to mimic a
			// database-level secondary region.
			desc: "four regions, region survival, zone config extensions",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_b": {
							NumReplicas: proto.Int32(6),
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
							},
							InheritedConstraints:        true,
							NullVoterConstraintsIsEmpty: true,
							VoterConstraints: []zonepb.ConstraintsConjunction{
								{
									NumReplicas: 2,
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
								{
									NumReplicas: 2,
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
							},
						},
						// Unused. Testing that this doesn't cause issues.
						"region_c": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
						},
					},
					// Unused. Testing that this doesn't cause issues.
					Global: &zonepb.ZoneConfig{
						LeasePreferences: []zonepb.LeasePreference{
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
								},
							},
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
								},
							},
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(6),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			// Database-level zone config with secondary region and super regions.
			// The database zone config does NOT omit the secondary region for
			// super regions (it operates on the full region set), so the secondary
			// appears normally in voter_constraints and lease_preferences.
			desc: "four regions, region survival, secondary region with super region",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
					"region_d",
				},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(6),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := zoneConfigForMultiRegionDatabase(tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}

func TestZoneConfigForMultiRegionTableAndSuperRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc           string
		localityConfig catpb.LocalityConfig
		regionConfig   multiregion.RegionConfig
		expected       zonepb.ZoneConfig
	}{
		{
			// RBT with region survival, secondary INSIDE super region.
			// The secondary (region_b) is in sr1={a,b,c}, so it appears in both
			// voter_constraints and lease_preferences.
			desc: "RBT super region, region survival, secondary inside",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_a"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// RBT with region survival, secondary OUTSIDE super region.
			// The secondary (region_d) is NOT in sr1={a,b,c}, so it is stripped
			// from both voter_constraints and lease_preferences.
			desc: "RBT super region, region survival, secondary outside",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_b"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_d"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// RBT with zone survival, secondary INSIDE super region.
			// The secondary (region_b) is in sr1={a,b,c}, so it appears in
			// lease_preferences. Under zone survival, voter_constraints has no
			// NumReplicas (all voters in home region).
			desc: "RBT super region, zone survival, secondary inside",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_a"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// RBT with zone survival, secondary OUTSIDE super region.
			// The secondary (region_d) is NOT in sr1={a,b,c}, so it is stripped
			// from lease_preferences.
			desc: "RBT super region, zone survival, secondary outside",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_a"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_d"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := regions.ZoneConfigForMultiRegionTable(tc.localityConfig, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, zc)
		})
	}
}

func TestZoneConfigForMultiRegionPartitionAndSuperRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc         string
		region       catpb.RegionName
		regionConfig multiregion.RegionConfig
		expected     zonepb.ZoneConfig
	}{
		{
			desc:   "3-region super region, zone survival (reproduction case)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, region survival",
			region: "region_b",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region in 6-region database, zone survival",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a", "region_b", "region_c",
					"region_d", "region_e", "region_f",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, zone survival, restricted placement",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			// Zone survival with secondary region INSIDE the super region.
			// The secondary region (region_b) is in sr1={a,b,c}, so it appears
			// in lease_preferences but not in voter_constraints (zone survival
			// puts all voters in the home region without NumReplicas).
			desc:   "super region, zone survival, secondary inside",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// Zone survival with secondary region OUTSIDE the super region.
			// The secondary region (region_d) is NOT in sr1={a,b,c}, so it is
			// stripped from both voter_constraints and lease_preferences.
			desc:   "super region, zone survival, secondary outside",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_d"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			// Region survival with secondary region INSIDE the super region.
			// The secondary (region_b) is in sr1={a,b,c}, so it appears in both
			// voter_constraints (with NumReplicas=2) and lease_preferences.
			desc:   "super region, region survival, secondary inside",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// Region survival with secondary region OUTSIDE the super region.
			// The secondary (region_d) is NOT in sr1={a,b,c}, so it is stripped
			// from both voter_constraints and lease_preferences.
			desc:   "super region, region survival, secondary outside",
			region: "region_b",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_d"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// Region survival, restricted placement, secondary INSIDE super region.
			// Restricted placement means numReplicas=numVoters. The secondary
			// (region_b) is in sr1={a,b,c}, so it appears in both voter_constraints
			// and lease_preferences. With 5 replicas across 3 regions (base=1,
			// extra=2), the affinity region (region_c) and the first other region
			// (region_a) each get 2 replicas.
			desc:   "super region, region survival, restricted, secondary inside",
			region: "region_c",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c", "region_d"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			// 6-region DB, secondary OUTSIDE super region, partition in super region.
			// Primary (region_d) and secondary (region_e) are both outside sr1={a,b,c}.
			// The secondary is stripped from voter_constraints and lease_preferences.
			desc:   "6-region super region, secondary outside",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a", "region_b", "region_c",
					"region_d", "region_e", "region_f",
				},
				"region_d",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_e"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := regions.ZoneConfigForMultiRegionPartition(tc.region, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, zc)
		})
	}
}

func protoRegionName(region catpb.RegionName) *catpb.RegionName {
	return &region
}

func TestZoneConfigForMultiRegionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc           string
		localityConfig catpb.LocalityConfig
		regionConfig   multiregion.RegionConfig
		expected       zonepb.ZoneConfig
	}{
		{
			desc: "4-region global table with zone survival",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{
					Global: &catpb.LocalityConfig_Global{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				GlobalReads:               proto.Bool(true),
				InheritedConstraints:      true,
				InheritedLeasePreferences: true,
			},
		},
		{
			desc: "4-region global table with region survival",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{
					Global: &catpb.LocalityConfig_Global{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				GlobalReads:               proto.Bool(true),
				InheritedConstraints:      true,
				InheritedLeasePreferences: true,
			},
		},
		{
			desc: "4-region global table with restricted placement",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(6),
				NumVoters:                   proto.Int32(3),
				GlobalReads:                 proto.Bool(true),
				NullVoterConstraintsIsEmpty: true,
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
			},
		},
		{
			// NOTE: this test case uses zone config extensions to mimic a
			// database-level secondary region.
			desc: "4-region global table with zone config extensions (for global)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Global: &zonepb.ZoneConfig{
						NumReplicas: proto.Int32(6),
						LeasePreferences: []zonepb.LeasePreference{
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
								},
							},
							{
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
								},
							},
						},
						InheritedConstraints:        true,
						NullVoterConstraintsIsEmpty: true,
						VoterConstraints: []zonepb.ConstraintsConjunction{
							{
								NumReplicas: 2,
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
								},
							},
							{
								NumReplicas: 2,
								Constraints: []zonepb.Constraint{
									{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
								},
							},
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(6),
				NumVoters:                   proto.Int32(5),
				GlobalReads:                 proto.Bool(true),
				InheritedConstraints:        false,
				NullVoterConstraintsIsEmpty: true,
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
			},
		},
		{
			desc: "4-region global table with zone config extensions (for regional)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(8),
						InheritedConstraints:      true,
						InheritedLeasePreferences: true,
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(5),
				NumVoters:                   proto.Int32(5),
				GlobalReads:                 proto.Bool(true),
				NullVoterConstraintsIsEmpty: true,
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
			},
		},
		{
			desc: "4-region global table with zone config extensions (for regional in primary region)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_b": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(5),
				NumVoters:                   proto.Int32(5),
				GlobalReads:                 proto.Bool(true),
				NullVoterConstraintsIsEmpty: true,
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
			},
		},
		{
			desc: "4-region global table with zone config extensions (for regional in non primary region)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_Global_{},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_c": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				GlobalReads:               proto.Bool(true),
				InheritedConstraints:      true,
				InheritedLeasePreferences: true,
			},
		},
		{
			desc: "4-region regional by row table with zone survival",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByRow_{
					RegionalByRow: &catpb.LocalityConfig_RegionalByRow{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by row table with region survival",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByRow_{
					RegionalByRow: &catpb.LocalityConfig_RegionalByRow{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with zone survival on primary region",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: nil,
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with regional survival on primary region",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: nil,
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with zone survival on non primary region",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: nil, // Set at the database level.
				NumVoters:   proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			desc: "4-region regional by table with regional survival on non primary region",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: nil, // Set at the database level.
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			desc: "4-region regional by table on non primary region with zone config extensions (for regional)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(8),
						InheritedConstraints:      true,
						InheritedLeasePreferences: true,
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(8),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			desc: "4-region regional by table on non primary region with zone config extensions (for regional in primary region)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_b": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			desc: "4-region regional by table on non primary region with zone config extensions (for regional in non primary region)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_c": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: nil, // Set at the database level.
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
		{
			desc: "4-region regional by table on non primary region with zone config extensions (for regional and for regional in non primary region)",
			localityConfig: catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(8),
						InheritedConstraints:      true,
						InheritedLeasePreferences: true,
					},
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_c": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(8),
				NumVoters:   proto.Int32(5),
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := regions.ZoneConfigForMultiRegionTable(tc.localityConfig, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, zc)
		})
	}
}

func TestZoneConfigForMultiRegionPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc         string
		region       catpb.RegionName
		regionConfig multiregion.RegionConfig
		expected     zonepb.ZoneConfig
	}{
		{
			desc:   "4-region table with zone survivability",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 nil, // Set at the database level.
				NumVoters:                   proto.Int32(3),
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc:   "4-region table with region survivability",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 nil, // Set at the database level.
				NumVoters:                   proto.Int32(5),
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc:   "4-region table with zone config extensions (for regional)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(8),
						InheritedConstraints:      true,
						InheritedLeasePreferences: true,
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(8),
				NumVoters:                   proto.Int32(5),
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
			},
		},
		{
			desc:   "4-region table with zone config extensions (for regional in primary region)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_b": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(5),
				NumVoters:                   proto.Int32(5),
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"},
						},
					},
				},
			},
		},
		{
			desc:   "4-region table with zone config extensions (for regional in non primary region)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_a": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 nil, // Set at the database level.
				NumVoters:                   proto.Int32(5),
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
		{
			desc:   "4-region table with zone config extensions (for regional and for regional in non primary region)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				nil,
				descpb.ZoneConfigExtensions{
					Regional: &zonepb.ZoneConfig{
						NumReplicas:               proto.Int32(8),
						InheritedConstraints:      true,
						InheritedLeasePreferences: true,
					},
					RegionalIn: map[catpb.RegionName]zonepb.ZoneConfig{
						"region_a": {
							LeasePreferences: []zonepb.LeasePreference{
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
									},
								},
								{
									Constraints: []zonepb.Constraint{
										{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
									},
								},
							},
							InheritedConstraints: true,
						},
					},
				},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas:                 proto.Int32(8),
				NumVoters:                   proto.Int32(5),
				InheritedConstraints:        true,
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{
						NumReplicas: 2,
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
						},
					},
					{
						Constraints: []zonepb.Constraint{
							{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
						},
					},
				},
			},
		},
	}

	// Super region test cases.
	superRegionTestCases := []struct {
		desc         string
		region       catpb.RegionName
		regionConfig multiregion.RegionConfig
		expected     zonepb.ZoneConfig
	}{
		{
			desc:   "3-region super region, zone survival (reproduction case)",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, region survival",
			region: "region_b",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region in 6-region database, zone survival",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a", "region_b", "region_c",
					"region_d", "region_e", "region_f",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, zone survival, restricted placement",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_RESTRICTED,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, zone survival, secondary INSIDE super region",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(3),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
		{
			desc:   "3-region super region, region survival, secondary INSIDE super region",
			region: "region_c",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{"region_a", "region_b", "region_c"},
				"region_a",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{{
					SuperRegionName: "sr1",
					Regions:         []catpb.RegionName{"region_a", "region_b", "region_c"},
				}},
				descpb.ZoneConfigExtensions{},
				multiregion.WithSecondaryRegion("region_b"),
			),
			expected: zonepb.ZoneConfig{
				NumReplicas: proto.Int32(5),
				NumVoters:   proto.Int32(5),
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"},
					}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
				},
				NullVoterConstraintsIsEmpty: true,
				VoterConstraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
					{NumReplicas: 2, Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"},
					}},
					{Constraints: []zonepb.Constraint{
						{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"},
					}},
				},
			},
		},
	}
	testCases = append(testCases, superRegionTestCases...)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := regions.ZoneConfigForMultiRegionPartition(tc.region, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, zc)
		})
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
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
				descpb.ZoneConfigExtensions{}),
		},
		{
			testName: "a super region should have at least three regions if the survival mode is region failure",
			err:      "super region sr1 only has 2 regions: at least 3 regions are required for surviving a region failure",
			regionConfig: multiregion.MakeRegionConfig(
				catpb.RegionNames{
					"region_a",
					"region_b",
					"region_c",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE, validRegionEnumID,
				descpb.DataPlacement_DEFAULT,
				[]descpb.SuperRegion{
					{
						SuperRegionName: "sr1",
						Regions:         []catpb.RegionName{"region_a", "region_b"},
					},
				},
				descpb.ZoneConfigExtensions{}),
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
