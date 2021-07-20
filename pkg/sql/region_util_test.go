// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
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
				descpb.RegionNames{
					"region_a",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_a",
				},
				"region_a",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
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
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := zoneConfigForMultiRegionDatabase(tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}

func protoRegionName(region descpb.RegionName) *descpb.RegionName {
	return &region
}

func TestZoneConfigForMultiRegionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc           string
		localityConfig descpb.TableDescriptor_LocalityConfig
		regionConfig   multiregion.RegionConfig
		expected       zonepb.ZoneConfig
	}{
		{
			desc: "4-region global table with zone survival",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_Global_{
					Global: &descpb.TableDescriptor_LocalityConfig_Global{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
			),
			expected: zonepb.ZoneConfig{
				GlobalReads:               proto.Bool(true),
				InheritedConstraints:      true,
				InheritedLeasePreferences: true,
			},
		},
		{
			desc: "4-region global table with region survival",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_Global_{
					Global: &descpb.TableDescriptor_LocalityConfig_Global{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
			),
			expected: zonepb.ZoneConfig{
				GlobalReads:               proto.Bool(true),
				InheritedConstraints:      true,
				InheritedLeasePreferences: true,
			},
		},
		{
			desc: "4-region regional by row table with zone survival",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByRow_{
					RegionalByRow: &descpb.TableDescriptor_LocalityConfig_RegionalByRow{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by row table with region survival",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByRow_{
					RegionalByRow: &descpb.TableDescriptor_LocalityConfig_RegionalByRow{},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with zone survival on primary region",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
					RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
						Region: nil,
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with regional survival on primary region",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
					RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
						Region: nil,
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
			),
			expected: *(zonepb.NewZoneConfig()),
		},
		{
			desc: "4-region regional by table with zone survival on non primary region",
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
					RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
			localityConfig: descpb.TableDescriptor_LocalityConfig{
				Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
					RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
						Region: protoRegionName("region_c"),
					},
				},
			},
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
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
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := zoneConfigForMultiRegionTable(tc.localityConfig, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, *zc)
		})
	}
}

func TestZoneConfigForMultiRegionPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc         string
		region       descpb.RegionName
		regionConfig multiregion.RegionConfig
		expected     zonepb.ZoneConfig
	}{
		{
			desc:   "4-region table with zone survivability",
			region: "region_a",
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
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
				descpb.RegionNames{
					"region_b",
					"region_c",
					"region_a",
					"region_d",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				descpb.InvalidID,
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
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			zc, err := zoneConfigForMultiRegionPartition(tc.region, tc.regionConfig)
			require.NoError(t, err)
			require.Equal(t, tc.expected, zc)
		})
	}
}
