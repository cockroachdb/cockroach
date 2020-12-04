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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestGenZoneConfigFromRegionConfigForDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc         string
		regionConfig descpb.DatabaseDescriptor_RegionConfig
		expected     *zonepb.ZoneConfig
	}{
		{
			desc: "one region, zone survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_a"},
				PrimaryRegion: "region_a",
				SurvivalGoal:  descpb.SurvivalGoal_ZONE_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
			},
		},
		{
			desc: "two regions, zone survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_b", "region_a"},
				PrimaryRegion: "region_a",
				SurvivalGoal:  descpb.SurvivalGoal_ZONE_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
			},
		},
		{
			desc: "three regions, zone survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_b", "region_c", "region_a"},
				PrimaryRegion: "region_b",
				SurvivalGoal:  descpb.SurvivalGoal_ZONE_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
			},
		},
		{
			desc: "three regions, region survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_b", "region_c", "region_a"},
				PrimaryRegion: "region_b",
				SurvivalGoal:  descpb.SurvivalGoal_REGION_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(3),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
				},
			},
		},
		{
			desc: "four regions, zone survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_b", "region_c", "region_a", "region_d"},
				PrimaryRegion: "region_b",
				SurvivalGoal:  descpb.SurvivalGoal_ZONE_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(4),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"}}},
				},
			},
		},
		{
			desc: "four regions, region survival",
			regionConfig: descpb.DatabaseDescriptor_RegionConfig{
				Regions:       []descpb.Region{"region_b", "region_c", "region_a", "region_d"},
				PrimaryRegion: "region_b",
				SurvivalGoal:  descpb.SurvivalGoal_REGION_FAILURE,
			},
			expected: &zonepb.ZoneConfig{
				NumReplicas: proto.Int32(4),
				LeasePreferences: []zonepb.LeasePreference{
					{Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
				},
				Constraints: []zonepb.ConstraintsConjunction{
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_b"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_c"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_a"}}},
					{NumReplicas: 1, Constraints: []zonepb.Constraint{{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_d"}}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			res := genZoneConfigFromRegionConfigForDatabase(tc.regionConfig)
			require.Equal(t, tc.expected, res)
		})
	}
}
