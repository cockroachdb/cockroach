// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package multiregion_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
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
				descpb.RegionNames{
					"region_a",
					"region_b",
				},
				"region_b",
				descpb.SurvivalGoal_ZONE_FAILURE,
				descpb.InvalidID,
			),
		},
		{
			err: "3 regions are required for surviving a region failure",
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{
					"region_a",
					"region_b",
				},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
			),
		},
		{
			err: "expected > 0 number of regions in the region config",
			regionConfig: multiregion.MakeRegionConfig(
				descpb.RegionNames{},
				"region_b",
				descpb.SurvivalGoal_REGION_FAILURE,
				validRegionEnumID,
			),
		},
	}

	for _, tc := range testCases {
		err := multiregion.ValidateRegionConfig(tc.regionConfig)

		require.Error(t, err)
		require.True(t, testutils.IsError(err, tc.err), "expected err %v, got %v", tc.err, err)
	}
}
