// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraints

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

func TestConstraints(t *testing.T) {
	testCases := []struct {
		description string
		clusterInfo []state.Region
	}{
		{
			clusterInfo: []state.Region{{
				Name: "a",
				Zones: []state.Zone{
					state.NewZoneWithSingleStore("a1", 1),
				},
			}},
		},
		//{
		//	clusterInfo: []state.Region{{
		//		Name: "a",
		//		Zones: []state.Zone{
		//			state.NewZoneWithSingleStore("a1", 5),
		//			state.NewZoneWithSingleStore("a2", 5),
		//			state.NewZoneWithSingleStore("a3", 5),
		//		},
		//	}},
		//},
	}
	// 3 zones 1 region 3 nodes in total
	// number of zones * n * n + number of regions * n * n + n * n
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			constraints := allConstraints(tc.clusterInfo)
			for _, constraint := range constraints {
				fmt.Println(constraint)
				//spanconfigtestutils.ParseZoneConfig(t, constraint).AsSpanConfig()
			}
		})
	}
}
