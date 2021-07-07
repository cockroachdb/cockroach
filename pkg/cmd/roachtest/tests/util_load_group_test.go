// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/stretchr/testify/require"
)

func TestLoadGroups(t *testing.T) {
	for _, tc := range []struct {
		numZones, numRoachNodes, numLoadNodes int
		loadGroups                            loadGroupList
	}{
		{
			3, 9, 3,
			loadGroupList{
				{
					option.NodeListOption{1, 2, 3},
					option.NodeListOption{4},
				},
				{
					option.NodeListOption{5, 6, 7},
					option.NodeListOption{8},
				},
				{
					option.NodeListOption{9, 10, 11},
					option.NodeListOption{12},
				},
			},
		},
		{
			3, 9, 1,
			loadGroupList{
				{
					option.NodeListOption{1, 2, 3, 4, 5, 6, 7, 8, 9},
					option.NodeListOption{10},
				},
			},
		},
		{
			4, 8, 2,
			loadGroupList{
				{
					option.NodeListOption{1, 2, 3, 4},
					option.NodeListOption{9},
				},
				{
					option.NodeListOption{5, 6, 7, 8},
					option.NodeListOption{10},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("%d/%d/%d", tc.numZones, tc.numRoachNodes, tc.numLoadNodes),
			func(t *testing.T) {
				l := option.NodeLister{NodeCount: tc.numRoachNodes + tc.numLoadNodes, Fatalf: t.Fatalf}
				lg := makeLoadGroups(l, tc.numZones, tc.numRoachNodes, tc.numLoadNodes)
				require.EqualValues(t, lg, tc.loadGroups)
			})
	}
	t.Run("panics with too many load nodes", func(t *testing.T) {
		require.Panics(t, func() {

			numZones, numRoachNodes, numLoadNodes := 2, 4, 3
			makeLoadGroups(nil, numZones, numRoachNodes, numLoadNodes)
		}, "Failed to panic when number of load nodes exceeded number of zones")
	})
	t.Run("panics with unequal zones per load node", func(t *testing.T) {
		require.Panics(t, func() {
			numZones, numRoachNodes, numLoadNodes := 4, 4, 3
			makeLoadGroups(nil, numZones, numRoachNodes, numLoadNodes)
		}, "Failed to panic when number of zones is not divisible by number of load nodes")
	})
}
