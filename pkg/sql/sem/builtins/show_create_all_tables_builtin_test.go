// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"math"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestTopologicalSort(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	monitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	acc := monitor.MakeBoundAccount()

	testCases := []struct {
		name         string
		dependsOnIDs map[int64][]int64
		expected     []int64
	}{
		{
			name: "basic test",
			dependsOnIDs: map[int64][]int64{
				1: {1, 2},
				2: {},
				3: nil,
			},
			expected: []int64{2, 1, 3},
		},
		{
			name: "depends on references non-existent IDs",
			dependsOnIDs: map[int64][]int64{
				1: {1, 2},
				2: {},
				3: nil,
				4: {5, 6},
				6: {2, 3},
			},
			expected: []int64{2, 1, 3, 6, 4},
		},
		{
			name: "handles cycles",
			dependsOnIDs: map[int64][]int64{
				1: {2},
				2: {3},
				3: {4},
				4: {5},
				5: {6},
				6: {1},
			},
			expected: []int64{6, 5, 4, 3, 2, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seen := map[int64]struct{}{}
			var allIDs, sorted []int64
			for i := range tc.dependsOnIDs {
				allIDs = append(allIDs, i)
			}
			// Sort by ids to guarantee stable output.
			sort.Slice(allIDs, func(i, j int) bool {
				return allIDs[i] < allIDs[j]
			})
			for _, i := range allIDs {
				err := topologicalSort(ctx, i, tc.dependsOnIDs, seen, &sorted, &acc)
				require.NoError(t, err)
			}
			require.Equal(t, tc.expected, sorted)
		})
	}
}
