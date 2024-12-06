// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-mem"),
		Settings: cluster.MakeTestingClusterSettings(),
	})
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
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
