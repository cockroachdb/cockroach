// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTaskSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		tasks      []int
		workerID   int
		numWorkers int
		expected   []int
	}{
		{
			name:       "One worker",
			tasks:      []int{1, 2, 3, 4, 5},
			workerID:   0,
			numWorkers: 1,
			expected:   []int{1, 2, 3, 4, 5},
		},
		{
			name:       "Three workers, worker 0",
			tasks:      []int{1, 2, 3, 4, 5},
			workerID:   0,
			numWorkers: 3,
			expected:   []int{1, 2},
		},
		{
			name:       "Three workers, worker 1",
			tasks:      []int{1, 2, 3, 4, 5},
			workerID:   1,
			numWorkers: 3,
			expected:   []int{3, 4},
		},
		{
			name:       "Three workers, worker 2",
			tasks:      []int{1, 2, 3, 4, 5},
			workerID:   2,
			numWorkers: 3,
			expected:   []int{5},
		},
		{
			name:       "More workers than tasks, worker 0",
			tasks:      []int{1, 2},
			workerID:   0,
			numWorkers: 4,
			expected:   []int{1},
		},
		{
			name:       "More workers than tasks, worker 1",
			tasks:      []int{1, 2},
			workerID:   1,
			numWorkers: 4,
			expected:   []int{2},
		},
		{
			name:       "More workers than tasks, worker 2",
			tasks:      []int{1, 2},
			workerID:   2,
			numWorkers: 4,
			expected:   nil,
		},
		{
			name:       "More workers than tasks, worker 3",
			tasks:      []int{1, 2},
			workerID:   3,
			numWorkers: 4,
			expected:   nil,
		},
		{
			name:       "5 tasks, 4 workers, worker 2",
			tasks:      []int{1, 2, 3, 4, 5},
			workerID:   2,
			numWorkers: 4,
			expected:   []int{5},
		},
		{
			name:       "7 tasks, 5 workers, worker 3",
			tasks:      []int{1, 2, 3, 4, 5, 6, 7},
			workerID:   3,
			numWorkers: 5,
			expected:   []int{7},
		},
		{
			name:       "10 tasks, 7 workers, worker 5",
			tasks:      []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			workerID:   5,
			numWorkers: 7,
			expected:   nil,
		},
		{
			name:       "4 tasks, 3 workers, worker 1",
			tasks:      []int{1, 2, 3, 4},
			workerID:   1,
			numWorkers: 3,
			expected:   []int{3, 4},
		},
		{
			name:       "Single task, multiple workers, middle worker",
			tasks:      []int{1},
			workerID:   1,
			numWorkers: 3,
			expected:   nil,
		},
		{
			name:       "Exact division - 6 tasks, 3 workers, worker 1",
			tasks:      []int{1, 2, 3, 4, 5, 6},
			workerID:   1,
			numWorkers: 3,
			expected:   []int{3, 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := taskSlice(tc.tasks, tc.workerID, tc.numWorkers)
			require.Equal(t, tc.expected, result)
		})
	}
}
