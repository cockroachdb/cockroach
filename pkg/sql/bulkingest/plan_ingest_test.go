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

func TestTaskSliceOneWorker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tasks := []int{1, 2, 3, 4, 5}
	require.Equal(t, taskSlice(tasks, 0, 1), []int{1, 2, 3, 4, 5})
}

func TestTaskSliceThreeWorkers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tasks := []int{1, 2, 3, 4, 5}
	require.Equal(t, taskSlice(tasks, 0, 3), []int{1, 2})
	require.Equal(t, taskSlice(tasks, 1, 3), []int{3, 4})
	require.Equal(t, taskSlice(tasks, 2, 3), []int{5})
}

func TestTaskSliceMoreWorkersThanTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tasks := []int{1, 2}
	require.Equal(t, taskSlice(tasks, 0, 4), []int{1})
	require.Equal(t, taskSlice(tasks, 1, 4), []int{2})
	require.Empty(t, taskSlice(tasks, 2, 4))
	require.Empty(t, taskSlice(tasks, 3, 4))
}
