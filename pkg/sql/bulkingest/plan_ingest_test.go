package bulkingest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskSliceOneWorker(t *testing.T) {
	tasks := []int{1, 2, 3, 4, 5}
	require.Equal(t, taskSlice(tasks, 0, 1), []int{1, 2, 3, 4, 5})
}

func TestTaskSliceThreeWorkers(t *testing.T) {
	tasks := []int{1, 2, 3, 4, 5}
	require.Equal(t, taskSlice(tasks, 0, 3), []int{1, 2})
	require.Equal(t, taskSlice(tasks, 1, 3), []int{3, 4})
	require.Equal(t, taskSlice(tasks, 2, 3), []int{5})
}
