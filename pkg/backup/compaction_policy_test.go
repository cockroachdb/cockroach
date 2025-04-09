// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackupCompactionHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testcases := []struct {
		name       string
		sizes      []int64
		windowSize int
		expected   [2]int
	}{
		{
			name:       "optimal at the beginning",
			sizes:      []int64{1, 2, 2, 5, 1, 2},
			windowSize: 3,
			expected:   [2]int{0, 3},
		},
		{
			name:       "optimal in the middle",
			sizes:      []int64{1, 3, 4, 5, 5, 2},
			windowSize: 3,
			expected:   [2]int{2, 5},
		},
		{
			name:       "optimal at the end",
			sizes:      []int64{1, 3, 4, 3, 2, 2},
			windowSize: 3,
			expected:   [2]int{3, 6},
		},
		{
			name:       "tied heuristic",
			sizes:      []int64{2, 3, 4, 1, 2, 3},
			windowSize: 3,
			expected:   [2]int{0, 3},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := minDeltaWindow(tc.sizes, tc.windowSize)
			require.Equal(t, tc.expected[0], start)
			require.Equal(t, tc.expected[1], end)
		})
	}

	st := cluster.MakeTestingClusterSettings()
	t.Run("too large window", func(t *testing.T) {
		var windowSize int64 = 5
		chain := make([]backuppb.BackupManifest, 5)
		backupCompactionWindow.Override(ctx, &st.SV, windowSize)
		execCfg := &sql.ExecutorConfig{Settings: st}
		_, _, err := minSizeDeltaHeuristic(ctx, execCfg, chain)
		require.Error(t, err)
	})
}
