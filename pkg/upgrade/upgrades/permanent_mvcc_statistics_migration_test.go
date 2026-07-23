// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMVCCStatisticsMigration is testing the upgrade that was associated with
// V23_2_MVCCStatisticsTable. We no longer support versions this old, but we
// still need to test that it happens as expected when creating a new cluster.
func TestMVCCStatisticsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	defer sqlDB.Close()

	t.Run("mvcc_statistics_table", func(t *testing.T) {
		_, err := sqlDB.Exec("SELECT * FROM system.mvcc_statistics")
		require.NoError(t, err, "system.public.mvcc_statistics exists")
	})

	t.Run("mvcc_stats_job", func(t *testing.T) {
		row := sqlDB.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 104")
		require.NotNil(t, row)
		require.NoError(t, row.Err())
		var count int
		err := row.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}
