// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMVCCStatisticsMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey),
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)
	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())

	// Introduce the sequence.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.Permanent_V23_2_MVCCStatisticsTable,
		nil,
		false,
	)

	db := tc.ServerConn(0)
	defer db.Close()

	t.Run("mvcc_statistics_table", func(t *testing.T) {
		_, err := sqlDB.Exec("SELECT * FROM system.mvcc_statistics")
		require.NoError(t, err, "system.public.mvcc_statistics exists")
	})

	t.Run("mvcc_stats_job", func(t *testing.T) {
		row := db.QueryRow("SELECT count(*) FROM system.public.jobs WHERE id = 104")
		require.NotNil(t, row)
		require.NoError(t, row.Err())
		var count int
		err := row.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}
