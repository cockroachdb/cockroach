// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDescriptorTxnKeyCleanup validates if an earlier version
// generates descriptor transaction keys, the version gate properly cleans
// them up. We also confirm no new keys are emitted after the version gate.
func TestDescriptorTxnKeyCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_DescriptorTxnKeyGeneration)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         (clusterversion.V26_2_DescriptorTxnKeyGeneration - 1).Version(),
				},
			},
		},
	}

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, clusterArgs.ServerArgs)
	defer ts.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	countDescriptorTxnKeys := func() int {
		db := ts.InternalDB().(descs.DB)
		count := 0
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			keyStart := ts.Codec().DescUpdatePrefix()
			rows, err := txn.KV().Scan(ctx, keyStart, keyStart.PrefixEnd(), 0)
			if err != nil {
				return err
			}
			count = len(rows)
			return nil
		}))
		return count
	}

	// Create a table and run a schema change on it.
	sqlDB.Exec(t, `CREATE TABLE t1 (n int primary key)`)
	sqlDB.Exec(t, `CREATE TABLE t3 (n int primary key)`)
	// Clean up any earlier keys.
	require.NoError(t, ts.LeaseManager().(*lease.Manager).CleanUpdateKeysForUpgrade(ctx))
	initialKeys := countDescriptorTxnKeys()
	sqlDB.Exec(t, `ALTER TABLE t1 RENAME to t2`)
	// We only expect one descriptor txn key.
	require.Equal(t, initialKeys+1, countDescriptorTxnKeys())
	// Run the upgrade, we should not generate any more keys.
	upgrades.Upgrade(t, ts.SQLConn(t), clusterversion.V26_2_DescriptorTxnKeyGeneration, nil, false)
	// No new keys should be generated here.
	sqlDB.Exec(t, `ALTER TABLE t3 RENAME to t4`)
	require.Equal(t, initialKeys+1, countDescriptorTxnKeys())
	// Next upgrade, should clean all of them up.
	upgrades.Upgrade(t, ts.SQLConn(t), clusterversion.V26_2_DescriptorTxnKeyCleanup, nil, false)
	require.Equal(t, 0, countDescriptorTxnKeys())
}
