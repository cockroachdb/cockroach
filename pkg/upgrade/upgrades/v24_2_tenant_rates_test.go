// Copyright 2024 The Cockroach Authors.
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

func TestTenantRatesMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	s, sqlDB := tc.Server(0), tc.ServerConn(0)

	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())
	_, err := sqlDB.Exec("SELECT current_rates, next_rates FROM system.tenant_usage")
	require.Error(t, err, "system.tenant_usage rates columns should not exist")
	upgrades.Upgrade(t, sqlDB, clusterversion.V24_2_TenantRates, nil, false)
	_, err = sqlDB.Exec("SELECT current_rates, next_rates FROM system.tenant_usage")
	require.NoError(t, err, "system.tenant_usage rates columns should exist")
}
