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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_3_AddResourceGroupsTable)

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

	_, err := sqlDB.Exec("SELECT * FROM system.resource_groups")
	require.Error(t, err)
	_, err = sqlDB.Exec("SELECT nextval('system.resource_group_id_seq')")
	require.Error(t, err)

	upgrades.Upgrade(t, sqlDB, clusterversion.V26_3_AddResourceGroupsTable, nil, false)

	_, err = sqlDB.Exec("SELECT id, name, config FROM system.resource_groups")
	require.NoError(t, err)

	// Reserved range: ids < 16 must be rejected by the CHECK.
	_, err = sqlDB.Exec(
		"INSERT INTO system.resource_groups (id, name, config) VALUES (0, 'a', b'')")
	require.Error(t, err)
	_, err = sqlDB.Exec(
		"INSERT INTO system.resource_groups (id, name, config) VALUES (15, 'b', b'')")
	require.Error(t, err)

	// id = 16 is the lowest legal value.
	_, err = sqlDB.Exec(
		"INSERT INTO system.resource_groups (id, name, config) VALUES (16, 'high', b'')")
	require.NoError(t, err)

	// Name is unique within the tenant.
	_, err = sqlDB.Exec(
		"INSERT INTO system.resource_groups (id, name, config) VALUES (17, 'high', b'')")
	require.Error(t, err)

	// The sequence must never hand out an id in the reserved range.
	var nextID int64
	require.NoError(t,
		sqlDB.QueryRow("SELECT nextval('system.resource_group_id_seq')").Scan(&nextID))
	require.GreaterOrEqual(t, nextID, int64(16))
}
