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
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestDeleteVersionTenantSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.V24_1.Version(),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	type versionOverride struct {
		tenantID int
		version  clusterversion.ClusterVersion
	}

	// findVersionOverrides returns the version overrides in
	// the `system.tenant_settings` table.
	findVersionOverrides := func() []versionOverride {
		var overrides []versionOverride
		rows, err := sqlDB.Query(
			"SELECT tenant_id, value FROM system.tenant_settings WHERE name = 'version' ORDER BY tenant_id",
		)
		require.NoError(t, err)

		for rows.Next() {
			var tenantID int
			var value []byte
			err := rows.Scan(&tenantID, &value)
			require.NoError(t, err)

			var cv clusterversion.ClusterVersion
			require.NoError(t, protoutil.Unmarshal(value, &cv))
			overrides = append(overrides, versionOverride{tenantID, cv})
		}

		require.NoError(t, rows.Err())
		return overrides
	}

	// First, simulate the situation by inserting the bad data.
	encodedVersion, err := clusterversion.EncodingFromVersionStr("23.1")
	require.NoError(t, err)

	tenantIDs := []int{0, 10}
	for _, tenantID := range tenantIDs {
		_, err = sqlDB.Exec(
			`INSERT INTO system.tenant_settings(tenant_id, name, value, "last_updated", "value_type") VALUES ($1, $2, $3, now(), $4)`,
			tenantID, "version", encodedVersion, "m",
		)
		require.NoError(t, err)
	}

	overrides := findVersionOverrides()
	require.Len(t, overrides, len(tenantIDs))

	for j, tenantID := range tenantIDs {
		require.Equal(t, tenantID, overrides[j].tenantID)
		require.Equal(t, "23.1", overrides[j].version.String())
	}

	// Now, run the upgrades
	upgrades.Upgrade(
		t, sqlDB, clusterversion.V24_2_DeleteTenantSettingsVersion, nil, false,
	)

	// The version override should have been deleted.
	overrides = findVersionOverrides()
	require.Empty(t, overrides)
}
