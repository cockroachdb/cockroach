// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDeleteAllTenantsVersionTenantSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.V24_1.Version(),
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

	// findAllTenantsOverride returns the version object in
	// `system.tenant_settings` that applies to all tenants, or `nil` if
	// there is no such override.
	findAllTenantsOverride := func() *clusterversion.ClusterVersion {
		var value []byte
		err := sqlDB.QueryRow(
			"SELECT value FROM system.tenant_settings WHERE tenant_id = 0 AND name = 'version'",
		).Scan(&value)

		if err != nil {
			if errors.Is(err, gosql.ErrNoRows) {
				return nil
			}

			require.NoError(t, err)
		}

		var cv clusterversion.ClusterVersion
		require.NoError(t, protoutil.Unmarshal(value, &cv))
		return &cv
	}

	// First, simulate the situation by inserting the bad row.
	encodedVersion, err := clusterversion.EncodingFromVersionStr("23.1")
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`INSERT INTO system.tenant_settings(tenant_id, name, value, "last_updated", "value_type") VALUES ($1, $2, $3, now(), $4)`,
		0, "version", encodedVersion, "m",
	)
	require.NoError(t, err)
	override := findAllTenantsOverride()
	require.NotNil(t, override)
	require.Equal(t, "23.1", override.String())

	// Now, run the upgrades
	upgrades.Upgrade(
		t, sqlDB, clusterversion.V24_2_DeleteAllTenantSettingsVersion, nil, false,
	)

	// The all-tenants override should have been deleted.
	override = findAllTenantsOverride()
	require.Nil(t, override)
}
