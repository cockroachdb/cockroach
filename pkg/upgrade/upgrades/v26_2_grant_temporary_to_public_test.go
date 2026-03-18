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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGrantTemporaryToPublic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_GrantTemporaryToPublic)

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
	ts := serverutils.StartServerOnly(t, clusterArgs.ServerArgs)
	defer ts.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	// Create a test database.
	sqlDB.Exec(t, `CREATE DATABASE test_temp_priv`)

	// Get the database ID.
	var dbID int
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_temp_priv' AND "parentID" = 0`).Scan(&dbID)

	// Simulate a pre-upgrade state by revoking the TEMPORARY privilege from
	// PUBLIC on the test database. This mimics a database created before the
	// TEMPORARY privilege was added.
	descDB := ts.InternalDB().(descs.DB)
	err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		mutableDB, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, descpb.ID(dbID))
		if err != nil {
			return err
		}
		if err := mutableDB.GetPrivileges().Revoke(
			username.PublicRoleName(),
			privilege.List{privilege.TEMPORARY},
			privilege.Database,
			false, /* grantOptionFor */
		); err != nil {
			return err
		}
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, mutableDB, txn.KV())
	})
	require.NoError(t, err)

	// Verify PUBLIC does not have TEMPORARY on the test database.
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		db, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, descpb.ID(dbID))
		if err != nil {
			return err
		}
		pubPrivs, found := db.GetPrivileges().FindUser(username.PublicRoleName())
		if found {
			require.False(t, privilege.TEMPORARY.IsSetIn(pubPrivs.Privileges),
				"expected PUBLIC to NOT have TEMPORARY before upgrade")
		}
		return nil
	})
	require.NoError(t, err)

	// Run the upgrade.
	upgrades.Upgrade(t, ts.SQLConn(t), clusterversion.V26_2_GrantTemporaryToPublic, nil, false)

	// Verify PUBLIC now has TEMPORARY on the test database.
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		db, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, descpb.ID(dbID))
		if err != nil {
			return err
		}
		pubPrivs, found := db.GetPrivileges().FindUser(username.PublicRoleName())
		require.True(t, found, "expected PUBLIC user to exist in privileges")
		require.True(t, privilege.TEMPORARY.IsSetIn(pubPrivs.Privileges),
			"expected PUBLIC to have TEMPORARY after upgrade")
		return nil
	})
	require.NoError(t, err)

	// Also verify that defaultdb (a system-created database) got the privilege.
	var defaultDBID int
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'defaultdb' AND "parentID" = 0`).Scan(&defaultDBID)

	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		db, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, descpb.ID(defaultDBID))
		if err != nil {
			return err
		}
		pubPrivs, found := db.GetPrivileges().FindUser(username.PublicRoleName())
		require.True(t, found, "expected PUBLIC user to exist on defaultdb")
		require.True(t, privilege.TEMPORARY.IsSetIn(pubPrivs.Privileges),
			"expected PUBLIC to have TEMPORARY on defaultdb after upgrade")
		return nil
	})
	require.NoError(t, err)
}
