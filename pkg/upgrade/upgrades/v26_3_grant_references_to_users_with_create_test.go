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

func TestGrantReferencesToUsersWithCreate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(
		t, clusterversion.V26_3_GrantReferencesToUsersWithCreate,
	)

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	// Create test tables and grant CREATE to a user with different grant options.
	sqlDB.Exec(t, `CREATE USER test_ref_user`)
	sqlDB.Exec(t, `CREATE TABLE test_ref_table (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `GRANT CREATE ON TABLE test_ref_table TO test_ref_user WITH GRANT OPTION`)
	sqlDB.Exec(t, `CREATE TABLE test_ref_table2 (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `GRANT CREATE ON TABLE test_ref_table2 TO test_ref_user`)

	// Get table IDs for post-upgrade verification.
	var tableID1, tableID2 int
	sqlDB.QueryRow(t,
		`SELECT 'test_ref_table'::regclass::oid::int`,
	).Scan(&tableID1)
	sqlDB.QueryRow(t,
		`SELECT 'test_ref_table2'::regclass::oid::int`,
	).Scan(&tableID2)

	// Verify the user has CREATE but not REFERENCES before the upgrade.
	var hasCreate, hasReferences bool
	sqlDB.QueryRow(t, `
		SELECT
			bool_or(privilege_type = 'CREATE'),
			bool_or(privilege_type = 'REFERENCES')
		FROM [SHOW GRANTS ON TABLE test_ref_table]
		WHERE grantee = 'test_ref_user'
	`).Scan(&hasCreate, &hasReferences)
	require.True(t, hasCreate, "expected test_ref_user to have CREATE before upgrade")
	require.False(t, hasReferences, "expected test_ref_user to NOT have REFERENCES before upgrade")

	// Run the upgrade.
	upgrades.Upgrade(
		t, ts.SQLConn(t),
		clusterversion.V26_3_GrantReferencesToUsersWithCreate, nil, false,
	)

	descDB := ts.InternalDB().(descs.DB)
	testRefUser := username.MakeSQLUsernameFromPreNormalizedString("test_ref_user")

	// Verify the user now has REFERENCES on test_ref_table with grant option,
	// mirroring CREATE's grant option.
	err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		tbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).
			WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID1))
		if err != nil {
			return err
		}
		userPrivs, found := tbl.GetPrivileges().FindUser(testRefUser)
		require.True(t, found, "expected test_ref_user in privileges")
		require.True(t, privilege.REFERENCES.IsSetIn(userPrivs.Privileges),
			"expected REFERENCES after upgrade")
		require.True(t, privilege.REFERENCES.IsSetIn(userPrivs.WithGrantOption),
			"expected REFERENCES WITH GRANT OPTION (mirroring CREATE)")
		return nil
	})
	require.NoError(t, err)

	// Verify the user has REFERENCES on test_ref_table2 without grant option,
	// mirroring CREATE's lack of grant option.
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		tbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).
			WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID2))
		if err != nil {
			return err
		}
		userPrivs, found := tbl.GetPrivileges().FindUser(testRefUser)
		require.True(t, found, "expected test_ref_user in privileges")
		require.True(t, privilege.REFERENCES.IsSetIn(userPrivs.Privileges),
			"expected REFERENCES after upgrade")
		require.False(t, privilege.REFERENCES.IsSetIn(userPrivs.WithGrantOption),
			"expected REFERENCES WITHOUT grant option (mirroring CREATE)")
		return nil
	})
	require.NoError(t, err)
}
