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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	sqlDB.Exec(t, `CREATE USER test_upgrade_user`)
	sqlDB.Exec(t, `CREATE USER test_unaffected_user`)
	sqlDB.Exec(t, `CREATE TABLE tbl1 (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `GRANT CREATE ON TABLE tbl1 TO test_upgrade_user WITH GRANT OPTION`)
	sqlDB.Exec(t, `GRANT SELECT ON TABLE tbl1 TO test_unaffected_user`)
	sqlDB.Exec(t, `CREATE TABLE tbl2 (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `GRANT CREATE ON TABLE tbl2 TO test_upgrade_user`)
	sqlDB.Exec(t, `GRANT ALL ON TABLE tbl2 TO test_unaffected_user`)

	// Create a sequence and a view with CREATE granted. The migration must skip
	// these because REFERENCES is not a valid privilege on sequences or views.
	sqlDB.Exec(t, `CREATE SEQUENCE seq1`)
	sqlDB.Exec(t, `GRANT SELECT, CREATE ON TABLE seq1 TO test_upgrade_user`)
	sqlDB.Exec(t, `CREATE VIEW v1 AS SELECT 1 FROM tbl1`)
	sqlDB.Exec(t, `GRANT SELECT, CREATE ON TABLE v1 TO test_upgrade_user`)

	// Get table IDs for post-upgrade verification.
	var tableID1, tableID2 int
	sqlDB.QueryRow(t, `SELECT 'tbl1'::regclass::oid::int`).Scan(&tableID1)
	sqlDB.QueryRow(t, `SELECT 'tbl2'::regclass::oid::int`).Scan(&tableID2)

	descDB := ts.InternalDB().(descs.DB)
	testUpgradeUser := username.MakeSQLUsernameFromPreNormalizedString("test_upgrade_user")
	testUnaffectedUser := username.MakeSQLUsernameFromPreNormalizedString("test_unaffected_user")

	// Verify test_upgrade_user has CREATE but not REFERENCES before the upgrade.
	// Also snapshot test_unaffected_user's privilege bits to verify they are
	// unchanged after migration (SELECT-only on table1, ALL on table2).
	type privSnapshot struct {
		privileges      uint64
		withGrantOption uint64
	}
	unaffectedBefore := make(map[int]privSnapshot)
	err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		getTable := func(id int) catalog.TableDescriptor {
			tbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).
				WithoutNonPublic().Get().Table(ctx, descpb.ID(id))
			require.NoError(t, err)
			return tbl
		}
		for _, id := range []int{tableID1, tableID2} {
			userPrivs, found := getTable(id).GetPrivileges().FindUser(testUpgradeUser)
			require.True(t, found, "expected test_upgrade_user in privileges on table %d", id)
			require.True(t, privilege.CREATE.IsSetIn(userPrivs.Privileges),
				"expected CREATE before upgrade on table %d", id)
			require.False(t, privilege.REFERENCES.IsSetIn(userPrivs.Privileges),
				"expected no REFERENCES before upgrade on table %d", id)

			uPrivs, found := getTable(id).GetPrivileges().FindUser(testUnaffectedUser)
			require.True(t, found, "expected test_unaffected_user in privileges on table %d", id)
			unaffectedBefore[id] = privSnapshot{
				privileges:      uPrivs.Privileges,
				withGrantOption: uPrivs.WithGrantOption,
			}
		}
		return nil
	})
	require.NoError(t, err)

	// GRANT REFERENCES should be blocked before the version is active.
	_, err = sqlDB.DB.ExecContext(ctx, `GRANT REFERENCES ON TABLE tbl1 TO test_upgrade_user`)
	require.ErrorContains(t, err, "REFERENCES privilege is not available until upgrade to 26.3 is finalized")

	// Run the upgrade.
	upgrades.Upgrade(
		t, ts.SQLConn(t),
		clusterversion.V26_3_GrantReferencesToUsersWithCreate, nil, false,
	)

	// Get sequence and view IDs for post-upgrade verification.
	var seqID, viewID int
	sqlDB.QueryRow(t, `SELECT 'seq1'::regclass::oid::int`).Scan(&seqID)
	sqlDB.QueryRow(t, `SELECT 'v1'::regclass::oid::int`).Scan(&viewID)

	// Verify privileges after the upgrade.
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		getTable := func(id int) catalog.TableDescriptor {
			tbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).
				WithoutNonPublic().Get().Table(ctx, descpb.ID(id))
			require.NoError(t, err)
			return tbl
		}

		// test_upgrade_user gets REFERENCES mirroring CREATE's grant option.
		privs1, found := getTable(tableID1).GetPrivileges().FindUser(testUpgradeUser)
		require.True(t, found)
		require.True(t, privilege.REFERENCES.IsSetIn(privs1.Privileges),
			"expected REFERENCES after upgrade on table1")
		require.True(t, privilege.REFERENCES.IsSetIn(privs1.WithGrantOption),
			"REFERENCES should mirror CREATE's WITH GRANT OPTION")

		privs2, found := getTable(tableID2).GetPrivileges().FindUser(testUpgradeUser)
		require.True(t, found)
		require.True(t, privilege.REFERENCES.IsSetIn(privs2.Privileges),
			"expected REFERENCES after upgrade on table2")
		require.False(t, privilege.REFERENCES.IsSetIn(privs2.WithGrantOption),
			"REFERENCES should mirror CREATE's lack of grant option")

		// Sequences and views must not get REFERENCES.
		for _, id := range []int{seqID, viewID} {
			privs, found := getTable(id).GetPrivileges().FindUser(testUpgradeUser)
			require.True(t, found,
				"expected test_upgrade_user in privileges on descriptor %d", id)
			require.False(t, privilege.REFERENCES.IsSetIn(privs.Privileges),
				"REFERENCES must not be granted on non-table descriptor %d", id)
		}

		// test_unaffected_user's privileges unchanged.
		for _, id := range []int{tableID1, tableID2} {
			uPrivs, found := getTable(id).GetPrivileges().FindUser(testUnaffectedUser)
			require.True(t, found,
				"expected test_unaffected_user in privileges on table %d", id)
			before := unaffectedBefore[id]
			require.Equal(t, before.privileges, uPrivs.Privileges,
				"test_unaffected_user privilege bits should not change on table %d", id)
			require.Equal(t, before.withGrantOption, uPrivs.WithGrantOption,
				"test_unaffected_user grant option bits should not change on table %d", id)
		}
		return nil
	})
	require.NoError(t, err)
}
