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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestAlterStatementsTablePKMigration verifies that V26_3_AlterStatementsTablePK
// migrates an existing system.statements table from PK(id) with a unique
// secondary index on fingerprint_id to PK(fingerprint_id) with the id column
// dropped. The pre-migration shape is established by injecting the legacy
// descriptor over the bootstrap-time descriptor.
func TestAlterStatementsTablePKMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t, "slow under deadlock+race")
	skip.UnderRace(t, "slow under race")

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_3_AlterStatementsTablePK)

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
	s := tc.Server(0)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// First upgrade to the version that creates the table.
	upgrades.Upgrade(
		t, tc.ServerConn(0),
		clusterversion.V26_2_AddSystemStatementsTable,
		nil,   /* done */
		false, /* expectError */
	)

	// Replace the bootstrap-time descriptor with the legacy (pre-V26_3) shape so
	// the migration has something to migrate.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.StatementsTable,
		getOldStatementsDescriptor)

	hasColumn := func(name string) bool {
		var exists bool
		sqlDB.QueryRow(t, `
			SELECT count(*) > 0
			FROM [SHOW COLUMNS FROM system.statements]
			WHERE column_name = $1
		`, name).Scan(&exists)
		return exists
	}
	hasIndex := func(name string) bool {
		var exists bool
		sqlDB.QueryRow(t, `
			SELECT count(*) > 0
			FROM [SHOW INDEXES FROM system.statements]
			WHERE index_name = $1
		`, name).Scan(&exists)
		return exists
	}
	primaryKeyColumns := func() []string {
		rows := sqlDB.QueryStr(t, `
			SELECT column_name
			FROM [SHOW INDEXES FROM system.statements]
			WHERE index_name = 'primary' AND storing = false
			ORDER BY seq_in_index
		`)
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, r[0])
		}
		return out
	}

	// Pre-migration: id is the PK, redundant unique index exists.
	require.True(t, hasColumn("id"), "expected id column before migration")
	require.True(t, hasIndex("statements_fingerprint_id_key"),
		"expected statements_fingerprint_id_key index before migration")
	require.Equal(t, []string{"id"}, primaryKeyColumns(),
		"expected id to be the primary key before migration")

	// Run the upgrade.
	upgrades.Upgrade(
		t, tc.ServerConn(0),
		clusterversion.V26_3_AlterStatementsTablePK,
		nil,   /* done */
		false, /* expectError */
	)

	// Post-migration: id column gone, redundant unique index gone, PK on fingerprint_id.
	require.False(t, hasColumn("id"), "expected id column to be dropped after migration")
	require.False(t, hasIndex("statements_fingerprint_id_key"),
		"expected statements_fingerprint_id_key index to be dropped after migration")
	require.Equal(t, []string{"fingerprint_id"}, primaryKeyColumns(),
		"expected fingerprint_id to be the primary key after migration")

	// The fingerprint secondary index should still exist.
	require.True(t, hasIndex("statements_fingerprint_idx"),
		"expected statements_fingerprint_idx to still exist after migration")

	// Confirm the table is still writable using the production INSERT pattern
	// from statementstore (ON CONFLICT against fingerprint_id).
	sqlDB.Exec(t, `INSERT INTO system.statements
		(fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (b'\x00\x00\x00\x00\x00\x00\x00\x02', 'SELECT 2', 'SELECT 2', 'defaultdb', '{}')
		ON CONFLICT (fingerprint_id) DO UPDATE SET last_upserted = now()`)
	sqlDB.Exec(t, `INSERT INTO system.statements
		(fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (b'\x00\x00\x00\x00\x00\x00\x00\x02', 'SELECT 2', 'SELECT 2', 'defaultdb', '{}')
		ON CONFLICT (fingerprint_id) DO UPDATE SET last_upserted = now()`)
}

// getOldStatementsDescriptor returns the system.statements table descriptor
// as it existed before V26_3_AlterStatementsTablePK: an id column as the
// primary key and a unique secondary index on fingerprint_id.
func getOldStatementsDescriptor() *descpb.TableDescriptor {
	uniqueRowIDExpr := descpb.Expression("unique_rowid()")
	nowExpr := descpb.Expression("now():::TIMESTAMPTZ")
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.StatementsTableName),
		ID:                      descpb.InvalidID, // filled in by InjectLegacyTable
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.SystemPublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDExpr},
			{Name: "fingerprint_id", ID: 2, Type: types.Bytes},
			{Name: "fingerprint", ID: 3, Type: types.String},
			{Name: "summary", ID: 4, Type: types.String},
			{Name: "db", ID: 5, Type: types.String},
			{Name: "metadata", ID: 6, Type: types.Jsonb},
			{Name: "created_at", ID: 7, Type: types.TimestampTZ, DefaultExpr: &nowExpr},
			{Name: "last_upserted", ID: 8, Type: types.TimestampTZ, DefaultExpr: &nowExpr},
		},
		NextColumnID: 9,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ID:          0,
				ColumnNames: []string{"id", "fingerprint_id", "fingerprint", "summary", "db", "metadata", "created_at", "last_upserted"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8},
			},
		},
		NextFamilyID: 1,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "statements_fingerprint_id_key",
				ID:                  2,
				Unique:              true,
				KeyColumnNames:      []string{"fingerprint_id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				StoreColumnNames:    []string{"fingerprint", "summary", "db"},
				StoreColumnIDs:      []descpb.ColumnID{3, 4, 5},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				ConstraintID:        2,
			},
			{
				Name:                "statements_fingerprint_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"fingerprint"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID:      4,
		Privileges:       catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID:   1,
		FormatVersion:    descpb.InterleavedFormatVersion,
		NextConstraintID: 3,
	}
}
