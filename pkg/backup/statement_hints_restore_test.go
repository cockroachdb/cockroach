// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStatementHintsRestoreFromOldBackup tests end-to-end restore from a
// backup with the old schema (pre-26.2) to a cluster with the new schema.
func TestStatementHintsRestoreFromOldBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a temporary directory for the backup.
	externalDir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Start a cluster to create a backup from.
	params := base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		ExternalIODir:     externalDir,
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlDBRunner := sqlutils.MakeSQLRunner(sqlDB)

	// Get the dynamic table ID for statement_hints.
	var tableID descpb.ID
	sqlDBRunner.QueryRow(t, "SELECT 'system.statement_hints'::REGCLASS::OID").Scan(&tableID)

	// Inject the old descriptor (before the new columns were added).
	// This simulates a 25.4 cluster that doesn't have the hint_type, hint_name,
	// enabled, and database columns.
	err := s.InternalDB().(descs.DB).DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		deprecatedDesc := hints.GetOldStatementHintsDescriptor(tableID)
		tab, err := txn.Descriptors().MutableByName(txn.KV()).Table(ctx,
			systemschema.SystemDB, schemadesc.GetPublicSchema(), string(catconstants.StatementHintsTableName))
		if err != nil {
			return err
		}
		builder := tabledesc.NewBuilder(deprecatedDesc)
		if err := builder.RunPostDeserializationChanges(); err != nil {
			return err
		}
		tab.TableDescriptor = builder.BuildCreatedMutableTable().TableDescriptor
		tab.Version = tab.ClusterVersion().Version + 1
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, tab, txn.KV())
	})
	require.NoError(t, err)

	// Insert data using the old schema (5 columns: row_id, hash, fingerprint,
	// hint, created_at).
	sqlDBRunner.Exec(t, `
		INSERT INTO system.statement_hints (fingerprint, hint)
		VALUES
			('SELECT * FROM t WHERE x = _', '\xDEADBEEF'::BYTES),
			('INSERT INTO t VALUES (_)', '\xCAFEBABE'::BYTES)
	`)

	// Take a backup.
	sqlDBRunner.Exec(t, `BACKUP INTO 'nodelocal://1/old_hints'`)

	// Start a new cluster to restore into (this will have the new schema with 9
	// columns).
	paramsRestore := base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		ExternalIODir:     externalDir,
	}
	sRestore, sqlDBRestore, _ := serverutils.StartServer(t, paramsRestore)
	defer sRestore.Stopper().Stop(ctx)
	sqlDBRestoreRunner := sqlutils.MakeSQLRunner(sqlDBRestore)

	// Restore the backup.
	sqlDBRestoreRunner.Exec(t, `RESTORE FROM LATEST IN 'nodelocal://1/old_hints'`)

	// Verify that the data was restored correctly with proper defaults.
	rows := sqlDBRestoreRunner.Query(t, `
		SELECT fingerprint, hint, hint_type, hint_name, enabled
		FROM system.statement_hints
		ORDER BY fingerprint
	`)
	defer rows.Close()

	type hintRow struct {
		fingerprint string
		hint        []byte
		hintType    string
		hintName    *string
		enabled     bool
	}

	var results []hintRow
	for rows.Next() {
		var r hintRow
		err := rows.Scan(&r.fingerprint, &r.hint, &r.hintType, &r.hintName, &r.enabled)
		require.NoError(t, err)
		results = append(results, r)
	}

	require.Len(t, results, 2)

	// First row.
	require.Equal(t, "INSERT INTO t VALUES (_)", results[0].fingerprint)
	require.Equal(t, []byte{0xCA, 0xFE, 0xBA, 0xBE}, results[0].hint)
	require.Equal(t, hintpb.HintTypeRewriteInlineHints, results[0].hintType, "hint_type should be backfilled")
	require.Nil(t, results[0].hintName, "hint_name should be NULL for old data")
	require.True(t, results[0].enabled, "enabled should default to true")

	// Second row.
	require.Equal(t, "SELECT * FROM t WHERE x = _", results[1].fingerprint)
	require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, results[1].hint)
	require.Equal(t, hintpb.HintTypeRewriteInlineHints, results[1].hintType, "hint_type should be backfilled")
	require.Nil(t, results[1].hintName, "hint_name should be NULL for old data")
	require.True(t, results[1].enabled, "enabled should default to true")

	// Verify we can insert new rows with all columns specified.
	sqlDBRestoreRunner.Exec(t, `
		INSERT INTO system.statement_hints (fingerprint, hint, hint_type, hint_name, enabled, database)
		VALUES ('UPDATE t SET x = _', '\xBEEF'::BYTES, 'optimization', 'update_hint', false, 'mydb')
	`)

	// Verify the new row has all columns populated.
	var fingerprint, hintType, hintName string
	var hint []byte
	var enabled bool
	sqlDBRestoreRunner.QueryRow(t, `
		SELECT fingerprint, hint, hint_type, hint_name, enabled
		FROM system.statement_hints
		WHERE fingerprint = 'UPDATE t SET x = _'
	`).Scan(&fingerprint, &hint, &hintType, &hintName, &enabled)

	require.Equal(t, "UPDATE t SET x = _", fingerprint)
	require.Equal(t, []byte{0xBE, 0xEF}, hint)
	require.Equal(t, "optimization", hintType)
	require.Equal(t, "update_hint", hintName)
	require.False(t, enabled)
}
