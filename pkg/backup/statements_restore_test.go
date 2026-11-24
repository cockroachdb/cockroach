// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/statementstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStatementsRestoreFromOldBackup verifies that restore succeeds when the
// backup was taken with the pre-V26_3_AlterStatementsTablePK schema (which has
// an extra `id` column). Without statementsRestoreFunc, defaultSystemTableRestoreFunc
// would issue `INSERT INTO system.statements (SELECT * FROM tempDB.statements)`,
// which fails the column-count check on the post-migration target.
//
// The restore func short-circuits to a no-op for the legacy schema because the
// statementstore writer was only wired up in v26.3, so any legacy backup is
// guaranteed to be empty in production. The test asserts the no-op behavior
// holds even when rows are present, so future schema changes don't accidentally
// reintroduce the column-mismatch failure mode.
func TestStatementsRestoreFromOldBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	backuptestutils.DisableFastRestoreForTest(t)

	ctx := context.Background()
	externalDir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Disable the statementstore writer on both clusters. Otherwise the target
	// cluster's background flush goroutine races our post-restore count(*) and
	// can populate system.statements with fingerprints from the RESTORE itself.
	makeSettings := func() *cluster.Settings {
		st := cluster.MakeTestingClusterSettings()
		statementstore.StatementStoreEnabled.Override(ctx, &st.SV, false)
		return st
	}

	// Source cluster: inject the pre-V26_3 statements descriptor.
	params := base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		ExternalIODir:     externalDir,
		Settings:          makeSettings(),
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlDBRunner := sqlutils.MakeSQLRunner(sqlDB)

	var tableID descpb.ID
	sqlDBRunner.QueryRow(t, "SELECT 'system.statements'::REGCLASS::OID").Scan(&tableID)

	err := s.InternalDB().(descs.DB).DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		legacyDesc := statementstore.GetOldStatementsDescriptor(tableID)
		tab, err := txn.Descriptors().MutableByName(txn.KV()).Table(ctx,
			systemschema.SystemDB, schemadesc.GetPublicSchema(), string(catconstants.StatementsTableName))
		if err != nil {
			return err
		}
		builder := tabledesc.NewBuilder(legacyDesc)
		if err := builder.RunPostDeserializationChanges(); err != nil {
			return err
		}
		tab.TableDescriptor = builder.BuildCreatedMutableTable().TableDescriptor
		tab.Version = tab.ClusterVersion().Version + 1
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, tab, txn.KV())
	})
	require.NoError(t, err)

	// Insert a row using the legacy schema. In production this never happens
	// (the writer is gated on V26_3), but we want to lock in the fact that
	// statementsRestoreFunc tolerates the case where it does — silently dropping
	// the legacy rows rather than failing restore.
	sqlDBRunner.Exec(t, `
		INSERT INTO system.statements (id, fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (1, b'\x00\x00\x00\x00\x00\x00\x00\x01', 'SELECT 1', 'SELECT 1', 'defaultdb', '{}')
	`)

	sqlDBRunner.Exec(t, `BACKUP INTO 'nodelocal://1/statements_legacy'`)

	// Target cluster: bootstraps with the post-V26_3 schema.
	paramsRestore := base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		ExternalIODir:     externalDir,
		Settings:          makeSettings(),
	}
	sRestore, sqlDBRestore, _ := serverutils.StartServer(t, paramsRestore)
	defer sRestore.Stopper().Stop(ctx)
	sqlDBRestoreRunner := sqlutils.MakeSQLRunner(sqlDBRestore)

	// Restore must succeed even though the source schema has an extra column.
	sqlDBRestoreRunner.Exec(t, `RESTORE FROM LATEST IN 'nodelocal://1/statements_legacy'`)

	// Legacy rows are intentionally dropped by statementsRestoreFunc.
	sqlDBRestoreRunner.CheckQueryResults(t,
		`SELECT count(*) FROM system.statements`,
		[][]string{{"0"}},
	)

	// Confirm the post-restore table is writable using the production INSERT
	// pattern from statementstore (ON CONFLICT against fingerprint_id).
	sqlDBRestoreRunner.Exec(t, `INSERT INTO system.statements
		(fingerprint_id, fingerprint, summary, db, metadata)
		VALUES (b'\x00\x00\x00\x00\x00\x00\x00\x02', 'SELECT 2', 'SELECT 2', 'defaultdb', '{}')
		ON CONFLICT (fingerprint_id) DO UPDATE SET last_upserted = now()`)
}
