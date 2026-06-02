// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestRestoreDescriptorRefsGate validates the dual-write/single-write
// behavior of the RESTORE job around the
// V26_3_DescriptorIDsInRestoreDetails cluster version gate:
//
//   - Below the gate, the job must populate both the legacy
//     RestoreDetails.{Database,Table,Type,Schema,Function}Descs slices
//     and the dedicated per-type info-key rows in system.job_info, so
//     that an older binary resuming the job can still drive it from the
//     payload.
//   - At or above the gate, the legacy slices must be empty and only
//     the info-key rows must be populated; this is what bounds the
//     RestoreDetails payload size so that the job's SetDetails write
//     no longer hits the raft command size limit at multi-million
//     descriptor scale.
//
// The test also exercises the reader fallback in tableDescRefs by
// inspecting the info-key rows directly via the package-private helper.
func TestRestoreDescriptorRefsGate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The below-gate subtest requires the gate version to still be above
	// MinSupported so that we can override the cluster version to one
	// step below it.
	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(
		t, clusterversion.V26_3_DescriptorIDsInRestoreDetails,
	)

	ctx := context.Background()

	type expectedCounts struct {
		databases, tables, types, schemas, functions int
	}

	// seedAndBackup populates a fresh database with one of each
	// descriptor type and returns the expected ref counts for a
	// subsequent DATABASE-level restore.
	seedAndBackup := func(t *testing.T, sqlDB *sqlutils.SQLRunner, dbName, backupURI string) expectedCounts {
		sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
		sqlDB.Exec(t, fmt.Sprintf("USE %s", dbName))
		sqlDB.Exec(t, "CREATE SCHEMA sc")
		sqlDB.Exec(t, "CREATE TYPE sc.color AS ENUM ('red', 'green', 'blue')")
		sqlDB.Exec(t, "CREATE TABLE sc.t (k INT PRIMARY KEY, c sc.color)")
		sqlDB.Exec(t, "CREATE FUNCTION sc.f() RETURNS INT LANGUAGE SQL AS 'SELECT 1'")
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE %s INTO '%s'", dbName, backupURI))
		// Drop the source so the restore creates fresh descriptors.
		sqlDB.Exec(t, "USE defaultdb")
		sqlDB.Exec(t, fmt.Sprintf("DROP DATABASE %s CASCADE", dbName))
		return expectedCounts{
			databases: 1,
			tables:    1,
			types:     2, // enum + array type
			schemas:   2, // implicit public + user-created sc
			functions: 1,
		}
	}

	runCase := func(t *testing.T, name string, override roachpb.Version, expectLegacy bool) {
		t.Run(name, func(t *testing.T) {
			var params base.TestClusterArgs
			// Run directly on the system tenant: a CV override on the
			// host cluster does not propagate to an auto-injected virtual
			// cluster, which then refuses to start at a binary version
			// newer than the host's logical version.
			params.ServerArgs.DefaultTestTenant = base.TestIsSpecificToStorageLayerAndNeedsASystemTenant
			if override != (roachpb.Version{}) {
				params.ServerArgs.Knobs.Server = &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         override,
				}
			}
			tc, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
				t, singleNode, backuptestutils.WithParams(params),
			)
			defer cleanupFn()

			backupURI := "nodelocal://1/" + name
			counts := seedAndBackup(t, sqlDB, "src", backupURI)

			// Pause the restore right before publishing so the descriptor
			// references written by createImportingDescriptors are
			// observable but the publish-time clobber has not yet run.
			sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_publishing_descriptors'")
			var jobID jobspb.JobID
			sqlDB.QueryRow(t, fmt.Sprintf(
				"RESTORE DATABASE src FROM LATEST IN '%s' WITH detached, new_db_name = 'dst'", backupURI,
			)).Scan(&jobID)
			jobutils.WaitForJobToPause(t, sqlDB, jobID)

			// The info-key rows must always be present on the new code
			// path, regardless of the cluster version.
			s := tc.ApplicationLayer(0)
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				assertRefsCount(t, ctx, txn, jobID, restoreDatabaseDescRefsKey, counts.databases)
				assertRefsCount(t, ctx, txn, jobID, restoreTableDescRefsKey, counts.tables)
				assertRefsCount(t, ctx, txn, jobID, restoreTypeDescRefsKey, counts.types)
				assertRefsCount(t, ctx, txn, jobID, restoreSchemaDescRefsKey, counts.schemas)
				assertRefsCount(t, ctx, txn, jobID, restoreFunctionDescRefsKey, counts.functions)
				return nil
			}))

			// The legacy slices on RestoreDetails are populated when CV
			// is below the gate (dual-write) and left nil otherwise.
			registry := s.JobRegistry().(*jobs.Registry)
			job, err := registry.LoadJob(ctx, jobID)
			require.NoError(t, err)
			details := job.Details().(jobspb.RestoreDetails)
			if expectLegacy {
				require.Len(t, details.DatabaseDescs, counts.databases, "legacy DatabaseDescs")
				require.Len(t, details.TableDescs, counts.tables, "legacy TableDescs")
				require.Len(t, details.TypeDescs, counts.types, "legacy TypeDescs")
				require.Len(t, details.SchemaDescs, counts.schemas, "legacy SchemaDescs")
				require.Len(t, details.FunctionDescs, counts.functions, "legacy FunctionDescs")
			} else {
				require.Empty(t, details.DatabaseDescs, "legacy DatabaseDescs should be empty above gate")
				require.Empty(t, details.TableDescs, "legacy TableDescs should be empty above gate")
				require.Empty(t, details.TypeDescs, "legacy TypeDescs should be empty above gate")
				require.Empty(t, details.SchemaDescs, "legacy SchemaDescs should be empty above gate")
				require.Empty(t, details.FunctionDescs, "legacy FunctionDescs should be empty above gate")
			}

			// Unblock and confirm the restore still completes.
			sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
			sqlDB.Exec(t, "RESUME JOB $1", jobID)
			jobutils.WaitForJobToSucceed(t, sqlDB, jobID)
		})
	}

	runCase(t, "below_gate",
		(clusterversion.V26_3_DescriptorIDsInRestoreDetails - 1).Version(),
		true /* expectLegacy */)
	runCase(t, "at_gate",
		roachpb.Version{} /* override: use the binary's latest version */, false /* expectLegacy */)
}

// TestRestoreDescriptorRefsReaderFallback validates that on the new
// binary, restore readers fall back to the legacy RestoreDetails
// descriptor slices when the info-key rows are absent. This is the
// state of a job created by an older binary that never wrote info-key
// rows (only possible during a mixed-version window).
//
// The test constructs the missing-info-key state by running a restore,
// pausing it, deleting the info-key rows, and confirming the read
// helpers still return the descriptor references via the legacy slices.
func TestRestoreDescriptorRefsReaderFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(
		t, clusterversion.V26_3_DescriptorIDsInRestoreDetails,
	)

	ctx := context.Background()

	// Run under the below-gate override so that the legacy slices are
	// populated and can serve the fallback path. The CV override does
	// not propagate to an auto-injected virtual cluster, so pin the
	// test to the system tenant.
	var params base.TestClusterArgs
	params.ServerArgs.DefaultTestTenant = base.TestIsSpecificToStorageLayerAndNeedsASystemTenant
	params.ServerArgs.Knobs.Server = &server.TestingKnobs{
		DisableAutomaticVersionUpgrade: make(chan struct{}),
		ClusterVersionOverride:         (clusterversion.V26_3_DescriptorIDsInRestoreDetails - 1).Version(),
	}
	tc, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, singleNode, backuptestutils.WithParams(params),
	)
	defer cleanupFn()

	sqlDB.Exec(t, "CREATE DATABASE src")
	sqlDB.Exec(t, "USE src")
	sqlDB.Exec(t, "CREATE SCHEMA sc")
	sqlDB.Exec(t, "CREATE TYPE sc.color AS ENUM ('red', 'green', 'blue')")
	sqlDB.Exec(t, "CREATE TABLE sc.t (k INT PRIMARY KEY, c sc.color)")
	sqlDB.Exec(t, "CREATE FUNCTION sc.f() RETURNS INT LANGUAGE SQL AS 'SELECT 1'")
	sqlDB.Exec(t, "BACKUP DATABASE src INTO 'nodelocal://1/reader_fallback'")
	sqlDB.Exec(t, "USE defaultdb")
	sqlDB.Exec(t, "DROP DATABASE src CASCADE")

	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_publishing_descriptors'")
	var jobID jobspb.JobID
	sqlDB.QueryRow(t,
		"RESTORE DATABASE src FROM LATEST IN 'nodelocal://1/reader_fallback' WITH detached, new_db_name = 'dst'",
	).Scan(&jobID)
	jobutils.WaitForJobToPause(t, sqlDB, jobID)

	s := tc.ApplicationLayer(0)
	idb := s.InternalDB().(isql.DB)

	// Snapshot the table-ref count via the info-key row, then delete the
	// row and confirm the read helper returns the same count via the
	// legacy slice fallback.
	var tableCount, dbCount, typeCount, schemaCount, fnCount int
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		refs, ok, err := getDescRefs(ctx, txn, jobID, restoreTableDescRefsKey)
		require.NoError(t, err)
		require.True(t, ok, "info-key row should exist before deletion")
		tableCount = len(refs)
		// Similarly capture the other types so we can assert fallback
		// parity across all five descriptor kinds.
		refs, _, err = getDescRefs(ctx, txn, jobID, restoreDatabaseDescRefsKey)
		require.NoError(t, err)
		dbCount = len(refs)
		refs, _, err = getDescRefs(ctx, txn, jobID, restoreTypeDescRefsKey)
		require.NoError(t, err)
		typeCount = len(refs)
		refs, _, err = getDescRefs(ctx, txn, jobID, restoreSchemaDescRefsKey)
		require.NoError(t, err)
		schemaCount = len(refs)
		refs, _, err = getDescRefs(ctx, txn, jobID, restoreFunctionDescRefsKey)
		require.NoError(t, err)
		fnCount = len(refs)
		return nil
	}))

	deleteInfoRow := func(t *testing.T, key string) {
		sqlDB.Exec(t,
			"DELETE FROM system.job_info WHERE job_id = $1 AND info_key = $2", jobID, key,
		)
	}
	deleteInfoRow(t, restoreTableDescRefsKey)
	deleteInfoRow(t, restoreDatabaseDescRefsKey)
	deleteInfoRow(t, restoreTypeDescRefsKey)
	deleteInfoRow(t, restoreSchemaDescRefsKey)
	deleteInfoRow(t, restoreFunctionDescRefsKey)

	// The info-key rows are gone; the helpers must now fall back to
	// the legacy slices and return identical counts.
	registry := s.JobRegistry().(*jobs.Registry)
	job, err := registry.LoadJob(ctx, jobID)
	require.NoError(t, err)
	details := job.Details().(jobspb.RestoreDetails)
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Confirm the info-key rows really are gone.
		_, ok, err := getDescRefs(ctx, txn, jobID, restoreTableDescRefsKey)
		require.NoError(t, err)
		require.False(t, ok, "info-key row should be deleted")

		refs, err := tableDescRefs(ctx, txn, jobID, details)
		require.NoError(t, err)
		require.Len(t, refs, tableCount, "tableDescRefs fallback count")

		refs, err = databaseDescRefs(ctx, txn, jobID, details)
		require.NoError(t, err)
		require.Len(t, refs, dbCount, "databaseDescRefs fallback count")

		refs, err = typeDescRefs(ctx, txn, jobID, details)
		require.NoError(t, err)
		require.Len(t, refs, typeCount, "typeDescRefs fallback count")

		refs, err = schemaDescRefs(ctx, txn, jobID, details)
		require.NoError(t, err)
		require.Len(t, refs, schemaCount, "schemaDescRefs fallback count")

		refs, err = functionDescRefs(ctx, txn, jobID, details)
		require.NoError(t, err)
		require.Len(t, refs, fnCount, "functionDescRefs fallback count")
		return nil
	}))

	// Cancel the job to keep the test cluster clean for shutdown.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	sqlDB.Exec(t, "CANCEL JOB $1", jobID)
	jobutils.WaitForJobToCancel(t, sqlDB, jobID)
}

// assertRefsCount reads the info-key row for a restore job and asserts
// the contained slice has the expected length.
func assertRefsCount(
	t *testing.T, ctx context.Context, txn isql.Txn, jobID jobspb.JobID, key string, expected int,
) {
	t.Helper()
	raw, ok, err := jobs.InfoStorageForJob(txn, jobID).Get(ctx, "test-restore-refs", key)
	require.NoError(t, err, key)
	require.True(t, ok, "%s row missing", key)
	var msg backuppb.RestoreDescRefs
	require.NoError(t, protoutil.Unmarshal(raw, &msg), key)
	require.Len(t, msg.Refs, expected, key)
	// Sanity-check the ref payload shape: every entry must carry a
	// non-zero ID and Version (descriptors are never persisted at v0).
	for i, r := range msg.Refs {
		require.NotZero(t, r.ID, "%s[%d].ID", key, i)
		require.NotZero(t, r.Version, "%s[%d].Version", key, i)
	}
}
