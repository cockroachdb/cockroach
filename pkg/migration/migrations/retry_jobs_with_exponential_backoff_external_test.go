// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRetriesWithExponentialBackoffMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.RetryJobsWithExponentialBackoff - 1),
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Inject the old copy of the descriptor.
	err := tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		id := systemschema.JobsTable.GetID()
		proto := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: deprecatedDescriptor()}}
		b := txn.NewBatch()
		b.Put(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, id), proto)
		return txn.Run(ctx, b)
	})
	require.NoError(t, err)

	// Validate that the old descriptor is injected successfully.
	// _, err = sqlDB.Exec("SELECT last_run, num_runs FROM system.jobs")
	// require.Error(t, err, "able to read last_run and num_runs from the jobs"+
	//	 " table, which is unexpected.")

	fmt.Printf("[SR] before migration\n")
	// Run the migration.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RetryJobsWithExponentialBackoff).String())

	fmt.Printf("[SR] after migration\n")

	// Validate that the jobs table has new schema.
	tdb.CheckQueryResults(t,
		"SELECT create_statement from [SHOW CREATE system.jobs]",
		[][]string{{systemschema.JobsTableSchema}})
	const colsVerificationQuery = "SELECT num_runs, last_run from system.jobs LIMIT 0"
	const indexVerificationQuery = "SELECT num_runs, last_run, claim_instance_id " +
		"from system.jobs@jobs_run_stats_idx LIMIT 0"
	for _, query := range []string{colsVerificationQuery, indexVerificationQuery} {
		tdb.Exec(t, query)
	}

	// Make sure that jobs work by running a job.
	validateJobExec(t, tdb)
}

// validateJobExec creates and alters a dummy table to trigger jobs machinery,
// which validates its working.
func validateJobExec(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	tdb.Exec(t, "DROP TABLE foo CASCADE;")
	var jobID int64
	tdb.QueryRow(t, `
SELECT job_id
  FROM [SHOW JOBS]
 WHERE job_type = 'SCHEMA CHANGE GC' AND description LIKE '%foo%';`,
	).Scan(&jobID)
	var status jobs.Status
	tdb.QueryRow(t,
		"SELECT status FROM [SHOW JOB WHEN COMPLETE $1]", jobID,
	).Scan(&status)
	require.Equal(t, jobs.StatusSucceeded, status)
}

// deprecatedDescriptor returns the system.jobs table descriptor that was used
// before adding two new columns and an index in the current version.
func deprecatedDescriptor() *descpb.TableDescriptor {
	uniqueRowIDString := "unique_rowid()"
	nowString := "now():::TIMESTAMP"
	pk := func(name string) descpb.IndexDescriptor {
		return descpb.IndexDescriptor{
			Name:                tabledesc.PrimaryKeyIndexName,
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{name},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		}
	}

	return &descpb.TableDescriptor{
		Name:                    "jobs",
		ID:                      keys.JobsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &uniqueRowIDString},
			{Name: "status", ID: 2, Type: types.String},
			{Name: "created", ID: 3, Type: types.Timestamp, DefaultExpr: &nowString},
			{Name: "payload", ID: 4, Type: types.Bytes},
			{Name: "progress", ID: 5, Type: types.Bytes, Nullable: true},
			{Name: "created_by_type", ID: 6, Type: types.String, Nullable: true},
			{Name: "created_by_id", ID: 7, Type: types.Int, Nullable: true},
			{Name: "claim_session_id", ID: 8, Type: types.Bytes, Nullable: true},
			{Name: "claim_instance_id", ID: 9, Type: types.Int, Nullable: true},
		},
		NextColumnID: 10,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "fam_0_id_status_created_payload",
				ID:          0,
				ColumnNames: []string{"id", "status", "created", "payload", "created_by_type", "created_by_id"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 6, 7},
			},
			{
				Name:            "progress",
				ID:              1,
				ColumnNames:     []string{"progress"},
				ColumnIDs:       []descpb.ColumnID{5},
				DefaultColumnID: 5,
			},
			{
				Name:        "claim",
				ID:          2,
				ColumnNames: []string{"claim_session_id", "claim_instance_id"},
				ColumnIDs:   []descpb.ColumnID{8, 9},
			},
		},
		NextFamilyID: 3,
		PrimaryIndex: pk("id"),
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "jobs_status_created_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"status", "created"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2, 3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "jobs_created_by_type_created_by_id_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"created_by_type", "created_by_id"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				KeyColumnIDs:        []descpb.ColumnID{6, 7},
				StoreColumnIDs:      []descpb.ColumnID{2},
				StoreColumnNames:    []string{"status"},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
		},
		NextIndexID: 4,
		Privileges: descpb.NewCustomSuperuserPrivilegeDescriptor(
			descpb.SystemAllowedPrivileges[keys.JobsTableID], security.NodeUserName()),
		FormatVersion:  descpb.InterleavedFormatVersion,
		NextMutationID: 1,
	}
}
