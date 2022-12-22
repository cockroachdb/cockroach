// Copyright 2022 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ jobs.Resumer = &fakeResumer{}

type fakeResumer struct {
}

// Resume implements the jobs.Resumer interface.
func (*fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return nil
}

// Resume implements the jobs.Resumer interface.
func (*fakeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return jobErr
}

// TestAlterSystemJobsTableAddJobTypeColumn verifies that the migrations that add & backfill
// the type column to system.jobs succeed.
func TestAlterSystemJobsTableAddJobTypeColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AddTypeColumnToJobsTable - 1),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationSchemas = []upgrades.Schema{
			{Name: "job_type", ValidationFn: upgrades.HasColumn},
			{Name: "fam_0_id_status_created_payload", ValidationFn: upgrades.HasColumnFamily},
			{Name: "jobs_job_type_idx", ValidationFn: upgrades.HasIndex},
		}
	)

	// Inject the old copy of the descriptor and validate that the schema matches the old version.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.JobsTable, getJobsTableDescriptorPriorToV23_1AddTypeColumnToJobsTable)
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.JobsTableID,
		systemschema.JobsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)

	// Start a job of each type.
	registry := s.JobRegistry().(*jobs.Registry)
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{}
	jobspb.ForEachType(func(typ jobspb.Type) {
		// The upgrade creates migration and schemachange jobs, so we do not
		// need to create more. We should not override resumers for these job types,
		// otherwise the upgrade will hang.
		if typ != jobspb.TypeMigration && typ != jobspb.TypeSchemaChange {
			registry.TestingResumerCreationKnobs[typ] = func(r jobs.Resumer) jobs.Resumer {
				return &fakeResumer{}
			}

			record := jobs.Record{
				Details: jobspb.JobDetailsForEveryJobType[typ],
				// We must pass a progress struct to be able to create a job. Since we
				// override the resumer for each type to be a fake resumer, the type of
				// progess we pass in does not matter.
				Progress: jobspb.ImportProgress{},
			}

			_, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
			require.NoError(t, err)
		}
	}, false)

	// Run the upgrade which adds the job_type column.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AddTypeColumnToJobsTable,
		nil,   /* done */
		false, /* expectError */
	)

	// Check that the type column exists but has not been populated yet.
	var count int
	row := sqlDB.QueryRow("SELECT count(*) FROM system.jobs WHERE job_type IS NOT NULL")
	err := row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, count, 0)

	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.JobsTableID,
		systemschema.JobsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)

	// Run the upgrade which backfills the job_type column.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1BackfillTypeColumnInJobsTable,
		nil,   /* done */
		false, /* expectError */
	)

	// Assert that we backfill the job_type column correctly for each type of job.
	row = sqlDB.QueryRow("SELECT count(*) FROM system.jobs WHERE job_type IS NULL")
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, count, 0)

	var typStr string
	rows, err := sqlDB.Query("SELECT distinct(job_type) FROM system.jobs")
	require.NoError(t, err)
	var seenTypes intsets.Fast
	for rows.Next() {
		err = rows.Scan(&typStr)
		require.NoError(t, err)
		seenTypes.Add(int(jobspb.Type_value[strings.ReplaceAll(typStr, " ", "_")]))
	}
	jobspb.ForEachType(func(typ jobspb.Type) {
		assert.True(t, seenTypes.Contains(int(typ)))
	}, false)
}

func getJobsTableDescriptorPriorToV23_1AddTypeColumnToJobsTable() *descpb.TableDescriptor {
	defaultID := "unique_rowid()"
	defaultCreated := "now():::TIMESTAMP"
	return &descpb.TableDescriptor{
		Name:                    string(catconstants.JobsTableName),
		ID:                      keys.JobsTableID,
		ParentID:                keys.SystemDatabaseID,
		UnexposedParentSchemaID: keys.PublicSchemaID,
		Version:                 1,
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, DefaultExpr: &defaultID},
			{Name: "status", ID: 2, Type: types.String},
			{Name: "created", ID: 3, Type: types.Timestamp, DefaultExpr: &defaultCreated},
			{Name: "payload", ID: 4, Type: types.Bytes},
			{Name: "progress", ID: 5, Type: types.Bytes},
			{Name: "created_by_type", ID: 6, Type: types.String, Nullable: true},
			{Name: "created_by_id", ID: 7, Type: types.Int, Nullable: true},
			{Name: "claim_session_id", ID: 8, Type: types.Bytes, Nullable: true},
			{Name: "claim_instance_id", ID: 9, Type: types.Int, Nullable: true},
			{Name: "num_runs", ID: 10, Type: types.Int, Nullable: true},
			{Name: "last_run", ID: 11, Type: types.Timestamp, Nullable: true},
		},
		NextColumnID: 12,
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
				ColumnNames: []string{"claim_session_id", "claim_instance_id", "num_runs", "last_run"},
				ColumnIDs:   []descpb.ColumnID{8, 9, 10, 11},
			},
		},
		NextFamilyID: 3,
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "id",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "jobs_status_created_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"status", "created"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2, 3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "jobs_created_by_type_created_by_id_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"created_by_type", "created_by_id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{6, 7},
				StoreColumnIDs:      []descpb.ColumnID{2},
				StoreColumnNames:    []string{"status"},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "jobs_run_stats_idx",
				ID:                  4,
				Unique:              false,
				KeyColumnNames:      []string{"claim_session_id", "status", "created"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{8, 2, 3},
				StoreColumnNames:    []string{"last_run", "num_runs", "claim_instance_id"},
				StoreColumnIDs:      []descpb.ColumnID{11, 10, 9},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
				Predicate:           systemschema.JobsRunStatsIdxPredicate,
			},
		},
		NextIndexID:    5,
		Privileges:     catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.ReadWriteData, username.NodeUserName()),
		NextMutationID: 1,
		FormatVersion:  3,
	}
}
