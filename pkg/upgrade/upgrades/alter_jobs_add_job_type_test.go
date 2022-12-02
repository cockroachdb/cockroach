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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct{}

func (r *fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return nil
}

func (r *fakeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return nil
}

// attempt to create details for each job type,
// exclude
//	jobspb.TypeUnspecified - there are no job details which yield this time.

var jobDetails = []jobspb.Details{
	jobspb.BackupDetails{},
	jobspb.RestoreDetails{},
	jobspb.SchemaChangeDetails{},
	jobspb.ImportDetails{},
	jobspb.ChangefeedDetails{},
	jobspb.CreateStatsDetails{},
	jobspb.CreateStatsDetails{
		Name: jobspb.AutoStatsName,
	}, // type autocreate stats createStatsName := d.CreateStats.Name
	jobspb.SchemaChangeGCDetails{},
	jobspb.TypeSchemaChangeDetails{},
	jobspb.StreamIngestionDetails{},
	jobspb.NewSchemaChangeDetails{},
	jobspb.MigrationDetails{},
	jobspb.AutoSpanConfigReconciliationDetails{},
	jobspb.AutoSQLStatsCompactionDetails{},
	jobspb.StreamReplicationDetails{},
	jobspb.RowLevelTTLDetails{},
	jobspb.SchemaTelemetryDetails{},
}

// TestAlterSystemJobsTableAddJobTypeColumn verifies that the migration to add the type column to the system.jobs
// table succeeds. This test creates jobs of each type before performing the migration and ensures that
// column values are backfilled correctly by the migration.
func TestAlterSystemJobsTableAddJobTypeColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	//Job type 0 is reserved for the unspecified job type, which is
	//unused.
	//for typ := 1; typ < jobspb.NumJobTypes; typ++ {
	//	jobs.RegisterConstructor(jobspb.Type(typ), func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
	//		return &fakeResumer{}
	//	}, jobs.UsesTenantCostControl)
	//}

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
			{Name: "type", ValidationFn: upgrades.HasColumn},
			{Name: "fam_0_id_status_created_payload", ValidationFn: upgrades.HasColumnFamily},
		}
	)

	// Inject the old copy of the descriptor.
	upgrades.InjectLegacyTable(ctx, t, s, systemschema.JobsTable, getDeprecatedJobsTableDescriptor)
	// Validate that the table sql_instances has the old schema.
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
	createJob := func(record jobs.Record) {
		_, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
	}

	// Job type 0 is reserved for the unspecified job type, which is
	// unused.
	for typ := 1; typ < jobspb.NumJobTypes; typ++ {
		createJob(jobs.Record{
			Details:  jobDetails[typ-1],
			Progress: jobspb.ImportProgress{},
		})
	}

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AddTypeColumnToJobsTable,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
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

	var typeStr string
	rows, err := sqlDB.Query("SELECT type FROM system.jobs")
	require.NoError(t, err)

	keys := map[string]struct{}{}
	for k := range jobspb.Type_value {
		keys[strings.ReplaceAll(k, "_", " ")] = struct{}{}
	}

	for rows.Next() {
		err = rows.Scan(&typeStr)
		if _, ok := keys[typeStr]; ok {
			delete(keys, typeStr)
		}
	}
	assert.True(t, len(keys) == 1)
	assert.Contains(t, keys, "UNSPECIFIED")

	var count int
	row := sqlDB.QueryRow("SELECT count(*) FROM system.jobs WHERE type IS NULL")
	row.Scan(&count)
	assert.Equal(t, count, 0)
}

func getDeprecatedJobsTableDescriptor() *descpb.TableDescriptor {
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
			KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC},
			KeyColumnIDs:        []descpb.ColumnID{1},
		},
		Indexes: []descpb.IndexDescriptor{
			{
				Name:                "jobs_status_created_idx",
				ID:                  2,
				Unique:              false,
				KeyColumnNames:      []string{"status", "created"},
				KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
				KeyColumnIDs:        []descpb.ColumnID{2, 3},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
			},
			{
				Name:                "jobs_created_by_type_created_by_id_idx",
				ID:                  3,
				Unique:              false,
				KeyColumnNames:      []string{"created_by_type", "created_by_id"},
				KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
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
				KeyColumnDirections: []catpb.IndexColumn_Direction{catpb.IndexColumn_ASC, catpb.IndexColumn_ASC, catpb.IndexColumn_ASC},
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
