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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

// TestCreateAdoptableJobPopulatesJobType verifies that the job_type column in system.jobs is populated
// by CreateAdoptableJobWithTxn after upgrading to V23_1AddTypeColumnToJobsTable.
func TestCreateAdoptableJobPopulatesJobType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AddTypeColumnToJobsTable),
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

	record := jobs.Record{
		Description: "fake job",
		Username:    username.TestUserName(),
		Details:     jobspb.ImportDetails{},
		Progress:    jobspb.ImportProgress{},
	}

	j, err := s.JobRegistry().(*jobs.Registry).CreateAdoptableJobWithTxn(ctx, record, 0, nil)
	require.NoError(t, err)
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.CheckQueryResults(t, fmt.Sprintf("SELECT job_type from system.jobs WHERE id = %d", j.ID()), [][]string{{"IMPORT"}})
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
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
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
				Username: username.TestUserName(),
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
