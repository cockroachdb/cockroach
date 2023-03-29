// Copyright 2023 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBackfillJobsInfoTable tests that the `system.job_info` table is
// backfilled with the progress and payload of each job in the `system.jobs`
// table.
func TestBackfillJobsInfoTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		// Disable all automatic jobs creation and adoption.
		ServerArgs: base.TestServerArgs{
			DisableSpanConfigs: true,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V22_2),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	r := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	defer tc.Stopper().Stop(ctx)
	r.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{}
	jobspb.ForEachType(func(typ jobspb.Type) {
		// The upgrade creates migration and schemachange jobs, so we do not
		// need to create more. We should not override resumers for these job types,
		// otherwise the upgrade will hang.
		if typ != jobspb.TypeMigration && typ != jobspb.TypeSchemaChange {
			r.TestingResumerCreationKnobs[typ] = func(r jobs.Resumer) jobs.Resumer {
				return &fakeResumer{}
			}
		}
	}, false)

	createJob := func(id jobspb.JobID, details jobspb.Details, progress jobspb.ProgressDetails) *jobs.Job {
		defaultRecord := jobs.Record{
			Details:  details,
			Progress: progress,
			Username: username.TestUserName(),
		}

		job, err := r.CreateJobWithTxn(ctx, defaultRecord, id, nil /* txn */)
		require.NoError(t, err)
		return job
	}

	// Create a few different types of jobs.
	createJob(1, jobspb.BackupDetails{}, jobspb.BackupProgress{})
	createJob(2, jobspb.RestoreDetails{}, jobspb.RestoreProgress{})
	createJob(3, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{})

	upgrades.Upgrade(t, tc.ServerConn(0), clusterversion.V23_1CreateSystemJobInfoTable, nil, false)

	// Create two more jobs that we should see written to both system.jobs and
	// system.job_info.
	createJob(4, jobspb.ImportDetails{}, jobspb.ImportProgress{})
	createJob(5, jobspb.SchemaChangeDetails{}, jobspb.SchemaChangeProgress{})

	// Validate that we see 2 rows (payload and progress) in the system.job_info
	// table for each row written in the system.jobs table since the last upgrade.
	sqlDB.CheckQueryResults(t, `
SELECT count(*) FROM system.jobs AS j, system.job_info AS i
WHERE j.id = i.job_id AND (j.payload = i.value OR j.progress = i.value) AND (j.id >= 1 AND j.id <= 5)
`,
		[][]string{{"4"}})

	upgrades.Upgrade(t, tc.ServerConn(0), clusterversion.V23_1JobInfoTableIsBackfilled, nil, false)

	// At this point the entire system.jobs table should be backfilled into the
	// system.job_info table.
	// We expect to see 14 rows because of:
	// - 4 rows from before
	// - 10 rows (payload + progress) for the 5 jobs in system.jobs
	sqlDB.CheckQueryResults(t, `
SELECT count(*) FROM system.jobs AS j, system.job_info AS i
WHERE j.id = i.job_id AND (j.payload = i.value OR j.progress = i.value) AND (j.id >= 1 AND j.id <= 5)
`,
		[][]string{{"14"}})
}
