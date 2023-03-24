// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestJobInfoUpgradeRegressionTests is a regression test where a job that is
// created before V23_1JobInfoTableIsBackfilled and continues to run during the
// V23_1JobInfoTableIsBackfilled upgrade will have duplicate payload and
// progress rows in the job_info table. Prior to the fix this caused the
// InfoStorage read path to error out on seeing more than one row per jobID,
// info_key.
func TestJobInfoUpgradeRegressionTests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey),
				BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`SET CLUSTER SETTING version = $1`, clusterversion.V23_1CreateSystemJobInfoTable.String())
	require.NoError(t, err)

	_, err = sqlDB.Exec(`SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.after.write_lock'`)
	require.NoError(t, err)

	var jobID jobspb.JobID
	require.NoError(t, sqlDB.QueryRow(`BACKUP INTO 'userfile:///foo' WITH detached`).Scan(&jobID))
	runner := sqlutils.MakeSQLRunner(sqlDB)
	jobutils.WaitForJobToPause(t, runner, jobID)

	runner.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM system.job_info WHERE job_id = %d`, jobID),
		[][]string{{"2"}})

	_, err = sqlDB.Exec(`SET CLUSTER SETTING version = $1`, clusterversion.V23_1JobInfoTableIsBackfilled.String())
	require.NoError(t, err)

	runner.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM system.job_info WHERE job_id = %d`, jobID),
		[][]string{{"4"}})

	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		_, _, err := infoStorage.Get(ctx, jobs.GetLegacyPayloadKey())
		return err
	})
	require.NoError(t, err)
}
