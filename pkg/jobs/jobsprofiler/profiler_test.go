// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsprofiler_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProfilerStorePlanDiagram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE foo (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO foo VALUES (1), (2)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		sql  string
		typ  jobspb.Type
	}{
		{
			name: "backup",
			sql:  "BACKUP TABLE foo INTO 'userfile:///foo'",
			typ:  jobspb.TypeBackup,
		},
		{
			name: "restore",
			sql:  "RESTORE TABLE foo FROM LATEST IN 'userfile:///foo' WITH into_db='test'",
			typ:  jobspb.TypeRestore,
		},
		{
			name: "changefeed",
			sql:  "CREATE CHANGEFEED FOR foo INTO 'null://sink'",
			typ:  jobspb.TypeChangefeed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.Exec(tc.sql)
			require.NoError(t, err)

			var jobID jobspb.JobID
			err = sqlDB.QueryRow(
				`SELECT id FROM crdb_internal.system_jobs WHERE job_type = $1`, tc.typ.String()).Scan(&jobID)
			require.NoError(t, err)

			execCfg := s.TenantOrServer().ExecutorConfig().(sql.ExecutorConfig)
			testutils.SucceedsSoon(t, func() error {
				var count int
				err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					infoStorage := jobs.InfoStorageForJob(txn, jobID)
					return infoStorage.Iterate(ctx, "dsp-diag-url", func(infoKey string, value []byte) error {
						count++
						return nil
					})
				})
				require.NoError(t, err)
				if count != 1 {
					return errors.Newf("expected a row for the DistSQL diagram to be written but found %d", count)
				}
				return nil
			})
		})
	}
}
