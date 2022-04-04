// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestValidateTTLScheduledJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		desc          string
		setup         func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64)
		expectedErrRe func(tableID descpb.ID, scheduleID int64) string
	}{
		{
			desc: "not pointing at a valid scheduled job",
			setup: func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64) {
				tableDesc.RowLevelTTL.ScheduleID = 0
				require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
					return col.WriteDesc(ctx, false /* kvBatch */, tableDesc, txn)
				}))
			},
			expectedErrRe: func(tableID descpb.ID, scheduleID int64) string {
				return fmt.Sprintf(`table id %d maps to a non-existent schedule id 0`, tableID)
			},
		},
		{
			desc: "scheduled job points at an different table",
			setup: func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64) {
				ie := s.InternalExecutor().(sqlutil.InternalExecutor)
				require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					sj, err := jobs.LoadScheduledJob(
						ctx,
						jobstest.NewJobSchedulerTestEnv(
							jobstest.UseSystemTables,
							timeutil.Now(),
							tree.ScheduledBackupExecutor,
						),
						scheduleID,
						ie,
						txn,
					)
					if err != nil {
						return err
					}
					var args catpb.ScheduledRowLevelTTLArgs
					if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, &args); err != nil {
						return err
					}
					args.TableID = 0
					any, err := pbtypes.MarshalAny(&args)
					if err != nil {
						return err
					}
					sj.SetExecutionDetails(sj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
					return sj.Update(ctx, ie, txn)
				}))
			},
			expectedErrRe: func(tableID descpb.ID, scheduleID int64) string {
				return fmt.Sprintf(`schedule id %d points to table id 0 instead of table id %d`, scheduleID, tableID)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			_, err := sqlDB.Exec(`CREATE TABLE t () WITH (ttl_expire_after = '10 mins')`)
			require.NoError(t, err)

			tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "defaultdb", "t")
			require.NotNil(t, tableDesc.GetRowLevelTTL())
			scheduleID := tableDesc.GetRowLevelTTL().ScheduleID

			tc.setup(t, sqlDB, kvDB, s, tableDesc, scheduleID)

			_, err = sqlDB.Exec(`SELECT crdb_internal.validate_ttl_scheduled_jobs()`)
			require.Error(t, err)
			require.Regexp(t, tc.expectedErrRe(tableDesc.GetID(), scheduleID), err)
			var pgxErr *pq.Error
			require.True(t, errors.As(err, &pgxErr))
			require.Regexp(
				t,
				fmt.Sprintf(`use crdb_internal.repair_ttl_table_scheduled_job\(%d\) to repair the missing job`, tableDesc.GetID()),
				pgxErr.Hint,
			)

			// Repair and check jobs are valid.
			_, err = sqlDB.Exec(`DROP SCHEDULE $1`, scheduleID)
			require.NoError(t, err)
			_, err = sqlDB.Exec(`SELECT crdb_internal.repair_ttl_table_scheduled_job($1)`, tableDesc.GetID())
			require.NoError(t, err)
			_, err = sqlDB.Exec(`SELECT crdb_internal.validate_ttl_scheduled_jobs()`)
			require.NoError(t, err)
		})
	}
}
