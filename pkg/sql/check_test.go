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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		cleanup       func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, tableID descpb.ID, scheduleID int64)
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
				return fmt.Sprintf(`table id %d does not have a maps to a non-existent scheduled job id %d`, tableID, scheduleID)
			},
			cleanup: func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, tableID descpb.ID, scheduleID int64) {
				_, err := sqlDB.Exec(`DROP SCHEDULE $1`, scheduleID)
				require.NoError(t, err)
				_, err = sqlDB.Exec(`SELECT crdb_internal.repair_ttl_table_scheduled_job($1)`, tableID)
				require.NoError(t, err)
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
			require.Regexp(
				t,
				fmt.Sprintf(`use crdb_internal.repair_ttl_table_scheduled_job(%d) to repair the missing job`, tableDesc.GetID()),
				err,
			)

			tc.cleanup(t, sqlDB, kvDB, tableDesc.GetID(), scheduleID)
			_, err = sqlDB.Exec(`SELECT crdb_internal.validate_ttl_scheduled_jobs()`)
			require.NoError(t, err)
		})
	}
}
