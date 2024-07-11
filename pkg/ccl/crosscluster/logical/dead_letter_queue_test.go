// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestLoggingDLQClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT)`)

	tableName := "foo"
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "defaultdb", tableName)
	familyDesc := &descpb.ColumnFamilyDescriptor{
		ID:   descpb.FamilyID(1),
		Name: "",
	}

	ed, err := cdcevent.NewEventDescriptor(tableDesc, familyDesc, false, false, hlc.Timestamp{})
	require.NoError(t, err)

	dlqClient := InitLoggingDeadLetterQueueClient()
	require.NoError(t, dlqClient.Create(ctx))

	type testCase struct {
		name           string
		expectedErrMsg string

		jobID       int64
		kv          streampb.StreamEvent_KV
		cdcEventRow cdcevent.Row
		applyError  error
		dlqReason   retryEligibility
	}

	testCases := []testCase{
		{
			name:        "log conflict for query",
			cdcEventRow: cdcevent.Row{EventDescriptor: ed},
		},
		{
			name:           "expect error when given nil cdcEventRow",
			expectedErrMsg: "cdc event row not initialized",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.applyError == nil {
				tc.applyError = errors.New("some error")
			}
			err := dlqClient.Log(ctx, tc.jobID, tc.kv, tc.cdcEventRow, tc.dlqReason)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

func TestDLQClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	ie := s.InternalDB().(isql.DB).Executor(isql.WithSessionData(sd))

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT)`)

	defaultDbName := "defaultdb"
	publicScName := "public"
	tableNames := []string{"foo", "bar"}

	familyDesc := &descpb.ColumnFamilyDescriptor{
		ID:   descpb.FamilyID(1),
		Name: "",
	}

	tableIDToPrefix := make(map[int32]string)
	tableNameToID := make(map[string]int32)
	tableToCdcEvent := make(map[string]cdcevent.Row)

	for _, tableName := range tableNames {
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), defaultDbName, tableName)
		ed, err := cdcevent.NewEventDescriptor(tableDesc, familyDesc, false, false, hlc.Timestamp{})
		require.NoError(t, err)

		tableToCdcEvent[tableName] = cdcevent.Row{EventDescriptor: ed}
		tableNameToID[tableName] = int32(tableDesc.GetID())
		tableIDToPrefix[int32(tableDesc.GetID())] = fmt.Sprintf("%s.%s", defaultDbName, publicScName)
	}

	dlqClient := InitDeadLetterQueueClient(ie, tableIDToPrefix)
	require.NoError(t, dlqClient.Create(ctx))

	type testCase struct {
		name           string
		expectedErrMsg string

		jobID        int64
		tableName    string
		kv           streampb.StreamEvent_KV
		dlqReason    retryEligibility
		mutationType ReplicationMutationType
		applyError   error
	}

	testCases := []testCase{
		{
			name:         "insert row into dlq table foo",
			jobID:        0,
			tableName:    "foo",
			dlqReason:    noSpace,
			mutationType: Insert,
		},
		{
			name:         "insert row into dlq table bar",
			jobID:        0,
			tableName:    "bar",
			dlqReason:    tooOld,
			mutationType: Insert,
		},
		{
			name:           "expect error when given nil cdcEventRow",
			expectedErrMsg: "cdc event row not initialized",
		},
	}

	type dlqRow struct {
		jobID        int64
		tableID      int32
		dlqReason    string
		mutationType string
		kv           []byte
		incomingRow  string
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.applyError == nil {
				tc.applyError = errors.New("some error")
			}

			cdcEventRow := tableToCdcEvent[tc.tableName]

			err := dlqClient.Log(ctx, tc.jobID, tc.kv, cdcEventRow, tc.dlqReason)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)

				tableID, ok := tableNameToID[tc.tableName]
				require.True(t, ok)
				tablePrefix, ok := tableIDToPrefix[tableID]
				require.True(t, ok)

				actualRow := dlqRow{}
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT
						ingestion_job_id,
						table_id,
						dlq_reason,
						mutation_type,
						key_value_bytes,
						incoming_row
				FROM %s`, fmt.Sprintf(dlqBaseTableName, tablePrefix, tableID))).Scan(
					&actualRow.jobID,
					&actualRow.tableID,
					&actualRow.dlqReason,
					&actualRow.mutationType,
					&actualRow.kv,
					&actualRow.incomingRow,
				)

				bytes, err := protoutil.Marshal(&tc.kv)
				require.NoError(t, err)

				expectedRow := dlqRow{
					jobID:        tc.jobID,
					tableID:      tableID,
					dlqReason:    tc.dlqReason.String(),
					mutationType: tc.mutationType.String(),
					kv:           bytes,
					incomingRow:  cdcEventRow.DebugString(),
				}
				require.Equal(t, expectedRow, actualRow)
			} else {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}
