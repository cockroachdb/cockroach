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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const defaultDbName = "defaultdb"

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
	sqlDB.Exec(t, `CREATE SCHEMA baz`)
	sqlDB.Exec(t, `CREATE TABLE baz.foo (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE baz.bar (a INT)`)

	tableNames := []fullyQualifiedTableName{
		{
			database: defaultDbName,
			schema:   "public",
			table:    "foo",
		},
		{
			database: defaultDbName,
			schema:   "baz",
			table:    "foo",
		},
		{
			database: defaultDbName,
			schema:   "baz",
			table:    "bar",
		},
	}

	tableNameToDesc := make(map[string]catalog.TableDescriptor)
	tableIDToName := make(map[int32]fullyQualifiedTableName)

	for _, name := range tableNames {
		desc := desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), name.database, name.schema, name.table)
		tableIDToName[int32(desc.GetID())] = name
		tableNameToDesc[fmt.Sprintf("%s.%s", name.schema, name.table)] = desc
	}

	// Build family desc for cdc event row
	familyDesc := &descpb.ColumnFamilyDescriptor{
		ID:   descpb.FamilyID(1),
		Name: "",
	}

	dlqClient := InitDeadLetterQueueClient(ie, tableIDToName)
	require.NoError(t, dlqClient.Create(ctx))

	type testCase struct {
		name           string
		expectedErrMsg string

		jobID        int64
		tableDesc    catalog.TableDescriptor
		kv           streampb.StreamEvent_KV
		dlqReason    retryEligibility
		mutationType ReplicationMutationType
		applyError   error
	}

	testCases := []testCase{
		{
			name:         "insert dlq fallback row for default.public.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["public.foo"],
			dlqReason:    noSpace,
			mutationType: Insert,
		},
		{
			name:         "insert dlq fallback row for default.baz.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["baz.foo"],
			dlqReason:    noSpace,
			mutationType: Insert,
		},
		{
			name:         "insert dlq fallback row for default.baz.bar",
			jobID:        1,
			tableDesc:    tableNameToDesc["baz.bar"],
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
		incomingRow  *tree.DJSON
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.applyError == nil {
				tc.applyError = errors.New("some error")
			}

			// Build cdc event row based on the expected test case output
			var cdcEventRow cdcevent.Row
			if tc.expectedErrMsg == "" {
				ed, err := cdcevent.NewEventDescriptor(tc.tableDesc, familyDesc, false, false, hlc.Timestamp{})
				require.NoError(t, err)
				cdcEventRow = cdcevent.Row{EventDescriptor: ed}
			}

			err := dlqClient.Log(ctx, tc.jobID, tc.kv, cdcEventRow, tc.dlqReason)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)

				tableID := int32(tc.tableDesc.GetID())
				name, ok := tableIDToName[tableID]
				require.True(t, ok)

				actualRow := dlqRow{}
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT
						ingestion_job_id,
						table_id,
						dlq_reason,
						mutation_type,
						key_value_bytes,
						incoming_row
				FROM %s`, fmt.Sprintf(dlqBaseTableName, defaultDbName, dlqSchemaName, name.schema, name.table))).Scan(
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
				}
				require.Equal(t, expectedRow, actualRow)
			} else {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

func TestDLQJSONQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	defer srv.Stopper().Stop(ctx)

	for _, l := range []serverutils.ApplicationLayerInterface{srv.ApplicationLayer(), srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
	CREATE TABLE foo (
		a INT, 
		b STRING, 
		rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
    CONSTRAINT foo_pkey PRIMARY KEY (rowid ASC)
	)`)
	tableDesc := cdctest.GetHydratedTableDescriptor(t, execCfg, "foo")
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:       jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
		TableID:    tableDesc.GetID(),
		FamilyName: "primary",
	})

	decoder, err := cdcevent.NewEventDecoder(ctx, &execCfg, targets, false, false)
	require.NoError(t, err)

	popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, srv.ExecutorConfig(), tableDesc)
	ie := srv.InternalDB().(isql.DB).Executor()
	defer cleanup()

	tableID := int32(tableDesc.GetID())
	dlqClient := InitDeadLetterQueueClient(ie, map[int32]fullyQualifiedTableName{
		tableID: {
			database: defaultDbName,
			schema:   "public",
			table:    "foo",
		},
	})
	require.NoError(t, dlqClient.Create(ctx))

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'hello')`)
	row := popRow(t)

	kv := roachpb.KeyValue{Key: row.Key, Value: row.Value}
	updatedRow, err := decoder.DecodeKV(
		ctx, kv, cdcevent.CurrentRow, row.Timestamp(), false)

	require.NoError(t, err)
	require.NoError(t, dlqClient.Log(ctx, 1, streampb.StreamEvent_KV{KeyValue: kv}, updatedRow, noSpace))

	dlqtableName := fmt.Sprintf(dlqBaseTableName, defaultDbName, dlqSchemaName, "public", "foo")

	var (
		a     int
		b     string
		rowID int
	)
	sqlDB.QueryRow(t, fmt.Sprintf(`SELECT incoming_row->>'a', incoming_row->>'b', incoming_row->>'rowid' FROM %s LIMIT 1`, dlqtableName)).Scan(&a, &b, &rowID)
	require.Equal(t, 1, a)
	require.Equal(t, "hello", b)
	require.NotZero(t, rowID)
}
