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
	"slices"
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

const (
	defaultDbName = "defaultdb"
	publicScName  = "public"
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
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), defaultDbName, tableName)
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
	sqlDB.Exec(t, `CREATE SCHEMA bar_`)
	sqlDB.Exec(t, `CREATE TABLE bar_.foo (a INT)`)
	sqlDB.Exec(t, `CREATE SCHEMA bar`)
	sqlDB.Exec(t, `CREATE TABLE bar._foo (a INT)`)

	sqlDB.Exec(t, `CREATE DATABASE a`)
	sqlDB.Exec(t, `CREATE SCHEMA a.baz`)
	sqlDB.Exec(t, `CREATE TABLE a.public.bar (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE a.baz.foo (a INT)`)

	dbAName := "a"

	tableNames := []fullyQualifiedTableName{
		{
			database: defaultDbName,
			schema:   publicScName,
			table:    "foo",
		},
		{
			database: defaultDbName,
			schema:   "baz",
			table:    "foo",
		},
		{
			database: defaultDbName,
			schema:   "bar",
			table:    "_foo",
		},
		{
			database: defaultDbName,
			schema:   "bar_",
			table:    "foo",
		},
		{
			database: dbAName,
			schema:   publicScName,
			table:    "bar",
		},
		{
			database: dbAName,
			schema:   "baz",
			table:    "foo",
		},
	}

	tableNameToDesc := make(map[string]catalog.TableDescriptor)
	tableIDToName := make(map[int32]fullyQualifiedTableName)
	var expectedDLQTables []string

	for _, name := range tableNames {
		desc := desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), name.database, name.schema, name.table)
		tableID := int32(desc.GetID())
		tableIDToName[tableID] = name
		fullyQualifiedName := fmt.Sprintf("%s.%s.%s", name.database, name.schema, name.table)
		tableNameToDesc[fullyQualifiedName] = desc
		expectedDLQTables = append(expectedDLQTables, fmt.Sprintf("dlq_%d_%s_%s", tableID, name.schema, name.table))
	}

	// Build family desc for cdc event row
	familyDesc := &descpb.ColumnFamilyDescriptor{
		ID:   descpb.FamilyID(1),
		Name: "",
	}

	dlqClient := InitDeadLetterQueueClient(ie, tableIDToName)
	require.NoError(t, dlqClient.Create(ctx))

	// Verify DLQ tables are created with their expected names
	dlqTableQueryResult := sqlDB.QueryStr(t,
		fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES FROM %s.%s]`, defaultDbName, dlqSchemaName))
	dlqTableQueryResult = append(dlqTableQueryResult, sqlDB.QueryStr(t,
		fmt.Sprintf(`SELECT table_name FROM [SHOW TABLES FROM %s.%s]`, dbAName, dlqSchemaName))...)

	var actualDQLTables []string
	for _, row := range dlqTableQueryResult {
		actualDQLTables = append(actualDQLTables, row...)
	}

	slices.Sort(expectedDLQTables)
	slices.Sort(actualDQLTables)
	require.Equal(t, expectedDLQTables, actualDQLTables)

	// Verify enum creation
	enumRow := [][]string{
		{dlqSchemaName, "mutation_type", "{insert,update,delete}"},
	}
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SELECT schema, name, values FROM [SHOW ENUMS FROM %s.%s]`, defaultDbName, dlqSchemaName), enumRow)
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SELECT schema, name, values FROM [SHOW ENUMS FROM %s.%s]`, dbAName, dlqSchemaName), enumRow)

	type testCase struct {
		name           string
		expectedErrMsg string

		jobID        int64
		tableDesc    catalog.TableDescriptor
		kv           streampb.StreamEvent_KV
		dlqReason    retryEligibility
		mutationType replicationMutationType
		applyError   error
	}

	testCases := []testCase{
		{
			name:         "insert dlq fallback row for default.public.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["defaultdb.public.foo"],
			dlqReason:    noSpace,
			mutationType: insertMutation,
		},
		{
			name:         "insert dlq fallback row for default.baz.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["defaultdb.baz.foo"],
			dlqReason:    tooOld,
			mutationType: insertMutation,
		},
		{
			name:         "insert dlq fallback row for default.bar._foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["defaultdb.bar._foo"],
			dlqReason:    tooOld,
			mutationType: insertMutation,
		},
		{
			name:         "insert dlq fallback row for default.bar_.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["defaultdb.bar_.foo"],
			dlqReason:    noSpace,
			mutationType: insertMutation,
		},
		{
			name:         "insert dlq fallback row for a.public.bar",
			jobID:        1,
			tableDesc:    tableNameToDesc["a.public.bar"],
			dlqReason:    noSpace,
			mutationType: insertMutation,
		},
		{
			name:         "insert dlq fallback row for a.baz.foo",
			jobID:        1,
			tableDesc:    tableNameToDesc["a.baz.foo"],
			dlqReason:    tooOld,
			mutationType: insertMutation,
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
				FROM %s`, name.toDLQTableName(tableID))).Scan(
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
	tableName := fullyQualifiedTableName{
		database: defaultDbName,
		schema:   publicScName,
		table:    "foo",
	}
	dlqClient := InitDeadLetterQueueClient(ie, map[int32]fullyQualifiedTableName{
		tableID: tableName,
	})
	require.NoError(t, dlqClient.Create(ctx))

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'hello')`)
	row := popRow(t)

	kv := roachpb.KeyValue{Key: row.Key, Value: row.Value}
	updatedRow, err := decoder.DecodeKV(
		ctx, kv, cdcevent.CurrentRow, row.Timestamp(), false)

	require.NoError(t, err)
	require.NoError(t, dlqClient.Log(ctx, 1, streampb.StreamEvent_KV{KeyValue: kv}, updatedRow, noSpace))

	dlqtableName := tableName.toDLQTableName(tableID)

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
