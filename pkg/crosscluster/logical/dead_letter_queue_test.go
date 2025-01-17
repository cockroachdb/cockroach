// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
	dbAName       = "a"
)

func setupDLQTestTables(
	ctx context.Context,
	t *testing.T,
	sqlDB *sqlutils.SQLRunner,
	kvDB *kv.DB,
	srv serverutils.TestServerInterface,
) (
	tableNameToDesc map[string]catalog.TableDescriptor,
	srcTableIDToName map[descpb.ID]dstTableMetadata,
	expectedDLQTables []string,
	ie isql.Executor,
) {
	s := srv.ApplicationLayer()
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	ie = s.InternalDB().(isql.DB).Executor(isql.WithSessionData(sd))

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

	dstTableMeta := []dstTableMetadata{
		// Base test case.
		{
			database: defaultDbName,
			schema:   publicScName,
			table:    "foo",
			tableID:  1,
		},
		// Verify that distinct DLQ tables are created for tables
		// in different databases with identical schema and table
		// names.
		{
			database: defaultDbName,
			schema:   "baz",
			table:    "foo",
			tableID:  1,
		},
		{
			database: dbAName,
			schema:   "baz",
			table:    "foo",
			tableID:  1,
		},
		// Verify that distinct DLQ tables are created for tables
		// with identical fully qualified names and distinct
		// table IDs.
		{
			database: defaultDbName,
			schema:   "bar",
			table:    "_foo",
			tableID:  1,
		},
		{
			database: defaultDbName,
			schema:   "bar_",
			table:    "foo",
			tableID:  2,
		},
	}

	tableNameToDesc = make(map[string]catalog.TableDescriptor)
	srcTableIDToName = make(map[descpb.ID]dstTableMetadata)
	expectedDLQTables = []string{}

	for _, md := range dstTableMeta {
		desc := desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), md.database, md.schema, md.table)
		srcTableID := desc.GetID()
		srcTableIDToName[srcTableID] = md
		fullyQualifiedName := fmt.Sprintf("%s.%s.%s", md.database, md.schema, md.table)
		tableNameToDesc[fullyQualifiedName] = desc
		expectedDLQTables = append(expectedDLQTables, fmt.Sprintf("dlq_%d_%s_%s", md.tableID, md.schema, md.table))
	}
	return tableNameToDesc, srcTableIDToName, expectedDLQTables, ie
}

func WaitForDLQLogs(t *testing.T, db *sqlutils.SQLRunner, tableName string, minNumRows int) {
	t.Logf("waiting for write conflicts to be logged in DLQ table %s", tableName)
	testutils.SucceedsSoon(t, func() error {
		query := fmt.Sprintf("SELECT count(*) FROM %s", tableName)
		var numRows int
		db.QueryRow(t, query).Scan(&numRows)
		if numRows < minNumRows {
			return errors.Newf("waiting for DLQ table '%s' to have %d rows, received %d rows instead",
				tableName,
				minNumRows,
				numRows)
		}
		return nil
	})
}

func TestNoopDLQClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dlqClient := InitNoopDeadLetterQueueClient()
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
			err := dlqClient.Log(ctx, tc.jobID, tc.kv, tc.cdcEventRow, tc.applyError, tc.dlqReason)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

func TestDLQCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	_, srcTableIDToName, expectedDLQTables, ie := setupDLQTestTables(ctx, t, sqlDB, kvDB, srv)

	dlqClient := InitDeadLetterQueueClient(ie, srcTableIDToName)
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

	// Verify that no custom enums were created
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SHOW ENUMS FROM %s.%s`, defaultDbName, dlqSchemaName), [][]string{})
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SHOW ENUMS FROM %s.%s`, dbAName, dlqSchemaName), [][]string{})
}

func TestDLQLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	tableNameToDesc, srcTableIDToName, _, ie := setupDLQTestTables(ctx, t, sqlDB, kvDB, srv)

	// Build family desc for cdc event row
	familyDesc := &descpb.ColumnFamilyDescriptor{
		ID:   descpb.FamilyID(1),
		Name: "",
	}

	dlqClient := InitDeadLetterQueueClient(ie, srcTableIDToName)
	require.NoError(t, dlqClient.Create(ctx))

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
		tableID      descpb.ID
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

			err := dlqClient.Log(ctx, tc.jobID, tc.kv, cdcEventRow, tc.applyError, tc.dlqReason)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)

				srcTableID := tc.tableDesc.GetID()
				md, ok := srcTableIDToName[srcTableID]
				require.True(t, ok)

				actualRow := dlqRow{}
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT
						ingestion_job_id,
						table_id,
						dlq_reason,
						mutation_type,
						key_value_bytes,
						incoming_row
				FROM %s`, md.toDLQTableName())).Scan(
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
					tableID:      md.tableID,
					dlqReason:    fmt.Sprintf("%s (%s)", tc.applyError.Error(), tc.dlqReason),
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

func TestDLQAllowList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pgErr := func(code pgcode.Code) error {
		return pgerror.New(code, "")
	}

	require.ErrorContains(t,
		canDlqError(errors.New("some unknown error")),
		"can only DLQ errors with pg codes")

	require.ErrorContains(t,
		canDlqError(pgErr(pgcode.StatementCompletionUnknown)),
		"unable to DLQ pgcode that indicates an internal or retryable error")
	require.ErrorContains(t,
		canDlqError(pgErr(pgcode.SerializationFailure)),
		"unable to DLQ pgcode that indicates an internal or retryable error")

	require.NoError(t, canDlqError(pgErr(pgcode.CheckViolation)))
}

func TestDLQJSONQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	tableID := tableDesc.GetID()
	tableName := dstTableMetadata{
		database: defaultDbName,
		schema:   publicScName,
		table:    "foo",
	}
	dlqClient := InitDeadLetterQueueClient(ie, map[descpb.ID]dstTableMetadata{
		tableID: tableName,
	})
	require.NoError(t, dlqClient.Create(ctx))

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'hello')`)
	row := popRow(t)

	kv := roachpb.KeyValue{Key: row.Key, Value: row.Value}
	updatedRow, err := decoder.DecodeKV(
		ctx, kv, cdcevent.CurrentRow, row.Timestamp(), false)

	require.NoError(t, err)
	require.NoError(t, dlqClient.Log(ctx, 1, streampb.StreamEvent_KV{KeyValue: kv}, updatedRow, errInjected, noSpace))

	dlqtableName := tableName.toDLQTableName()

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

func TestEndToEndDLQ(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("validated", func(t *testing.T) {
		testEndToEndDLQ(t, "validated")
	})

	t.Run("immediate", func(t *testing.T) {
		testEndToEndDLQ(t, "immediate")
	})
}

func testEndToEndDLQ(t *testing.T, mode string) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "SET CLUSTER SETTING logical_replication.consumer.retry_queue_duration = '100ms'")
	dbA.Exec(t, "SET CLUSTER SETTING logical_replication.consumer.retry_queue_backoff  = '1ms'")
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	type testCase struct {
		tableName         string
		supportsImmediate bool
		sql               string
		sqlA              string
		sqlB              string
		reason            string
	}
	allTests := []testCase{
		{
			tableName:         "unique_index",
			supportsImmediate: true,
			sql: `
				CREATE TABLE unique_index (
    		    key STRING PRIMARY KEY NOT NULL DEFAULT gen_random_uuid()::string,
    		    value STRING NOT NULL
				);
				CREATE UNIQUE INDEX data_value ON unique_index(value);
				INSERT INTO unique_index(value) VALUES ('this-will-conflict');`,
			reason: `duplicate key value violates unique constraint`,
		},
		{
			tableName:         "missing_foreign_key",
			supportsImmediate: false,
			sql: `
				CREATE TABLE parent (
					key STRING PRIMARY KEY NOT NULL
				);
				CREATE TABLE missing_foreign_key (
					key STRING PRIMARY KEY NOT NULL,
					foreign_key STRING REFERENCES parent (key)
				);`,
			sqlB: `
				INSERT INTO parent(key) VALUES ('parent');
				INSERT INTO missing_foreign_key (key, foreign_key) values ('will_dlq', 'parent');`,
			reason: `violates foreign key constraint`,
		},
		{
			tableName:         "constrained",
			supportsImmediate: false,
			sql: `
				CREATE TABLE constrained (
					key STRING PRIMARY KEY,
					dbname STRING NOT NULL,
					-- current_database() evaluates to NULL in the internal executor.
					-- so this check constraint will fail on replication
					CHECK(dbname = COALESCE(current_database(), 'tothedlq'))
				);`,

			sqlB:   `INSERT INTO constrained (key, dbname) VALUES ('foobar', current_database());`,
			reason: `failed to satisfy CHECK constraint`,
		},
	}
	var tests []testCase
	for _, test := range allTests {
		if mode == "immediate" && !test.supportsImmediate {
			continue
		}
		tests = append(tests, test)
	}

	for _, tc := range tests {
		if tc.sql != "" {
			dbA.Exec(t, tc.sql)
			dbB.Exec(t, tc.sql)
		}
		if tc.sqlA != "" {
			dbA.Exec(t, tc.sqlA)
		}
		if tc.sqlB != "" {
			dbB.Exec(t, tc.sqlB)
		}
	}

	var jobs []catpb.JobID
	for _, tc := range tests {
		var jobID catpb.JobID
		dbA.QueryRow(t, fmt.Sprintf(
			`CREATE LOGICAL REPLICATION STREAM FROM TABLE "%s" ON '%s' INTO TABLE "%s" WITH mode = '%s'`,
			tc.tableName, dbBURL.String(), tc.tableName, mode)).Scan(&jobID)
		jobs = append(jobs, jobID)
	}

	now := s.Clock().Now()
	for _, job := range jobs {
		WaitUntilReplicatedTime(t, now, dbA, job)
	}

	for _, tc := range tests {
		id := sqlutils.QueryTableID(t, dbA.DB, "a", "public", tc.tableName)
		dlq := fmt.Sprintf("crdb_replication.dlq_%d_public_%s", id, tc.tableName)
		dlqRows := dbA.QueryStr(t, fmt.Sprintf("SELECT dlq_reason FROM %s", dlq))
		require.Len(t, dlqRows, 1, "dlq for table '%s' is empty'", tc.tableName)
		for _, msg := range dlqRows {
			require.Contains(t, msg[0], tc.reason)
		}
	}
}
