// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var zeroDuration time.Duration

type ttlServer interface {
	JobRegistry() interface{}
}

type rowLevelTTLTestJobTestHelper struct {
	server           ttlServer
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	testCluster      serverutils.TestClusterInterface
	sqlDB            *sqlutils.SQLRunner
	kvDB             *kv.DB
	executeSchedules func() error
}

func newRowLevelTTLTestJobTestHelper(
	t *testing.T, testingKnobs *sql.TTLTestingKnobs, testMultiTenant bool, numNodes int,
) (*rowLevelTTLTestJobTestHelper, func()) {
	th := &rowLevelTTLTestJobTestHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledRowLevelTTLExecutor,
		),
	}

	baseTestingKnobs := base.TestingKnobs{
		JobsTestingKnobs: &jobs.TestingKnobs{
			JobSchedulerEnv: th.env,
			TakeOverJobsScheduling: func(fn func(ctx context.Context, maxSchedules int64) error) {
				th.executeSchedules = func() error {
					defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
					return fn(context.Background(), 0 /* allSchedules */)
				}
			},
			CaptureJobExecutionConfig: func(config *scheduledjobs.JobExecutionConfig) {
				th.cfg = config
			},
		},
		TTL: testingKnobs,
	}

	replicationMode := base.ReplicationAuto
	if numNodes > 1 {
		replicationMode = base.ReplicationManual
	}
	testCluster := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: replicationMode,
		ServerArgs: base.TestServerArgs{
			Knobs:             baseTestingKnobs,
			InsecureWebAccess: true,
		},
	})
	th.testCluster = testCluster
	ts := testCluster.Server(0)
	// As `ALTER TABLE ... SPLIT AT ...` is not supported in multi-tenancy, we
	// do not run those tests.
	if testMultiTenant {
		tenantServer, db := serverutils.StartTenant(
			t, ts, base.TestTenantArgs{
				TenantID:     serverutils.TestTenantID(),
				TestingKnobs: baseTestingKnobs,
			},
		)
		th.sqlDB = sqlutils.MakeSQLRunner(db)
		th.server = tenantServer
	} else {
		db := serverutils.OpenDBConn(
			t,
			ts.ServingSQLAddr(),
			"",    /* useDatabase */
			false, /* insecure */
			ts.Stopper(),
		)
		th.sqlDB = sqlutils.MakeSQLRunner(db)
		th.server = ts
	}
	require.NotNil(t, th.cfg)

	th.kvDB = ts.DB()

	return th, func() {
		testCluster.Stopper().Stop(context.Background())
	}
}

func (h *rowLevelTTLTestJobTestHelper) waitForScheduledJob(
	t *testing.T, expectedStatus jobs.Status, expectedErrorRe string,
) {
	h.env.SetTime(timeutil.Now().Add(time.Hour * 24))
	require.NoError(t, h.executeSchedules())

	query := fmt.Sprintf(
		`SELECT status, error FROM [SHOW JOBS] 
		WHERE job_id IN (
			SELECT id FROM %s
			WHERE created_by_id IN (SELECT schedule_id FROM %s WHERE executor_type = 'scheduled-row-level-ttl-executor')
		)`,
		h.env.SystemJobsTableName(),
		h.env.ScheduledJobsTableName(),
	)

	testutils.SucceedsSoon(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var status, errorStr string
		if err := h.sqlDB.DB.QueryRowContext(
			context.Background(),
			query,
		).Scan(&status, &errorStr); err != nil {
			return errors.Wrapf(err, "expected to scan row for a job, got")
		}

		if status != string(expectedStatus) {
			return errors.Newf("expected status %s, got %s (error: %s)", expectedStatus, status, errorStr)
		}
		if expectedErrorRe != "" {
			r, err := regexp.Compile(expectedErrorRe)
			require.NoError(t, err)
			if !r.MatchString(errorStr) {
				return errors.Newf("expected error matches %s, got %s", expectedErrorRe, errorStr)
			}
		}
		return nil
	})
}

func (h *rowLevelTTLTestJobTestHelper) verifyNonExpiredRows(
	t *testing.T, tableName string, expirationExpression string, expectedNumNonExpiredRows int,
) {
	// Check we have the number of expected rows.
	var actualNumNonExpiredRows int
	h.sqlDB.QueryRow(
		t,
		fmt.Sprintf(`SELECT count(1) FROM %s`, tableName),
	).Scan(&actualNumNonExpiredRows)
	require.Equal(t, expectedNumNonExpiredRows, actualNumNonExpiredRows)

	// Also check all the rows expire way into the future.
	h.sqlDB.QueryRow(
		t,
		fmt.Sprintf(`SELECT count(1) FROM %s WHERE %s >= now()`, tableName, expirationExpression),
	).Scan(&actualNumNonExpiredRows)
	require.Equal(t, expectedNumNonExpiredRows, actualNumNonExpiredRows)
}

// todo(ewall): migrate usages to verifyExpiredRows and switch SPLIT AT usage to SplitTable
func (h *rowLevelTTLTestJobTestHelper) verifyExpiredRowsJobOnly(
	t *testing.T, expectedNumExpiredRows int,
) {
	rows := h.sqlDB.Query(t, `
				SELECT crdb_j.status, crdb_j.progress
				FROM crdb_internal.system_jobs AS crdb_j
				WHERE crdb_j.job_type = 'ROW LEVEL TTL'
			`)
	jobCount := 0
	for rows.Next() {
		var status string
		var progressBytes []byte
		require.NoError(t, rows.Scan(&status, &progressBytes))

		require.Equal(t, string(jobs.StatusSucceeded), status)

		var progress jobspb.Progress
		require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))

		actualNumExpiredRows := progress.UnwrapDetails().(jobspb.RowLevelTTLProgress).JobRowCount
		require.Equal(t, int64(expectedNumExpiredRows), actualNumExpiredRows)
		jobCount++
	}
	require.Equal(t, 1, jobCount)
}

type processor struct {
	spanCount int64
	rowCount  int64
}

func (h *rowLevelTTLTestJobTestHelper) verifyExpiredRows(
	t *testing.T, expectedSQLInstanceIDToProcessorMap map[base.SQLInstanceID]*processor,
) {
	rows := h.sqlDB.Query(t, `
				SELECT crdb_j.status, crdb_j.progress
				FROM crdb_internal.system_jobs AS crdb_j
				WHERE crdb_j.job_type = 'ROW LEVEL TTL'
			`)
	jobCount := 0
	for rows.Next() {
		var status string
		var progressBytes []byte
		require.NoError(t, rows.Scan(&status, &progressBytes))

		require.Equal(t, string(jobs.StatusSucceeded), status)

		var progress jobspb.Progress
		require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
		rowLevelTTLProgress := progress.UnwrapDetails().(jobspb.RowLevelTTLProgress)

		processorProgresses := rowLevelTTLProgress.ProcessorProgresses
		processorIDs := make(map[int32]struct{}, len(processorProgresses))
		sqlInstanceIDs := make(map[base.SQLInstanceID]struct{}, len(processorProgresses))
		expectedJobSpanCount := int64(0)
		expectedJobRowCount := int64(0)
		for i, processorProgress := range rowLevelTTLProgress.ProcessorProgresses {
			processorID := processorProgress.ProcessorID
			require.NotContains(t, processorIDs, processorID, i)

			sqlInstanceID := processorProgress.SQLInstanceID
			require.NotContains(t, sqlInstanceIDs, sqlInstanceID, i)
			sqlInstanceIDs[sqlInstanceID] = struct{}{}

			expectedProcessor, ok := expectedSQLInstanceIDToProcessorMap[sqlInstanceID]
			require.True(t, ok, i)

			expectedProcessorSpanCount := expectedProcessor.spanCount
			require.Equal(t, expectedProcessorSpanCount, processorProgress.ProcessorSpanCount)
			expectedJobSpanCount += expectedProcessorSpanCount

			expectedProcessorRowCount := expectedProcessor.rowCount
			require.Equal(t, expectedProcessorRowCount, processorProgress.ProcessorRowCount)
			expectedJobRowCount += expectedProcessorRowCount
		}
		require.Equal(t, expectedJobSpanCount, rowLevelTTLProgress.JobSpanCount)
		require.Equal(t, expectedJobRowCount, rowLevelTTLProgress.JobRowCount)
		jobCount++
	}
	require.Equal(t, 1, jobCount)
}

func TestRowLevelTTLNoTestingKnobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		nil,  /* SQLTestingKnobs */
		true, /* testMultiTenant */
		1,    /* numNodes */
	)
	defer cleanupFunc()

	th.sqlDB.Exec(t, `CREATE TABLE t (id INT PRIMARY KEY) WITH (ttl_expire_after = '1 minute')`)
	th.sqlDB.Exec(t, `INSERT INTO t (id, crdb_internal_expiration) VALUES (1, now() - '1 month')`)

	// Force the schedule to execute.
	th.waitForScheduledJob(t, jobs.StatusFailed, `found a recent schema change on the table`)
}

// TestRowLevelTTLInterruptDuringExecution tests that row-level TTL errors
// as appropriate if there is some sort of "interrupting" request.
func TestRowLevelTTLInterruptDuringExecution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	createTable := `CREATE TABLE t (
	id INT PRIMARY KEY
) WITH (ttl_expire_after = '10 minutes');
ALTER TABLE t SPLIT AT VALUES (1), (2);
INSERT INTO t (id, crdb_internal_expiration) VALUES (1, now() - '1 month'), (2, now() - '1 month');`

	testCases := []struct {
		desc                        string
		expectedTTLError            string
		aostDuration                time.Duration
		preDeleteChangeTableVersion bool
		preSelectStatement          string
	}{
		{
			desc:             "schema change too recent to start TTL job",
			expectedTTLError: "found a recent schema change on the table at .*, aborting",
			aostDuration:     -48 * time.Hour,
		},
		{
			desc:             "schema change during job",
			expectedTTLError: "error during row deletion: table has had a schema change since the job has started at .*, aborting",
			aostDuration:     zeroDuration,
			// We cannot use a schema change to change the version in this test as
			// we overtook the job adoption method, which means schema changes get
			// blocked and may not run.
			preDeleteChangeTableVersion: true,
		},
		{
			desc:               "disable cluster setting",
			expectedTTLError:   `ttl jobs are currently disabled by CLUSTER SETTING sql.ttl.job.enabled`,
			preSelectStatement: `SET CLUSTER SETTING sql.ttl.job.enabled = false`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration:                &tc.aostDuration,
					PreDeleteChangeTableVersion: tc.preDeleteChangeTableVersion,
					PreSelectStatement:          tc.preSelectStatement,
				},
				false, /* testMultiTenant */
				1,     /* numNodes */
			)
			defer cleanupFunc()
			th.sqlDB.Exec(t, createTable)

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StatusFailed, tc.expectedTTLError)
		})
	}
}

func TestRowLevelTTLJobDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	createTable := func(addPause bool) string {
		var pauseStr string
		if addPause {
			pauseStr = `, ttl_pause = true`
		}
		return fmt.Sprintf(`CREATE TABLE t (
	id INT PRIMARY KEY
) WITH (ttl_expire_after = '10 minutes'%s);
INSERT INTO t (id, crdb_internal_expiration) VALUES (1, now() - '1 month'), (2, now() - '1 month');`, pauseStr)
	}

	testCases := []struct {
		desc             string
		expectedTTLError string
		setup            string
	}{
		{
			desc:             "disabled by cluster setting",
			expectedTTLError: "ttl jobs are currently disabled by CLUSTER SETTING sql.ttl.job.enabled",
			setup:            createTable(false) + `SET CLUSTER SETTING sql.ttl.job.enabled = false`,
		},
		{
			desc:             "disabled by TTL pause",
			expectedTTLError: "ttl jobs on table t are currently paused",
			setup:            createTable(true),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration: &zeroDuration,
				},
				true, /* testMultiTenant */
				1,    /* numNodes */
			)
			defer cleanupFunc()

			th.sqlDB.ExecMultiple(t, strings.Split(tc.setup, ";")...)

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StatusFailed, tc.expectedTTLError)

			var numRows int
			th.sqlDB.QueryRow(t, `SELECT count(1) FROM t`).Scan(&numRows)
			require.Equal(t, 2, numRows)
		})
	}
}

func TestRowLevelTTLJobMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc     string
		splitAts []int
	}{
		{
			desc:     "no split",
			splitAts: []int{},
		},
		{
			desc:     "1 split",
			splitAts: []int{10_000},
		},
		{
			desc:     "2 splits",
			splitAts: []int{10_000, 20_000},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			const numNodes = 5
			splitAts := tc.splitAts
			numRanges := len(splitAts) + 1
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration:              &zeroDuration,
					ReturnStatsError:          true,
					ExpectedNumSpanPartitions: numRanges,
				},
				false, /* testMultiTenant */ // SHOW RANGES FROM TABLE does not work with multi-tenant
				numNodes,
			)
			defer cleanupFunc()

			sqlDB := th.sqlDB

			// Create table
			tableName := "tbl"
			expirationExpr := "expire_at"
			sqlDB.Exec(t, fmt.Sprintf(
				`CREATE TABLE %s (
			id INT PRIMARY KEY,
			expire_at TIMESTAMPTZ
			) WITH (ttl_expiration_expression = '%s')`,
				tableName, expirationExpr,
			))

			// Split table
			ranges := sqlDB.QueryStr(t, fmt.Sprintf(
				`SELECT lease_holder FROM [SHOW RANGES FROM INDEX %s@primary WITH DETAILS]`,
				tableName,
			))
			require.Equal(t, 1, len(ranges))
			leaseHolderNodeIDInt, err := strconv.Atoi(ranges[0][0])
			leaseHolderNodeID := roachpb.NodeID(leaseHolderNodeIDInt)
			require.NoError(t, err)
			leaseHolderServerIdx := -1
			testCluster := th.testCluster
			for i := 0; i < testCluster.NumServers(); i++ {
				s := testCluster.Server(i)
				if s.NodeID() == leaseHolderNodeID {
					leaseHolderServerIdx = i
					break
				}
			}
			require.NotEqual(t, -1, leaseHolderServerIdx)

			const expiredRowsPerRange = 5
			const nonExpiredRowsPerRange = 5
			const rowsPerRange = expiredRowsPerRange + nonExpiredRowsPerRange
			type rangeSplit struct {
				sqlInstanceID base.SQLInstanceID
				offset        int
			}
			// points to split the range
			splitPoints := make([]serverutils.SplitPoint, len(splitAts))
			// all ranges including the original range (1 more than number of splitPoints)
			leaseHolderSQLInstanceID := base.SQLInstanceID(leaseHolderNodeID)
			rangeSplits := []rangeSplit{{
				sqlInstanceID: leaseHolderSQLInstanceID,
				offset:        0,
			}}
			for i, splitAt := range splitAts {
				newLeaseHolderServerIdx := (leaseHolderServerIdx + 1 + i) % numNodes
				splitPoints[i] = serverutils.SplitPoint{
					TargetNodeIdx: newLeaseHolderServerIdx,
					Vals:          []interface{}{splitAt},
				}
				newLeaseHolderNodeID := testCluster.Server(newLeaseHolderServerIdx).NodeID()
				rangeSplits = append(rangeSplits, rangeSplit{
					sqlInstanceID: base.SQLInstanceID(newLeaseHolderNodeID),
					offset:        splitAt,
				})
			}
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(
				th.kvDB,
				keys.SystemSQLCodec,
				"defaultdb", /* database */
				tableName,
			)
			testCluster.SplitTable(t, tableDesc, splitPoints)
			newRanges := sqlDB.QueryStr(t, fmt.Sprintf(
				`SHOW RANGES FROM INDEX %s@primary`,
				tableName,
			))
			require.Equal(t, numRanges, len(newRanges))

			// Populate table - even pk is non-expired, odd pk is expired
			expectedNumNonExpiredRows := 0
			ts := timeutil.Now()
			nonExpiredTs := ts.Add(time.Hour * 24 * 30)
			expiredTs := ts.Add(-time.Hour)
			const insertStatement = `INSERT INTO tbl VALUES ($1, $2)`
			expectedSQLInstanceIDToProcessorMap := make(map[base.SQLInstanceID]*processor, numRanges)
			for _, rangeSplit := range rangeSplits {
				offset := rangeSplit.offset
				for i := offset; i < offset+rowsPerRange; {
					sqlDB.Exec(t, insertStatement, i, nonExpiredTs)
					i++
					expectedNumNonExpiredRows++
					sqlDB.Exec(t, insertStatement, i, expiredTs)
					i++
				}
				expectedSQLInstanceID := rangeSplit.sqlInstanceID
				expectedProcessor, ok := expectedSQLInstanceIDToProcessorMap[expectedSQLInstanceID]
				if !ok {
					expectedProcessor = &processor{}
					expectedSQLInstanceIDToProcessorMap[expectedSQLInstanceID] = expectedProcessor
				}
				expectedProcessor.spanCount++
				expectedProcessor.rowCount += expiredRowsPerRange
			}

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StatusSucceeded, "")

			// Verify results
			th.verifyNonExpiredRows(t, tableName, expirationExpr, expectedNumNonExpiredRows)
			th.verifyExpiredRows(t, expectedSQLInstanceIDToProcessorMap)
		})
	}
}

// TestRowLevelTTLJobRandomEntries inserts random entries into a given table
// and runs a TTL job on them.
func TestRowLevelTTLJobRandomEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()

	var indexableTyps []*types.T
	for _, typ := range types.Scalar {
		// TODO(#76419): DateFamily has a broken `-infinity` case.
		if colinfo.ColumnTypeIsIndexable(typ) && typ.Family() != types.DateFamily {
			indexableTyps = append(indexableTyps, typ)
		}
	}

	type testCase struct {
		desc                 string
		createTable          string
		preSetup             []string
		postSetup            []string
		numExpiredRows       int
		numNonExpiredRows    int
		numSplits            int
		forceNonMultiTenant  bool
		expirationExpression string
		addRow               func(th *rowLevelTTLTestJobTestHelper, t *testing.T, createTableStmt *tree.CreateTable, ts time.Time)
	}
	// Add some basic one and three column row-level TTL tests.
	testCases := []testCase{
		{
			desc: "one column pk",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days')`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "one column pk, table ranges overlap",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days')`,
			preSetup: []string{
				`CREATE TABLE tbm (id INT PRIMARY KEY)`,
				`ALTER TABLE tbm SPLIT AT VALUES (1)`,
			},
			postSetup: []string{
				`CREATE TABLE tbl2 (id INT PRIMARY KEY)`,
				`ALTER TABLE tbl2 SPLIT AT VALUES (1)`,
			},
			numExpiredRows:      1001,
			numNonExpiredRows:   5,
			forceNonMultiTenant: true,
		},
		{
			desc: "one column pk with statistics",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days', ttl_row_stats_poll_interval = '1 minute')`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "one column pk with child labels & statistics",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days', ttl_row_stats_poll_interval = '1 minute', ttl_label_metrics = true)`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "one column pk, concurrentSchemaChange",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10)`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
			numSplits:         10,
		},
		{
			desc: "three column pk",
			createTable: `CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	other_col INT,
	"quote-kw-col" TIMESTAMPTZ,
	text TEXT,
	PRIMARY KEY (id, other_col, "quote-kw-col")
) WITH (ttl_expire_after = '30 days')`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "three column pk with rate limit",
			createTable: `CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	other_col INT,
	"quote-kw-col" TIMESTAMPTZ,
	text TEXT,
	PRIMARY KEY (id, other_col, "quote-kw-col")
) WITH (ttl_expire_after = '30 days', ttl_delete_rate_limit = 350)`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "three column pk, concurrentSchemaChange",
			createTable: `CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	other_col INT,
	"quote-kw-col" TIMESTAMPTZ,
	text TEXT,
	PRIMARY KEY (id, other_col, "quote-kw-col")
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10)`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
			numSplits:         10,
		},
		{
			desc: "three column pk, concurrentSchemaChange with index",
			createTable: `CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	other_col INT,
	"quote-kw-col" TIMESTAMPTZ,
	text TEXT,
	INDEX text_idx (text),
	PRIMARY KEY (id, other_col, "quote-kw-col")
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10)`,
			postSetup: []string{
				`ALTER INDEX tbl@text_idx SPLIT AT VALUES ('bob')`,
			},
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
			numSplits:         10,
		},
		{
			desc: "ttl expiration expression",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  expire_at TIMESTAMPTZ
) WITH (ttl_expiration_expression = 'expire_at')`,
			numExpiredRows:       1001,
			numNonExpiredRows:    5,
			expirationExpression: "expire_at",
			addRow: func(th *rowLevelTTLTestJobTestHelper, t *testing.T, _ *tree.CreateTable, ts time.Time) {
				th.sqlDB.Exec(
					t,
					"INSERT INTO tbl (expire_at) VALUES ($1)",
					ts,
				)
			},
		},
	}
	// Also randomly generate random PKs.
	for i := 0; i < 5; i++ {
		testCases = append(
			testCases,
			testCase{
				desc: fmt.Sprintf("random %d", i+1),
				createTable: fmt.Sprintf(
					`CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	rand_col_1 %s,
	rand_col_2 %s,
	text TEXT,
	PRIMARY KEY (id, rand_col_1, rand_col_2)
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = %d, ttl_delete_batch_size = %d)`,
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					1+rng.Intn(100),
					1+rng.Intn(100),
				),
				numSplits:         1 + rng.Intn(9),
				numExpiredRows:    rng.Intn(2000),
				numNonExpiredRows: rng.Intn(100),
			},
		)
	}

	defaultAddRow := func(th *rowLevelTTLTestJobTestHelper, t *testing.T, createTableStmt *tree.CreateTable, ts time.Time) {
		insertColumns := []string{"crdb_internal_expiration"}
		placeholders := []string{"$1"}
		values := []interface{}{ts}

		for _, def := range createTableStmt.Defs {
			if def, ok := def.(*tree.ColumnTableDef); ok {
				if def.HasDefaultExpr() {
					continue
				}
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(placeholders)+1))
				var b bytes.Buffer
				lexbase.EncodeRestrictedSQLIdent(&b, string(def.Name), lexbase.EncNoFlags)
				insertColumns = append(insertColumns, b.String())

				d := randgen.RandDatum(rng, def.Type.(*types.T), false /* nullOk */)
				f := tree.NewFmtCtx(tree.FmtBareStrings)
				d.Format(f)
				values = append(values, f.CloseAndGetString())
			}
		}

		th.sqlDB.Exec(
			t,
			fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s)",
				createTableStmt.Table.Table(),
				strings.Join(insertColumns, ","),
				strings.Join(placeholders, ","),
			),
			values...,
		)
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Log to make it slightly easier to reproduce a random config.
			t.Logf("test case: %#v", tc)
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration:     &zeroDuration,
					ReturnStatsError: true,
				},
				tc.numSplits == 0 && !tc.forceNonMultiTenant, // SPLIT AT does not work with multi-tenant
				1, /* numNodes */
			)
			defer cleanupFunc()

			for _, stmt := range tc.preSetup {
				t.Logf("running pre statement: %s", stmt)
				th.sqlDB.Exec(t, stmt)
			}

			th.sqlDB.Exec(t, tc.createTable)

			// Extract the columns from CREATE TABLE.
			stmt, err := parser.ParseOne(tc.createTable)
			require.NoError(t, err)
			createTableStmt, ok := stmt.AST.(*tree.CreateTable)
			require.True(t, ok)

			// Split the ranges by a random PK value.
			if tc.numSplits > 0 {
				tbDesc := desctestutils.TestingGetPublicTableDescriptor(
					th.kvDB,
					keys.SystemSQLCodec,
					"defaultdb",
					createTableStmt.Table.Table(),
				)
				require.NotNil(t, tbDesc)

				for i := 0; i < tc.numSplits; i++ {
					var values []interface{}
					var placeholders []string

					// Note we can split a PRIMARY KEY partially.
					numKeyCols := 1 + rng.Intn(tbDesc.GetPrimaryIndex().NumKeyColumns())
					for idx := 0; idx < numKeyCols; idx++ {
						col, err := catalog.MustFindColumnByID(tbDesc, tbDesc.GetPrimaryIndex().GetKeyColumnID(idx))
						require.NoError(t, err)
						placeholders = append(placeholders, fmt.Sprintf("$%d", idx+1))

						d := randgen.RandDatum(rng, col.GetType(), false)
						f := tree.NewFmtCtx(tree.FmtBareStrings)
						d.Format(f)
						values = append(values, f.CloseAndGetString())
					}
					th.sqlDB.Exec(
						t,
						fmt.Sprintf(
							"ALTER TABLE %s SPLIT AT VALUES (%s)",
							createTableStmt.Table.Table(),
							strings.Join(placeholders, ","),
						),
						values...,
					)
				}
			}

			addRow := defaultAddRow
			if tc.addRow != nil {
				addRow = tc.addRow
			}

			// Add expired and non-expired rows.

			for i := 0; i < tc.numExpiredRows; i++ {
				addRow(th, t, createTableStmt, timeutil.Now().Add(-time.Hour))
			}
			for i := 0; i < tc.numNonExpiredRows; i++ {
				addRow(th, t, createTableStmt, timeutil.Now().Add(time.Hour*24*30))
			}

			for _, stmt := range tc.postSetup {
				t.Logf("running post statement: %s", stmt)
				th.sqlDB.Exec(t, stmt)
			}

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StatusSucceeded, "")

			tableName := createTableStmt.Table.Table()
			expirationExpression := "crdb_internal_expiration"
			if tc.expirationExpression != "" {
				expirationExpression = tc.expirationExpression
			}

			th.verifyNonExpiredRows(t, tableName, expirationExpression, tc.numNonExpiredRows)

			th.verifyExpiredRowsJobOnly(t, tc.numExpiredRows)
		})
	}
}

func TestOutboundForeignKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		false, /* testMultiTenant */
		1,     /* numNodes */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, "CREATE TABLE parent (id INT PRIMARY KEY)")
	sqlDB.Exec(t, "CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ, parent_id INT REFERENCES parent (id)) WITH (ttl_expiration_expression = 'expire_at')")

	sqlDB.Exec(t, "INSERT INTO parent VALUES (1)")
	sqlDB.Exec(t, "INSERT INTO tbl VALUES (1, '2020-01-01', 1)")

	// Force the schedule to execute.
	th.waitForScheduledJob(t, jobs.StatusSucceeded, "")

	results := sqlDB.QueryStr(t, "SELECT * FROM tbl")
	require.Empty(t, results)
}
