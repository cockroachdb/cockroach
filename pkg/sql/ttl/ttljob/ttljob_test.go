// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

type rowLevelTTLTestJobTestHelper struct {
	server           serverutils.ApplicationLayerInterface
	env              *jobstest.JobSchedulerTestEnv
	testCluster      serverutils.TestClusterInterface
	sqlDB            *sqlutils.SQLRunner
	kvDB             *kv.DB
	executeSchedules func() error
}

func newRowLevelTTLTestJobTestHelper(
	t *testing.T, testingKnobs *sql.TTLTestingKnobs, numNodes int, runMinActiveVersion bool,
) (*rowLevelTTLTestJobTestHelper, func()) {
	th := &rowLevelTTLTestJobTestHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledRowLevelTTLExecutor,
		),
	}

	v := clusterversion.Latest
	if runMinActiveVersion {
		v = clusterversion.MinSupported
	}
	makeSettings := func() *cluster.Settings {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.Latest.Version(),
			v.Version(),
			false, /* initializeVersion */
		)
		return st
	}

	jobsInterval := 5 * time.Second
	if skip.Duress() {
		jobsInterval = 30 * time.Second
	}
	requestFilter, _ := testutils.TestingRequestFilterRetryTxnWithPrefix(t, "ttljob-", 1)
	baseTestingKnobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			ClusterVersionOverride:         v.Version(),
			DisableAutomaticVersionUpgrade: make(chan struct{}),
		},
		SQLEvalContext: &eval.TestingKnobs{
			TenantLogicalVersionKeyOverride: v,
		},
		Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: requestFilter,
		},
		JobsTestingKnobs: &jobs.TestingKnobs{
			JobSchedulerEnv: th.env,
			IntervalOverrides: jobs.TestingIntervalOverrides{
				Adopt:  &jobsInterval,
				Cancel: &jobsInterval,
			},
			TakeOverJobsScheduling: func(fn func(ctx context.Context, maxSchedules int64) error) {
				th.executeSchedules = func() error {
					th.env.SetTime(timeutil.Now().Add(time.Hour * 24))
					defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
					return fn(context.Background(), 0 /* allSchedules */)
				}
			},
		},
		TTL: testingKnobs,
	}

	replicationMode := base.ReplicationAuto
	if numNodes > 1 {
		replicationMode = base.ReplicationManual
	}

	testCluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: replicationMode,
		ServerArgs: base.TestServerArgs{
			Settings:          makeSettings(),
			Knobs:             baseTestingKnobs,
			InsecureWebAccess: true,
		},
	})
	th.testCluster = testCluster
	th.server = testCluster.ApplicationLayer(0)
	th.sqlDB = sqlutils.MakeSQLRunner(th.server.SQLConn(t))
	th.kvDB = testCluster.Server(0).DB()

	return th, func() {
		testCluster.Stopper().Stop(context.Background())
	}
}

func (h *rowLevelTTLTestJobTestHelper) waitForScheduledJob(
	t *testing.T, expectedStatus jobs.State, expectedErrorRe string,
) {
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

	var regex *regexp.Regexp
	if expectedErrorRe != "" {
		var err error
		regex, err = regexp.Compile(expectedErrorRe)
		require.NoError(t, err)
	}

	var rows [][]string
	testutils.SucceedsWithin(t, func() error {
		// Force newly created job to be adopted and verify it succeeds.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		rows = h.sqlDB.QueryStr(t, query)
		if len(rows) == 0 {
			return errors.New("no job rows found yet")
		}
		for _, row := range rows {
			status := row[0]
			switch jobs.State(status) {
			case jobs.StatePending:
				return errors.New("job is pending")
			case jobs.StateRunning:
				return errors.New("job still running")
			case jobs.StateReverting:
				return errors.New("job is reverting")
			}
		}
		return nil // job is done
	}, 3*time.Minute)

	// At this point, job has completed (not running). Assert correctness.
	var actualStatuses []string
	var actualErrors []string
	matched := false
	for _, row := range rows {
		status := row[0]
		errStr := row[1]
		actualStatuses = append(actualStatuses, status)
		actualErrors = append(actualErrors, errStr)
		if status == string(expectedStatus) && (regex == nil || regex.MatchString(errStr)) {
			matched = true
			break
		}
	}

	require.True(t, matched, `
expectedStatus="%s"
actualStatuses="%s"
 expectedError="%s"
  actualErrors="%s"`,
		expectedStatus, strings.Join(actualStatuses, `", "`), expectedErrorRe, strings.Join(actualErrors, `", "`))
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

		require.Equal(t, string(jobs.StateSucceeded), status)

		var progress jobspb.Progress
		require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))

		actualNumExpiredRows := progress.UnwrapDetails().(jobspb.RowLevelTTLProgress).JobDeletedRowCount
		require.Equal(t, int64(expectedNumExpiredRows), actualNumExpiredRows)

		// Verify job reached 100% completion
		var fractionComplete float32
		if f, ok := progress.Progress.(*jobspb.Progress_FractionCompleted); ok {
			fractionComplete = f.FractionCompleted
		}
		require.InEpsilon(t, float32(1.0), fractionComplete, 0.001,
			"expected job to reach 100%% completion, got %.3f", fractionComplete)
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
				SELECT crdb_j.status, crdb_j.progress, crdb_j.id
				FROM crdb_internal.system_jobs AS crdb_j
				WHERE crdb_j.job_type = 'ROW LEVEL TTL'
			`)
	jobCount := 0
	for rows.Next() {
		var status string
		var progressBytes []byte
		var jobID jobspb.JobID
		require.NoError(t, rows.Scan(&status, &progressBytes, &jobID))

		require.Equal(t, string(jobs.StateSucceeded), status)

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
			require.Equal(t, expectedProcessorSpanCount, processorProgress.ProcessedSpanCount)
			expectedJobSpanCount += expectedProcessorSpanCount

			expectedProcessorRowCount := expectedProcessor.rowCount
			require.Equal(t, expectedProcessorRowCount, processorProgress.DeletedRowCount)
			expectedJobRowCount += expectedProcessorRowCount
		}
		// Check span completion based on mode
		if rowLevelTTLProgress.UseCheckpointing {
			// In checkpointing mode, verify that the completed spans cover the entire table.
			// Load completed spans from jobfrontier storage.
			if expectedJobSpanCount > 0 {
				ctx := context.Background()
				internalDB := h.server.InternalDB().(isql.DB)

				var completedSpans []roachpb.Span
				err := internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					resolvedSpans, found, err := jobfrontier.GetResolvedSpans(ctx, txn, jobID, "ttl_completed_spans")
					if err != nil {
						return err
					}
					if !found {
						return nil // No completed spans stored yet
					}

					// Extract just the spans (we don't care about timestamps for TTL)
					completedSpans = make([]roachpb.Span, len(resolvedSpans))
					for i, rs := range resolvedSpans {
						completedSpans[i] = rs.Span
					}
					return nil
				})
				require.NoError(t, err)
				require.Greater(t, len(completedSpans), 0, "expected completed spans to be stored in jobfrontier, but found none")

				// Get the table's span to compare against
				tableDesc := desctestutils.TestingGetPublicTableDescriptor(
					h.kvDB,
					h.server.Codec(),
					"defaultdb",
					"tbl",
				)
				tableSpan := tableDesc.PrimaryIndexSpan(h.server.Codec())

				// Check if completed spans cover the entire table span
				var spanGroup roachpb.SpanGroup
				spanGroup.Add(completedSpans...)
				mergedSpans := spanGroup.Slice()
				require.Greater(t, len(mergedSpans), 0)

				// Find the overall span covered by completed spans
				overallStart := mergedSpans[0].Key
				overallEnd := mergedSpans[len(mergedSpans)-1].EndKey
				actualCoverage := roachpb.Span{Key: overallStart, EndKey: overallEnd}

				// Verify the completed spans cover the entire table
				require.True(t, actualCoverage.Contains(tableSpan),
					"completed spans should cover entire table span")
			}
		} else {
			// In legacy mode, check the deprecated span count fields
			require.Equal(t, expectedJobSpanCount, rowLevelTTLProgress.JobProcessedSpanCount)
			require.Equal(t, expectedJobSpanCount, rowLevelTTLProgress.JobTotalSpanCount)
		}
		require.Equal(t, expectedJobRowCount, rowLevelTTLProgress.JobDeletedRowCount)

		// Verify job reached 100% completion
		var fractionComplete float32
		if f, ok := progress.Progress.(*jobspb.Progress_FractionCompleted); ok {
			fractionComplete = f.FractionCompleted
		}
		require.InEpsilon(t, float32(1.0), fractionComplete, 0.001,
			"expected job to reach 100%% completion, got %.3f", fractionComplete)

		jobCount++
	}
	require.Equal(t, 1, jobCount)
}

func TestRowLevelTTLNoTestingKnobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc                string
		runMinActiveVersion bool
	}{
		{"latest", false},
		{"min active version", true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				nil, /* SQLTestingKnobs */
				1,   /* numNodes */
				tc.runMinActiveVersion,
			)
			defer cleanupFunc()

			th.sqlDB.Exec(t, `CREATE TABLE t (id INT PRIMARY KEY) WITH (ttl_expire_after = '1 minute')`)
			th.sqlDB.Exec(t, `INSERT INTO t (id, crdb_internal_expiration) VALUES (1, now() - '1 month')`)

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StateFailed, `found a recent schema change on the table`)
		})
	}
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
			expectedTTLError: "found a recent schema change on the table at .*, job will run at the next scheduled time",
			aostDuration:     -48 * time.Hour,
		},
		{
			desc:             "schema change during job",
			expectedTTLError: "error during row deletion: table has had a schema change since the job has started at .*, job will run at the next scheduled time",
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
				1,     /* numNodes */
				false, /* runMinActiveVersion */
			)
			defer cleanupFunc()
			th.sqlDB.Exec(t, createTable)

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StateFailed, tc.expectedTTLError)
		})
	}
}

func TestRowLevelTTLAlterTypeInPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc                string
		runMinActiveVersion bool
	}{
		{"latest", false},
		{"min active version", true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration: &zeroDuration,
					// Prior to https://github.com/cockroachdb/cockroach/pull/145374, this
					// pre-select ALTER TYPE statement would hang forever.
					PreSelectStatement: `ALTER TYPE defaultdb.public.typ ADD VALUE 'c'`,
				},
				1, /* numNodes */
				tc.runMinActiveVersion,
			)
			defer cleanupFunc()
			th.sqlDB.Exec(t, `CREATE TYPE typ AS ENUM ('foo', 'bar')`)
			th.sqlDB.Exec(t, `CREATE TABLE t (
	id INT,
  v typ,
  PRIMARY KEY (id, v)
) WITH (ttl_expire_after = '10 minutes')`)
			th.sqlDB.Exec(t, `ALTER TABLE t SPLIT AT VALUES (1), (2)`)
			th.sqlDB.Exec(t, `INSERT INTO t (id, v, crdb_internal_expiration) VALUES (1, 'foo', now() - '1 month'), (2, 'bar', now() - '1 month')`)

			// Prior to https://github.com/cockroachdb/cockroach/pull/145374, the job
			// would fail with a "comparison of two different versions of enum" error.
			th.waitForScheduledJob(t, jobs.StateSucceeded, "")
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
				1,     /* numNodes */
				false, /* runMinActiveVersion */
			)
			defer cleanupFunc()

			th.sqlDB.ExecMultiple(t, strings.Split(tc.setup, ";")...)

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StateFailed, tc.expectedTTLError)

			var numRows int
			th.sqlDB.QueryRow(t, `SELECT count(1) FROM t`).Scan(&numRows)
			require.Equal(t, 2, numRows)
		})
	}
}

func TestRowLevelTTLJobMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "the test times out under race")

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
		for _, runAtMinClusterVersion := range []bool{false, true} {
			desc := tc.desc
			if runAtMinClusterVersion {
				desc += " (min cluster version)"
			}
			t.Run(desc, func(t *testing.T) {
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
					numNodes,
					runAtMinClusterVersion,
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
					th.server.Codec(),
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
				th.waitForScheduledJob(t, jobs.StateSucceeded, "")

				// Verify results
				th.verifyNonExpiredRows(t, tableName, expirationExpr, expectedNumNonExpiredRows)
				th.verifyExpiredRows(t, expectedSQLInstanceIDToProcessorMap)
			})
		}
	}
}

// TestRowLevelTTLJobRandomEntries inserts random entries into a given table
// and runs a TTL job on them.
func TestRowLevelTTLJobRandomEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "this test is very slow")
	skip.UnderShort(t, "slow test")

	rng, _ := randutil.NewTestRand()

	collatedStringType := types.MakeCollatedString(types.String, "en" /* locale */)
	var indexableTyps []*types.T
	for _, typ := range append(types.Scalar, collatedStringType) {
		if !colinfo.ColumnTypeIsIndexable(typ) {
			continue
		}
		ok := func() bool {
			switch typ.Family() {
			case types.DateFamily:
				// TODO(#76419): DateFamily has a broken `-infinity` case.
				return false
			case types.JsonFamily:
				// TODO(#99432): JsonFamily has broken cases. This is because the
				// test is wrapping JSON objects in multiple single quotes which
				// causes parsing errors.
				return false
			case types.LTreeFamily:
				// LTREE is only supported in 25.4+, so if we happen to run the
				// test in the mixed version variant, we can't use the type.
				return int(clusterversion.MinSupported) >= int(clusterversion.V25_4)
			default:
				return true
			}
		}()
		if ok {
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
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
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
			desc: "three column pk DESC",
			createTable: `CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	other_col INT,
	"quote-kw-col" TIMESTAMPTZ,
	text TEXT,
	PRIMARY KEY (id, other_col DESC, "quote-kw-col")
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
) WITH (ttl_expire_after = '30 days', ttl_select_rate_limit = 350, ttl_delete_rate_limit = 350)`,
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
	// Also randomly generate random PKs and families.
	generateFamilyClauses := func(colNames []string) string {
		familyClauses := strings.Builder{}
		numFamilies := rng.Intn(len(colNames))
		for fam := 0; fam < numFamilies && len(colNames) > 0; fam++ {
			rng.Shuffle(len(colNames), func(i, j int) {
				colNames[i], colNames[j] = colNames[j], colNames[i]
			})
			familySize := 1 + rng.Intn(len(colNames))
			familyClauses.WriteString(fmt.Sprintf("FAMILY fam%d (", fam))
			for col := 0; col < familySize; col++ {
				if col > 0 {
					familyClauses.WriteString(", ")
				}
				familyClauses.WriteString(colNames[col])
			}
			colNames = colNames[familySize:]
			familyClauses.WriteString("), ")
		}
		return familyClauses.String()
	}
	for i := 0; i < 5; i++ {
		familyClauses := generateFamilyClauses([]string{"id", "rand_col_1", "rand_col_2", "t", "i"})
		testCases = append(
			testCases,
			testCase{
				desc: fmt.Sprintf("random %d", i+1),
				createTable: fmt.Sprintf(
					`CREATE TABLE tbl (
	id UUID DEFAULT gen_random_uuid(),
	rand_col_1 %s,
	rand_col_2 %s,
	t TEXT NULL,
	i INT8 NULL,
	%s
	PRIMARY KEY (id, rand_col_1, rand_col_2)
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = %d, ttl_delete_batch_size = %d)`,
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					familyClauses,
					10+rng.Intn(100),
					10+rng.Intn(100),
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

				nullOK := def.Nullable.Nullability == tree.Null
				d := randgen.RandDatum(rng, def.Type.(*types.T), nullOK)
				if d == tree.DNull {
					values = append(values, nil)
				} else {
					f := tree.NewFmtCtx(tree.FmtBareStrings)
					d.Format(f)
					values = append(values, f.CloseAndGetString())
				}
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
			runAtMixedClusterVersion := rng.Intn(2) == 0
			// Log to make it slightly easier to reproduce a random config.
			t.Logf("test case (runAtMixedClusterVersion=%t): %#v", runAtMixedClusterVersion, tc)
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration: &zeroDuration,
				},
				1, /* numNodes */
				runAtMixedClusterVersion,
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
					th.server.Codec(),
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
			th.waitForScheduledJob(t, jobs.StateSucceeded, "")

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

func TestRowLevelTTLCancelStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
			ExtraStatsQuery:  "SELECT pg_sleep(100)",
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	th.sqlDB.Exec(t, `
CREATE TABLE t (
  id INT PRIMARY KEY,
  expire_at TIMESTAMPTZ
) WITH (
  ttl_expiration_expression = 'expire_at',
  ttl_row_stats_poll_interval = '1 minute'
)`)
	th.sqlDB.Exec(t, `INSERT INTO t (id, expire_at) VALUES (1, '2020-01-01')`)

	// Force the schedule to execute. Normally, the job would not fail due to a
	// stats error, but we have set the ReturnStatsError knob to true in this
	// test.
	th.waitForScheduledJob(t, jobs.StateFailed, "cancelling TTL stats query because TTL job completed")

	results := th.sqlDB.QueryStr(t, "SELECT * FROM t")
	require.Empty(t, results)
}

func TestOutboundForeignKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		desc                string
		runMinActiveVersion bool
	}{
		{"latest", false},
		{"min active version", true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration:     &zeroDuration,
					ReturnStatsError: true,
				},
				1, /* numNodes */
				tc.runMinActiveVersion,
			)
			defer cleanupFunc()

			sqlDB := th.sqlDB
			sqlDB.Exec(t, "CREATE TABLE parent (id INT PRIMARY KEY)")
			sqlDB.Exec(t, "CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ, parent_id INT REFERENCES parent (id)) WITH (ttl_expiration_expression = 'expire_at')")

			sqlDB.Exec(t, "INSERT INTO parent VALUES (1)")
			sqlDB.Exec(t, "INSERT INTO tbl VALUES (1, '2020-01-01', 1)")

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StateSucceeded, "")

			results := sqlDB.QueryStr(t, "SELECT * FROM tbl")
			require.Empty(t, results)
		})
	}
}

func TestInboundForeignKeyOnDeleteCascade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, "CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')")
	sqlDB.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, tbl_id INT REFERENCES tbl (id) ON DELETE CASCADE)")

	sqlDB.Exec(t, "INSERT INTO tbl VALUES (1, '2020-01-01')")
	sqlDB.Exec(t, "INSERT INTO child VALUES (1, 1)")

	// Force the schedule to execute.
	th.waitForScheduledJob(t, jobs.StateSucceeded, "")

	results := sqlDB.QueryStr(t, "SELECT * FROM tbl")
	require.Empty(t, results)

	results = sqlDB.QueryStr(t, "SELECT * FROM child")
	require.Empty(t, results)
}

func TestInboundForeignKeyOnDeleteRestrict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, "CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')")
	sqlDB.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, tbl_id INT REFERENCES tbl (id) ON DELETE RESTRICT)")

	sqlDB.Exec(t, "INSERT INTO tbl VALUES (1, '2020-01-01')")
	sqlDB.Exec(t, "INSERT INTO child VALUES (1, 1)")

	// Force the schedule to execute.
	th.waitForScheduledJob(t, jobs.StateFailed, `delete on table "tbl" violates foreign key constraint "child_tbl_id_fkey" on table "child"`)

	results := sqlDB.QueryStr(t, "SELECT * FROM tbl")
	require.Len(t, results, 1)

	results = sqlDB.QueryStr(t, "SELECT * FROM child")
	require.Len(t, results, 1)
}

func TestInboundForeignKeyOnDeleteRestrictNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, "CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')")
	sqlDB.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, tbl_id INT REFERENCES tbl (id) ON DELETE RESTRICT)")

	sqlDB.Exec(t, "INSERT INTO tbl VALUES (1, '2020-01-01')")
	sqlDB.Exec(t, "INSERT INTO child VALUES (1, NULL)")

	// Force the schedule to execute.
	th.waitForScheduledJob(t, jobs.StateSucceeded, "")

	results := sqlDB.QueryStr(t, "SELECT * FROM tbl")
	require.Len(t, results, 0)

	results = sqlDB.QueryStr(t, "SELECT * FROM child")
	require.Len(t, results, 1)
}

func TestMakeTTLJobDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc                 string
		tableSelectBatchSize int
		jobSelectBatchSize   int
	}{
		{
			desc:                 "default ttl_select_batch_size",
			tableSelectBatchSize: 0,
			jobSelectBatchSize:   ttlbase.DefaultSelectBatchSizeValue,
		},
		{
			desc:                 "override ttl_select_batch_size",
			tableSelectBatchSize: 1,
			jobSelectBatchSize:   1,
		},
	}

	getCreateTable := func(selectBatchSize int) string {
		const createTable = `CREATE TABLE t (
    id INT PRIMARY KEY,
    expire_at TIMESTAMPTZ
) WITH (
    %s
    ttl_expiration_expression = 'expire_at',
    ttl_job_cron = '* * * * *'
)`
		selectBatchSizeClause := ""
		if selectBatchSize > 0 {
			selectBatchSizeClause = fmt.Sprintf("ttl_select_batch_size = %d,", selectBatchSize)
		}
		return fmt.Sprintf(createTable, selectBatchSizeClause)
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration: &zeroDuration,
				},
				1,     /* numNodes */
				false, /* runMinActiveVersion */
			)
			defer cleanupFunc()
			createTable := getCreateTable(testCase.tableSelectBatchSize)
			th.sqlDB.Exec(t, createTable)
			th.waitForScheduledJob(t, jobs.StateSucceeded, "")
			rows := th.sqlDB.QueryStr(t, "SELECT description FROM [SHOW JOBS SELECT id FROM system.jobs WHERE job_type = 'ROW LEVEL TTL']")
			t.Log(rows)
			require.Len(t, rows, 1)
			row := rows[0]
			require.Contains(t, row[0], fmt.Sprintf("LIMIT %d", testCase.jobSelectBatchSize))
		})
	}
}

// TestRowLevelTTLJobCancelPrivileges verifies the privileges required to cancel
// TTL jobs. TTL jobs are owned by the user who created the schedule (not the
// node user), so the standard job privilege model applies:
//   - Admin users can cancel any TTL job
//   - Users with CONTROLJOB privilege can cancel any TTL job
//   - The job owner can cancel their own job
//   - Users without privileges cannot cancel TTL jobs
func TestRowLevelTTLJobCancelPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "job adoption interval is longer under duress, so the test is too slow")

	type testCase struct {
		name string
		// cancelUser is the user that will attempt to cancel the job.
		cancelUser string
		// cancelUserPrivileges are the privileges to grant to cancelUser.
		cancelUserPrivileges []string
		// expectCancel indicates whether the cancel should succeed.
		expectCancel bool
	}

	testCases := []testCase{
		{
			name:                 "admin can cancel any TTL job",
			cancelUser:           "adminuser",
			cancelUserPrivileges: []string{"GRANT admin TO adminuser"},
			expectCancel:         true,
		},
		{
			name:                 "user with CONTROLJOB can cancel any TTL job",
			cancelUser:           "controljobuser",
			cancelUserPrivileges: []string{"GRANT SYSTEM CONTROLJOB TO controljobuser"},
			expectCancel:         true,
		},
		{
			name:                 "job owner can cancel own job",
			cancelUser:           "ttluser",
			cancelUserPrivileges: nil, // ttluser owns the job, which is sufficient
			expectCancel:         true,
		},
		{
			name:                 "user without privileges cannot cancel TTL job",
			cancelUser:           "noprivuser",
			cancelUserPrivileges: nil,
			expectCancel:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use a channel to signal when the job has started.
			jobStarted := make(chan struct{})

			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration: &zeroDuration,
					// Use BeforeProcessorStart to synchronize with the job. The hook
					// signals that the job has started, then blocks until the job's
					// context is canceled (which happens when the job is canceled).
					BeforeProcessorStart: func(ctx context.Context) error {
						close(jobStarted)
						<-ctx.Done()
						return ctx.Err()
					},
				},
				1,     /* numNodes */
				false, /* runMinActiveVersion */
			)
			defer cleanupFunc()

			// Create ttluser who will own the table/schedule.
			th.sqlDB.Exec(t, `CREATE USER ttluser WITH PASSWORD 'password'`)
			th.sqlDB.Exec(t, `GRANT admin TO ttluser`)

			// Create the cancel user if different from ttluser.
			if tc.cancelUser != "ttluser" {
				th.sqlDB.Exec(t, fmt.Sprintf(`CREATE USER %s WITH PASSWORD 'password'`, tc.cancelUser))
			}

			// Grant privileges to the cancel user.
			for _, priv := range tc.cancelUserPrivileges {
				th.sqlDB.Exec(t, priv)
			}

			// Connect as ttluser and create a table with TTL.
			ttlUserConn := th.server.SQLConn(t, serverutils.UserPassword("ttluser", "password"), serverutils.ClientCerts(false))
			ttlUserDB := sqlutils.MakeSQLRunner(ttlUserConn)
			ttlUserDB.Exec(t, `CREATE TABLE t (
				id INT PRIMARY KEY,
				expire_at TIMESTAMPTZ
			) WITH (ttl_expiration_expression = 'expire_at')`)
			ttlUserDB.Exec(t, `INSERT INTO t (id, expire_at) VALUES (1, '2000-01-01')`)

			// Revoke admin from ttluser now that the table is created.
			th.sqlDB.Exec(t, `REVOKE admin FROM ttluser`)

			// Execute the schedule to create a TTL job.
			require.NoError(t, th.executeSchedules())

			// Wait for the job to start and reach the synchronization point.
			select {
			case <-jobStarted:
			case <-time.After(30 * time.Second):
				t.Fatal("timed out waiting for TTL job to start")
			}

			// Find the job ID.
			rows := th.sqlDB.QueryStr(t, `
				SELECT job_id FROM [SHOW JOBS]
				WHERE job_type = 'ROW LEVEL TTL' AND status = 'running'
			`)
			require.Len(t, rows, 1, "expected exactly one running TTL job")
			jobID, err := strconv.ParseInt(rows[0][0], 10, 64)
			require.NoError(t, err)

			// Verify the job is owned by ttluser, not node.
			var jobOwner string
			th.sqlDB.QueryRow(t, `SELECT user_name FROM [SHOW JOBS] WHERE job_id = $1`, jobID).Scan(&jobOwner)
			require.Equal(t, "ttluser", jobOwner, "TTL job should be owned by the user who created the schedule")

			// Connect as the cancel user.
			cancelConn := th.server.SQLConn(t, serverutils.UserPassword(tc.cancelUser, "password"), serverutils.ClientCerts(false))

			// Attempt to cancel the job.
			_, err = cancelConn.Exec(fmt.Sprintf(`CANCEL JOB %d`, jobID))

			if tc.expectCancel {
				require.NoError(t, err, "expected %s to be able to cancel the job", tc.cancelUser)

				// Wait for the job to reach canceled status.
				testutils.SucceedsWithin(t, func() error {
					var status string
					th.sqlDB.QueryRow(t, `SELECT status FROM [SHOW JOBS] WHERE job_id = $1`, jobID).Scan(&status)
					if status != "canceled" {
						return errors.Newf("job status is %q, waiting for 'canceled'", status)
					}
					return nil
				}, 5*time.Minute)
			} else {
				require.Errorf(t, err, "expected %s to not be able to cancel the job", tc.cancelUser)
				require.ErrorContains(t, err, "does not have privileges")
			}
		})
	}
}

// TestRowLevelTTLRunsAsTableOwner verifies that the row-level TTL job runs its
// statements as the TTL table's owner: the job expires rows on a table owned by
// a non-admin user, keeps working under a deny-all FORCE ROW LEVEL SECURITY
// policy and when the owner lacks EXECUTE on a referenced UDF, runs the table's
// DELETE triggers with the owner as current_user, and drives foreign-key
// cascades into referencing tables. The cascade cases give the referencing
// tables to a different owner, with no grants to the TTL table's owner, so that
// the descriptor overrides computed by ttlSessionOverrides — not the owner's
// own privileges — are what authorize the cascades.
func TestRowLevelTTLRunsAsTableOwner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc  string
		setup []string
		// tableName overrides the name of the TTL table checked for emptiness
		// after the job runs; it defaults to "tbl".
		tableName string
		// verify runs after the TTL job has succeeded.
		verify func(t *testing.T, sqlDB *sqlutils.SQLRunner)
	}{
		{
			desc: "table owned by non-admin user",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
		},
		{
			// tbl references a UDF (via a CHECK constraint), so the job leaves the
			// node-user fast path and runs as the owner. The owner is subject to
			// FORCE ROW LEVEL SECURITY on its own table, so the BypassRLS flag in
			// the descriptor overrides is what lets the job still see and delete
			// expired rows under the deny-all policy. Without the fast-path exit the
			// delete would run as node, which bypasses RLS inherently and would not
			// exercise the override at all.
			desc: "deny-all FORCE ROW LEVEL SECURITY policy cannot block expiration",
			setup: []string{
				`CREATE FUNCTION rls_check(i INT) RETURNS BOOL IMMUTABLE LANGUAGE SQL AS 'SELECT true'`,
				`CREATE TABLE tbl (
					id INT PRIMARY KEY,
					expire_at TIMESTAMPTZ,
					CHECK (rls_check(id))
				) WITH (ttl_expiration_expression = 'expire_at')`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`CREATE POLICY deny_all ON tbl USING (false)`,
				`ALTER TABLE tbl ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
		},
		{
			// The computed column's UDF is built when the table is scanned; the
			// EXECUTE override is what lets the owner evaluate it after EXECUTE is
			// revoked from public.
			desc: "UDF computed column with EXECUTE revoked from the owner",
			setup: []string{
				`CREATE FUNCTION plus_one(i INT) RETURNS INT IMMUTABLE LANGUAGE SQL AS 'SELECT i + 1'`,
				`CREATE TABLE tbl (
					id INT PRIMARY KEY,
					expire_at TIMESTAMPTZ,
					c INT AS (plus_one(id)) STORED
				) WITH (ttl_expiration_expression = 'expire_at')`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`REVOKE EXECUTE ON FUNCTION plus_one FROM public`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
		},
		{
			desc: "row-level DELETE trigger runs as the table owner",
			setup: []string{
				`CREATE TABLE audit (id SERIAL PRIMARY KEY, usr STRING)`,
				`GRANT INSERT ON TABLE audit TO tbl_owner`,
				`CREATE FUNCTION log_delete() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
					BEGIN
						INSERT INTO audit (usr) VALUES (current_user);
						RETURN OLD;
					END
				$$`,
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`GRANT EXECUTE ON FUNCTION log_delete TO tbl_owner`,
				`CREATE TRIGGER log_del BEFORE DELETE ON tbl FOR EACH ROW EXECUTE FUNCTION log_delete()`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				sqlDB.CheckQueryResults(t,
					`SELECT DISTINCT usr FROM audit`,
					[][]string{{"tbl_owner"}},
				)
			},
		},
		{
			desc: "ON DELETE CASCADE child owned by another user",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE cascade_child (id INT PRIMARY KEY, pid INT REFERENCES tbl(id) ON DELETE CASCADE)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE cascade_child OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
				`INSERT INTO cascade_child VALUES (10, 1), (20, 2)`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				// The cascade deleted the child rows.
				require.Empty(t, sqlDB.QueryStr(t, `SELECT id FROM cascade_child`))
			},
		},
		{
			desc: "ON DELETE SET NULL child owned by another user",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE setnull_child (id INT PRIMARY KEY, pid INT REFERENCES tbl(id) ON DELETE SET NULL)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE setnull_child OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
				`INSERT INTO setnull_child VALUES (10, 1), (20, 2)`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				// The cascade set the child FK column to NULL.
				sqlDB.CheckQueryResults(t,
					`SELECT id, pid FROM setnull_child ORDER BY id`,
					[][]string{{"10", "NULL"}, {"20", "NULL"}},
				)
			},
		},
		{
			// The SET NULL cascade deletes tbl rows, cascades into child, and the
			// child deletion cascades into grandchild — two levels below the TTL
			// table, all owned by another user.
			desc: "cascade chain reaches a grandchild",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES tbl(id) ON DELETE CASCADE)`,
				`CREATE TABLE grandchild (id INT PRIMARY KEY, pid INT REFERENCES child(id) ON DELETE SET NULL)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE child OWNER TO child_owner`,
				`ALTER TABLE grandchild OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
				`INSERT INTO child VALUES (10, 1), (20, 2)`,
				`INSERT INTO grandchild VALUES (100, 10), (200, 20)`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				require.Empty(t, sqlDB.QueryStr(t, `SELECT id FROM child`))
				sqlDB.CheckQueryResults(t,
					`SELECT id, pid FROM grandchild ORDER BY id`,
					[][]string{{"100", "NULL"}, {"200", "NULL"}},
				)
			},
		},
		{
			// diamond reaches the same child twice: first via a NO ACTION edge
			// (grant-only), later via a CASCADE edge through mid, which must still
			// enqueue diamond_child so grandchild gets its grants.
			desc: "diamond where a scan-only child is later reached by a cascade",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE mid (id INT PRIMARY KEY, pid INT REFERENCES tbl(id) ON DELETE CASCADE)`,
				`CREATE TABLE diamond_child (
					id INT PRIMARY KEY,
					pid_tbl INT REFERENCES tbl(id),
					pid_mid INT REFERENCES mid(id) ON DELETE CASCADE
				)`,
				`CREATE TABLE grandchild (id INT PRIMARY KEY, pid INT REFERENCES diamond_child(id) ON DELETE CASCADE)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE mid OWNER TO child_owner`,
				`ALTER TABLE diamond_child OWNER TO child_owner`,
				`ALTER TABLE grandchild OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
				`INSERT INTO mid VALUES (10, 1), (20, 2)`,
				`INSERT INTO diamond_child (id, pid_mid) VALUES (100, 10), (200, 20)`,
				`INSERT INTO grandchild VALUES (1000, 100), (2000, 200)`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				require.Empty(t, sqlDB.QueryStr(t, `SELECT id FROM diamond_child`))
				require.Empty(t, sqlDB.QueryStr(t, `SELECT id FROM grandchild`))
			},
		},
		{
			// Rewriting setnull_child.pid re-checks the second foreign key on the
			// same column, which existence-scans other_parent; the SELECT override
			// on other_parent is what lets plan building succeed.
			desc: "SET NULL child with a second foreign key on the rewritten column",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE other_parent (id INT PRIMARY KEY)`,
				`CREATE TABLE setnull_child (
					id INT PRIMARY KEY,
					pid INT REFERENCES tbl(id) ON DELETE SET NULL,
					FOREIGN KEY (pid) REFERENCES other_parent(id)
				)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE other_parent OWNER TO child_owner`,
				`ALTER TABLE setnull_child OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
				`INSERT INTO other_parent VALUES (1), (2)`,
				`INSERT INTO setnull_child VALUES (10, 1), (20, 2)`,
			},
			verify: func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
				sqlDB.CheckQueryResults(t,
					`SELECT id, pid FROM setnull_child ORDER BY id`,
					[][]string{{"10", "NULL"}, {"20", "NULL"}},
				)
			},
		},
		{
			// The RESTRICT existence check is planned (and privilege-checked)
			// regardless of data, so the job needs the SELECT override on the
			// child even when no rows reference the expiring ones.
			desc: "RESTRICT child with no referencing rows",
			setup: []string{
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TABLE restrict_child (id INT PRIMARY KEY, pid INT REFERENCES tbl(id) ON DELETE RESTRICT)`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`ALTER TABLE restrict_child OWNER TO child_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
		},
		{
			// A DELETE trigger whose function lives in a user-defined schema the
			// owner has no USAGE on. The function is referenced by ID and
			// authorized by the EXECUTE override, so it resolves and runs during
			// the delete without a USAGE grant on its containing schema. This
			// covers cross-schema function resolution, which the same-schema UDF
			// cases do not exercise.
			desc: "referenced trigger function in a non-public schema",
			setup: []string{
				`CREATE SCHEMA fnschema`,
				`REVOKE USAGE ON SCHEMA fnschema FROM public`,
				`CREATE FUNCTION fnschema.log_del() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$ BEGIN RETURN OLD; END $$`,
				`CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`CREATE TRIGGER log_del BEFORE DELETE ON tbl FOR EACH ROW EXECUTE FUNCTION fnschema.log_del()`,
				`ALTER TABLE tbl OWNER TO tbl_owner`,
				`INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
		},
		{
			// The owner holds no USAGE on the user-created schema; the schema
			// USAGE override is what lets name resolution succeed.
			desc: "TTL table in a non-public schema",
			setup: []string{
				`CREATE SCHEMA ttlschema`,
				// ALTER TABLE ... OWNER TO requires the new owner to hold CREATE
				// on the schema; revoke it again so the job runs with the owner
				// holding no schema privileges at all.
				`GRANT USAGE, CREATE ON SCHEMA ttlschema TO tbl_owner`,
				`CREATE TABLE ttlschema.tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`,
				`ALTER TABLE ttlschema.tbl OWNER TO tbl_owner`,
				`REVOKE USAGE, CREATE ON SCHEMA ttlschema FROM tbl_owner`,
				`INSERT INTO ttlschema.tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`,
			},
			tableName: "ttlschema.tbl",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
				t,
				&sql.TTLTestingKnobs{
					AOSTDuration:     &zeroDuration,
					ReturnStatsError: true,
				},
				1,     /* numNodes */
				false, /* runMinActiveVersion */
			)
			defer cleanupFunc()

			sqlDB := th.sqlDB
			sqlDB.Exec(t, `CREATE USER tbl_owner`)
			sqlDB.Exec(t, `CREATE USER child_owner`)
			for _, stmt := range tc.setup {
				sqlDB.Exec(t, stmt)
			}

			// Force the schedule to execute.
			th.waitForScheduledJob(t, jobs.StateSucceeded, "")

			tableName := tc.tableName
			if tableName == "" {
				tableName = "tbl"
			}
			results := sqlDB.QueryStr(t, "SELECT * FROM "+tableName)
			require.Empty(t, results)

			if tc.verify != nil {
				tc.verify(t, sqlDB)
			}
		})
	}
}

// TestRowLevelTTLStatsRunAsTableOwner verifies that the row-count statistics
// queries also run as the TTL table's owner with the descriptor overrides
// applied: under a deny-all FORCE ROW LEVEL SECURITY policy, the RLS bypass is
// what lets the count queries see the table's rows at all. A regression here is
// silent — the count queries would succeed but report zero rows — so the test
// asserts the reported metric value rather than just job success.
//
// tbl references a UDF (via a CHECK constraint) so the job leaves the node-user
// fast path and runs as the owner; otherwise the stats queries would run as
// node, which bypasses RLS inherently and would not exercise the BypassRLS
// override.
func TestRowLevelTTLStatsRunAsTableOwner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Hold the delete processors until the initial stats fetch has published
	// its counts, so the deletes cannot race with the row counts the test
	// asserts on. ReturnStatsError deliberately stays unset: with a poll
	// interval configured, job completion always cancels the stats group, and
	// the knob would turn that expected cancellation into a job failure (see
	// TestRowLevelTTLCancelStats). A stats regression is instead caught by the
	// metric assertion below.
	statsPublished := make(chan struct{})
	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration: &zeroDuration,
			BeforeProcessorStart: func(ctx context.Context) error {
				select {
				case <-statsPublished:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, `CREATE USER tbl_owner`)
	sqlDB.Exec(t, `CREATE FUNCTION rls_check(i INT) RETURNS BOOL IMMUTABLE LANGUAGE SQL AS 'SELECT true'`)
	sqlDB.Exec(t, `CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ, CHECK (rls_check(id)))
		WITH (ttl_expiration_expression = 'expire_at', ttl_row_stats_poll_interval = '1 minute')`)
	sqlDB.Exec(t, `ALTER TABLE tbl OWNER TO tbl_owner`)
	sqlDB.Exec(t, `CREATE POLICY deny_all ON tbl USING (false)`)
	sqlDB.Exec(t, `ALTER TABLE tbl ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY`)
	sqlDB.Exec(t, `INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`)
	sqlDB.Exec(t, `INSERT INTO tbl VALUES (3, '2200-01-01'), (4, '2200-01-01'), (5, '2200-01-01')`)

	// Release the processors once both stats gauges have been published;
	// fetchStatistics sets total_rows first and total_expired_rows last, so
	// waiting on both means the initial stats fetch has completed with the
	// table still fully populated. On timeout — e.g. if a stats regression
	// makes the counts come back wrong — release the processors anyway and
	// let the metric assertion below fail with a useful message.
	go func() {
		defer close(statsPublished)
		deadline := timeutil.Now().Add(time.Minute)
		for timeutil.Now().Before(deadline) {
			var totalRows, expiredRows int
			err := sqlDB.DB.QueryRowContext(context.Background(),
				`SELECT
					sum(IF(name = 'jobs.row_level_ttl.total_rows', value, 0))::INT,
					sum(IF(name = 'jobs.row_level_ttl.total_expired_rows', value, 0))::INT
				FROM crdb_internal.node_metrics
				WHERE name IN ('jobs.row_level_ttl.total_rows', 'jobs.row_level_ttl.total_expired_rows')`,
			).Scan(&totalRows, &expiredRows)
			if err == nil && totalRows == 5 && expiredRows == 2 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	th.waitForScheduledJob(t, jobs.StateSucceeded, "")

	// The count ran before any deletes, so it must have seen all five rows
	// despite the deny-all policy.
	var totalRows int
	sqlDB.QueryRow(t,
		`SELECT value::INT FROM crdb_internal.node_metrics WHERE name = 'jobs.row_level_ttl.total_rows'`,
	).Scan(&totalRows)
	require.Equal(t, 5, totalRows)

	sqlDB.CheckQueryResults(t, `SELECT id FROM tbl ORDER BY id`,
		[][]string{{"3"}, {"4"}, {"5"}})
}

// interlockKeyRE extracts the confirmation key from the error returned when
// setting an unsafe cluster setting without the interlock key set.
var interlockKeyRE = regexp.MustCompile(`unsafe_setting_interlock_key = '([^']+)'`)

// setUnsafeClusterSetting runs a SET CLUSTER SETTING statement for an unsafe
// setting, satisfying the interlock by extracting the confirmation key from the
// first attempt's error and setting it before retrying.
func setUnsafeClusterSetting(t *testing.T, sqlDB *sqlutils.SQLRunner, setStmt string) {
	t.Helper()
	_, err := sqlDB.DB.ExecContext(context.Background(), setStmt)
	require.Error(t, err, "expected interlock error on first attempt")
	m := interlockKeyRE.FindStringSubmatch(err.Error())
	require.Len(t, m, 2, "could not find interlock key in error: %v", err)
	sqlDB.Exec(t, "SET unsafe_setting_interlock_key = $1", m[1])
	sqlDB.Exec(t, setStmt)
}

// TestRowLevelTTLRunAsTableOwnerDisabled verifies that the
// sql.ttl.run_as_table_owner.unsafe.enabled opt-out restores the previous node-user
// behavior: with the setting off, the TTL delete's trigger runs with node as
// current_user rather than the table owner.
func TestRowLevelTTLRunAsTableOwnerDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	setUnsafeClusterSetting(t, sqlDB,
		"SET CLUSTER SETTING sql.ttl.run_as_table_owner.unsafe.enabled = false")

	sqlDB.Exec(t, `CREATE USER tbl_owner`)
	sqlDB.Exec(t, `CREATE TABLE audit (id SERIAL PRIMARY KEY, usr STRING)`)
	sqlDB.Exec(t, `CREATE FUNCTION log_delete() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
		BEGIN
			INSERT INTO audit (usr) VALUES (current_user);
			RETURN OLD;
		END
	$$`)
	sqlDB.Exec(t, `CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`)
	sqlDB.Exec(t, `ALTER TABLE tbl OWNER TO tbl_owner`)
	sqlDB.Exec(t, `CREATE TRIGGER log_del BEFORE DELETE ON tbl FOR EACH ROW EXECUTE FUNCTION log_delete()`)
	sqlDB.Exec(t, `INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`)

	th.waitForScheduledJob(t, jobs.StateSucceeded, "")

	require.Empty(t, sqlDB.QueryStr(t, `SELECT * FROM tbl`))
	// With the owner-based path disabled, the delete (and its trigger) ran as
	// the node user, so current_user recorded by the trigger is "node".
	sqlDB.CheckQueryResults(t, `SELECT DISTINCT usr FROM audit`, [][]string{{"node"}})
}

// TestRowLevelTTLRunsAsTableOwnerPermanentFailure verifies that a privilege
// error the owner-based overrides cannot resolve — a DELETE trigger writing to
// a table the owner has no access to — fails the job permanently rather than
// retrying forever. A retriable classification would leave the job running and
// the wait below would time out.
func TestRowLevelTTLRunsAsTableOwnerPermanentFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanupFunc := newRowLevelTTLTestJobTestHelper(
		t,
		&sql.TTLTestingKnobs{
			AOSTDuration:     &zeroDuration,
			ReturnStatsError: true,
		},
		1,     /* numNodes */
		false, /* runMinActiveVersion */
	)
	defer cleanupFunc()

	sqlDB := th.sqlDB
	sqlDB.Exec(t, `CREATE USER tbl_owner`)
	// audit is owned by root; tbl_owner is granted no privileges on it, and the
	// FK closure does not include it, so no override authorizes the trigger's
	// INSERT.
	sqlDB.Exec(t, `CREATE TABLE audit (id SERIAL PRIMARY KEY, usr STRING)`)
	sqlDB.Exec(t, `CREATE FUNCTION log_delete() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
		BEGIN
			INSERT INTO audit (usr) VALUES (current_user);
			RETURN OLD;
		END
	$$`)
	sqlDB.Exec(t, `CREATE TABLE tbl (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`)
	sqlDB.Exec(t, `ALTER TABLE tbl OWNER TO tbl_owner`)
	sqlDB.Exec(t, `GRANT EXECUTE ON FUNCTION log_delete TO tbl_owner`)
	sqlDB.Exec(t, `CREATE TRIGGER log_del BEFORE DELETE ON tbl FOR EACH ROW EXECUTE FUNCTION log_delete()`)
	sqlDB.Exec(t, `INSERT INTO tbl VALUES (1, '2020-01-01'), (2, '2020-01-01')`)

	// The job error names both the missing privilege and the cluster setting
	// that restores the previous node-user behavior, so an operator who hits
	// this during an unattended upgrade can act on SHOW JOBS alone.
	th.waitForScheduledJob(t, jobs.StateFailed,
		`sql\.ttl\.run_as_table_owner\.unsafe\.enabled.*does not have INSERT privilege`)

	// The rows survive the failed delete.
	require.Len(t, sqlDB.QueryStr(t, `SELECT * FROM tbl`), 2)
}
