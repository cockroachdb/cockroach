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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type rowLevelTTLTestJobTestHelper struct {
	server           serverutils.TestServerInterface
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	sqlDB            *sqlutils.SQLRunner
	kvDB             *kv.DB
	executeSchedules func() error
}

func newRowLevelTTLTestJobTestHelper(
	t *testing.T, testingKnobs sql.TTLTestingKnobs,
) (*rowLevelTTLTestJobTestHelper, func()) {
	th := &rowLevelTTLTestJobTestHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledRowLevelTTLExecutor,
		),
	}

	knobs := &jobs.TestingKnobs{
		JobSchedulerEnv: th.env,
		TakeOverJobsScheduling: func(fn func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error) {
			th.executeSchedules = func() error {
				defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
				return th.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return fn(ctx, 0 /* allSchedules */, txn)
				})
			}
		},

		CaptureJobExecutionConfig: func(config *scheduledjobs.JobExecutionConfig) {
			th.cfg = config
		},
	}

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: knobs,
			TTL:              &testingKnobs,
		},
	}

	s, db, kvDB := serverutils.StartServer(t, args)
	require.NotNil(t, th.cfg)
	th.kvDB = kvDB
	th.sqlDB = sqlutils.MakeSQLRunner(db)
	th.server = s

	return th, func() {
		s.Stopper().Stop(context.Background())
	}
}

func (h *rowLevelTTLTestJobTestHelper) waitForScheduledJob(
	t *testing.T, expectedStatus jobs.Status, expectedErrorRe string,
) {
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

func (h *rowLevelTTLTestJobTestHelper) waitForSuccessfulScheduledJob(t *testing.T) {
	h.waitForScheduledJob(t, jobs.StatusSucceeded, "")
}

// TestRowLevelTTLInterruptDuringExecution tests that row-level TTL errors
// as appropriate if there is some sort of "interrupting" request.
func TestRowLevelTTLInterruptDuringExecution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	createTable := `CREATE TABLE t (
	id INT PRIMARY KEY
) WITH (ttl_expire_after = '10 minutes', ttl_range_concurrency = 2);
ALTER TABLE t SPLIT AT VALUES (1), (2);
INSERT INTO t (id, crdb_internal_expiration) VALUES (1, now() - '1 month'), (2, now() - '1 month');`

	mockVersion := descpb.DescriptorVersion(0)
	testCases := []struct {
		desc                              string
		expectedTTLError                  string
		aostDuration                      time.Duration
		mockDescriptorVersionDuringDelete *descpb.DescriptorVersion
		onDeleteLoopStart                 func(*testing.T, **sqlutils.SQLRunner) func() error
	}{
		{
			desc:             "schema change too recent to start TTL job",
			expectedTTLError: "found a recent schema change on the table at .*, aborting",
			aostDuration:     -48 * time.Hour,
		},
		{
			desc:             "schema change during job",
			expectedTTLError: "error during row deletion: table has had a schema change since the job has started at .*, aborting",
			aostDuration:     time.Duration(0),
			// We cannot use a schema change to change the version in this test as
			// we overtook the job adoption method, which means schema changes get
			// blocked and may not run.
			mockDescriptorVersionDuringDelete: &mockVersion,
		},
		{
			desc:             "disable cluster setting",
			expectedTTLError: `ttl jobs are currently disabled by CLUSTER SETTING sql.ttl.job.enabled`,
			onDeleteLoopStart: func(t *testing.T, sqlDB **sqlutils.SQLRunner) func() error {
				return func() error {
					(*sqlDB).Exec(t, `SET CLUSTER SETTING sql.ttl.job.enabled = false`)
					return nil
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var onDeleteLoopStart func() error
			var sqlDB *sqlutils.SQLRunner
			if tc.onDeleteLoopStart != nil {
				onDeleteLoopStart = tc.onDeleteLoopStart(t, &sqlDB)
			}
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(t, sql.TTLTestingKnobs{
				AOSTDuration:                      &tc.aostDuration,
				MockDescriptorVersionDuringDelete: tc.mockDescriptorVersionDuringDelete,
				OnDeleteLoopStart:                 onDeleteLoopStart,
			})
			defer cleanupFunc()
			sqlDB = th.sqlDB
			sqlDB.Exec(t, createTable)

			// Force the schedule to execute.
			th.env.SetTime(timeutil.Now().Add(time.Hour * 24))
			require.NoError(t, th.executeSchedules())

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
) WITH (ttl_expire_after = '10 minutes', ttl_range_concurrency = 2%s);
ALTER TABLE t SPLIT AT VALUES (1), (2);
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
			var zeroDuration time.Duration
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(t, sql.TTLTestingKnobs{
				AOSTDuration: &zeroDuration,
			})
			defer cleanupFunc()

			th.sqlDB.Exec(t, tc.setup)

			// Force the schedule to execute.
			th.env.SetTime(timeutil.Now().Add(time.Hour * 24))
			require.NoError(t, th.executeSchedules())

			th.waitForScheduledJob(t, jobs.StatusFailed, tc.expectedTTLError)
			var numRows int
			th.sqlDB.QueryRow(t, `SELECT count(1) FROM t`).Scan(&numRows)
			require.Equal(t, 2, numRows)
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
		desc              string
		createTable       string
		numExpiredRows    int
		numNonExpiredRows int
		numSplits         int
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
			desc: "one column pk with statistics",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days', ttl_row_stats_poll_interval = '1 minute')`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
		},
		{
			desc: "one column pk, concurrentSchemaChange",
			createTable: `CREATE TABLE tbl (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	text TEXT
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10, ttl_range_concurrency = 3)`,
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
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10, ttl_range_concurrency = 3)`,
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
	INDEX (text),
	PRIMARY KEY (id, other_col, "quote-kw-col")
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = 50, ttl_delete_batch_size = 10, ttl_range_concurrency = 3)`,
			numExpiredRows:    1001,
			numNonExpiredRows: 5,
			numSplits:         10,
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
) WITH (ttl_expire_after = '30 days', ttl_select_batch_size = %d, ttl_delete_batch_size = %d, ttl_range_concurrency = %d)`,
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					randgen.RandTypeFromSlice(rng, indexableTyps).SQLString(),
					1+rng.Intn(100),
					1+rng.Intn(100),
					1+rng.Intn(3),
				),
				numSplits:         1 + rng.Intn(9),
				numExpiredRows:    rng.Intn(2000),
				numNonExpiredRows: rng.Intn(100),
			},
		)
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Log to make it slightly easier to reproduce a random config.
			t.Logf("test case: %#v", tc)

			var zeroDuration time.Duration
			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(t, sql.TTLTestingKnobs{
				AOSTDuration: &zeroDuration,
				OnStatisticsError: func(err error) {
					require.NoError(t, err, "error gathering statistics")
				},
			})
			defer cleanupFunc()

			rangeBatchSize := 1 + rng.Intn(3)
			t.Logf("range batch size: %d", rangeBatchSize)

			th.sqlDB.Exec(t, tc.createTable)
			th.sqlDB.Exec(t, `SET CLUSTER SETTING sql.ttl.range_batch_size = $1`, rangeBatchSize)

			// Extract the columns from CREATE TABLE.
			stmt, err := parser.ParseOne(tc.createTable)
			require.NoError(t, err)
			createTableStmt, ok := stmt.AST.(*tree.CreateTable)
			require.True(t, ok)

			addRow := func(ts time.Time) {
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

			tbDesc := desctestutils.TestingGetPublicTableDescriptor(
				th.kvDB,
				keys.SystemSQLCodec,
				"defaultdb",
				createTableStmt.Table.Table(),
			)
			require.NotNil(t, tbDesc)

			// Split the ranges by a random PK value.
			if tc.numSplits > 0 {
				for i := 0; i < tc.numSplits; i++ {
					var values []interface{}
					var placeholders []string
					for idx := 0; idx < tbDesc.GetPrimaryIndex().NumKeyColumns(); idx++ {
						col, err := tbDesc.FindColumnWithID(tbDesc.GetPrimaryIndex().GetKeyColumnID(idx))
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

			// Add expired and non-expired rows.
			for i := 0; i < tc.numExpiredRows; i++ {
				addRow(timeutil.Now().Add(-time.Hour))
			}
			for i := 0; i < tc.numNonExpiredRows; i++ {
				addRow(timeutil.Now().Add(time.Hour * 24 * 30))
			}

			// Force the schedule to execute.
			th.env.SetTime(timeutil.Now().Add(time.Hour * 24))
			require.NoError(t, th.executeSchedules())

			th.waitForSuccessfulScheduledJob(t)

			// Check we have the number of expected rows.
			var numRows int
			th.sqlDB.QueryRow(
				t,
				fmt.Sprintf(`SELECT count(1) FROM %s`, createTableStmt.Table.Table()),
			).Scan(&numRows)
			require.Equal(t, tc.numNonExpiredRows, numRows)

			// Also check all the rows expire way into the future.
			th.sqlDB.QueryRow(
				t,
				fmt.Sprintf(`SELECT count(1) FROM %s WHERE crdb_internal_expiration >= now()`, createTableStmt.Table.Table()),
			).Scan(&numRows)
			require.Equal(t, tc.numNonExpiredRows, numRows)
		})
	}
}
