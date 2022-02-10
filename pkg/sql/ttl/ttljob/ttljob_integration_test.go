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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
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
	executeSchedules func() error
}

func newRowLevelTTLTestJobTestHelper(t *testing.T) (*rowLevelTTLTestJobTestHelper, func()) {
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

	var zeroDuration time.Duration
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: knobs,
			TTL: &sql.TTLTestingKnobs{
				AOSTDuration: &zeroDuration,
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, args)
	require.NotNil(t, th.cfg)
	th.sqlDB = sqlutils.MakeSQLRunner(db)
	th.server = s

	return th, func() {
		s.Stopper().Stop(context.Background())
	}
}

func (h *rowLevelTTLTestJobTestHelper) waitForSuccessfulScheduledJob(t *testing.T) {
	query := fmt.Sprintf(
		`SELECT status, error FROM [SHOW JOBS] 
		WHERE job_id IN (
			SELECT job_id FROM %s
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

		if status != string(jobs.StatusSucceeded) {
			return errors.Newf("expected status %s, got %s (error: %s)", jobs.StatusSucceeded, status, errorStr)
		}
		return nil
	})
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
				numExpiredRows:    rng.Intn(2000),
				numNonExpiredRows: rng.Intn(100),
			},
		)
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Log to make it slightly easier to reproduce a random config.
			t.Logf("test case: %#v", tc)

			th, cleanupFunc := newRowLevelTTLTestJobTestHelper(t)
			defer cleanupFunc()

			th.sqlDB.Exec(t, tc.createTable)

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
