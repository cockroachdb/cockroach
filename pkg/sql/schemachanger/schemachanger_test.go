// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaChangeWaitsForOtherSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("wait for legacy schema changes", func(t *testing.T) {
		// This test starts an legacy schema change job (job 1), and then starts
		// another legacy schema change job (job 2) and a declarative schema change
		// job (job 3) while job 1 is backfilling. Job 1 is resumed after job 2
		// has started running.
		ctx := context.Background()

		var job1Backfill sync.Once
		var job2Resume sync.Once
		var job3Wait sync.Once
		// Closed when we enter the RunBeforeBackfill knob of job 1.
		job1BackfillNotification := make(chan struct{})
		// Closed when we're ready to continue with job 1.
		job1ContinueNotification := make(chan struct{})
		// Closed when job 2 starts.
		job2ResumeNotification := make(chan struct{})
		// Closed when job 3 starts waiting for concurrent schema changes to finish.
		job3WaitNotification := make(chan struct{})
		var job1ID jobspb.JobID

		var s serverutils.TestServerInterface
		var kvDB *kv.DB
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					// Only block in job 2.
					if job1ID == 0 || jobID == job1ID {
						job1ID = jobID
						return nil
					}
					job2Resume.Do(func() {
						close(job2ResumeNotification)
					})
					return nil
				},
				RunBeforeBackfill: func() error {
					job1Backfill.Do(func() {
						close(job1BackfillNotification)
						<-job1ContinueNotification
					})
					return nil
				},
			},
			SQLDeclarativeSchemaChanger: &scrun.TestingKnobs{
				BeforeStage: func(p scplan.Plan, idx int) error {
					// Assert that when job 3 is running, there are no mutations other
					// than the ones associated with this schema change.
					if p.Params.ExecutionPhase < scop.PostCommitPhase {
						return nil
					}
					table := desctestutils.TestingGetTableDescriptor(
						kvDB, keys.SystemSQLCodec, "db", "public", "t")
					// There are 2 schema changes that should precede job 3.
					// The declarative schema changer uses the same mutation ID for all
					// its mutations.
					for _, m := range table.AllMutations() {
						assert.Equal(t, int(m.MutationID()), 3)
					}
					return nil
				},
				BeforeWaitingForConcurrentSchemaChanges: func(_ []string) {
					job3Wait.Do(func() {
						close(job3WaitNotification)
					})
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}
		var sqlDB *gosql.DB
		s, sqlDB, kvDB = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)

		g := ctxgroup.WithContext(ctx)

		// Start job 1: An index schema change, which does not use the new schema
		// changer.
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.ExecContext(ctx, `CREATE INDEX idx ON db.t(a)`)
			assert.NoError(t, err)
			return nil
		})

		<-job1BackfillNotification

		// Start job 3: A column schema change which uses the new schema changer.
		// The transaction will not actually commit until job 1 has finished.
		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1`)
			assert.NoError(t, err)
			return nil
		})

		<-job3WaitNotification

		// Start job 2: Another index schema change which does not use the new
		// schema changer.
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.ExecContext(ctx, `CREATE INDEX idx2 ON db.t(a)`)
			assert.NoError(t, err)
			return nil
		})

		// Wait for job 2 to start.
		<-job2ResumeNotification

		// Finally, let job 1 finish, which will unblock the
		// others.
		close(job1ContinueNotification)
		require.NoError(t, g.Wait())

		// Check that job 3 was created last.
		tdb.CheckQueryResults(t,
			fmt.Sprintf(`SELECT job_type, status, description FROM crdb_internal.jobs WHERE job_type = '%s' OR job_type = '%s' ORDER BY created`,
				jobspb.TypeSchemaChange.String(), jobspb.TypeNewSchemaChange.String(),
			),
			[][]string{
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded), `CREATE INDEX idx ON db.public.t (a)`},
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded), `CREATE INDEX idx2 ON db.public.t (a)`},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded), `schema change job`},
			},
		)
	})

	t.Run("wait for declarative schema changes", func(t *testing.T) {
		// This test starts a declarative schema change job (job 1), and then starts
		// another declarative schema change job (job 2) while job 1 is backfilling.
		ctx := context.Background()

		var job1Backfill sync.Once
		var job2Wait sync.Once
		// Closed when we enter the RunBeforeBackfill knob of job 1.
		job1BackfillNotification := make(chan struct{})
		// Closed when we're ready to continue with job 1.
		job1ContinueNotification := make(chan struct{})
		// Closed when job 2 starts waiting for concurrent schema changes to finish.
		job2WaitNotification := make(chan struct{})

		stmt1 := `ALTER TABLE db.t ADD COLUMN b INT8 DEFAULT 1`
		stmt2 := `ALTER TABLE db.t ADD COLUMN c INT8 DEFAULT 2`

		var kvDB *kv.DB
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scrun.TestingKnobs{
				BeforeStage: func(p scplan.Plan, idx int) error {
					// Verify that we never queue mutations for job 2 before finishing job
					// 1.
					if p.Params.ExecutionPhase < scop.PostCommitPhase {
						return nil
					}
					table := desctestutils.TestingGetTableDescriptor(
						kvDB, keys.SystemSQLCodec, "db", "public", "t")
					mutations := table.AllMutations()
					if len(mutations) == 0 {
						t.Errorf("unexpected empty mutations")
						return errors.Errorf("test failure")
					}
					var idsSeen []descpb.MutationID
					for _, m := range mutations {
						if len(idsSeen) == 0 || m.MutationID() > idsSeen[len(idsSeen)-1] {
							idsSeen = append(idsSeen, m.MutationID())
						}
					}
					highestID := idsSeen[len(idsSeen)-1]
					assert.Truef(t, highestID <= 1, "unexpected mutation IDs %v", idsSeen)
					// Block job 1 during the backfill.
					s := p.Stages[idx]
					stmt := p.TargetState.Statements[0].Statement
					if stmt != stmt1 || s.Type() != scop.BackfillType {
						return nil
					}
					for _, op := range s.EdgeOps {
						if backfillOp, ok := op.(*scop.BackfillIndex); ok && backfillOp.IndexID == descpb.IndexID(2) {
							job1Backfill.Do(func() {
								close(job1BackfillNotification)
								<-job1ContinueNotification
							})
						}
					}

					return nil
				},
				BeforeWaitingForConcurrentSchemaChanges: func(stmts []string) {
					if stmts[0] != stmt2 {
						return
					}
					job2Wait.Do(func() {
						close(job2WaitNotification)
					})
				},
			},
		}

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, kvDB = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)

		g := ctxgroup.WithContext(ctx)

		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, stmt1)
			assert.NoError(t, err)
			return nil
		})

		<-job1BackfillNotification

		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, stmt2)
			assert.NoError(t, err)
			return nil
		})

		<-job2WaitNotification
		close(job1ContinueNotification)
		require.NoError(t, g.Wait())

		tdb.CheckQueryResults(t,
			fmt.Sprintf(`SELECT job_type, status FROM crdb_internal.jobs WHERE job_type = '%s' OR job_type = '%s' ORDER BY created`,
				jobspb.TypeSchemaChange.String(), jobspb.TypeNewSchemaChange.String(),
			),
			[][]string{
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
			},
		)
	})
}

func TestConcurrentOldSchemaChangesCannotStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var doOnce sync.Once
	// Closed when we enter the RunBeforeBackfill knob.
	beforeBackfillNotification := make(chan struct{})
	// Closed when we're ready to continue with the schema change.
	continueNotification := make(chan struct{})

	var kvDB *kv.DB
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				// Assert that old schema change jobs never run in this test.
				t.Errorf("unexpected old schema change job %d", jobID)
				return nil
			},
		},
		SQLDeclarativeSchemaChanger: &scrun.TestingKnobs{
			BeforeStage: func(p scplan.Plan, idx int) error {
				// Verify that we never get a mutation ID not associated with the schema
				// change that is running.
				if p.Params.ExecutionPhase < scop.PostCommitPhase {
					return nil
				}
				table := desctestutils.TestingGetTableDescriptor(
					kvDB, keys.SystemSQLCodec, "db", "public", "t")
				for _, m := range table.AllMutations() {
					assert.LessOrEqual(t, int(m.MutationID()), 2)
				}
				s := p.Stages[idx]
				if s.Type() != scop.BackfillType {
					return nil
				}
				for _, op := range s.EdgeOps {
					if _, ok := op.(*scop.BackfillIndex); ok {
						doOnce.Do(func() {
							close(beforeBackfillNotification)
							<-continueNotification
						})
					}
				}
				return nil
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	var s serverutils.TestServerInterface
	var sqlDB *gosql.DB
	s, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)

	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
		assert.NoError(t, err)
		_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1`)
		assert.NoError(t, err)
		return nil
	})

	<-beforeBackfillNotification

	{
		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)

		_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'off'`)
		require.NoError(t, err)
		for _, stmt := range []string{
			`ALTER TABLE db.t ADD COLUMN c INT DEFAULT 2`,
			`CREATE INDEX ON db.t(a)`,
			`ALTER TABLE db.t RENAME COLUMN a TO c`,
			`CREATE TABLE db.t2 (i INT PRIMARY KEY, a INT REFERENCES db.t)`,
			`CREATE VIEW db.v AS SELECT a FROM db.t`,
			`ALTER TABLE db.t RENAME TO db.new`,
			`GRANT ALL ON db.t TO root`,
			`TRUNCATE TABLE db.t`,
			`DROP TABLE db.t`,
		} {
			_, err = conn.ExecContext(ctx, stmt)
			assert.Truef(t,
				testutils.IsError(err, `cannot perform a schema change on table "t"`) ||
					testutils.IsError(err, `cannot perform TRUNCATE on "t" which has indexes being dropped`),
				"statement: %s, error: %s", stmt, err,
			)
		}
	}

	close(continueNotification)
	require.NoError(t, g.Wait())
}

func TestInsertDuringAddColumnNotWritingToCurrentPrimaryIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var doOnce sync.Once
	// Closed when we enter the RunBeforeBackfill knob.
	beforeBackfillNotification := make(chan struct{})
	// Closed when we're ready to continue with the schema change.
	continueNotification := make(chan struct{})

	var kvDB *kv.DB
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				// Assert that old schema change jobs never run in this test.
				t.Errorf("unexpected old schema change job %d", jobID)
				return nil
			},
		},
		SQLDeclarativeSchemaChanger: &scrun.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				// Verify that we never get a mutation ID not associated with the schema
				// change that is running.
				if p.Params.ExecutionPhase < scop.PostCommitPhase {
					return nil
				}
				table := desctestutils.TestingGetTableDescriptor(
					kvDB, keys.SystemSQLCodec, "db", "public", "t")
				for _, m := range table.AllMutations() {
					assert.LessOrEqual(t, int(m.MutationID()), 2)
				}
				s := p.Stages[stageIdx]
				if s.Type() != scop.BackfillType {
					return nil
				}
				for _, op := range s.EdgeOps {
					if _, ok := op.(*scop.BackfillIndex); ok {
						doOnce.Do(func() {
							close(beforeBackfillNotification)
							<-continueNotification
						})
					}
				}
				return nil
			},
		},
	}

	var s serverutils.TestServerInterface
	var sqlDB *gosql.DB
	s, sqlDB, kvDB = serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "t")

	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
		assert.NoError(t, err)
		_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 100`)
		assert.NoError(t, err)
		return nil
	})

	<-beforeBackfillNotification

	// At this point the backfill operation is paused as it's about to begin.
	// The new column `b` is not yet public, so a concurrent insert should:
	// - in the current primary index, only insert a value for `a`,
	// - in the new secondary index, which will be the future primary index,
	//   insert a value both for `a` and the default value for `b`, because that
	//   new index is delete-and-write-only as it is being backfilled.
	tdb.Exec(t, `
		SET tracing = on,kv;
		INSERT INTO db.t (a) VALUES (10);
		SET tracing = off;`)

	// Trigger the resumption and conclusion of the backfill,
	// and hence of the ADD COLUMN transaction.
	close(continueNotification)
	require.NoError(t, g.Wait())

	// Check that the expectations set out above are verified.
	results := tdb.QueryStr(t, `
		SELECT message
		FROM [SHOW KV TRACE FOR SESSION]
		WHERE message LIKE 'CPut %' OR message LIKE 'InitPut %'`)
	require.GreaterOrEqual(t, len(results), 2)
	require.Equal(t, fmt.Sprintf("CPut /Table/%d/1/10/0 -> /TUPLE/", desc.GetID()), results[0][0])
	require.Equal(t, fmt.Sprintf("InitPut /Table/%d/2/10/0 -> /TUPLE/2:2:Int/100", desc.GetID()), results[1][0])
}

// TestDropJobCancelable ensure that certain operations like
// drops are not cancelable for simple operations.
func TestDropJobCancelable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc       string
		query      string
		cancelable bool
	}{
		{
			"simple drop sequence",
			"BEGIN;DROP SEQUENCE db.sq1; END;",
			false,
		},
		{
			"simple drop view",
			"BEGIN;DROP VIEW db.v1; END;",
			false,
		},
		{
			"simple drop table",
			"BEGIN;DROP TABLE db.t1 CASCADE; END;",
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			params, _ := tests.CreateTestServerParams()

			// Wait groups for synchronizing various parts of the test.
			var schemaChangeStarted sync.WaitGroup
			schemaChangeStarted.Add(1)
			var blockSchemaChange sync.WaitGroup
			blockSchemaChange.Add(1)
			var finishedSchemaChange sync.WaitGroup
			finishedSchemaChange.Add(1)
			// Atomic for checking if job control hook
			// was enabled.
			jobControlHookEnabled := uint64(0)

			params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
			params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					if atomic.SwapUint64(&jobControlHookEnabled, 0) == 1 {
						schemaChangeStarted.Done()
						blockSchemaChange.Wait()
					}
					return nil
				},
			}

			s, sqlDB, _ := serverutils.StartServer(t, params)

			ctx := context.Background()
			defer s.Stopper().Stop(ctx)

			// Setup.
			_, err := sqlDB.Exec(`
CREATE DATABASE db;
CREATE TABLE db.t1 (name VARCHAR(256));
CREATE TABLE db.t2 (name VARCHAR(256));
CREATE VIEW db.v1 AS (SELECT a.name as name2, b.name FROM db.t1 as a, db.t2 as b);
CREATE SEQUENCE db.sq1;
`)
			require.NoError(t, err)

			go func() {
				atomic.StoreUint64(&jobControlHookEnabled, 1)
				_, err := sqlDB.Exec(tc.query)
				if tc.cancelable && !testutils.IsError(err, "job canceled by user") {
					t.Errorf("expected user to have canceled job, got %v", err)
				}
				if !tc.cancelable && err != nil {
					t.Error(err)
				}
				finishedSchemaChange.Done()
			}()

			schemaChangeStarted.Wait()
			rows, err := sqlDB.Query(`
SELECT job_id FROM [SHOW JOBS]
WHERE 
	job_type = 'SCHEMA CHANGE' AND 
	status = $1`, jobs.StatusRunning)
			if err != nil {
				t.Fatalf("unexpected error querying rows %s", err)
			}
			for rows.Next() {
				jobID := ""
				err := rows.Scan(&jobID)
				if err != nil {
					t.Fatalf("unexpected error fetching job ID %s", err)
				}
				_, err = sqlDB.Exec(`CANCEL JOB $1`, jobID)
				if !tc.cancelable && !testutils.IsError(err, "not cancelable") {
					t.Fatalf("expected schema change job to be not cancelable; found %v ", err)
				} else if tc.cancelable && err != nil {
					t.Fatal(err)
				}
			}
			blockSchemaChange.Done()
			finishedSchemaChange.Wait()
		})
	}
}

// TODO (Chengxiong): Remove this version gating test in 22.2
func TestNewSchemaChangerVersionGating(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("new_schema_changer_version_enabled", func(t *testing.T) {
		params, _ := tests.CreateTestServerParams()
		// Override binary version to be older.
		params.Knobs.Server = &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.EnableDeclarativeSchemaChanger),
		}

		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY);`)

		results := tdb.QueryStr(t, "EXPLAIN (DDL) DROP TABLE db.t;")
		require.Equal(t, len(results), 1)
		require.Equal(t, len(results[0]), 1)
		require.Contains(t, results[0][0], "https://cockroachdb.github.io/scplan/viz.html")
	})

	t.Run("new_schema_changer_version_disabled", func(t *testing.T) {
		params, _ := tests.CreateTestServerParams()
		// Override binary version to be older.
		params.Knobs.Server = &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.EnableDeclarativeSchemaChanger - 1),
		}

		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY);`)

		_, err := sqlDB.Query(`EXPLAIN (DDL) DROP TABLE db.t;`)
		require.Error(t, err)
		require.Equal(
			t,
			"pq: cannot explain a statement which is not supported by the declarative schema changer",
			err.Error(),
		)
	})
}
