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
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
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
			_, err := sqlDB.ExecContext(ctx, `SET use_declarative_schema_changer='off'`)
			assert.NoError(t, err)
			_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx ON db.t(a)`)
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
			_, err := sqlDB.ExecContext(ctx, `SET use_declarative_schema_changer='off'`)
			assert.NoError(t, err)
			_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx2 ON db.t(a)`)
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
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded), `ALTER TABLE db.public.t ADD COLUMN b INT8 DEFAULT 1`},
			},
		)
	})

	t.Run("wait for declarative schema changes for tables", func(t *testing.T) {
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
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
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

	t.Run("wait for declarative schema changes for schema", func(t *testing.T) {
		// This test starts a declarative schema change job (job 1), and then starts
		// another declarative schema change job (job 2) involving dropping schemas.
		// Both of these jobs will need to concurrently touch the database descriptor.
		ctx := context.Background()

		var jobWaitForPostCommit sync.Once
		var jobWaitBeforeWait sync.Once
		// Closed when we're ready to continue with job 1.
		job2StartExecution := make(chan struct{})
		job2ContinueNotification := make(chan struct{})
		completionCount := int32(0)

		stmt1 := `DROP SCHEMA db.s1`
		stmt2 := `DROP SCHEMA db.s2`
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					if p.Params.ExecutionPhase != scop.PostCommitPhase {
						return nil
					}
					jobWaitForPostCommit.Do(func() {
						job2StartExecution <- struct{}{}
						job2ContinueNotification <- struct{}{}
					})
					return nil
				},
				BeforeWaitingForConcurrentSchemaChanges: func(stmts []string) {
					if stmts[0] == stmt2 {
						atomic.AddInt32(&completionCount, 1)
						jobWaitBeforeWait.Do(func() {
							<-job2ContinueNotification
						})
					}
				},
			},
		}

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, _ = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE SCHEMA db.s1`)
		tdb.Exec(t, `CREATE SCHEMA db.s2`)

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

		<-job2StartExecution

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

		require.NoError(t, g.Wait())

		tdb.CheckQueryResults(t,
			fmt.Sprintf(`SELECT job_type, status FROM crdb_internal.jobs WHERE job_type = '%s' OR job_type = '%s' ORDER BY created`,
				jobspb.TypeSchemaChange.String(), jobspb.TypeNewSchemaChange.String(),
			),
			[][]string{
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded)},
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded)},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
			},
		)
		// We should observe the schema change was tried at least twice.
		require.GreaterOrEqual(t, atomic.LoadInt32(&completionCount), int32(1))
	})
	t.Run("wait for declarative schema changes for type", func(t *testing.T) {
		// This test starts a declarative schema change job (job 1), and then starts
		// another declarative schema change job (job 2) involving dropping tables.
		// Both of these jobs will need to concurrently touch type descriptors.
		ctx := context.Background()

		var jobWaitForPostCommit sync.Once
		var jobWaitBeforeWait sync.Once
		// Closed when we're ready to continue with job 1.
		job2StartExecution := make(chan struct{})
		job2ContinueNotification := make(chan struct{})
		completionCount := int32(0)

		stmt1 := `DROP TABLE db.t1`
		stmt2 := `DROP TABLE db.t2`
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					if p.Params.ExecutionPhase != scop.PostCommitPhase {
						return nil
					}
					jobWaitForPostCommit.Do(func() {
						job2StartExecution <- struct{}{}
						job2ContinueNotification <- struct{}{}
					})
					return nil
				},
				BeforeWaitingForConcurrentSchemaChanges: func(stmts []string) {
					if stmts[0] == stmt2 {
						atomic.AddInt32(&completionCount, 1)
						jobWaitBeforeWait.Do(func() {
							<-job2ContinueNotification
						})
					}
				},
			},
		}

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, _ = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TYPE db.status AS ENUM ('open', 'closed', 'inactive');`)
		tdb.Exec(t, `CREATE TABLE db.t1(t db.status)`)
		tdb.Exec(t, `CREATE TABLE db.t2(t db.status)`)

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

		<-job2StartExecution

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
		// We should observe the schema change was tried at least twice.
		require.GreaterOrEqual(t, atomic.LoadInt32(&completionCount), int32(1))
	})
}

// TestConcurrentSchemaChangesWait ensures that when a schema change
// is run concurrently with a declarative schema change, that it waits for
// the declarative schema change to complete before proceeding.
//
// Each concurrent schema change is run both as a single statement, where the
// test expects an automatic retry, and as part of an explicit transaction
// which has returned rows, in order to ensure that an error with the proper
// error code is returned.
func TestConcurrentSchemaChangesWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const defaultInitialStmt = `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1`
	type concurrentWaitTest struct {
		// initial statement run under the declarative schema changer, paused on
		// the first post commit phase.
		initial string
		// concurrent statement run under the legacy schema changer
		concurrent string
	}
	ctx := context.Background()
	runConcurrentSchemaChangeCase := func(t *testing.T, stmts concurrentWaitTest, implicit bool) {
		defer log.Scope(t).Close(t)
		var doOnce sync.Once
		// Closed when we enter the BeforeStage knob with a post commit or later
		// phase.
		beforePostCommitNotification := make(chan struct{})
		// Closed when we're ready to continue with the schema change.
		continueNotification := make(chan struct{})
		// Sent on when we're waiting for the initial schema change.
		waitingForConcurrent := make(chan struct{})

		var kvDB *kv.DB
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeWaitingForConcurrentSchemaChanges: func(_ []string) {
					waitingForConcurrent <- struct{}{}
				},
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
					if s.Phase < scop.PostCommitPhase {
						return nil
					}
					doOnce.Do(func() {
						close(beforePostCommitNotification)
						<-continueNotification
					})
					return nil
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		}

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, kvDB = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		initialSchemaChange := func() error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET use_declarative_schema_changer = 'unsafe'`)
			assert.NoError(t, err)
			for _, s := range strings.Split(stmts.initial, ";") {
				_, err = conn.ExecContext(ctx, s)
				assert.NoError(t, err)
			}
			return nil
		}
		concurrentSchemaChangeImplicit := func() error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			for _, s := range append([]string{
				`SET use_declarative_schema_changer = 'off'`,
			}, strings.Split(stmts.concurrent, ";")...) {
				if _, err = conn.ExecContext(ctx, s); err != nil {
					return err
				}
			}
			return nil
		}
		concurrentSchemaChangeExplicit := func() error {
			var sawRestart bool
			defer func() { assert.True(t, sawRestart) }()
			return crdb.Execute(func() (err error) {
				conn, err := sqlDB.Conn(ctx)
				if err != nil {
					return err
				}
				defer func() { _ = conn.Close() }()
				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer func() {
					if err != nil {
						var pqErr *pq.Error
						sawRestart = sawRestart ||
							errors.As(err, &pqErr) &&
								string(pqErr.Code) == pgcode.SerializationFailure.String()
						_ = tx.Rollback()
					}
				}()
				// Execute something first to ensure that a restart is sent.
				if _, err := tx.Exec("SELECT * FROM db.other_t"); err != nil {
					return err
				}
				for _, s := range strings.Split(stmts.concurrent, ";") {
					if _, err := tx.ExecContext(ctx, s); err != nil {
						return err
					}
				}
				return tx.Commit()
			})
		}

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.other_t (a INT PRIMARY KEY)`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
		tdb.Exec(t, `CREATE USER testuser`)
		tdb.Exec(t, `CREATE SCHEMA db.sc`)
		tdb.Exec(t, `ALTER SCHEMA db.sc OWNER to testuser`)
		tdb.Exec(t, `CREATE TABLE db.sc.t (a INT PRIMARY KEY)`)
		tdb.Exec(t, `ALTER TABLE db.sc.t OWNER to testuser`)
		var initialSchemaChangeGroup errgroup.Group
		var concurrentSchemaChangeGroup errgroup.Group
		initialSchemaChangeGroup.Go(initialSchemaChange)
		<-beforePostCommitNotification
		if implicit {
			concurrentSchemaChangeGroup.Go(concurrentSchemaChangeImplicit)
		} else {
			concurrentSchemaChangeGroup.Go(concurrentSchemaChangeExplicit)
		}
		<-waitingForConcurrent
		close(continueNotification)
		require.NoError(t, initialSchemaChangeGroup.Wait())
		require.NoError(t, concurrentSchemaChangeGroup.Wait())
	}

	stmts := []concurrentWaitTest{
		{defaultInitialStmt, `ALTER TABLE db.t ADD COLUMN c INT DEFAULT 2`},
		{defaultInitialStmt, `CREATE INDEX ON db.t(a)`},
		{defaultInitialStmt, `ALTER TABLE db.t RENAME COLUMN a TO c`},
		{defaultInitialStmt, `CREATE TABLE db.t2 (i INT PRIMARY KEY, a INT REFERENCES db.t)`},
		{defaultInitialStmt, `CREATE VIEW db.v AS SELECT a FROM db.t`},
		{defaultInitialStmt, `ALTER TABLE db.t RENAME TO db.new`},
		{defaultInitialStmt, `TRUNCATE TABLE db.t`},
		{defaultInitialStmt, `DROP TABLE db.t`},
		{"USE db; DROP OWNED BY testuser;", `DROP DATABASE db`},
	}
	for i := range stmts {
		stmt := stmts[i] // copy for closure
		t.Run(stmt.concurrent, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "implicit", func(t *testing.T, implicit bool) {
				runConcurrentSchemaChangeCase(t, stmt, implicit)
			})
		})
	}
}

func TestSchemaChangerJobRunningStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var runningStatus0, runningStatus1 atomic.Value
	var jr *jobs.Registry
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
			AfterStage: func(p scplan.Plan, stageIdx int) error {
				if p.Params.ExecutionPhase < scop.PostCommitPhase || stageIdx > 1 {
					return nil
				}
				job, err := jr.LoadJob(ctx, p.JobID)
				require.NoError(t, err)
				switch stageIdx {
				case 0:
					runningStatus0.Store(job.Progress().RunningStatus)
				case 1:
					runningStatus1.Store(job.Progress().RunningStatus)
				}
				return nil
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	jr = s.JobRegistry().(*jobs.Registry)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'off'`)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'unsafe'`)
	tdb.Exec(t, `ALTER TABLE db.t ADD COLUMN b INT NOT NULL DEFAULT (123)`)

	require.NotNil(t, runningStatus0.Load())
	require.Regexp(t, "PostCommit.* pending", runningStatus0.Load().(string))
	require.NotNil(t, runningStatus1.Load())
	require.Regexp(t, "PostCommit.* pending", runningStatus1.Load().(string))
}

func TestSchemaChangerJobErrorDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var jobIDValue int64
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
			AfterStage: func(p scplan.Plan, stageIdx int) error {
				if p.Params.ExecutionPhase == scop.PostCommitPhase && stageIdx == 1 {
					atomic.StoreInt64(&jobIDValue, int64(p.JobID))
					// We need to explicitly decorate the error here.
					// In any case, what we're testing here is that the decoration gets
					// properly serialized inside the job payload.
					return p.DecorateErrorWithPlanDetails(errors.Errorf("boom"))
				}
				return nil
			},
		},
		EventLog:         &sql.EventLogTestingKnobs{SyncWrites: true},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'off'`)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'unsafe'`)
	tdb.ExpectErr(t, `boom`, `ALTER TABLE db.t ADD COLUMN b INT NOT NULL DEFAULT (123)`)
	jobID := jobspb.JobID(atomic.LoadInt64(&jobIDValue))

	// Check that the error is featured in the jobs table.
	results := tdb.QueryStr(t, `SELECT execution_errors FROM crdb_internal.jobs WHERE job_id = $1`, jobID)
	require.Len(t, results, 1)
	require.Regexp(t, `^\{\"reverting execution from .* on 1 failed: boom\"\}$`, results[0][0])

	// Check that the error details are also featured in the jobs table.
	checkErrWithDetails := func(ee *errorspb.EncodedError) {
		require.NotNil(t, ee)
		jobErr := errors.DecodeError(ctx, *ee)
		require.Error(t, jobErr)
		require.Equal(t, "boom", jobErr.Error())
		ed := errors.GetAllDetails(jobErr)
		require.Len(t, ed, 3)
		require.Regexp(t, "^• Schema change plan for .*", ed[0])
		require.Regexp(t, "^stages graphviz: https.*", ed[1])
		require.Regexp(t, "^dependencies graphviz: https.*", ed[2])
	}
	results = tdb.QueryStr(t, `SELECT encode(payload, 'hex') FROM crdb_internal.system_jobs WHERE id = $1`, jobID)
	require.Len(t, results, 1)
	b, err := hex.DecodeString(results[0][0])
	require.NoError(t, err)
	var p jobspb.Payload
	err = protoutil.Unmarshal(b, &p)
	require.NoError(t, err)
	checkErrWithDetails(p.FinalResumeError)
	require.LessOrEqual(t, 1, len(p.RetriableExecutionFailureLog))
	checkErrWithDetails(p.RetriableExecutionFailureLog[0].Error)

	// Check that the error is featured in the event log.
	const eventLogCountQuery = `SELECT count(*) FROM system.eventlog WHERE "eventType" = $1`
	results = tdb.QueryStr(t, eventLogCountQuery, "finish_schema_change")
	require.EqualValues(t, [][]string{{"0"}}, results)
	results = tdb.QueryStr(t, eventLogCountQuery, "finish_schema_change_rollback")
	require.EqualValues(t, [][]string{{"1"}}, results)
	results = tdb.QueryStr(t, eventLogCountQuery, "reverse_schema_change")
	require.EqualValues(t, [][]string{{"1"}}, results)
	const eventLogErrorQuery = `SELECT (info::JSONB)->>'Error' FROM system.eventlog WHERE "eventType" = 'reverse_schema_change'`
	results = tdb.QueryStr(t, eventLogErrorQuery)
	require.EqualValues(t, [][]string{{"boom"}}, results)
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
		SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
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
		WHERE message LIKE 'CPut %' OR message LIKE 'Put %'`)
	require.GreaterOrEqual(t, len(results), 2)
	require.Equal(t, fmt.Sprintf("CPut /Table/%d/1/10/0 -> /TUPLE/", desc.GetID()), results[0][0])

	// The write to the temporary index is wrapped for the delete-preserving
	// encoding. We need to unwrap it to verify its data. To do this, we pull
	// the hex-encoded wrapped data, decode it, then pretty-print it to ensure
	// it looks right.
	wrappedPutRE := regexp.MustCompile(fmt.Sprintf(
		"Put /Table/%d/3/10/0 -> /BYTES/0x([0-9a-f]+)$", desc.GetID(),
	))
	match := wrappedPutRE.FindStringSubmatch(results[1][0])
	require.NotEmpty(t, match)
	var val roachpb.Value
	wrapped, err := hex.DecodeString(match[1])
	require.NoError(t, err)
	val.SetBytes(wrapped)
	wrapper, err := rowenc.DecodeWrapper(&val)
	require.NoError(t, err)
	val.SetTagAndData(wrapper.Value)
	require.Equal(t, "/TUPLE/2:2:Int/100", val.PrettyPrint())
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

			go func(query string, isCancellable bool) {
				atomic.StoreUint64(&jobControlHookEnabled, 1)
				_, err := sqlDB.Exec(query)
				if isCancellable && !testutils.IsError(err, "job canceled by user") {
					t.Errorf("expected user to have canceled job, got %v", err)
				}
				if !isCancellable && err != nil {
					t.Error(err)
				}
				finishedSchemaChange.Done()
			}(tc.query, tc.cancelable)

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
