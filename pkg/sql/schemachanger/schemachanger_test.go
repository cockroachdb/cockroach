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
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaChangerJobRunningStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var runningStatus0, runningStatus1 atomic.Value
	var jr *jobs.Registry
	var params base.TestServerArgs
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
	jr = s.ApplicationLayer().JobRegistry().(*jobs.Registry)

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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var jobIDValue int64
	var params base.TestServerArgs
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var doOnce sync.Once
	// Closed when we enter the RunBeforeBackfill knob.
	beforeBackfillNotification := make(chan struct{})
	// Closed when we're ready to continue with the schema change.
	continueNotification := make(chan struct{})

	var getTableDescriptor func() catalog.TableDescriptor
	var params base.TestServerArgs
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
				table := getTableDescriptor()
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

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tenantID := serverutils.TestTenantID().ToUint64()
	getTableDescriptor = func() catalog.TableDescriptor {
		return desctestutils.TestingGetPublicTableDescriptor(kvDB, s.ApplicationLayer().Codec(), "db", "t")
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	desc := getTableDescriptor()

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
	matched, err := regexp.MatchString(
		fmt.Sprintf("CPut (?:/Tenant/%d)?/Table/%d/1/10/0 -> /TUPLE/",
			tenantID, desc.GetID()),
		results[0][0])
	require.NoError(t, err)
	require.True(t, matched)

	// The write to the temporary index is wrapped for the delete-preserving
	// encoding. We need to unwrap it to verify its data. To do this, we pull
	// the hex-encoded wrapped data, decode it, then pretty-print it to ensure
	// it looks right.
	wrappedPutRE := regexp.MustCompile(fmt.Sprintf(
		"Put (?:/Tenant/%d)?/Table/%d/3/10/0 -> /BYTES/0x([0-9a-f]+)$", tenantID, desc.GetID(),
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

			var params base.TestServerArgs
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

// TestSchemaChangeWaitsForConcurrentSchemaChanges tests that if a schema
// change on a table is issued when there is already an ongoing schema change
// on that table, it will wait until that ongoing schema change finishes before
// proceeding.
func TestSchemaChangeWaitsForConcurrentSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tf := func(t *testing.T, modeFor1stStmt, modeFor2ndStmt sessiondatapb.NewSchemaChangerMode) {
		ctx, cancel := context.WithCancel(context.Background())
		createIndexChan := make(chan struct{})
		addColChan := make(chan struct{})
		var closeMainChanOnce, closeAlterPKChanOnce sync.Once

		var params base.TestServerArgs
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				// If the blocked schema changer is from legacy schema changer, we let
				// it hijack this knob (which is originally design for declarative
				// schema changer) if `stmt` is nil.
				WhileWaitingForConcurrentSchemaChanges: func(stmts []string) {
					if (len(stmts) == 1 && strings.Contains(stmts[0], "ADD COLUMN")) ||
						stmts == nil {
						closeAlterPKChanOnce.Do(func() {
							close(addColChan)
						})
					}
				},
			},
			DistSQL: &execinfra.TestingKnobs{
				RunBeforeBackfillChunk: func(_ roachpb.Span) error {
					closeMainChanOnce.Do(func() {
						close(createIndexChan)
					})
					<-addColChan // wait for AddCol to unblock me
					return nil
				},
			},
			// Decrease the adopt loop interval so that retries happen quickly.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			// Prevent the GC job from running so we ensure that all the keys which
			// were written remain.
			GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(jobID jobspb.JobID) error {
				<-ctx.Done()
				return ctx.Err()
			}},
		}
		s, sqlDB, kvDB := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)
		defer cancel()
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY, j INT NOT NULL);")
		tdb.Exec(t, "INSERT INTO t SELECT k, k+1 FROM generate_series(1,1000) AS tmp(k);")

		// Execute 1st DDL asynchronously and block until it's executing.
		tdb.Exec(t, `SET use_declarative_schema_changer = $1`, modeFor1stStmt.String())
		go func() {
			tdb.Exec(t, `CREATE INDEX idx ON t (j);`)
		}()
		<-createIndexChan

		// Execute 2st DDL synchronously. During waiting, it will unblock 1st DDL so
		// it will eventually be able to proceed after waiting for a while.
		tdb.Exec(t, `SET use_declarative_schema_changer = $1`, modeFor2ndStmt.String())
		tdb.Exec(t, `ALTER TABLE t ADD COLUMN k INT DEFAULT 30;`)

		// There should be 2 k/v pairs per row:
		// 1. the old primary index (i : j)
		// 2. the new secondary index keyed on j with key suffix on i (j; i : ), from CREATE INDEX
		// Additionally, if ADD COLUMN uses declarative schema changer, there will
		// one 1 more k/v pair for each row:
		// 3. the new primary index (i : j, k), from ADD COLUMN
		expectedKeyCount := 2000
		if modeFor2ndStmt == sessiondatapb.UseNewSchemaChangerUnsafeAlways {
			expectedKeyCount = 3000
		}
		requireTableKeyCount(ctx, t, s.ApplicationLayer().Codec(), kvDB,
			"defaultdb", "t", expectedKeyCount)
	}

	t.Run("declarative-then-declarative", func(t *testing.T) {
		tf(t, sessiondatapb.UseNewSchemaChangerUnsafeAlways, sessiondatapb.UseNewSchemaChangerUnsafeAlways)
	})

	t.Run("declarative-then-legacy", func(t *testing.T) {
		tf(t, sessiondatapb.UseNewSchemaChangerUnsafeAlways, sessiondatapb.UseNewSchemaChangerOff)
	})

	t.Run("legacy-then-declarative", func(t *testing.T) {
		tf(t, sessiondatapb.UseNewSchemaChangerOff, sessiondatapb.UseNewSchemaChangerUnsafeAlways)
	})

	// legacy + legacy case is tested in TestLegacySchemaChangerWaitsForOtherSchemaChanges
	// because the waiting occurred under a different code path.
}

// requireTableKeyCount ensures that `db`.`tbl` has `keyCount` kv-pairs in it.
func requireTableKeyCount(
	ctx context.Context,
	t *testing.T,
	codec keys.SQLCodec,
	kvDB *kv.DB,
	db string,
	tbl string,
	keyCount int,
) {
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, db, tbl)
	tablePrefix := codec.TablePrefix(uint32(tableDesc.GetID()))
	tableEnd := tablePrefix.PrefixEnd()
	kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0)
	require.NoError(t, err)
	require.Equal(t, keyCount, len(kvs))
}
