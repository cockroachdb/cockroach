// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

	typeSchemaChangeFunc := func(t *testing.T, modeFor1stStmt, modeFor2ndStmt sessiondatapb.NewSchemaChangerMode) {
		ctx, cancel := context.WithCancel(context.Background())
		addTypeStartedChan := make(chan struct{})
		resumeAddTypeJobChan := make(chan struct{})
		var closeAddTypeValueChanOnce sync.Once
		schemaChangeWaitCompletedChan := make(chan struct{})

		var params base.TestServerArgs
		params.Knobs = base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				// If the blocked schema changer is from legacy schema changer, we let
				// it hijack this knob (which is originally design for declarative
				// schema changer) if `stmt` is nil.
				WhileWaitingForConcurrentSchemaChanges: func(stmts []string) {
					if (len(stmts) == 1 && strings.Contains(stmts[0], "DROP TYPE")) ||
						stmts == nil {
						closeAddTypeValueChanOnce.Do(func() {
							close(resumeAddTypeJobChan)
							close(schemaChangeWaitCompletedChan)
						})
					}
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
		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)
		defer cancel()
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		tdb.Exec(t, "CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');")

		// Execute 1st DDL asynchronously and block until it's executing.
		tdb.Exec(t, `SET use_declarative_schema_changer = $1`, modeFor1stStmt.String())
		go func() {
			tdb.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints="typeschemachanger.before.exec"`)
			tdb.ExpectErr(t,
				".*was paused before it completed with reason: pause point \"typeschemachanger.before.exec\" hit",
				`ALTER TYPE status ADD VALUE 'unknown';`)
			close(addTypeStartedChan)
			<-resumeAddTypeJobChan // wait for DROP TYPE to unblock me
			tdb.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints=""`)
			tdb.Exec(t, "RESUME JOB (SELECT job_id FROM crdb_internal.jobs WHERE status='paused' FETCH FIRST 1 ROWS ONLY);\n")
		}()
		<-addTypeStartedChan

		// Execute 2st DDL synchronously. During waiting, it will unblock 1st DDL so
		// it will eventually be able to proceed after waiting for a while.
		tdb.Exec(t, `SET use_declarative_schema_changer = $1`, modeFor2ndStmt.String())
		tdb.Exec(t, `DROP TYPE STATUS;`)
		// After completion make sure we actually waited for schema changes.
		<-schemaChangeWaitCompletedChan
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

	t.Run("typedesc legacy-then-declarative", func(t *testing.T) {
		typeSchemaChangeFunc(t, sessiondatapb.UseNewSchemaChangerOff, sessiondatapb.UseNewSchemaChangerUnsafeAlways)
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

// TestConcurrentSchemaChanges is an integration style tests where we issue many
// schema changes concurrently (drops, renames, add/drop columns, and create/drop
// indexes) for a period of time and assert that they all successfully finish
// eventually. This test will also intentionally toggle different schema changer
// modes.
func TestConcurrentSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t, "this test is long running (>3 mins).")
	skip.UnderDuress(t, "test is already integration style and long running")

	const testDuration = 3 * time.Minute
	const renameDBInterval = 5 * time.Second
	const renameSCInterval = 4 * time.Second
	const renameTblInterval = 3 * time.Second
	const addColInterval = 1 * time.Second
	const dropColInterval = 1 * time.Second
	const createIdxInterval = 1 * time.Second
	const dropIdxInterval = 1 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	g := ctxgroup.WithContext(ctx)

	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, setupConn, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	dbName, scName, tblName := "testdb", "testsc", "t"
	useLegacyOrDeclarative := func(sqlDB *gosql.DB) error {
		decl := rand.Intn(2) == 0
		if !decl {
			_, err := sqlDB.Exec("SET use_declarative_schema_changer='off';")
			return err
		}
		_, err := sqlDB.Exec("SET use_declarative_schema_changer='on';")
		return err
	}

	createSchema := func(conn *gosql.DB) error {
		return testutils.SucceedsSoonError(func() error {
			_, err := conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v;", dbName))
			if err != nil {
				return err
			}
			_, err = conn.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v.%v;", dbName, scName))
			if err != nil {
				return err
			}
			_, err = conn.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.%v.%v(col INT PRIMARY KEY);", dbName, scName, tblName))
			if err != nil {
				return err
			}
			_, err = conn.Exec(fmt.Sprintf("DELETE FROM %v.%v.%v;", dbName, scName, tblName))
			if err != nil {
				return err
			}
			_, err = conn.Exec(fmt.Sprintf("INSERT INTO %v.%v.%v SELECT generate_series(1,100);", dbName, scName, tblName))
			if err != nil {
				return err
			}
			return nil
		})
	}
	require.NoError(t, createSchema(setupConn))

	// repeatWorkWithInterval repeats `work` indefinitely every `workInterval` until
	// `ctx` is cancelled.
	repeatWorkWithInterval := func(
		workerName string, workInterval time.Duration, work func(workConn *gosql.DB) error,
	) func(context.Context) error {
		return func(workerCtx context.Context) error {
			workConn := s.SQLConn(t)
			workConn.SetMaxOpenConns(1)
			for {
				jitteredInterval := workInterval * time.Duration(0.8+0.4*rand.Float32())
				select {
				case <-workerCtx.Done():
					t.Logf("%v is signaled to finish work", workerName)
					return nil
				case <-time.After(jitteredInterval):
					if err := work(workConn); err != nil {
						t.Logf("%v encounters error %v; signal to main routine and finish working", workerName, err.Error())
						return err
					}
				}
			}
		}
	}

	var nextObjectID atomic.Int64
	// A goroutine that repeatedly renames database `testdb` randomly.
	g.GoCtx(repeatWorkWithInterval("rename-db-worker", renameDBInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		drop := rand.Intn(2) == 0
		if drop {
			if _, err := workerConn.Exec(fmt.Sprintf("DROP DATABASE %v CASCADE", dbName)); err != nil {
				return err
			}
			t.Logf("DROP DATABASE %v", dbName)
			return createSchema(workerConn)
		}
		newDBName := fmt.Sprintf("testdb_%v", nextObjectID.Add(1))
		if newDBName == dbName {
			return nil
		}
		if _, err := workerConn.Exec(fmt.Sprintf("ALTER DATABASE %v RENAME TO %v", dbName, newDBName)); err != nil {
			return err
		}
		dbName = newDBName
		t.Logf("RENAME DATABASE TO %v", newDBName)
		return nil
	}))

	// A goroutine that renames schema `testdb.testsc` randomly.
	g.GoCtx(repeatWorkWithInterval("rename-schema-worker", renameSCInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		drop := rand.Intn(2) == 0
		newSCName := fmt.Sprintf("testsc_%v", nextObjectID.Add(1))
		if scName == newSCName {
			return nil
		}
		var err error
		if !drop {
			_, err = workerConn.Exec(fmt.Sprintf("ALTER SCHEMA %v.%v RENAME TO %v", dbName, scName, newSCName))
		} else {
			_, err = workerConn.Exec(fmt.Sprintf("DROP SCHEMA %v.%v CASCADE", dbName, scName))
		}
		if err == nil {
			if !drop {
				scName = newSCName
				t.Logf("RENAME SCHEMA TO %v", newSCName)
			} else {
				t.Logf("DROP SCHEMA TO %v", scName)
				return createSchema(workerConn)
			}
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema) {
			err = nil // mute those errors as they're expected
			t.Logf("Parent database is renamed; skipping this schema renaming.")
		}
		return err
	}))

	// A goroutine that renames table `testdb.testsc.t` randomly.
	g.GoCtx(repeatWorkWithInterval("rename-tbl-worker", renameTblInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		newTblName := fmt.Sprintf("t_%v", nextObjectID.Add(1))
		drop := rand.Intn(2) == 0
		var err error
		if !drop {
			_, err = workerConn.Exec(fmt.Sprintf(`ALTER TABLE %v.%v.%v RENAME TO %v`, dbName, scName, tblName, newTblName))
		} else {
			_, err = workerConn.Exec(fmt.Sprintf(`DROP TABLE %v.%v.%v`, dbName, scName, tblName))
		}
		if err == nil {
			if !drop {
				tblName = newTblName
				t.Logf("RENAME TABLE TO %v", newTblName)
			} else {
				t.Logf("DROP TABLE %v", newTblName)
				return createSchema(workerConn)
			}
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema, pgcode.InvalidSchemaName, pgcode.UndefinedObject, pgcode.UndefinedTable) {
			err = nil
			t.Logf("Parent database or schema is renamed; skipping this table renaming.")
		}
		return err
	}))

	// A goroutine that adds columns to `testdb.testsc.t` randomly.
	g.GoCtx(repeatWorkWithInterval("add-column-worker", addColInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		dbName, scName, tblName := dbName, scName, tblName
		newColName := fmt.Sprintf("col_%v", nextObjectID.Add(1))

		_, err := workerConn.Exec(fmt.Sprintf("ALTER TABLE %v.%v.%v ADD COLUMN %v INT DEFAULT %v",
			dbName, scName, tblName, newColName, rand.Intn(100)))
		if err == nil {
			t.Logf("ADD COLUMN %v TO %v.%v.%v", newColName, dbName, scName, tblName)
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema,
			pgcode.InvalidSchemaName, pgcode.UndefinedTable, pgcode.DuplicateColumn) {
			err = nil
			t.Logf("Parent database or schema or table is renamed or column already exists; skipping this column addition.")
		}
		return err
	}))

	// A goroutine that drops columns from `testdb.testsc.t` randomly.
	g.GoCtx(repeatWorkWithInterval("drop-column-worker", dropColInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		// Randomly pick a non-PK column to drop.
		dbName, scName, tblName := dbName, scName, tblName
		colName, err := getANonPrimaryKeyColumn(workerConn, dbName, scName, tblName)
		if err != nil || colName == "" {
			return err
		}

		_, err = workerConn.Exec(fmt.Sprintf("ALTER TABLE %v.%v.%v DROP COLUMN %v;",
			dbName, scName, tblName, colName))
		if err == nil {
			t.Logf("DROP COLUMN %v FROM %v.%v.%v", colName, dbName, scName, tblName)
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema,
			pgcode.InvalidSchemaName, pgcode.UndefinedTable, pgcode.UndefinedColumn, pgcode.ObjectNotInPrerequisiteState) {
			err = nil
			t.Logf("Parent database or schema or table is renamed; skipping this column removal.")
		}
		return err
	}))

	// A goroutine that creates secondary index on a randomly selected column.
	g.GoCtx(repeatWorkWithInterval("create-index-worker", createIdxInterval, func(workerConn *gosql.DB) error {
		newIndexName := fmt.Sprintf("idx_%v", nextObjectID.Add(1))

		// Randomly pick a non-PK column to create an index on.
		dbName, scName, tblName := dbName, scName, tblName
		colName, err := getANonPrimaryKeyColumn(workerConn, dbName, scName, tblName)
		if err != nil || colName == "" {
			return err
		}

		_, err = workerConn.Exec(fmt.Sprintf("CREATE INDEX %v ON %v.%v.%v (%v);",
			newIndexName, dbName, scName, tblName, colName))
		if err == nil {
			t.Logf("CREATE INDEX %v ON %v.%v.%v(%v)", newIndexName, dbName, scName, tblName, colName)
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema,
			pgcode.InvalidSchemaName, pgcode.UndefinedTable, pgcode.UndefinedColumn, pgcode.DuplicateRelation) ||
			testutils.IsError(err, catalog.ErrDescriptorDropped.Error()) {
			// Besides the potential name changes, it's possible this column has been
			// dropped by the drop-column-worker or the secondary index name already
			// exists.
			err = nil
			t.Logf("Parent database or schema or table is renamed or column is dropped or index already exists; skipping this index creation.")
		}
		return err
	}))

	// A goroutine that drops a secondary index randomly.
	g.GoCtx(repeatWorkWithInterval("drop-index-worker", dropIdxInterval, func(workerConn *gosql.DB) error {
		if err := useLegacyOrDeclarative(workerConn); err != nil {
			return err
		}
		// Randomly pick a public, secondary index to drop.
		dbName, scName, tblName := dbName, scName, tblName
		indexName, err := getASecondaryIndex(workerConn, dbName, scName, tblName)
		if err != nil || indexName == "" {
			return err
		}
		_, err = workerConn.Exec(fmt.Sprintf("DROP INDEX %v.%v.%v@%v;", dbName, scName, tblName, indexName))
		if err == nil {
			t.Logf("DROP INDEX %v FROM %v.%v.%v", indexName, dbName, scName, tblName)
		} else if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema,
			pgcode.InvalidSchemaName, pgcode.UndefinedTable, pgcode.UndefinedObject) {
			// Besides the potential name changes, it's possible that the index no
			// longer exists if the drop-column-worker attempts to drop the column
			// this index keyed on, so, we mute pgcode.UndefinedObject error as well.
			err = nil
			t.Logf("Parent database or schema or table is renamed or index is dropped; skipping this index removal.")
		}
		return err
	}))

	err := g.Wait()
	require.NoError(t, err)
}

// getANonPrimaryKeyColumn returns a non-primary-key column from table `dbName.scName.tblName`.
func getANonPrimaryKeyColumn(workerConn *gosql.DB, dbName, scName, tblName string) (string, error) {
	colNameRow, err := workerConn.Query(fmt.Sprintf(`
SELECT column_name 
FROM [show columns from %s.%s.%s] 
WHERE column_name != 'col'
ORDER BY random();  -- shuffle column output
`, dbName, scName, tblName))
	if err != nil {
		if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema, pgcode.InvalidSchemaName, pgcode.UndefinedTable) {
			return "", nil
		}
		return "", err
	}
	nonPKCols, err := sqlutils.RowsToStrMatrix(colNameRow)
	if err != nil {
		return "", err
	}
	if len(nonPKCols) == 0 {
		return "", nil
	}
	return nonPKCols[0][0], nil
}

// getASecondaryIndex returns a secondary index from table `dbName.scName.tblName`.
func getASecondaryIndex(workerConn *gosql.DB, dbName, scName, tblName string) (string, error) {
	colNameRow, err := workerConn.Query(fmt.Sprintf(`
SELECT index_name 
FROM [show indexes from %s.%s.%s]
WHERE index_name NOT LIKE '%%_pkey'
ORDER BY random();
`, dbName, scName, tblName))
	if err != nil {
		if isPQErrWithCode(err, pgcode.UndefinedDatabase, pgcode.UndefinedSchema, pgcode.InvalidSchemaName, pgcode.UndefinedTable) {
			return "", nil
		}
		return "", err
	}
	nonPKCols, err := sqlutils.RowsToStrMatrix(colNameRow)
	if err != nil {
		return "", err
	}
	if len(nonPKCols) == 0 {
		return "", nil
	}
	return nonPKCols[0][0], nil
}

// IsPQErrWithCode returns true if `err` is a pq error whose code is in `codes`.
func isPQErrWithCode(err error, codes ...pgcode.Code) bool {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		for _, code := range codes {
			if pgcode.MakeCode(string(pqErr.Code)) == code {
				return true
			}
		}
	}
	return false
}

// TestCompareLegacyAndDeclarative tests that when processing a sequence of
// DDL statements, legacy and declarative schema change should transition the
// descriptors into the same final state.
func TestCompareLegacyAndDeclarative(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "too slow under stress race")

	ss := &staticSQLStmtLineProvider{
		stmts: []string{
			// Statements expected to succeed.
			"SET sql_safe_updates = false;",
			"CREATE DATABASE testdb1; SET DATABASE = testdb1",
			"CREATE TABLE testdb1.t1 (i INT PRIMARY KEY); CREATE TABLE testdb1.t2 (i INT PRIMARY KEY REFERENCES testdb1.t1(i));",
			"DROP DATABASE testdb1 CASCADE  -- current db is dropped; expect no post-execution checks",
			"USE defaultdb",
			"CREATE TABLE t2 (i INT PRIMARY KEY, j INT NOT NULL);",
			"CREATE TABLE t1 (i INT PRIMARY KEY, j INT REFERENCES t2(i));",
			"INSERT INTO t2 SELECT k, k+1 FROM generate_series(1,1000) AS tmp(k);",
			"INSERT INTO t1 SELECT k-1, k FROM generate_series(1,1000) AS tmp(k);",
			"CREATE INDEX t1_idx_1 ON t1(j);",
			"CREATE INDEX t2_idx_1 ON t2(j);",
			"ALTER TABLE t1 ADD COLUMN k INT DEFAULT 34;",
			"ALTER TABLE t2 ADD COLUMN p INT DEFAULT 50;",
			"ALTER TABLE t2 DROP COLUMN p;",
			"ALTER TABLE t2 ALTER PRIMARY KEY USING COLUMNS (j);",
			"DROP TABLE IF EXISTS t1, t2;",
			"CREATE TABLE t1 (rowid INT NOT NULL);",
			"ALTER TABLE t1 ALTER PRIMARY KEY USING COLUMNS (rowid); -- special case where column name `rowid` is used",
			"CREATE TABLE t8 (i INT PRIMARY KEY, j STRING);",
			"CREATE INVERTED INDEX ON t8 (j gin_trgm_ops);",

			// Statements expected to fail.
			"CREATE TABLE t1 (); -- expect a DuplicateRelation error",
			"ALTER TABLE t1 DROP COLUMN xyz; -- expect a rejected (sql_safe_updates = true) warning",
			"ALTER TABLE t1 DROP COLUMN xyz; -- expect a UndefinedColumn error",
			"ALTER TABLE txyz ADD COLUMN i INT DEFAULT 30; -- expect a UndefinedTable error",
			"SELECT (*) FROM t1; -- expect a Syntax error",
			"FROM t1 SELECT *; -- ditto",
			"sdfsd  -- ditto",
			"CREATE VIEW v AS (SELECT (*,1) FROM t);  -- ditto",
			"CREATE MATERIALIZED VIEW v AS (xlsd);  -- ditto",
			"CREATE FUNCTION f1() RETURNS INT LANGUAGE SQL AS $$ SELECT $$vsd $$;  -- ditto",
			"CREATE FUNCTION f1() RETURNS INT LANGUAGE SQL AS $funcTag$ SELECT $$vsd $funcTag$;  -- ditto",
			"CREATE TABLE t9 (i INT PRIMARY KEY);",
			"BEGIN;",
			"ALTER TABLE t9 DROP CONSTRAINT t9_pkey;",
			"COMMIT; -- expect a FeatureNotSupported error but should be executed on both clusters",
			"ALTER t9 ADD COLUMN j INT DEFAULT 30;  -- ensure we did not silently skip COMMIT on DSC cluster",

			// Statements with TCL commands or empty content.
			"",
			"BEGIN;",
			"INSERT INTO t2 VALUES (1001, 1002); INSERT INTO t1 VALUES (1000, 1001);",
			"COMMIT;",
			"CREATE TABLE t3 (i INT NOT NULL); INSERT INTO t3 SELECT generate_series(1,1000);",
			"BEGIN; ALTER TABLE t3 ALTER PRIMARY KEY USING COLUMNS (i); INSERT INTO t3 VALUES (1001); COMMIT;",
			"DROP TABLE IF EXISTS t3; CREATE TABLE t3 (i INT NOT NULL); BEGIN;",
			"ALTER TABLE t3 ADD PRIMARY KEY (i);",
			"COMMIT;",
			"BEGIN;",
			"SELECT 1/0;  -- move txn into ERROR state",
			"DROP TABLE IF EXISTS t2;  -- expect to be ignored",
			"INSERT INTO t2 VALUES (1002, 1003); INSERT INTO t1 VALUES (1001, 1002);  -- expect to be ignored",
			"ROLLBACK;",
			"DROP TABLE IF EXISTS t3; CREATE TABLE t3 (i INT PRIMARY KEY);",
			"BEGIN;",
			"ALTER TABLE t3 DROP CONSTRAINT t3_pkey;",
			"DELETE FROM t3 WHERE i = 1;  -- expect to result in an error",
			"ROLLBACK;",
			"BEGIN; ALTER TABLE t3 ADD COLUMN j INT CREATE FAMILY;",
			"ROLLBACK;",
			"BEGIN; SAVEPOINT cockroach_restart;",
			"RELEASE SAVEPOINT cockroach_restart;  -- move txn into DONE state",
			"SELECT 1;  -- expect to be ignored",
			"COMMIT;",
			"CREATE TABLE t11 (i INT NOT NULL); ALTER TABLE t11 ALTER PRIMARY KEY USING COLUMNS (i); -- multi-statement implicit txn",

			// statements that will be altered due to known behavioral differences in LSC vs DSC.
			"ALTER TABLE t1 ADD COLUMN xyz INT DEFAULT 30, ALTER PRIMARY KEY USING COLUMNS (j), DROP COLUMN i; -- unimplemented in legacy schema changer; expect to skip this line",
			"CREATE SEQUENCE s;",
			`CREATE TABLE t4 (i INT CHECK (i > nextval('s')) CHECK (i > 0), CONSTRAINT "ck_i" CHECK (i > nextval('s'::REGCLASS)), CONSTRAINT "ck_i2" CHECK (i > 0)); -- expect to rewrite expressions that reference sequences to just (True)`,
			"ALTER TABLE t4 ADD CHECK (i > nextval('s')); -- ditto",
			"ALTER TABLE t4 ADD COLUMN j INT CHECK (j > 0) CHECK (i+j > nextval('s'));  -- ditto",
			"CREATE TABLE t5 (i INT NOT NULL);",
			"ALTER TABLE t5 ALTER PRIMARY KEY USING COLUMNS (i);",
			"SET use_DEClarative_sChema_changer = off; -- expect to skip this line",
			`SET CLUSTER SETTING sql.scHEma.fOrce_declarative_statements="-CREATE SCHEMA, +CREATE SEQUENCE"  -- expect to skip this line`,
			"CREATE TABLE t6 (i INT PRIMARY KEY, j INT NOT NULL, k INT NOT NULL);",
			"ALTER TABLE t6 DROP COLUMN i; -- unimplemented in legacy schema changer; expect to skip this line",
			"ALTER TABLE t6 ALTER PRIMARY KEY USING COLUMNS (j), DROP COLUMN i; -- ditto",
			"ALTER TABLE t6 ALTER PRIMARY KEY USING COLUMNS (j), DROP COLUMN k; -- ditto",
			"ALTER TABLE t6 ADD COLUMN p INT DEFAULT 30, DROP COLUMN p; -- ditto",
			"SET experimental_enable_temp_tables = true;",
			"CREATE TEMPORARY TABLE t7 (i INT);  -- expect to skip this line",
			"CREATE TEMP TABLE t7 (i INT);  -- ditto",
			"CREATE TABLE t10 (i INT NOT NULL, j INT NOT NULL, k INT NOT NULL, PRIMARY KEY (i, k) USING HASH WITH (bucket_count=3));",
			"INSERT INTO t10 VALUES (0, 1, 2);",
			"ALTER TABLE t10 ALTER PRIMARY KEY USING COLUMNS (i, k) USING HASH;  -- expect to be rewritten to have `DROP COLUMN IF EXISTS old-shard-col` appended to it",
			"ALTER TABLE t10 ALTER PRIMARY KEY USING COLUMNS (i, k); -- ditto",
			"ALTER TABLE t10 ALTER PRIMARY KEY USING COLUMNS (j) USING HASH;  -- expect to not be rewritten because old-shard-col is used",
		},
	}

	sctest.CompareLegacyAndDeclarative(t, ss)
}

func TestSchemaChangerFailsOnMissingDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
			AfterStage: func(p scplan.Plan, stageIdx int) error {
				if p.Params.ExecutionPhase != scop.PostCommitPhase || stageIdx > 1 {
					return nil
				}

				return catalog.ErrDescriptorNotFound
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'off'`)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)
	tdb.Exec(t, `SET use_declarative_schema_changer = 'unsafe'`)
	tdb.ExpectErr(t, "descriptor not found", `ALTER TABLE db.t ADD COLUMN b INT NOT NULL DEFAULT (123)`)
	// Validate the job has hit a terminal state.
	tdb.CheckQueryResults(t, "SELECT status FROM crdb_internal.jobs WHERE statement LIKE '%ADD COLUMN%'",
		[][]string{{"failed"}})
}
