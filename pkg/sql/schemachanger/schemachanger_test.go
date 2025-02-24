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
					runningStatus0.Store(job.Progress().StatusMessage)
				case 1:
					runningStatus1.Store(job.Progress().StatusMessage)
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
	results := tdb.QueryStr(t, `SELECT encode(payload, 'hex') FROM crdb_internal.system_jobs WHERE id = $1`, jobID)
	require.Len(t, results, 1)
	b, err := hex.DecodeString(results[0][0])
	require.NoError(t, err)
	var p jobspb.Payload
	err = protoutil.Unmarshal(b, &p)
	require.NoError(t, err)
	checkErrWithDetails(p.FinalResumeError)

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
	require.Equal(t, 1, len(results))
	require.Equal(t, 1, len(results[0]))
	require.Regexp(t, `^boom`, results[0][0])
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
	status = $1`, jobs.StateRunning)
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

// TestCreateObjectConcurrency validates that concurrent create object with
// independent references never hit txn retry errors. All objects are created
// under the same schema.
func TestCreateObjectConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Validate concurrency behaviour for objects under a schema
	tests := []struct {
		name       string
		setupStmt  string
		firstStmt  string
		secondStmt string
	}{
		{
			name: "create table with function references",
			setupStmt: `
CREATE FUNCTION public.fn1 (input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT input::INT8;
                                $$;
CREATE FUNCTION public.wrap(input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT public.fn1(input);
                                $$;
CREATE FUNCTION public.wrap2(input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT public.fn1(input);
                                $$;
`,
			firstStmt:  "CREATE TABLE t1(n int default public.wrap(10))",
			secondStmt: "CREATE TABLE t2(n int default public.wrap2(10))",
		},
		{
			name: "create table with type reference",
			setupStmt: `
CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');
CREATE TYPE status1 AS ENUM ('open', 'closed', 'inactive');
`,
			firstStmt:  "CREATE TABLE t1(n status)",
			secondStmt: "CREATE TABLE t2(n status1)",
		},
		{
			name: "create view with type references",
			setupStmt: `
CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');
CREATE TYPE status1 AS ENUM ('open', 'closed', 'inactive');
CREATE FUNCTION public.fn1 (input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT input::INT8;
                                $$;
CREATE FUNCTION public.wrap(input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT public.fn1(input);
                                $$;
CREATE FUNCTION public.wrap2(input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT public.fn1(input);
                                $$;
CREATE TABLE t1(n int default public.wrap(10));
CREATE TABLE t2(n int default public.wrap2(10));
`,
			// Note: Views cannot invoke UDFs directly yet.
			firstStmt:  "CREATE VIEW v1 AS (SELECT n, 'open'::status FROM public.t1)",
			secondStmt: "CREATE VIEW v2 AS (SELECT n, 'open'::status1 FROM public.t2)",
		},
		{
			name: "create sequence with ownership",
			setupStmt: `
CREATE TABLE t1(n int);
CREATE TABLE t2(n int);
`,
			firstStmt:  "CREATE SEQUENCE sq1 OWNED BY t1.n",
			secondStmt: "CREATE SEQUENCE sq2 OWNED BY t2.n",
		},
		{
			name:       "create type",
			firstStmt:  "CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');",
			secondStmt: "CREATE TYPE status1 AS ENUM ('open', 'closed', 'inactive');",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
				// This would work with secondary tenants as well, but the span config
				// limited logic can hit transaction retries on the span_count table.
				DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(138733),
			})
			defer s.Stopper().Stop(ctx)

			runner := sqlutils.MakeSQLRunner(sqlDB)

			// Ensure we don't commit any DDLs in a transaction.
			runner.Exec(t, `SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'`)
			runner.Exec(t, "SET autocommit_before_ddl = false")

			firstConn, err := sqlDB.Conn(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, firstConn.Close())
			}()
			secondConn, err := sqlDB.Conn(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, secondConn.Close())
			}()

			firstConnReady := make(chan struct{})
			secondConnReady := make(chan struct{})

			runner.Exec(t, test.setupStmt)

			grp := ctxgroup.WithContext(ctx)

			grp.Go(func() error {
				defer close(firstConnReady)
				tx, err := firstConn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				_, err = tx.Exec(test.firstStmt)
				if err != nil {
					return err
				}
				firstConnReady <- struct{}{}
				<-secondConnReady
				return tx.Commit()
			})
			grp.Go(func() error {
				defer close(secondConnReady)
				tx, err := secondConn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				_, err = tx.Exec(test.secondStmt)
				if err != nil {
					return err
				}
				<-firstConnReady
				secondConnReady <- struct{}{}
				return tx.Commit()
			})
			require.NoError(t, grp.Wait())
		})
	}
}
