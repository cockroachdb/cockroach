// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTypeSchemaChangeRetriesTransparently tests that a type schema change
// that runs into a non permanent error will retry transparently.
func TestTypeSchemaChangeRetriesTransparently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Protects errorReturned.
	var mu syncutil.Mutex
	errorReturned := false
	params, _ := createTestServerParamsAllowTenants()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			mu.Lock()
			defer mu.Unlock()
			// Return a retryable error on the first call.
			if errorReturned {
				return nil
			}
			errorReturned = true
			return context.DeadlineExceeded
		},
	}
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM();
`); err != nil {
		t.Fatal(err)
	}

	// The retry should happen within the job and succeed.
	if _, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`); err != nil {
		t.Fatal(err)
	}
}

// TestFailedTypeSchemaChangeRetriesTransparently fails the initial type schema
// change operation and then tests that if the cleanup job runs into a
// non-permanent error, it is retried transparently.
func TestFailedTypeSchemaChangeRetriesTransparently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Protects errReturned.
	var mu syncutil.Mutex
	// Ensures just the first try to cleanup returns a retryable error.
	errReturned := false
	params, _ := createTestServerParamsAllowTenants()
	cleanupSuccessfullyFinished := make(chan struct{})
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			return jobs.MarkAsPermanentJobError(errors.New("yikes"))
		},
		RunAfterOnFailOrCancel: func() error {
			mu.Lock()
			defer mu.Unlock()
			if errReturned {
				return nil
			}
			errReturned = true
			close(cleanupSuccessfullyFinished)
			return context.DeadlineExceeded
		},
	}
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	_, err := sqlDB.Exec(`
SET use_declarative_schema_changer = 'off';
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM();
`)
	require.NoError(t, err)

	// The initial drop should fail.
	_, err = sqlDB.Exec(`DROP TYPE d.t`)
	testutils.IsError(err, "yikes")

	// The cleanup job, which is expected to drain names, should retry
	// transparently.
	<-cleanupSuccessfullyFinished

	// type descriptor name + array alias name.
	namespaceEntries := []string{"t", "_t"}
	for _, name := range namespaceEntries {
		rows := sqlDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM system.namespace WHERE name= '%s'`, name))
		var count int
		err = rows.Scan(&count)
		require.NoError(t, err)
		if count != 0 {
			t.Fatalf("expected namespace entries to be cleaned up for type desc %q", name)
		}
	}
}

// TestFailedTypeSchemaChangeIgnoresDrops when a type schema change notices
// a dropped descriptor during a rollback that is treated as a non-retriable
// error.
func TestFailedTypeSchemaChangeIgnoresDrops(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	startDrop := make(chan struct{})
	dropFinished := make(chan struct{})
	cancelled := atomic.Bool{}
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			if cancelled.Swap(true) == false {
				// Kick off a DROP DATABASE job to clean up this descriptor.
				close(startDrop)
				<-dropFinished
				// Fail this schema change so that the rollback logic executes.
				return jobs.MarkAsPermanentJobError(errors.New("yikes"))
			} else {
				return nil
			}
		},
	}
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	grp := ctxgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(dropFinished)
		<-startDrop
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "SET use_declarative_schema_changer = 'off';")
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "DROP DATABASE d CASCADE")
		return err
	})

	// Create a type.
	_, err := sqlDB.Exec(`
SET use_declarative_schema_changer = 'off';
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM('a', 'b', 'c');
`)
	require.NoError(t, err)

	// The initial drop should fail.
	_, err = sqlDB.Exec(`ALTER TYPE d.t DROP VALUE 'c'`)
	testutils.IsError(err, "yikes")

	require.NoError(t, grp.Wait())
}

func TestAddDropValuesInTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := createTestServerParamsAllowTenants()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
USE d;
CREATE TYPE greetings AS ENUM('hi', 'hello', 'yo');
CREATE TABLE use_greetings(k INT PRIMARY KEY, v greetings);
INSERT INTO use_greetings VALUES(1, 'yo');
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		query        string
		errorRe      string
		succeedAfter []string
		failAfter    []string
	}{
		{
			`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE greetings DROP VALUE 'hello'; 
ALTER TYPE greetings ADD VALUE 'howdy'; 
ALTER TYPE greetings DROP VALUE 'yo'; 
COMMIT`,
			"transaction committed but schema change aborted with error",
			[]string{
				`SELECT 'hello'::greetings`,
				`SELECT 'yo'::greetings`,
			},
			[]string{
				`SELECT 'howdy'::greetings`,
			},
		},
		{
			`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE greetings ADD VALUE 'sup'; 
ALTER TYPE greetings ADD VALUE 'howdy'; 
ALTER TYPE greetings DROP VALUE 'yo'; 
COMMIT`,
			"transaction committed but schema change aborted with error",
			[]string{
				`SELECT 'yo'::greetings`,
			},
			[]string{
				`SELECT 'sup'::greetings`,
				`SELECT 'howdy'::greetings`,
			},
		},
		{
			`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE greetings DROP VALUE 'hi'; 
ALTER TYPE greetings DROP VALUE 'hello'; 
ALTER TYPE greetings DROP VALUE 'yo'; 
COMMIT`,
			"transaction committed but schema change aborted with error",
			[]string{
				`SELECT 'hi'::greetings`,
				`SELECT 'hello'::greetings`,
				`SELECT 'yo'::greetings`,
			},
			nil,
		},
		{
			`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE greetings ADD VALUE 'sup'; 
ALTER TYPE greetings ADD VALUE 'howdy'; 
ALTER TYPE greetings DROP VALUE 'hello'; 
COMMIT`,
			"",
			[]string{
				`SELECT 'sup'::greetings`,
				`SELECT 'howdy'::greetings`,
			},
			[]string{
				`SELECT 'hello'::greetings`,
			},
		},
		{
			// This test works on a type created in the same txn that modifies it.
			`BEGIN;
SET LOCAL autocommit_before_ddl = false;
CREATE TYPE abc AS ENUM ('a', 'b', 'c');
ALTER TYPE abc ADD VALUE 'd';
ALTER TYPE abc DROP VALUE 'c';
COMMIT`,
			"",
			[]string{
				`SELECT 'a'::abc`,
				`SELECT 'b'::abc`,
				`SELECT 'd'::abc`,
			},
			[]string{
				`SELECT 'c'::abc`,
			},
		},
	}

	for i, tc := range testCases {
		_, err := sqlDB.Exec(tc.query)
		if err != nil {
			if tc.errorRe == "" {
				t.Fatalf("#%d: unexpected error while executing query: %v", i, err)
			}
			if !testutils.IsError(err, tc.errorRe) {
				t.Fatalf("#%d: expected error: %q, got error: %v", i, tc.errorRe, err)
			}
		}
		if err == nil && tc.errorRe != "" {
			t.Fatalf("#%d: unexpected success, expected error: %q", i, tc.errorRe)
		}

		for _, query := range tc.succeedAfter {
			if _, err := sqlDB.Exec(query); err != nil {
				t.Fatalf("#%d: expected %q to succeed, got error: %v", i, query, err)
			}
		}
		for _, query := range tc.failAfter {
			if _, err := sqlDB.Exec(query); err == nil {
				t.Fatalf("#%d: expected %q to fail, did not get error:", i, query)
			}
		}
	}
}

// Simulates the following scenario:
// - We have an enum, which starts out with values 'a' and 'b'.
// - We drop 'a' and add 'c' in job 1, which is slow, and finishes after job 2.
// - We drop 'b' and add 'd' in job 2, which is fast, and finishes before job 1.
// - job 2 fails due to an error, triggering a rollback.
// This test ensures that roll back is isolated to just the enum labels the
// job was responsible for acting upon. This is to say that 'a' should be
// unaffected by job 2 failing and should be successfully removed.
func TestEnumMemberTransitionIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	// Protects blocker.
	var mu syncutil.Mutex
	blocker := make(chan struct{})
	var unblocker chan struct{} = nil
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			// First entrant to the function blocks, second entrant returns an error.
			mu.Lock()
			if blocker != nil {
				unblocker = blocker
				blocker = nil
				mu.Unlock()
				<-unblocker
				return nil
			}
			mu.Unlock()
			return errors.New("boom")
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)

	droppingAFinished := make(chan struct{})
	droppingBFinished := make(chan struct{})

	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Setup.
	if _, err := sqlDB.Exec(`
CREATE TYPE ab AS ENUM ('a', 'b')`,
	); err != nil {
		t.Fatal(err)
	}

	go func() {
		_, err := sqlDB.Exec(`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE ab DROP VALUE 'a'; ALTER TYPE ab ADD VALUE 'c';
COMMIT`)
		if err != nil {
			t.Error(err)
		}
		close(droppingAFinished)
	}()

	go func() {
		// Only try dropping 'b' once the previous function (dropping 'a') is
		// blocking.
		for {
			mu.Lock()
			if blocker == nil {
				mu.Unlock()
				break
			}
			mu.Unlock()
		}
		_, err := sqlDB.Exec(`BEGIN;
SET LOCAL autocommit_before_ddl = false;
ALTER TYPE ab DROP VALUE 'b';
ALTER TYPE ab ADD VALUE 'd';
COMMIT`)
		if err == nil {
			t.Error("expected error, found nil")
		}
		if !testutils.IsError(err, "boom") {
			t.Errorf("expected boom, found %v", err)
		}
		// Unblock the job to drop 'a'.
		close(unblocker)
		close(droppingBFinished)
	}()

	// Ensure both the functions above have finished running before proceeding to
	// check the effects.
	<-droppingAFinished
	<-droppingBFinished

	// 'b' should not have been dropped, as this job was forced to roll back.
	if _, err := sqlDB.Exec(`SELECT 'b'::ab `); err != nil {
		t.Fatal(err)
	}
	// 'd' should not have been added, as the job was forced to roll back.
	_, err := sqlDB.Exec(`SELECT 'd'::ab`)
	if err == nil {
		t.Fatal("expected error, found nil")
	}
	if !testutils.IsError(err, `invalid input value for enum ab: "d"`) {
		t.Fatalf(`expected invalid input value for enum ab: "a", found %v`, err)
	}

	// 'a' was dropped in independent to and in a separate job to 'b', so we expect
	// the effects to be isolated.
	_, err = sqlDB.Exec(`SELECT 'a'::ab`)
	if err == nil {
		t.Fatal("expected error, found nil")
	}
	if !testutils.IsError(err, `invalid input value for enum ab: "a"`) {
		t.Fatalf(`expected invalid input value for enum ab: "a", found %v`, err)
	}
	if _, err := sqlDB.Exec(`SELECT 'c'::ab`); err != nil {
		t.Fatal(err)
	}
}

// TestTypeChangeJobCancelSemantics ensures that type change jobs that involve
// en enum member being dropped are cancelable and those that don't are not
// cancelable.
func TestTypeChangeJobCancelSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc       string
		query      string
		cancelable bool
	}{
		{
			"simple drop",
			"ALTER TYPE db.greetings DROP VALUE 'yo'",
			true,
		},
		{
			"simple add",
			"ALTER TYPE db.greetings ADD VALUE 'sup'",
			false,
		},
		{
			"txn add drop",
			"BEGIN; SET LOCAL autocommit_before_ddl = false; ALTER TYPE db.greetings ADD VALUE 'sup'; ALTER TYPE db.greetings DROP VALUE 'yo'; COMMIT",
			true,
		},
		{
			"txn add add",
			"BEGIN; SET LOCAL autocommit_before_ddl = false; ALTER TYPE db.greetings ADD VALUE 'sup'; ALTER TYPE db.greetings ADD VALUE 'hello'; COMMIT",
			false,
		},
		{
			"txn drop drop",
			"BEGIN; SET LOCAL autocommit_before_ddl = false; ALTER TYPE db.greetings DROP VALUE 'yo'; ALTER TYPE db.greetings DROP VALUE 'hi'; COMMIT",
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			params, _ := createTestServerParamsAllowTenants()

			// Wait groups for synchronizing various parts of the test.
			typeSchemaChangeStarted := make(chan struct{})
			blockTypeSchemaChange := make(chan struct{})
			finishedSchemaChange := make(chan struct{})

			params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
			params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
				RunBeforeEnumMemberPromotion: func(ctx context.Context) error {
					close(typeSchemaChangeStarted)
					select {
					case <-blockTypeSchemaChange:
					case <-ctx.Done():
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
CREATE TYPE db.greetings AS ENUM ('hi', 'yo');
`)
			require.NoError(t, err)

			go func(query string, isCancellable bool) {
				_, err := sqlDB.Exec(query)
				if isCancellable && !testutils.IsError(err, "job canceled by user") {
					t.Errorf("expected user to have canceled job, got %v", err)
				}
				if !isCancellable && err != nil {
					t.Error(err)
				}
				close(finishedSchemaChange)
			}(tc.query, tc.cancelable)

			<-typeSchemaChangeStarted

			_, err = sqlDB.Exec(`CANCEL JOB (
SELECT job_id FROM [SHOW JOBS]
WHERE 
	job_type = 'TYPEDESC SCHEMA CHANGE' AND 
	status = $1
	)`, jobs.StateRunning)

			if !tc.cancelable && !testutils.IsError(err, "not cancelable") {
				t.Fatalf("expected type schema change job to be not cancelable; found %v ", err)
			} else if tc.cancelable && err != nil {
				t.Fatal(err)
			}

			// If the type schema change job is cancelable, then the cleanup job
			// should kick in soon and the enum should be back in its original state
			// eventually.
			if tc.cancelable {
				testutils.SucceedsSoon(t, func() error {
					if _, err := sqlDB.Exec(`SELECT 'yo':::db.greetings`); err != nil {
						return err
					}
					if _, err := sqlDB.Exec(`SELECT 'hi':::db.greetings`); err != nil {
						return err
					}
					_, err := sqlDB.Exec(`SELECT 'sup':::db.greetings`)
					if err == nil {
						return errors.New("expected error, found none")
					}
					if !testutils.IsError(err, "invalid input value for enum") {
						return errors.Newf("expected invalid input for enum error, found %s", pgerror.FullError(err))
					}
					return nil
				})
			}
			close(blockTypeSchemaChange)
			<-finishedSchemaChange
		})
	}
}

func TestAddDropEnumValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()
	ctx := context.Background()

	params, _ := createTestServerParamsAllowTenants()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Set up schema necessary to execute all unit test cases below. We
	// execute the schema setup statements within a single, implicit txn
	// to ensure all the necessary schema elements required for the subsequent
	// test cases have been set up successfully.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
USE d;
CREATE TYPE e1 AS ENUM('check', 'enum', 'value', 'reference', 'cases', 'unused value');
-- case 1: enum value referenced within a UDF with language = SQL.
CREATE FUNCTION f1() RETURNS e1 LANGUAGE SQL AS $$ SELECT 'check'::e1 $$;
-- case 2: enum value referenced within a UDF with language = PLPGSQL.
CREATE FUNCTION f2() RETURNS e1 AS $$                                                                                                                          
  begin                                                                              
  	select 'enum'::e1;                                                                     
  end $$                                                                               
  language plpgsql;
-- case 3: array type alias for enum referenced within a UDF with language = SQL.
CREATE TYPE e2 AS ENUM('check', 'array', 'type', 'usage', 'cases', 'within', 'udfs', 'unused value');
CREATE FUNCTION f3() RETURNS _e2 LANGUAGE SQL AS $$ SELECT '{check, array, type}'::_e2; $$;
CREATE FUNCTION f4() RETURNS e2[] LANGUAGE SQL AS $$ SELECT ARRAY['usage'::e2, 'cases'::e2]; $$;
-- case 4: array type alias for enum referenced within a UDF with language = PLPGSQL.
CREATE FUNCTION f5() RETURNS e2[] AS $$
	declare
		b e2[] := ARRAY['within'::e2, 'udfs'::e2];
  begin                                                                              
  	return b;                                                                     
  end $$                                                                               
  language plpgsql;
-- case 5: enum value referenced with a view query.
CREATE TYPE e3 AS ENUM('check', 'enum', 'type', 'usage', 'cases', 'within', 'views', 'unused value');
CREATE VIEW v1 AS (SELECT 'check'::e3);
-- case 7: array type alias for enum referenced within a view query
CREATE VIEW v2 AS (SELECT '{enum, type, usage}'::_e3);
-- case 6: enum value referenced within a table.
CREATE TYPE e4 AS ENUM('check', 'enum', 'type', 'usage', 'cases', 'within', 'tables', 'invalid value', 'unused value');
CREATE TABLE t1(use_enum e4 CHECK (use_enum != 'invalid value'::e4), use_enum_computed e4 AS (IF (use_enum = 'check'::e4, 'type'::e4, 'cases'::e4)) STORED);
INSERT INTO t1 VALUES('check'::e4);
INSERT INTO t1 VALUES('enum'::e4);
-- case 8: array type alias for enum referenced within a table
CREATE TABLE t2(use_enum_arr e4[]);
INSERT INTO t2 VALUES(ARRAY['usage'::e4, 'cases'::e4]);
INSERT INTO t2 VALUES('{within, tables}'::_e4);
-- Add default column, on update expr, indexes
CREATE TYPE e5 AS ENUM('usage', 'in', 'default column', 'update expr');
CREATE TABLE t3(id int, enum_arr_default e5[] DEFAULT ARRAY['default column'::e5]);
CREATE TABLE t4(id int, enum_val e5 DEFAULT 'usage'::e5 ON UPDATE 'update expr'::e5);
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		query   string
		success bool
		err     string
	}{
		{
			`ALTER TYPE e1 DROP VALUE 'check'`,
			false,
			"could not remove enum value \"check\" as it is being used in a routine \"f1\"",
		},
		{
			`ALTER TYPE e1 DROP VALUE 'enum'`,
			false,
			"could not remove enum value \"enum\" as it is being used in a routine \"f2\"",
		},
		{
			`ALTER TYPE e1 DROP VALUE 'unused value'`,
			true,
			"",
		},
		{
			`DROP FUNCTION f1; ALTER TYPE e1 DROP VALUE 'check'`,
			true,
			"",
		},
		{
			`DROP FUNCTION f2; ALTER TYPE e1 DROP VALUE 'enum'`,
			true,
			"",
		},
		{
			`ALTER TYPE e2 DROP VALUE 'check'`,
			false,
			"could not remove enum value \"check\" as it is being used in a routine \"f3\"",
		},
		{
			`ALTER TYPE e2 DROP VALUE 'usage'`,
			false,
			"could not remove enum value \"usage\" as it is being used in a routine \"f4\"",
		},
		{
			`ALTER TYPE e2 DROP VALUE 'within'`,
			false,
			"could not remove enum value \"within\" as it is being used in a routine \"f5\"",
		},
		{
			`ALTER TYPE e2 DROP VALUE 'unused value'`,
			true,
			"",
		},
		{
			`DROP FUNCTION f3; ALTER TYPE e2 DROP VALUE 'check'`,
			true,
			"",
		},
		{
			`DROP FUNCTION f4; ALTER TYPE e2 DROP VALUE 'usage'`,
			true,
			"",
		},
		{
			`DROP FUNCTION f5; ALTER TYPE e2 DROP VALUE 'within'`,
			true,
			"",
		},
		{
			`ALTER TYPE e3 DROP VALUE 'check'`,
			false,
			"could not remove enum value \"check\" as it is being used in view \"v1\"",
		},
		{
			`ALTER TYPE e3 DROP VALUE 'enum'`,
			false,
			"could not remove enum value \"enum\" as it is being used in view \"v2\"",
		},
		{
			`ALTER TYPE e3 DROP VALUE 'unused value'`,
			true,
			"",
		},
		{
			`DROP VIEW v1; ALTER TYPE e3 DROP VALUE 'check'`,
			true,
			"",
		},
		{
			`DROP VIEW v2; ALTER TYPE e3 DROP VALUE 'enum'`,
			true,
			"",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'enum'`,
			false,
			"could not remove enum value \"enum\" as it is being used by \"t1\" in row: use_enum='enum', use_enum_computed='cases'",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'usage'`,
			false,
			"could not remove enum value \"usage\" as it is being used by table \"d.public.t2\"",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'type'`,
			false,
			"could not remove enum value \"type\" as it is being used in a computed column of \"t1\"",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'tables'`,
			false,
			"could not remove enum value \"tables\" as it is being used by table \"d.public.t2\"",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'invalid value'`,
			false,
			"could not remove enum value \"invalid value\" as it is being used in a check constraint of \"t1\"",
		},
		{
			`ALTER TYPE e4 DROP VALUE 'unused value'`,
			true,
			"",
		},
		{
			`ALTER TYPE e5 DROP VALUE 'default column'`,
			false,
			"could not remove enum value \"default column\" as it is being used in a default expresion of \"t3\"",
		},
		{
			`ALTER TYPE e5 DROP VALUE 'update expr'`,
			false,
			" could not remove enum value \"update expr\" as it is being used in an ON UPDATE expression of \"t4\"",
		},
		{
			`DROP TABLE t1; ALTER TYPE e4 DROP VALUE 'enum'; ALTER TYPE e4 DROP VALUE 'type'`,
			true,
			"",
		},
		{
			`DROP TABLE t2; ALTER TYPE e4 DROP VALUE 'usage'; ALTER TYPE e4 DROP VALUE 'tables'`,
			true,
			"",
		},
		{
			`DROP TABLE t3; ALTER TYPE e5 DROP VALUE 'default column'`,
			true,
			"",
		},
		{
			`DROP TABLE t4; ALTER TYPE e5 DROP VALUE 'update expr'`,
			true,
			"",
		},
	}

	for i, tc := range testCases {
		_, err := sqlDB.Exec(tc.query)
		if tc.success {
			require.NoErrorf(t, err, "#%d: unexpected error while executing query: %v", i, err)
		} else {
			require.Errorf(t, err, "#%d: expected error %s, but got no error", i, tc.err)
			if !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("#%d: expected error %s, got error %s", i, tc.err, err)
			}
		}
	}
}
