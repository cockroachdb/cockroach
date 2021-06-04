// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDrainingNamesAreCleanedTypeChangeOnFailure ensures that draining names
// are cleaned up if the type schema change job runs into a failure in Resume().
func TestDrainingNamesAreCleanedOnTypeChangeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			return errors.New("boom")
		},
	}
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM()
`); err != nil {
		t.Fatal(err)
	}

	// Try a rename. This should fail with "boom".
	_, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`)
	if err == nil {
		t.Fatal("expected error, found nil")
	}
	if !testutils.IsError(err, "boom") {
		t.Fatalf("expected boom, found %v", err)
	}

	// The failure hook should kick in and drain the names.
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.Exec(`CREATE TYPE d.t AS ENUM ('drained')`)
		return err
	})
}

// TestTypeSchemaChangeHandlesDeletedDescriptor ensures that the type schema
// change process is resilient to deleted descriptors.
func TestTypeSchemaChangeHandlesDeletedDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var delTypeDesc func()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			delTypeDesc()
			return nil
		},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM();
`); err != nil {
		t.Fatal(err)
	}

	// Set up delTypeDesc to delete t.
	desc := catalogkv.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")
	delTypeDesc = func() {
		// Delete the descriptor.
		if err := kvDB.Del(ctx, catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID())); err != nil {
			t.Error(err)
		}
	}

	// A job running on this descriptor shouldn't fail horribly.
	if _, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`); err != nil {
		t.Fatal(err)
	}
}

// TestTypeSchemaChangeRetriesTransparently tests that a type schema change
// that runs into a non permanent error will retry transparently.
func TestTypeSchemaChangeRetriesTransparently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Protects errorReturned.
	var mu syncutil.Mutex
	errorReturned := false
	params, _ := tests.CreateTestServerParams()
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
	params, _ := tests.CreateTestServerParams()
	cleanupSuccessfullyFinished := make(chan struct{})
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			return errors.New("yikes")
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
SET CLUSTER SETTING sql.defaults.drop_enum_value.enabled = true;
SET enable_drop_enum_value = true;
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

func TestAddDropValuesInTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
SET CLUSTER SETTING sql.defaults.drop_enum_value.enabled = true;
SET enable_drop_enum_value = true;
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

	params, _ := tests.CreateTestServerParams()
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
SET CLUSTER SETTING sql.defaults.drop_enum_value.enabled = true;
SET enable_drop_enum_value = true;
CREATE TYPE ab AS ENUM ('a', 'b')`,
	); err != nil {
		t.Fatal(err)
	}

	go func() {
		_, err := sqlDB.Exec(`BEGIN; ALTER TYPE ab DROP VALUE 'a'; ALTER TYPE ab ADD VALUE 'c'; COMMIT`)
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
		_, err := sqlDB.Exec(`BEGIN; ALTER TYPE ab DROP VALUE 'b'; ALTER TYPE ab ADD VALUE 'd'; COMMIT`)
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
			"BEGIN; ALTER TYPE db.greetings ADD VALUE 'sup'; ALTER TYPE db.greetings DROP VALUE 'yo'; COMMIT",
			true,
		},
		{
			"txn add add",
			"BEGIN; ALTER TYPE db.greetings ADD VALUE 'sup'; ALTER TYPE db.greetings ADD VALUE 'hello'; COMMIT",
			false,
		},
		{
			"txn drop drop",
			"BEGIN; ALTER TYPE db.greetings DROP VALUE 'yo'; ALTER TYPE db.greetings DROP VALUE 'hi'; COMMIT",
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			params, _ := tests.CreateTestServerParams()

			// Wait groups for synchronizing various parts of the test.
			var typeSchemaChangeStarted sync.WaitGroup
			typeSchemaChangeStarted.Add(1)
			var blockTypeSchemaChange sync.WaitGroup
			blockTypeSchemaChange.Add(1)
			var finishedSchemaChange sync.WaitGroup
			finishedSchemaChange.Add(1)

			params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
			params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
				RunBeforeEnumMemberPromotion: func() error {
					typeSchemaChangeStarted.Done()
					blockTypeSchemaChange.Wait()
					return nil
				},
			}

			s, sqlDB, _ := serverutils.StartServer(t, params)

			ctx := context.Background()
			defer s.Stopper().Stop(ctx)

			// Setup.
			_, err := sqlDB.Exec(`
SET CLUSTER SETTING sql.defaults.drop_enum_value.enabled = true;
SET enable_drop_enum_value = true;
CREATE DATABASE db;
CREATE TYPE db.greetings AS ENUM ('hi', 'yo');
`)
			require.NoError(t, err)

			go func() {
				_, err := sqlDB.Exec(tc.query)
				if tc.cancelable && !testutils.IsError(err, "job canceled by user") {
					t.Errorf("expected user to have canceled job, got %v", err)
				}
				if !tc.cancelable && err != nil {
					t.Error(err)
				}
				finishedSchemaChange.Done()
			}()

			typeSchemaChangeStarted.Wait()

			_, err = sqlDB.Exec(`CANCEL JOB (
SELECT job_id FROM [SHOW JOBS]
WHERE 
	job_type = 'TYPEDESC SCHEMA CHANGE' AND 
	status = $1
	)`, jobs.StatusRunning)

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
						return errors.Newf("expected invalid input for enum error, found %v", err)
					}
					return nil
				})
			}
			blockTypeSchemaChange.Done()
			finishedSchemaChange.Wait()
		})
	}
}
