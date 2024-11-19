// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestInsertBeforeOldColumnIsDropped converts a column from INT to STRING
// and then tries inserting 'hello' into the new column before the old column
// has been dropped expecting a parse error on insert.
func TestInsertBeforeOldColumnIsDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var s serverutils.TestServerInterface
	params, _ := createTestServerParams()
	childJobStartNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	var doOnce sync.Once
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				// Block in the job to drop old indexes, which has mutation ID 2.
				scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
				if err != nil {
					return err
				}
				pl := scJob.Payload()
				if pl.GetSchemaChange().TableMutationID < 2 {
					return nil
				}
				doOnce.Do(func() {
					childJobStartNotification <- struct{}{}
					<-waitBeforeContinuing
				})
				return nil
			},
		},
	}

	var db *gosql.DB
	s, db, _ = serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `
CREATE TABLE test (x INT);
CREATE TABLE test2 (x STRING);
INSERT INTO test2 VALUES ('hello');`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.ExecMultiple(t,
			`SET enable_experimental_alter_column_type_general = true;`,
			// TODO(spilchen): This test is designed for the legacy schema changer.
			// Update it for the declarative schema changer (DSC).
			`SET use_declarative_schema_changer = 'off';`,
			`ALTER TABLE test ALTER COLUMN x TYPE STRING;`)
		wg.Done()
	}()

	// Wait until column swap progresses and we start to run child jobs
	// before continuing.
	<-childJobStartNotification

	expected := "pq: This table is still undergoing the ALTER COLUMN TYPE " +
		"schema change, this insert"

	// This insert uses the insert fast path.
	sqlDB.ExpectErrSucceedsSoon(t, expected, `
	INSERT INTO test VALUES ('hello');
	`)

	// This insert uses the regular insert path.
	sqlDB.ExpectErrSucceedsSoon(t, expected, `
	INSERT INTO test (SELECT * FROM test2);
	`)

	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}

// TestInsertBeforeOldColumnIsDroppedUsingExpr converts a column from INT to
// bool and then tries inserting true into the new column before the old column
// is dropped.
func TestInsertBeforeOldColumnIsDroppedUsingExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var s serverutils.TestServerInterface
	params, _ := createTestServerParams()
	childJobStartNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	var doOnce sync.Once
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeResume: func(jobID jobspb.JobID) error {
				// Block in the job to drop old indexes, which has mutation ID 2.
				scJob, err := s.JobRegistry().(*jobs.Registry).LoadJob(ctx, jobID)
				if err != nil {
					return err
				}
				pl := scJob.Payload()
				if pl.GetSchemaChange().TableMutationID < 2 {
					return nil
				}
				doOnce.Do(func() {
					childJobStartNotification <- struct{}{}
					<-waitBeforeContinuing
				})
				return nil
			},
		},
	}

	var db *gosql.DB
	s, db, _ = serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `
CREATE TABLE test (x INT);
CREATE TABLE test2 (x BOOL);
INSERT INTO test2 VALUES (true);
`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.ExecMultiple(t,
			`SET enable_experimental_alter_column_type_general = true;`,
			// TODO(spilchen): This test is designed for the legacy schema changer.
			// Update it for the declarative schema changer (DSC).
			`SET use_declarative_schema_changer = 'off';`,
			`ALTER TABLE test ALTER COLUMN x TYPE BOOL USING (x > 0);`)
		wg.Done()
	}()

	// Wait until column swap progresses and we start to run child jobs
	// before continuing.
	<-childJobStartNotification

	expected := "pq: column x is undergoing the ALTER COLUMN TYPE USING " +
		"EXPRESSION schema change, inserts are not supported until the schema " +
		"change is finalized, tried to insert true into x"

	sqlDB.ExpectErrSucceedsSoon(t, expected, `
	INSERT INTO test VALUES (true);
	`)

	// This insert uses the regular insert path.
	sqlDB.ExpectErrSucceedsSoon(t, expected, `
	INSERT INTO test (SELECT * FROM test2);
	`)

	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}

// TestVisibilityDuringAlterColumnType tests to see that only one column
// is visible at a time during the swap process and ensures that the visible
// column has the correct type.
func TestVisibilityDuringAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeComputedColumnSwap: func() {
				// Notify the tester that the alter column type is about to happen.
				swapNotification <- struct{}{}
				// Wait for the tester to finish before continuing the swap.
				<-waitBeforeContinuing
			},
		},
		// Decrease the adopt loop interval so that retries happen quickly.
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.ExecMultiple(t, `SET enable_experimental_alter_column_type_general = true;`,
		// TODO(spilchen): This test is designed for the legacy schema changer.
		// Update it for the declarative schema changer (DSC).
		`SET use_declarative_schema_changer = 'off'`)

	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (x INT);
INSERT INTO t.test VALUES (1), (2), (3);
`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `ALTER TABLE t.test ALTER COLUMN x TYPE STRING`)
		wg.Done()
	}()

	<-swapNotification

	expected := [][]string{{"t.public.test",
		`CREATE TABLE public.test (
	x INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT test_pkey PRIMARY KEY (rowid ASC)
)`}}

	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t.test", expected)

	// Let the schema change process continue.
	waitBeforeContinuing <- struct{}{}
	wg.Wait()

	expected = [][]string{{"t.public.test",
		`CREATE TABLE public.test (
	x STRING NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT test_pkey PRIMARY KEY (rowid ASC)
)`}}

	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t.test", expected)
}

// TestAlterColumnTypeFailureRollback ensures that the mutations are cleaned up
// if the alter column type fails.
func TestAlterColumnTypeFailureRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true;`)
	sqlDB.Exec(t, `CREATE DATABASE t;`)
	sqlDB.Exec(t, `CREATE TABLE t.test (x STRING);`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES ('1'), ('2'), ('HELLO');`)

	expected := "pq: failed to construct index entries during backfill: could not parse \"HELLO\" as type int: strconv.ParseInt: parsing \"HELLO\": invalid syntax"
	sqlDB.ExpectErr(t, expected, `ALTER TABLE t.test ALTER COLUMN x TYPE INT USING x::INT8;`)

	// Ensure that the add column and column swap mutations are cleaned up.
	testutils.SucceedsSoon(t, func() error {
		desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.ApplicationLayer().Codec(), "t", "test")
		if len(desc.AllMutations()) != 0 {
			return errors.New("expected no mutations on TableDescriptor")
		}
		return nil
	})
}

// TestQueryIntToString changes a column from int to string, then
// tries inserting 'hello' into the column and expects it to eventually succeed.
func TestQueryIntToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParams()
	// Decrease the adopt loop interval so that retries happen quickly.
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true;`)

	sqlDB.Exec(t, `CREATE DATABASE t;`)
	sqlDB.Exec(t, `CREATE TABLE t.test (x INT, y INT, z INT);`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2);`)
	sqlDB.Exec(t, `ALTER TABLE t.test ALTER COLUMN y TYPE STRING;`)

	sqlDB.ExecSucceedsSoon(t, `INSERT INTO t.test VALUES (3, 'HELLO', 3);`)

	expected := [][]string{{"1", "1", "1"}, {"2", "2", "2"}, {"3", "HELLO", "3"}}
	sqlDB.CheckQueryResults(t, `SELECT * FROM t.test ORDER BY x`, expected)
}

func TestSchemaChangeBeforeAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})

	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePrimaryKeySwap: func() {
				swapNotification <- struct{}{}
				<-waitBeforeContinuing
			},
		},
	}
	defer close(waitBeforeContinuing)
	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)
	sqlDB.ExecMultiple(t,
		`SET use_declarative_schema_changer = 'off';`,
		`SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off';`)
	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (x INT NOT NULL, y INT);
`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `
ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (x);
`)
		wg.Done()
	}()

	<-swapNotification

	expected := "pq: unimplemented: ALTER COLUMN TYPE requiring rewrite of on-disk data is currently not " +
		"supported for columns that are part of an index"
	sqlDB.ExpectErr(t, expected, `
SET enable_experimental_alter_column_type_general = true;
ALTER TABLE t.test ALTER COLUMN y TYPE STRING;`)
	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}

// TestSchemaChangeWhileExecutingAlterColumnType tests that other schema changes
// cannot be queued while an ALTER COLUMN TYPE schema change is in progress.
func TestSchemaChangeWhileExecutingAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParams()
	childJobStartNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	var doOnce sync.Once
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeComputedColumnSwap: func() {
				doOnce.Do(func() {
					childJobStartNotification <- struct{}{}
					<-waitBeforeContinuing
				})
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (x INT);
`)

	tableID := sqlutils.QueryTableID(t, db, "t", "public", "test")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.ExecMultiple(t,
			`SET enable_experimental_alter_column_type_general = true;`,
			`SET use_declarative_schema_changer = off;`,
			`ALTER TABLE t.test ALTER COLUMN x TYPE STRING;`,
		)
		wg.Done()
	}()

	<-childJobStartNotification

	expected := fmt.Sprintf(`pq: relation "test" \(%d\): unimplemented: cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress`, tableID)
	sqlDB.ExpectErr(t, expected, `
SET use_declarative_schema_changer = off;
ALTER TABLE t.test ADD COLUMN y INT;
	`)

	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}
