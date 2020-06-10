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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// TestInsertBeforeOldColumnIsDropped converts a column from INT to STRING
// and then tries inserting 'hello' into the new column before the old column
// has been dropped expecting a parse error on insert.
func TestInsertBeforeOldColumnIsDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	childJobStartNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	var doOnce sync.Once
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeChildJobs: func() {
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
CREATE TABLE test (x INT);
CREATE TABLE test2 (x STRING);
INSERT INTO test2 VALUES ('hello');`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `
SET enable_experimental_alter_column_type_general = true;
ALTER TABLE test ALTER COLUMN x TYPE STRING;`)
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
	params, _ := tests.CreateTestServerParams()
	childJobStartNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	var doOnce sync.Once
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeChildJobs: func() {
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
CREATE TABLE test (x INT);
CREATE TABLE test2 (x BOOL);
INSERT INTO test2 VALUES (true);
`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `
SET enable_experimental_alter_column_type_general = true;
ALTER TABLE test ALTER COLUMN x TYPE BOOL USING (x > 0);`)
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

	defer setTestJobsAdoptInterval()()

	ctx := context.Background()
	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeComputedColumnSwap: func() {
				// Notify the tester that the alter column type is about to happen.
				swapNotification <- struct{}{}
				// Wait for the tester to finish before continuing the swap.
				<-waitBeforeContinuing
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true;`)

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
		`CREATE TABLE test (
	x INT8 NULL,
	FAMILY "primary" (x, rowid)
)`}}

	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t.test", expected)

	// Let the schema change process continue.
	waitBeforeContinuing <- struct{}{}
	wg.Wait()

	expected = [][]string{{"t.public.test",
		`CREATE TABLE test (
	x STRING NULL,
	FAMILY "primary" (x, rowid)
)`}}

	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t.test", expected)
}

// TestAlterColumnTypeFailureRollback ensures that the mutations are cleaned up
// if the alter column type fails.
func TestAlterColumnTypeFailureRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true;`)

	expected := "pq: could not parse \"HELLO\" as type int: strconv.ParseInt: parsing \"HELLO\": invalid syntax"

	sqlDB.ExpectErr(t, expected, `
CREATE DATABASE t;
CREATE TABLE t.test (x STRING);
INSERT INTO t.test VALUES ('1'), ('2'), ('HELLO');
ALTER TABLE t.test ALTER COLUMN x TYPE INT;
`)

	// Ensure that the add column and column swap mutations are cleaned up.
	testutils.SucceedsSoon(t, func() error {
		desc := sqlbase.GetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		if len(desc.Mutations) != 0 {
			return errors.New("expected no mutations on TableDescriptor")
		}
		return nil
	})
}

// TestQueryIntToString changes a column from int to string, then
// tries inserting 'hello' into the column and expects it to eventually succeed.
func TestQueryIntToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	params, _ := tests.CreateTestServerParams()

	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true;`)

	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (x INT, y INT, z INT);
INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2);
ALTER TABLE t.test ALTER COLUMN y TYPE STRING;
`)

	sqlDB.ExecSucceedsSoon(t, `INSERT INTO t.test VALUES (3, 'HELLO', 3);`)

	expected := [][]string{{"1", "1", "1"}, {"2", "2", "2"}, {"3", "HELLO", "3"}}
	sqlDB.CheckQueryResults(t, `SELECT * FROM t.test ORDER BY x`, expected)
}

func TestSchemaChangeBeforeAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePrimaryKeySwap: func() {
				swapNotification <- struct{}{}
				<-waitBeforeContinuing
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(ctx)

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

	expected := "pq: unimplemented: table test is currently undergoing a schema change"
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
	params, _ := tests.CreateTestServerParams()
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sqlDB.Exec(t, `
SET enable_experimental_alter_column_type_general = true;
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;
`)
		wg.Done()
	}()

	<-childJobStartNotification

	expected := "pq: unimplemented: cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress"
	sqlDB.ExpectErr(t, expected, `
ALTER TABLE t.test ADD COLUMN y INT;
	`)

	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}
