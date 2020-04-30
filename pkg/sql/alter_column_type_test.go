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
	"errors"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestInsertBeforeOldColumnIsDropped converts a column from INT to STRING
// and then tries inserting 'hello' into the new column before the old column
// has been dropped expecting a parse error on insert.
func TestInsertBeforeOldColumnIsDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SkipComputedColumnSwapCleanup: true,
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(db)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	expected := "pq: This table is still undergoing the ALTER COLUMN TYPE schema change, this insert is not supported until the schema change is finalized: could not parse \"hello\" as type int: strconv.ParseInt: parsing \"hello\": invalid syntax"

	// This insert uses the insert fast path.
	sqlDB.ExpectErr(t, expected, `
SET enable_experimental_alter_column_type_general = true;
CREATE TABLE test (x INT);
ALTER TABLE test ALTER COLUMN x  TYPE STRING;
INSERT INTO test VALUES ('hello');
`)

	expected = "pq: This table is still undergoing the ALTER COLUMN TYPE schema change, this insert may not be supported until the schema change is finalized: could not parse \"hello\" as type int: strconv.ParseInt: parsing \"hello\": invalid syntax"

	// This insert uses the regular insert path.
	sqlDB.ExpectErr(t, expected, `
CREATE TABLE test2 (x STRING);
INSERT INTO test2 VALUES ('hello');
INSERT INTO test (SELECT * FROM test2);
`)
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

// TestAlterColumnTypeFailureRollback tests running a alter column type
// change on a table created in the same transaction.
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
		desc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
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
CREATE TABLE t.test (x INT NOT NULL);
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
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;`)
	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}
