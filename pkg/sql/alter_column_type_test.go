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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAlterColumnTypeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`SET enable_alter_column_type_general = true;`)
	if err != nil {
		t.Error(err)
	}
	_, err = sqlDB.Exec(`
CREATE DATABASE t;
BEGIN;
CREATE TABLE t.test (x INT);
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;
INSERT INTO t.test VALUES ('hello');
COMMIT;
`)

	expected := "pq: unimplemented: performing an ALTER TABLE ... ALTER COLUMN TYPE in the same txn as the table was created is not supported"
	actual := err.Error()

	if actual != expected {
		t.Fatalf("expected error to be %s, got %s", expected, actual)
	}
}

// Test while old column still exists - parse error.
func TestInsertBeforeOldColumnIsDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	_, err := sqlDB.Exec(`
CREATE TABLE test (x INT);
ALTER TABLE test ALTER COLUMN x TYPE STRING;
INSERT INTO test VALUES ('henlo');
`)

	expected := "pq: Column is under ALTER COLUMN TYPE schema change, this insert may not be supported until the schema change is finished (the original column must be dropped): could not parse \"henlo\" as type int: strconv.ParseInt: parsing \"henlo\": invalid syntax"
	actual := err.Error()
	if actual != expected {
		t.Fatalf("expected error to be %s, got %s", expected, actual)
	}
}

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
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT);
INSERT INTO t.test VALUES (1), (2), (3);
`); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN x TYPE STRING`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-swapNotification

	row := sqlDB.QueryRow("SHOW CREATE TABLE t.test")
	var scanName, create string
	if err := row.Scan(&scanName, &create); err != nil {
		t.Error(err)
	}
	expected := `CREATE TABLE test (
	x INT8 NULL,
	FAMILY "primary" (x, rowid)
)`
	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}

	// Let the schema change process continue.
	waitBeforeContinuing <- struct{}{}
	wg.Wait()

	row = sqlDB.QueryRow("SHOW CREATE TABLE t.test")
	if err := row.Scan(&scanName, &create); err != nil {
		t.Error(err)
	}
	expected = `CREATE TABLE test (
	x STRING NULL,
	FAMILY "primary" (x, rowid)
)`
	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}
}

// TestAlterColumnTypeFailureRollback tests running a primary key
// change on a table created in the same transaction.
func TestAlterColumnTypeFailureRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x STRING);
INSERT INTO t.test VALUES ('1'), ('2'), ('HELLO');
ALTER TABLE 
t.test ALTER COLUMN x TYPE INT;
`)
	actual := err.Error()
	expected := "pq: could not parse \"HELLO\" as type int: strconv.ParseInt: parsing \"HELLO\": invalid syntax"

	if actual != expected {
		t.Fatalf("expected %s, got %s", expected, actual)
	}

	desc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// Expect to find two drop mutations, one to drop the Column, one to drop the
	// Computed Column swap.
	if len(desc.Mutations) != 2 {
		t.Fatalf("expected to find 2 mutations, but found %d", len(desc.Mutations))

		if desc.Mutations[0].Direction != sqlbase.DescriptorMutation_DROP {
			t.Fatal("expected to be a drop mutation")
			if desc.Mutations[0].GetColumn().Name != "x_1" {
				t.Fatal("expected to be dropping column x_1")
			}
		}

		if desc.Mutations[0].Direction != sqlbase.DescriptorMutation_DROP {
			t.Fatal("expected to be a drop mutation")
		}

		column := desc.Mutations[0].GetColumn()
		if column == nil {
			t.Fatal("expected column to be non-nil")
		}
		if column.Name != "x_1" {
			t.Fatalf("expected to column to have name x_1, found: %s", column.Name)
		}

		if desc.Mutations[1].Direction != sqlbase.DescriptorMutation_DROP {
			t.Fatal("expected to be a drop mutation")
		}

		if desc.Mutations[1].GetComputedColumnSwap() == nil {
			t.Fatal("expected to find a ComputedColumnSwap mutation with direction drop")
		}

	}
}

func TestAddDropColumnDuringAlterColumnType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	waitBeforeContinuing := make(chan struct{})
	swapNotification := make(chan struct{})

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

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT);
`); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`
ALTER TABLE t.test ADD COLUMN y INT;
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;
ALTER TABLE t.test ADD COLUMN z INT;
ALTER TABLE t.test DROP COLUMN y;
`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-swapNotification

	var name, created string
	dest := []interface{}{&name, &created}
	expected := [][]interface{}{{
		"t.public.test",
		"CREATE TABLE test (\n\tx INT8 NULL,\n\ty INT8 NULL,\n\tFAMILY \"primary\" (x, rowid, y)\n)",
	}}
	err := sql.QueryExpect(`SHOW CREATE TABLE t.test`, sqlDB, expected, dest)
	if err != nil {
		t.Error(err)
	}

	waitBeforeContinuing <- struct{}{}
	wg.Wait()

	expected = [][]interface{}{{
		"t.public.test",
		"CREATE TABLE test (\n\tx STRING NULL,\n\tz INT8 NULL,\n\tFAMILY \"primary\" (x, rowid, z)\n)",
	}}
	err = sql.QueryExpect(`SHOW CREATE TABLE t.test`, sqlDB, expected, dest)
	if err != nil {
		t.Error(err)
	}
}

func TestIntToStringBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE TABLE test (x INT);
ALTER TABLE test ALTER COLUMN x TYPE STRING;
INSERT INTO test VALUES ('1'), ('2'), ('3');
`); err != nil {
		t.Error(err)
	}

	var x string
	dest := []interface{}{&x}

	expected := [][]interface{}{{"1"}, {"2"}, {"3"}}
	err := sql.QueryExpect(`SELECT * FROM test ORDER BY x`, sqlDB, expected, dest)
	if err != nil {
		t.Error(err)
	}
}

func TestQueryIntToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()

	params, _ := tests.CreateTestServerParams()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t3 (x INT, y INT, z INT);
INSERT INTO d.t3 VALUES (1, 1, 1), (2, 2, 2);
ALTER TABLE d.t3 ALTER COLUMN y TYPE STRING;
`); err != nil {
		t.Error(err)
	}

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.Exec(`INSERT INTO d.t3 VALUES (3, 'HELLO', 3);`)
		return err
	})

	var y string
	var x, z int
	dest := []interface{}{&x, &y, &z}

	expected := [][]interface{}{{1, "1", 1}, {2, "2", 2}, {3, "HELLO", 3}}
	err := sql.QueryExpect(`SELECT * FROM d.t3 ORDER BY x`, sqlDB, expected, dest)
	if err != nil {
		t.Error(err)
	}
}

func TestAlterColumnTypeBeforeAlterPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeComputedColumnSwap: func() {
				swapNotification <- struct{}{}
				<-waitBeforeContinuing
			},
		},
		DistSQL: &execinfra.TestingKnobs{},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT NOT NULL);
`); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;
`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-swapNotification

	expected := "pq: unimplemented: table test is currently undergoing a schema change"
	_, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (x);`)
	waitBeforeContinuing <- struct{}{}
	wg.Wait()
	if err.Error() != expected {
		t.Fatalf("expected error: %s, got: %s", expected, err.Error())
	}
}

func TestAlterPrimaryKeyBeforeAlterColumnType(t *testing.T) {
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
		DistSQL: &execinfra.TestingKnobs{},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT NOT NULL);
`); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`
ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (x);
`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-swapNotification

	expected := "pq: unimplemented: table test is currently undergoing a schema change"
	_, err := sqlDB.Exec(`
SET enable_alter_column_type_general = true;
ALTER TABLE t.test ALTER COLUMN x TYPE STRING;`)
	waitBeforeContinuing <- struct{}{}
	wg.Wait()
	if err.Error() != expected {
		t.Fatalf("expected error: %s, got: %s", expected, err.Error())
	}
}

func TestAlterColumnTypeWithAlterPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(
		`SET enable_alter_column_type_general = true;`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT NOT NULL, y INT NOT NULL);
`); err != nil {
		t.Error(err)
	}

	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN x TYPE STRING;`); err != nil {
		t.Error(err)
	}

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.Exec(`ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (x,y);`)
		return err
	})

	testutils.SucceedsSoon(t, func() error {
		var name, created string
		dest := []interface{}{&name, &created}
		expected := [][]interface{}{{
			"t.public.test",
			"CREATE TABLE test (\n\tx STRING NOT NULL,\n\ty INT8 NOT NULL,\n\tCONSTRAINT \"primary\" PRIMARY KEY (x ASC, y ASC),\n\tFAMILY \"primary\" (x, y, rowid)\n)",
		}}
		err := sql.QueryExpect(`SHOW CREATE TABLE t.test`, sqlDB, expected, dest)
		if err != nil {
			t.Error(err)
		}
		return nil
	})
}
