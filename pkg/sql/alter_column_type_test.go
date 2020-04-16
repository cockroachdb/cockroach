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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAlterColumnTypeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`
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

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT);
INSERT INTO t.test VALUES (1), (2), (3);
`); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
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
		t.Fatal(err)
	}
	expected = `CREATE TABLE test (
	x STRING NULL,
	FAMILY "primary" (x, rowid)
)`
	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}
}

// Column Families test
func TestColumnFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test(id INT, id2 INT, FAMILY f1 (id), FAMILY f2 (id2));
INSERT INTO t.test VALUES (1), (2), (3);
ALTER TABLE t.test ALTER COLUMN id2 TYPE STRING;
`); err != nil {
		t.Fatal(err)
	}

	row := sqlDB.QueryRow("SHOW CREATE TABLE t.test")
	var scanName, create string
	if err := row.Scan(&scanName, &create); err != nil {
		t.Fatal(err)
	}
	expected := `CREATE TABLE test (
	id INT8 NULL,
	id2 STRING NULL,
	FAMILY f1 (id, rowid),
	FAMILY f2 (id2)
)`

	if create != expected {
		t.Fatalf("expected %s, found %s", expected, create)
	}
}

func TestAlterColumnTypeTimestampTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
BEGIN;
CREATE TABLE t.test (x TIMESTAMPTZ(6));
INSERT INTO t.test VALUES ('2016-01-25 10:10:10.555555-05:00');
ALTER TABLE t.test ALTER COLUMN x TYPE TIMESTAMPTZ(3);
INSERT INTO t.test VALUES ('2016-01-26 10:10:10.555555-05:00');
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlDB.Query("SELECT * FROM t.test ORDER BY x")
	if err != nil {
		t.Fatal(err)
	}
	var date string
	for _, tc := range []struct {
		expected string
	}{
		{"2016-01-25T15:10:10.556Z"},
		{"2016-01-26T15:10:10.556Z"},
	} {
		rows.Next()
		if err = rows.Scan(&date); err != nil {
			t.Fatal(err)
		}
		if date != tc.expected {
			t.Fatalf("expected %s, got %s", tc.expected, date)
		}
	}
}

// Generalize this test?
func TestAlterColumnTypeIntToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTestJobsAdoptInterval()()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT, y INT, z INT);
INSERT INTO t.test VALUES (1, 1, 1), (2, 2, 2);
ALTER TABLE t.test ALTER COLUMN y TYPE STRING;
`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		rows, err := sqlDB.Query(`SHOW CREATE TABLE t.test;`)
		if err != nil {
			return err
		}
		var a, b string
		rows.Next()
		if err = rows.Scan(&a, &b); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("out: %s,%s", a, b)
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.Exec(`INSERT INTO t.test VALUES (3, 'HELLO', 3);`)
		return err
	})

	rows, err := sqlDB.Query("SELECT * FROM t.test ORDER BY x")
	if err != nil {
		t.Fatal(err)
	}
	var x, z int
	var y string
	for _, tc := range []struct {
		x int
		y string
		z int
	}{
		{1, "1", 1},
		{2, "2", 2},
		{3, "HELLO", 3},
	} {
		rows.Next()
		if err = rows.Scan(&x, &y, &z); err != nil {
			t.Fatal(err)
		}
		if x != tc.x || y != tc.y || z != tc.z {
			t.Fatalf("expected %d %s %d, got %d %s %d", tc.x, tc.y, tc.z, x, y, z)
		}
	}
}
