// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
		// TODO(dan): HACK until the changefeed can control pgwire flushing.
		ConnResultsBufferBytes: 1,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	assertPayloads(t, rows, []string{
		`foo: [0]->{"a": 0, "b": "initial"}`,
	})

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
	assertPayloads(t, rows, []string{
		`foo: [1]->{"a": 1, "b": "a"}`,
		`foo: [2]->{"a": 2, "b": "b"}`,
	})

	sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
	assertPayloads(t, rows, []string{
		`foo: [2]->{"a": 2, "b": "c"}`,
		`foo: [3]->{"a": 3, "b": "d"}`,
	})

	sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
	assertPayloads(t, rows, []string{
		`foo: [1]->`,
	})
}

func TestChangefeedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
		// TODO(dan): HACK until the changefeed can control pgwire flushing.
		ConnResultsBufferBytes: 1,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

	t.Run(`envelope=row`, func(t *testing.T) {
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR DATABASE d WITH envelope='row'`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		assertPayloads(t, rows, []string{`foo: [1]->{"a": 1, "b": "a"}`})
	})
	t.Run(`envelope=key_only`, func(t *testing.T) {
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR DATABASE d WITH envelope='key_only'`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		assertPayloads(t, rows, []string{`foo: [1]->`})
	})
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
		// TODO(dan): HACK until the changefeed can control pgwire flushing.
		ConnResultsBufferBytes: 1,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'b')`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR DATABASE d`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	assertPayloads(t, rows, []string{
		`foo: [1]->{"a": 1, "b": "a"}`,
		`bar: [2]->{"a": 2, "b": "b"}`,
	})
}

func TestChangefeedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
		// TODO(dan): HACK until the changefeed can control pgwire flushing.
		ConnResultsBufferBytes: 1,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
	var ts string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, ts)
	defer closeFeedRowsHack(t, sqlDB, rows)

	assertPayloads(t, rows, []string{
		`foo: [2]->{"a": 2, "b": "after"}`,
	})
}

func TestChangefeedSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()
	t.Skip("#26661")

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
		// TODO(dan): HACK until the changefeed can control pgwire flushing.
		ConnResultsBufferBytes: 1,
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (0)`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (1)`)
	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b INT`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (2)`)
	sqlDB.Exec(t, `INSERT INTO foo (a, b) VALUES (3, 4)`)
	assertPayloads(t, rows, []string{
		`foo: [0]->{"a": 0}`,
		`foo: [1]->{"a": 1}`,
		`foo: [2]->{"a": 2, "b": null}`,
		`foo: [3]->{"a": 3, "b": 4}`,
	})

	// TODO(dan): Test a schema change that uses a backfill once we figure out
	// the user facing semantics of that.
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope`,
	); !testutils.IsError(err, `no test producer: nope`) {
		t.Fatalf(`expected 'no test producer: nope' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO ''`,
	); !testutils.IsError(err, `omit the SINK clause`) {
		t.Fatalf(`expected 'omit the SINK clause' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, ``,
	); !testutils.IsError(err, `omit the SINK clause`) {
		t.Fatalf(`expected 'omit the SINK clause' error got: %+v`, err)
	}

}

func assertPayloads(t *testing.T, rows *gosql.Rows, expected []string) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) && rows.Next() {
		var topic string
		var key, value []byte
		if err := rows.Scan(&topic, &key, &value); err != nil {
			t.Fatalf(`%+v`, err)
		}
		actual = append(actual, fmt.Sprintf(`%s: %s->%s`, topic, key, value))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf(`%+v`, err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func closeFeedRowsHack(t *testing.T, sqlDB *sqlutils.SQLRunner, rows *gosql.Rows) {
	// TODO(dan): We should just be able to close the `gosql.Rows` but that
	// currently blocks forever without this.
	sqlDB.Exec(t, `CANCEL QUERIES (
		SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'CREATE CHANGEFEED%'
	)`)
	rows.Close()
}
