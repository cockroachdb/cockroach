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
	gojson "encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/crdb"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	// 'initial' is skipped because only the latest value ('updated') is emitted
	// by the initial scan.
	assertPayloads(t, rows, []string{
		`foo: [0]->{"a": 0, "b": "updated"}`,
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
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH envelope='row'`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		assertPayloads(t, rows, []string{`foo: [1]->{"a": 1, "b": "a"}`})
	})
	t.Run(`envelope=key_only`, func(t *testing.T) {
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH envelope='key_only'`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		assertPayloads(t, rows, []string{`foo: [1]->`})
	})
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo, bar`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	assertPayloads(t, rows, []string{
		`foo: [1]->{"a": 1, "b": "a"}`,
		`bar: [2]->{"a": 2, "b": "b"}`,
	})
}

func TestChangefeedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

func TestChangefeedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	var ts0 string
	if err := crdb.ExecuteTx(ctx, sqlDB.DB, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(
			`INSERT INTO foo VALUES (0) RETURNING cluster_logical_timestamp()`,
		).Scan(&ts0)
	}); err != nil {
		t.Fatal(err)
	}

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH updated, resolved`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	var ts1 string
	if err := crdb.ExecuteTx(ctx, sqlDB.DB, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(
			`INSERT INTO foo VALUES (1) RETURNING cluster_logical_timestamp()`,
		).Scan(&ts1)
	}); err != nil {
		t.Fatal(err)
	}

	assertPayloads(t, rows, []string{
		`foo: [0]->{"__crdb__": {"updated": "` + ts0 + `"}, "a": 0}`,
		`foo: [1]->{"__crdb__": {"updated": "` + ts1 + `"}, "a": 1}`,
	})

	// Check that we eventually get a resolved timestamp greater than ts1.
	expectResolvedTimestampGreaterThan(t, rows, ts1)
}

func TestChangefeedSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	t.Run(`historical`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE historical (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)
		var start string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&start)
		sqlDB.Exec(t, `INSERT INTO historical (a, b) VALUES (0, '0')`)
		sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (1)`)
		sqlDB.Exec(t, `ALTER TABLE historical ALTER COLUMN b SET DEFAULT 'after'`)
		sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (2)`)
		sqlDB.Exec(t, `ALTER TABLE historical ADD COLUMN c INT`)
		sqlDB.Exec(t, `INSERT INTO historical (a) VALUES (3)`)
		sqlDB.Exec(t, `INSERT INTO historical (a, c) VALUES (4, 14)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR historical WITH cursor=$1`, start)
		defer closeFeedRowsHack(t, sqlDB, rows)
		assertPayloads(t, rows, []string{
			`historical: [0]->{"a": 0, "b": "0"}`,
			`historical: [1]->{"a": 1, "b": "before"}`,
			`historical: [2]->{"a": 2, "b": "after"}`,
			`historical: [3]->{"a": 3, "b": "after", "c": null}`,
			`historical: [4]->{"a": 4, "b": "after", "c": 14}`,
		})
	})

	t.Run(`add column`, func(t *testing.T) {
		// NB: the default is a nullable column
		sqlDB.Exec(t, `CREATE TABLE add_column (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO add_column VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_column`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE add_column ADD COLUMN b STRING`)
		sqlDB.Exec(t, `INSERT INTO add_column VALUES (2, '2')`)
		assertPayloads(t, rows, []string{
			`add_column: [1]->{"a": 1}`,
			`add_column: [2]->{"a": 2, "b": "2"}`,
		})
	})

	t.Run(`add column not null`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE add_column_notnull (a INT PRIMARY KEY)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_column_notnull WITH resolved`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE add_column_notnull ADD COLUMN b STRING NOT NULL`)
		sqlDB.Exec(t, `INSERT INTO add_column_notnull VALUES (2, '2')`)
		skipResolvedTimestamps(t, rows)
		if err := rows.Err(); !testutils.IsError(err, `cannot operate on tables being backfilled`) {
			t.Fatalf(`expected "cannot operate on tables being backfilled" error got: %+v`, err)
		}
	})

	t.Run(`add column with default`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_column_def`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
		sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2, '2')`)
		assertPayloads(t, rows, []string{
			`add_column_def: [1]->{"a": 1}`,
		})
		if rows.Next() {
			t.Fatal(`unexpected row`)
		}
		if err := rows.Err(); !testutils.IsError(err, `cannot operate on tables being backfilled`) {
			t.Fatalf(`expected "cannot operate on tables being backfilled" error got: %+v`, err)
		}
	})

	t.Run(`add column computed`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE add_column_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
		sqlDB.Exec(t, `INSERT INTO add_column_comp VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_column_comp`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE add_column_comp ADD COLUMN c INT AS (a + 10) STORED`)
		sqlDB.Exec(t, `INSERT INTO add_column_comp (a) VALUES (2)`)
		assertPayloads(t, rows, []string{
			`add_column_comp: [1]->{"a": 1, "b": 6}`,
		})
		if rows.Next() {
			t.Fatal(`unexpected row`)
		}
		if err := rows.Err(); !testutils.IsError(err, `cannot operate on tables being backfilled`) {
			t.Fatalf(`expected "cannot operate on tables being backfilled" error got: %+v`, err)
		}
	})

	t.Run(`rename column`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE rename_column (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO rename_column VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR rename_column`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE rename_column RENAME COLUMN b TO c`)
		sqlDB.Exec(t, `INSERT INTO rename_column VALUES (2, '2')`)
		assertPayloads(t, rows, []string{
			`rename_column: [1]->{"a": 1, "b": "1"}`,
			`rename_column: [2]->{"a": 2, "c": "2"}`,
		})
	})

	t.Run(`drop column`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR drop_column`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
		sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)
		assertPayloads(t, rows, []string{
			`drop_column: [1]->{"a": 1, "b": "1"}`,
		})
		if rows.Next() {
			t.Fatal(`unexpected row`)
		}
		if err := rows.Err(); !testutils.IsError(err, `cannot operate on tables being backfilled`) {
			t.Fatalf(`expected "cannot operate on tables being backfilled" error got: %+v`, err)
		}
	})

	t.Run(`add default`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE add_default (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO add_default (a, b) VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_default`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE add_default ALTER COLUMN b SET DEFAULT 'd'`)
		sqlDB.Exec(t, `INSERT INTO add_default (a) VALUES (2)`)
		assertPayloads(t, rows, []string{
			`add_default: [1]->{"a": 1, "b": "1"}`,
			`add_default: [2]->{"a": 2, "b": "d"}`,
		})
	})

	t.Run(`alter default`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE alter_default (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)
		sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR alter_default`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE alter_default ALTER COLUMN b SET DEFAULT 'after'`)
		sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (2)`)
		assertPayloads(t, rows, []string{
			`alter_default: [1]->{"a": 1, "b": "before"}`,
			`alter_default: [2]->{"a": 2, "b": "after"}`,
		})
	})

	t.Run(`drop default`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE drop_default (a INT PRIMARY KEY, b STRING DEFAULT 'd')`)
		sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR drop_default`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE drop_default ALTER COLUMN b DROP DEFAULT`)
		sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (2)`)
		assertPayloads(t, rows, []string{
			`drop_default: [1]->{"a": 1, "b": "d"}`,
			`drop_default: [2]->{"a": 2, "b": null}`,
		})
	})

	t.Run(`drop not null`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE drop_notnull (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR drop_notnull`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE drop_notnull ALTER b DROP NOT NULL`)
		sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (2, NULL)`)
		assertPayloads(t, rows, []string{
			`drop_notnull: [1]->{"a": 1, "b": "1"}`,
			`drop_notnull: [2]->{"a": 2, "b": null}`,
		})
	})

	t.Run(`checks`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE checks (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO checks VALUES (1)`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR checks`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE checks ADD CONSTRAINT c CHECK (a < 5) NOT VALID`)
		sqlDB.Exec(t, `INSERT INTO checks VALUES (2)`)
		sqlDB.Exec(t, `ALTER TABLE checks VALIDATE CONSTRAINT c`)
		sqlDB.Exec(t, `INSERT INTO checks VALUES (3)`)
		sqlDB.Exec(t, `ALTER TABLE checks DROP CONSTRAINT c`)
		sqlDB.Exec(t, `INSERT INTO checks VALUES (6)`)
		assertPayloads(t, rows, []string{
			`checks: [1]->{"a": 1}`,
			`checks: [2]->{"a": 2}`,
			`checks: [3]->{"a": 3}`,
			`checks: [6]->{"a": 6}`,
		})
	})

	t.Run(`add index`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR add_index`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `CREATE INDEX b_idx ON add_index (b)`)
		sqlDB.Exec(t, `SELECT * FROM add_index@b_idx`)
		sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, '2')`)
		assertPayloads(t, rows, []string{
			`add_index: [1]->{"a": 1, "b": "1"}`,
			`add_index: [2]->{"a": 2, "b": "2"}`,
		})
	})

	t.Run(`unique`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE "unique" (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO "unique" VALUES (1, '1')`)
		rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR "unique"`)
		defer closeFeedRowsHack(t, sqlDB, rows)
		sqlDB.Exec(t, `ALTER TABLE "unique" ADD CONSTRAINT u UNIQUE (b)`)
		sqlDB.Exec(t, `INSERT INTO "unique" VALUES (2, '2')`)
		assertPayloads(t, rows, []string{
			`unique: [1]->{"a": 1, "b": "1"}`,
			`unique: [2]->{"a": 2, "b": "2"}`,
		})
	})
}

func TestChangefeedInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// TODO(dan): Work around a race in CANCEL QUERIES (#28033) by only
	// canceling them once.
	var cancelQueriesOnce sync.Once
	closeFeedRowsCancelOnceHack := func(t *testing.T, sqlDB *sqlutils.SQLRunner, rows *gosql.Rows) {
		cancelQueriesOnce.Do(func() {
			// TODO(dan): We should just be able to close the `gosql.Rows` but
			// that currently blocks forever without this.
			sqlDB.Exec(t, `CANCEL QUERIES (
			SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'CREATE CHANGEFEED%'
		)`)
		})
		rows.Close()
	}

	sqlDB.Exec(t, `CREATE TABLE grandparent (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO grandparent VALUES (0, 'grandparent-0')`)
	grandparent := sqlDB.Query(t, `CREATE CHANGEFEED FOR grandparent`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, grandparent)
	assertPayloads(t, grandparent, []string{
		`grandparent: [0]->{"a": 0, "b": "grandparent-0"}`,
	})

	sqlDB.Exec(t,
		`CREATE TABLE parent (a INT PRIMARY KEY, b STRING) INTERLEAVE IN PARENT grandparent (a)`)
	sqlDB.Exec(t, `INSERT INTO grandparent VALUES (1, 'grandparent-1')`)
	sqlDB.Exec(t, `INSERT INTO parent VALUES (1, 'parent-1')`)
	parent := sqlDB.Query(t, `CREATE CHANGEFEED FOR parent`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, parent)
	assertPayloads(t, grandparent, []string{
		`grandparent: [1]->{"a": 1, "b": "grandparent-1"}`,
	})
	assertPayloads(t, parent, []string{
		`parent: [1]->{"a": 1, "b": "parent-1"}`,
	})

	sqlDB.Exec(t,
		`CREATE TABLE child (a INT PRIMARY KEY, b STRING) INTERLEAVE IN PARENT parent (a)`)
	sqlDB.Exec(t, `INSERT INTO grandparent VALUES (2, 'grandparent-2')`)
	sqlDB.Exec(t, `INSERT INTO parent VALUES (2, 'parent-2')`)
	sqlDB.Exec(t, `INSERT INTO child VALUES (2, 'child-2')`)
	child := sqlDB.Query(t, `CREATE CHANGEFEED FOR child`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, child)
	assertPayloads(t, grandparent, []string{
		`grandparent: [2]->{"a": 2, "b": "grandparent-2"}`,
	})
	assertPayloads(t, parent, []string{
		`parent: [2]->{"a": 2, "b": "parent-2"}`,
	})
	assertPayloads(t, child, []string{
		`child: [2]->{"a": 2, "b": "child-2"}`,
	})
}

func TestChangefeedColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// Table with 2 column families.
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY (a), FAMILY (b))`)
	if _, err := sqlDB.DB.Query(
		`CREATE CHANGEFEED FOR foo`,
	); !testutils.IsError(err, `exactly 1 column family`) {
		t.Errorf(`expected "exactly 1 column family" error got: %+v`, err)
	}

	// Table with a second column family added after the changefeed starts.
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, FAMILY f_a (a))`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
	bar := sqlDB.Query(t, `CREATE CHANGEFEED FOR bar`)
	defer closeFeedRowsHack(t, sqlDB, bar)
	assertPayloads(t, bar, []string{
		`bar: [0]->{"a": 0}`,
	})
	sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b STRING CREATE FAMILY f_b`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
	bar.Next()
	if err := bar.Err(); !testutils.IsError(err, `exactly 1 column family`) {
		t.Errorf(`expected "exactly 1 column family" error got: %+v`, err)
	}
}

func TestChangefeedComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	// TODO(dan): Also test a non-STORED computed column once we support them.
	sqlDB.Exec(t, `CREATE TABLE cc (
		a INT, b INT AS (a + 1) STORED, c INT AS (a + 2) STORED, PRIMARY KEY (b, a)
	)`)
	sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (1)`)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR cc`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	assertPayloads(t, rows, []string{
		`cc: [2, 1]->{"a": 1, "b": 2, "c": 3}`,
	})

	sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (10)`)
	assertPayloads(t, rows, []string{
		`cc: [11, 10]->{"a": 10, "b": 11, "c": 12}`,
	})
}

func TestChangefeedTruncateRenameDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// TODO(dan): TRUNCATE cascades, test for this too.
	sqlDB.Exec(t, `CREATE TABLE truncate (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO truncate VALUES (1)`)
	truncate := sqlDB.Query(t, `CREATE CHANGEFEED FOR truncate`)
	defer closeFeedRowsHack(t, sqlDB, truncate)
	assertPayloads(t, truncate, []string{`truncate: [1]->{"a": 1}`})
	sqlDB.Exec(t, `TRUNCATE TABLE truncate`)
	truncate.Next()
	if err := truncate.Err(); !testutils.IsError(err, `"truncate" was dropped or truncated`) {
		t.Errorf(`expected ""truncate" was dropped or truncated" error got: %+v`, err)
	}

	sqlDB.Exec(t, `CREATE TABLE rename (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO rename VALUES (1)`)
	rename := sqlDB.Query(t, `CREATE CHANGEFEED FOR rename`)
	defer closeFeedRowsHack(t, sqlDB, rename)
	assertPayloads(t, rename, []string{`rename: [1]->{"a": 1}`})
	sqlDB.Exec(t, `ALTER TABLE rename RENAME TO renamed`)
	sqlDB.Exec(t, `INSERT INTO renamed VALUES (2)`)
	rename.Next()
	if err := rename.Err(); !testutils.IsError(err, `"rename" was renamed to "renamed"`) {
		t.Errorf(`expected ""rename" was renamed to "renamed"" error got: %+v`, err)
	}

	sqlDB.Exec(t, `CREATE TABLE drop (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO drop VALUES (1)`)
	drop := sqlDB.Query(t, `CREATE CHANGEFEED FOR drop`)
	defer closeFeedRowsHack(t, sqlDB, drop)
	assertPayloads(t, drop, []string{`drop: [1]->{"a": 1}`})
	sqlDB.Exec(t, `DROP TABLE drop`)
	drop.Next()
	if err := drop.Err(); !testutils.IsError(err, `"drop" was dropped or truncated`) {
		t.Errorf(`expected ""drop" was dropped or truncated" error got: %+v`, err)
	}
}

func TestChangefeedMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// TODO(dan): Work around a race in CANCEL QUERIES (#28033) by only
	// canceling them once.
	var cancelQueriesOnce sync.Once
	closeFeedRowsCancelOnceHack := func(t *testing.T, sqlDB *sqlutils.SQLRunner, rows *gosql.Rows) {
		cancelQueriesOnce.Do(func() {
			// TODO(dan): We should just be able to close the `gosql.Rows` but
			// that currently blocks forever without this.
			sqlDB.Exec(t, `CANCEL QUERIES (
				SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'CREATE CHANGEFEED%'
			)`)
		})
		rows.Close()
	}
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

	start := timeutil.Now()
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

	if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 0 {
		t.Errorf(`expected 0 got %d`, c)
	}
	if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 0 {
		t.Errorf(`expected 0 got %d`, c)
	}
	if c := s.MustGetSQLCounter(`changefeed.emit_nanos`); c != 0 {
		t.Errorf(`expected 0 got %d`, c)
	}
	if c := s.MustGetSQLCounter(`changefeed.flushes`); c != 0 {
		t.Errorf(`expected 0 got %d`, c)
	}
	if c := s.MustGetSQLCounter(`changefeed.flush_nanos`); c != 0 {
		t.Errorf(`expected 0 got %d`, c)
	}
	if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c != noMinHighWaterSentinel {
		t.Errorf(`expected %d got %d`, noMinHighWaterSentinel, c)
	}

	foo := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, foo)
	foo.Next()
	testutils.SucceedsSoon(t, func() error {
		if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 1 {
			return errors.Errorf(`expected 1 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 11 {
			return errors.Errorf(`expected 11 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.emit_nanos`); c <= 0 {
			return errors.Errorf(`expected > 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.flushes`); c <= 0 {
			return errors.Errorf(`expected > 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.flush_nanos`); c <= 0 {
			return errors.Errorf(`expected > 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c <= start.UnixNano() {
			return errors.Errorf(`expected > %d got %d`, start.UnixNano(), c)
		}
		return nil
	})

	// Check that two changefeeds add correctly.
	sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
	fooCopy := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, fooCopy)
	fooCopy.Next()
	testutils.SucceedsSoon(t, func() error {
		if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 4 {
			return errors.Errorf(`expected 4 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 44 {
			return errors.Errorf(`expected 44 got %d`, c)
		}
		return nil
	})

	// Not reading from fooTimestamps will backpressure the changefeed and the
	// min_high_water will stagnate.
	fooTimestamps := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH resolved`)
	defer closeFeedRowsCancelOnceHack(t, sqlDB, fooTimestamps)
	stalled := s.MustGetSQLCounter(`changefeed.min_high_water`)
	for i := 0; i < 100; {
		i++
		newMinResolved := s.MustGetSQLCounter(`changefeed.min_high_water`)
		if newMinResolved != stalled {
			stalled = newMinResolved
			i = 0
		}
	}
	// Reading until a resolved timestamp past that should updated the
	// min_high_water.
	fooTimestamps.Next()
	fooTimestamps.Next()
	expectResolvedTimestampGreaterThan(t, fooTimestamps, fmt.Sprintf("%d.0", stalled))
	testutils.SucceedsSoon(t, func() error {
		if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c <= stalled {
			return errors.Errorf(`expected > %d got %d`, stalled, c)
		}
		return nil
	})

	// Cancel all the changefeeds and check that the min_high_water returns to the
	// no high-water sentinel.
	cancelQueriesOnce.Do(func() {
		// TODO(dan): We should just be able to close the `gosql.Rows` but
		// that currently blocks forever without this.
		sqlDB.Exec(t, `CANCEL QUERIES (
			SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'CREATE CHANGEFEED%'
		)`)
	})
	testutils.SucceedsSoon(t, func() error {
		if c := s.MustGetSQLCounter(`changefeed.min_high_water`); c != noMinHighWaterSentinel {
			return errors.Errorf(`expected %d got %d`, noMinHighWaterSentinel, c)
		}
		return nil
	})
}

func TestChangefeedRetryableSinkError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	var failSink int64
	failSinkHook := func() error {
		if atomic.LoadInt64(&failSink) != 0 {
			return retryableSinkError{cause: fmt.Errorf("synthetic retryable error")}
		}
		return nil
	}
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &distsqlrun.TestingKnobs{
				Changefeed: &TestingKnobs{
					AfterSinkFlush: failSinkHook,
				},
			},
		},
		UseDatabase: "d",
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	// Create original data table.
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `CREATE USER sinkuser WITH PASSWORD sink`)
	sqlDB.Exec(t, `GRANT ALL ON DATABASE d TO sinkuser`)

	// Create changefeed into SQL Sink.
	row := sqlDB.QueryRow(t, fmt.Sprintf(`CREATE CHANGEFEED FOR foo INTO 'experimental-sql://sinkuser:sink@%s/d'`, s.ServingAddr()))
	var jobID string
	row.Scan(&jobID)

	// Insert initial rows into bank table.
	for i := 0; i < 50; i++ {
		sqlDB.Exec(t, `INSERT INTO d.foo VALUES($1, $2)`, i, fmt.Sprintf("value %d", i))
	}
	// Set SQL Sink to return a retryable error.
	atomic.StoreInt64(&failSink, 1)

	// Insert set of rows while sink if failing.
	for i := 50; i < 100; i++ {
		sqlDB.Exec(t, `INSERT INTO d.foo VALUES($1, $2)`, i, fmt.Sprintf("value %d", i))
	}

	// Verify that sink is failing requests.
	registry := s.JobRegistry().(*jobs.Registry)
	retryCounter := registry.MetricsStruct().Changefeed.(*Metrics).SinkErrorRetries
	testutils.SucceedsSoon(t, func() error {
		if retryCounter.Counter.Count() < 3 {
			return fmt.Errorf("insufficient sink error retries detected")
		}
		return nil
	})
	atomic.StoreInt64(&failSink, 0)
	for i := 100; i < 150; i++ {
		sqlDB.Exec(t, `INSERT INTO d.foo VALUES($1, $2)`, i, fmt.Sprintf("value %d", i))
	}

	validator := Validators{
		NewOrderValidator(`foo`),
		NewFingerprintValidator(sqlDB.DB, `foo`, `fprint`, []string{`pgwire`}),
	}
	rows := sqlDB.Query(t, "SELECT topic, key, value FROM d.sqlsink")
	for rows.Next() {
		var topic gosql.NullString
		var key, value []byte
		if err := rows.Scan(&topic, &key, &value); err != nil {
			t.Fatal(err)
		}

		updated, resolved, err := ParseJSONValueTimestamps(value)
		if err != nil {
			t.Fatal(err)
		}

		if topic.Valid {
			validator.NoteRow(`pgwire`, string(key), string(value), updated)
		} else {
			if err := validator.NoteResolved(`pgwire`, resolved); err != nil {
				t.Fatal(err)
			}
		}

		for _, f := range validator.Failures() {
			t.Error(f)
		}
	}

	sqlDB.Exec(t, `CANCEL JOB $1`, jobID)
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo WITH format=avro`,
	); !testutils.IsError(err, `format=avro is not yet supported`) {
		t.Errorf(`expected 'format=avro is not yet supported' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo WITH format=nope`,
	); !testutils.IsError(err, `unknown format: nope`) {
		t.Errorf(`expected 'unknown format: nope' error got: %+v`, err)
	}

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo WITH envelope=diff`,
	); !testutils.IsError(err, `envelope=diff is not yet supported`) {
		t.Errorf(`expected 'envelope=diff is not yet supported' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo WITH envelope=nope`,
	); !testutils.IsError(err, `unknown envelope: nope`) {
		t.Errorf(`expected 'unknown envelope: nope' error got: %+v`, err)
	}

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO ''`,
	); !testutils.IsError(err, `omit the SINK clause`) {
		t.Errorf(`expected 'omit the SINK clause' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, ``,
	); !testutils.IsError(err, `omit the SINK clause`) {
		t.Errorf(`expected 'omit the SINK clause' error got: %+v`, err)
	}

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope`,
	); !testutils.IsError(err, `use of CHANGEFEED requires an enterprise license`) {
		t.Errorf(`expected 'use of CHANGEFEED requires an enterprise license' error got: %+v`, err)
	}

	// Watching system.jobs would create a cycle, since the resolved timestamp
	// high-water mark is saved in it.
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR system.jobs`,
	); !testutils.IsError(err, `CHANGEFEEDs are not supported on system tables`) {
		t.Errorf(`expected 'CHANGEFEEDs are not supported on system tables' error got: %+v`, err)
	}
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR bar`,
	); !testutils.IsError(err, `table "bar" does not exist`) {
		t.Errorf(`expected 'table "bar" does not exist' error got: %+v`, err)
	}
	sqlDB.Exec(t, `CREATE SEQUENCE seq`)
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR seq`,
	); !testutils.IsError(err, `CHANGEFEED cannot target sequences: seq`) {
		t.Errorf(`expected 'CHANGEFEED cannot target sequences: seq' error got: %+v`, err)
	}
	sqlDB.Exec(t, `CREATE VIEW vw AS SELECT a, b FROM foo`)
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR vw`,
	); !testutils.IsError(err, `CHANGEFEED cannot target views: vw`) {
		t.Errorf(`expected 'CHANGEFEED cannot target views: vw' error got: %+v`, err)
	}
	// Backup has the same bad error message #28170.
	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR information_schema.tables`,
	); !testutils.IsError(err, `table "information_schema.tables" does not exist`) {
		t.Errorf(`expected 'table "information_schema.tables" does not exist' error got: %+v`, err)
	}
}

func TestChangefeedPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `CREATE USER testuser`)

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingAddr(), "TestChangefeedPermissions-testuser", url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	if _, err := testuser.Exec(
		`CREATE CHANGEFEED FOR foo`,
	); !testutils.IsError(err, `only superusers are allowed to CREATE CHANGEFEED`) {
		t.Errorf(`expected 'only superusers are allowed to CREATE CHANGEFEED' error got: %+v`, err)
	}
}

func assertPayloads(t *testing.T, rows *gosql.Rows, expected []string) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) && rows.Next() {
		var topic gosql.NullString
		var key, value []byte
		if err := rows.Scan(&topic, &key, &value); err != nil {
			t.Fatalf(`%+v`, err)
		}
		if !topic.Valid {
			// Ignore resolved timestamp notifications.
			continue
		}
		actual = append(actual, fmt.Sprintf(`%s: %s->%s`, topic.String, key, value))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf(`%+v`, err)
	}

	// The tests that use this aren't concerned with order, just that these are
	// the next len(expected) messages.
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func skipResolvedTimestamps(t *testing.T, rows *gosql.Rows) {
	for rows.Next() {
		var table gosql.NullString
		var key, value []byte
		if err := rows.Scan(&table, &key, &value); err != nil {
			t.Fatal(err)
		}
		if table.Valid {
			t.Errorf(`unexpected row %s: %s->%s`, table.String, key, value)
		}
	}
}

func expectResolvedTimestampGreaterThan(t testing.TB, rows *gosql.Rows, ts string) {
	t.Helper()
	for {
		if !rows.Next() {
			t.Fatal(`expected a resolved timestamp notification`)
		}
		var ignored interface{}
		var value []byte
		if err := rows.Scan(&ignored, &ignored, &value); err != nil {
			t.Fatal(err)
		}

		var valueRaw struct {
			CRDB struct {
				Resolved string `json:"resolved"`
			} `json:"__crdb__"`
		}
		if err := gojson.Unmarshal(value, &valueRaw); err != nil {
			t.Fatal(err)
		}

		parseTimeToDecimal := func(s string) *apd.Decimal {
			t.Helper()
			d, _, err := apd.NewFromString(s)
			if err != nil {
				t.Fatal(err)
			}
			return d
		}
		if parseTimeToDecimal(valueRaw.CRDB.Resolved).Cmp(parseTimeToDecimal(ts)) > 0 {
			break
		}
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
