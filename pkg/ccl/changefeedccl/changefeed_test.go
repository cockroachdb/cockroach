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
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
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
	sqlDB.QueryRow(t,
		`BEGIN; INSERT INTO foo VALUES (0); SELECT cluster_logical_timestamp(); COMMIT`,
	).Scan(&ts0)

	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH updated, resolved`)
	defer closeFeedRowsHack(t, sqlDB, rows)

	var ts1 string
	sqlDB.QueryRow(t,
		`BEGIN; INSERT INTO foo VALUES (1); SELECT cluster_logical_timestamp(); COMMIT`,
	).Scan(&ts1)

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
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)

	var start string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&start)
	sqlDB.Exec(t, `INSERT INTO foo (a, b) VALUES (0, '0')`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (1)`)
	sqlDB.Exec(t, `ALTER TABLE foo ALTER COLUMN b SET DEFAULT 'after'`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (2)`)
	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN c INT`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (3)`)
	sqlDB.Exec(t, `INSERT INTO foo (a, c) VALUES (4, 14)`)
	rows := sqlDB.Query(t, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, start)
	defer closeFeedRowsHack(t, sqlDB, rows)
	assertPayloads(t, rows, []string{
		`foo: [0]->{"a": 0, "b": "0"}`,
		`foo: [1]->{"a": 1, "b": "before"}`,
		`foo: [2]->{"a": 2, "b": "after"}`,
		`foo: [3]->{"a": 3, "b": "after", "c": null}`,
		`foo: [4]->{"a": 4, "b": "after", "c": 14}`,
	})

	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d INT`)
	sqlDB.Exec(t, `INSERT INTO foo (a, d) VALUES (5, 15)`)
	assertPayloads(t, rows, []string{
		`foo: [5]->{"a": 5, "b": "after", "c": null, "d": 15}`,
	})

	// TODO(dan): Test a schema change that uses a backfill once we figure out
	// the user facing semantics of that.
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

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

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
