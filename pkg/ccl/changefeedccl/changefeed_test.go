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
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2, "b": "c"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "d"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))

	// NB running TestChangefeedBasics, which includes a DELETE, with
	// cloudStorageTest is a regression test for #36994.
}

func TestChangefeedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

		t.Run(`envelope=row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='row'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`foo: [1]->{"a": 1, "b": "a"}`})
		})
		t.Run(`envelope=deprecated_row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='deprecated_row'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`foo: [1]->{"a": 1, "b": "a"}`})
		})
		t.Run(`envelope=key_only`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='key_only'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`foo: [1]->`})
		})
		t.Run(`envelope=wrapped`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope='wrapped'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`foo: [1]->{"after": {"a": 1, "b": "a"}}`})
		})
		t.Run(`envelope=wrapped,key_in_value`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH key_in_value, envelope='wrapped'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`foo: [1]->{"after": {"a": 1, "b": "a"}, "key": [1]}`})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'b')`)

		fooAndBar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, fooAndBar)

		assertPayloads(t, fooAndBar, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`bar: [2]->{"after": {"a": 2, "b": "b"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		// To make sure that these timestamps are after 'before' and before
		// 'after', throw a couple sleeps around them. We round timestamps to
		// Microsecond granularity for Postgres compatibility, so make the
		// sleeps 10x that.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
		time.Sleep(10 * time.Microsecond)

		var tsLogical string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)
		var tsClock time.Time
		sqlDB.QueryRow(t, `SELECT clock_timestamp()`).Scan(&tsClock)

		time.Sleep(10 * time.Microsecond)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

		fooLogical := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, tsLogical)
		defer closeFeed(t, fooLogical)
		assertPayloads(t, fooLogical, []string{
			`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		nanosStr := strconv.FormatInt(tsClock.UnixNano(), 10)
		fooNanosStr := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, nanosStr)
		defer closeFeed(t, fooNanosStr)
		assertPayloads(t, fooNanosStr, []string{
			`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		timeStr := tsClock.Format(`2006-01-02 15:04:05.999999`)
		fooString := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, timeStr)
		defer closeFeed(t, fooString)
		assertPayloads(t, fooString, []string{
			`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})

		// Check that the cursor is properly hooked up to the job statement
		// time. The sinkless tests currently don't have a way to get the
		// statement timestamp, so only verify this for enterprise.
		if e, ok := fooLogical.(*cdctest.TableFeed); ok {
			var bytes []byte
			sqlDB.QueryRow(t, `SELECT payload FROM system.jobs WHERE id=$1`, e.JobID).Scan(&bytes)
			var payload jobspb.Payload
			require.NoError(t, protoutil.Unmarshal(bytes, &payload))
			require.Equal(t, parseTimeToHLC(t, tsLogical), payload.GetChangefeed().StatementTime)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated, resolved`)
		defer closeFeed(t, foo)

		// Grab the first non resolved-timestamp row.
		var row0 *cdctest.TestFeedMessage
		for {
			var err error
			row0, err = foo.Next()
			assert.NoError(t, err)
			if len(row0.Value) > 0 {
				break
			}
		}

		// If this changefeed uses jobs (and thus stores a ChangefeedDetails), get
		// the statement timestamp from row0 and verify that they match. Otherwise,
		// just skip the row.
		if !strings.Contains(t.Name(), `sinkless`) {
			d, err := foo.(*cdctest.TableFeed).Details()
			assert.NoError(t, err)
			expected := `{"after": {"a": 0}, "updated": "` + d.StatementTime.AsOfSystemTime() + `"}`
			assert.Equal(t, expected, string(row0.Value))
		}

		// Assert the remaining key using assertPayloads, since we know the exact
		// timestamp expected.
		var ts1 string
		if err := crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (1) RETURNING cluster_logical_timestamp()`,
			).Scan(&ts1)
		}); err != nil {
			t.Fatal(err)
		}
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}, "updated": "` + ts1 + `"}`,
		})

		// Check that we eventually get a resolved timestamp greater than ts1.
		parsed := parseTimeToHLC(t, ts1)
		for {
			if resolved := expectResolvedTimestamp(t, foo); parsed.Less(resolved) {
				break
			}
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedResolvedFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		const freq = 10 * time.Millisecond
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved=$1`, freq.String())
		defer closeFeed(t, foo)

		// We get each resolved timestamp notification once in each partition.
		// Grab the first `2 * #partitions`, sort because we might get all from
		// one partition first, and compare the first and last.
		resolved := make([]hlc.Timestamp, 2*len(foo.Partitions()))
		for i := range resolved {
			resolved[i] = expectResolvedTimestamp(t, foo)
		}
		sort.Slice(resolved, func(i, j int) bool { return resolved[i].Less(resolved[j]) })
		first, last := resolved[0], resolved[len(resolved)-1]

		if d := last.GoTime().Sub(first.GoTime()); d < freq {
			t.Errorf(`expected %s between resolved timestamps, but got %s`, freq, d)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedSchemaChangeNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		// Schema changes that predate the changefeed.
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
			historical := feed(t, f, `CREATE CHANGEFEED FOR historical WITH cursor=$1`, start)
			defer closeFeed(t, historical)
			assertPayloads(t, historical, []string{
				`historical: [0]->{"after": {"a": 0, "b": "0"}}`,
				`historical: [1]->{"after": {"a": 1, "b": "before"}}`,
				`historical: [2]->{"after": {"a": 2, "b": "after"}}`,
				`historical: [3]->{"after": {"a": 3, "b": "after", "c": null}}`,
				`historical: [4]->{"after": {"a": 4, "b": "after", "c": 14}}`,
			})
		})

		t.Run(`add column`, func(t *testing.T) {
			// NB: the default is a nullable column
			sqlDB.Exec(t, `CREATE TABLE add_column (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column VALUES (1)`)
			addColumn := feed(t, f, `CREATE CHANGEFEED FOR add_column`)
			defer closeFeed(t, addColumn)
			assertPayloads(t, addColumn, []string{
				`add_column: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column ADD COLUMN b STRING`)
			sqlDB.Exec(t, `INSERT INTO add_column VALUES (2, '2')`)
			assertPayloads(t, addColumn, []string{
				`add_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`rename column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rename_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO rename_column VALUES (1, '1')`)
			renameColumn := feed(t, f, `CREATE CHANGEFEED FOR rename_column`)
			defer closeFeed(t, renameColumn)
			assertPayloads(t, renameColumn, []string{
				`rename_column: [1]->{"after": {"a": 1, "b": "1"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rename_column RENAME COLUMN b TO c`)
			sqlDB.Exec(t, `INSERT INTO rename_column VALUES (2, '2')`)
			assertPayloads(t, renameColumn, []string{
				`rename_column: [2]->{"after": {"a": 2, "c": "2"}}`,
			})
		})

		t.Run(`add default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_default (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO add_default (a, b) VALUES (1, '1')`)
			addDefault := feed(t, f, `CREATE CHANGEFEED FOR add_default`)
			defer closeFeed(t, addDefault)
			sqlDB.Exec(t, `ALTER TABLE add_default ALTER COLUMN b SET DEFAULT 'd'`)
			sqlDB.Exec(t, `INSERT INTO add_default (a) VALUES (2)`)
			assertPayloads(t, addDefault, []string{
				`add_default: [1]->{"after": {"a": 1, "b": "1"}}`,
				`add_default: [2]->{"after": {"a": 2, "b": "d"}}`,
			})
		})

		t.Run(`drop default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_default (a INT PRIMARY KEY, b STRING DEFAULT 'd')`)
			sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (1)`)
			dropDefault := feed(t, f, `CREATE CHANGEFEED FOR drop_default`)
			defer closeFeed(t, dropDefault)
			sqlDB.Exec(t, `ALTER TABLE drop_default ALTER COLUMN b DROP DEFAULT`)
			sqlDB.Exec(t, `INSERT INTO drop_default (a) VALUES (2)`)
			assertPayloads(t, dropDefault, []string{
				`drop_default: [1]->{"after": {"a": 1, "b": "d"}}`,
				`drop_default: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})

		t.Run(`drop not null`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_notnull (a INT PRIMARY KEY, b STRING NOT NULL)`)
			sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (1, '1')`)
			dropNotNull := feed(t, f, `CREATE CHANGEFEED FOR drop_notnull`)
			defer closeFeed(t, dropNotNull)
			sqlDB.Exec(t, `ALTER TABLE drop_notnull ALTER b DROP NOT NULL`)
			sqlDB.Exec(t, `INSERT INTO drop_notnull VALUES (2, NULL)`)
			assertPayloads(t, dropNotNull, []string{
				`drop_notnull: [1]->{"after": {"a": 1, "b": "1"}}`,
				`drop_notnull: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})

		t.Run(`checks`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE checks (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (1)`)
			checks := feed(t, f, `CREATE CHANGEFEED FOR checks`)
			defer closeFeed(t, checks)
			sqlDB.Exec(t, `ALTER TABLE checks ADD CONSTRAINT c CHECK (a < 5) NOT VALID`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (2)`)
			sqlDB.Exec(t, `ALTER TABLE checks VALIDATE CONSTRAINT c`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (3)`)
			sqlDB.Exec(t, `ALTER TABLE checks DROP CONSTRAINT c`)
			sqlDB.Exec(t, `INSERT INTO checks VALUES (6)`)
			assertPayloads(t, checks, []string{
				`checks: [1]->{"after": {"a": 1}}`,
				`checks: [2]->{"after": {"a": 2}}`,
				`checks: [3]->{"after": {"a": 3}}`,
				`checks: [6]->{"after": {"a": 6}}`,
			})
		})

		t.Run(`add index`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, '1')`)
			addIndex := feed(t, f, `CREATE CHANGEFEED FOR add_index`)
			defer closeFeed(t, addIndex)
			sqlDB.Exec(t, `CREATE INDEX b_idx ON add_index (b)`)
			sqlDB.Exec(t, `SELECT * FROM add_index@b_idx`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, '2')`)
			assertPayloads(t, addIndex, []string{
				`add_index: [1]->{"after": {"a": 1, "b": "1"}}`,
				`add_index: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`unique`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE "unique" (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO "unique" VALUES (1, '1')`)
			unique := feed(t, f, `CREATE CHANGEFEED FOR "unique"`)
			defer closeFeed(t, unique)
			sqlDB.Exec(t, `ALTER TABLE "unique" ADD CONSTRAINT u UNIQUE (b)`)
			sqlDB.Exec(t, `INSERT INTO "unique" VALUES (2, '2')`)
			assertPayloads(t, unique, []string{
				`unique: [1]->{"after": {"a": 1, "b": "1"}}`,
				`unique: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
		})

		t.Run(`alter default`, func(t *testing.T) {
			sqlDB.Exec(
				t, `CREATE TABLE alter_default (a INT PRIMARY KEY, b STRING DEFAULT 'before')`)
			sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (1)`)
			alterDefault := feed(t, f, `CREATE CHANGEFEED FOR alter_default`)
			defer closeFeed(t, alterDefault)
			sqlDB.Exec(t, `ALTER TABLE alter_default ALTER COLUMN b SET DEFAULT 'after'`)
			sqlDB.Exec(t, `INSERT INTO alter_default (a) VALUES (2)`)
			assertPayloads(t, alterDefault, []string{
				`alter_default: [1]->{"after": {"a": 1, "b": "before"}}`,
				`alter_default: [2]->{"after": {"a": 2, "b": "after"}}`,
			})
		})

		// Test adding a column with explicitly setting the default value to be NULL
		t.Run(`add column with DEFAULT NULL`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE t (id INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO t VALUES (1)`)
			defaultNull := feed(t, f, `CREATE CHANGEFEED FOR t`)
			defer closeFeed(t, defaultNull)
			sqlDB.Exec(t, `ALTER TABLE t ADD COLUMN c INT DEFAULT NULL`)
			sqlDB.Exec(t, `INSERT INTO t VALUES (2, 2)`)
			assertPayloads(t, defaultNull, []string{
				// Verify that no column backfill occurs
				`t: [1]->{"after": {"id": 1}}`,
				`t: [2]->{"after": {"c": 2, "id": 2}}`,
			})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

func TestChangefeedSchemaChangeNoAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(mrtracy): Re-enable when allow-backfill option is added.")

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run(`add column not null`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_notnull (a INT PRIMARY KEY)`)
			addColumnNotNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_notnull WITH resolved`)
			defer closeFeed(t, addColumnNotNull)
			sqlDB.Exec(t, `ALTER TABLE add_column_notnull ADD COLUMN b STRING NOT NULL`)
			sqlDB.Exec(t, `INSERT INTO add_column_notnull VALUES (2, '2')`)
			skipResolvedTimestamps(t, addColumnNotNull)
			if _, err := addColumnNotNull.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def`)
			defer closeFeed(t, addColumnDef)
			assertPayloads(t, addColumnDef, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2, '2')`)
			if _, err := addColumnDef.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp`)
			defer closeFeed(t, addColComp)
			assertPayloads(t, addColComp, []string{
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			if _, err := addColComp.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column`)
			defer closeFeed(t, dropColumn)
			assertPayloads(t, dropColumn, []string{
				`drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)
			if _, err := dropColumn.Next(); !testutils.IsError(err, `tables being backfilled`) {
				t.Fatalf(`expected "tables being backfilled" error got: %+v`, err)
			}
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

// Test schema changes that require a backfill when the backfill option is
// allowed.
func TestChangefeedSchemaChangeAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def`)
			defer closeFeed(t, addColumnDef)
			assertPayloads(t, addColumnDef, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			assertPayloads(t, addColumnDef, []string{
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `add_column_def: [1]->{"after": {"a": 1}}`,
				// `add_column_def: [2]->{"after": {"a": 2}}`,
				`add_column_def: [1]->{"after": {"a": 1, "b": "d"}}`,
				`add_column_def: [2]->{"after": {"a": 2, "b": "d"}}`,
			})
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp`)
			defer closeFeed(t, addColComp)
			assertPayloads(t, addColComp, []string{
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				`add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			assertPayloads(t, addColComp, []string{
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				// `add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6, "c": 11}}`,
				`add_col_comp: [2]->{"after": {"a": 2, "b": 7, "c": 12}}`,
			})
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2, '2')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column`)
			defer closeFeed(t, dropColumn)
			assertPayloads(t, dropColumn, []string{
				`drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
				`drop_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (3)`)
			// Dropped columns are immediately invisible.
			assertPayloads(t, dropColumn, []string{
				`drop_column: [1]->{"after": {"a": 1}}`,
				`drop_column: [2]->{"after": {"a": 2}}`,
				`drop_column: [3]->{"after": {"a": 3}}`,
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `drop_column: [1]->{"after": {"a": 1}}`,
				// `drop_column: [2]->{"after": {"a": 2}}`,
			})
		})

		t.Run(`multiple alters`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE multiple_alters (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (2, '2')`)

			// Set up a hook to pause the changfeed on the next emit.
			var wg sync.WaitGroup
			waitSinkHook := func(_ context.Context) error {
				wg.Wait()
				return nil
			}
			knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = waitSinkHook

			multipleAlters := feed(t, f, `CREATE CHANGEFEED FOR multiple_alters`)
			defer closeFeed(t, multipleAlters)
			assertPayloads(t, multipleAlters, []string{
				`multiple_alters: [1]->{"after": {"a": 1, "b": "1"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "b": "2"}}`,
			})

			// Wait on the next emit, queue up three ALTERs. The next poll process
			// will see all of them at once.
			wg.Add(1)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters DROP COLUMN b`)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters ADD COLUMN c STRING DEFAULT 'cee'`)
			waitForSchemaChange(t, sqlDB, `ALTER TABLE multiple_alters ADD COLUMN d STRING DEFAULT 'dee'`)
			wg.Done()

			assertPayloads(t, multipleAlters, []string{
				// Backfill no-ops for DROP. Dropped columns are immediately invisible.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
				// Scan output for DROP
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `multiple_alters: [1]->{"after": {"a": 1}}`,
				// `multiple_alters: [2]->{"after": {"a": 2}}`,
				// Backfill no-ops for column C
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `multiple_alters: [1]->{"after": {"a": 1}}`,
				// `multiple_alters: [2]->{"after": {"a": 2}}`,
				// Scan output for column C
				`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
				// Backfill no-ops for column D (C schema change is complete)
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// `multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				// `multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
				// Scan output for column C
				`multiple_alters: [1]->{"after": {"a": 1, "c": "cee", "d": "dee"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "c": "cee", "d": "dee"}}`,
			})
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// Regression test for #34314
func TestChangefeedAfterSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE after_backfill (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO after_backfill VALUES (0)`)
		sqlDB.Exec(t, `ALTER TABLE after_backfill ADD COLUMN b INT DEFAULT 1`)
		sqlDB.Exec(t, `INSERT INTO after_backfill VALUES (2, 3)`)
		afterBackfill := feed(t, f, `CREATE CHANGEFEED FOR after_backfill`)
		defer closeFeed(t, afterBackfill)
		assertPayloads(t, afterBackfill, []string{
			`after_backfill: [0]->{"after": {"a": 0, "b": 1}}`,
			`after_backfill: [2]->{"after": {"a": 2, "b": 3}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

func TestChangefeedInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		sqlDB.Exec(t, `CREATE TABLE grandparent (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (0, 'grandparent-0')`)
		grandparent := feed(t, f, `CREATE CHANGEFEED FOR grandparent`)
		defer closeFeed(t, grandparent)
		assertPayloads(t, grandparent, []string{
			`grandparent: [0]->{"after": {"a": 0, "b": "grandparent-0"}}`,
		})

		sqlDB.Exec(t,
			`CREATE TABLE parent (a INT PRIMARY KEY, b STRING) `+
				`INTERLEAVE IN PARENT grandparent (a)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (1, 'grandparent-1')`)
		sqlDB.Exec(t, `INSERT INTO parent VALUES (1, 'parent-1')`)
		parent := feed(t, f, `CREATE CHANGEFEED FOR parent`)
		defer closeFeed(t, parent)
		assertPayloads(t, grandparent, []string{
			`grandparent: [1]->{"after": {"a": 1, "b": "grandparent-1"}}`,
		})
		assertPayloads(t, parent, []string{
			`parent: [1]->{"after": {"a": 1, "b": "parent-1"}}`,
		})

		sqlDB.Exec(t,
			`CREATE TABLE child (a INT PRIMARY KEY, b STRING) INTERLEAVE IN PARENT parent (a)`)
		sqlDB.Exec(t, `INSERT INTO grandparent VALUES (2, 'grandparent-2')`)
		sqlDB.Exec(t, `INSERT INTO parent VALUES (2, 'parent-2')`)
		sqlDB.Exec(t, `INSERT INTO child VALUES (2, 'child-2')`)
		child := feed(t, f, `CREATE CHANGEFEED FOR child`)
		defer closeFeed(t, child)
		assertPayloads(t, grandparent, []string{
			`grandparent: [2]->{"after": {"a": 2, "b": "grandparent-2"}}`,
		})
		assertPayloads(t, parent, []string{
			`parent: [2]->{"after": {"a": 2, "b": "parent-2"}}`,
		})
		assertPayloads(t, child, []string{
			`child: [2]->{"after": {"a": 2, "b": "child-2"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY (a), FAMILY (b))`)
		if strings.Contains(t.Name(), `enterprise`) {
			sqlDB.ExpectErr(t, `exactly 1 column family`, `CREATE CHANGEFEED FOR foo`)
		} else {
			sqlDB.ExpectErr(t, `exactly 1 column family`, `EXPERIMENTAL CHANGEFEED FOR foo`)
		}

		// Table with a second column family added after the changefeed starts.
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, FAMILY f_a (a))`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
		defer closeFeed(t, bar)
		assertPayloads(t, bar, []string{
			`bar: [0]->{"after": {"a": 0}}`,
		})
		sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b STRING CREATE FAMILY f_b`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
		if _, err := bar.Next(); !testutils.IsError(err, `exactly 1 column family`) {
			t.Errorf(`expected "exactly 1 column family" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// TODO(dan): Also test a non-STORED computed column once we support them.
		sqlDB.Exec(t, `CREATE TABLE cc (
		a INT, b INT AS (a + 1) STORED, c INT AS (a + 2) STORED, PRIMARY KEY (b, a)
	)`)
		sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (1)`)

		cc := feed(t, f, `CREATE CHANGEFEED FOR cc`)
		defer closeFeed(t, cc)

		assertPayloads(t, cc, []string{
			`cc: [2, 1]->{"after": {"a": 1, "b": 2, "c": 3}}`,
		})

		sqlDB.Exec(t, `INSERT INTO cc (a) VALUES (10)`)
		assertPayloads(t, cc, []string{
			`cc: [11, 10]->{"after": {"a": 10, "b": 11, "c": 12}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedUpdatePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// This NOT NULL column checks a regression when used with UPDATE-ing a
		// primary key column or with DELETE.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'bar')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "bar"}}`,
		})

		sqlDB.Exec(t, `UPDATE foo SET a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": null}`,
			`foo: [1]->{"after": {"a": 1, "b": "bar"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTruncateRenameDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		sqlDB.Exec(t, `CREATE TABLE truncate (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE truncate_cascade (b INT PRIMARY KEY REFERENCES truncate (a))`)
		sqlDB.Exec(t,
			`BEGIN; INSERT INTO truncate VALUES (1); INSERT INTO truncate_cascade VALUES (1); COMMIT`)
		truncate := feed(t, f, `CREATE CHANGEFEED FOR truncate`)
		defer closeFeed(t, truncate)
		truncateCascade := feed(t, f, `CREATE CHANGEFEED FOR truncate_cascade`)
		defer closeFeed(t, truncateCascade)
		assertPayloads(t, truncate, []string{`truncate: [1]->{"after": {"a": 1}}`})
		assertPayloads(t, truncateCascade, []string{`truncate_cascade: [1]->{"after": {"b": 1}}`})
		sqlDB.Exec(t, `TRUNCATE TABLE truncate CASCADE`)
		if _, err := truncate.Next(); !testutils.IsError(err, `"truncate" was dropped or truncated`) {
			t.Errorf(`expected ""truncate" was dropped or truncated" error got: %+v`, err)
		}
		if _, err := truncateCascade.Next(); !testutils.IsError(
			err, `"truncate_cascade" was dropped or truncated`,
		) {
			t.Errorf(`expected ""truncate_cascade" was dropped or truncated" error got: %+v`, err)
		}

		sqlDB.Exec(t, `CREATE TABLE rename (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO rename VALUES (1)`)
		rename := feed(t, f, `CREATE CHANGEFEED FOR rename`)
		defer closeFeed(t, rename)
		assertPayloads(t, rename, []string{`rename: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `ALTER TABLE rename RENAME TO renamed`)
		sqlDB.Exec(t, `INSERT INTO renamed VALUES (2)`)
		if _, err := rename.Next(); !testutils.IsError(err, `"rename" was renamed to "renamed"`) {
			t.Errorf(`expected ""rename" was renamed to "renamed"" error got: %+v`, err)
		}

		sqlDB.Exec(t, `CREATE TABLE drop (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO drop VALUES (1)`)
		drop := feed(t, f, `CREATE CHANGEFEED FOR drop`)
		defer closeFeed(t, drop)
		assertPayloads(t, drop, []string{`drop: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `DROP TABLE drop`)
		if _, err := drop.Next(); !testutils.IsError(err, `"drop" was dropped or truncated`) {
			t.Errorf(`expected ""drop" was dropped or truncated" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		beforeEmitRowCh := make(chan struct{}, 2)
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			<-beforeEmitRowCh
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		s := f.Server()
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
		if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
			t.Errorf(`expected %d got %d`, 0, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.buffer_entries.in`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.MustGetSQLCounter(`changefeed.buffer_entries.out`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}

		beforeEmitRowCh <- struct{}{}
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = foo.Next()
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
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
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.buffer_entries.in`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.buffer_entries.out`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			return nil
		})

		// Not reading from foo will backpressure it and max_behind_nanos will grow.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		const expectedLatency = 100 * time.Millisecond
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = $1`,
			(expectedLatency / 3).String())
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.close_fraction = 1.0`)

		testutils.SucceedsSoon(t, func() error {
			waitForBehindNanos := 2 * expectedLatency.Nanoseconds()
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c < waitForBehindNanos {
				return errors.Errorf(
					`waiting for the feed to be > %d nanos behind got %d`, waitForBehindNanos, c)
			}
			return nil
		})

		// Unblocking the emit should bring the max_behind_nanos back down.
		// Unfortunately, this is sensitive to how many closed timestamp updates are
		// received. If we get them too fast, it takes longer to process them then
		// they come in and we fall continually further behind. The target_duration
		// and close_fraction settings above are tuned to try to avoid this.
		close(beforeEmitRowCh)
		_, _ = foo.Next()
		testutils.SucceedsSoon(t, func() error {
			waitForBehindNanos := expectedLatency.Nanoseconds()
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c > waitForBehindNanos {
				return errors.Errorf(
					`waiting for the feed to be < %d nanos behind got %d`, waitForBehindNanos, c)
			}
			return nil
		})

		// Check that two changefeeds add correctly.
		// Set cluster settings back so we don't interfere with schema changes.
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		fooCopy := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = fooCopy.Next()
		_, _ = fooCopy.Next()
		testutils.SucceedsSoon(t, func() error {
			// We can't assert exactly 4 or 88 in case we get (allowed) duplicates
			// from RangeFeed.
			if c := s.MustGetSQLCounter(`changefeed.emitted_messages`); c < 4 {
				return errors.Errorf(`expected >= 4 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.emitted_bytes`); c < 88 {
				return errors.Errorf(`expected >= 88 got %d`, c)
			}
			return nil
		})

		// Cancel all the changefeeds and check that max_behind_nanos returns to 0.
		require.NoError(t, foo.Close())
		require.NoError(t, fooCopy.Close())
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			return nil
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, func(t *testing.T) {
		t.Skip("https://github.com/cockroachdb/cockroach/issues/38443")
		enterpriseTest(testFn)
	})
}

func TestChangefeedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		origAfterSinkFlushHook := knobs.AfterSinkFlush
		var failSink int64
		failSinkHook := func() error {
			switch atomic.LoadInt64(&failSink) {
			case 1:
				return MarkRetryableError(fmt.Errorf("synthetic retryable error"))
			case 2:
				return fmt.Errorf("synthetic terminal error")
			}
			return origAfterSinkFlushHook()
		}
		knobs.AfterSinkFlush = failSinkHook

		// Set up a new feed and verify that the sink is started up.
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		// Set sink to return unique retryable errors and insert a row. Verify that
		// sink is failing requests.
		atomic.StoreInt64(&failSink, 1)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		registry := f.Server().JobRegistry().(*jobs.Registry)
		retryCounter := registry.MetricsStruct().Changefeed.(*Metrics).ErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Counter.Count() < 3 {
				return fmt.Errorf("insufficient error retries detected")
			}
			return nil
		})

		// Fix the sink and insert another row. Check that nothing funky happened.
		atomic.StoreInt64(&failSink, 0)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		// Set sink to return a terminal error and insert a row. Ensure that we
		// eventually get the error message back out.
		atomic.StoreInt64(&failSink, 2)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)
		for {
			_, err := foo.Next()
			if err == nil {
				continue
			}
			require.EqualError(t, err, `synthetic terminal error`)
			break
		}
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
}

// TestChangefeedDataTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table data.
func TestChangefeedDataTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://github.com/cockroachdb/cockroach/issues/37154")

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(db)

		// Create the data table; it will only contain a single row with multiple
		// versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		counter := 0
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, fmt.Sprintf("version %d", counter))
		}

		// Create the initial version of the row and the changefeed itself. The initial
		// version is necessary to prevent CREATE CHANGEFEED itself from hanging.
		upsertRow()
		dataExpiredRows := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		defer closeFeed(t, dataExpiredRows)

		// Set up our emit trap and update the row, which will allow us to "pause" the
		// changefeed in order to force a GC.
		atomic.StoreInt32(&shouldWait, 1)
		upsertRow()
		<-wait

		// Upsert two additional versions. One of these will be deleted by the GC
		// process before changefeed polling is resumed.
		upsertRow()
		upsertRow()

		// Force a GC of the table. This should cause both older versions of the
		// table to be deleted, with the middle version being lost to the changefeed.
		forceTableGC(t, f.Server(), sqlDB, "d", "foo")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that, at some point, Next() returns a "must be after replica GC
		// threshold" error. In the common case, that'll be the third call, but
		// various conditions will cause RangeFeed to emit duplicates and so it may
		// be a few more.
		//
		// TODO(tbg): this should keep track of the values seen and once we have
		// observed all four (which should never happen), fail the test.
		for {
			msg, err := dataExpiredRows.Next()
			if testutils.IsError(err, `must be after replica GC threshold`) {
				break
			}
			if msg != nil {
				log.Infof(ctx, "ignoring message %s", msg)
			}
		}
	}

	t.Run("sinkless", sinklessTest(testFn))
	t.Run("enterprise", enterpriseTest(testFn))
}

// TestChangefeedSchemaTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table's schema.
func TestChangefeedSchemaTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(db)

		// Create the data table; it will only contain a single row with multiple
		// versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		counter := 0
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, fmt.Sprintf("version %d", counter))
		}

		// Create the initial version of the row and the changefeed itself. The initial
		// version is necessary to prevent CREATE CHANGEFEED itself from hanging.
		upsertRow()
		dataExpiredRows := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		defer closeFeed(t, dataExpiredRows)

		// Set up our emit trap and update the row, which will allow us to "pause" the
		// changefeed in order to force a GC.
		atomic.StoreInt32(&shouldWait, 1)
		upsertRow()
		<-wait

		// Upsert two additional versions. One of these will be deleted by the GC
		// process before changefeed polling is resumed.
		waitForSchemaChange(t, sqlDB, "ALTER TABLE foo ADD COLUMN c STRING")
		upsertRow()
		waitForSchemaChange(t, sqlDB, "ALTER TABLE foo ADD COLUMN d STRING")
		upsertRow()

		// Force a GC of the table. This should cause both older versions of the
		// table to be deleted, with the middle version being lost to the changefeed.
		forceTableGC(t, f.Server(), sqlDB, "system", "descriptor")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that the third call to Next() returns an error (the first is the
		// initial row, the second is the first change. The third should detect the
		// GC interval mismatch).
		_, _ = dataExpiredRows.Next()
		_, _ = dataExpiredRows.Next()
		if _, err := dataExpiredRows.Next(); !testutils.IsError(err, `GC threshold`) {
			t.Errorf(`expected "GC threshold" error got: %+v`, err)
		}
	}

	t.Run("sinkless", sinklessTest(testFn))
	t.Run("enterprise", enterpriseTest(testFn))
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	// Changefeeds default to rangefeed, but for now, rangefeed defaults to off.
	// Verify that this produces a useful error.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = false`)
	sqlDB.Exec(t, `CREATE TABLE rangefeed_off (a INT PRIMARY KEY)`)
	sqlDB.ExpectErr(
		t, `rangefeeds require the kv.rangefeed.enabled setting`,
		`EXPERIMENTAL CHANGEFEED FOR rangefeed_off`,
	)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled TO DEFAULT`)

	sqlDB.ExpectErr(
		t, `unknown format: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH format=nope`,
	)

	sqlDB.ExpectErr(
		t, `unknown envelope: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH envelope=nope`,
	)
	sqlDB.ExpectErr(
		t, `negative durations are not accepted: resolved='-1s'`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH resolved='-1s'`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify timestamp in the future`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH cursor=$1`, timeutil.Now().Add(time.Hour),
	)

	sqlDB.ExpectErr(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO ''`,
	)
	sqlDB.ExpectErr(
		t, `omit the SINK clause`,
		`CREATE CHANGEFEED FOR foo INTO $1`, ``,
	)

	enableEnterprise := utilccl.TestingDisableEnterprise()
	sqlDB.ExpectErr(
		t, `CHANGEFEED requires an enterprise license`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope`,
	)
	enableEnterprise()

	// Watching system.jobs would create a cycle, since the resolved timestamp
	// high-water mark is saved in it.
	sqlDB.ExpectErr(
		t, `not supported on system tables`,
		`EXPERIMENTAL CHANGEFEED FOR system.jobs`,
	)
	sqlDB.ExpectErr(
		t, `table "bar" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR bar`,
	)
	sqlDB.Exec(t, `CREATE SEQUENCE seq`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED cannot target sequences: seq`,
		`EXPERIMENTAL CHANGEFEED FOR seq`,
	)
	sqlDB.Exec(t, `CREATE VIEW vw AS SELECT a, b FROM foo`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED cannot target views: vw`,
		`EXPERIMENTAL CHANGEFEED FOR vw`,
	)
	// Backup has the same bad error message #28170.
	sqlDB.ExpectErr(
		t, `"information_schema.tables" does not exist`,
		`EXPERIMENTAL CHANGEFEED FOR information_schema.tables`,
	)

	// TODO(dan): These two tests shouldn't need initial data in the table
	// to pass.
	sqlDB.Exec(t, `CREATE TABLE dec (a DECIMAL PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO dec VALUES (1.0)`)
	sqlDB.ExpectErr(
		t, `pq: column a: decimal with no precision`,
		`EXPERIMENTAL CHANGEFEED FOR dec WITH format=$1, confluent_schema_registry=$2`,
		optFormatAvro, `bar`,
	)
	sqlDB.Exec(t, `CREATE TABLE "oid" (a OID PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO "oid" VALUES (3::OID)`)
	sqlDB.ExpectErr(
		t, `pq: column a: type OID not yet supported with avro`,
		`EXPERIMENTAL CHANGEFEED FOR "oid" WITH format=$1, confluent_schema_registry=$2`,
		optFormatAvro, `bar`,
	)

	// Check that confluent_schema_registry is only accepted if format is avro.
	sqlDB.ExpectErr(
		t, `unknown sink query parameter: confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?confluent_schema_registry=foo`,
	)

	// Check unavailable kafka.
	sqlDB.ExpectErr(
		t, `client has run out of available brokers`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`,
	)

	// kafka_topic_prefix was referenced by an old version of the RFC, it's
	// "topic_prefix" now.
	sqlDB.ExpectErr(
		t, `unknown sink query parameter: kafka_topic_prefix`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?kafka_topic_prefix=foo`,
	)

	// schema_topic will be implemented but isn't yet.
	sqlDB.ExpectErr(
		t, `schema_topic is not yet supported`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?schema_topic=foo`,
	)

	// Sanity check kafka tls parameters.
	sqlDB.ExpectErr(
		t, `param tls_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=foo`,
	)
	sqlDB.ExpectErr(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?ca_cert=!`,
	)
	sqlDB.ExpectErr(
		t, `ca_cert requires tls_enabled=true`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?&ca_cert=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `param client_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_cert=!`,
	)
	sqlDB.ExpectErr(
		t, `param client_key must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_key=!`,
	)
	sqlDB.ExpectErr(
		t, `client_cert requires tls_enabled=true`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?client_cert=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `client_cert requires client_key to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_cert=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `client_key requires client_cert to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_key=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `invalid client certificate`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&client_cert=Zm9v&client_key=Zm9v`,
	)

	// Sanity check kafka sasl parameters.
	sqlDB.ExpectErr(
		t, `param sasl_enabled must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=maybe`,
	)
	sqlDB.ExpectErr(
		t, `param sasl_handshake must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_handshake=maybe`,
	)
	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled to configure SASL handshake behavior`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_handshake=false`,
	)
	sqlDB.ExpectErr(
		t, `sasl_user must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true`,
	)
	sqlDB.ExpectErr(
		t, `sasl_password must be provided when SASL is enabled`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_user=a`,
	)
	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled if a SASL user is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_user=a`,
	)
	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled if a SASL password is provided`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_password=a`,
	)

	// The avro format doesn't support key_in_value yet.
	sqlDB.ExpectErr(
		t, `key_in_value is not supported with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)

	// The cloudStorageSink is particular about the options it will work with.
	sqlDB.ExpectErr(
		t, `this sink is incompatible with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`experimental-nodelocal:///bar`, `schemareg-nope`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`experimental-nodelocal:///bar`,
	)

	// WITH key_in_value requires envelope=wrapped
	sqlDB.ExpectErr(
		t, `key_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, envelope='key_only'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `key_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, envelope='row'`, `kafka://nope`,
	)
}

func TestChangefeedPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE USER testuser`)

		s := f.Server()
		pgURL, cleanupFunc := sqlutils.PGUrl(
			t, s.ServingSQLAddr(), "TestChangefeedPermissions-testuser", url.User("testuser"),
		)
		defer cleanupFunc()
		testuser, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer testuser.Close()

		stmt := `EXPERIMENTAL CHANGEFEED FOR foo`
		if strings.Contains(t.Name(), `enterprise`) {
			stmt = `CREATE CHANGEFEED FOR foo`
		}
		if _, err := testuser.Exec(stmt); !testutils.IsError(err, `only users with the admin role`) {
			t.Errorf(`expected 'only users with the admin role' error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		// Intentionally don't use the TestFeedFactory because we want to
		// control the placeholders.
		s := f.Server()
		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		sink.Scheme = sinkSchemeExperimentalSQL
		sink.Path = `d`

		var jobID int64
		sqlDB.QueryRow(t,
			`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, envelope = $2`, sink.String(), `wrapped`,
		).Scan(&jobID)

		// Secrets get removed from the sink parameter.
		expectedSink := sink
		expectedSink.RawQuery = ``

		var description string
		sqlDB.QueryRow(t,
			`SELECT description FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
		).Scan(&description)
		expected := `CREATE CHANGEFEED FOR TABLE foo INTO '` + expectedSink.String() +
			`' WITH envelope = 'wrapped', updated`
		if description != expected {
			t.Errorf(`got "%s" expected "%s"`, description, expected)
		}
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
}
func TestChangefeedPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`).(*cdctest.TableFeed)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
			`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
			`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
			`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
		})

		// Wait for the high-water mark on the job to be updated after the initial
		// scan, to make sure we don't get the initial scan data again.
		m, err := foo.Next()
		if err != nil {
			t.Fatal(err)
		} else if m.Key != nil {
			t.Fatalf(`expected a resolved timestamp got %s: %s->%s`, m.Topic, m.Key, m.Value)
		}

		sqlDB.Exec(t, `PAUSE JOB $1`, foo.JobID)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		sqlDB.Exec(t, `RESUME JOB $1`, foo.JobID)
		assertPayloads(t, foo, []string{
			`foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestManyChangefeedsOneTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'init')`)

		foo1 := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo1)
		foo2 := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo2)
		foo3 := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo3)

		// Make sure all the changefeeds are going.
		assertPayloads(t, foo1, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}}`})
		assertPayloads(t, foo2, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}}`})
		assertPayloads(t, foo3, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}}`})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v0')`)
		assertPayloads(t, foo1, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'v1')`)
		assertPayloads(t, foo1, []string{
			`foo: [1]->{"after": {"a": 1, "b": "v1"}}`,
		})
		assertPayloads(t, foo2, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "v1"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v2')`)
		assertPayloads(t, foo1, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v2"}}`,
		})
		assertPayloads(t, foo2, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v2"}}`,
		})
		assertPayloads(t, foo3, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}}`,
			`foo: [0]->{"after": {"a": 0, "b": "v2"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "v1"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestUnspecifiedPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT)`)
		var id0 int
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES (0) RETURNING rowid`).Scan(&id0)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		var id1 int
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES (1) RETURNING rowid`).Scan(&id1)

		assertPayloads(t, foo, []string{
			fmt.Sprintf(`foo: [%d]->{"after": {"a": 0, "rowid": %d}}`, id0, id0),
			fmt.Sprintf(`foo: [%d]->{"after": {"a": 1, "rowid": %d}}`, id1, id1),
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

// TestChangefeedNodeShutdown ensures that an enterprise changefeed continues
// running after the original job-coordinator node is shut down.
func TestChangefeedNodeShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#32232")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	flushCh := make(chan struct{}, 1)
	defer close(flushCh)
	knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
		AfterSinkFlush: func() error {
			select {
			case flushCh <- struct{}{}:
			default:
			}
			return nil
		},
	}}}

	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
		},
	})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

	// Create a factory which uses server 1 as the output of the Sink, but
	// executes the CREATE CHANGEFEED statement on server 0.
	sink, cleanup := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	f := cdctest.MakeTableFeedFactory(tc.Server(1), tc.ServerConn(0), flushCh, sink)
	foo := feed(t, f, "CREATE CHANGEFEED FOR foo")
	defer closeFeed(t, foo)

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'second')`)
	assertPayloads(t, foo, []string{
		`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		`foo: [1]->{"after": {"a": 1, "b": "second"}}`,
	})

	// TODO(mrtracy): At this point we need to wait for a resolved timestamp,
	// in order to ensure that there isn't a repeat when the job is picked up
	// again. As an alternative, we could use a verifier instead of assertPayloads.

	// Wait for the high-water mark on the job to be updated after the initial
	// scan, to make sure we don't get the initial scan data again.

	// Stop server 0, which is where the table feed connects.
	tc.StopServer(0)

	sqlDB.Exec(t, `UPSERT INTO foo VALUES(0, 'updated')`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (3, 'third')`)

	assertPayloads(t, foo, []string{
		`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		`foo: [3]->{"after": {"a": 3, "b": "third"}}`,
	})
}

func TestChangefeedTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)

		// Reset the counts.
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

		// Start some feeds (and read from them to make sure they've started.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		fooBar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH format=json`)
		defer closeFeed(t, fooBar)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})
		assertPayloads(t, fooBar, []string{
			`bar: [1]->{"after": {"a": 1}}`,
			`foo: [1]->{"after": {"a": 1}}`,
		})

		var expectedSink string
		if strings.Contains(t.Name(), `sinkless`) || strings.Contains(t.Name(), `poller`) {
			expectedSink = `sinkless`
		} else {
			expectedSink = `experimental-sql`
		}

		counts := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
		require.Equal(t, int32(2), counts[`changefeed.create.sink.`+expectedSink])
		require.Equal(t, int32(2), counts[`changefeed.create.format.json`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.1`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.2`])
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedMemBufferCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		// The RowContainer used internally by the memBuffer seems to request from
		// the budget in 10240 chunks. Set this number high enough for one but not
		// for a second. I'd love to be able to derive this from constants, but I
		// don't see how to do that without a refactor.
		knobs.MemBufferCapacity = 20000
		beforeEmitRowCh := make(chan struct{}, 1)
		knobs.BeforeEmitRow = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-beforeEmitRowCh:
			}
			return nil
		}
		defer close(beforeEmitRowCh)

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'small')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// Small amounts of data fit in the buffer.
		beforeEmitRowCh <- struct{}{}
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "small"}}`,
		})

		// Put enough data in to overflow the buffer and verify that at some point
		// we get the "memory budget exceeded" error.
		sqlDB.Exec(t, `INSERT INTO foo SELECT i, 'foofoofoo' FROM generate_series(1, $1) AS g(i)`, 1000)
		if _, err := foo.Next(); !testutils.IsError(err, `memory budget exceeded`) {
			t.Fatalf(`expected "memory budget exceeded" error got: %v`, err)
		}
	}

	// The mem buffer is only used with RangeFeed.
	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

// Regression test for #41694
func TestChangefeedRestartDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(i time.Duration) { jobs.DefaultAdoptInterval = i }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		beforeEmitRowCh := make(chan error, 20)
		knobs.BeforeEmitRow = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-beforeEmitRowCh:
				return err
			}
		}

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0), (1), (2), (3)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`).(*cdctest.TableFeed)
		defer closeFeed(t, foo)

		// TODO(dan): At a high level, all we're doing is trying to restart a
		// changefeed in the middle of changefeed backfill after a schema change
		// finishes. It turns out this is pretty hard to do with our current testing
		// knobs and this test ends up being pretty brittle. I'd love it if anyone
		// thought of a better way to do this.

		// Read the initial data in the rows.
		for i := 0; i < 4; i++ {
			beforeEmitRowCh <- nil
		}
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		// Run a schema change that backfills kvs.
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'backfill'`)

		// Unblock emit for each kv written by the schema change's backfill. The
		// changefeed actually emits these, but we lose it to overaggressive
		// duplicate detection in tableFeed.
		// TODO(dan): Track duplicates more precisely in tableFeed.
		for i := 0; i < 4; i++ {
			beforeEmitRowCh <- nil
		}

		// Unblock the emit for *all but one* of the rows emitted by the changefeed
		// backfill (run after the schema change completes and the final table
		// descriptor is written). The reason this test has 4 rows is because the
		// `sqlSink` that powers `tableFeed` only flushes after it has 3 rows, so we
		// need 1 more than that to guarantee that this first one gets flushed.
		for i := 0; i < 3; i++ {
			beforeEmitRowCh <- nil
		}
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "backfill"}}`,
		})

		// Restart the changefeed without allowing the second row to be backfilled.
		sqlDB.Exec(t, `PAUSE JOB $1`, foo.JobID)
		// Make extra sure that the zombie changefeed can't write any more data.
		beforeEmitRowCh <- MarkRetryableError(errors.New(`nope don't write it`))

		// Insert some data that we should only see out of the changefeed after it
		// re-runs the backfill.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (6, 'bar')`)

		// Unblock all later emits, we don't need this control anymore.
		close(beforeEmitRowCh)

		// Resume the changefeed and the backfill should start up again. Currently
		// this does the entire backfill again, you could imagine in the future that
		// we do some sort of backfill checkpointing and start the backfill up from
		// the last checkpoint.
		sqlDB.Exec(t, `RESUME JOB $1`, foo.JobID)
		assertPayloads(t, foo, []string{
			// The changefeed actually emits this row, but we lose it to
			// overaggressive duplicate detection in tableFeed.
			// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
			// `foo: [0]->{"after": {"a": 0, "b": "backfill"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "backfill"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "backfill"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "backfill"}}`,
		})

		assertPayloads(t, foo, []string{
			`foo: [6]->{"after": {"a": 6, "b": "bar"}}`,
		})
	}

	// Only the enterprise version uses jobs.
	t.Run(`enterprise`, enterpriseTest(testFn))
}
