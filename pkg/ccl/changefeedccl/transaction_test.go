// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestChangefeedTransactionInsertAndUpdate(t *testing.T) {
	// If a row is inserted and updated in the same transaction,
	// we send only the update message.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'light')`)
		sqlDB.Exec(t, `UPDATE ephemera set b='dark' where a=1`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "dark"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionInsertAndUpsert(t *testing.T) {
	// If a row is inserted and upserted over in the same transaction,
	// we send only the upsert message.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'light')`)
		sqlDB.Exec(t, `UPSERT INTO ephemera values (1, 'dark')`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "dark"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionDoubleUpdate(t *testing.T) {
	// If a previously-existing row is updated twice in the same transaction,
	// we send only the second update message with the "before" set to before
	// the transaction.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'void')`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "void"}, "before": null}`,
		})

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `UPSERT INTO ephemera values (1, 'light')`)
		sqlDB.Exec(t, `UPSERT INTO ephemera values (1, 'dark')`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "dark"}, "before": {"a": 1, "b": "void"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionReplaceRow(t *testing.T) {
	// If a previously-existing row is deleted, and
	// then another row is created with the same primary key
	// within the same transaction, we treat it as an update.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'void')`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "void"}, "before": null}`,
		})

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `DELETE FROM ephemera WHERE a=1`)
		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'light')`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "light"}, "before": {"a": 1, "b": "void"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionRollback(t *testing.T) {
	// A rolled-back write does not trigger a message
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'void')`)

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `UPSERT INTO ephemera values (1, 'light')`)
		sqlDB.Exec(t, `ROLLBACK`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": {"a": 1, "b": "void"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionInsertAndDelete(t *testing.T) {
	// If a row is inserted and deleted in the same transaction,
	// we send a delete message but don't show any information but the primary key.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE ephemera (a INT PRIMARY KEY, b STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR ephemera with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `INSERT INTO ephemera values (1, 'mayfly')`)
		sqlDB.Exec(t, `DELETE FROM ephemera where a=1`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`ephemera: [1]->{"after": null, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestChangefeedTransactionCrossTable(t *testing.T) {
	// If a transaction touches multiple tables,
	// we show the net effect on each row.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE blogs (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE posts (id INT PRIMARY KEY, blog_id INT, title STRING)`)

		ephemeraFeed := feed(t, f, `CREATE CHANGEFEED FOR blogs, posts with diff`)
		defer closeFeed(t, ephemeraFeed)

		sqlDB.Exec(t, `BEGIN TRANSACTION`)
		sqlDB.Exec(t, `INSERT INTO blogs values (1, 'my blog')`)
		sqlDB.Exec(t, `INSERT INTO posts values (0, 1, 'first post')`)
		sqlDB.Exec(t, `UPDATE blogs SET name = 'Musings' WHERE id=1`)
		sqlDB.Exec(t, `COMMIT`)

		assertPayloads(t, ephemeraFeed, []string{
			`blogs: [1]->{"after": {"id": 1, "name": "Musings"}, "before": null}`,
			`posts: [0]->{"after": {"blog_id": 1, "id": 0, "title": "first post"}, "before": null}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}
