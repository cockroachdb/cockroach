package changefeedccl

import (
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/schematestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestChangefeedUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		_ = maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

		// Set up a type and table.
		sqlDB.Exec(t, `CREATE TYPE t AS ENUM ('hello', 'howdy', 'hi')`)
		sqlDB.Exec(t, `CREATE TABLE tt (x INT PRIMARY KEY, y t)`)
		sqlDB.Exec(t, `INSERT INTO tt VALUES (0, 'hello')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR tt`)
		defer closeFeed(t, cf)

		assertPayloads(t, cf, []string{
			`tt: [0]->{"after": {"x": 0, "y": "hello"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO tt VALUES (1, 'howdy'), (2, 'hi')`)
		assertPayloads(t, cf, []string{
			`tt: [1]->{"after": {"x": 1, "y": "howdy"}}`,
			`tt: [2]->{"after": {"x": 2, "y": "hi"}}`,
		})

		// Alter the type and insert a new value.
		sqlDB.Exec(t, `ALTER TYPE t ADD VALUE 'hiya'`)
		sqlDB.Exec(t, `INSERT INTO tt VALUES (3, 'hiya')`)
		assertPayloads(t, cf, []string{
			`tt: [3]->{"after": {"x": 3, "y": "hiya"}}`,
		})

		// If we create a new type and add that type to tt, it should be picked
		// up by the schema feed.
		sqlDB.Exec(t, `CREATE TYPE t2 AS ENUM ('bye', 'cya')`)
		sqlDB.Exec(t, `ALTER TABLE tt ADD COLUMN z t2 DEFAULT 'bye'`)
		sqlDB.Exec(t, `INSERT INTO tt VALUES (4, 'hello', 'cya')`)

		assertPayloads(t, cf, []string{
			`tt: [0]->{"after": {"x": 0, "y": "hello", "z": "bye"}}`,
			`tt: [1]->{"after": {"x": 1, "y": "howdy", "z": "bye"}}`,
			`tt: [2]->{"after": {"x": 2, "y": "hi", "z": "bye"}}`,
			`tt: [3]->{"after": {"x": 3, "y": "hiya", "z": "bye"}}`,
			`tt: [4]->{"after": {"x": 4, "y": "hello", "z": "cya"}}`,
		})

		// If we rename a value in an existing type, it doesn't count as a change
		// but the rename is reflected in future changes.
		sqlDB.Exec(t, `ALTER TYPE t RENAME VALUE 'hi' TO 'yo'`)
		sqlDB.Exec(t, `UPDATE tt SET z='cya' where x=2`)

		assertPayloads(t, cf, []string{
			`tt: [2]->{"after": {"x": 2, "y": "yo", "z": "cya"}}`,
		})

	}

	cdcTest(t, testFn)
}

// If the schema_change_policy is 'stop' and we drop columns which are not
// targeted by the changefeed, it should not stop.
func TestNoStopAfterNonTargetColumnDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c WITH schema_change_policy='stop'`,
			optOutOfMetamorphicEnrichedEnvelope{"requires families"})
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'b1', 'c1')`)

		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
		})

		// Check that dropping a watched column still stops the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN b`)
		msg, err := cf.Next()
		require.True(t, testutils.IsError(err, `schema change occurred at`),
			`expected "schema change occurred at ..." got: msg=%s, err=%+v`, msg, err)
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})
}

// If we drop columns which are not targeted by the changefeed, it should not backfill.
// schema
func TestNoBackfillAfterNonTargetColumnDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}

		// Open up the changefeed.
		// We specify `updated` so that identical messages with different timestamps
		// aren't filtered out as duplicates. The appearance of such messages would
		// indicate that a backfill did happen even though it should not have.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c WITH updated`, args...)
		defer closeFeed(t, cf)
		assertPayloadsStripTs(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'b1', 'c1')`)
		assertPayloadsStripTs(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
		})

		// Check that dropping a watched column still backfills.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN c`)
		assertPayloadsStripTs(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b"}}`,
			`hasfams.b_and_c: [1]->{"after": {"b": "b1"}}`,
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})
}

// schema
func TestChangefeedColumnDropsWithFamilyAndNonFamilyTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `CREATE TABLE nofams (id int primary key, a string, b string, c string)`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)
		sqlDB.Exec(t, `INSERT INTO nofams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c, TABLE nofams`, args...)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
			`nofams: [0]->{"after": {"a": "a", "b": "b", "c": "c", "id": 0}}`,
		})

		// Dropping an unwatched column from hasfams does not affect the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'b1', 'c1')`)
		sqlDB.Exec(t, `INSERT INTO nofams VALUES (1, 'a1', 'b1', 'c1')`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
			`nofams: [1]->{"after": {"a": "a1", "b": "b1", "c": "c1", "id": 1}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN b`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"c": "c"}}`,
			`hasfams.b_and_c: [1]->{"after": {"c": "c1"}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE nofams DROP COLUMN b`)
		assertPayloads(t, cf, []string{
			`nofams: [0]->{"after": {"a": "a", "c": "c", "id": 0}}`,
			`nofams: [1]->{"after": {"a": "a1", "c": "c1", "id": 1}}`,
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedColumnDropsOnMultipleFamiliesWithTheSameName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `CREATE TABLE alsohasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)
		sqlDB.Exec(t, `INSERT INTO alsohasfams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c, TABLE alsohasfams FAMILY id_a`, args...)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
			`alsohasfams.id_a: [0]->{"after": {"a": "a", "id": 0}}`,
		})

		// Dropping an unwatched column from hasfams does not affect the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'b1', 'c1')`)
		sqlDB.Exec(t, `INSERT INTO alsohasfams VALUES (1, 'a1', 'b1', 'c1')`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
			`alsohasfams.id_a: [1]->{"after": {"a": "a1", "id": 1}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE alsohasfams DROP COLUMN a`)
		assertPayloads(t, cf, []string{
			`alsohasfams.id_a: [0]->{"after": {"id": 0}}`,
			`alsohasfams.id_a: [1]->{"after": {"id": 1}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN b`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"c": "c"}}`,
			`hasfams.b_and_c: [1]->{"after": {"c": "c1"}}`,
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedColumnDropsOnTheSameTableWithMultipleFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY id_a, TABLE hasfams FAMILY b_and_c`, args...)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
			`hasfams.id_a: [0]->{"after": {"a": "a", "id": 0}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		assertPayloads(t, cf, []string{
			`hasfams.id_a: [0]->{"after": {"id": 0}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN b`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"c": "c"}}`,
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})
}

func TestChangefeedColumnDropsOnTheSameTableWithMultipleFamiliesWithManualSchemaLocked(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY id_a, TABLE hasfams FAMILY b_and_c`, args...)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
			`hasfams.id_a: [0]->{"after": {"a": "a", "id": 0}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams SET (schema_locked=false)`)
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `ALTER TABLE hasfams SET (schema_locked=true)`)
		assertPayloads(t, cf, []string{
			`hasfams.id_a: [0]->{"after": {"id": 0}}`,
		})

		// Check that dropping a watched column will backfill the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams SET (schema_locked=false)`)
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN b`)
		sqlDB.Exec(t, `ALTER TABLE hasfams SET (schema_locked=true)`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"c": "c"}}`,
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})
}

func TestNoStopAfterNonTargetAddColumnWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id INT PRIMARY KEY, a STRING, b STRING, c STRING, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c WITH schema_change_policy='stop'`, args...)

		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
		})

		// Adding a column with a backfill to the default column family does not stop the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams ADD COLUMN d STRING DEFAULT 'default'`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'a1', 'b1', 'c1', 'd1')`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
		})

		// Adding a column with a backfill to a non-target column family does not stop the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams ADD COLUMN e STRING DEFAULT 'default' FAMILY id_a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (2, 'a2', 'b2', 'c2', 'd2', 'e2')`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [2]->{"after": {"b": "b2", "c": "c2"}}`,
		})

		// Check that adding a column to a watched family stops the changefeed.
		sqlDB.Exec(t, `ALTER TABLE hasfams ADD COLUMN f INT DEFAULT 0 FAMILY b_and_c`)
		if _, err := cf.Next(); !testutils.IsError(err, `schema change occurred at`) {
			t.Errorf(`expected "schema change occurred at ..." got: %+v`, err.Error())
		}
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedSchemaChangeNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		_ = maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

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

	cdcTest(t, testFn)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"),
		log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// Test checkpointing during schema change backfills that can be paused and
// resumed multiple times during execution
func TestChangefeedSchemaChangeBackfillCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rnd, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %d", seed)

	// This test asserts that a second checkpoint made after resumption does its
	// best to not lose information from the first checkpoint, therefore the
	// maxCheckpointSize should be large enough to hold both without any
	// truncation
	var maxCheckpointSize int64 = 100 << 20

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		usingLegacySchemaChanger := maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)
		// NB: For the `ALTER TABLE foo ADD COLUMN ... DEFAULT` schema change,
		// the expected boundary is different depending on if we are using the
		// legacy schema changer or not.
		expectedBoundaryType := jobspb.ResolvedSpan_RESTART
		if usingLegacySchemaChanger {
			expectedBoundaryType = jobspb.ResolvedSpan_BACKFILL
		}

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Initialize table
		sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 50
			return nil
		}

		// Setup changefeed job details, avoid relying on initial scan functionality
		baseFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='100ms', min_checkpoint_frequency='100ms', no_initial_scan`)
		jobFeed := baseFeed.(cdctest.EnterpriseTestFeed)
		jobRegistry := s.Server.JobRegistry().(*jobs.Registry)

		// Ensure events are consumed for sinks that don't buffer (ex: Kafka)
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				_, err := baseFeed.Next()
				if err != nil {
					return err
				}
			}
		})
		defer func() {
			closeFeed(t, baseFeed)
			_ = g.Wait()
		}()

		// Helper to read job progress
		loadProgress := func() jobspb.Progress {
			jobID := jobFeed.JobID()
			job, err := jobRegistry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			return job.Progress()
		}

		// Ensure the changefeed has begun normal execution
		testutils.SucceedsSoon(t, func() error {
			prog := loadProgress()
			if p := prog.GetHighWater(); p != nil && !p.IsEmpty() {
				t.Logf("highwater: %s", p)
				return nil
			}
			return errors.New("waiting for highwater")
		})

		// Pause job and setup overrides to force a checkpoint
		require.NoError(t, jobFeed.Pause())

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.SpanCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		var tableSpan roachpb.Span
		refreshTableSpan := func() {
			fooDesc := desctestutils.TestingGetPublicTableDescriptor(
				s.SystemServer.DB(), s.Codec, "d", "foo")
			tableSpan = fooDesc.PrimaryIndexSpan(s.Codec)
		}

		// FilterSpanWithMutation should ensure that once the backfill begins, the following resolved events
		// that are for that backfill (are of the timestamp right after the backfill timestamp) resolve some
		// but not all of the time, which results in a checkpoint eventually being created
		numGaps := 0
		var backfillTimestamp hlc.Timestamp
		var initialCheckpoint roachpb.SpanGroup
		var foundCheckpoint int32
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			// Stop resolving anything after checkpoint set to avoid eventually resolving the full span
			if initialCheckpoint.Len() > 0 {
				return true, nil
			}

			// A backfill begins when the associated resolved event arrives, which has a
			// timestamp such that all backfill spans have a timestamp of
			// timestamp.Next().
			if r.BoundaryType == expectedBoundaryType {
				// NB: We wait until the schema change is public before looking
				// up the table span. When using the declarative schema changer,
				// the table span will be different before and after the schema
				// change due to a primary index swap.
				refreshTableSpan()
				backfillTimestamp = r.Timestamp
				t.Logf("backfill timestamp: %s", backfillTimestamp)
				return false, nil
			}

			// Check if we've set a checkpoint yet
			progress := loadProgress()
			if spanLevelCheckpoint := loadCheckpoint(t, progress); spanLevelCheckpoint != nil {
				minCheckpointTS := spanLevelCheckpoint.MinTimestamp()
				// Checkpoint timestamp should be the timestamp of the spans from the backfill
				if !minCheckpointTS.Equal(backfillTimestamp.Next()) {
					return false, changefeedbase.WithTerminalError(
						errors.AssertionFailedf("expected checkpoint timestamp %s, found %s", backfillTimestamp, minCheckpointTS))
				}
				initialCheckpoint = makeSpanGroupFromCheckpoint(t, spanLevelCheckpoint)
				atomic.StoreInt32(&foundCheckpoint, 1)
			}

			// Filter non-backfill-related spans
			if !r.Timestamp.Equal(backfillTimestamp.Next()) {
				// Only allow spans prior to a valid backfillTimestamp to avoid moving past the backfill
				return backfillTimestamp.IsSet() && !r.Timestamp.LessEq(backfillTimestamp.Next()), nil
			}

			// At the end of a backfill, kv feed will emit a resolved span for the whole table.
			// Filter this out because we would like to leave gaps.
			if r.Span.Equal(tableSpan) {
				return true, nil
			}

			// Ensure that we have at least 2 gaps, so when a second checkpoint happens later in this test,
			// the second checkpoint can still leave at least one gap.
			if numGaps >= 2 {
				return rnd.Intn(10) > 7, nil
			}
			numGaps += 1
			return true, nil
		}

		require.NoError(t, jobFeed.Resume())
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'd'`)

		// Wait for a checkpoint to have been set
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&foundCheckpoint) != 0 {
				return nil
			}
			return errors.New("waiting for checkpoint")
		})
		t.Logf("initial checkpoint: %s", initialCheckpoint.Slice())

		require.NoError(t, jobFeed.Pause())

		// All spans up to the backfill event should've been resolved, therefore the
		// highwater mark should be that of the backfill event
		progress := loadProgress()
		h := progress.GetHighWater()
		require.True(t, h.Equal(backfillTimestamp))

		// We ensure that if the job is resumed, it builds off of the existing
		// checkpoint, not resolving any already-checkpointed-spans while also
		// setting a new checkpoint that contains both initially checkpointed spans
		// as well as the newly resolved ones
		var secondCheckpoint roachpb.SpanGroup
		foundCheckpoint = 0
		numGaps = 0
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			// Stop resolving anything after second checkpoint set to avoid backfill completion
			if secondCheckpoint.Len() > 0 {
				return true, nil
			}

			// Once we've set a checkpoint that covers new spans, record it
			progress := loadProgress()
			if spanLevelCheckpoint := loadCheckpoint(t, progress); spanLevelCheckpoint != nil {
				currentCheckpoint := makeSpanGroupFromCheckpoint(t, spanLevelCheckpoint)
				// Ensure that the second checkpoint both contains all spans in the first checkpoint as well as new spans
				if currentCheckpoint.Encloses(initialCheckpoint.Slice()...) && !initialCheckpoint.Encloses(currentCheckpoint.Slice()...) {
					secondCheckpoint = currentCheckpoint
					atomic.StoreInt32(&foundCheckpoint, 1)
				}
			}

			// Filter non-backfill-related spans
			if !r.Timestamp.Equal(backfillTimestamp.Next()) {
				// Only allow spans prior to a valid backfillTimestamp to avoid moving past the backfill
				return backfillTimestamp.IsSet() && !r.Timestamp.LessEq(backfillTimestamp.Next()), nil
			}

			require.Falsef(t, initialCheckpoint.Encloses(r.Span), "second backfill should not resolve checkpointed span")

			// At the end of a backfill, kv feed will emit a resolved span for the whole table.
			// Filter this out because we would like to leave at least one gap.
			if r.Span.Equal(tableSpan) {
				return true, nil
			}

			// Ensure there is at least one gap so that we can receive resolved spans later.
			if numGaps >= 1 {
				return rnd.Intn(10) > 7, nil
			}
			numGaps += 1
			return true, nil
		}

		require.NoError(t, jobFeed.Resume())
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&foundCheckpoint) != 0 {
				return nil
			}
			return errors.New("waiting for second checkpoint")
		})
		t.Logf("second checkpoint: %s", secondCheckpoint.Slice())

		require.NoError(t, jobFeed.Pause())
		for _, span := range initialCheckpoint.Slice() {
			require.Truef(t, secondCheckpoint.Contains(span.Key), "second checkpoint should contain all values in first checkpoint")
		}

		// Collect spans we attempt to resolve after when we resume.
		var resolved []roachpb.Span
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			resolved = append(resolved, r.Span)
			return false, nil
		}

		// Resume job.
		require.NoError(t, jobFeed.Resume())

		// checkpoint should eventually be gone once backfill completes.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if loadCheckpoint(t, progress) != nil {
				return errors.New("checkpoint still non-empty")
			}
			return nil
		})

		// Pause job to avoid race on the resolved array
		require.NoError(t, jobFeed.Pause())

		// Verify that none of the resolved spans after resume were checkpointed.
		t.Logf("Table Span: %s, Second Checkpoint: %v, Resolved Spans: %v", tableSpan, secondCheckpoint, resolved)
		for _, sp := range resolved {
			require.Falsef(t, !sp.Equal(tableSpan) && secondCheckpoint.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// Test schema changes that require a backfill when the backfill option is
// allowed when using the legacy schema changer.
//
// TODO: remove this test when the legacy schema changer is  deprecated.
func TestChangefeedSchemaChangeAllowBackfill_Legacy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		t.Log("using legacy schema changer")
		sqlDB.Exec(t, "SET create_table_with_schema_locked=false")
		sqlDB.Exec(t, "SET use_declarative_schema_changer='off'")
		sqlDB.Exec(t, "SET CLUSTER SETTING  sql.defaults.use_declarative_schema_changer='off'")
		sqlDB.Exec(t, "SET CLUSTER SETTING  sql.defaults.create_table_with_schema_locked='false'")

		// Expected semantics:
		//
		// 1) DROP COLUMN
		// If the table descriptor is at version 1 when the `ALTER TABLE` stmt is issued,
		// we expect the changefeed level backfill to be triggered at the `ModificationTime` of
		// version 2 of the said descriptor. This is because this is the descriptor
		// version at which the dropped column stops being visible to SELECTs. Note that
		// this means we will see row updates resulting from the schema-change level
		// backfill _after_ the changefeed level backfill.
		//
		// 2) ADD COLUMN WITH DEFAULT & ADD COLUMN AS ... STORED
		// If the table descriptor is at version 1 when the `ALTER TABLE` stmt is issued,
		// we expect the changefeed level backfill to be triggered at the
		// `ModificationTime` of version 4 of said descriptor. This is because this is the
		// descriptor version which makes the schema-change level backfill for the
		// newly-added column public. This means we wil see row updates resulting from the
		// schema-change level backfill _before_ the changefeed level backfill.

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def WITH updated`)
			defer closeFeed(t, addColumnDef)
			assertPayloadsStripTs(t, addColumnDef, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			ts := schematestutils.FetchDescVersionModificationTime(t, s.TestServer.Server,
				`d`, `public`, `add_column_def`, 4)

			// Schema change backfill
			assertPayloadsStripTs(t, addColumnDef, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
			})
			// Changefeed level backfill
			assertPayloads(t, addColumnDef, []string{
				fmt.Sprintf(`add_column_def: [1]->{"after": {"a": 1, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_column_def: [2]->{"after": {"a": 2, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp WITH updated`)
			defer closeFeed(t, addColComp)
			assertPayloadsStripTs(t, addColComp, []string{
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				`add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			assertPayloadsStripTs(t, addColComp, []string{
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				`add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
			})
			ts := schematestutils.FetchDescVersionModificationTime(t, s.TestServer.Server,
				`d`, `public`, `add_col_comp`, 4)

			assertPayloads(t, addColComp, []string{
				fmt.Sprintf(`add_col_comp: [1]->{"after": {"a": 1, "b": 6, "c": 11}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_col_comp: [2]->{"after": {"a": 2, "b": 7, "c": 12}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2, '2')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column WITH updated`)
			defer closeFeed(t, dropColumn)
			assertPayloadsStripTs(t, dropColumn, []string{
				`drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
				`drop_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (3)`)

			// since the changefeed level backfill (which flushes the sink before
			// the backfill) occurs before the schema-change backfill for a drop
			// column, the order in which the sink receives both backfills is
			// uncertain. the only guarantee here is per-key ordering guarantees,
			// so we must check both backfills in the same assertion.
			assertPayloadsPerKeyOrderedStripTs(t, dropColumn, []string{
				// Changefeed level backfill for DROP COLUMN b.
				`drop_column: [1]->{"after": {"a": 1}}`,
				`drop_column: [2]->{"after": {"a": 2}}`,
				// Schema-change backfill for DROP COLUMN b.
				`drop_column: [1]->{"after": {"a": 1}}`,
				`drop_column: [2]->{"after": {"a": 2}}`,
				// Insert 3 into drop_column
				`drop_column: [3]->{"after": {"a": 3}}`,
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
			knobs := s.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = waitSinkHook

			multipleAlters := feed(t, f, `CREATE CHANGEFEED FOR multiple_alters WITH updated`)
			defer closeFeed(t, multipleAlters)
			assertPayloadsStripTs(t, multipleAlters, []string{
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

			// assertions are grouped this way because the sink is flushed prior
			// to a changefeed level backfill, ensuring all messages are received
			// at the start of the assertion
			assertPayloadsPerKeyOrderedStripTs(t, multipleAlters, []string{
				// Changefeed level backfill for DROP COLUMN b.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
				// Schema-change backfill for DROP COLUMN b.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
				// Schema-change backfill for ADD COLUMN c.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
			})
			assertPayloadsPerKeyOrderedStripTs(t, multipleAlters, []string{
				// Changefeed level backfill for ADD COLUMN c.
				`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
				// Schema change level backfill for ADD COLUMN d.
				`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
			})
			ts := schematestutils.FetchDescVersionModificationTime(t, s.TestServer.Server,
				`d`, `public`, `multiple_alters`, 10)
			// Changefeed level backfill for ADD COLUMN d.
			assertPayloads(t, multipleAlters, []string{
				// Backfill no-ops for column D (C schema change is complete)
				// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
				// Scan output for column C
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1, "c": "cee", "d": "dee"}, "updated": "%s"}`, ts.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2, "c": "cee", "d": "dee"}, "updated": "%s"}`, ts.AsOfSystemTime()),
			})
		})
	}

	cdcTestWithSystem(t, testFn)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// TestChangefeedSchemaChangeAllowBackfill tests schema changes that require a
// backfill when the backfill option is allowed.
func TestChangefeedSchemaChangeAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Expected semantics:
		//
		// 1) DROP COLUMN
		//
		// If the table descriptor is at version 1 when the `ALTER TABLE` stmt is issued,
		// we expect the changefeed level backfill to be triggered at the `ModificationTime` of
		// version 2 of the said descriptor. This is because this is the descriptor
		// version at which the dropped column stops being visible to SELECTs.
		//
		// 2) ADD COLUMN WITH DEFAULT & ADD COLUMN AS ... STORED
		//
		// If the table descriptor is at version 1 when the `ALTER TABLE` stmt
		// is issued, we expect the backfill to be triggered at the
		// `ModificationTime` of version 7 of said descriptor. This is because
		// this is the descriptor version at which the KV-level backfill is finished and
		// the primary index swap takes place to make the newly-added column public.

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2)`)
			addColumnDef := feed(t, f, `CREATE CHANGEFEED FOR add_column_def WITH updated`)
			defer closeFeed(t, addColumnDef)
			assertPayloadsStripTs(t, addColumnDef, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)
			ts := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `add_column_def`, 7)
			assertPayloads(t, addColumnDef, []string{
				fmt.Sprintf(`add_column_def: [1]->{"after": {"a": 1, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_column_def: [2]->{"after": {"a": 2, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_col_comp (a INT PRIMARY KEY, b INT AS (a + 5) STORED)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_col_comp (a) VALUES (2)`)
			addColComp := feed(t, f, `CREATE CHANGEFEED FOR add_col_comp WITH updated`)
			defer closeFeed(t, addColComp)
			assertPayloadsStripTs(t, addColComp, []string{
				`add_col_comp: [1]->{"after": {"a": 1, "b": 6}}`,
				`add_col_comp: [2]->{"after": {"a": 2, "b": 7}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_col_comp ADD COLUMN c INT AS (a + 10) STORED`)
			ts := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `add_col_comp`, 7)
			assertPayloads(t, addColComp, []string{
				fmt.Sprintf(`add_col_comp: [1]->{"after": {"a": 1, "b": 6, "c": 11}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_col_comp: [2]->{"after": {"a": 2, "b": 7, "c": 12}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

		t.Run(`drop column`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2, '2')`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column WITH updated`)
			defer closeFeed(t, dropColumn)
			assertPayloadsStripTs(t, dropColumn, []string{
				`drop_column: [1]->{"after": {"a": 1, "b": "1"}}`,
				`drop_column: [2]->{"after": {"a": 2, "b": "2"}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			ts := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `drop_column`, 6)

			// Backfill for DROP COLUMN b.
			assertPayloads(t, dropColumn, []string{
				fmt.Sprintf(`drop_column: [1]->{"after": {"a": 1}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`drop_column: [2]->{"after": {"a": 2}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})

			// Insert 3 into drop_column.
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (3)`)
			assertPayloadsStripTs(t, dropColumn, []string{
				`drop_column: [3]->{"after": {"a": 3}}`,
			})
		})

		t.Run(`multiple alters`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE multiple_alters (a INT PRIMARY KEY, b STRING) WITH (schema_locked = false)`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (1, '1')`)
			sqlDB.Exec(t, `INSERT INTO multiple_alters VALUES (2, '2')`)

			// Set up a hook to pause the changfeed on the next emit.
			var wg sync.WaitGroup
			waitSinkHook := func(_ context.Context) error {
				wg.Wait()
				return nil
			}
			knobs := s.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = waitSinkHook

			multipleAlters := feed(t, f, `CREATE CHANGEFEED FOR multiple_alters WITH updated`)
			defer closeFeed(t, multipleAlters)
			assertPayloadsStripTs(t, multipleAlters, []string{
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

			// When dropping the column, the desc goes from version 1->9 with the schema change being visible at
			// version 2. Then, when adding column c, it goes from 9->17, with the schema change being visible at
			// the 7th step (version 15). Finally, when adding column d, it goes from 17->25 ith the schema change
			// being visible at the 7th step (version 23).
			// TODO(#142936): Investigate if this descriptor version hardcoding is sound.
			dropTS := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `multiple_alters`, 6)
			addTS := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `multiple_alters`, 15)
			addTS2 := schematestutils.FetchDescVersionModificationTime(t, s.Server, `d`, `public`, `multiple_alters`, 23)

			assertPayloads(t, multipleAlters, []string{
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1}, "updated": "%s"}`, dropTS.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2}, "updated": "%s"}`, dropTS.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}, "updated": "%s"}`, addTS.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}, "updated": "%s"}`, addTS.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1, "c": "cee", "d": "dee"}, "updated": "%s"}`, addTS2.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2, "c": "cee", "d": "dee"}, "updated": "%s"}`, addTS2.AsOfSystemTime()),
			})
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	})

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// TestChangefeedSchemaChangeBackfillScope tests that when a changefeed is watching multiple tables and only
// one needs a backfill, we only see backfill rows emitted for that one table.
func TestChangefeedSchemaChangeBackfillScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		usingLegacySchemaChanger := maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

		t.Run(`add column with default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_def (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `CREATE TABLE no_def_change (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (1)`)
			sqlDB.Exec(t, `INSERT INTO add_column_def VALUES (2)`)
			sqlDB.Exec(t, `INSERT INTO no_def_change VALUES (3)`)
			combinedFeed := feed(t, f, `CREATE CHANGEFEED FOR add_column_def, no_def_change WITH updated`)
			defer closeFeed(t, combinedFeed)
			assertPayloadsStripTs(t, combinedFeed, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
				`no_def_change: [3]->{"after": {"a": 3}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_def ADD COLUMN b STRING DEFAULT 'd'`)

			var ts hlc.Timestamp
			if usingLegacySchemaChanger {
				// Schema change becomes public at version 4.
				ts = schematestutils.FetchDescVersionModificationTime(t, s.TestServer.Server,
					`d`, `public`, `add_column_def`, 4)
				// The legacy schema changer rewrites KVs in place, so we see
				// an additional backfill before the changefeed-level backfill.
				assertPayloadsStripTs(t, combinedFeed, []string{
					`add_column_def: [1]->{"after": {"a": 1}}`,
					`add_column_def: [2]->{"after": {"a": 2}}`,
				})
			} else {
				// The primary index swap occurs at version 7.
				ts = schematestutils.FetchDescVersionModificationTime(t, s.TestServer.Server,
					`d`, `public`, `add_column_def`, 7)
			}
			assertPayloads(t, combinedFeed, []string{
				fmt.Sprintf(`add_column_def: [1]->{"after": {"a": 1, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_column_def: [2]->{"after": {"a": 2, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

	}

	cdcTestWithSystem(t, testFn)
	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
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
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn)
	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

func TestChangefeedEachColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY most (a,b), FAMILY only_c (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)

		// Must specify WITH split_column_families
		sqlDB.ExpectErrWithTimeout(t, `multiple column families`, `CREATE CHANGEFEED FOR foo`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families`, args...)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
			`foo.only_c: [0]->{"after": {"c": "cat"}}`,
		})

		// No messages for unaffected column families.
		sqlDB.Exec(t, `UPDATE foo SET c='lion' WHERE a=0`)
		sqlDB.Exec(t, `UPDATE foo SET c='tiger' WHERE a=0`)
		assertPayloads(t, foo, []string{
			`foo.only_c: [0]->{"after": {"c": "lion"}}`,
			`foo.only_c: [0]->{"after": {"c": "tiger"}}`,
		})

		// No messages on insert for families where no non-null values were set.
		sqlDB.Exec(t, `INSERT INTO foo values (1, 'puppy', null)`)
		sqlDB.Exec(t, `INSERT INTO foo values (2, null, 'kitten')`)
		assertPayloads(t, foo, []string{
			`foo.most: [1]->{"after": {"a": 1, "b": "puppy"}}`,
			`foo.most: [2]->{"after": {"a": 2, "b": null}}`,
			`foo.only_c: [2]->{"after": {"c": "kitten"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a>0`)

		// Deletes send a message for each column family.
		fooWithDiff := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families, diff`, args...)
		defer closeFeed(t, fooWithDiff)
		sqlDB.Exec(t, `DELETE FROM foo WHERE a=0`)
		assertPayloads(t, fooWithDiff, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}, "before": null}`,
			`foo.only_c: [0]->{"after": {"c": "tiger"}, "before": null}`,
			`foo.most: [0]->{"after": null, "before": {"a": 0, "b": "dog"}}`,
			`foo.only_c: [0]->{"after": null, "before": {"c": "tiger"}}`,
		})

		// Table with a second column family added after the changefeed starts.
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, FAMILY f_a (a))`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`, args...)
		defer closeFeed(t, bar)
		assertPayloads(t, bar, []string{
			`bar: [0]->{"after": {"a": 0}}`,
		})
		sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b STRING CREATE FAMILY f_b`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
		if _, err := bar.Next(); !testutils.IsError(err, `created on a table with a single column family`) {
			t.Errorf(`expected "column family" error got: %+v`, err)
		}
	}

	cdcTest(t, testFn, withAllowChangefeedErr("expects terminal error"))
}

func TestChangefeedSingleColumnFamilySchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	// requireErrorSoon times out after 30 seconds
	skip.UnderStress(t)
	skip.UnderRace(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY most (a,b), FAMILY rest (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)

		arg := optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families; see #145927"}
		fooMost := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY most`, arg)
		defer closeFeed(t, fooMost)
		assertPayloads(t, fooMost, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
		})

		fooRest := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest`, arg)
		defer closeFeed(t, fooRest)
		assertPayloads(t, fooRest, []string{
			`foo.rest: [0]->{"after": {"c": "cat"}}`,
		})

		// Add a column to an existing family, it shows up in the feed for that family
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN more int DEFAULT 11 FAMILY most`)
		assertPayloads(t, fooMost, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog", "more": 11}}`,
		})

		// Removing all columns in a watched family fails the feed
		waitForSchemaChange(t, sqlDB, `ALTER TABLE foo DROP column c`)
		requireTerminalErrorSoon(context.Background(), t, fooRest,
			regexp.MustCompile(`CHANGEFEED targeting nonexistent or removed column family rest of table foo`))
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn, withAllowChangefeedErr("expects terminal error"))
	}, withMaybeUseLegacySchemaChanger())
}

func TestChangefeedEachColumnFamilySchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY f1 (a,b), FAMILY f2 (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families`, args...)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo.f1: [0]->{"after": {"a": 0, "b": "dog"}}`,
			`foo.f2: [0]->{"after": {"c": "cat"}}`,
		})

		// Add a column to an existing family
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d string DEFAULT 'hi' FAMILY f2`)
		assertPayloads(t, foo, []string{
			`foo.f2: [0]->{"after": {"c": "cat", "d": "hi"}}`,
		})

		// Add a column to a new family.
		// Behavior here is a little wonky with default values in a way
		// that's likely to change with declarative schema changer,
		// so not asserting anything either way about that.
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN e string CREATE FAMILY f3`)
		sqlDB.Exec(t, `UPDATE foo SET e='hello' WHERE a=0`)
		assertPayloads(t, foo, []string{
			`foo.f3: [0]->{"after": {"e": "hello"}}`,
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn)
	}, withMaybeUseLegacySchemaChanger())
}

// Reproduce issue for #114196. This test verifies that changefeed with custom
// key column works with CDC queries correctly.
func TestChangefeedCustomKeyColumnWithCDCQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := map[string]struct {
		shouldSkip           bool
		createTableStmt      string
		createChangeFeedStmt string
		stmts                []string
		payloadsAfterStmts   []string
	}{
		`select_star`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT * FROM foo`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"a": 0, "b": "dog", "c": "cat"}`, `foo: ["dog1"]->{"a": 1, "b": "dog1", "c": "cat1"}`},
		},
		`select_with_filter`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT * FROM foo WHERE b='dog'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"a": 0, "b": "dog", "c": "cat"}`},
		},
		`select_multiple_columns`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='c', unordered AS SELECT b, c FROM foo WHERE b='dog'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["cat"]->{"b": "dog", "c": "cat"}`},
		},
		`custom_key_with_created_column`: {
			shouldSkip:           true,
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='double_b', unordered AS SELECT concat(b, c) AS double_b FROM foo`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["cat"]->{"c": "cat"}`},
		},
		`select_star_with_builtin_funcs`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='c', unordered AS SELECT *, concat(b, c) FROM foo`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["cat"]->{"a": 0, "b": "dog", "c": "cat", "concat": "dogcat"}`, `foo: ["cat1"]->{"a": 1, "b": "dog1", "c": "cat1", "concat": "dog1cat1"}`},
		},
		`select_stored_column`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d STRING AS (concat(b, c)) STORED)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='d', unordered AS SELECT * FROM foo WHERE d='dog1cat1'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["dog1cat1"]->{"a": 1, "b": "dog1", "c": "cat1", "d": "dog1cat1"}`},
		},
		`select_virtual_column`: {
			shouldSkip:           true,
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d STRING AS (concat(b, c)) VIRTUAL)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='d', unordered AS SELECT d FROM foo WHERE d='dog1cat1'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog', 'cat')`, `INSERT INTO foo VALUES (1, 'dog1', 'cat1')`},
			payloadsAfterStmts:   []string{`foo: ["dog1cat1"]->{"a": 1, "b": "dog1", "c": "cat1", "d": "dog1cat1"}`},
		},
		`select_with_filter_IN`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT * FROM foo WHERE b IN ('dog', 'dog1')`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog')`, `INSERT INTO foo VALUES (1, 'dog1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"a": 0, "b": "dog"}`, `foo: ["dog1"]->{"a": 1, "b": "dog1"}`},
		},
		`select_with_delete`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT *, event_op() = 'delete' AS deleted FROM foo WHERE 'hello' != 'world'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog')`, `INSERT INTO foo VALUES (1, 'dog1')`, `DELETE FROM foo WHERE a=1`},
			payloadsAfterStmts:   []string{`foo: [null]->{"a": 1, "b": null, "deleted": true}`, `foo: ["dog"]->{"a": 0, "b": "dog", "deleted": false}`, `foo: ["dog1"]->{"a": 1, "b": "dog1", "deleted": false}`},
		},
		`select_with_filter_delete`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT * FROM foo WHERE event_op() = 'delete'`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog')`, `INSERT INTO foo VALUES (1, 'dog1')`, `DELETE FROM foo WHERE a=1`},
			payloadsAfterStmts:   []string{`foo: [null]->{"a": 1, "b": null}`},
		},
		`select_with_cdc_prev`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT *, (cdc_prev) FROM foo`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog')`, `UPSERT INTO foo VALUES (0,'dog1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"a": 0, "b": "dog", "cdc_prev": null}`, `foo: ["dog1"]->{"a": 0, "b": "dog1", "cdc_prev": {"a": 0, "b": "dog"}}`},
		},
		`select_with_filter_cdc_prev`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT b FROM foo WHERE (cdc_prev).a = 0`,
			stmts:                []string{`INSERT INTO foo VALUES (0, 'dog')`, `UPSERT INTO foo VALUES (0, 'dog1')`},
			payloadsAfterStmts:   []string{`foo: ["dog1"]->{"b": "dog1"}`},
		},
		`select_with_hidden_column`: {
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT VISIBLE)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='b', unordered AS SELECT b FROM foo`,
			stmts:                []string{`INSERT INTO foo(a,b) VALUES (0, 'dog')`, `INSERT INTO foo(a,b) VALUES (1, 'dog1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"b": "dog"}`, `foo: ["dog1"]->{"b": "dog1"}`},
		},
		`select_with_cdc_prev_column`: {
			shouldSkip:           true,
			createTableStmt:      `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
			createChangeFeedStmt: `CREATE CHANGEFEED WITH key_column='cdc_prev.a', unordered AS SELECT * FROM foo`,
			stmts:                []string{`INSERT INTO foo(a,b) VALUES (0, 'dog')`, `INSERT INTO foo(a,b) VALUES (1, 'dog1')`},
			payloadsAfterStmts:   []string{`foo: ["dog"]->{"b": "dog"}`, `foo: ["dog1"]->{"b": "dog1"}`},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if test.shouldSkip {
				t.Logf("skipping this test because %s is currently not supported; "+
					"see #115267 for more details", name)
				return
			}
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)
				sqlDB.Exec(t, test.createTableStmt)
				foo := feed(t, f, test.createChangeFeedStmt)
				defer closeFeed(t, foo)
				for _, stmt := range test.stmts {
					sqlDB.Exec(t, stmt)
				}
				assertPayloads(t, foo, test.payloadsAfterStmts)
			}
			cdcTest(t, testFn, feedTestForceSink("kafka"))
		})
	}
}

func TestChangefeedEnrichedSourceSchemaInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name                          string
		format                        string
		expectedRow                   string
		expectedRowsAfterSchemaChange []string
	}{
		{
			name:        "json",
			format:      "json",
			expectedRow: `foo: {"a": 1, "b": "key1"}->{"after": {"a": 1, "b": "key1", "c": 100}, "op": "c"}`,
			expectedRowsAfterSchemaChange: []string{
				`foo: {"a": 1, "b": "key1"}->{"after": {"a": 1, "b": "key1", "c": 100, "d": "new_col"}, "op": "u"}`,
				`foo: {"a": 2, "b": "key2"}->{"after": {"a": 2, "b": "key2", "c": 200, "d": "new_value"}, "op": "c"}`,
			},
		},
		{
			name:        "avro",
			format:      "avro",
			expectedRow: `foo: {"a":{"long":1},"b":{"string":"key1"}}->{"after": {"foo": {"a": {"long": 1}, "b": {"string": "key1"}, "c": {"long": 100}}}, "op": {"string": "c"}}`,
			expectedRowsAfterSchemaChange: []string{
				`foo: {"a":{"long":1},"b":{"string":"key1"}}->{"after": {"foo": {"a": {"long": 1}, "b": {"string": "key1"}, "c": {"long": 100}, "d": {"string": "new_col"}}}, "op": {"string": "u"}}`,
				`foo: {"a":{"long":2},"b":{"string":"key2"}}->{"after": {"foo": {"a": {"long": 2}, "b": {"string": "key2"}, "c": {"long": 200}, "d": {"string": "new_value"}}}, "op": {"string": "c"}}`,
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

				var originalTableID int

				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `CREATE TABLE foo (a INT, b STRING, c INT, PRIMARY KEY (a, b))`)
				sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'key1', 100)`)

				stmt := fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='source', format=%s`, testCase.format)
				foo := feed(t, f, stmt)
				defer closeFeed(t, foo)

				sourceAssertion := func(actualSource map[string]any) {
					if testCase.format == "avro" {
						actualSourceValue := actualSource["source"].(map[string]any)
						require.Equal(t, map[string]any{"string": "foo"}, actualSourceValue["table_name"])
						require.Equal(t, map[string]any{"string": "public"}, actualSourceValue["schema_name"])
						require.Equal(t, map[string]any{"string": "d"}, actualSourceValue["database_name"])
						require.Equal(t, map[string]any{"array": []any{"a", "b"}}, actualSourceValue["primary_keys"])

						num := actualSourceValue["crdb_internal_table_id"].(map[string]any)["int"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						originalTableID = int(idInt)
					} else {
						require.Equal(t, "foo", actualSource["table_name"])
						require.Equal(t, "public", actualSource["schema_name"])
						require.Equal(t, "d", actualSource["database_name"])
						require.Equal(t, []any{"a", "b"}, actualSource["primary_keys"])

						num := actualSource["crdb_internal_table_id"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						originalTableID = int(idInt)
					}
				}
				assertPayloadsEnriched(t, foo, []string{testCase.expectedRow}, sourceAssertion)

				sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d STRING DEFAULT 'new_col'`)
				sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'key2', 200, 'new_value')`)

				sourceAssertionAfterSchemaChange := func(actualSource map[string]any) {
					if testCase.format == "avro" {
						actualSourceValue := actualSource["source"].(map[string]any)
						require.Equal(t, map[string]any{"string": "foo"}, actualSourceValue["table_name"])
						require.Equal(t, map[string]any{"string": "public"}, actualSourceValue["schema_name"])
						require.Equal(t, map[string]any{"string": "d"}, actualSourceValue["database_name"])
						require.Equal(t, map[string]any{"array": []any{"a", "b"}}, actualSourceValue["primary_keys"])

						num := actualSourceValue["crdb_internal_table_id"].(map[string]any)["int"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						require.Equal(t, originalTableID, int(idInt))
					} else {
						require.Equal(t, "foo", actualSource["table_name"])
						require.Equal(t, "public", actualSource["schema_name"])
						require.Equal(t, "d", actualSource["database_name"])
						require.Equal(t, []any{"a", "b"}, actualSource["primary_keys"])

						num := actualSource["crdb_internal_table_id"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						require.Equal(t, originalTableID, int(idInt))
					}
				}
				assertPayloadsEnriched(t, foo, testCase.expectedRowsAfterSchemaChange, sourceAssertionAfterSchemaChange)
			}

			cdcTest(t, testFn, feedTestRestrictSinks("kafka"))
		})
	}
}

func TestChangefeedEnrichedSourceSchemaInfoOnPrimaryKeyChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name             string
		format           string
		expectedRow      string
		expectedRowAfter string
	}{
		{
			name:             "json",
			format:           "json",
			expectedRow:      `foo: {"a": 1}->{"after": {"a": 1, "b": "initial"}, "op": "c"}`,
			expectedRowAfter: `foo: {"b": "new_key"}->{"after": {"a": 2, "b": "new_key"}, "op": "c"}`,
		},
		{
			name:             "avro",
			format:           "avro",
			expectedRow:      `foo: {"a":{"long":1}}->{"after": {"foo": {"a": {"long": 1}, "b": {"string": "initial"}}}, "op": {"string": "c"}}`,
			expectedRowAfter: `foo: {"b":{"string":"new_key"}}->{"after": {"foo": {"a": {"long": 2}, "b": {"string": "new_key"}}}, "op": {"string": "c"}}`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

				var originalTableID int

				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'initial')`)

				stmt := fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='source', format=%s`, testCase.format)
				foo := feed(t, f, stmt)
				defer closeFeed(t, foo)

				sourceAssertion := func(actualSource map[string]any) {
					if testCase.format == "avro" {
						actualSourceValue := actualSource["source"].(map[string]any)
						require.Equal(t, map[string]any{"string": "foo"}, actualSourceValue["table_name"])
						require.Equal(t, map[string]any{"string": "public"}, actualSourceValue["schema_name"])
						require.Equal(t, map[string]any{"string": "d"}, actualSourceValue["database_name"])
						require.Equal(t, map[string]any{"array": []any{"a"}}, actualSourceValue["primary_keys"])

						num := actualSourceValue["crdb_internal_table_id"].(map[string]any)["int"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						originalTableID = int(idInt)
					} else {
						require.Equal(t, "foo", actualSource["table_name"])
						require.Equal(t, "public", actualSource["schema_name"])
						require.Equal(t, "d", actualSource["database_name"])
						require.Equal(t, []any{"a"}, actualSource["primary_keys"])

						num := actualSource["crdb_internal_table_id"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						originalTableID = int(idInt)
					}
				}
				if testCase.format == "json" {
					assertPayloadsEnriched(t, foo, []string{testCase.expectedRow}, sourceAssertion)
				} else {
					assertPayloadsEnriched(t, foo, []string{testCase.expectedRow}, sourceAssertion)
				}

				sqlDB.Exec(t, `ALTER TABLE foo ALTER COLUMN b SET NOT NULL`)
				sqlDB.Exec(t, `ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b)`)
				sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'new_key')`)

				sourceAssertionAfterPKChange := func(actualSource map[string]any) {
					if testCase.format == "avro" {
						actualSourceValue := actualSource["source"].(map[string]any)
						require.Equal(t, map[string]any{"string": "foo"}, actualSourceValue["table_name"])
						require.Equal(t, map[string]any{"string": "public"}, actualSourceValue["schema_name"])
						require.Equal(t, map[string]any{"string": "d"}, actualSourceValue["database_name"])
						require.Equal(t, map[string]any{"array": []any{"b"}}, actualSourceValue["primary_keys"])

						num := actualSourceValue["crdb_internal_table_id"].(map[string]any)["int"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						require.Equal(t, originalTableID, int(idInt))
					} else {
						require.Equal(t, "foo", actualSource["table_name"])
						require.Equal(t, "public", actualSource["schema_name"])
						require.Equal(t, "d", actualSource["database_name"])
						require.Equal(t, []any{"b"}, actualSource["primary_keys"])

						num := actualSource["crdb_internal_table_id"].(gojson.Number)
						idInt, err := num.Int64()
						require.NoError(t, err)
						require.Equal(t, originalTableID, int(idInt))
					}
				}
				assertPayloadsEnriched(t, foo, []string{testCase.expectedRowAfter}, sourceAssertionAfterPKChange)
			}

			cdcTest(t, testFn, feedTestForceSink("kafka"))
		})
	}
}

func TestChangefeedEnrichedTableIDStableOnRename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='source'`)
		defer closeFeed(t, foo)

		var originalTableID int

		// Capture first row (before rename)
		sourceAssertionBefore := func(actualSource map[string]any) {
			num := actualSource["crdb_internal_table_id"].(gojson.Number)
			id, err := num.Int64()
			require.NoError(t, err)
			originalTableID = int(id)
		}
		assertPayloadsEnriched(t, foo, []string{`foo: {"i": 1}->{"after": {"i": 1}, "op": "c"}`}, sourceAssertionBefore)

		// Rename table
		sqlDB.Exec(t, `ALTER TABLE foo RENAME TO bar`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2)`)

		// Capture second row (after rename)
		sourceAssertionAfter := func(actualSource map[string]any) {
			num := actualSource["crdb_internal_table_id"].(gojson.Number)
			id, err := num.Int64()
			require.NoError(t, err)
			require.Equal(t, originalTableID, int(id))
		}
		assertPayloadsEnriched(t, foo, []string{`foo: {"i": 2}->{"after": {"i": 2}, "op": "c"}`}, sourceAssertionAfter)
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedWorksOnRBRChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFnJSON := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		t.Run("regional by row change works", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT) WITH (schema_locked=false)`)
			defer sqlDB.Exec(t, `DROP TABLE rbr`)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (0, NULL)`)
			rbr := feed(t, f, `CREATE CHANGEFEED FOR rbr`)
			defer closeFeed(t, rbr)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (1, 2)`)
			assertPayloads(t, rbr, []string{
				`rbr: [0]->{"after": {"a": 0, "b": null}}`,
				`rbr: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rbr SET LOCALITY REGIONAL BY ROW`)
			assertPayloads(t, rbr, []string{
				`rbr: ["us-east-1", 0]->{"after": {"a": 0, "b": null, "crdb_region": "us-east-1"}}`,
				`rbr: ["us-east-1", 1]->{"after": {"a": 1, "b": 2, "crdb_region": "us-east-1"}}`,
			})
		})
	}
	testFnAvro := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		t.Run("regional by row change works", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT) WITH (schema_locked=false)`)
			defer sqlDB.Exec(t, `DROP TABLE rbr`)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (0, NULL)`)
			rbr := feed(t, f, `CREATE CHANGEFEED FOR rbr WITH format=avro`)
			defer closeFeed(t, rbr)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (1, 2)`)
			assertPayloads(t, rbr, []string{
				`rbr: {"a":{"long":0}}->{"after":{"rbr":{"a":{"long":0},"b":null}}}`,
				`rbr: {"a":{"long":1}}->{"after":{"rbr":{"a":{"long":1},"b":{"long":2}}}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rbr SET LOCALITY REGIONAL BY ROW`)
			assertPayloads(t, rbr, []string{
				`rbr: {"a":{"long":0},"crdb_region":{"string":"us-east-1"}}->{"after":{"rbr":{"a":{"long":0},"b":null,"crdb_region":{"string":"us-east-1"}}}}`,
				`rbr: {"a":{"long":1},"crdb_region":{"string":"us-east-1"}}->{"after":{"rbr":{"a":{"long":1},"b":{"long":2},"crdb_region":{"string":"us-east-1"}}}}`,
			})
		})
		t.Run("regional by row as change works", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT, region crdb_internal_region NOT NULL DEFAULT 'us-east-1') WITH (schema_locked = false)`)
			defer sqlDB.Exec(t, `DROP TABLE rbr`)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (0, NULL)`)
			rbr := feed(t, f, `CREATE CHANGEFEED FOR rbr WITH format=avro`)
			defer closeFeed(t, rbr)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (1, 2)`)
			assertPayloads(t, rbr, []string{
				`rbr: {"a":{"long":0}}->{"after":{"rbr":{"a":{"long":0},"b":null,"region":{"string":"us-east-1"}}}}`,
				`rbr: {"a":{"long":1}}->{"after":{"rbr":{"a":{"long":1},"b":{"long":2},"region":{"string":"us-east-1"}}}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rbr SET LOCALITY REGIONAL BY ROW AS region`)
			assertPayloads(t, rbr, []string{
				`rbr: {"a":{"long":0},"region":{"string":"us-east-1"}}->{"after":{"rbr":{"a":{"long":0},"b":null,"region":{"string":"us-east-1"}}}}`,
				`rbr: {"a":{"long":1},"region":{"string":"us-east-1"}}->{"after":{"rbr":{"a":{"long":1},"b":{"long":2},"region":{"string":"us-east-1"}}}}`,
			})
		})
	}

	withTestServerRegion := func(args *base.TestServerArgs) {
		args.Locality.Tiers = append(args.Locality.Tiers, roachpb.Tier{
			Key:   "region",
			Value: testServerRegion,
		})
	}

	// Tenants skipped because of:
	//
	// error executing 'ALTER DATABASE d PRIMARY REGION
	// "us-east-1"': pq: get_live_cluster_regions: unimplemented:
	// operation is unsupported inside virtual clusters
	//
	// TODO(knz): This seems incorrect; see issue #109418.
	opts := []feedTestOption{
		feedTestNoTenants,
		feedTestEnterpriseSinks,
		withArgsFn(withTestServerRegion),
	}
	cdcTestNamedWithSystem(t, "format=json", testFnJSON, opts...)
	cdcTestNamed(t, "format=avro", testFnAvro, append(opts, feedTestForceSink("kafka"))...)
}

func TestChangefeedRBRAvroAddRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We need a cluster here to make sure we have multiple active
	// regions that we can add to the database.
	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	f := makeKafkaFeedFactory(t, cluster, db)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY) WITH (schema_locked = false)`)
	waitForSchemaChange(t, sqlDB, `ALTER TABLE rbr SET LOCALITY REGIONAL BY ROW`)
	sqlDB.Exec(t, `INSERT INTO rbr VALUES (0)`)
	rbr := feed(t, f, `CREATE CHANGEFEED FOR rbr WITH format=avro`)
	defer closeFeed(t, rbr)
	assertPayloads(t, rbr, []string{
		`rbr: {"a":{"long":0},"crdb_region":{"string":"us-east1"}}->{"after":{"rbr":{"a":{"long":0},"crdb_region":{"string":"us-east1"}}}}`,
	})

	// We do not expect a backfill from the ADD REGION, but we do
	// expect the new rows with the added region to be encoded
	// correctly.
	sqlDB.Exec(t, `ALTER DATABASE d ADD REGION "us-east2"`)
	sqlDB.Exec(t, `INSERT INTO rbr (crdb_region, a) VALUES ('us-east2', 1)`)
	assertPayloads(t, rbr, []string{
		`rbr: {"a":{"long":1},"crdb_region":{"string":"us-east2"}}->{"after":{"rbr":{"a":{"long":1},"crdb_region":{"string":"us-east2"}}}}`,
	})

	// An update is seen as a DELETE and and INSERT
	sqlDB.Exec(t, `UPDATE rbr SET crdb_region = 'us-east2' WHERE a = 0`)
	assertPayloads(t, rbr, []string{
		`rbr: {"a":{"long":0},"crdb_region":{"string":"us-east1"}}->{"after":null}`,
		`rbr: {"a":{"long":0},"crdb_region":{"string":"us-east2"}}->{"after":{"rbr":{"a":{"long":0},"crdb_region":{"string":"us-east2"}}}}`,
	})
}

func TestChangefeedStopOnSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	schemaChangeTimestampRegexp := regexp.MustCompile(`schema change occurred at ([0-9]+\.[0-9]+)`)
	timestampStrFromError := func(t *testing.T, err error) string {
		require.Regexp(t, schemaChangeTimestampRegexp, err)
		m := schemaChangeTimestampRegexp.FindStringSubmatch(err.Error())
		return m[1]
	}
	waitForSchemaChangeErrorAndCloseFeed := func(t *testing.T, f cdctest.TestFeed) (tsStr string) {
		t.Helper()
		for {
			if ev, err := f.Next(); err != nil {
				log.Infof(context.Background(), "got event %v %v", ev, err)
				tsStr = timestampStrFromError(t, err)
				_ = f.Close()
				return tsStr
			}
		}
	}
	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sysDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sysDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '50ms'")

		t.Run("add column not null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_not_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_not_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (0)`)
			addColumnNotNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_not_null `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop'`)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (1)`)
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [0]->{"after": {"a": 0}}`,
				`add_column_not_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_not_null ADD COLUMN b INT NOT NULL DEFAULT 0`)
			sqlDB.Exec(t, "INSERT INTO add_column_not_null VALUES (2, 1)")
			tsStr := waitForSchemaChangeErrorAndCloseFeed(t, addColumnNotNull)
			addColumnNotNull = feed(t, f, `CREATE CHANGEFEED FOR add_column_not_null `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop', cursor = '`+tsStr+`'`)
			defer closeFeed(t, addColumnNotNull)
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [2]->{"after": {"a": 2, "b": 1}}`,
			})
		})
		t.Run("add column null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (0)`)
			addColumnNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_null `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop'`)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (1)`)
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [0]->{"after": {"a": 0}}`,
				`add_column_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_null ADD COLUMN b INT`)
			sqlDB.Exec(t, "INSERT INTO add_column_null VALUES (2, NULL)")
			tsStr := waitForSchemaChangeErrorAndCloseFeed(t, addColumnNull)
			addColumnNull = feed(t, f, `CREATE CHANGEFEED FOR add_column_null `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop', cursor = '`+tsStr+`'`)
			defer closeFeed(t, addColumnNull)
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_comp_col (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_comp_col`)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (0)`)
			addCompCol := feed(t, f, `CREATE CHANGEFEED FOR add_comp_col `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop'`)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (1)`)
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [0]->{"after": {"a": 0}}`,
				`add_comp_col: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_comp_col ADD COLUMN b INT AS (a + 1) STORED`)
			sqlDB.Exec(t, "INSERT INTO add_comp_col VALUES (2)")
			tsStr := waitForSchemaChangeErrorAndCloseFeed(t, addCompCol)
			addCompCol = feed(t, f, `CREATE CHANGEFEED FOR add_comp_col `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop', cursor = '`+tsStr+`'`)
			defer closeFeed(t, addCompCol)
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [2]->{"after": {"a": 2, "b": 3}}`,
			})
		})
		t.Run("drop column", func(t *testing.T) {
			// Sinkless feeds are not currently able to restart in the face of
			// any schema changes. Dropping a column in the declarative schema
			// changer means that an extra error will occur.
			if _, isSinkless := f.(*sinklessFeedFactory); isSinkless {
				return
			}
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE drop_column`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (0, NULL)`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop'`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, 2)`)
			assertPayloads(t, dropColumn, []string{
				`drop_column: [0]->{"after": {"a": 0, "b": null}}`,
				`drop_column: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)
			tsStr := waitForSchemaChangeErrorAndCloseFeed(t, dropColumn)
			dropColumn = feed(t, f, `CREATE CHANGEFEED FOR drop_column `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop', cursor = '`+tsStr+`'`)
			defer closeFeed(t, dropColumn)
			assertPayloads(t, dropColumn, []string{
				`drop_column: [2]->{"after": {"a": 2}}`,
			})
		})
		t.Run("add index", func(t *testing.T) {
			// This case does not exit
			sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE add_index`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (0, NULL)`)
			addIndex := feed(t, f, `CREATE CHANGEFEED FOR add_index `+
				`WITH schema_change_events='column_changes', schema_change_policy='stop'`)
			defer closeFeed(t, addIndex)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, 2)`)
			assertPayloads(t, addIndex, []string{
				`add_index: [0]->{"after": {"a": 0, "b": null}}`,
				`add_index: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `CREATE INDEX ON add_index (b)`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, NULL)`)
			assertPayloads(t, addIndex, []string{
				`add_index: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
	}

	cdcTestWithSystem(t, testFn)
}

func TestChangefeedNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)
	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))

		usingLegacySchemaChanger := maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sysDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sysDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10ms'")

		t.Run("add column not null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_not_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_not_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (0)`)
			addColumnNotNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_not_null `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addColumnNotNull)
			sqlDB.Exec(t, `INSERT INTO add_column_not_null VALUES (1)`)
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [0]->{"after": {"a": 0}}`,
				`add_column_not_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_not_null ADD COLUMN b INT NOT NULL DEFAULT 0`)
			sqlDB.Exec(t, "INSERT INTO add_column_not_null VALUES (2, 1)")
			assertPayloads(t, addColumnNotNull, []string{
				`add_column_not_null: [2]->{"after": {"a": 2, "b": 1}}`,
			})
		})
		t.Run("add column null", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_column_null (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_column_null`)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (0)`)
			addColumnNull := feed(t, f, `CREATE CHANGEFEED FOR add_column_null `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addColumnNull)
			sqlDB.Exec(t, `INSERT INTO add_column_null VALUES (1)`)
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [0]->{"after": {"a": 0}}`,
				`add_column_null: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_column_null ADD COLUMN b INT`)
			sqlDB.Exec(t, "INSERT INTO add_column_null VALUES (2, NULL)")
			assertPayloads(t, addColumnNull, []string{
				`add_column_null: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
		t.Run(`add column computed`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE add_comp_col (a INT PRIMARY KEY)`)
			defer sqlDB.Exec(t, `DROP TABLE add_comp_col`)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (0)`)
			addCompCol := feed(t, f, `CREATE CHANGEFEED FOR add_comp_col `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addCompCol)
			sqlDB.Exec(t, `INSERT INTO add_comp_col VALUES (1)`)
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [0]->{"after": {"a": 0}}`,
				`add_comp_col: [1]->{"after": {"a": 1}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE add_comp_col ADD COLUMN b INT AS (a + 1) STORED`)
			sqlDB.Exec(t, "INSERT INTO add_comp_col VALUES (2)")
			assertPayloads(t, addCompCol, []string{
				`add_comp_col: [2]->{"after": {"a": 2, "b": 3}}`,
			})
		})
		t.Run("drop column", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE drop_column (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE drop_column`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (0, NULL)`)
			dropColumn := feed(t, f, `CREATE CHANGEFEED FOR drop_column `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, dropColumn)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (1, 2)`)
			assertPayloads(t, dropColumn, []string{
				`drop_column: [0]->{"after": {"a": 0, "b": null}}`,
				`drop_column: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE drop_column DROP COLUMN b`)
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (2)`)

			var payloads []string
			if usingLegacySchemaChanger {
				// NB: Legacy schema changes modify the physical KVs in place while
				// the changefeed is running, so you see a "backfill" even though
				// the changefeed does not perform one. If we did not specify
				// `schema_change_policy='nobackfill'`, then we would have seen
				// 0 and 1 an additional time before seeing row 2.
				payloads = []string{
					`drop_column: [0]->{"after": {"a": 0}}`,
					`drop_column: [1]->{"after": {"a": 1}}`,
					`drop_column: [2]->{"after": {"a": 2}}`,
				}
			} else {
				payloads = []string{
					`drop_column: [2]->{"after": {"a": 2}}`,
				}
			}
			assertPayloads(t, dropColumn, payloads)
		})
		t.Run("add index", func(t *testing.T) {
			// This case does not exit
			sqlDB.Exec(t, `CREATE TABLE add_index (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE add_index`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (0, NULL)`)
			addIndex := feed(t, f, `CREATE CHANGEFEED FOR add_index `+
				`WITH schema_change_policy='nobackfill'`)
			defer closeFeed(t, addIndex)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (1, 2)`)
			assertPayloads(t, addIndex, []string{
				`add_index: [0]->{"after": {"a": 0, "b": null}}`,
				`add_index: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `CREATE INDEX ON add_index (b)`)
			sqlDB.Exec(t, `INSERT INTO add_index VALUES (2, NULL)`)
			assertPayloads(t, addIndex, []string{
				`add_index: [2]->{"after": {"a": 2, "b": null}}`,
			})
		})
	}

	cdcTestWithSystem(t, testFn)
}

func TestChangefeedStoredComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn)
}

func TestChangefeedVirtualComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := map[string]struct {
		formatOpt               changefeedbase.FormatType
		virtualColumnVisibility changefeedbase.VirtualColumnVisibility
		changeFeedStmt          string
		payloadAfterInsert      []string
		payloadAfterUpdate      []string
	}{
		`format="json",virtual_columns="omitted"`: {
			formatOpt:               changefeedbase.OptFormatJSON,
			virtualColumnVisibility: changefeedbase.OptVirtualColumnsOmitted,
			payloadAfterInsert:      []string{`cc: [1]->{"after": {"a": 1, "b": 1}, "before": null}`},
			payloadAfterUpdate:      []string{`cc: [1]->{"after": {"a": 1, "b": 10}, "before": {"a": 1, "b": 1}}`},
		},
		`format="json",virtual_columns="null"`: {
			formatOpt:               changefeedbase.OptFormatJSON,
			virtualColumnVisibility: changefeedbase.OptVirtualColumnsNull,
			payloadAfterInsert:      []string{`cc: [1]->{"after": {"a": 1, "b": 1, "c": null}, "before": null}`},
			payloadAfterUpdate:      []string{`cc: [1]->{"after": {"a": 1, "b": 10, "c": null}, "before": {"a": 1, "b": 1, "c": null}}`},
		},
		`format="avro",virtual_columns="omitted"`: {
			formatOpt:               changefeedbase.OptFormatAvro,
			virtualColumnVisibility: changefeedbase.OptVirtualColumnsOmitted,
			payloadAfterInsert:      []string{`cc: {"a":{"long":1}}->{"after":{"cc":{"a":{"long":1},"b":{"long":1}}},"before":null}`},
			payloadAfterUpdate:      []string{`cc: {"a":{"long":1}}->{"after":{"cc":{"a":{"long":1},"b":{"long":10}}},"before":{"cc_before":{"a":{"long":1},"b":{"long":1}}}}`},
		},
		`format="avro",virtual_columns="null"`: {
			formatOpt:               changefeedbase.OptFormatAvro,
			virtualColumnVisibility: changefeedbase.OptVirtualColumnsNull,
			payloadAfterInsert:      []string{`cc: {"a":{"long":1}}->{"after":{"cc":{"a":{"long":1},"b":{"long":1},"c":null}},"before":null}`},
			payloadAfterUpdate:      []string{`cc: {"a":{"long":1}}->{"after":{"cc":{"a":{"long":1},"b":{"long":10},"c":null}},"before":{"cc_before":{"a":{"long":1},"b":{"long":1},"c":null}}}`},
		},
	}

	for _, test := range tests {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)

			sqlDB.Exec(t, `CREATE TABLE cc (
					a INT primary key, b INT, c INT AS (b + 1) VIRTUAL NOT NULL
				)`)
			defer sqlDB.Exec(t, `DROP TABLE cc`)

			sqlDB.Exec(t, `INSERT INTO cc VALUES (1, 1)`)

			changeFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR cc WITH diff, format="%s", virtual_columns="%s"`,
				test.formatOpt, test.virtualColumnVisibility))
			defer closeFeed(t, changeFeed)

			assertPayloads(t, changeFeed, test.payloadAfterInsert)

			sqlDB.Exec(t, `UPDATE cc SET b=10 WHERE a=1`)

			assertPayloads(t, changeFeed, test.payloadAfterUpdate)
		}

		if test.formatOpt != changefeedbase.OptFormatAvro {
			cdcTest(t, testFn)
		} else {
			cdcTest(t, testFn, feedTestForceSink("kafka"))
		}
	}
}

func TestChangefeedUpdatePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn)
}

// Primary key changes are supported by changefeeds starting in 21.1. This tests
// that basic behavior works.
func TestChangefeedPrimaryKeyChangeWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY USING HASH, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		const baseStmt = `CREATE CHANGEFEED FOR foo WITH resolved = '100ms'`
		foo := feed(t, f, baseStmt)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [2, 0]->{"after": {"a": 0, "b": "updated"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [11, 1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [6, 2]->{"after": {"a": 2, "b": "b"}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b) USING HASH`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3, 'c'), (4, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [6, "c"]->{"after": {"a": 3, "b": "c"}}`,
			`foo: [15, "d"]->{"after": {"a": 4, "b": "d"}}`,
		})

		// ALTER PRIMARY KEY should work and we should see the changed
		// primary key in subsequent writes.
		sqlDB.Exec(t, `
BEGIN;
ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (a);
INSERT INTO foo VALUES (5, 'e');
UPDATE foo SET a = 6 WHERE b = 'a';
COMMIT;
INSERT INTO foo VALUES (1, 'f');
`)
		// Note that the primary key change is asynchronous and that only the
		// subsequent write will be displayed using the new primary key.
		assertPayloads(t, foo, []string{
			`foo: [6, "a"]->{"after": {"a": 6, "b": "a"}}`,
			`foo: [14, "e"]->{"after": {"a": 5, "b": "e"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "f"}}`,
		})
	}

	cdcTest(t, testFn)
}

// Primary key changes are supported by changefeeds starting in 21.1. This test
// specifically focuses on backfill behavior when a single transaction changes
// multiple tables including a primary key change to one and a column change
// requiring a backfill to another.
//
// Note that at time of writing, this change will not end up occurring in the
// same transaction and thus at the same moment but in later code changes, it
// will.
func TestChangefeedPrimaryKeyChangeWorksWithMultipleTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'a')`)

		const baseStmt = `CREATE CHANGEFEED FOR foo, bar WITH resolved = '100ms'`
		cf := feed(t, f, baseStmt)
		defer closeFeed(t, cf)

		// maybeHandleRestart deals with the fact that sinkless changefeeds don't
		// gracefully handle primary index changes but rather force the client to
		// deal with restarting the changefeed as of the last resolved timestamp.
		//
		// This ends up being pretty sane; sinkless changefeeds already require this
		// behavior in the face of other transient failures so clients already need
		// to implement this logic.
		maybeHandleRestart := func(t *testing.T) (cleanup func()) {
			return func() {}
		}

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, cf, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
			`bar: [1]->{"after": {"a": 1, "b": "a"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'b'), (3, 'c')`)
		assertPayloads(t, cf, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
			`bar: [2]->{"after": {"a": 2, "b": "b"}}`,
			`bar: [3]->{"after": {"a": 3, "b": "c"}}`,
		})

		sqlDB.Exec(t, `
BEGIN;
ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b);
INSERT INTO bar VALUES (4, 'd'), (5, 'e');
INSERT INTO foo VALUES (3, 'c');
COMMIT;
INSERT INTO foo VALUES (4, 'd');
INSERT INTO bar VALUES (6, 'f');
`)

		assertPayloads(t, cf, []string{
			`bar: [4]->{"after": {"a": 4, "b": "d"}}`,
			`bar: [5]->{"after": {"a": 5, "b": "e"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "c"}}`,
		})
		defer maybeHandleRestart(t)()
		assertPayloads(t, cf, []string{
			`foo: ["d"]->{"after": {"a": 4, "b": "d"}}`,
			`bar: [6]->{"after": {"a": 6, "b": "f"}}`,
		})
	}

	cdcTest(t, testFn)
}

// TestChangefeedCheckpointSchemaChange tests to make sure that writes that
// occur in the same transaction that performs an immediately visible schema
// change, like drop column, observe the schema change. Also, this tests that
// resuming from that cursor from the same timestamp as the schema change
// only includes later updates (thus validating the cursor semantics as they
// pertain to schema changes). It also does that test using an initial
// backfill, which makes the cursor, more or less, inclusive rather than
// exclusive.
func TestChangefeedCheckpointSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Uses transactions below, which don't support declarative schema changer.
		sqlDB.Exec(t, "SET create_table_with_schema_locked=false")
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'initial')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH resolved = '100ms', updated`,
			optOutOfMetamorphicEnrichedEnvelope{reason: "this test uses readNextMessages directly, which the metamorphic enriched envelope does not support"})

		// Sketch of the test is as follows:
		//
		//  1) Write some rows into two tables, foo and bar.
		//  2) In a transaction, write to and update both tables,
		//     and drop a column on foo.
		//  3) Ensure that the messages are the 3 writes to foo, 2
		//     writes to bar and then the 3 values of foo being
		//     rewritten. Also note that none of the foo values are
		//     under the old schema.
		//  4) Extract the timestamp from the initial messages.
		//  5) Run a cursor-based changefeed from that timestamp with
		//     no initial_scan. See only the 3 touch writes.
		//  6) Run a cursor-based changefeed from that timestamp with
		//     with initial_scan. See all 8 writes at the same timestamps.
		//
		assertPayloadsStripTs(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
			`bar: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})

		_, err := s.DB.Exec("SET autocommit_before_ddl = false")
		require.NoError(t, err)
		require.NoError(t, crdb.ExecuteTx(context.Background(), s.DB, nil, func(tx *gosql.Tx) error {
			for _, stmt := range []string{
				`CREATE TABLE baz ()`,
				`INSERT INTO foo VALUES (2, 'initial')`,
				`INSERT INTO foo VALUES (1, 'initial')`,
				`UPSERT INTO foo VALUES (0, 'updated')`,
				`ALTER TABLE foo DROP COLUMN b`,
				`UPSERT INTO bar VALUES (0, 'updated')`,
				`UPSERT INTO bar VALUES (1, 'initial')`,
			} {
				if _, err := tx.Exec(stmt); err != nil {
					return err
				}
			}
			return nil
		}))
		_, err = s.DB.Exec("RESET autocommit_before_ddl")
		require.NoError(t, err)

		expected := []string{
			`bar: [0]->{"after": {"a": 0, "b": "updated"}}`,
			`bar: [1]->{"after": {"a": 1, "b": "initial"}}`,
			`foo: [0]->{"after": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			// Touch writes due to column backfill.
			`foo: [0]->{"after": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
		}
		msgs, err := readNextMessages(context.Background(), foo, len(expected))
		require.NoError(t, err)

		var msgsFormatted []string
		for _, m := range msgs {
			msgsFormatted = append(msgsFormatted, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, m.Value))
		}

		// Sort the messages by their timestamp.
		re := regexp.MustCompile(`.*(, "updated": "(\d+\.\d+)")}.*`)
		getHLC := func(i int) string { return re.FindStringSubmatch(msgsFormatted[i])[2] }
		trimHlC := func(s string) string {
			indexes := re.FindStringSubmatchIndex(s)
			return s[:indexes[2]] + s[indexes[3]:]
		}
		sort.Slice(msgsFormatted, func(i, j int) bool {
			a, b := getHLC(i), getHLC(j)
			if a == b {
				return msgsFormatted[i] < msgsFormatted[j]
			}
			return a < b
		})
		schemaChangeTS := getHLC(0)
		stripped := make([]string, len(msgsFormatted))
		for i, m := range msgsFormatted {
			stripped[i] = trimHlC(m)
		}
		require.Equal(t, expected, stripped)
		// Make sure there are no more messages.
		{
			next, err := foo.Next()
			require.NoError(t, err)
			require.NotNil(t, next.Resolved)
		}
		closeFeed(t, foo)

		t.Run("cursor, no backfill", func(t *testing.T) {
			// Resume at exactly the timestamp of the schema change, observe only
			// events after it.
			foo = feed(t, f,
				"CREATE CHANGEFEED FOR foo, bar WITH"+
					" resolved = '100ms', updated, cursor = $1",
				schemaChangeTS)
			defer closeFeed(t, foo)
			// Observe only the touch writes.
			assertPayloads(t, foo, msgsFormatted[5:])
			// Make sure there are no more messages.
			{
				next, err := foo.Next()
				require.NoError(t, err)
				require.NotNil(t, next.Resolved)
			}
		})

		t.Run("cursor, with backfill", func(t *testing.T) {
			// Resume at exactly the timestamp of the schema change, observe the
			// writes at that timestamp exactly, but with the new schema change.
			foo = feed(t, f,
				"CREATE CHANGEFEED FOR foo, bar WITH"+
					" resolved = '100ms', updated, cursor = $1, initial_scan",
				schemaChangeTS)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, msgsFormatted)
			// Make sure there are no more messages.
			{
				next, err := foo.Next()
				require.NoError(t, err)
				require.NotNil(t, next.Resolved)
			}
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedPredicateWithSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes too long under race")

	setupSQL := []string{
		`CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`,
		`CREATE SCHEMA alt`,
		`CREATE TYPE alt.status AS ENUM ('alt_open', 'alt_closed')`,
		`CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  e status DEFAULT 'inactive',
  PRIMARY KEY (a, b)
)`,
		`INSERT INTO foo (a, b) VALUES (1, 'one')`,
		`INSERT INTO foo (a, b, c, e) VALUES (2, 'two', 'c string', 'open')`,
	}
	initialPayload := []string{
		`foo: [1, "one"]->{"a": 1, "b": "one", "c": null, "e": "inactive"}`,
		`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
	}

	type testCase struct {
		name                string
		disableSchemaLocked bool
		createFeedStmt      string   // Create changefeed statement.
		initialPayload      []string // Expected payload after create.
		alterStmt           string   // Alter statement to execute.
		afterAlterStmt      string   // Execute after alter statement.
		expectErr           string   // Alter may result in changefeed terminating with error.
		payload             []string // Expect the following payload after executing afterAlterStmt.
	}

	testFn := func(tc testCase) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)

			if tc.disableSchemaLocked {
				sqlDB.Exec(t, "SET create_table_with_schema_locked=false")
			}
			sqlDB.ExecMultiple(t, setupSQL...)
			foo := feed(t, f, tc.createFeedStmt)
			feedJob := foo.(cdctest.EnterpriseTestFeed)
			defer closeFeed(t, foo)

			assertPayloads(t, foo, tc.initialPayload)

			sqlDB.Exec(t, tc.alterStmt)

			// Execute afterAlterStmt immediately following alterStmt. Sometimes, we
			// need to e.g. insert new rows in order to observe changefeed error.
			if tc.afterAlterStmt != "" {
				sqlDB.Exec(t, tc.afterAlterStmt)
			}

			if tc.expectErr != "" {
				require.NoError(t, feedJob.WaitForState(
					func(s jobs.State) bool { return s == jobs.StateFailed }))
				require.Regexp(t, tc.expectErr, feedJob.FetchTerminalJobErr())
			} else {
				assertPayloads(t, foo, tc.payload)
			}
		}
	}

	for _, tc := range []testCase{
		{
			name:           "add column no default",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo ADD COLUMN new STRING",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			payload: []string{
				`foo: [3, "tres"]->{"a": 3, "b": "tres", "c": null, "e": "inactive", "new": null}`,
			},
		},
		{
			name:           "add column",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo ADD COLUMN new STRING DEFAULT 'new'",
			payload: []string{
				`foo: [1, "one"]->{"a": 1, "b": "one", "c": null, "e": "inactive", "new": "new"}`,
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open", "new": "new"}`,
			},
		},
		{
			// This test adds a column with 'alt.status' type. The table already has a
			// column "e" with "public.status" type. Verify that we correctly resolve
			// enums with the same enum name.
			name:           "add alt.status",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo ADD COLUMN alt alt.status DEFAULT 'alt_closed'",
			afterAlterStmt: "INSERT INTO foo (a, b, alt) VALUES (3, 'tres', 'alt_open')",
			payload: []string{
				`foo: [1, "one"]->{"a": 1, "alt": "alt_closed", "b": "one", "c": null, "e": "inactive"}`,
				`foo: [2, "two"]->{"a": 2, "alt": "alt_closed", "b": "two", "c": "c string", "e": "open"}`,
				`foo: [3, "tres"]->{"a": 3, "alt": "alt_open", "b": "tres", "c": null, "e": "inactive"}`,
			},
		},
		{
			name:           "drop column",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo DROP COLUMN c",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			payload: []string{
				`foo: [1, "one"]->{"a": 1, "b": "one", "e": "inactive"}`,
				`foo: [2, "two"]->{"a": 2, "b": "two", "e": "open"}`,
				`foo: [3, "tres"]->{"a": 3, "b": "tres", "e": "inactive"}`,
			},
		},
		{
			name:           "drop referenced column projection",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT a, b, c, e FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo DROP COLUMN c",
			expectErr:      `column "c" does not exist`,
		},
		{
			name:           "drop referenced column filter",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo WHERE c IS NOT NULL",
			initialPayload: []string{
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
			},
			alterStmt: "ALTER TABLE foo DROP COLUMN c",
			expectErr: `column "c" does not exist`,
		},
		{
			name:           "rename referenced column projection",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT a, b, c, e FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo RENAME COLUMN c TO c_new",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			expectErr:      `column "c" does not exist`,
		},
		{
			name:           "rename referenced column filter",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo WHERE c IS NOT NULL",
			initialPayload: []string{
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
			},
			alterStmt:      "ALTER TABLE foo RENAME COLUMN c TO c_new",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			expectErr:      `column "c" does not exist`,
		},
		{
			name:           "alter enum",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TYPE status ADD VALUE 'pending'",
			afterAlterStmt: "INSERT INTO foo (a, b, e) VALUES (3, 'tres', 'pending')",
			payload: []string{
				`foo: [3, "tres"]->{"a": 3, "b": "tres", "c": null, "e": "pending"}`,
			},
		},
		{
			name:           "alter enum value fails",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo WHERE e = 'open'",
			initialPayload: []string{
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
			},
			alterStmt:      "ALTER TYPE status RENAME VALUE 'open' TO 'active'",
			afterAlterStmt: "INSERT INTO foo (a, b, e) VALUES (3, 'tres', 'active')",
			expectErr:      `invalid input value for enum status: "open"`,
		},
		{
			name:           "alter enum use correct enum version",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT e, (cdc_prev).e AS prev_e FROM foo",
			initialPayload: []string{
				`foo: [1, "one"]->{"e": "inactive", "prev_e": null}`,
				`foo: [2, "two"]->{"e": "open", "prev_e": null}`,
			},
			alterStmt:      "ALTER TYPE status ADD VALUE 'done'",
			afterAlterStmt: "UPDATE foo SET e = 'done', c = 'c value' WHERE a = 1",
			payload: []string{
				`foo: [1, "one"]->{"e": "done", "prev_e": "inactive"}`,
			},
		},
		{
			// Alter and rename a column. The changefeed expression does not
			// explicitly involve the column in question (c) -- so, schema change works
			// fine. Note: we get 2 backfill events -- one for each logical change
			// (rename column, then add column).
			name:                "add and rename column",
			disableSchemaLocked: true, // legacy schema change
			createFeedStmt:      "CREATE CHANGEFEED AS SELECT *, (cdc_prev).e as old_e FROM foo",
			initialPayload: []string{
				`foo: [1, "one"]->{"a": 1, "b": "one", "c": null, "e": "inactive", "old_e": null}`,
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open", "old_e": null}`,
			},
			alterStmt: "ALTER TABLE foo RENAME COLUMN c to c_old, ADD COLUMN c int DEFAULT 42",
			payload: []string{
				`foo: [1, "one"]->{"a": 1, "b": "one", "c": 42, "c_old": null, "e": "inactive", "old_e": "inactive"}`,
				`foo: [1, "one"]->{"a": 1, "b": "one", "c_old": null, "e": "inactive", "old_e": "inactive"}`,
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": 42, "c_old": "c string", "e": "open", "old_e": "open"}`,
				`foo: [2, "two"]->{"a": 2, "b": "two", "c_old": "c string", "e": "open", "old_e": "open"}`,
			},
		},
		{
			// Alter and rename a column. The changefeed expression does
			// explicitly involve the column in question (c) -- so we expect
			// to get an error because as soon as the first rename goes through, column
			// no longer exists.
			name:                "add and rename column error",
			disableSchemaLocked: true, // legacy schema change
			createFeedStmt:      "CREATE CHANGEFEED AS SELECT c, (cdc_prev).c AS prev_c FROM foo",
			initialPayload: []string{
				`foo: [1, "one"]->{"c": null, "prev_c": null}`,
				`foo: [2, "two"]->{"c": "c string", "prev_c": null}`,
			},
			alterStmt: "ALTER TABLE foo RENAME COLUMN c to c_old, ADD COLUMN c int DEFAULT 42",
			expectErr: `column "c" does not exist`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testOpts := []feedTestOption{feedTestEnterpriseSinks}
			if tc.expectErr != "" {
				testOpts = append(testOpts, withAllowChangefeedErr(tc.expectErr))
			}
			cdcTest(t, testFn(tc), testOpts...)
		})
	}
}

// Regression for #85008.
func TestSchemachangeDoesNotBreakSinklessFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, log.SetVModule("kv_feed=2,changefeed_processors=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE mytable (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO mytable VALUES (0)`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE mytable`)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`mytable: [0]->{"after": {"id": 0}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE mytable ADD COLUMN val INT DEFAULT 0`)
		assertPayloads(t, cf, []string{
			`mytable: [0]->{"after": {"id": 0, "val": 0}}`,
		})
		sqlDB.Exec(t, `INSERT INTO mytable VALUES (1,1)`)
		assertPayloads(t, cf, []string{
			`mytable: [1]->{"after": {"id": 1, "val": 1}}`,
		})
	}

	runWithAndWithoutRegression141453(t, testFn, func(t *testing.T, testFn cdcTestFn) {
		cdcTest(t, testFn, feedTestForceSink("sinkless"))
	})
}
