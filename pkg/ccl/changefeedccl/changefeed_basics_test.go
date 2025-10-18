package changefeedccl

import (
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn, feedTestForceSink("kafka"))
	cdcTest(t, testFn, feedTestForceSink("enterprise"))
	cdcTest(t, testFn, feedTestForceSink("webhook"))
	cdcTest(t, testFn, feedTestForceSink("pubsub"))
	cdcTest(t, testFn, feedTestForceSink("sinkless"))
	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))

	// NB running TestChangefeedBasics, which includes a DELETE, with
	// cloudStorageTest is a regression test for #36994.
}

func TestDatabaseLevelChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		sqlDB.Exec(t, `CREATE TABLE foo2 (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo2 VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
			`foo2: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo2: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo2: [2]->{"after": {"a": 2, "b": "b"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2, "b": "c"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "d"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo2 VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo2: [2]->{"after": {"a": 2, "b": "c"}}`,
			`foo2: [3]->{"after": {"a": 3, "b": "d"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo2 WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo2: [1]->{"after": null}`,
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedBasicQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		foo := feed(t, f, `CREATE CHANGEFEED AS SELECT *, event_op() AS op, cdc_prev FROM foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"a": 0, "b": "updated", "cdc_prev": null, "op": "insert"}`,
		})
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"a": 1, "b": "a", "cdc_prev": null, "op": "insert"}`,
			`foo: [2]->{"a": 2, "b": "b", "cdc_prev": null, "op": "insert"}`,
		})
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"a": 2, "b": "c", "cdc_prev": {"a": 2, "b": "b"}, "op": "update"}`,
			`foo: [3]->{"a": 3, "b": "d", "cdc_prev": null, "op": "insert"}`,
		})
		// Deleted rows with bare envelope are emitted with only
		// the key columns set.
		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"a": 1, "b": null, "cdc_prev": {"a": 1, "b": "a"}, "op": "delete"}`,
		})
	}

	cdcTest(t, testFn)
}

// Same test as TestChangefeedBasicQuery, but using wrapped envelope with CDC query.
func TestChangefeedBasicQueryWrapped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		// Currently, parquet format (which may be injected by feed() call),  doesn't
		// know how to handle tuple types (cdc_prev); so, force JSON format.
		foo := feed(t, f, `
CREATE CHANGEFEED WITH envelope='wrapped', format='json', diff
AS SELECT b||a AS ba, event_op() AS op  FROM foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"ba": "updated0", "op": "insert"}, "before": null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"ba": "a1", "op": "insert"}, "before": null}`,
			`foo: [2]->{"after": {"ba": "b2", "op": "insert"}, "before": null}`,
		})

		// Wrapped envelope results in "before" having entire previous row state -- *not* projection.
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"ba": "c2", "op": "update"}, "before": {"a": 2, "b": "b"}}`,
			`foo: [3]->{"after": {"ba": "d3", "op": "insert"}, "before": null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": null, "before": {"a": 1, "b": "a"}}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("webhook"))
}

// Same test as TestChangefeedBasicQueryWrapped, but this time using AVRO.
func TestChangefeedBasicQueryWrappedAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		foo := feed(t, f, `
CREATE CHANGEFEED WITH envelope='wrapped', format='avro', diff
AS SELECT *, event_op() AS op  FROM foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":0}}->{"after":{"foo":{"a":{"long":0},"b":{"string":"updated"},"op":{"string":"insert"}}},"before":null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"a"},"op":{"string":"insert"}}},"before":null}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"string":"b"},"op":{"string":"insert"}}},"before":null}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"string":"c"},"op":{"string":"update"}}},"before":{"foo_before":{"a":{"long":2},"b":{"string":"b"}}}}`,
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"d"},"op":{"string":"insert"}}},"before":null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":null,"before":{"foo_before":{"a":{"long":1},"b":{"string":"a"}}}}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestToJSONAsChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo values (1, 'hello')`)
		sqlDB.CheckQueryResults(t,
			`SELECT crdb_internal.to_json_as_changefeed_with_flags(foo.*) from foo`,
			[][]string{{`{"after": {"a": 1, "b": "hello"}}`}},
		)
		sqlDB.CheckQueryResults(t,
			`SELECT crdb_internal.to_json_as_changefeed_with_flags(foo.*, 'updated', 'diff') from foo`,
			[][]string{{`{"after": {"a": 1, "b": "hello"}, "before": null, "updated": "0.0000000000"}`}},
		)

		sqlDB.CheckQueryResults(t,
			`SELECT crdb_internal.to_json_as_changefeed_with_flags(foo.*, 'updated', 'envelope=row') from foo`,
			[][]string{{`{"__crdb__": {"updated": "0.0000000000"}, "a": 1, "b": "hello"}`}},
		)

		sqlDB.ExpectErrWithTimeout(t, `unknown envelope: lobster`,
			`SELECT crdb_internal.to_json_as_changefeed_with_flags(foo.*, 'updated', 'envelope=lobster') from foo`)
	}

	cdcTest(t, testFn)
}

// TestChangefeedSendError validates that SendErrors do not fail the changefeed
// as they can occur in normal situations such as a cluster update
func TestChangefeedSendError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Allow triggering a single sendError
		sendErrorCh := make(chan error, 1)
		knobs.FeedKnobs.OnRangeFeedValue = func() error {
			select {
			case err := <-sendErrorCh:
				return err
			default:
				return nil
			}
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		sendErrorCh <- kvcoord.TestNewSendError("test sendError")
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)

		// Changefeed should've been retried due to the SendError
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sli, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		retryCounter := sli.ErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Value() < 1 {
				return fmt.Errorf("no retry has occured")
			}
			return nil
		})

		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
			`foo: [4]->{"after": {"a": 4}}`,
		})
	}, feedTestEnterpriseSinks, withAllowChangefeedErr("injects error"))
}

func TestChangefeedBasicConfluentKafka(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f,
			fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":0}}->{"after":{"foo":{"a":{"long":0},"b":{"string":"updated"}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"a"}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"string":"b"}}}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"string":"c"}}}}`,
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"d"}}}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":null}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedQuotedTableNameTopicName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE "MyTable" (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO "MyTable" VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO "MyTable" VALUES (0, 'updated')`)

		foo := feed(t, f,
			`CREATE CHANGEFEED FOR d.public."MyTable" WITH diff, full_table_name`)
		defer closeFeed(t, foo)

		// The topic name should be d.public.MyTable and not d.public._u0022_MyTable_u0022_
		// or d.public."MyTable".
		assertPayloads(t, foo, []string{
			`d.public.MyTable: [0]->{"after": {"a": 0, "b": "updated"}, "before": null}`,
		})
	}

	cdcTest(t, testFn)
}

// TestChangefeedQuotedIdentifiersTopicName is similar to
// TestChangefeedQuotedTableNameTopicName, but for quoted identifiers
// in the SELECT clause instead of the table name.
func TestChangefeedQuotedIdentifiersTopicName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE mytable (
			id INT PRIMARY KEY,
			"SomeField" JSONB,
			"AnotherField" JSONB
		)`)

		sqlDB.Exec(t, `INSERT INTO mytable VALUES (
			1,
			'{"PropA": "value1", "prop_b": "value2"}'::jsonb,
			'{"PropC": "value3", "prop_d": "value4"}'::jsonb
		)`)

		sqlDB.Exec(t, `INSERT INTO mytable VALUES (
			2,
			'{"PropA": "value5", "prop_b": "value6"}'::jsonb,
			'{"PropC": "value7", "prop_d": "value8"}'::jsonb
		)`)

		foo := feed(t, f, `CREATE CHANGEFEED WITH diff, full_table_name, on_error=pause, envelope=wrapped AS SELECT
			id,
			"SomeField"->>'PropA' AS "PropA",
			"SomeField"->>'prop_b' AS "PropB",
			"AnotherField"->>'PropC' AS "PropC",
			"AnotherField"->>'prop_d' AS "PropD"
		FROM public.mytable`)
		defer closeFeed(t, foo)

		// The topic should show up as d.public.mytable and not as
		// d.public.u0022_mytable_u0022 or d.public."MyTable".
		assertPayloads(t, foo, []string{
			`d.public.mytable: [1]->{"after": {"PropA": "value1", "PropB": "value2", "PropC": "value3", "PropD": "value4", "id": 1}, "before": null}`,
			`d.public.mytable: [2]->{"after": {"PropA": "value5", "PropB": "value6", "PropC": "value7", "PropD": "value8", "id": 2}, "before": null}`,
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}, "before": null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}, "before": null}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}, "before": null}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2, "b": "c"}, "before": {"a": 2, "b": "b"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "d"}, "before": null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": null, "before": {"a": 1, "b": "a"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'new a')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "new a"}, "before": null}`,
		})
	}

	cdcTest(t, testFn)
}

func TestDatabaseLevelChangefeedDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		sqlDB.Exec(t, `CREATE TABLE foo2 (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo2 VALUES (0, 'updated')`)
		d := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d WITH diff`)
		defer closeFeed(t, d)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, d, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}, "before": null}`,
			`foo2: [0]->{"after": {"a": 0, "b": "updated"}, "before": null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, d, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}, "before": null}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}, "before": null}`,
		})
		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, d, []string{
			`foo2: [1]->{"after": {"a": 1, "b": "a"}, "before": null}`,
			`foo2: [2]->{"after": {"a": 2, "b": "b"}, "before": null}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, d, []string{
			`foo: [2]->{"after": {"a": 2, "b": "c"}, "before": {"a": 2, "b": "b"}}`,
			`foo: [3]->{"after": {"a": 3, "b": "d"}, "before": null}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo2 VALUES (2, 'c'), (3, 'd')`)
		assertPayloads(t, d, []string{
			`foo2: [2]->{"after": {"a": 2, "b": "c"}, "before": {"a": 2, "b": "b"}}`,
			`foo2: [3]->{"after": {"a": 3, "b": "d"}, "before": null}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloads(t, d, []string{
			`foo: [1]->{"after": null, "before": {"a": 1, "b": "a"}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo2 WHERE a = 1`)
		assertPayloads(t, d, []string{
			`foo2: [1]->{"after": null, "before": {"a": 1, "b": "a"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'new a')`)
		assertPayloads(t, d, []string{
			`foo: [1]->{"after": {"a": 1, "b": "new a"}, "before": null}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (1, 'new a')`)
		assertPayloads(t, d, []string{
			`foo2: [1]->{"after": {"a": 1, "b": "new a"}, "before": null}`,
		})
	}

	cdcTest(t, testFn)
}

func TestMissingTableErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, cleanup := makeServer(t)
	defer cleanup()

	t.Run("changefeed on non existing table fails", func(t *testing.T) {
		kvSQL := sqlutils.MakeSQLRunner(s.DB)
		kvSQL.ExpectErr(t, `^pq: failed to resolve targets in the CHANGEFEED stmt: table "foo" does not exist`,
			`CREATE CHANGEFEED FOR foo`,
		)
	})
}

func TestChangefeedMissingDatabaseErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR DATABASE foo`, `database "foo" does not exist`)
	})
}

func TestChangefeedCannotTargetSystemDatabaseErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR DATABASE system`, `changefeed cannot target the system database`)
	})
}

func TestChangefeedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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
		// TODO(#139660): add envelope=enriched here
	}

	// some sinks are incompatible with envelope
	cdcTest(t, testFn, feedTestRestrictSinks("sinkless", "enterprise", "kafka"))
}

func TestChangefeedFullTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

		t.Run(`envelope=row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH full_table_name`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`})
		})
	})
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn)
}

func TestChangefeedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	require.NoError(t, log.SetVModule("event_processing=3,blocking_buffer=2"))

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		// NB: The test server is a single node and the hlc clock is a
		// singleton. Any transaction (ie. `INSERT INTO`) or call to
		// s.Server.Clock().Now() will share this clock. Any read of the clock
		// increments its current logical time. Thus, the operations below which
		// happen in sequence will have strictly increasing logical timestamps.
		beforeInsert := s.Server.Clock().Now()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
		insertTimestamp := s.Server.Clock().Now()

		tsLogical := s.Server.Clock().Now()
		tsClock := timeutil.FromUnixNanos(tsLogical.WallTime)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

		// Sanity check that operations happened in the expected order.
		require.True(t, beforeInsert.Less(insertTimestamp) && insertTimestamp.Less(tsLogical) && tsLogical.Less(s.Server.Clock().Now()),
			fmt.Sprintf("beforeInsert: %s, insertTimestamp: %s, tsLogical: %s", beforeInsert, insertTimestamp, tsLogical))

		t.Run("negative cursor", func(t *testing.T) {
			// The below function is currently used to test negative timestamp in cursor i.e of the form
			// "-3us".
			// Using this function we can calculate the difference with the time that was before
			// the insert statement, which is set as the new cursor value inside createChangefeedJobRecord
			calculateCursor := func(currentTime *hlc.Timestamp) string {
				//  Should convert to microseconds as that is the maximum precision we support
				diff := (beforeInsert.WallTime - currentTime.WallTime) / 1000
				diffStr := strconv.FormatInt(diff, 10) + "us"
				return diffStr
			}

			knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
			knobs.OverrideCursor = calculateCursor

			// The "-3 days" is a placeholder here - it will be replaced with actual difference
			// in createChangefeedJobRecord
			fooInterval := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, "-3 days")
			defer closeFeed(t, fooInterval)
			assertPayloads(t, fooInterval, []string{
				`foo: [1]->{"after": {"a": 1, "b": "before"}}`,
				`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
			})

			// We do not need to override for the remaining cases
			knobs.OverrideCursor = nil
		})

		t.Run("decimal cursor", func(t *testing.T) {
			fooLogical := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, eval.TimestampToDecimalDatum(tsLogical).String())
			defer closeFeed(t, fooLogical)
			assertPayloads(t, fooLogical, []string{
				`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
			})

			// Check that the cursor is properly hooked up to the job statement
			// time. The sinkless tests currently don't have a way to get the
			// statement timestamp, so only verify this for enterprise.
			if e, ok := fooLogical.(cdctest.EnterpriseTestFeed); ok {
				var bytes []byte
				sqlDB.QueryRow(t, jobutils.JobPayloadByIDQuery, e.JobID()).Scan(&bytes)
				var payload jobspb.Payload
				require.NoError(t, protoutil.Unmarshal(bytes, &payload))
				require.Equal(t, tsLogical, payload.GetChangefeed().StatementTime)
			}
		})

		t.Run("nanos cursor", func(t *testing.T) {
			nanosStr := strconv.FormatInt(tsClock.UnixNano(), 10)
			fooNanosStr := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, nanosStr)
			defer closeFeed(t, fooNanosStr)
			assertPayloads(t, fooNanosStr, []string{
				`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
			})
		})

		t.Run("datetime cursor", func(t *testing.T) {
			timeStr := tsClock.Format(`2006-01-02 15:04:05.999999`)
			fooString := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, timeStr)
			defer closeFeed(t, fooString)
			assertPayloads(t, fooString, []string{
				`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
			})
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated, resolved`, optOutOfMetamorphicEnrichedEnvelope{reason: "this test calls testfeed.Next() directly"})
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
		if jf, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			d, err := jf.Details()
			assert.NoError(t, err)
			expected := `{"after": {"a": 0}, "updated": "` + d.StatementTime.AsOfSystemTime() + `"}`
			assert.Equal(t, expected, string(row0.Value))
		}

		// Assert the remaining key using assertPayloads, since we know the exact
		// timestamp expected.
		var ts1 string
		if err := crdb.ExecuteTx(ctx, s.DB, nil /* txopts */, func(tx *gosql.Tx) error {
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
			if resolved, _ := expectResolvedTimestamp(t, foo); parsed.Less(resolved) {
				break
			}
		}
	}

	cdcTest(t, testFn)
}

func TestChangefeedMVCCTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE mvcc_timestamp_test_table (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`)

		rowCount := 5
		expectedPayloads := make([]string, rowCount)
		for i := 0; i < rowCount; i++ {
			row := sqlDB.QueryRow(t, `INSERT INTO mvcc_timestamp_test_table VALUES (DEFAULT) RETURNING id, cluster_logical_timestamp()`)

			var id string
			var mvccTimestamp string
			row.Scan(&id, &mvccTimestamp)
			expectedPayloads[i] = fmt.Sprintf(`mvcc_timestamp_test_table: ["%[1]s"]->{"after": {"id": "%[1]s"}, "mvcc_timestamp": "%[2]s"}`,
				id, mvccTimestamp)
		}

		changeFeed := feed(t, f, `CREATE CHANGEFEED FOR mvcc_timestamp_test_table WITH mvcc_timestamp`)
		defer closeFeed(t, changeFeed)
		assertPayloads(t, changeFeed, expectedPayloads)
	}

	cdcTest(t, testFn)
}

func TestChangefeedMVCCTimestampsAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE mvcc_timestamp_test_table (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`)

		const rowCount = 5
		expectedPayloads := make([]string, rowCount)
		for i := 0; i < rowCount; i++ {
			row := sqlDB.QueryRow(t, `INSERT INTO mvcc_timestamp_test_table VALUES (DEFAULT) RETURNING id, cluster_logical_timestamp()`)

			var id string
			var mvccTimestamp string
			row.Scan(&id, &mvccTimestamp)
			expectedPayloads[i] = fmt.Sprintf(`mvcc_timestamp_test_table: {"id":{"string":"%[1]s"}}->{"after":{"mvcc_timestamp_test_table":{"id":{"string":"%[1]s"}}},"mvcc_timestamp":{"string":"%[2]s"}}`,
				id, mvccTimestamp)
		}

		changeFeed := feed(t, f, `CREATE CHANGEFEED FOR mvcc_timestamp_test_table WITH mvcc_timestamp, format='avro'`)
		defer closeFeed(t, changeFeed)
		assertPayloads(t, changeFeed, expectedPayloads)
	}

	cdcTest(t, testFn, feedTestForceSink(`kafka`))
}

func TestChangefeedResolvedFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		const freq = 10 * time.Millisecond
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved=$1`, freq.String())
		defer closeFeed(t, foo)

		// We get each resolved timestamp notification once in each partition.
		// Grab the first `2 * #partitions`, sort because we might get all from
		// one partition first, and compare the first and last.
		resolved := make([]hlc.Timestamp, 2*len(foo.Partitions()))
		for i := range resolved {
			resolved[i], _ = expectResolvedTimestamp(t, foo)
		}
		sort.Slice(resolved, func(i, j int) bool { return resolved[i].Less(resolved[j]) })
		first, last := resolved[0], resolved[len(resolved)-1]

		if d := last.GoTime().Sub(first.GoTime()); d < freq {
			t.Errorf(`expected %s between resolved timestamps, but got %s`, freq, d)
		}
	}

	cdcTest(t, testFn)
}

func TestChangefeedRandomExpressions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t)
	skip.UnderRace(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		rng, _ := randutil.NewTestRand()
		tblName := "seed"
		defer s.DB.Close()

		setup := sqlsmith.Setups[tblName](rng)
		sqlDB.ExecMultiple(t, setup...)

		// TODO: PopulateTableWithRandData doesn't work with enums
		dropEnumQry := "ALTER TABLE seed DROP COLUMN _enum;"
		sqlDB.Exec(t, dropEnumQry)

		// Attempt to insert a few more than our target 100 values, since there's a
		// small chance we may not succeed that many inserts.
		numInserts := 110
		inserts := make([]string, 0, numInserts)
		for rows := 0; rows < 100; {
			var err error
			var newRows int
			if newRows, err = randgen.PopulateTableWithRandData(rng, s.DB, tblName, numInserts, &inserts); err != nil {
				t.Fatal(err)
			}
			rows += newRows
		}

		limitQry := "DELETE FROM seed WHERE rowid NOT IN (SELECT rowid FROM seed ORDER BY rowid LIMIT 100);"
		sqlDB.Exec(t, limitQry)

		// Put the enums back. enum_range('hi'::greeting)[rowid%7] will give nulls when rowid%7=0 or 6.
		addEnumQry := "ALTER TABLE seed ADD COLUMN _enum greeting;"
		sqlDB.Exec(t, addEnumQry)
		populateEnumQry := "UPDATE seed SET _enum = enum_range('hi'::greeting)[rowid%7];"
		sqlDB.Exec(t, populateEnumQry)
		// Get values to log setup.
		t.Logf("setup:\n%s\n%s\n%s\n%s\n%s\n%s",
			strings.Join(setup, "\n"),
			dropEnumQry,
			strings.Join(inserts, "\n"),
			limitQry,
			addEnumQry,
			populateEnumQry)

		queryGen, err := sqlsmith.NewSmither(s.DB, rng,
			sqlsmith.DisableWith(),
			sqlsmith.DisableMutations(),
			sqlsmith.DisableLimits(),
			sqlsmith.DisableAggregateFuncs(),
			sqlsmith.DisableWindowFuncs(),
			sqlsmith.DisableJoins(),
			sqlsmith.DisableUDFs(),
			sqlsmith.DisableIndexHints(),
			sqlsmith.SetScalarComplexity(0.5),
			sqlsmith.SetComplexity(0.5),
		)
		require.NoError(t, err)
		defer queryGen.Close()
		numNonTrivialTestRuns := 0
		n := 150
		whereClausesChecked := make(map[string]struct{}, n)
		for i := 0; i < n; i++ {
			query := queryGen.Generate()
			where, ok := getWhereClause(query)
			if !ok {
				continue
			}
			if _, alreadyChecked := whereClausesChecked[where]; alreadyChecked {
				continue
			}
			whereClausesChecked[where] = struct{}{}
			query = "SELECT array_to_string(IFNULL(array_agg(distinct rowid),'{}'),'|') FROM seed WHERE " + where
			t.Log(query)
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			rows := s.DB.QueryRowContext(timeoutCtx, query)
			var expectedRowIDsStr string
			if err := rows.Scan(&expectedRowIDsStr); err != nil {
				t.Logf("Skipping query %s because error %s", query, err)
				cancel()
				continue
			}
			cancel()
			expectedRowIDs := strings.Split(expectedRowIDsStr, "|")
			if expectedRowIDsStr == "" {
				t.Logf("Skipping predicate %s because it returned no rows", where)
				continue
			}
			createStmt := `CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT rowid FROM seed WHERE ` + where
			t.Logf("Expecting statement %s to emit %d events", createStmt, len(expectedRowIDs))
			seedFeed, err := f.Feed(createStmt)
			if err != nil {
				t.Logf("Test tolerating create changefeed error: %s", err.Error())
				if seedFeed != nil {
					closeFeedIgnoreError(t, seedFeed)
				}
				continue
			}
			assertedPayloads := make([]string, len(expectedRowIDs))
			for i, id := range expectedRowIDs {
				assertedPayloads[i] = fmt.Sprintf(`seed: [%s]->{"rowid": %s}`, id, id)
			}
			err = assertPayloadsBaseErr(context.Background(), seedFeed, assertedPayloads, false, false, nil, changefeedbase.OptEnvelopeWrapped)
			closeFeedIgnoreError(t, seedFeed)
			if err != nil {
				// Skip errors that may come up during SQL execution. If the SQL query
				// didn't fail with these errors, it's likely because the query was built in
				// a way that did not have to execute on the row that caused the error, but
				// the CDC query did.
				// Since we get the error that caused the changefeed job to
				// fail from scraping the job status and creating a new
				// error, we unfortunately don't have the pgcode and have to
				// rely on known strings.
				validPgErrs := []string{
					"argument is not an object",
					"cannot subtract infinite dates",
					"dwithin distance cannot be less than zero",
					"error parsing EWKB",
					"error parsing EWKT",
					"error parsing GeoJSON",
					"expected LineString",
					"geometry type is unsupported",
					"invalid escape string",
					"invalid regular expression",
					"no locations to init GEOS",
					"parameter has to be of type Point",
					"regexp compilation failed",
					"result out of range",
					"should be of length",
					"unknown DateStyle parameter",
				}
				containsKnownPgErr := func(e error) (interface{}, bool) {
					for _, v := range validPgErrs {
						if strings.Contains(e.Error(), v) {
							return nil, true
						}
					}
					return nil, false
				}
				if _, contains := errors.If(err, containsKnownPgErr); contains {
					t.Logf("Skipping statement %s because it encountered pgerror %s", createStmt, err)
					continue
				}

				t.Fatal(err)
			}
			numNonTrivialTestRuns++
		}
		require.Greater(t, numNonTrivialTestRuns, 0, "Expected >0 predicates to be nontrivial out of %d attempts", n)
		t.Logf("%d predicates checked: all had the same result in SELECT and CHANGEFEED", numNonTrivialTestRuns)

	}

	cdcTest(t, testFn, feedTestForceSink(`kafka`))
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	noInitialScanTests := map[string]string{
		`no cursor - no initial scan`:     `CREATE CHANGEFEED FOR no_initial_scan WITH no_initial_scan, resolved='1s'`,
		`no cursor - no initial backfill`: `CREATE CHANGEFEED FOR no_initial_scan WITH initial_scan = 'no', resolved='1s'`,
	}

	initialScanTests := map[string]string{
		`cursor - with initial scan`:     `CREATE CHANGEFEED FOR initial_scan WITH initial_scan, resolved='1s', cursor='%s'`,
		`cursor - with initial backfill`: `CREATE CHANGEFEED FOR initial_scan WITH initial_scan = 'yes', resolved='1s', cursor='%s'`,
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		for testName, changefeedStmt := range noInitialScanTests {
			t.Run(testName, func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE no_initial_scan (a INT PRIMARY KEY)`)
				defer sqlDB.Exec(t, `DROP TABLE no_initial_scan`)
				sqlDB.Exec(t, `INSERT INTO no_initial_scan VALUES (1)`)

				noInitialScan := feed(t, f, changefeedStmt)
				defer closeFeed(t, noInitialScan)

				expectResolvedTimestamp(t, noInitialScan)

				sqlDB.Exec(t, `INSERT INTO no_initial_scan VALUES (2)`)
				assertPayloads(t, noInitialScan, []string{
					`no_initial_scan: [2]->{"after": {"a": 2}}`,
				})
			})
		}

		for testName, changefeedStmtFormat := range initialScanTests {
			t.Run(testName, func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE initial_scan (a INT PRIMARY KEY)`)
				defer sqlDB.Exec(t, `DROP TABLE initial_scan`)
				sqlDB.Exec(t, `INSERT INTO initial_scan VALUES (1), (2), (3)`)
				var tsStr string
				var i int
				sqlDB.QueryRow(t, `SELECT count(*), cluster_logical_timestamp() from initial_scan`).Scan(&i, &tsStr)
				initialScan := feed(t, f, fmt.Sprintf(changefeedStmtFormat, tsStr))
				defer closeFeed(t, initialScan)

				assertPayloads(t, initialScan, []string{
					`initial_scan: [1]->{"after": {"a": 1}}`,
					`initial_scan: [2]->{"after": {"a": 2}}`,
					`initial_scan: [3]->{"after": {"a": 3}}`,
				})
				sqlDB.Exec(t, `INSERT INTO initial_scan VALUES (4)`)
				assertPayloads(t, initialScan, []string{
					`initial_scan: [4]->{"after": {"a": 4}}`,
				})
			})
		}
	}

	cdcTest(t, testFn)
}

func TestChangefeedProjectionDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (id int primary key, a string)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'a')`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH envelope='wrapped' AS SELECT * FROM foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`foo: [0]->{"after": {"a": "a", "id": 0}}`})
		sqlDB.Exec(t, `DELETE FROM foo WHERE id = 0`)
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": null}`,
		})
	}
	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

func TestChangefeedSingleColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d STRING, FAMILY most (a,b), FAMILY rest (c, d))`)
		sqlDB.Exec(t, `INSERT INTO foo(a,b,c) values (0, 'dog', 'cat')`)
		sqlDB.Exec(t, `INSERT INTO foo(a,b,c) values (1, 'dollar', 'cent')`)

		sqlDB.ExpectErrWithTimeout(t, `nosuchfamily`, `CREATE CHANGEFEED FOR foo FAMILY nosuchfamily`)

		// TODO(#145927): unskip this when we have family or topic info in enriched feeds.
		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}

		fooMost := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY most`, args...)
		defer closeFeed(t, fooMost)
		assertPayloads(t, fooMost, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
			`foo.most: [1]->{"after": {"a": 1, "b": "dollar"}}`,
		})

		fooRest := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest`, args...)
		defer closeFeed(t, fooRest)
		assertPayloads(t, fooRest, []string{
			`foo.rest: [0]->{"after": {"c": "cat", "d": null}}`,
			`foo.rest: [1]->{"after": {"c": "cent", "d": null}}`,
		})

		fooBoth := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest, foo FAMILY most`, args...)
		defer closeFeed(t, fooBoth)
		assertPayloads(t, fooBoth, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
			`foo.rest: [0]->{"after": {"c": "cat", "d": null}}`,
			`foo.most: [1]->{"after": {"a": 1, "b": "dollar"}}`,
			`foo.rest: [1]->{"after": {"c": "cent", "d": null}}`,
		})

		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 0`)
		assertPayloads(t, fooBoth, []string{
			`foo.most: [0]->{"after": null}`,
			`foo.rest: [0]->{"after": null}`,
		})

	}
	cdcTest(t, testFn)
}

func TestChangefeedCustomKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'dog', 'cat')`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH key_column='b', unordered`,
			optOutOfMetamorphicEnrichedEnvelope{reason: "custom key not supported in test framework"})
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: ["dog"]->{"after": {"a": 0, "b": "dog", "c": "cat"}}`,
		})
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'dog', 'zebra')`)
		assertPayloads(t, foo, []string{
			`foo: ["dog"]->{"after": {"a": 1, "b": "dog", "c": "zebra"}}`,
		})
		sqlDB.Exec(t, `ALTER TABLE foo RENAME COLUMN b to b2`)
		requireTerminalErrorSoon(context.Background(), t, foo, regexp.MustCompile(`required column b not present`))
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"), withAllowChangefeedErr("expects error"))
}
func TestChangefeedCustomKeyAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH format='avro', key_column='b', unordered`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"b":{"string":"dog"}}->{"after":{"foo":{"a":{"long":0},"b":{"string":"dog"},"c":{"string":"cat"}}}}`,
		})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedColumnFamilyAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY most (a,b), FAMILY justc (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families, format=avro`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo.most: {"a":{"long":0}}->{"after":{"foo_u002e_most":{"a":{"long":0},"b":{"string":"dog"}}}}`,
			`foo.justc: {"a":{"long":0}}->{"after":{"foo_u002e_justc":{"c":{"string":"cat"}}}}`,
		})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedBareAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH format=avro, schema_change_policy=stop AS SELECT * FROM foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":0}}->{"record":{"foo":{"a":{"long":0},"b":{"string":"dog"}}}}`,
		})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedEnriched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	withField := func(fieldName string, schema map[string]any) map[string]any {
		s := maps.Clone(schema)
		s["field"] = fieldName
		return s
	}

	key := map[string]any{"a": 0}
	keySchema := map[string]any{
		"name": "foo.key",
		"fields": []map[string]any{
			{"field": "a", "optional": false, "type": "int64"},
		},
		"optional": false,
		"type":     "struct",
	}

	afterVal := map[string]any{"a": 0, "b": "dog"}
	afterSchema := map[string]any{
		"name":  "foo.after.value",
		"field": "after",
		"fields": []map[string]any{
			{"field": "a", "optional": false, "type": "int64"},
			{"field": "b", "optional": true, "type": "string"},
		},
		"optional": false,
		"type":     "struct",
	}

	payload := map[string]any{
		"after": afterVal,
		"op":    "c",
		// TODO(#139662): add `before`
		// NOTE: ts_ns is stripped by the test framework, and source is specified separately
	}

	// Create an enriched source provider with no data. The contents of source
	// will be tested in another test, we just want to make sure the structure &
	// schema is right here.
	esp, err := newEnrichedSourceProvider(changefeedbase.EncodingOptions{}, getTestingEnrichedSourceData())
	require.NoError(t, err)
	source, err := esp.GetJSON(cdcevent.TestingMakeEventRowFromEncDatums([]rowenc.EncDatum{}, nil, 0, false), eventContext{})
	require.NoError(t, err)

	var sourceMap map[string]any
	require.NoError(t, gojson.Unmarshal([]byte(source.String()), &sourceMap))

	sourceSchema, err := esp.KafkaConnectJSONSchema().AsJSON()
	require.NoError(t, err)
	var sourceSchemaMap map[string]any
	require.NoError(t, gojson.Unmarshal([]byte(sourceSchema.String()), &sourceSchemaMap))
	sourceSchemaMap["field"] = "source"

	tsNsSchema := map[string]any{"field": "ts_ns", "optional": false, "type": "int64"}
	opSchema := map[string]any{"field": "op", "optional": false, "type": "string"}

	cases := []struct {
		name                 string
		enrichedProperties   []string
		messageWithoutSource map[string]any
		withSource           bool
		expectedKey          map[string]any
		keyInValue           bool
	}{
		{
			name:                 "with nothing",
			messageWithoutSource: payload,
			expectedKey:          key,
		},
		{
			name:               "with schema",
			enrichedProperties: []string{"schema"},
			messageWithoutSource: map[string]any{
				"payload": payload,
				"schema": map[string]any{
					"name":     "cockroachdb.envelope",
					"optional": false,
					"fields": []map[string]any{
						afterSchema,
						tsNsSchema,
						opSchema,
					},
					"type": "struct",
				},
			},
			expectedKey: map[string]any{
				"payload": key,
				"schema":  keySchema,
			},
		},
		{
			name:                 "with source",
			enrichedProperties:   []string{"source"},
			messageWithoutSource: payload,
			withSource:           true,
			expectedKey:          key,
		},
		{
			name:               "with schema and source",
			enrichedProperties: []string{"schema", "source"},
			messageWithoutSource: map[string]any{
				"payload": payload,
				"schema": map[string]any{
					"name":     "cockroachdb.envelope",
					"optional": false,
					"fields": []map[string]any{
						afterSchema,
						sourceSchemaMap,
						tsNsSchema,
						opSchema,
					},
					"type": "struct",
				},
			},
			withSource: true,
			expectedKey: map[string]any{
				"payload": key,
				"schema":  keySchema,
			},
		},
		{
			name:       "with key_in_value",
			keyInValue: true,
			messageWithoutSource: map[string]any{
				"after": afterVal,
				"op":    "c",
				"key":   key,
			},
			expectedKey: key,
		},
		{
			name:               "with source and key_in_value",
			enrichedProperties: []string{"source"},
			keyInValue:         true,
			messageWithoutSource: map[string]any{
				"after": afterVal,
				"op":    "c",
				"key":   key,
			},
			withSource:  true,
			expectedKey: key,
		},
		{
			name:               "with schema and key_in_value",
			enrichedProperties: []string{"schema"},
			keyInValue:         true,
			messageWithoutSource: map[string]any{
				"payload": map[string]any{
					"after": afterVal,
					"op":    "c",
					// NOTE: this key does not have its schema in it here, because that would be redundant/strange.
					"key": key,
				},
				"schema": map[string]any{
					"name":     "cockroachdb.envelope",
					"optional": false,
					"fields": []map[string]any{
						afterSchema,
						withField("key", keySchema),
						tsNsSchema,
						opSchema,
					},
					"type": "struct",
				},
			},
			withSource: false,
			expectedKey: map[string]any{
				"payload": key,
				"schema":  keySchema,
			},
		},
		{
			name:               "with schema, source, and key_in_value",
			enrichedProperties: []string{"schema", "source"},
			keyInValue:         true,
			messageWithoutSource: map[string]any{
				"payload": map[string]any{
					"after": afterVal,
					"op":    "c",
					"key":   key,
				},
				"schema": map[string]any{
					"name":     "cockroachdb.envelope",
					"optional": false,
					"fields": []map[string]any{
						afterSchema,
						sourceSchemaMap,
						withField("key", keySchema),
						tsNsSchema,
						opSchema,
					},
					"type": "struct",
				},
			},
			withSource: true,
			expectedKey: map[string]any{
				"payload": key,
				"schema":  keySchema,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

				// The webhook testfeed removes the key from the value, so skip keyInValue and schema'd tests for it.
				_, isWebhook := f.(*webhookFeedFactory)
				if isWebhook && (tc.keyInValue || slices.Contains(tc.enrichedProperties, "schema")) {
					return
				}

				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)

				create := fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='%s'`,
					strings.Join(tc.enrichedProperties, ","))
				if tc.keyInValue {
					create += ", key_in_value"
				}
				foo := feed(t, f, create)
				defer closeFeed(t, foo)

				// The webhook testfeed relies on source.table_name for assertion matching. See: #145927
				topic := "foo"
				if isWebhook && !slices.Contains(tc.enrichedProperties, "source") {
					topic = ""
				}

				assertion := fmt.Sprintf("%s: %s->%s", topic, toJSON(t, tc.expectedKey), toJSON(t, tc.messageWithoutSource))
				sourceAssertion := func(actualSource map[string]any) {
					if tc.withSource {
						// Just check the source's structure.
						require.ElementsMatch(t, slices.Collect(maps.Keys(sourceMap)), slices.Collect(maps.Keys(actualSource)))
					} else {
						require.Empty(t, actualSource)
					}
				}
				assertPayloadsEnriched(t, foo, []string{assertion}, sourceAssertion)
			}
			supportedSinks := []string{"kafka", "pubsub", "sinkless", "webhook"}
			for _, sink := range supportedSinks {
				cdcTest(t, testFn, feedTestForceSink(sink))
			}
		})
	}
}

func TestChangefeedEnrichedAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name               string
		enrichedProperties []string
		withSource         bool
	}{
		{name: "none", enrichedProperties: []string{""}},
		{name: "with source", enrichedProperties: []string{"source"}, withSource: true},
		// no change in output from the first two -- the schema is part of the avro format
		{name: "with schema", enrichedProperties: []string{"schema"}},
		{name: "with schema and source", enrichedProperties: []string{"schema", "source"}, withSource: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)
				enrichedPropsStr := ""
				if len(tc.enrichedProperties) > 0 {
					enrichedPropsStr = fmt.Sprintf(", enriched_properties='%s'", strings.Join(tc.enrichedProperties, ","))
				}
				foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH envelope=enriched, format=avro, confluent_schema_registry='localhost:90909' %s`, enrichedPropsStr))
				defer closeFeed(t, foo)

				assertionKey := `{"a":{"long":0}}`
				assertionAfter := `"after": {"foo": {"a": {"long": 0}, "b": {"string": "dog"}}}`

				sourceAssertion := func(actualSource map[string]any) {
					if tc.withSource {
						require.NotNil(t, actualSource)
					} else {
						require.Nil(t, actualSource)
					}
				}
				assertPayloadsEnriched(t, foo, []string{
					fmt.Sprintf(`foo: %s->{%s, "op": {"string": "c"}}`,
						assertionKey, assertionAfter),
				}, sourceAssertion)
			}
			cdcTest(t, testFn, feedTestForceSink("kafka"))
		})
	}
}

func TestChangefeedEnrichedWithDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name      string
		options   []string
		sinks     []string
		assertion func(topic string) []string
	}{
		{
			name: "json with diff", options: []string{"diff", "format=json"}, sinks: []string{"kafka", "pubsub", "sinkless", "webhook", "cloudstorage"},
			assertion: func(topic string) []string {
				return []string{
					fmt.Sprintf(`%s: {"a": 0}->{"after": {"a": 0, "b": "dog"}, "before": null, "op": "c"}`, topic),
					fmt.Sprintf(`%s: {"a": 0}->{"after": {"a": 0, "b": "cat"}, "before": {"a": 0, "b": "dog"}, "op": "u"}`, topic),
					fmt.Sprintf(`%s: {"a": 0}->{"after": null, "before": {"a": 0, "b": "cat"}, "op": "d"}`, topic),
				}
			},
		},
		{
			name: "json without diff", options: []string{"format=json"}, sinks: []string{"kafka", "pubsub", "sinkless", "webhook"},
			assertion: func(topic string) []string {
				return []string{
					fmt.Sprintf(`%s: {"a": 0}->{"after": {"a": 0, "b": "dog"}, "op": "c"}`, topic),
					fmt.Sprintf(`%s: {"a": 0}->{"after": {"a": 0, "b": "cat"}, "op": "u"}`, topic),
					fmt.Sprintf(`%s: {"a": 0}->{"after": null, "op": "d"}`, topic),
				}
			},
		},
		{
			name: "avro with diff", options: []string{"diff", "format=avro"}, sinks: []string{"kafka"},
			assertion: func(topic string) []string {
				return []string{
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": {"foo": {"a": {"long": 0}, "b": {"string": "dog"}}}, "before": null, "op": {"string": "c"}}`, topic),
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": {"foo": {"a": {"long": 0}, "b": {"string": "cat"}}}, "before": {"foo_before": {"a": {"long": 0}, "b": {"string": "dog"}}}, "op": {"string": "u"}}`, topic),
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": null, "before": {"foo_before": {"a": {"long": 0}, "b": {"string": "cat"}}}, "op": {"string": "d"}}`, topic),
				}
			},
		},
		{
			name: "avro without diff", options: []string{"format=avro"}, sinks: []string{"kafka"},
			assertion: func(topic string) []string {
				return []string{
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": {"foo": {"a": {"long": 0}, "b": {"string": "dog"}}}, "op": {"string": "c"}}`, topic),
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": {"foo": {"a": {"long": 0}, "b": {"string": "cat"}}}, "op": {"string": "u"}}`, topic),
					fmt.Sprintf(`%s: {"a":{"long":0}}->{"after": null, "op": {"string": "d"}}`, topic),
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)

				foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH envelope=enriched, %s`, strings.Join(tc.options, ", ")))
				defer closeFeed(t, foo)

				sqlDB.Exec(t, `UPDATE foo SET b = 'cat'`)
				sqlDB.Exec(t, `DELETE FROM foo WHERE b = 'cat'`)

				// TODO(#139660): the webhook sink forces topic_in_value, but
				// this is not supported by the enriched envelope type. We should adapt
				// the test framework to account for this.
				topic := "foo"
				if _, ok := foo.(*webhookFeed); ok {
					topic = ""
				}

				assertPayloadsEnriched(t, foo, tc.assertion(topic), nil)
			}
			for _, sink := range tc.sinks {
				cdcTest(t, testFn, feedTestForceSink(sink))
			}
		})
	}
}

func TestChangefeedEnrichedSourceWithDataAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "ts_{ns,hlc}", func(t *testing.T, withUpdated bool) {
		testutils.RunTrueAndFalse(t, "mvcc_ts", func(t *testing.T, withMVCCTS bool) {
			clusterName := "clusterName123"
			dbVersion := "v999.0.0"
			defer build.TestingOverrideVersion(dbVersion)()
			mkTestFn := func(sink string) func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
					clusterID := s.Server.ExecutorConfig().(sql.ExecutorConfig).NodeInfo.LogicalClusterID().String()

					sqlDB := sqlutils.MakeSQLRunner(s.DB)

					sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
					sqlDB.Exec(t, `INSERT INTO foo values (0)`)

					var tableID int
					sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'foo' AND database_name = 'd'`).Scan(&tableID)

					stmt := `CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='source', format=avro`
					if withMVCCTS {
						stmt += ", mvcc_timestamp"
					}
					if withUpdated {
						stmt += ", updated"
					}
					testFeed := feed(t, f, stmt)
					defer closeFeed(t, testFeed)

					var jobID int64
					var nodeName string
					var sourceAssertion func(actualSource map[string]any)
					if ef, ok := testFeed.(cdctest.EnterpriseTestFeed); ok {
						jobID = int64(ef.JobID())
					}
					sqlDB.QueryRow(t, `SELECT value FROM crdb_internal.node_runtime_info where component = 'DB' and field = 'Host'`).Scan(&nodeName)

					sourceAssertion = func(actualSource map[string]any) {
						var nodeID any
						actualSourceValue := actualSource["source"].(map[string]any)
						nodeID = actualSourceValue["node_id"].(map[string]any)["string"]

						require.NotNil(t, nodeID)

						sourceNodeLocality := fmt.Sprintf(`region=%s`, testServerRegion)

						const dummyMvccTimestamp = "1234567890.0001"
						jobIDStr := strconv.FormatInt(jobID, 10)

						dummyUpdatedTSNS := 12345678900001000
						dummyUpdatedTSHLC :=
							hlc.Timestamp{WallTime: int64(dummyUpdatedTSNS), Logical: 0}.AsOfSystemTime()

						var assertion string
						assertionMap := map[string]any{
							"source": map[string]any{
								"changefeed_sink":        map[string]any{"string": sink},
								"cluster_id":             map[string]any{"string": clusterID},
								"cluster_name":           map[string]any{"string": clusterName},
								"crdb_internal_table_id": map[string]any{"int": tableID},
								"database_name":          map[string]any{"string": "d"},
								"db_version":             map[string]any{"string": dbVersion},
								"job_id":                 map[string]any{"string": jobIDStr},
								// Note that the field is still present in the avro schema, so it appears here as nil.
								"mvcc_timestamp":       nil,
								"node_id":              map[string]any{"string": nodeID},
								"origin":               map[string]any{"string": "cockroachdb"},
								"node_name":            map[string]any{"string": nodeName},
								"primary_keys":         map[string]any{"array": []any{"i"}},
								"schema_name":          map[string]any{"string": "public"},
								"source_node_locality": map[string]any{"string": sourceNodeLocality},
								"table_name":           map[string]any{"string": "foo"},
								"ts_ns":                nil,
								"ts_hlc":               nil,
							},
						}
						if withMVCCTS {
							mvccTsMap := actualSource["source"].(map[string]any)["mvcc_timestamp"].(map[string]any)
							assertReasonableMVCCTimestamp(t, mvccTsMap["string"].(string))

							mvccTsMap["string"] = dummyMvccTimestamp
							assertionMap["source"].(map[string]any)["mvcc_timestamp"] = map[string]any{"string": dummyMvccTimestamp}
						}
						if withUpdated {
							tsnsMap := actualSource["source"].(map[string]any)["ts_ns"].(map[string]any)
							tsns := tsnsMap["long"].(gojson.Number)
							tsnsInt, err := tsns.Int64()
							require.NoError(t, err)
							tsnsString := tsns.String()
							assertReasonableMVCCTimestamp(t, tsnsString)
							tsnsMap["long"] = dummyUpdatedTSNS
							assertionMap["source"].(map[string]any)["ts_ns"] = map[string]any{"long": dummyUpdatedTSNS}

							tshlcMap := actualSource["source"].(map[string]any)["ts_hlc"].(map[string]any)
							assertEqualTSNSHLCWalltime(t, tsnsInt, tshlcMap["string"].(string))

							tshlcMap["string"] = dummyUpdatedTSHLC
							assertionMap["source"].(map[string]any)["ts_hlc"] = map[string]any{"string": dummyUpdatedTSHLC}
						}
						assertion = toJSON(t, assertionMap)

						value, err := reformatJSON(actualSource)
						require.NoError(t, err)
						require.JSONEq(t, assertion, string(value))
					}

					assertPayloadsEnriched(t, testFeed, []string{`foo: {"i":{"long":0}}->{"after": {"foo": {"i": {"long": 0}}}, "op": {"string": "c"}}`}, sourceAssertion)
				}
			}
			testLocality := roachpb.Locality{
				Tiers: []roachpb.Tier{{
					Key:   "region",
					Value: testServerRegion,
				}}}
			cdcTest(t, mkTestFn("kafka"), feedTestForceSink("kafka"), feedTestUseClusterName(clusterName),
				feedTestUseLocality(testLocality))

		})
	})
}

func TestChangefeedEnrichedSourceWithDataJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "ts_{ns,hlc}", func(t *testing.T, withUpdated bool) {
		testutils.RunTrueAndFalse(t, "mvcc_ts", func(t *testing.T, withMVCCTS bool) {
			clusterName := "clusterName123"
			dbVersion := "v999.0.0"
			defer build.TestingOverrideVersion(dbVersion)()
			mkTestFn := func(sink string) func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
					clusterID := s.Server.ExecutorConfig().(sql.ExecutorConfig).NodeInfo.LogicalClusterID().String()

					sqlDB := sqlutils.MakeSQLRunner(s.DB)

					sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)
					sqlDB.Exec(t, `INSERT INTO foo values (0)`)

					stmt := `CREATE CHANGEFEED FOR foo WITH envelope=enriched, enriched_properties='source', format=json`
					if withMVCCTS {
						stmt += ", mvcc_timestamp"
					}
					if withUpdated {
						stmt += ", updated"
					}
					testFeed := feed(t, f, stmt)
					defer closeFeed(t, testFeed)

					var jobID int64
					var nodeName string
					var tableID int

					var sourceAssertion func(actualSource map[string]any)
					if ef, ok := testFeed.(cdctest.EnterpriseTestFeed); ok {
						jobID = int64(ef.JobID())
					}
					sqlDB.QueryRow(t, `SELECT value FROM crdb_internal.node_runtime_info where component = 'DB' and field = 'Host'`).Scan(&nodeName)
					sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'foo' AND database_name = 'd'`).Scan(&tableID)

					sourceAssertion = func(actualSource map[string]any) {
						nodeID := actualSource["node_id"]
						require.NotNil(t, nodeID)

						sourceNodeLocality := fmt.Sprintf(`region=%s`, testServerRegion)

						// There are some differences between how we specify sinks here and their actual names.
						if sink == "sinkless" {
							sink = sinkTypeSinklessBuffer.String()
						}

						const dummyMvccTimestamp = "1234567890.0001"
						jobIDStr := strconv.FormatInt(jobID, 10)

						dummyUpdatedTSNS := 12345678900001000
						dummyUpdatedTSHLC :=
							hlc.Timestamp{WallTime: int64(dummyUpdatedTSNS), Logical: 0}.AsOfSystemTime()

						var assertion string
						assertionMap := map[string]any{
							"cluster_id":             clusterID,
							"cluster_name":           clusterName,
							"crdb_internal_table_id": tableID,
							"db_version":             dbVersion,
							"job_id":                 jobIDStr,
							"node_id":                nodeID,
							"node_name":              nodeName,
							"origin":                 "cockroachdb",
							"changefeed_sink":        sink,
							"source_node_locality":   sourceNodeLocality,
							"database_name":          "d",
							"schema_name":            "public",
							"table_name":             "foo",
							"primary_keys":           []any{"i"},
						}
						if withMVCCTS {
							assertReasonableMVCCTimestamp(t, actualSource["mvcc_timestamp"].(string))
							actualSource["mvcc_timestamp"] = dummyMvccTimestamp
							assertionMap["mvcc_timestamp"] = dummyMvccTimestamp
						}
						if withUpdated {
							tsns := actualSource["ts_ns"].(gojson.Number)
							tsnsInt, err := tsns.Int64()
							require.NoError(t, err)
							assertReasonableMVCCTimestamp(t, tsns.String())
							actualSource["ts_ns"] = dummyUpdatedTSNS
							assertionMap["ts_ns"] = dummyUpdatedTSNS
							assertEqualTSNSHLCWalltime(t, tsnsInt, actualSource["ts_hlc"].(string))
							actualSource["ts_hlc"] = dummyUpdatedTSHLC
							assertionMap["ts_hlc"] = dummyUpdatedTSHLC
						}
						assertion = toJSON(t, assertionMap)

						value, err := reformatJSON(actualSource)
						require.NoError(t, err)
						require.JSONEq(t, assertion, string(value))
					}

					assertPayloadsEnriched(t, testFeed, []string{`foo: {"i": 0}->{"after": {"i": 0}, "op": "c"}`}, sourceAssertion)
				}
			}
			for _, sink := range []string{"kafka", "pubsub", "sinkless", "cloudstorage", "webhook"} {
				testLocality := roachpb.Locality{
					Tiers: []roachpb.Tier{{
						Key:   "region",
						Value: testServerRegion,
					}}}
				cdcTest(t, mkTestFn(sink), feedTestForceSink(sink), feedTestUseClusterName(clusterName),
					feedTestUseLocality(testLocality))
			}
		})
	})
}

func TestChangefeedBareJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH schema_change_policy=stop AS SELECT * FROM foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`foo: [0]->{"a": 0, "b": "dog"}`})
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
	cdcTest(t, testFn, feedTestForceSink("enterprise"))
	cdcTest(t, testFn, feedTestForceSink("pubsub"))
	cdcTest(t, testFn, feedTestForceSink("sinkless"))
	cdcTest(t, testFn, feedTestForceSink("webhook"))
	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

func TestChangefeedExternalConnectionSchemaRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog')`)

		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()

		name := fmt.Sprintf("schemareg%d", rand.Uint64())

		sqlDB.Exec(t, fmt.Sprintf(`CREATE EXTERNAL CONNECTION "%s" AS '%s'`, name, schemaReg.URL()))

		sql := fmt.Sprintf("CREATE CHANGEFEED WITH format=avro, confluent_schema_registry='external://%s' AS SELECT * FROM foo", name)

		foo := feed(t, f, sql)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{`foo: {"a":{"long":0}}->{"record":{"foo":{"a":{"long":0},"b":{"string":"dog"}}}}`})
	}
	// Test helpers for avro assume Kafka
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedOutputTopics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster, _, cleanup := startTestCluster(t)
	defer cleanup()
	s := cluster.Server(1)

	pgURL, cleanup := pgurlutils.PGUrl(t, s.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	pgBase, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	var actual string
	connector := pq.ConnectorWithNoticeHandler(pgBase, func(n *pq.Error) {
		actual = n.Message
	})

	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()

	sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

	sqlDB.Exec(t, `CREATE TABLE ☃ (i INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO ☃ VALUES (0)`)

	t.Run("kafka", func(t *testing.T) {
		actual = "(no notice)"
		f := makeKafkaFeedFactory(t, s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	})

	t.Run("pubsub v2", func(t *testing.T) {
		actual = "(no notice)"
		f := makePubsubFeedFactory(s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'gcpubsub://does.not.matter/'`)
		defer closeFeed(t, testFeed)
		// Pubsub doesn't sanitize the topic name.
		require.Equal(t, `changefeed will emit to topic ☃`, actual)
	})

	t.Run("webhooks does not emit anything", func(t *testing.T) {
		actual = "(no notice)"
		f := makePubsubFeedFactory(s, dbWithHandler)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR ☃ INTO 'webhook-https://does.not.matter/'`)
		defer closeFeed(t, testFeed)
		require.Equal(t, `(no notice)`, actual)
	})
}

func TestChangefeedFailOnTableOffline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if _, err := w.Write([]byte("42,42\n")); err != nil {
				t.Logf("failed to write: %s", err.Error())
			}
		}
	}))
	defer dataSrv.Close()

	cdcTestNamedWithSystem(t, "import fails changefeed", func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, `CREATE TABLE for_import (a INT PRIMARY KEY, b INT) WITH (schema_locked=false)`)
		defer sqlDB.Exec(t, `DROP TABLE for_import`)
		sqlDB.Exec(t, `INSERT INTO for_import VALUES (0, NULL)`)
		forImport := feed(t, f, `CREATE CHANGEFEED FOR for_import `)
		defer closeFeed(t, forImport)
		assertPayloads(t, forImport, []string{
			`for_import: [0]->{"after": {"a": 0, "b": null}}`,
		})
		sqlDB.Exec(t, `IMPORT INTO for_import CSV DATA ($1)`, dataSrv.URL)
		requireTerminalErrorSoon(context.Background(), t, forImport,
			regexp.MustCompile(`CHANGEFEED cannot target offline table: for_import \(offline reason: "importing"\)`))
	}, withAllowChangefeedErr("expects terminal error"))

	cdcTestNamedWithSystem(t, "reverted import fails changefeed with earlier cursor", func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sysSQLDB := sqlutils.MakeSQLRunner(s.SystemDB)
		sysSQLDB.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1'")

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE for_import (a INT PRIMARY KEY, b INT)`)

		var start string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&start)
		sqlDB.Exec(t, "INSERT INTO for_import VALUES (0, 10);")

		// Start an import job which will immediately pause after ingestion
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest';")
		go func() {
			sqlDB.ExpectErrWithRetry(t, `pause point`, `IMPORT INTO for_import CSV DATA ($1);`, `result is ambiguous`, dataSrv.URL)
		}()
		sqlDB.CheckQueryResultsRetry(
			t,
			fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS] WHERE job_type='IMPORT' AND status='%s'`, jobs.StatePaused),
			[][]string{{"1"}},
		)

		// Cancel to trigger a revert and verify revert completion
		var jobID string
		sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] where job_type='IMPORT'`).Scan(&jobID)
		sqlDB.Exec(t, `CANCEL JOB $1`, jobID)
		sqlDB.CheckQueryResultsRetry(
			t,
			fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS] WHERE job_type='IMPORT' AND status='%s'`, jobs.StateCanceled),
			[][]string{{"1"}},
		)
		sqlDB.CheckQueryResultsRetry(t, "SELECT count(*) FROM for_import", [][]string{{"1"}})

		// Changefeed should fail regardless
		forImport := feed(t, f, `CREATE CHANGEFEED FOR for_import WITH cursor=$1`, start)
		defer closeFeed(t, forImport)
		requireTerminalErrorSoon(context.Background(), t, forImport,
			regexp.MustCompile(`CHANGEFEED cannot target offline table: for_import \(offline reason: "importing"\)`))
	}, withAllowChangefeedErr("expects terminal error"))
}

func TestCDCPrev(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)
		// TODO(#85143): remove schema_change_policy='stop' from this test.
		foo := feed(t, f, `CREATE CHANGEFEED WITH envelope='row', schema_change_policy='stop' AS SELECT (cdc_prev).b AS old FROM foo`)
		defer closeFeed(t, foo)

		// cdc_prev values are null during initial scan
		assertPayloads(t, foo, []string{
			`foo: [0]->{"old": null}`,
		})

		// cdc_prev values are null for an insert event
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'original')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"old": null}`,
		})

		// cdc_prev returns the previous value on an update
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'updated')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"old": "original"}`,
		})
	}

	// envelope=wrapped is required for some sinks, but
	// envelope=wrapped output with cdc_prev looks silly.
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestManyChangefeedsOneTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'init')`)

		foo1 := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo1)
		foo2 := feed(t, f, `CREATE CHANGEFEED FOR foo`) // without diff
		defer closeFeed(t, foo2)
		foo3 := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo3)

		// Make sure all the changefeeds are going.
		assertPayloads(t, foo1, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}, "before": null}`})
		assertPayloads(t, foo2, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}}`})
		assertPayloads(t, foo3, []string{`foo: [0]->{"after": {"a": 0, "b": "init"}, "before": null}`})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v0')`)
		assertPayloads(t, foo1, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}, "before": {"a": 0, "b": "init"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'v1')`)
		assertPayloads(t, foo1, []string{
			`foo: [1]->{"after": {"a": 1, "b": "v1"}, "before": null}`,
		})
		assertPayloads(t, foo2, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "v1"}}`,
		})

		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'v2')`)
		assertPayloads(t, foo1, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v2"}, "before": {"a": 0, "b": "v0"}}`,
		})
		assertPayloads(t, foo2, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v2"}}`,
		})
		assertPayloads(t, foo3, []string{
			`foo: [0]->{"after": {"a": 0, "b": "v0"}, "before": {"a": 0, "b": "init"}}`,
			`foo: [0]->{"after": {"a": 0, "b": "v2"}, "before": {"a": 0, "b": "v0"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "v1"}, "before": null}`,
		})
	}

	cdcTest(t, testFn)
}

func TestUnspecifiedPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn)
}

func TestChangefeedOnErrorOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		t.Run(`pause on error`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

			knobs := s.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = func(_ context.Context) error {
				return changefeedbase.WithTerminalError(errors.New("should fail with custom error"))
			}

			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH on_error='pause'`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

			feedJob := foo.(cdctest.EnterpriseTestFeed)

			// check for paused status on failure
			require.NoError(t, feedJob.WaitForState(func(s jobs.State) bool { return s == jobs.StatePaused }))

			// Verify job progress contains paused on error status.
			jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
			registry := s.Server.JobRegistry().(*jobs.Registry)
			job, err := registry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			require.Contains(t, job.Progress().StatusMessage, "job failed (should fail with custom error) but is being paused because of on_error=pause")
			knobs.BeforeEmitRow = nil

			require.NoError(t, feedJob.Resume())
			// changefeed should continue to work after it has been resumed
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			})

			closeFeed(t, foo)
			// cancellation should still go through if option is in place
			// to avoid race condition, check only that the job is progressing to be
			// canceled (we don't know what stage it will be in)
			require.NoError(t, feedJob.WaitForState(func(s jobs.State) bool {
				return s == jobs.StateCancelRequested ||
					s == jobs.StateReverting ||
					s == jobs.StateCanceled
			}))
		})

		t.Run(`fail on error`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

			knobs := s.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = func(_ context.Context) error {
				return changefeedbase.WithTerminalError(errors.New("should fail with custom error"))
			}

			foo := feed(t, f, `CREATE CHANGEFEED FOR bar WITH on_error = 'fail'`)
			sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'a')`)
			defer closeFeed(t, foo)

			feedJob := foo.(cdctest.EnterpriseTestFeed)

			require.NoError(t, feedJob.WaitForState(func(s jobs.State) bool { return s == jobs.StateFailed }))
			require.EqualError(t, feedJob.FetchTerminalJobErr(), "should fail with custom error")
		})

		t.Run(`default`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE quux (a INT PRIMARY KEY, b STRING)`)

			knobs := s.TestingKnobs.
				DistSQL.(*execinfra.TestingKnobs).
				Changefeed.(*TestingKnobs)
			knobs.BeforeEmitRow = func(_ context.Context) error {
				return changefeedbase.WithTerminalError(errors.New("should fail with custom error"))
			}

			foo := feed(t, f, `CREATE CHANGEFEED FOR quux`)
			sqlDB.Exec(t, `INSERT INTO quux VALUES (1, 'a')`)
			defer closeFeed(t, foo)

			feedJob := foo.(cdctest.EnterpriseTestFeed)

			// if no option is provided, fail should be the default behavior
			require.NoError(t, feedJob.WaitForState(func(s jobs.State) bool { return s == jobs.StateFailed }))
			require.EqualError(t, feedJob.FetchTerminalJobErr(), "should fail with custom error")
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, withAllowChangefeedErr("expects error"))
}

func TestChangefeedCaseInsensitiveOpts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Sanity check for case insensitive options
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Set up a type and table.
		sqlDB.Exec(t, `CREATE TABLE insensitive (x INT PRIMARY KEY, y string)`)
		sqlDB.Exec(t, `INSERT INTO insensitive VALUES (0, 'hello')`)

		t.Run(`format=JSON`, func(t *testing.T) {
			cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE insensitive WITH format=JSON`)
			defer closeFeed(t, cf)
			assertPayloads(t, cf, []string{`insensitive: [0]->{"after": {"x": 0, "y": "hello"}}`})
		})

		t.Run(`envelope=ROW`, func(t *testing.T) {
			cf := feed(t, f, `CREATE CHANGEFEED FOR insensitive WITH envelope='ROW'`)
			defer closeFeed(t, cf)
			assertPayloads(t, cf, []string{`insensitive: [0]->{"x": 0, "y": "hello"}`})
		})

		t.Run(`schema_change_events=COLUMN_CHANGES, schema_change_policy=STOP`, func(t *testing.T) {
			cf := feed(t, f, `CREATE CHANGEFEED FOR insensitive `+
				`WITH schema_change_events=COLUMN_CHANGES, schema_change_policy=STOP`)
			defer closeFeed(t, cf)
			assertPayloads(t, cf, []string{`insensitive: [0]->{"after": {"x": 0, "y": "hello"}}`})
		})

		t.Run(`on_error=FAIL`, func(t *testing.T) {
			cf := feed(t, f, `CREATE CHANGEFEED FOR insensitive WITH on_error=FAIL`)
			defer closeFeed(t, cf)
			assertPayloads(t, cf, []string{`insensitive: [0]->{"after": {"x": 0, "y": "hello"}}`})
		})
	}

	// Some sinks are incompatible with envelope
	cdcTest(t, testFn, feedTestRestrictSinks("sinkless", "enterprise", "kafka"))
}

func TestChangefeedOnlyInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	initialScanOnlyTests := map[string]string{
		`initial scan only`:     `CREATE CHANGEFEED FOR foo WITH initial_scan_only`,
		`initial backfill only`: `CREATE CHANGEFEED FOR foo WITH initial_scan = 'only'`,
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		for testName, changefeedStmt := range initialScanOnlyTests {
			t.Run(testName, func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
				sqlDB.Exec(t, `INSERT INTO foo (a) SELECT * FROM generate_series(1, 5000);`)
				defer func() {
					sqlDB.Exec(t, `DROP TABLE foo`)
				}()

				feed := feed(t, f, changefeedStmt)
				defer closeFeed(t, feed)

				// Insert few more rows after the feed started -- we should not see those emitted.
				sqlDB.Exec(t, "INSERT INTO foo VALUES (5005), (5007), (5009)")

				var expectedMessages []string
				for i := 1; i <= 5000; i++ {
					expectedMessages = append(expectedMessages, fmt.Sprintf(
						`foo: [%d]->{"after": {"a": %d}}`, i, i,
					))
				}

				assertPayloads(t, feed, expectedMessages)

				// It would be nice to assert that after we've seen expectedMessages,
				// that none of the unexpected messages show up before job termination.
				// However, if any of those unexpected messages were emitted, then, we
				// would expect this test to flake (hopefully, with an error message
				// that makes it clear that the unexpected event happen).
				jobFeed := feed.(cdctest.EnterpriseTestFeed)
				require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool {
					return s == jobs.StateSucceeded
				}))
			})
		}
	}

	// "enterprise" and "webhook" sink implementations are too slow
	// for a test that reads 5k messages.
	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestOmitSinks("enterprise", "webhook"))
}

func TestChangefeedOnlyInitialScanCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := map[string]struct {
		changefeedStmt  string
		expectedPayload []string
	}{
		`initial scan only with csv`: {
			changefeedStmt: `CREATE CHANGEFEED FOR foo WITH initial_scan_only, format = csv`,
			expectedPayload: []string{
				`1,Alice`,
				`2,Bob`,
				`3,Carol`,
			},
		},
		`initial backfill only with csv`: {
			changefeedStmt: `CREATE CHANGEFEED FOR foo WITH initial_scan = 'only', format = csv`,
			expectedPayload: []string{
				`1,Alice`,
				`2,Bob`,
				`3,Carol`,
			},
		},
		`initial backfill only with csv multiple tables`: {
			changefeedStmt: `CREATE CHANGEFEED FOR foo, bar WITH initial_scan = 'only', format = csv`,
			expectedPayload: []string{
				`1,a`,
				`2,b`,
				`3,c`,
				`1,Alice`,
				`2,Bob`,
				`3,Carol`,
			},
		},
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		for testName, testData := range tests {
			t.Run(testName, func(t *testing.T) {
				sqlDB.Exec(t, "CREATE TABLE foo (id INT PRIMARY KEY, name STRING)")
				sqlDB.Exec(t, "CREATE TABLE bar (id INT PRIMARY KEY, name STRING)")

				sqlDB.Exec(t, "INSERT INTO foo VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
				sqlDB.Exec(t, "INSERT INTO bar VALUES (1, 'a'), (2, 'b'), (3, 'c')")

				sqlDB.CheckQueryResultsRetry(t, `SELECT count(*) FROM foo,bar`, [][]string{{`9`}})

				feed := feed(t, f, testData.changefeedStmt)

				sqlDB.Exec(t, "INSERT INTO foo VALUES (4, 'Doug'), (5, 'Elaine'), (6, 'Fred')")
				sqlDB.Exec(t, "INSERT INTO bar VALUES (4, 'd'), (5, 'e'), (6, 'f')")

				var actualMessages []string
				g := ctxgroup.WithContext(context.Background())
				g.Go(func() error {
					for {
						m, err := feed.Next()
						if err != nil {
							return err
						}
						if len(m.Resolved) > 0 {
							continue
						}
						actualMessages = append(actualMessages, string(m.Value))
					}
				})
				defer func(expectedPayload []string) {
					closeFeed(t, feed)
					sqlDB.Exec(t, `DROP TABLE foo`)
					sqlDB.Exec(t, `DROP TABLE bar`)
					_ = g.Wait()
					require.Equal(t, len(expectedPayload), len(actualMessages))
					sort.Strings(expectedPayload)
					sort.Strings(actualMessages)
					for i := range expectedPayload {
						require.Equal(t, expectedPayload[i], actualMessages[i])
					}
				}(testData.expectedPayload)

				jobFeed := feed.(cdctest.EnterpriseTestFeed)
				require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool {
					return s == jobs.StateSucceeded
				}))
			})
		}
	}

	// TODO(#119289): re-enable pulsar
	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestOmitSinks("pulsar"))
}

func TestChangefeedOnlyInitialScanCSVSinkless(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	initialScanOnlyCSVTests := map[string]string{
		`initial scan only with csv`:     `CREATE CHANGEFEED FOR foo WITH initial_scan_only, format = csv`,
		`initial backfill only with csv`: `CREATE CHANGEFEED FOR foo WITH initial_scan = 'only', format = csv`,
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		for testName, changefeedStmt := range initialScanOnlyCSVTests {
			t.Run(testName, func(t *testing.T) {
				sqlDB.Exec(t, "CREATE TABLE foo (id INT PRIMARY KEY, name STRING)")
				sqlDB.Exec(t, "INSERT INTO foo VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")

				sqlDB.CheckQueryResultsRetry(t, `SELECT count(*) FROM foo`, [][]string{{`3`}})

				feed := feed(t, f, changefeedStmt)

				sqlDB.Exec(t, "INSERT INTO foo VALUES (4, 'Doug'), (5, 'Elaine'), (6, 'Fred')")

				expectedMessages := []string{
					`1,Alice`,
					`2,Bob`,
					`3,Carol`,
				}
				var actualMessages []string

				defer func() {
					closeFeed(t, feed)
					sqlDB.Exec(t, `DROP TABLE foo`)
					require.Equal(t, len(expectedMessages), len(actualMessages))
					sort.Strings(expectedMessages)
					sort.Strings(actualMessages)
					for i := range expectedMessages {
						require.Equal(t, expectedMessages[i], actualMessages[i])
					}
				}()

				for {
					m, err := feed.Next()
					if err != nil || m == nil {
						break
					}
					actualMessages = append(actualMessages, string(m.Value))
				}
			})
		}
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

// TestPubsubAttributes tests that the "attributes" field in the
// `pubsub_sink_config` behaves as expected.
func TestPubsubAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		db := sqlutils.MakeSQLRunner(s.DB)

		// asserts the next message has these attributes and is sent to each of the supplied topics.
		expectAttributes := func(feed cdctest.TestFeed, attributes map[string]string, allowedTopics ...string) {
			// Keep popping messages until we see all the expected topics.
			seenTopics := make(map[string]struct{})
			for len(seenTopics) < len(allowedTopics) {
				msg, err := feed.(*pubsubFeed).Next()
				require.NoError(t, err)

				raw := msg.RawMessage.(*mockPubsubMessage)

				require.Contains(t, allowedTopics, msg.Topic)
				if attributes == nil {
					require.Nil(t, raw.attributes)
				} else {
					require.True(t, reflect.DeepEqual(attributes, raw.attributes),
						"%#v=%#v", attributes, raw.attributes)
				}
				seenTopics[msg.Topic] = struct{}{}
				t.Logf("message %s: %s -> %s, %v", msg.Key, msg.Value, msg.Topic, raw.attributes)
			}
		}

		t.Run("separate tables", func(t *testing.T) {
			db.Exec(t, "CREATE TABLE one (i int)")
			db.Exec(t, "CREATE TABLE two (i int)")

			foo, err := f.Feed(`CREATE CHANGEFEED FOR TABLE one, TABLE two ` +
				`INTO 'gcpubsub://testfeed?with_table_name_attribute=true' `)
			require.NoError(t, err)

			db.Exec(t, "INSERT INTO one VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "one"}, "one")

			db.Exec(t, "INSERT INTO two VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "two"}, "two")

			require.NoError(t, foo.Close())
		})

		t.Run("same table different families", func(t *testing.T) {
			db.Exec(t, "CREATE TABLE withFams (i int, j int, k int, FAMILY ifam(i), FAMILY jfam(j))")
			db.Exec(t, "CREATE TABLE withoutFams (i int)")

			foo, err := f.Feed(`CREATE CHANGEFEED FOR TABLE withFams FAMILY ifam, TABLE withFams FAMILY jfam, ` +
				`TABLE withoutFams INTO 'gcpubsub://testfeed?with_table_name_attribute=true'`)
			require.NoError(t, err)

			// We get two messages because the changefeed is targeting two familes.
			// Each message should reference the same table.
			db.Exec(t, "INSERT INTO withFams VALUES (1, 2, 3)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "withfams"}, "withfams.jfam", "withfams.ifam")

			db.Exec(t, "INSERT INTO withoutFams VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "withoutfams"}, "withoutfams")

			require.NoError(t, foo.Close())
		})

		t.Run("different tables with one topic", func(t *testing.T) {
			db.Exec(t, "CREATE TABLE a (i int)")
			db.Exec(t, "CREATE TABLE b (i int)")
			db.Exec(t, "CREATE TABLE c (i int)")
			foo, err := f.Feed(`CREATE CHANGEFEED FOR TABLE a, TABLE b, TABLE c ` +
				`INTO 'gcpubsub://testfeed?topic_name=mytopicname&with_table_name_attribute=true'`)
			require.NoError(t, err)

			// Ensure each message goes in a different batch with its own
			// attributes. Ie. ensure batching is not per-topic only, but also
			// per-table when we enable the table name attribute.
			db.Exec(t, "INSERT INTO a VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "a"}, "mytopicname")
			db.Exec(t, "INSERT INTO b VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "b"}, "mytopicname")
			db.Exec(t, "INSERT INTO c VALUES (1)")
			expectAttributes(foo, map[string]string{"TABLE_NAME": "c"}, "mytopicname")

			require.NoError(t, foo.Close())
		})

		t.Run("no attributes", func(t *testing.T) {
			db.Exec(t, "CREATE TABLE non (i int)")
			foo, err := f.Feed(`CREATE CHANGEFEED FOR TABLE non`)
			require.NoError(t, err)

			db.Exec(t, "INSERT INTO non VALUES (1)")
			expectAttributes(foo, nil, "non")

			require.NoError(t, foo.Close())
		})
	}

	cdcTest(t, testFn, feedTestForceSink("pubsub"))
}

// TestChangefeedAvroDecimalColumnWithDiff is a regression test for
// https://github.com/cockroachdb/cockroach/issues/118647.
func TestChangefeedAvroDecimalColumnWithDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE test1 (c1 INT PRIMARY KEY, c2 INT, c3 DECIMAL(19, 0))`)
		sqlDB.Exec(t, `INSERT INTO test1 VALUES (1, 2, 3);`)

		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()
		str := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE test1 WITH OPTIONS (avro_schema_prefix = 'crdb_cdc_', diff, confluent_schema_registry ="%s", format = 'avro', on_error = 'pause', updated);`, schemaReg.URL())
		testFeed := feed(t, f, str)
		defer closeFeed(t, testFeed)

		_, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedMVCCTimestampWithQueries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1);`)

		feed, err := f.Feed(`CREATE CHANGEFEED WITH mvcc_timestamp, format=json, envelope=bare AS SELECT * FROM foo`)
		require.NoError(t, err)
		defer closeFeed(t, feed)

		msgs, err := readNextMessages(ctx, feed, 1)
		require.NoError(t, err)

		var m map[string]any
		require.NoError(t, gojson.Unmarshal(msgs[0].Value, &m))
		ts := m["__crdb__"].(map[string]any)["mvcc_timestamp"].(string)
		assertReasonableMVCCTimestamp(t, ts)
	}

	cdcTest(t, testFn)
}

func TestChangefeedExtraHeaders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Headers are not supported in the v1 kafka sink.
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.new_kafka_sink.enabled = true`)

		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1);`)

		cases := []struct {
			name        string
			headersArg  string
			wantHeaders cdctest.Headers
			expectErr   bool
		}{
			{
				name:        "single header",
				headersArg:  `{"X-Someheader": "somevalue"}`,
				wantHeaders: cdctest.Headers{{K: "X-Someheader", V: []byte("somevalue")}},
			},
			{
				name:       "multiple headers",
				headersArg: `{"X-Someheader": "somevalue", "X-Someotherheader": "someothervalue"}`,
				wantHeaders: cdctest.Headers{
					{K: "X-Someheader", V: []byte("somevalue")},
					{K: "X-Someotherheader", V: []byte("someothervalue")},
				},
			},
			{
				name:       "inappropriate json",
				headersArg: `4`,
				expectErr:  true,
			},
			{
				name:       "also inappropriate json",
				headersArg: `["X-Someheader", "somevalue"]`,
				expectErr:  true,
			},
			{
				name:       "invalid json",
				headersArg: `xxxx`,
				expectErr:  true,
			},
		}

		for _, c := range cases {
			feed, err := f.Feed(fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH extra_headers='%s'`, c.headersArg))
			if c.expectErr {
				require.Error(t, err)
				continue
			} else {
				require.NoError(t, err)
			}

			assertPayloads(t, feed, []string{
				fmt.Sprintf(`foo: [1]%s->{"after": {"key": 1}}`, c.wantHeaders.String()),
			})
			closeFeed(t, feed)
		}
	}

	cdcTest(t, testFn, feedTestRestrictSinks("kafka", "webhook"))
}

func TestChangefeedBareFullProtobuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		envelope     string
		withDiff     bool
		withSource   bool
		expectedRows []string
	}

	tests := []testCase{
		{
			envelope: "bare",
			expectedRows: []string{
				`pricing: {"id":1}->{"values":{"discount":15.75,"id":1,"name":"Chair","options":["Brown","Black"],"tax":"2.500"},"__crdb__":{"key":{"id":1},"topic":"pricing"}}`,
				`pricing: {"id":2}->{"values":{"discount":20,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"},"__crdb__":{"key":{"id":2},"topic":"pricing"}}`,
				`pricing: {"id":2}->{"values":{"discount":25.5,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"},"__crdb__":{"key":{"id":2},"topic":"pricing"}}`,
				`pricing: {"id":1}->{"values":{"discount":10,"id":1,"name":"Armchair","options":["Red"],"tax":"1.000"},"__crdb__":{"key":{"id":1},"topic":"pricing"}}`,
				`pricing: {"id":3}->{"values":{"discount":50,"id":3,"name":"Sofa","options":["Gray"],"tax":"4.250"},"__crdb__":{"key":{"id":3},"topic":"pricing"}}`,
				`pricing: {"id":2}->{"values":{"discount":null,"id":2,"name":null,"options":null,"tax":null},"__crdb__":{"key":{"id":2},"topic":"pricing"}}`,
			},
		},
		{
			envelope: "wrapped",
			withDiff: true,
			expectedRows: []string{
				`pricing: {"id":1}->{"after":{"values":{"discount":15.75,"id":1,"name":"Chair","options":["Brown","Black"],"tax":"2.500"}},"key":{"id":1},"topic":"pricing"}`,
				`pricing: {"id":2}->{"after":{"values":{"discount":20,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"key":{"id":2},"topic":"pricing"}`,
				`pricing: {"id":2}->{"after":{"values":{"discount":25.5,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"before":{"values":{"discount":20,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"key":{"id":2},"topic":"pricing"}`,
				`pricing: {"id":1}->{"after":{"values":{"discount":10,"id":1,"name":"Armchair","options":["Red"],"tax":"1.000"}},"before":{"values":{"discount":15.75,"id":1,"name":"Chair","options":["Brown","Black"],"tax":"2.500"}},"key":{"id":1},"topic":"pricing"}`,
				`pricing: {"id":3}->{"after":{"values":{"discount":50,"id":3,"name":"Sofa","options":["Gray"],"tax":"4.250"}},"key":{"id":3},"topic":"pricing"}`,
				`pricing: {"id":2}->{"before":{"values":{"discount":25.5,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"key":{"id":2},"topic":"pricing"}`,
			},
		},
		{
			envelope: "wrapped",
			expectedRows: []string{
				`pricing: {"id":1}->{"after":{"values":{"discount":15.75,"id":1,"name":"Chair","options":["Brown","Black"],"tax":"2.500"}},"key":{"id":1},"topic":"pricing"}`,
				`pricing: {"id":2}->{"after":{"values":{"discount":20,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"key":{"id":2},"topic":"pricing"}`,
				`pricing: {"id":2}->{"after":{"values":{"discount":25.5,"id":2,"name":"Table","options":["Brown","Black"],"tax":"1.23456789"}},"key":{"id":2},"topic":"pricing"}`,
				`pricing: {"id":1}->{"after":{"values":{"discount":10,"id":1,"name":"Armchair","options":["Red"],"tax":"1.000"}},"key":{"id":1},"topic":"pricing"}`,
				`pricing: {"id":3}->{"after":{"values":{"discount":50,"id":3,"name":"Sofa","options":["Gray"],"tax":"4.250"}},"key":{"id":3},"topic":"pricing"}`,
				`pricing: {"id":2}->{"key":{"id":2},"topic":"pricing"}`,
			},
		},
		{
			envelope:   "enriched",
			withDiff:   true,
			withSource: true,
			expectedRows: []string{
				`pricing: {"id":1}->{"after": {"values": {"discount": 10, "id": 1, "name": "Armchair", "options": ["Red"], "tax": "1.000"}}, "before": {"values": {"discount": 15.75, "id": 1, "name": "Chair", "options": ["Brown", "Black"], "tax": "2.500"}}, "key": {"id": 1}, "op": 2}`,
				`pricing: {"id":1}->{"after": {"values": {"discount": 15.75, "id": 1, "name": "Chair", "options": ["Brown", "Black"], "tax": "2.500"}}, "key": {"id": 1}, "op": 1}`,
				`pricing: {"id":2}->{"after": {"values": {"discount": 20, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 1}`,
				`pricing: {"id":2}->{"after": {"values": {"discount": 25.5, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "before": {"values": {"discount": 20, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 2}`,
				`pricing: {"id":2}->{"before": {"values": {"discount": 25.5, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 3}`,
				`pricing: {"id":3}->{"after": {"values": {"discount": 50, "id": 3, "name": "Sofa", "options": ["Gray"], "tax": "4.250"}}, "key": {"id": 3}, "op": 1}`,
			},
		},
		{
			envelope: "enriched",
			withDiff: true,
			expectedRows: []string{
				`pricing: {"id":1}->{"after": {"values": {"discount": 10, "id": 1, "name": "Armchair", "options": ["Red"], "tax": "1.000"}}, "before": {"values": {"discount": 15.75, "id": 1, "name": "Chair", "options": ["Brown", "Black"], "tax": "2.500"}}, "key": {"id": 1}, "op": 2}`,
				`pricing: {"id":1}->{"after": {"values": {"discount": 15.75, "id": 1, "name": "Chair", "options": ["Brown", "Black"], "tax": "2.500"}}, "key": {"id": 1}, "op": 1}`,
				`pricing: {"id":2}->{"after": {"values": {"discount": 20, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 1}`,
				`pricing: {"id":2}->{"after": {"values": {"discount": 25.5, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "before": {"values": {"discount": 20, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 2}`,
				`pricing: {"id":2}->{"before": {"values": {"discount": 25.5, "id": 2, "name": "Table", "options": ["Brown", "Black"], "tax": "1.23456789"}}, "key": {"id": 2}, "op": 3}`,
				`pricing: {"id":3}->{"after": {"values": {"discount": 50, "id": 3, "name": "Sofa", "options": ["Gray"], "tax": "4.250"}}, "key": {"id": 3}, "op": 1}`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("envelope=%s", tc.envelope), func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)

				sqlDB.Exec(t, `
					CREATE TABLE pricing (
						id INT PRIMARY KEY,
						name STRING,
						discount FLOAT,
						tax DECIMAL,
						options STRING[]
					)`)
				sqlDB.Exec(t, `
					INSERT INTO pricing VALUES
						(1, 'Chair', 15.75, 2.500, ARRAY['Brown', 'Black']),
						(2, 'Table', 20.00, 1.23456789, ARRAY['Brown', 'Black'])`)

				var opts []string
				opts = append(opts, fmt.Sprintf("envelope='%s'", tc.envelope))
				opts = append(opts, "format='protobuf'", "key_in_value", "topic_in_value")
				if tc.withDiff {
					opts = append(opts, "diff")
				}
				if tc.withSource {
					opts = append(opts, "enriched_properties='source'")
				}
				feed := feed(t, f, fmt.Sprintf("CREATE CHANGEFEED FOR pricing WITH %s", strings.Join(opts, ", ")))
				defer closeFeed(t, feed)

				sqlDB.Exec(t, `UPDATE pricing SET discount = 25.50 WHERE id = 2`)
				sqlDB.Exec(t, `UPSERT INTO pricing (id, name, discount, tax, options) VALUES (1, 'Armchair', 10.00, 1.000, ARRAY['Red'])`)
				sqlDB.Exec(t, `INSERT INTO pricing VALUES (3, 'Sofa', 50.00, 4.250, ARRAY['Gray'])`)
				sqlDB.Exec(t, `DELETE FROM pricing WHERE id = 2`)

				if tc.envelope == "enriched" {
					sourceAssertion := func(source map[string]any) {
						if tc.withSource {
							require.NotNil(t, source)
							require.Equal(t, "kafka", source["changefeed_sink"])
							require.Equal(t, "d", source["database_name"])
							require.Equal(t, "public", source["schema_name"])
							require.Equal(t, "pricing", source["table_name"])
							require.Equal(t, "cockroachdb", source["origin"])
							require.ElementsMatch(t, []any{"id"}, source["primary_keys"].([]any))
						} else {
							require.Nil(t, source)
						}
					}
					assertPayloadsEnriched(t, feed, tc.expectedRows, sourceAssertion)
				} else {

					assertPayloads(t, feed, tc.expectedRows)
				}

			}
			cdcTest(t, testFn, feedTestForceSink("kafka"))
		})
	}
}
