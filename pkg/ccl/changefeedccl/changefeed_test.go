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
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	// Imported to allow multi-tenant tests
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	// Imported to allow locality-related table mutations
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloudimpl" // registers cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testServerRegion = "us-east-1"

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))

	// NB running TestChangefeedBasics, which includes a DELETE, with
	// cloudStorageTest is a regression test for #36994.
}

func TestChangefeedBasicConfluentKafka(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestChangefeedDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kvServer, kvSQLdb, cleanup := startTestServer(t, func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	})
	defer cleanup()

	tenantArgs := base.TestTenantArgs{
		// crdb_internal.create_tenant called by StartTenant
		TenantID: serverutils.TestTenantID(),
		// Non-enterprise changefeeds are currently only
		// disabled by setting DisableOutbound true
		// everywhere.
		ExternalIODirConfig: base.ExternalIODirConfig{
			DisableOutbound: true,
		},
		UseDatabase: `d`,
	}

	tenantServer, tenantDB := serverutils.StartTenant(t, kvServer, tenantArgs)
	tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
	tenantSQL.Exec(t, serverSetupStatements)

	tenantSQL.Exec(t, `CREATE TABLE foo_in_tenant (pk INT PRIMARY KEY)`)
	t.Run("changefeed on non-tenant table fails", func(t *testing.T) {
		kvSQL := sqlutils.MakeSQLRunner(kvSQLdb)
		kvSQL.Exec(t, `CREATE TABLE d.foo (pk INT PRIMARY KEY)`)

		tenantSQL.ExpectErr(t, `table "foo" does not exist`,
			`CREATE CHANGEFEED FOR foo`,
		)
	})
	t.Run("sinkful changefeed fails", func(t *testing.T) {
		tenantSQL.ExpectErr(t, "Outbound IO is disabled by configuration, cannot create changefeed into kafka",
			`CREATE CHANGEFEED FOR foo_in_tenant INTO 'kafka://does-not-matter'`,
		)
	})
	t.Run("sinkless changefeed works", func(t *testing.T) {
		sqlAddr := tenantServer.SQLAddr()
		sink, cleanup := sqlutils.PGUrl(t, sqlAddr, t.Name(), url.User(security.RootUser))
		defer cleanup()

		// kvServer is used here because we require a
		// TestServerInterface implementor. It is only used as
		// the return value for f.Server()
		f := makeSinklessFeedFactory(kvServer, sink)
		tenantSQL.Exec(t, `INSERT INTO foo_in_tenant VALUES (1)`)
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo_in_tenant`)
		assertPayloads(t, feed, []string{
			`foo_in_tenant: [1]->{"after": {"pk": 1}}`,
		})
	})
}

func TestChangefeedTenantsExternalIOEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kvServer, _, cleanup := startTestServer(t, func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	})
	defer cleanup()

	tenantArgs := base.TestTenantArgs{
		// crdb_internal.create_tenant called by StartTenant
		TenantID:    serverutils.TestTenantID(),
		UseDatabase: `d`,
		TestingKnobs: base.TestingKnobs{
			DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}

	tenantServer, tenantDB := serverutils.StartTenant(t, kvServer, tenantArgs)
	tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
	tenantSQL.Exec(t, serverSetupStatements)
	tenantSQL.Exec(t, `CREATE TABLE foo_in_tenant (pk INT PRIMARY KEY)`)

	t.Run("sinkful changefeed fails if protect_data_from_gc_on_pause is set", func(t *testing.T) {
		tenantSQL.ExpectErr(t, "operation is unsupported in multi-tenancy mode",
			`CREATE CHANGEFEED FOR foo_in_tenant INTO 'kafka://does-not-matter' WITH protect_data_from_gc_on_pause`,
		)
	})

	t.Run("sinkful changefeed works", func(t *testing.T) {
		f := makeKafkaFeedFactory(&testServerShim{
			TestServerInterface: kvServer,
			sqlServer:           tenantServer},
			tenantDB)
		tenantSQL.Exec(t, `INSERT INTO foo_in_tenant VALUES (1)`)
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo_in_tenant`)
		defer closeFeed(t, feed)
		assertPayloads(t, feed, []string{
			`foo_in_tenant: [1]->{"after": {"pk": 1}}`,
		})
	})
}

func TestChangefeedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
}

func TestChangefeedFullTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

		t.Run(`envelope=row`, func(t *testing.T) {
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH full_table_name`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{`d.public.foo: [1]->{"after": {"a": 1, "b": "a"}}`})
		})
	}
	//TODO(zinger): Plumb this option through to all encoders so it works in sinkless mode
	//t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		if e, ok := fooLogical.(cdctest.EnterpriseTestFeed); ok {
			var bytes []byte
			sqlDB.QueryRow(t, `SELECT payload FROM system.jobs WHERE id=$1`, e.JobID()).Scan(&bytes)
			var payload jobspb.Payload
			require.NoError(t, protoutil.Unmarshal(bytes, &payload))
			require.Equal(t, parseTimeToHLC(t, tsLogical), payload.GetChangefeed().StatementTime)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		if jf, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			d, err := jf.Details()
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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedMVCCTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedResolvedFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		t.Run(`no cursor - no initial scan`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE no_initial_scan (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO no_initial_scan VALUES (1)`)

			noInitialScan := feed(t, f, `CREATE CHANGEFEED FOR no_initial_scan `+
				`WITH no_initial_scan, resolved='1s'`)
			defer closeFeed(t, noInitialScan)
			expectResolvedTimestamp(t, noInitialScan)
			sqlDB.Exec(t, `INSERT INTO no_initial_scan VALUES (2)`)
			assertPayloads(t, noInitialScan, []string{
				`no_initial_scan: [2]->{"after": {"a": 2}}`,
			})
		})

		t.Run(`cursor - with initial scan`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE initial_scan (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO initial_scan VALUES (1), (2), (3)`)
			var tsStr string
			var i int
			sqlDB.QueryRow(t, `SELECT count(*), cluster_logical_timestamp() from initial_scan`).Scan(&i, &tsStr)
			initialScan := feed(t, f, `CREATE CHANGEFEED FOR initial_scan `+
				`WITH initial_scan, resolved='1s', cursor='`+tsStr+`'`)
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedExternalIODisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("sinkful changefeeds not allowed with disabled external io", func(t *testing.T) {
		disallowedSinkProtos := []string{
			changefeedbase.SinkSchemeExperimentalSQL,
			changefeedbase.SinkSchemeKafka,
			changefeedbase.SinkSchemeNull, // Doesn't work because all sinkful changefeeds are disallowed
			// Cloud sink schemes
			"experimental-s3",
			"experimental-gs",
			"experimental-nodelocal",
			"experimental-http",
			"experimental-https",
			"experimental-azure",
		}
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			ExternalIODirConfig: base.ExternalIODirConfig{
				DisableOutbound: true,
			},
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
		for _, proto := range disallowedSinkProtos {
			sqlDB.ExpectErr(t, "Outbound IO is disabled by configuration, cannot create changefeed",
				"CREATE CHANGEFEED FOR target_table INTO $1",
				fmt.Sprintf("%s://does-not-matter", proto),
			)
		}
	})

	withDisabledOutbound := func(args *base.TestServerArgs) { args.ExternalIODirConfig.DisableOutbound = true }
	t.Run("sinkless changfeeds are allowed with disabled external io",
		sinklessTestWithServerArgs(withDisabledOutbound,
			func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(db)
				sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
				sqlDB.Exec(t, "INSERT INTO target_table VALUES (1)")
				feed := feed(t, f, "CREATE CHANGEFEED FOR target_table")
				defer closeFeed(t, feed)
				assertPayloads(t, feed, []string{
					`target_table: [1]->{"after": {"pk": 1}}`,
				})
			}))
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedSchemaChangeNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1, regexp.MustCompile("cdc ux violation"),
		log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// Test schema changes that require a backfill when the backfill option is
// allowed.
func TestChangefeedSchemaChangeAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

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
			ts := fetchDescVersionModificationTime(t, db, f, `add_column_def`, 4)
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
			ts := fetchDescVersionModificationTime(t, db, f, `add_col_comp`, 4)
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
			ts := fetchDescVersionModificationTime(t, db, f, `drop_column`, 2)
			assertPayloads(t, dropColumn, []string{
				fmt.Sprintf(`drop_column: [1]->{"after": {"a": 1}, "updated": "%s"}`, ts.AsOfSystemTime()),
				fmt.Sprintf(`drop_column: [2]->{"after": {"a": 2}, "updated": "%s"}`, ts.AsOfSystemTime()),
			})
			sqlDB.Exec(t, `INSERT INTO drop_column VALUES (3)`)
			assertPayloadsStripTs(t, dropColumn, []string{
				`drop_column: [3]->{"after": {"a": 3}}`,
				`drop_column: [1]->{"after": {"a": 1}}`,
				`drop_column: [2]->{"after": {"a": 2}}`,
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

			ts := fetchDescVersionModificationTime(t, db, f, `multiple_alters`, 2)
			// Changefeed level backfill for DROP COLUMN b.
			assertPayloads(t, multipleAlters, []string{
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1}, "updated": "%s"}`, ts.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2}, "updated": "%s"}`, ts.AsOfSystemTime()),
			})
			assertPayloadsStripTs(t, multipleAlters, []string{
				// Schema-change backfill for DROP COLUMN b.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
				// Schema-change backfill for ADD COLUMN c.
				`multiple_alters: [1]->{"after": {"a": 1}}`,
				`multiple_alters: [2]->{"after": {"a": 2}}`,
			})
			ts = fetchDescVersionModificationTime(t, db, f, `multiple_alters`, 7)
			// Changefeed level backfill for ADD COLUMN c.
			assertPayloads(t, multipleAlters, []string{
				fmt.Sprintf(`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}, "updated": "%s"}`, ts.AsOfSystemTime()),
				fmt.Sprintf(`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}, "updated": "%s"}`, ts.AsOfSystemTime()),
			})
			// Schema change level backfill for ADD COLUMN d.
			assertPayloadsStripTs(t, multipleAlters, []string{
				`multiple_alters: [1]->{"after": {"a": 1, "c": "cee"}}`,
				`multiple_alters: [2]->{"after": {"a": 2, "c": "cee"}}`,
			})
			ts = fetchDescVersionModificationTime(t, db, f, `multiple_alters`, 10)
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

	// TODO(ssd): tenant tests skipped because of f.Server() use
	// in fetchDescVersionModificationTime
	t.Run(`sinkless`, sinklessTest(testFn, feedTestNoTenants))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// Test schema changes that require a backfill on only some watched tables within a changefeed.
func TestChangefeedSchemaChangeBackfillScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

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
			ts := fetchDescVersionModificationTime(t, db, f, `add_column_def`, 4)
			// Schema change backfill
			assertPayloadsStripTs(t, combinedFeed, []string{
				`add_column_def: [1]->{"after": {"a": 1}}`,
				`add_column_def: [2]->{"after": {"a": 2}}`,
			})
			// Changefeed level backfill
			assertPayloads(t, combinedFeed, []string{
				fmt.Sprintf(`add_column_def: [1]->{"after": {"a": 1, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
				fmt.Sprintf(`add_column_def: [2]->{"after": {"a": 2, "b": "d"}, "updated": "%s"}`,
					ts.AsOfSystemTime()),
			})
		})

	}

	// TODO(ssd): tenant tests skipped because of f.Server() use
	// in fetchDescVerionModifationTime
	t.Run(`sinkless`, sinklessTest(testFn, feedTestNoTenants))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

// fetchDescVersionModificationTime fetches the `ModificationTime` of the specified
// `version` of `tableName`'s table descriptor.
func fetchDescVersionModificationTime(
	t testing.TB, db *gosql.DB, f cdctest.TestFeedFactory, tableName string, version int,
) hlc.Timestamp {
	tblKey := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
	header := roachpb.RequestHeader{
		Key:    tblKey,
		EndKey: tblKey.PrefixEnd(),
	}
	dropColTblID := sqlutils.QueryTableID(t, db, `d`, "public", tableName)
	req := &roachpb.ExportRequest{
		RequestHeader: header,
		MVCCFilter:    roachpb.MVCCFilter_All,
		StartTime:     hlc.Timestamp{},
		ReturnSST:     true,
	}
	clock := hlc.NewClock(hlc.UnixNano, time.Minute)
	hh := roachpb.Header{Timestamp: clock.Now()}
	res, pErr := kv.SendWrappedWith(context.Background(),
		f.Server().DB().NonTransactionalSender(), hh, req)
	if pErr != nil {
		t.Fatal(pErr.GoError())
	}
	for _, file := range res.(*roachpb.ExportResponse).Files {
		it, err := storage.NewMemSSTIterator(file.SST, false /* verify */)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		for it.SeekGE(storage.NilKey); ; it.Next() {
			if ok, err := it.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				continue
			}
			k := it.UnsafeKey()
			remaining, _, _, err := keys.SystemSQLCodec.DecodeIndexPrefix(k.Key)
			if err != nil {
				t.Fatal(err)
			}
			_, tableID, err := encoding.DecodeUvarintAscending(remaining)
			if err != nil {
				t.Fatal(err)
			}
			if tableID != uint64(dropColTblID) {
				continue
			}
			unsafeValue := it.UnsafeValue()
			if unsafeValue == nil {
				t.Fatal(errors.New(`value was dropped or truncated`))
			}
			value := roachpb.Value{RawBytes: unsafeValue}
			var desc descpb.Descriptor
			if err := value.GetProto(&desc); err != nil {
				t.Fatal(err)
			}
			if tableDesc, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, k.Timestamp); tableDesc != nil {
				if int(tableDesc.Version) == version {
					return tableDesc.ModificationTime
				}
			}
		}
	}
	t.Fatal(errors.New(`couldn't find table desc for given version`))
	return hlc.Timestamp{}
}

// Regression test for #34314
func TestChangefeedAfterSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("cdc ux violation"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("Found violation of CDC's guarantees: %v", entries)
	}
}

func TestChangefeedInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled = true`)
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
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedAuthorization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name, statement, errMsg string
	}{
		{name: `kafka`,
			statement: `CREATE CHANGEFEED FOR d.table_a INTO 'kafka://nope'`,
			errMsg:    `connecting to kafka`,
		},
		{name: `cloud`,
			statement: `CREATE CHANGEFEED FOR d.table_a INTO 'experimental-nodelocal://12/nope/'`,
			errMsg:    `connecting to node 12`,
		},
		{name: `sinkless`,
			statement: `EXPERIMENTAL CHANGEFEED FOR d.table_a WITH resolved='1'`,
			errMsg:    `missing unit in duration`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, stop := startTestServer(t, nil)
			defer stop()
			rootDB := sqlutils.MakeSQLRunner(db)

			rootDB.Exec(t, `create user guest with password 'password'`)
			rootDB.Exec(t, `create user feedcreator with controlchangefeed password 'hunter2'`)

			pgURL := url.URL{
				Scheme: "postgres",
				User:   url.UserPassword(`guest`, `password`),
				Host:   s.ServingSQLAddr(),
			}

			db2, err := gosql.Open("postgres", pgURL.String())
			require.NoError(t, err)
			guestDB := sqlutils.MakeSQLRunner(db2)
			defer db2.Close()

			pgURL = url.URL{
				Scheme: "postgres",
				User:   url.UserPassword(`feedcreator`, `hunter2`),
				Host:   s.ServingSQLAddr(),
			}

			db3, err := gosql.Open("postgres", pgURL.String())
			require.NoError(t, err)
			feedCreatorDB := sqlutils.MakeSQLRunner(db3)
			defer db3.Close()

			rootDB.Exec(t, `create type type_a as enum ('a');`)
			rootDB.Exec(t, `create table table_a (id int, type type_a);`)

			guestDB.ExpectErr(t, `permission denied to create changefeed`, tc.statement)
			feedCreatorDB.ExpectErr(t, `user feedcreator does not have SELECT privilege on relation table_a`, tc.statement)

			// Actual success would hang in sinkless and require cleanup in enterprise, so checking for successful authorization
			// on a non-root user by asserting we get to an unrelated error

			/*
				        // This could be tested much more cleanly with the below code,
						// but https://github.com/cockroachdb/cockroach/issues/49313 deeply breaks
						// all of our cdc test helpers when running as not admin.
						// TODO(zinger): Give this test a happier ending once #49313 is fixed.
						nonRootFeedFactory := cdctest.MakeSinklessFeedFactory(f.Server(), feedCreatorPgURL)
						nonRootFeed := feed(t, nonRootFeedFactory, createChangefeedCmd)
						closeFeed(t, nonRootFeed)
			*/

			rootDB.Exec(t, `grant select on table table_a to feedcreator`)
			feedCreatorDB.ExpectErr(t, tc.errMsg, tc.statement)
		})
	}
}

func requireErrorSoon(
	ctx context.Context, t *testing.T, f cdctest.TestFeed, errRegex *regexp.Regexp,
) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		if _, err := f.Next(); err != nil {
			assert.Regexp(t, errRegex, err)
			done <- struct{}{}
		}
	}()
	select {
	case <-ctx.Done():
		t.Fatal("timed out waiting for changefeed to fail")
	case <-done:
	}
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

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		t.Run("import fails changefeed", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE for_import (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE for_import`)
			sqlDB.Exec(t, `INSERT INTO for_import VALUES (0, NULL)`)
			forImport := feed(t, f, `CREATE CHANGEFEED FOR for_import `)
			defer closeFeed(t, forImport)
			assertPayloads(t, forImport, []string{
				`for_import: [0]->{"after": {"a": 0, "b": null}}`,
			})
			sqlDB.Exec(t, `IMPORT INTO for_import CSV DATA ($1)`, dataSrv.URL)
			requireErrorSoon(context.Background(), t, forImport,
				regexp.MustCompile(`CHANGEFEED cannot target offline table: for_import \(offline reason: "importing"\)`))
		})
	}
	// TODO(ssd): tenant tests skipped because of:
	// changefeed_test.go:1409: error executing 'IMPORT INTO
	// for_import CSV DATA ($1)': pq: fake protectedts.Provide
	t.Run(`sinkless`, sinklessTest(testFn, feedTestNoTenants))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedFailOnRBRChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rbrErrorRegex := regexp.MustCompile(`CHANGEFEED cannot target REGIONAL BY ROW tables: rbr`)
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		t.Run("regional by row change fails changefeed", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT)`)
			defer sqlDB.Exec(t, `DROP TABLE rbr`)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (0, NULL)`)
			rbr := feed(t, f, `CREATE CHANGEFEED FOR rbr `)
			defer closeFeed(t, rbr)
			sqlDB.Exec(t, `INSERT INTO rbr VALUES (1, 2)`)
			assertPayloads(t, rbr, []string{
				`rbr: [0]->{"after": {"a": 0, "b": null}}`,
				`rbr: [1]->{"after": {"a": 1, "b": 2}}`,
			})
			sqlDB.Exec(t, `ALTER TABLE rbr SET LOCALITY REGIONAL BY ROW`)
			requireErrorSoon(context.Background(), t, rbr, rbrErrorRegex)
		})
	}
	withTestServerRegion := func(args *base.TestServerArgs) {
		args.Locality.Tiers = append(args.Locality.Tiers, roachpb.Tier{
			Key:   "region",
			Value: testServerRegion,
		})
	}

	// Tenants skiped because of:
	//
	// error executing 'ALTER DATABASE d PRIMARY REGION
	// "us-east-1"': pq: get_live_cluster_regions: unimplemented:
	// operation is unsupported in multi-tenancy mode
	t.Run(`sinkless`, sinklessTestWithServerArgs(withTestServerRegion, testFn, feedTestNoTenants))
	t.Run(`enterprise`, enterpriseTestWithServerArgs(withTestServerRegion, testFn))
	t.Run(`cloudstorage`, cloudStorageTestWithServerArg(withTestServerRegion, testFn))
	t.Run(`kafka`, kafkaTestWithServerArgs(withTestServerRegion, testFn))
	t.Run(`webhook`, webhookTestWithServerArgs(withTestServerRegion, testFn))
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
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.close_fraction = .99")

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
			// NB: You might expect to only see the new row here but we'll see them
			// all because we cannot distinguish between the index backfill and
			// foreground writes. See #35738.
			assertPayloads(t, dropColumn, []string{
				`drop_column: [0]->{"after": {"a": 0}}`,
				`drop_column: [1]->{"after": {"a": 1}}`,
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.close_fraction = .99")

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
			// NB: You might expect to only see the new row here but we'll see them
			// all because we cannot distinguish between the index backfill and
			// foreground writes. See #35738.
			assertPayloads(t, dropColumn, []string{
				`drop_column: [0]->{"after": {"a": 0}}`,
				`drop_column: [1]->{"after": {"a": 1}}`,
				`drop_column: [2]->{"after": {"a": 2}}`,
			})
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedUpdatePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedTruncateOrDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	assertFailuresCounter := func(t *testing.T, m *Metrics, exp int64) {
		t.Helper()
		// If this changefeed is running as a job, we anticipate that it will move
		// through the failed state and will increment the metric. Sinkless feeds
		// don't contribute to the failures counter.
		if strings.Contains(t.Name(), `sinkless`) {
			return
		}
		testutils.SucceedsSoon(t, func() error {
			if got := m.Failures.Count(); got != exp {
				return errors.Errorf("expected %d failures, got %d", exp, got)
			}
			return nil
		})
	}

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		registry := f.Server().JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)

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
		if _, err := truncate.Next(); !testutils.IsError(err, `"truncate" was truncated`) {
			t.Fatalf(`expected ""truncate" was truncated" error got: %+v`, err)
		}
		if _, err := truncateCascade.Next(); !testutils.IsError(
			err, `"truncate_cascade" was truncated`,
		) {
			t.Fatalf(`expected ""truncate_cascade" was truncated" error got: %+v`, err)
		}
		assertFailuresCounter(t, metrics, 2)

		sqlDB.Exec(t, `CREATE TABLE drop (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO drop VALUES (1)`)
		drop := feed(t, f, `CREATE CHANGEFEED FOR drop`)
		defer closeFeed(t, drop)
		assertPayloads(t, drop, []string{`drop: [1]->{"after": {"a": 1}}`})
		sqlDB.Exec(t, `DROP TABLE drop`)
		if _, err := drop.Next(); !testutils.IsError(err, `"drop" was dropped`) {
			t.Errorf(`expected ""drop" was dropped" error got: %+v`, err)
		}
		assertFailuresCounter(t, metrics, 3)
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		if c := s.MustGetSQLCounter(`changefeed.table_metadata_nanos`); c != 0 {
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
			if c := s.MustGetSQLCounter(`changefeed.running`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
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
		const expectedLatency = 5 * time.Second
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

		// Cancel all the changefeeds and check that max_behind_nanos returns to 0
		// and the number running returns to 0.
		require.NoError(t, foo.Close())
		require.NoError(t, fooCopy.Close())
		testutils.SucceedsSoon(t, func() error {
			if c := s.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			if c := s.MustGetSQLCounter(`changefeed.running`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			return nil
		})
	}
	// TODO(ssd): tenant tests skipped because of f.Server() use
	t.Run(`sinkless`, sinklessTest(testFn, feedTestNoTenants))
	t.Run(`enterprise`, func(t *testing.T) {
		skip.WithIssue(t, 38443)
		enterpriseTest(testFn)
	})
}

func TestChangefeedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		var failEmit int64
		knobs.BeforeEmitRow = func(_ context.Context) error {
			switch atomic.LoadInt64(&failEmit) {
			case 1:
				return changefeedbase.MarkRetryableError(fmt.Errorf("synthetic retryable error"))
			case 2:
				return fmt.Errorf("synthetic terminal error")
			default:
				return nil
			}
		}

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
		atomic.StoreInt64(&failEmit, 1)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		registry := f.Server().JobRegistry().(*jobs.Registry)

		retryCounter := registry.MetricsStruct().Changefeed.(*Metrics).ErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Counter.Count() < 3 {
				return fmt.Errorf("insufficient error retries detected")
			}
			return nil
		})

		// Verify job progress contains retryable error status.
		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
		job, err := registry.LoadJob(context.Background(), jobID)
		require.NoError(t, err)
		require.Contains(t, job.Progress().RunningStatus, "synthetic retryable error")

		// Verify `SHOW JOBS` also shows this information.
		var runningStatus string
		sqlDB.QueryRow(t,
			`SELECT running_status FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
		).Scan(&runningStatus)
		require.Contains(t, runningStatus, "synthetic retryable error")

		// Fix the sink and insert another row. Check that nothing funky happened.
		atomic.StoreInt64(&failEmit, 0)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		assertPayloads(t, foo, []string{
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		// Set sink to return a terminal error and insert a row. Ensure that we
		// eventually get the error message back out.
		atomic.StoreInt64(&failEmit, 2)
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

	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

// TestChangefeedDataTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table data.
func TestChangefeedDataTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

		// Create the data table; it will only contain a
		// single row with multiple versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)

		counter := 0
		upsertedValues := make(map[int]struct{})
		upsertRow := func() {
			counter++
			sqlDB.Exec(t, `UPSERT INTO foo (a, b) VALUES (1, $1)`, counter)
			upsertedValues[counter] = struct{}{}
		}

		// Create the initial version of the row and the
		// changefeed itself. The initial version is necessary
		// to ensure that there is at least one row to
		// backfill.
		upsertRow()

		// Set emit trap to ensure the backfill will pause.
		// The backfill happens before the construction of the
		// rangefeed. Further the backfill sends rows to the
		// changeAggregator via an unbuffered channel, so
		// blocking the emit should block the scan from
		// finishing.
		atomic.StoreInt32(&shouldWait, 1)

		dataExpiredRows := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		defer closeFeed(t, dataExpiredRows)

		// Ensure our changefeed is started and waiting during the backfill.
		<-wait

		// Upsert additional versions. One of these will be
		// deleted by the GC process before the rangefeed is
		// started.
		upsertRow()
		upsertRow()
		upsertRow()

		// Force a GC of the table. This should cause both
		// versions of the table to be deleted.
		forceTableGC(t, f.Server(), sqlDB, "d", "foo")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that, at some point, Next() returns a "must
		// be after replica GC threshold" error. In the common
		// case, that'll be the second call, the first will
		// should return the row from the backfill and the
		// second should be returning
		for {
			msg, err := dataExpiredRows.Next()
			if testutils.IsError(err, `must be after replica GC threshold`) {
				t.Logf("got expected GC error: %s", err)
				break
			}
			if msg != nil {
				t.Logf("ignoring message: %s", msg)
				var decodedMessage struct {
					After struct {
						A int
						B int
					}
				}
				err = json.Unmarshal(msg.Value, &decodedMessage)
				require.NoError(t, err)
				delete(upsertedValues, decodedMessage.After.B)
				if len(upsertedValues) == 0 {
					t.Error("TestFeed emitted all values despite GC running")
					return
				}
			}
		}
	}
	// NOTE(ssd): This test doesn't apply to enterprise
	// changefeeds since enterprise changefeeds create a protected
	// timestamp before beginning their backfill.
	//
	// TODO(ssd): Tenant test disabled because this test requires
	// the fully TestServerInterface.
	t.Run("sinkless", sinklessTest(testFn, feedTestNoTenants))
}

// TestChangefeedSchemaTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table's schema.
func TestChangefeedSchemaTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	// TODO(ssd): tenant tests skipped because of f.Server() use
	// in forceTableGC
	t.Run("sinkless", sinklessTest(testFn, feedTestNoTenants))
	t.Run("enterprise", enterpriseTest(testFn))
	t.Run("cloudstorage", cloudStorageTest(testFn))
	t.Run("kafka", kafkaTest(testFn))
	t.Run(`webhook`, func(t *testing.T) {
		skip.WithIssue(t, 66991, "flaky test")
		webhookTest(testFn)(t)
	})
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{{
				Key:   "region",
				Value: testServerRegion,
			}},
		},
	})
	schemaReg := cdctest.StartTestSchemaRegistry()
	defer schemaReg.Close()

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
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	// Feature flag for changefeeds is off — test that CREATE CHANGEFEED and
	// EXPERIMENTAL CHANGEFEED FOR surface error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.changefeed.enabled = false`)
	sqlDB.ExpectErr(t, `feature CHANGEFEED was disabled by the database administrator`,
		`CREATE CHANGEFEED FOR foo`)
	sqlDB.ExpectErr(t, `feature CHANGEFEED was disabled by the database administrator`,
		`EXPERIMENTAL CHANGEFEED FOR foo`)

	sqlDB.Exec(t, `SET CLUSTER SETTING feature.changefeed.enabled = true`)

	sqlDB.ExpectErr(
		t, `unknown format: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH format=nope`,
	)

	sqlDB.ExpectErr(
		t, `unknown envelope: nope`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH envelope=nope`,
	)

	sqlDB.ExpectErr(
		t, `time: invalid duration "bar"`,
		`EXPERIMENTAL CHANGEFEED FOR foo WITH resolved='bar'`,
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

	// Regional by row tables are not currently supported
	sqlDB.Exec(t, fmt.Sprintf(`ALTER DATABASE defaultdb PRIMARY REGION "%s"`, testServerRegion))
	sqlDB.Exec(t, `CREATE TABLE test_cdc_rbr_fails (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE test_cdc_rbr_fails SET LOCALITY REGIONAL BY ROW`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED cannot target REGIONAL BY ROW tables: test_cdc_rbr_fails`,
		`CREATE CHANGEFEED FOR test_cdc_rbr_fails`,
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
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)
	sqlDB.Exec(t, `CREATE TABLE "oid" (a OID PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO "oid" VALUES (3::OID)`)
	sqlDB.ExpectErr(
		t, `pq: column a: type OID not yet supported with avro`,
		`EXPERIMENTAL CHANGEFEED FOR "oid" WITH format=$1, confluent_schema_registry=$2`,
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)

	unknownParams := func(sink string, params ...string) string {
		return fmt.Sprintf(`unknown %s sink query parameters: [%s]`, sink, strings.Join(params, ", "))
	}

	// Check that confluent_schema_registry is only accepted if format is avro.
	// TODO: This should be testing it as a WITH option and check avro_schema_prefix too
	sqlDB.ExpectErr(
		t, unknownParams("SQL", "confluent_schema_registry", "weird"),
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?confluent_schema_registry=foo&weird=bar`,
	)

	// Check unavailable kafka.
	sqlDB.ExpectErr(
		t, `client has run out of available brokers`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`,
	)

	// Test that a well-formed URI gets as far as unavailable kafka error.
	sqlDB.ExpectErr(
		t, `client has run out of available brokers`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=true&topic_name=foo'`,
	)

	// kafka_topic_prefix was referenced by an old version of the RFC, it's
	// "topic_prefix" now.
	sqlDB.ExpectErr(
		t, unknownParams(`kafka`, `kafka_topic_prefix`),
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?kafka_topic_prefix=foo`,
	)

	// topic_name is only honored for kafka sinks
	sqlDB.ExpectErr(
		t, unknownParams("SQL", "topic_name"),
		`CREATE CHANGEFEED FOR foo INTO $1`, `experimental-sql://d/?topic_name=foo`,
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
		t, `param insecure_tls_skip_verify must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=foo`,
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
	sqlDB.ExpectErr(
		t, `sasl_enabled must be enabled to configure SASL mechanism`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_mechanism=SCRAM-SHA-256`,
	)
	sqlDB.ExpectErr(
		t, `param sasl_mechanism must be one of SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope/?sasl_enabled=true&sasl_mechanism=unsuppported`,
	)
	sqlDB.ExpectErr(
		t, `client has run out of available brokers`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
	)
	// The avro format doesn't support key_in_value or topic_in_value yet.
	sqlDB.ExpectErr(
		t, `key_in_value is not supported with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `topic_in_value is not supported with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)

	// The cloudStorageSink is particular about the options it will work with.
	sqlDB.ExpectErr(
		t, `this sink is incompatible with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`experimental-nodelocal://0/bar`, schemaReg.URL(),
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`experimental-nodelocal://0/bar`,
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

	// WITH topic_in_value requires envelope=wrapped
	sqlDB.ExpectErr(
		t, `topic_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, envelope='key_only'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `topic_in_value is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, envelope='row'`, `kafka://nope`,
	)

	// WITH diff requires envelope=wrapped
	sqlDB.ExpectErr(
		t, `diff is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH diff, envelope='key_only'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `diff is only usable with envelope=wrapped`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH diff, envelope='row'`, `kafka://nope`,
	)

	// WITH initial_scan and no_initial_scan disallowed
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan, no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH no_initial_scan, initial_scan`, `kafka://nope`,
	)

	// Sanity check schema registry tls parameters.
	sqlDB.ExpectErr(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`kafka://nope`, `https://schemareg-nope/?ca_cert=!`,
	)
	sqlDB.ExpectErr(
		t, `failed to parse certificate data`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`kafka://nope`, `https://schemareg-nope/?ca_cert=Zm9v`,
	)

	// Sanity check http sink options.
	sqlDB.ExpectErr(
		t, `unsupported sink: https. HTTP endpoints can be used with webhook-https and experimental-https`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `param insecure_tls_skip_verify must be a bool`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?insecure_tls_skip_verify=foo`,
	)
	sqlDB.ExpectErr(
		t, `param ca_cert must be base 64 encoded`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?ca_cert=?`,
	)
	sqlDB.ExpectErr(
		t, `failed to parse certificate data`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-https://fake-host?ca_cert=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `sink requires https`,
		`CREATE CHANGEFEED FOR foo INTO $1`, `webhook-http://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with format=experimental_avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='experimental_avro', confluent_schema_registry=$2`,
		`webhook-https://fake-host`, schemaReg.URL(),
	)
	sqlDB.ExpectErr(
		t, `problem parsing option webhook_client_timeout: time: invalid duration "not_an_integer"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='not_an_integer'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `option webhook_client_timeout must be a positive duration`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='0s'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `option webhook_client_timeout must be a positive duration`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='-500s'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `problem parsing option webhook_client_timeout: time: missing unit in duration`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='0.5'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with envelope=key_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='key_only'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with envelope=row`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH envelope='row'`,
		`webhook-https://fake-host`,
	)
}

func TestChangefeedDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intentionally don't use the TestFeedFactory because we want to
	// control the placeholders.
	s, db, stopServer := startTestServer(t, nil)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

	sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	sink.Scheme = changefeedbase.SinkSchemeExperimentalSQL
	sink.Path = `d`

	var jobID jobspb.JobID
	sqlDB.QueryRow(t,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, envelope = $2`, sink.String(), `wrapped`,
	).Scan(&jobID)

	var description string
	sqlDB.QueryRow(t,
		`SELECT description FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
	).Scan(&description)
	expected := `CREATE CHANGEFEED FOR TABLE foo INTO '` + sink.String() +
		`' WITH envelope = 'wrapped', updated`
	require.Equal(t, expected, description)
}

func TestChangefeedPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`)
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

		feedJob := foo.(cdctest.EnterpriseTestFeed)
		sqlDB.Exec(t, `PAUSE JOB $1`, feedJob.JobID())
		// PAUSE JOB only requests the job to be paused. Block until it's paused.
		opts := retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     time.Second,
			Multiplier:     2,
		}
		ctx := context.Background()
		if err := retry.WithMaxAttempts(ctx, opts, 10, func() error {
			var status string
			sqlDB.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, feedJob.JobID()).Scan(&status)
			if jobs.Status(status) != jobs.StatusPaused {
				return errors.New("could not pause job")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		sqlDB.Exec(t, `RESUME JOB $1`, feedJob.JobID())
		assertPayloads(t, foo, []string{
			`foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedPauseUnpauseCursorAndInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)
		var tsStr string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp() from foo`).Scan(&tsStr)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH initial_scan, resolved='10ms', cursor='`+tsStr+`'`)
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
		expectResolvedTimestamp(t, foo)
		expectResolvedTimestamp(t, foo)

		feedJob := foo.(cdctest.EnterpriseTestFeed)
		require.NoError(t, feedJob.Pause())

		foo.(seenTracker).reset()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
		require.NoError(t, feedJob.Resume())
		assertPayloads(t, foo, []string{
			`foo: [16]->{"after": {"a": 16, "b": "f"}}`,
		})
	}

	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx      = context.Background()
		userSpan = roachpb.Span{
			Key:    keys.UserTableDataMin,
			EndKey: keys.TableDataMax,
		}
		done               = make(chan struct{})
		blockRequestCh     = make(chan chan chan struct{}, 1)
		requestBlockedScan = func() (waitForBlockedScan func() (unblockScan func())) {
			blockRequest := make(chan chan struct{})
			blockRequestCh <- blockRequest // test sends to filter to request a block
			return func() (unblockScan func()) {
				toClose := <-blockRequest // filter sends back to test to report blocked
				return func() {
					close(toClose) // test closes to unblock filter
				}
			}
		}
		requestFilter = kvserverbase.ReplicaRequestFilter(func(
			ctx context.Context, ba roachpb.BatchRequest,
		) *roachpb.Error {
			if ba.Txn == nil || ba.Txn.Name != "changefeed backfill" {
				return nil
			}
			scanReq, ok := ba.GetArg(roachpb.Scan)
			if !ok {
				return nil
			}
			if !userSpan.Contains(scanReq.Header().Span()) {
				return nil
			}
			select {
			case notifyCh := <-blockRequestCh:
				waitUntilClosed := make(chan struct{})
				notifyCh <- waitUntilClosed
				select {
				case <-waitUntilClosed:
				case <-done:
				}
			default:
			}
			return nil
		})
		mkGetPtsRec = func(t *testing.T, ptp protectedts.Provider, clock *hlc.Clock) func() *ptpb.Record {
			return func() (r *ptpb.Record) {
				t.Helper()
				require.NoError(t, ptp.Refresh(ctx, clock.Now()))
				ptp.Iterate(ctx, userSpan.Key, userSpan.EndKey, func(record *ptpb.Record) (wantMore bool) {
					r = record
					return false
				})
				return r
			}
		}
		mkCheckRecord = func(t *testing.T, tableID int) func(r *ptpb.Record) error {
			expectedKeys := map[string]struct{}{
				string(keys.SystemSQLCodec.TablePrefix(uint32(tableID))):        {},
				string(keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)): {},
			}
			return func(ptr *ptpb.Record) error {
				if ptr == nil {
					return errors.Errorf("expected protected timestamp")
				}
				require.Equal(t, len(ptr.Spans), len(expectedKeys), ptr.Spans, expectedKeys)
				for _, s := range ptr.Spans {
					require.Contains(t, expectedKeys, string(s.Key))
				}
				return nil
			}
		}
		checkNoRecord = func(ptr *ptpb.Record) error {
			if ptr != nil {
				return errors.Errorf("expected protected timestamp to not exist, found %v", ptr)
			}
			return nil
		}
		mkWaitForRecordCond = func(t *testing.T, getRecord func() *ptpb.Record, check func(record *ptpb.Record) error) func() {
			return func() {
				t.Helper()
				testutils.SucceedsSoon(t, func() error { return check(getRecord()) })
			}
		}
	)

	t.Run(`enterprise`, enterpriseTestWithServerArgs(
		func(args *base.TestServerArgs) {
			storeKnobs := &kvserver.StoreTestingKnobs{}
			storeKnobs.TestingRequestFilter = requestFilter
			args.Knobs.Store = storeKnobs
		},
		func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
			defer close(done)
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1`)
			sqlDB.Exec(t, `ALTER RANGE system CONFIGURE ZONE USING gc.ttlseconds = 1`)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

			var tableID int
			sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables `+
				`WHERE name = 'foo' AND database_name = current_database()`).
				Scan(&tableID)

			ptp := f.Server().DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
			getPtsRec := mkGetPtsRec(t, ptp, f.Server().Clock())
			waitForRecord := mkWaitForRecordCond(t, getPtsRec, mkCheckRecord(t, tableID))
			waitForNoRecord := mkWaitForRecordCond(t, getPtsRec, checkNoRecord)
			waitForBlocked := requestBlockedScan()

			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`)
			defer closeFeed(t, foo)
			{
				// Ensure that there's a protected timestamp on startup that goes
				// away after the initial scan.
				unblock := waitForBlocked()
				require.NotNil(t, getPtsRec())
				unblock()
				assertPayloads(t, foo, []string{
					`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
					`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
					`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
					`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
					`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
				})
				expectResolvedTimestamp(t, foo)
				waitForNoRecord()
			}

			{
				// Ensure that a protected timestamp is created for a backfill due
				// to a schema change and removed after.
				waitForBlocked = requestBlockedScan()
				sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN c INT NOT NULL DEFAULT 1`)
				unblock := waitForBlocked()
				waitForRecord()
				unblock()
				assertPayloads(t, foo, []string{
					`foo: [1]->{"after": {"a": 1, "b": "a", "c": 1}}`,
					`foo: [2]->{"after": {"a": 2, "b": "b", "c": 1}}`,
					`foo: [4]->{"after": {"a": 4, "b": "c", "c": 1}}`,
					`foo: [7]->{"after": {"a": 7, "b": "d", "c": 1}}`,
					`foo: [8]->{"after": {"a": 8, "b": "e", "c": 1}}`,
				})
				expectResolvedTimestamp(t, foo)
				waitForNoRecord()
			}

			{
				// Ensure that the protected timestamp is removed when the job is
				// canceled.
				waitForBlocked = requestBlockedScan()
				sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d INT NOT NULL DEFAULT 2`)
				unblock := waitForBlocked()
				waitForRecord()
				sqlDB.Exec(t, `CANCEL JOB $1`, foo.(cdctest.EnterpriseTestFeed).JobID())
				waitForNoRecord()
				unblock()
			}
		}))
}

func TestChangefeedProtectedTimestampOnPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(shouldPause bool) cdcTestFn {
		return func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

			var tableID int
			sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables `+
				`WHERE name = 'foo' AND database_name = current_database()`).
				Scan(&tableID)
			stmt := `CREATE CHANGEFEED FOR foo WITH resolved`
			if shouldPause {
				stmt += ", " + changefeedbase.OptProtectDataFromGCOnPause
			}
			foo := feed(t, f, stmt)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
				`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
				`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
				`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
				`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
			})
			expectResolvedTimestamp(t, foo)

			// Pause the job then ensure that it has a reasonable protected timestamp.

			ctx := context.Background()
			serverCfg := f.Server().DistSQLServer().(*distsql.ServerImpl).ServerConfig
			jr := serverCfg.JobRegistry
			pts := serverCfg.ProtectedTimestampProvider

			feedJob := foo.(cdctest.EnterpriseTestFeed)
			require.NoError(t, feedJob.Pause())
			{
				j, err := jr.LoadJob(ctx, feedJob.JobID())
				require.NoError(t, err)
				progress := j.Progress()
				details := progress.Details.(*jobspb.Progress_Changefeed).Changefeed
				if shouldPause {
					require.NotEqual(t, uuid.Nil, details.ProtectedTimestampRecord)
					var r *ptpb.Record
					require.NoError(t, serverCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
						r, err = pts.GetRecord(ctx, txn, details.ProtectedTimestampRecord)
						return err
					}))
					require.Equal(t, r.Timestamp, *progress.GetHighWater())
				} else {
					require.Equal(t, uuid.Nil, details.ProtectedTimestampRecord)
				}
			}

			// Resume the job and ensure that the protected timestamp is removed once
			// the changefeed has caught up.
			require.NoError(t, feedJob.Resume())
			testutils.SucceedsSoon(t, func() error {
				expectResolvedTimestamp(t, foo)
				j, err := jr.LoadJob(ctx, feedJob.JobID())
				require.NoError(t, err)
				details := j.Progress().Details.(*jobspb.Progress_Changefeed).Changefeed
				if details.ProtectedTimestampRecord != uuid.Nil {
					return fmt.Errorf("expected no protected timestamp record")
				}
				return nil
			})
		}
	}

	testutils.RunTrueAndFalse(t, "protect_on_pause", func(t *testing.T, shouldPause bool) {
		t.Run(`enterprise`, enterpriseTest(testFn(shouldPause)))
		t.Run(`cloudstorage`, cloudStorageTest(testFn(shouldPause)))
		t.Run(`kafka`, kafkaTest(testFn(shouldPause)))
		t.Run(`webhook`, webhookTest(testFn(shouldPause)))
	})

}

// This test ensures that the changefeed attempts to verify its initial protected
// timestamp record and that when that verification fails, the job is canceled
// and the record removed.
func TestChangefeedProtectedTimestampsVerificationFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	verifyRequestCh := make(chan *roachpb.AdminVerifyProtectedTimestampRequest, 1)
	requestFilter := kvserverbase.ReplicaRequestFilter(func(
		ctx context.Context, ba roachpb.BatchRequest,
	) *roachpb.Error {
		if r, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
			req := r.(*roachpb.AdminVerifyProtectedTimestampRequest)
			verifyRequestCh <- req
			return roachpb.NewError(errors.Errorf("failed to verify protection %v on %v", req.RecordID, ba.RangeID))
		}
		return nil
	})

	setStoreKnobs := func(args *base.TestServerArgs) {
		storeKnobs := &kvserver.StoreTestingKnobs{}
		storeKnobs.TestingRequestFilter = requestFilter
		args.Knobs.Store = storeKnobs
	}

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		_, err := f.Feed(`CREATE CHANGEFEED FOR foo WITH resolved`)
		// Make sure we got the injected error.
		require.Regexp(t, "failed to verify", err)
		// Make sure we tried to verify the request.
		r := <-verifyRequestCh
		cfg := f.Server().ExecutorConfig().(sql.ExecutorConfig)
		kvDB := cfg.DB
		pts := cfg.ProtectedTimestampProvider
		// Make sure that the canceled job gets moved through its OnFailOrCancel
		// phase and removes its protected timestamp.
		testutils.SucceedsSoon(t, func() error {
			err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := pts.GetRecord(ctx, txn, r.RecordID)
				return err
			})
			if err == nil {
				return errors.Errorf("expected record to be removed")
			}
			if errors.Is(err, protectedts.ErrNotExists) {
				return nil
			}
			return err
		})
	}

	t.Run(`enterprise`, enterpriseTestWithServerArgs(setStoreKnobs, testFn))
	t.Run(`cloudstorage`, cloudStorageTestWithServerArg(setStoreKnobs, testFn))
	t.Run(`kafka`, kafkaTestWithServerArgs(setStoreKnobs, testFn))
	t.Run(`webhook`, webhookTestWithServerArgs(setStoreKnobs, testFn))
}

func TestManyChangefeedsOneTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, func(t *testing.T) {
		skip.WithIssue(t, 67034, "flakey test")
		webhookTest(testFn)(t)
	})
}

func TestUnspecifiedPrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	t.Run(`kafka`, kafkaTest(testFn))
}

// TestChangefeedNodeShutdown ensures that an enterprise changefeed continues
// running after the original job-coordinator node is shut down.
func TestChangefeedNodeShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 32232)

	knobs := base.TestingKnobs{
		DistSQL:          &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
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
	f := makeTableFeedFactory(tc.Server(1), tc.ServerConn(0), sink)
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
	defer log.Scope(t).Close(t)

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

// Regression test for #41694.
func TestChangefeedRestartDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(yevgeniy): Rework this test.  It's too brittle.

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

		unblockMessages := func(n int) {
			for i := 0; i < n; i++ {
				beforeEmitRowCh <- nil
			}
		}

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0), (1), (2), (3)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, foo)

		// TODO(dan): At a high level, all we're doing is trying to restart a
		// changefeed in the middle of changefeed backfill after a schema change
		// finishes. It turns out this is pretty hard to do with our current testing
		// knobs and this test ends up being pretty brittle. I'd love it if anyone
		// thought of a better way to do this.

		// Read the initial data in the rows.
		unblockMessages(4)
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0}, "before": null}`,
			`foo: [1]->{"after": {"a": 1}, "before": null}`,
			`foo: [2]->{"after": {"a": 2}, "before": null}`,
			`foo: [3]->{"after": {"a": 3}, "before": null}`,
		})

		// Run a schema change that backfills kvs.
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'backfill'`)

		// Unblock emit for each kv written by the schema change's backfill. The
		// changefeed actually emits these, but we lose it to overaggressive
		// duplicate detection in tableFeed.
		// TODO(dan): Track duplicates more precisely in tableFeed.
		unblockMessages(4)

		// Unblock the emit for *all but one* of the rows emitted by the changefeed
		// backfill (run after the schema change completes and the final table
		// descriptor is written). The reason this test has 4 rows is because the
		// `sqlSink` that powers `tableFeed` only flushes after it has 3 rows, so we
		// need 1 more than that to guarantee that this first one gets flushed.
		for i := 0; i < 3; i++ {
			beforeEmitRowCh <- nil
		}
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0}, "before": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}, "before": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}, "before": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}, "before": {"a": 3}}`,
			`foo: [0]->{"after": {"a": 0, "b": "backfill"}, "before": {"a": 0}}`,
		})

		// `foo: [0]->{"after": {"a": 0, "b": "backfill"}, "before": {"a": 0}}`,
		feedJob := foo.(cdctest.EnterpriseTestFeed)
		require.NoError(t, feedJob.Pause())

		// Make extra sure that the zombie changefeed can't write any more data.
		beforeEmitRowCh <- changefeedbase.MarkRetryableError(errors.New(`nope don't write it`))

		// Insert some data that we should only see out of the changefeed after it
		// re-runs the backfill.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (6, 'bar')`)

		// Unblock all later emits, we don't need this control anymore.
		close(beforeEmitRowCh)

		// Resume the changefeed and the backfill should start up again. Currently
		// this does the entire backfill again, you could imagine in the future that
		// we do some sort of backfill checkpointing and start the backfill up from
		// the last checkpoint.
		require.NoError(t, feedJob.Resume())
		assertPayloads(t, foo, []string{
			// The changefeed actually emits this row, but we lose it to
			// overaggressive duplicate detection in tableFeed.
			// TODO(dan): Track duplicates more precisely in sinklessFeed/tableFeed.
			// `foo: [0]->{"after": {"a": 0, "b": "backfill"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "backfill"}, "before": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2, "b": "backfill"}, "before": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3, "b": "backfill"}, "before": {"a": 3}}`,
		})

		assertPayloads(t, foo, []string{
			`foo: [6]->{"after": {"a": 6, "b": "bar"}, "before": null}`,
		})
	}

	t.Run(`kafka`, kafkaTest(testFn))
}

func TestChangefeedHandlesDrainingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "Takes too long with race enabled")

	shouldDrain := true
	knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{
		DrainFast:  true,
		Changefeed: &TestingKnobs{},
		Flowinfra: &flowinfra.TestingKnobs{
			FlowRegistryDraining: func() bool {
				if shouldDrain {
					shouldDrain = false
					return true
				}
				return false
			},
		},
	}}

	sinkDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	tc := serverutils.StartNewTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase:   "test",
			Knobs:         knobs,
			ExternalIODir: sinkDir,
		}})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)

	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		10,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)

	// Introduce 4 splits to get 5 ranges.  We need multiple ranges in order to run distributed
	// flow.
	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT (SELECT i*2 FROM generate_series(1, 4) AS g(i))")
	sqlDB.Exec(t, "ALTER TABLE test.foo SCATTER")

	// Create a factory which executes the CREATE CHANGEFEED statement on server 0.
	// This statement should fail, but the job itself ought to be creaated.
	// After some time, that job should be adopted by another node, and executed successfully.
	f := makeCloudFeedFactory(tc.Server(1), tc.ServerConn(0), sinkDir)

	feed := feed(t, f, "CREATE CHANGEFEED FOR foo")
	defer closeFeed(t, feed)

	// At this point, the job created by feed will fail to start running on node 0 due to draining
	// registry.  However, this job will be retried, and it should succeeded.
	// Note: This test is a bit unrealistic in that if the registry is draining, that
	// means that the server is draining (i.e being shut down).  We don't do a full shutdown
	// here, but we are simulating a restart by failing to start a flow the first time around.
	assertPayloads(t, feed, []string{
		`foo: [1]->{"after": {"k": 1, "v": 1}}`,
		`foo: [2]->{"after": {"k": 2, "v": 0}}`,
		`foo: [3]->{"after": {"k": 3, "v": 1}}`,
		`foo: [4]->{"after": {"k": 4, "v": 0}}`,
		`foo: [5]->{"after": {"k": 5, "v": 1}}`,
		`foo: [6]->{"after": {"k": 6, "v": 0}}`,
		`foo: [7]->{"after": {"k": 7, "v": 1}}`,
		`foo: [8]->{"after": {"k": 8, "v": 0}}`,
		`foo: [9]->{"after": {"k": 9, "v": 1}}`,
		`foo: [10]->{"after": {"k": 10, "v": 0}}`,
	})
}

// Primary key changes are supported by changefeeds starting in 21.1. This tests
// that basic behavior works.
func TestChangefeedPrimaryKeyChangeWorks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		const baseStmt = `CREATE CHANGEFEED FOR foo WITH resolved = '100ms'`
		foo := feed(t, f, baseStmt)
		defer closeFeed(t, foo)

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
		if strings.HasSuffix(t.Name(), "sinkless") {
			maybeHandleRestart = func(t *testing.T) func() {
				var resolved hlc.Timestamp
				for {
					m, err := foo.Next()
					if err != nil {
						assert.Contains(t, err.Error(),
							fmt.Sprintf("schema change occurred at %s", resolved.Next().AsOfSystemTime()))
						break
					}
					resolved = extractResolvedTimestamp(t, m)
				}
				const restartStmt = baseStmt + ", cursor = $1"
				foo = feed(t, f, restartStmt, resolved.AsOfSystemTime())
				return func() {
					closeFeed(t, foo)
				}
			}
		}

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

		sqlDB.Exec(t, `ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b)`)
		defer maybeHandleRestart(t)()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3, 'c'), (4, 'd')`)
		assertPayloads(t, foo, []string{
			`foo: ["c"]->{"after": {"a": 3, "b": "c"}}`,
			`foo: ["d"]->{"after": {"a": 4, "b": "d"}}`,
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
			`foo: ["a"]->{"after": {"a": 6, "b": "a"}}`,
			`foo: ["e"]->{"after": {"a": 5, "b": "e"}}`,
		})
		defer maybeHandleRestart(t)()
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "f"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
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

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
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
		if strings.HasSuffix(t.Name(), "sinkless") {
			maybeHandleRestart = func(t *testing.T) func() {
				var resolvedTS hlc.Timestamp
				for {
					m, err := cf.Next()
					if err != nil {
						assert.Contains(t, err.Error(), fmt.Sprintf("schema change occurred at %s", resolvedTS.Next().AsOfSystemTime()))
						break
					}
					resolvedTS = extractResolvedTimestamp(t, m)
				}
				const restartStmt = baseStmt + ", cursor = $1"
				cf = feed(t, f, restartStmt, resolvedTS.AsOfSystemTime())
				return func() {
					closeFeed(t, cf)
				}
			}
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

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`cloudstorage`, cloudStorageTest(testFn))
	t.Run(`kafka`, kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

// Primary key changes are supported by changefeeds starting in 21.1.
func TestChangefeedPrimaryKeyChangeMixedVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// 'initial' is skipped because only the latest value ('updated') is
		// emitted by the initial scan.
		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})

		// Expect to see an error because primary key changes are not supported
		// in the mixed version state.
		sqlDB.Exec(t, `ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b)`)
		_, err := foo.Next()
		require.Regexp(t, `primary key change occurred`, err)
	}

	setMixedVersion := func(args *base.TestServerArgs) {
		args.Knobs.Server = &server.TestingKnobs{
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V20_2),
			DisableAutomaticVersionUpgrade: 1,
		}
	}

	t.Run("enterprise", enterpriseTestWithServerArgs(setMixedVersion, testFn))
	t.Run("cloudstorage", cloudStorageTestWithServerArg(setMixedVersion, testFn))
	t.Run("kafka", kafkaTestWithServerArgs(setMixedVersion, testFn))
	t.Run("webhook", webhookTestWithServerArgs(setMixedVersion, testFn))
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

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'initial')`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH resolved = '100ms', updated`)

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

		require.NoError(t, crdb.ExecuteTx(context.Background(), db, nil, func(tx *gosql.Tx) error {
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
		msgs, err := readNextMessages(foo, len(expected), false)
		require.NoError(t, err)

		// Sort the messages by their timestamp.
		re := regexp.MustCompile(`.*(, "updated": "(\d+\.\d+)")}.*`)
		getHLC := func(i int) string { return re.FindStringSubmatch(msgs[i])[2] }
		trimHlC := func(s string) string {
			indexes := re.FindStringSubmatchIndex(s)
			return s[:indexes[2]] + s[indexes[3]:]
		}
		sort.Slice(msgs, func(i, j int) bool {
			a, b := getHLC(i), getHLC(j)
			if a == b {
				return msgs[i] < msgs[j]
			}
			return a < b
		})
		schemaChangeTS := getHLC(0)
		stripped := make([]string, len(msgs))
		for i, m := range msgs {
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
			assertPayloads(t, foo, msgs[5:])
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
			assertPayloads(t, foo, msgs)
			// Make sure there are no more messages.
			{
				next, err := foo.Next()
				require.NoError(t, err)
				require.NotNil(t, next.Resolved)
			}
		})

	}

	t.Run("enterprise", enterpriseTest(testFn))
	t.Run("cloudstorage", cloudStorageTest(testFn))
	t.Run("kafka", kafkaTest(testFn))
	t.Run(`webhook`, webhookTest(testFn))
}

func TestChangefeedBackfillCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	rnd, _ := randutil.NewPseudoRand()

	var maxCheckopointSize int64
	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 1000)`)

		fooDesc := catalogkv.TestingGetTableDescriptor(
			f.Server().DB(), keys.SystemSQLCodec, "d", "foo")
		tableSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

		knobs := f.Server().(*server.TestServer).Cfg.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Make it appear as if this table has many ranges.  We do this by limiting the number
		// of keys returned by Scan request.
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(100)
		}

		// Emit resolved events for majority of spans.  Be extra paranoid and ensure that
		// we have at least 1 span for which we don't emit resolved timestamp (to force checkpointing).
		haveGaps := false
		knobs.ShouldSkipResolved = func(r *jobspb.ResolvedSpan) bool {
			if r.Span.Equal(tableSpan) {
				// Do not emit resolved events for the entire table span.
				// We "simulate" large table by splitting single table span into many parts, so
				// we want to resolve those sub-spans instead of the entire table span.
				// However, we have to emit something -- otherwise the entire changefeed
				// machine would not work.
				r.Span.EndKey = tableSpan.Key.Next()
				return false
			}
			if haveGaps {
				return rnd.Intn(10) > 7
			}
			haveGaps = true
			return true
		}

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.FrontierCheckpointFrequency.Override(
			context.Background(), &f.Server().ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.FrontierCheckpointMaxBytes.Override(
			context.Background(), &f.Server().ClusterSettings().SV, maxCheckopointSize)

		registry := f.Server().JobRegistry().(*jobs.Registry)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='100ms'`)
		// Some test feeds (kafka) are not buffered, so we have to consume messages.
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				_, err := foo.Next()
				if err != nil {
					return err
				}
			}
		})
		defer func() {
			closeFeed(t, foo)
			_ = g.Wait()
		}()

		jobFeed := foo.(cdctest.EnterpriseTestFeed)
		loadProgress := func() jobspb.Progress {
			jobID := jobFeed.JobID()
			job, err := registry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			return job.Progress()
		}

		// Wait for non-nil checkpoint.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if p := progress.GetChangefeed(); p != nil && p.Checkpoint != nil && len(p.Checkpoint.Spans) > 0 {
				return nil
			}
			return errors.New("waiting for checkpoint")
		})

		// Pause the job and read and verify the latest checkpoint information.
		require.NoError(t, jobFeed.Pause())
		progress := loadProgress()
		require.NotNil(t, progress.GetChangefeed())
		h := progress.GetHighWater()
		noHighWater := (h == nil || h.IsEmpty())
		require.True(t, noHighWater)

		jobCheckpoint := progress.GetChangefeed().Checkpoint
		require.Less(t, 0, len(jobCheckpoint.Spans))
		var checkpoint roachpb.SpanGroup
		checkpoint.Add(jobCheckpoint.Spans...)

		// Collect spans we attempt to resolve after when we resume.
		var resolved []roachpb.Span
		knobs.ShouldSkipResolved = func(r *jobspb.ResolvedSpan) bool {
			if !r.Span.Equal(tableSpan) {
				resolved = append(resolved, r.Span)
			}
			return false
		}

		// Resume job.
		require.NoError(t, jobFeed.Resume())

		// Wait for the high water mark to be non-zero.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if p := progress.GetHighWater(); p != nil && !p.IsEmpty() {
				return nil
			}
			return errors.New("waiting for highwater")
		})

		// At this point, highwater mark should be set, and previous checkpoint should be gone.
		progress = loadProgress()
		require.NotNil(t, progress.GetChangefeed())
		require.Equal(t, 0, len(progress.GetChangefeed().Checkpoint.Spans))

		// Verify that none of the resolved spans after resume were checkpointed.
		for _, sp := range resolved {
			require.Falsef(t, checkpoint.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}
	}

	for _, sz := range []int64{100 << 20, 100} {
		maxCheckopointSize = sz
		t.Run(fmt.Sprintf("enterprise-limit=%s", humanize.Bytes(uint64(sz))), enterpriseTest(testFn))
		t.Run(fmt.Sprintf("cloudstorage-limit=%s", humanize.Bytes(uint64(sz))), cloudStorageTest(testFn))
		t.Run(fmt.Sprintf("kafka-limit=%s", humanize.Bytes(uint64(sz))), kafkaTest(testFn))
	}
}
