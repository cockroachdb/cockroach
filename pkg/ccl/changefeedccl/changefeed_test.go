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
	"encoding/base64"
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

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // multi-tenant tests
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"    // locality-related table mutations
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // registers cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testServerRegion = "us-east-1"

func TestChangefeedReplanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "multinode setup doesn't work under testrace")

	assertReplanCounter := func(t *testing.T, m *Metrics, exp int64) {
		t.Helper()
		// If this changefeed is running as a job, we anticipate that it will move
		// through the failed state and will increment the metric. Sinkless feeds
		// don't contribute to the failures counter.
		if strings.Contains(t.Name(), `sinkless`) {
			return
		}
		testutils.SucceedsSoon(t, func() error {
			if got := m.ReplanCount.Count(); got != exp {
				return errors.Errorf("expected %d failures, got %d", exp, got)
			}
			return nil
		})
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()

		numNodes := 3
		errChan := make(chan error, 1)
		readyChan := make(chan struct{})
		defaultServerArgs := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					Changefeed: &TestingKnobs{
						HandleDistChangefeedError: func(err error) error {
							select {
							case errChan <- err:
								return err
							default:
								return nil
							}
						},
						ShouldReplan: func(ctx context.Context, oldPlan, newPlan *sql.PhysicalPlan) bool {
							select {
							case <-readyChan:
								return true
							default:
								return false
							}
						},
					},
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			UseDatabase:              "d",
			DisableDefaultTestTenant: true,
		}

		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ServerArgs: defaultServerArgs,
		})
		defer tc.Stopper().Stop(ctx)

		registry := tc.Server(0).JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)

		db := tc.ServerConn(0)
		serverutils.SetClusterSetting(t, tc, "changefeed.replan_flow_frequency", time.Millisecond*100)

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0);`)

		feedFactory := makeKafkaFeedFactory(tc, db)

		cf := feed(t, feedFactory, "CREATE CHANGEFEED FOR d.foo")
		defer closeFeed(t, cf)

		feed, ok := cf.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.TickHighWaterMark(tc.Server(0).Clock().Now()))

		sqlDB.ExecMultiple(t,
			`INSERT INTO foo (a) SELECT * FROM generate_series(1, 1000);`,
			`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 1000, 50));`,
			`ALTER TABLE foo SCATTER;`,
		)

		timeout := 20 * time.Second
		if util.RaceEnabled {
			timeout *= 3
		}

		readyChan <- struct{}{}

		select {
		case err := <-errChan:
			require.Regexp(t, "physical plan has changed", err)
			assertReplanCounter(t, metrics, 1)
			log.Info(ctx, "replan triggered")
		case <-time.After(timeout):
			t.Fatal("expected distflow to error but hasn't after 20 seconds")
		}
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

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

		sqlDB.ExpectErr(t, `unknown envelope: lobster`,
			`SELECT crdb_internal.to_json_as_changefeed_with_flags(foo.*, 'updated', 'envelope=lobster') from foo`)
	}

	cdcTest(t, testFn)
}

func TestChangefeedIdleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		changefeedbase.IdleTimeout.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 3*time.Second)

		// Idleness functionality is version gated
		knobs := s.TestingKnobs.Server.(*server.TestingKnobs)
		knobs.BinaryVersionOverride = clusterversion.ByKey(clusterversion.TODOPreV22_1)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		currentlyIdle := registry.MetricsStruct().JobMetrics[jobspb.TypeChangefeed].CurrentlyIdle
		waitForIdleCount := func(numIdle int64) {
			testutils.SucceedsSoon(t, func() error {
				if currentlyIdle.Value() != numIdle {
					return fmt.Errorf("expected (%+v) idle changefeeds, found (%+v)", numIdle, currentlyIdle.Value())
				}
				return nil
			})
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (b INT PRIMARY KEY)`)
		cf1 := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo WITH resolved='10ms'") // higher resolved frequency for faster test
		cf2 := feed(t, f, "CREATE CHANGEFEED FOR TABLE bar WITH resolved='10ms'")
		defer closeFeed(t, cf1)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0)`)
		waitForIdleCount(0)
		waitForIdleCount(2) // Both should eventually be considered idle

		jobFeed := cf2.(cdctest.EnterpriseTestFeed)
		require.NoError(t, jobFeed.Pause())
		waitForIdleCount(1) // Paused jobs aren't considered idle

		require.NoError(t, jobFeed.Resume())
		waitForIdleCount(2) // Resumed job should eventually become idle

		closeFeed(t, cf2)
		waitForIdleCount(1) // The cancelled changefeed isn't considered idle

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		waitForIdleCount(0)
		waitForIdleCount(1)

		assertPayloads(t, cf1, []string{
			`foo: [0]->{"after": {"a": 0}}`,
			`foo: [1]->{"after": {"a": 1}}`,
		})
	}, feedTestEnterpriseSinks)
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
	}, feedTestEnterpriseSinks)
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

func TestChangefeedTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kvServer, kvSQLdb, cleanup := startTestFullServer(t, feedTestOptions{argsFn: func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	}})
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
	tenantSQL.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)
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
		sink, cleanup := sqlutils.PGUrl(t, sqlAddr, t.Name(), url.User(username.RootUser))
		defer cleanup()

		// kvServer is used here because we require a
		// TestServerInterface implementor. It is only used as
		// the return value for f.Server()
		f := makeSinklessFeedFactory(kvServer, sink, nil)
		tenantSQL.Exec(t, `INSERT INTO foo_in_tenant VALUES (1)`)
		feed := feed(t, f, `CREATE CHANGEFEED FOR foo_in_tenant`)
		assertPayloads(t, feed, []string{
			`foo_in_tenant: [1]->{"after": {"pk": 1}}`,
		})
	})
}

func TestMissingTableErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, cleanup := makeServer(t)
	defer cleanup()

	t.Run("changefeed on non existing table fails", func(t *testing.T) {
		kvSQL := sqlutils.MakeSQLRunner(s.DB)
		kvSQL.ExpectErr(t, `^pq: failed to resolve targets in the CHANGEFEED stmt: table "foo" does not exist$`,
			`CREATE CHANGEFEED FOR foo`,
		)
	})
}

func TestChangefeedTenantsExternalIOEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, cleanup := makeTenantServer(t, withArgsFn(func(args *base.TestServerArgs) {
		args.ExternalIODirConfig.DisableOutbound = true
	}))
	defer cleanup()

	tenantSQL := sqlutils.MakeSQLRunner(s.DB)
	tenantSQL.Exec(t, `CREATE TABLE foo_in_tenant (pk INT PRIMARY KEY)`)

	t.Run("sinkful changefeed works", func(t *testing.T) {
		f, cleanup := makeFeedFactory(t, "kafka", s.Server, s.DB)
		defer cleanup()
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

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		// To make sure that these timestamps are after 'before' and before
		// 'after', throw a couple sleeps around them. We round timestamps to
		// Microsecond granularity for Postgres compatibility, so make the
		// sleeps 10x that.
		beforeInsert := s.Server.Clock().Now()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
		time.Sleep(10 * time.Microsecond)

		var tsLogical string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)
		var tsClock time.Time
		sqlDB.QueryRow(t, `SELECT clock_timestamp()`).Scan(&tsClock)

		time.Sleep(10 * time.Microsecond)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

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
				sqlDB.Exec(t, `INSERT INTO no_initial_scan VALUES (1)`)

				noInitialScan := feed(t, f, changefeedStmt)
				defer func() {
					closeFeed(t, noInitialScan)
					sqlDB.Exec(t, `DROP TABLE no_initial_scan`)
				}()

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
				sqlDB.Exec(t, `INSERT INTO initial_scan VALUES (1), (2), (3)`)
				var tsStr string
				var i int
				sqlDB.QueryRow(t, `SELECT count(*), cluster_logical_timestamp() from initial_scan`).Scan(&i, &tsStr)
				initialScan := feed(t, f, fmt.Sprintf(changefeedStmtFormat, tsStr))
				defer func() {
					closeFeed(t, initialScan)
					sqlDB.Exec(t, `DROP TABLE initial_scan`)
				}()
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

func TestChangefeedBackfillObservability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sli, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		pendingRanges := sli.BackfillPendingRanges

		// Create a table with multiple ranges
		numRanges := 10
		rowsPerRange := 20
		sqlDB.Exec(t, fmt.Sprintf(`
  CREATE TABLE foo (key INT PRIMARY KEY);
  INSERT INTO foo (key) SELECT * FROM generate_series(1, %d);
  ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(%d, %d, %d));
  `, numRanges*rowsPerRange, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange))
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM [SHOW RANGES FROM TABLE foo]`,
			[][]string{{fmt.Sprint(numRanges)}},
		)

		// Allow control of the scans
		scanCtx, scanCancel := context.WithCancel(context.Background())
		scanChan := make(chan struct{})
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			select {
			case <-scanCtx.Done():
				return scanCtx.Err()
			case <-scanChan:
				return nil
			}
		}

		require.Equal(t, pendingRanges.Value(), int64(0))
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// Progress the initial backfill halfway through its ranges
		for i := 0; i < numRanges/2; i++ {
			scanChan <- struct{}{}
		}
		testutils.SucceedsSoon(t, func() error {
			count := pendingRanges.Value()
			if count != int64(numRanges/2) {
				return fmt.Errorf("range count %d should be %d", count, numRanges/2)
			}
			return nil
		})

		// Ensure that the pending count is cleared if the backfill completes
		// regardless of successful scans
		scanCancel()
		testutils.SucceedsSoon(t, func() error {
			count := pendingRanges.Value()
			if count > 0 {
				return fmt.Errorf("range count %d should be 0", count)
			}
			return nil
		})
	}

	// Can't run on tenants due to lack of SPLIT AT support (#54254)
	cdcTest(t, testFn, feedTestNoTenants, feedTestEnterpriseSinks)
}

func TestChangefeedUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)
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
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c WITH schema_change_policy='stop'`)
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
		if _, err := cf.Next(); !testutils.IsError(err, `schema change occurred at`) {
			require.Regexp(t, `expected "schema change occurred at ..." got: %+v`, err)
		}
	}

	cdcTest(t, testFn)
}

func TestChangefeedProjectionDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (id int primary key, a string)`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'a')`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT * FROM foo`)
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: [0]->{"a": "a", "id": 0}`,
		})
		sqlDB.Exec(t, `DELETE FROM foo WHERE id = 0`)
		assertPayloads(t, foo, []string{
			`foo: [0]->{}`,
		})
	}
	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

// If we drop columns which are not targeted by the changefeed, it should not backfill.
func TestNoBackfillAfterNonTargetColumnDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c`)
		defer closeFeed(t, cf)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b", "c": "c"}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN a`)
		sqlDB.Exec(t, `INSERT INTO hasfams VALUES (1, 'b1', 'c1')`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [1]->{"after": {"b": "b1", "c": "c1"}}`,
		})

		// Check that dropping a watched column still backfills.
		sqlDB.Exec(t, `ALTER TABLE hasfams DROP COLUMN c`)
		assertPayloads(t, cf, []string{
			`hasfams.b_and_c: [0]->{"after": {"b": "b"}}`,
			`hasfams.b_and_c: [1]->{"after": {"b": "b1"}}`,
		})
	}

	cdcTest(t, testFn)
}

func TestChangefeedColumnDropsWithFamilyAndNonFamilyTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `CREATE TABLE nofams (id int primary key, a string, b string, c string)`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)
		sqlDB.Exec(t, `INSERT INTO nofams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c, TABLE nofams`)
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
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `CREATE TABLE alsohasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)
		sqlDB.Exec(t, `INSERT INTO alsohasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c, TABLE alsohasfams FAMILY id_a`)
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
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id int primary key, a string, b string, c string, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY id_a, TABLE hasfams FAMILY b_and_c`)
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

	cdcTest(t, testFn)
}

func TestNoStopAfterNonTargetAddColumnWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE hasfams (id INT PRIMARY KEY, a STRING, b STRING, c STRING, FAMILY id_a (id, a), FAMILY b_and_c (b, c))`)
		sqlDB.Exec(t, `INSERT INTO hasfams values (0, 'a', 'b', 'c')`)

		// Open up the changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR TABLE hasfams FAMILY b_and_c WITH schema_change_policy='stop'`)

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

	cdcTest(t, testFn)
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
		sqlDB.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)
		sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
		for _, proto := range disallowedSinkProtos {
			sqlDB.ExpectErr(t, "Outbound IO is disabled by configuration, cannot create changefeed",
				"CREATE CHANGEFEED FOR target_table INTO $1",
				fmt.Sprintf("%s://does-not-matter", proto),
			)
		}
	})

	withDisabledOutbound := func(args *base.TestServerArgs) { args.ExternalIODirConfig.DisableOutbound = true }
	cdcTestNamed(t, "sinkless changfeeds are allowed with disabled external io", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "CREATE TABLE target_table (pk INT PRIMARY KEY)")
		sqlDB.Exec(t, "INSERT INTO target_table VALUES (1)")
		feed := feed(t, f, "CREATE CHANGEFEED FOR target_table")
		defer closeFeed(t, feed)
		assertPayloads(t, feed, []string{
			`target_table: [1]->{"after": {"pk": 1}}`,
		})
	}, feedTestForceSink("sinkless"), withArgsFn(withDisabledOutbound))
}

// Test how Changefeeds react to schema changes that do not require a backfill
// operation.
func TestChangefeedSchemaChangeNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

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

// Test checkpointing when the highwater does not move due to some issues with
// specific spans lagging behind
func TestChangefeedLaggingSpanCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rnd, _ := randutil.NewPseudoRand()

	s, db, stopServer := startTestFullServer(t, feedTestOptions{})
	defer stopServer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	knobs := s.(*server.TestServer).Cfg.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)

	// Initialize table with multiple ranges.
	sqlDB.Exec(t, `
  CREATE TABLE foo (key INT PRIMARY KEY);
  INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000);
  ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 1000, 50));
  `)

	// Checkpoint progress frequently, allow a large enough checkpoint, and
	// reduce the lag threshold to allow lag checkpointing to trigger
	changefeedbase.FrontierCheckpointFrequency.Override(
		context.Background(), &s.ClusterSettings().SV, 10*time.Millisecond)
	changefeedbase.FrontierCheckpointMaxBytes.Override(
		context.Background(), &s.ClusterSettings().SV, 100<<20)
	changefeedbase.FrontierHighwaterLagCheckpointThreshold.Override(
		context.Background(), &s.ClusterSettings().SV, 10*time.Millisecond)

	// We'll start changefeed with the cursor.
	var tsStr string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp() from foo`).Scan(&tsStr)
	cursor := parseTimeToHLC(t, tsStr)

	// Rangefeed will skip some of the checkpoints to simulate lagging spans.
	var laggingSpans roachpb.SpanGroup
	numLagging := 0
	knobs.FeedKnobs.ShouldSkipCheckpoint = func(checkpoint *roachpb.RangeFeedCheckpoint) bool {
		// Skip spans that were skipped before; otherwise skip some spans.
		seenBefore := laggingSpans.Encloses(checkpoint.Span)
		if seenBefore || (numLagging < 5 && rnd.Int()%3 == 0) {
			if !seenBefore {
				laggingSpans.Add(checkpoint.Span)
				numLagging++
			}
			return true /* skip */
		}
		return false
	}

	var jobID jobspb.JobID
	sqlDB.QueryRow(t,
		`CREATE CHANGEFEED FOR foo INTO 'null://' WITH resolved='50ms', no_initial_scan, cursor=$1`, tsStr,
	).Scan(&jobID)

	// Helper to read job progress
	jobRegistry := s.JobRegistry().(*jobs.Registry)
	loadProgress := func() jobspb.Progress {
		job, err := jobRegistry.LoadJob(context.Background(), jobID)
		require.NoError(t, err)
		return job.Progress()
	}

	// Should eventually checkpoint all spans around the lagging span
	testutils.SucceedsSoon(t, func() error {
		progress := loadProgress()
		if p := progress.GetChangefeed(); p != nil && p.Checkpoint != nil && !p.Checkpoint.Timestamp.IsEmpty() {
			return nil
		}
		return errors.New("waiting for checkpoint")
	})

	sqlDB.Exec(t, "PAUSE JOB $1", jobID)
	waitForJobStatus(sqlDB, t, jobID, jobs.StatusPaused)

	// We expect highwater to be 0 (because we skipped some spans) or exactly cursor
	// (this is mostly due to racy updates sent from aggregators to the frontier.
	// However, the checkpoint timestamp should be at least at the cursor.
	progress := loadProgress()
	require.True(t, progress.GetHighWater().IsEmpty() || progress.GetHighWater().EqOrdering(cursor),
		"expected empty highwater or %s,  found %s", cursor, progress.GetHighWater())
	require.NotNil(t, progress.GetChangefeed().Checkpoint)
	require.Less(t, 0, len(progress.GetChangefeed().Checkpoint.Spans))
	checkpointTS := progress.GetChangefeed().Checkpoint.Timestamp
	require.True(t, cursor.LessEq(checkpointTS))

	var incorrectCheckpointErr error
	knobs.FeedKnobs.OnRangeFeedStart = func(spans []kvcoord.SpanTimePair) {
		setErr := func(stp kvcoord.SpanTimePair, expectedTS hlc.Timestamp) {
			incorrectCheckpointErr = errors.Newf(
				"rangefeed for span %s expected to start @%s, started @%s instead",
				stp.Span, expectedTS, stp.StartAfter)
		}

		for _, sp := range spans {
			if laggingSpans.Encloses(sp.Span) {
				if !sp.StartAfter.Equal(cursor) {
					setErr(sp, cursor)
				}
			} else {
				if !sp.StartAfter.Equal(checkpointTS) {
					setErr(sp, checkpointTS)
				}
			}
		}
	}

	sqlDB.Exec(t, "RESUME JOB $1", jobID)
	waitForJobStatus(sqlDB, t, jobID, jobs.StatusRunning)

	// Wait until highwater advances past cursor.
	testutils.SucceedsSoon(t, func() error {
		progress := loadProgress()
		if hw := progress.GetHighWater(); hw != nil && cursor.LessEq(*hw) {
			return nil
		}
		return errors.New("waiting for checkpoint advance")
	})

	sqlDB.Exec(t, "PAUSE JOB $1", jobID)
	waitForJobStatus(sqlDB, t, jobID, jobs.StatusPaused)
	// Verify we didn't see incorrect timestamps when resuming.
	require.NoError(t, incorrectCheckpointErr)
}

// Test checkpointing during schema change backfills that can be paused and
// resumed multiple times during execution
func TestChangefeedSchemaChangeBackfillCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rnd, _ := randutil.NewPseudoRand()

	// This test asserts that a second checkpoint made after resumption does its
	// best to not lose information from the first checkpoint, therefore the
	// maxCheckpointSize should be large enough to hold both without any
	// truncation
	var maxCheckpointSize int64 = 100 << 20

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Initialize table
		sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 1000)`)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 10
			return nil
		}

		// Setup changefeed job details, avoid relying on initial scan functionality
		baseFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='100ms', no_initial_scan`)
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
				return nil
			}
			return errors.New("waiting for highwater")
		})

		// Pause job and setup overrides to force a checkpoint
		require.NoError(t, jobFeed.Pause())

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.FrontierCheckpointFrequency.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.FrontierCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		// Note the tableSpan to avoid resolved events that leave no gaps
		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")
		tableSpan := fooDesc.PrimaryIndexSpan(s.Codec)

		// FilterSpanWithMutation should ensure that once the backfill begins, the following resolved events
		// that are for that backfill (are of the timestamp right after the backfill timestamp) resolve some
		// but not all of the time, which results in a checkpoint eventually being created
		haveGaps := false
		var backfillTimestamp hlc.Timestamp
		var initialCheckpoint roachpb.SpanGroup
		var foundCheckpoint int32
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) bool {
			// Stop resolving anything after checkpoint set to avoid eventually resolving the full span
			if initialCheckpoint.Len() > 0 {
				return true
			}

			// A backfill begins when the backfill resolved event arrives, which has a
			// timestamp such that all backfill spans have a timestamp of
			// timestamp.Next()
			if r.BoundaryType == jobspb.ResolvedSpan_BACKFILL {
				backfillTimestamp = r.Timestamp
				return false
			}

			// Check if we've set a checkpoint yet
			progress := loadProgress()
			if p := progress.GetChangefeed(); p != nil && p.Checkpoint != nil && len(p.Checkpoint.Spans) > 0 {
				// Checkpoint timestamp should be the timestamp of the spans from the backfill
				require.True(t, p.Checkpoint.Timestamp.Equal(backfillTimestamp.Next()))
				initialCheckpoint.Add(p.Checkpoint.Spans...)
				atomic.StoreInt32(&foundCheckpoint, 1)
			}

			// Filter non-backfill-related spans
			if !r.Timestamp.Equal(backfillTimestamp.Next()) {
				// Only allow spans prior to a valid backfillTimestamp to avoid moving past the backfill
				return !(backfillTimestamp.IsEmpty() || r.Timestamp.LessEq(backfillTimestamp.Next()))
			}

			// Only allow resolving if we definitely won't have a completely resolved table
			if !r.Span.Equal(tableSpan) && haveGaps {
				return rnd.Intn(10) > 7
			}
			haveGaps = true
			return true
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
		haveGaps = false
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) bool {
			// Stop resolving anything after second checkpoint set to avoid backfill completion
			if secondCheckpoint.Len() > 0 {
				return true
			}

			// Once we've set a checkpoint that covers new spans, record it
			progress := loadProgress()
			if p := progress.GetChangefeed(); p != nil && p.Checkpoint != nil {
				var currentCheckpoint roachpb.SpanGroup
				currentCheckpoint.Add(p.Checkpoint.Spans...)

				// Ensure that the second checkpoint both contains all spans in the first checkpoint as well as new spans
				if currentCheckpoint.Encloses(initialCheckpoint.Slice()...) && !initialCheckpoint.Encloses(currentCheckpoint.Slice()...) {
					secondCheckpoint = currentCheckpoint
					atomic.StoreInt32(&foundCheckpoint, 1)
				}
			}

			// Filter non-backfill-related spans
			if !r.Timestamp.Equal(backfillTimestamp.Next()) {
				// Only allow spans prior to a valid backfillTimestamp to avoid moving past the backfill
				return !(backfillTimestamp.IsEmpty() || r.Timestamp.LessEq(backfillTimestamp.Next()))
			}

			require.Falsef(t, initialCheckpoint.Encloses(r.Span), "second backfill should not resolve checkpointed span")

			// Only allow resolving if we definitely won't have a completely resolved table
			if !r.Span.Equal(tableSpan) && haveGaps {
				return rnd.Intn(10) > 7
			}
			haveGaps = true
			return true
		}

		require.NoError(t, jobFeed.Resume())
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&foundCheckpoint) != 0 {
				return nil
			}
			return errors.New("waiting for second checkpoint")
		})

		require.NoError(t, jobFeed.Pause())
		for _, span := range initialCheckpoint.Slice() {
			require.Truef(t, secondCheckpoint.Contains(span.Key), "second checkpoint should contain all values in first checkpoint")
		}

		// Collect spans we attempt to resolve after when we resume.
		var resolved []roachpb.Span
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) bool {
			resolved = append(resolved, r.Span)
			return false
		}

		// Resume job.
		require.NoError(t, jobFeed.Resume())

		// checkpoint should eventually be gone once backfill completes.
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress()
			if p := progress.GetChangefeed(); p == nil || p.Checkpoint == nil || len(p.Checkpoint.Spans) == 0 {
				return nil
			}
			return errors.New("checkpoint still non-empty")
		})

		// Pause job to avoid race on the resolved array
		require.NoError(t, jobFeed.Pause())

		// Verify that none of the resolved spans after resume were checkpointed.
		for _, sp := range resolved {
			require.Falsef(t, !sp.Equal(tableSpan) && secondCheckpoint.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)

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

// Test schema changes that require a backfill when the backfill option is
// allowed.
func TestChangefeedSchemaChangeAllowBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

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
			ts := fetchDescVersionModificationTime(t, s, `add_column_def`, 4)
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
			ts := fetchDescVersionModificationTime(t, s, `add_col_comp`, 4)
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
			ts := fetchDescVersionModificationTime(t, s, `multiple_alters`, 10)
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

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

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
			ts := fetchDescVersionModificationTime(t, s, `add_column_def`, 4)
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

	cdcTestWithSystem(t, testFn)
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
	t testing.TB, s TestServerWithSystem, tableName string, version int,
) hlc.Timestamp {
	tblKey := s.Codec.TablePrefix(keys.DescriptorTableID)
	header := roachpb.RequestHeader{
		Key:    tblKey,
		EndKey: tblKey.PrefixEnd(),
	}
	dropColTblID := sqlutils.QueryTableID(t, s.DB, `d`, "public", tableName)
	req := &roachpb.ExportRequest{
		RequestHeader: header,
		MVCCFilter:    roachpb.MVCCFilter_All,
		StartTime:     hlc.Timestamp{},
	}
	clock := hlc.NewClockWithSystemTimeSource(time.Minute /* maxOffset */)
	hh := roachpb.Header{Timestamp: clock.Now()}
	res, pErr := kv.SendWrappedWith(context.Background(),
		s.SystemServer.DB().NonTransactionalSender(), hh, req)
	if pErr != nil {
		t.Fatal(pErr.GoError())
	}
	for _, file := range res.(*roachpb.ExportResponse).Files {
		it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
			LowerBound: keys.MinKey,
			UpperBound: keys.MaxKey,
		})
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
			if _, hasRange := it.HasPointAndRange(); hasRange {
				t.Fatalf("unexpected MVCC range key at %s", k)
			}
			remaining, _, _, err := s.Codec.DecodeIndexPrefix(k.Key)
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
			value := roachpb.Value{RawBytes: unsafeValue, Timestamp: k.Timestamp}
			b, err := descbuilder.FromSerializedValue(&value)
			if err != nil {
				t.Fatal(err)
			}
			require.NotNil(t, b)
			if b.DescriptorType() == catalog.Table {
				tbl := b.BuildImmutable().(catalog.TableDescriptor)
				if int(tbl.GetVersion()) == version {
					return tbl.GetModificationTime()
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

func TestChangefeedEachColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY most (a,b), FAMILY only_c (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)

		// Must specify WITH split_column_families
		sqlDB.ExpectErr(t, `multiple column families`, `CREATE CHANGEFEED FOR foo`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families`)
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
		fooWithDiff := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families, diff`)
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
		bar := feed(t, f, `CREATE CHANGEFEED FOR bar`)
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

	cdcTest(t, testFn)
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

		sqlDB.ExpectErr(t, `nosuchfamily`, `CREATE CHANGEFEED FOR foo FAMILY nosuchfamily`)

		fooMost := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY most`)
		defer closeFeed(t, fooMost)
		assertPayloads(t, fooMost, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
			`foo.most: [1]->{"after": {"a": 1, "b": "dollar"}}`,
		})

		fooRest := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest`)
		defer closeFeed(t, fooRest)
		assertPayloads(t, fooRest, []string{
			`foo.rest: [0]->{"after": {"c": "cat", "d": null}}`,
			`foo.rest: [1]->{"after": {"c": "cent", "d": null}}`,
		})

		fooBoth := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest, foo FAMILY most`)
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

func TestChangefeedSingleColumnFamilySchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// requireErrorSoon times out after 30 seconds
	skip.UnderStress(t)
	skip.UnderRace(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY most (a,b), FAMILY rest (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)

		fooMost := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY most`)
		defer closeFeed(t, fooMost)
		assertPayloads(t, fooMost, []string{
			`foo.most: [0]->{"after": {"a": 0, "b": "dog"}}`,
		})

		fooRest := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY rest`)
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
		requireErrorSoon(context.Background(), t, fooRest,
			regexp.MustCompile(`CHANGEFEED targeting nonexistent or removed column family rest of table foo`))

	}
	cdcTest(t, testFn)
}

func TestChangefeedEachColumnFamilySchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)

		// Table with 2 column families.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY f1 (a,b), FAMILY f2 (c))`)
		sqlDB.Exec(t, `INSERT INTO foo values (0, 'dog', 'cat')`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH split_column_families`)
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
	cdcTest(t, testFn)
}

func TestChangefeedAuthorization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		rootDB := sqlutils.MakeSQLRunner(s.DB)
		rootDB.Exec(t, `create user guest`)
		rootDB.Exec(t, `create user feedcreator with controlchangefeed`)
		rootDB.Exec(t, `create type type_a as enum ('a')`)
		rootDB.Exec(t, `create table table_a (id int, type type_a)`)
		rootDB.Exec(t, `create table table_b (id int, type type_a)`)
		rootDB.Exec(t, `insert into table_a(id) values (0)`)

		expectSuccess := func(stmt string) {
			successfulFeed := feed(t, f, stmt)
			defer closeFeed(t, successfulFeed)
			_, err := successfulFeed.Next()
			require.NoError(t, err)
		}

		// Users with CONTROLCHANGEFEED need SELECT privileges as well.
		asUser(t, f, `feedcreator`, func() {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a`,
				`user feedcreator does not have SELECT privilege on relation table_a`)
		})

		rootDB.Exec(t, `GRANT SELECT ON table_a TO guest`)
		rootDB.Exec(t, `GRANT CHANGEFEED ON table_b TO guest`)
		rootDB.Exec(t, `GRANT SELECT ON table_a TO feedcreator`)

		// Users without the controlchangefeed role option need the CHANGEFEED privilege
		// on every referenced table.
		asUser(t, f, `guest`, func() {
			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a, table_b -- as guest`,
				`CHANGEFEED privilege`)
		})

		// Users with controlchangefeed need the SELECT privilege on every table.
		asUser(t, f, `feedcreator`, func() {
			expectSuccess(`CREATE CHANGEFEED FOR table_a`)

			expectErrCreatingFeed(t, f, `CREATE CHANGEFEED FOR table_a, table_b -- as feedcreator`,
				`user feedcreator does not have SELECT privilege on relation table_b`)
		})

		// GRANT CHANGEFEED ON DATABASE is an error.
		rootDB.ExpectErr(t, `invalid privilege type CHANGEFEED for database`, `GRANT CHANGEFEED ON DATABASE d TO guest`)

		// CHANGEFEED can be granted as a default privilege on all new tables in a schema
		rootDB.ExecMultiple(t,
			`ALTER DEFAULT PRIVILEGES IN SCHEMA d.public GRANT CHANGEFEED ON TABLES TO guest`,
			`CREATE TABLE table_c (id int primary key)`,
			`INSERT INTO table_c values (0)`,
		)

		asUser(t, f, `guest`, func() {
			expectSuccess(`CREATE CHANGEFEED FOR table_c`)
		})

		// GRANT CHANGEFED ON prefix.* grants CHANGEFEED on all current tables with that prefix.
		rootDB.Exec(t, `GRANT CHANGEFEED ON d.public.* TO guest`)
		rootDB.Exec(t, `GRANT SELECT ON d.* TO guest`)
		asUser(t, f, `guest`, func() {
			expectSuccess(`CREATE CHANGEFEED FOR table_c`)
		})

		// SHOW GRANTS includes CHANGEFEED privileges.
		var count int
		rootDB.QueryRow(t, `select count(*) from [show grants] where privilege_type = 'CHANGEFEED';`).Scan(&count)
		require.Greater(t, count, 0, `Number of CHANGEFEED grants`)

	}
	cdcTest(t, testFn)
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

func TestChangefeedAvroNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stop := makeServer(t)
	defer stop()
	schemaReg := cdctest.StartTestSchemaRegistry()
	defer schemaReg.Close()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, "CREATE table foo (i int)")
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0)`)

	sql := fmt.Sprintf("CREATE CHANGEFEED FOR d.foo INTO 'null://' WITH format=experimental_avro, confluent_schema_registry='%s'", schemaReg.URL())
	expectNotice(t, s.Server, sql, `avro is no longer experimental, use format=avro`)
}

func TestChangefeedOutputTopics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		pgURL, cleanup := sqlutils.PGUrl(t, s.Server.SQLAddr(), t.Name(), url.User(username.RootUser))
		defer cleanup()
		pgBase, err := pq.NewConnector(pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		actual := "(no notice)"
		connector := pq.ConnectorWithNoticeHandler(pgBase, func(n *pq.Error) {
			actual = n.Message
		})

		dbWithHandler := gosql.OpenDB(connector)
		defer dbWithHandler.Close()

		sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

		sqlDB.Exec(t, `CREATE TABLE ☃ (i INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO ☃ VALUES (0)`)

		tg := newTeeGroup()
		feedCh := make(chan *sarama.ProducerMessage, 1024)
		wrapSink := func(s Sink) Sink {
			return &fakeKafkaSink{
				Sink:   s,
				tg:     tg,
				feedCh: feedCh,
			}
		}

		jobFeed := newJobFeed(dbWithHandler, wrapSink)
		jobFeed.jobID = jobspb.InvalidJobID

		c := &kafkaFeed{
			jobFeed:        jobFeed,
			seenTrackerMap: make(map[string]struct{}),
			source:         feedCh,
			tg:             tg,
		}
		defer func() {
			err = c.Close()
			require.NoError(t, err)
		}()
		kafkaFeed, ok := f.(*kafkaFeedFactory)
		require.True(t, ok)
		kafkaFeed.di.prepareJob(c.jobFeed)

		sqlDB.Exec(t, `CREATE CHANGEFEED FOR ☃ INTO 'kafka://does.not.matter/'`)
		require.Equal(t, `changefeed will emit to topic _u2603_`, actual)
	}
	cdcTest(t, testFn, feedTestForceSink("kafka"))
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

	cdcTestNamed(t, "import fails changefeed", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
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

	cdcTestNamedWithSystem(t, "reverted import fails changefeed with earlier cursor", func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sysSQLDB := sqlutils.MakeSQLRunner(s.SystemDB)
		sysSQLDB.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1'")
		sysSQLDB.Exec(t, "SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = true")

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE for_import (a INT PRIMARY KEY, b INT)`)

		var start string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&start)
		sqlDB.Exec(t, "INSERT INTO for_import VALUES (0, 10);")

		// Start an import job which will immediately pause after ingestion
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest';")
		go func() {
			sqlDB.ExpectErr(t, `pause point`, `IMPORT INTO for_import CSV DATA ($1);`, dataSrv.URL)
		}()
		sqlDB.CheckQueryResultsRetry(
			t,
			fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS] WHERE job_type='IMPORT' AND status='%s'`, jobs.StatusPaused),
			[][]string{{"1"}},
		)

		// Cancel to trigger a revert and verify revert completion
		var jobID string
		sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] where job_type='IMPORT'`).Scan(&jobID)
		sqlDB.Exec(t, `CANCEL JOB $1`, jobID)
		sqlDB.CheckQueryResultsRetry(
			t,
			fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS] WHERE job_type='IMPORT' AND status='%s'`, jobs.StatusCanceled),
			[][]string{{"1"}},
		)
		sqlDB.CheckQueryResultsRetry(t, "SELECT count(*) FROM for_import", [][]string{{"1"}})

		// Changefeed should fail regardless
		forImport := feed(t, f, `CREATE CHANGEFEED FOR for_import WITH cursor=$1`, start)
		defer closeFeed(t, forImport)
		requireErrorSoon(context.Background(), t, forImport,
			regexp.MustCompile(`CHANGEFEED cannot target offline table: for_import \(offline reason: "importing"\)`))
	})
}

func TestChangefeedRestartMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE test_tab (a INT PRIMARY KEY, b INT UNIQUE NOT NULL)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (0, 0)`)

	row := sqlDB.QueryRow(t, `SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE test_tab] LIMIT 1`)
	var rangeID, leaseHolder int
	row.Scan(&rangeID, &leaseHolder)

	// Start the changefeed on a node other than the leaseholder
	// so that it is likely that the changeAggregator and
	// changeFrontier are on different nodes.
	feedServerID := ((leaseHolder - 1) + 1) % 3
	t.Logf("Range %d is on lease holder %d, running rangefeed on server %d (store id: %d)", rangeID, leaseHolder, feedServerID, cluster.Server(feedServerID).GetFirstStoreID())
	db = cluster.ServerConn(feedServerID)
	sqlDB = sqlutils.MakeSQLRunner(db)

	f := makeKafkaFeedFactory(cluster, db)
	feed := feed(t, f, "CREATE CHANGEFEED FOR test_tab WITH updated")
	defer closeFeed(t, feed)
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0, "b": 0}}`,
	})

	waitForSchemaChange(t, sqlDB, `ALTER TABLE test_tab ALTER PRIMARY KEY USING COLUMNS (b)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (1, 11)`)
	// No backfill, but we should see the newly insert value
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [11]->{"after": {"a": 1, "b": 11}}`,
	})

	waitForSchemaChange(t, sqlDB, `ALTER TABLE test_tab SET LOCALITY REGIONAL BY ROW`)
	// schema-changer backfill for the ADD COLUMN
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0, "b": 0}}`,
		`test_tab: [11]->{"after": {"a": 1, "b": 11}}`,
	})
	// changefeed backfill for the ADD COLUMN
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: ["us-east1", 0]->{"after": {"a": 0, "b": 0, "crdb_region": "us-east1"}}`,
		`test_tab: ["us-east1", 11]->{"after": {"a": 1, "b": 11, "crdb_region": "us-east1"}}`,
	})

	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (2, 22)`)
	// Newly inserted data works
	assertPayloadsStripTs(t, feed, []string{
		`test_tab: ["us-east1", 22]->{"after": {"a": 2, "b": 22, "crdb_region": "us-east1"}}`,
	})
}

func TestChangefeedStopPolicyMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE test_tab (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (0)`)

	row := sqlDB.QueryRow(t, `SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE test_tab] LIMIT 1`)
	var rangeID, leaseHolder int
	row.Scan(&rangeID, &leaseHolder)

	// Start the changefeed on a node other than the leaseholder
	// so that it is likely that the changeAggregator and
	// changeFrontier are on different nodes.
	feedServerID := ((leaseHolder - 1) + 1) % 3
	t.Logf("Range %d is on lease holder %d, running rangefeed on server %d (store id: %d)", rangeID, leaseHolder, feedServerID, cluster.Server(feedServerID).GetFirstStoreID())
	db = cluster.ServerConn(feedServerID)
	sqlDB = sqlutils.MakeSQLRunner(db)

	f := makeKafkaFeedFactory(cluster, db)
	feed := feed(t, f, "CREATE CHANGEFEED FOR test_tab WITH schema_change_policy='stop'")
	defer closeFeed(t, feed)
	sqlDB.Exec(t, `INSERT INTO test_tab VALUES (1)`)
	assertPayloads(t, feed, []string{
		`test_tab: [0]->{"after": {"a": 0}}`,
		`test_tab: [1]->{"after": {"a": 1}}`,
	})
	sqlDB.Exec(t, `ALTER TABLE test_tab ADD COLUMN b INT NOT NULL DEFAULT 0`)

	waitForSchemaChangeError := func(t *testing.T, f cdctest.TestFeed) {
		t.Helper()
		for {
			if _, err := f.Next(); err != nil {
				require.Contains(t, err.Error(), "schema change occurred at")
				break
			}
		}
	}
	waitForSchemaChangeError(t, feed)
}

func TestChangefeedWorksOnRBRChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFnJSON := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		t.Run("regional by row change works", func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT)`)
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
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT)`)
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
			sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY, b INT, region crdb_internal_region NOT NULL DEFAULT 'us-east-1')`)
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
	// operation is unsupported in multi-tenancy mode
	opts := []feedTestOption{
		feedTestNoTenants,
		feedTestEnterpriseSinks,
		withArgsFn(withTestServerRegion),
	}
	cdcTestNamed(t, "format=json", testFnJSON, opts...)
	cdcTestNamed(t, "format=avro", testFnAvro, append(opts, feedTestForceSink("kafka"))...)
}

func TestChangefeedRBRAvroAddRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We need a cluster here to make sure we have multiple active
	// regions that we can add to the database.
	cluster, db, cleanup := startTestCluster(t)
	defer cleanup()

	f := makeKafkaFeedFactory(cluster, db)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE rbr (a INT PRIMARY KEY)`)
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
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'")

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
				skip.WithIssue(t, 84511)
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

	cdcTest(t, testFn)
}

func TestChangefeedNoBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		disableDeclarativeSchemaChangesForTest(t, sqlDB)
		// Shorten the intervals so this test doesn't take so long. We need to wait
		// for timestamps to get resolved.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.experimental_poll_interval = '200ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'")

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

	cdcTest(t, testFn)
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

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)

		drainUntilErr := func(f cdctest.TestFeed) (err error) {
			var msg *cdctest.TestFeedMessage
			for msg, err = f.Next(); msg != nil; msg, err = f.Next() {
			}
			return err
		}

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
		if err := drainUntilErr(truncate); !testutils.IsError(err, `"truncate" was truncated`) {
			t.Fatalf(`expected ""truncate" was truncated" error got: %+v`, err)
		}
		if err := drainUntilErr(truncateCascade); !testutils.IsError(
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
		const dropOrOfflineRE = `"drop" was dropped|CHANGEFEED cannot target offline table: drop`
		if err := drainUntilErr(drop); !testutils.IsError(err, dropOrOfflineRE) {
			t.Errorf(`expected %q error, instead got: %+v`, dropOrOfflineRE, err)
		}
		assertFailuresCounter(t, metrics, 3)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
	// will sometimes fail, non deterministic
}

func TestChangefeedMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.flushed_bytes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.flushes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
			t.Errorf(`expected %d got %d`, 0, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.in`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.out`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.table_metadata_nanos`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH metrics_label='tier0'`)
		_, err := foo.Next()
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.flushed_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.flushes`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.running`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.in`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.out`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			return nil
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)

		// Check that two changefeeds add correctly.
		// Set cluster settings back so we don't interfere with schema changes.
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		fooCopy := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = fooCopy.Next()
		_, _ = fooCopy.Next()
		testutils.SucceedsSoon(t, func() error {
			// We can't assert exactly 4 or 88 in case we get (allowed) duplicates
			// from RangeFeed.
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c < 4 {
				return errors.Errorf(`expected >= 4 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c < 88 {
				return errors.Errorf(`expected >= 88 got %d`, c)
			}
			return nil
		})

		// Cancel all the changefeeds and check that max_behind_nanos returns to 0
		// and the number running returns to 0.
		require.NoError(t, foo.Close())
		require.NoError(t, fooCopy.Close())
		testutils.SucceedsSoon(t, func() error {
			if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.running`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			return nil
		})
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

func TestChangefeedRetryableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		var failEmit int64
		knobs.BeforeEmitRow = func(_ context.Context) error {
			switch atomic.LoadInt64(&failEmit) {
			case 1:
				return changefeedbase.MarkRetryableError(fmt.Errorf("synthetic retryable error"))
			case 2:
				return changefeedbase.WithTerminalError(errors.New("synthetic terminal error"))
			default:
				return nil
			}
		}

		// Set up a new feed and verify that the sink is started up.
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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
		registry := s.Server.JobRegistry().(*jobs.Registry)

		sli, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		retryCounter := sli.ErrorRetries
		testutils.SucceedsSoon(t, func() error {
			if retryCounter.Value() < 3 {
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

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedJobUpdateFailsIfNotClaimed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 91548)

	// Set TestingKnobs to return a known session for easier
	// comparison.
	testSession := sqllivenesstestutils.NewAlwaysAliveSession("known-test-session")
	adoptionInterval := 20 * time.Minute
	sessionOverride := withKnobsFn(func(knobs *base.TestingKnobs) {
		knobs.SQLLivenessKnobs = &sqlliveness.TestingKnobs{
			SessionOverride: func(_ context.Context) (sqlliveness.Session, error) {
				return testSession, nil
			},
		}
		// This is a hack to avoid the job adoption loop from
		// immediately re-adopting the job that is running. The job
		// adoption loop basically just sets the claim ID, which will
		// undo our deletion of the claim ID below.
		knobs.JobsTestingKnobs.(*jobs.TestingKnobs).IntervalOverrides.Adopt = &adoptionInterval
	})
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		errChan := make(chan error, 1)
		knobs.HandleDistChangefeedError = func(err error) error {
			errChan <- err
			return err
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)
		sqlDB.Exec(t, `INSERT INTO foo (a, b) VALUES (1, 1)`)

		cf := feed(t, f, "CREATE CHANGEFEED FOR TABLE foo")
		jobID := cf.(cdctest.EnterpriseTestFeed).JobID()
		defer func() {
			// Manually update job status to avoid closeFeed waitng for the registry to cancel it
			sqlDB.Exec(t, `UPDATE system.jobs SET status = $1 WHERE id = $2`, jobs.StatusFailed, jobID)
			closeFeed(t, cf)
		}()

		assertPayloads(t, cf, []string{
			`foo: [1]->{"after": {"a": 1, "b": 1}}`,
		})

		// Mimic the claim dying and being cleaned up by
		// another node.
		sqlDB.Exec(t, `UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`, jobID)

		// Expect that the distflow fails since it can't
		// update the checkpoint.
		select {
		case err := <-errChan:
			require.Error(t, err)
			// TODO(ssd): Replace this error in the jobs system with
			// an error type we can check against.
			require.Contains(t, err.Error(), fmt.Sprintf("expected session \"%s\" but found NULL", testSession.ID().String()))
		case <-time.After(5 * time.Second):
			t.Fatal("expected distflow to fail but it hasn't after 5 seconds")
		}
	}

	// TODO: Figure out why this freezes on tenants
	cdcTest(t, testFn, sessionOverride, feedTestNoTenants, feedTestEnterpriseSinks)
}

// TestChangefeedDataTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table data.
func TestChangefeedDataTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.FeedKnobs.BeforeScanRequest = func(_ *kv.Batch) error {
			if atomic.LoadInt32(&shouldWait) == 0 {
				return nil
			}
			wait <- struct{}{}
			<-resume
			return nil
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

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

		// The changefeed needs to be initialized in a background goroutine because
		// pgx will try to pull results from it as soon as it runs the conn.Query
		// method, but that will block until `resume` is signaled.
		changefeedInit := make(chan cdctest.TestFeed, 1)
		var dataExpiredRows cdctest.TestFeed
		defer func() {
			if dataExpiredRows != nil {
				closeFeed(t, dataExpiredRows)
			}
		}()
		go func() {
			feed, err := f.Feed("CREATE CHANGEFEED FOR TABLE foo")
			if err == nil {
				changefeedInit <- feed
			}
			close(changefeedInit)
		}()

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
		forceTableGC(t, s.SystemServer, sqlDB, "d", "foo")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}
		dataExpiredRows = <-changefeedInit
		require.NotNil(t, dataExpiredRows)

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
	// TODO(samiskin): Tenant test disabled because this test requires
	// forceTableGC which doesn't work on tenants
	cdcTestWithSystem(t, testFn, feedTestForceSink("sinkless"), feedTestNoTenants)
}

// TestChangefeedSchemaTTL ensures that changefeeds fail with an error in the case
// where the feed has fallen behind the GC TTL of the table's schema.
func TestChangefeedSchemaTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		// Set a very simple channel-based, wait-and-resume function as the
		// BeforeEmitRow hook.
		var shouldWait int32
		wait := make(chan struct{})
		resume := make(chan struct{})
		knobs := s.TestingKnobs.
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

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

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
		forceTableGC(t, s.SystemServer, sqlDB, "system", "descriptor")

		// Resume our changefeed normally.
		atomic.StoreInt32(&shouldWait, 0)
		resume <- struct{}{}

		// Verify that the third call to Next() returns an error (the first is the
		// initial row, the second is the first change.
		// Note: rows, and the error message may arrive in any order, so we just loop
		// until we see an error.
		for {
			_, err := dataExpiredRows.Next()
			if err != nil {
				require.Regexp(t, `GC threshold`, err)
				break
			}
		}

	}

	// TODO(samiskin): tenant tests skipped because of forceTableGC not working
	// with a TestTenantInterface
	cdcTestWithSystem(t, testFn, feedTestNoTenants)
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

	sqlDB.ExpectErr(
		t, `CHANGEFEED targets TABLE foo and TABLE foo are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo, foo`,
	)
	sqlDB.ExpectErr(
		t, `CHANGEFEED targets TABLE foo and TABLE defaultdb.foo are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo, defaultdb.foo`,
	)
	sqlDB.Exec(t,
		`CREATE TABLE threefams (a int, b int, c int, family f_a(a), family f_b(b), family f_c(c))`)
	sqlDB.ExpectErr(
		t, `CHANGEFEED targets TABLE foo FAMILY f_a and TABLE foo FAMILY f_a are duplicates`,
		`EXPERIMENTAL CHANGEFEED FOR foo family f_a, foo FAMILY f_b, foo FAMILY f_a`,
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
		t, `.*column a: decimal with no precision`,
		`EXPERIMENTAL CHANGEFEED FOR dec WITH format=$1, confluent_schema_registry=$2`,
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)
	sqlDB.Exec(t, `CREATE TABLE "oid" (a OID PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO "oid" VALUES (3::OID)`)
	sqlDB.ExpectErr(
		t, `.*column a: type OID not yet supported with avro`,
		`EXPERIMENTAL CHANGEFEED FOR "oid" WITH format=$1, confluent_schema_registry=$2`,
		changefeedbase.OptFormatAvro, schemaReg.URL(),
	)

	unknownParams := func(sink string, params ...string) string {
		return fmt.Sprintf(`unknown %s sink query parameters: [%s]`, sink, strings.Join(params, ", "))
	}

	// Check that sink URLs have valid scheme
	sqlDB.ExpectErr(
		t, `no scheme found for sink URL`,
		`CREATE CHANGEFEED FOR foo INTO 'kafka%3A%2F%2Fnope%0A'`,
	)

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
	sqlDB.ExpectErr(
		t, `this sink is incompatible with option webhook_client_timeout`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='1s'`,
		`kafka://nope/`,
	)
	// The avro format doesn't support key_in_value or topic_in_value yet.
	sqlDB.ExpectErr(
		t, `key_in_value is not supported with format=avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH key_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `topic_in_value is not supported with format=avro`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topic_in_value, format='experimental_avro'`,
		`kafka://nope`,
	)

	// The topics option should not be exposed to users since it is used
	// internally to display topics in the show changefeed jobs query
	sqlDB.ExpectErr(
		t, `invalid option "topics"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH topics='foo,bar'`,
		`kafka://nope`,
	)

	// The cloudStorageSink is particular about the options it will work with.
	sqlDB.ExpectErr(
		t, `this sink is incompatible with option confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='avro', confluent_schema_registry=$2`,
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

	// WITH initial_scan and no_initial_scan disallowed
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan, no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH no_initial_scan, initial_scan`, `kafka://nope`,
	)

	// WITH only_initial_scan and no_initial_scan disallowed
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH no_initial_scan, initial_scan_only`, `kafka://nope`,
	)

	// WITH initial_scan_only and initial_scan disallowed
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan, initial_scan_only`, `kafka://nope`,
	)

	// WITH only_initial_scan and end_time disallowed
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan_only, end_time = '1'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH end_time = '1', initial_scan_only`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH end_time = '1', initial_scan = 'only'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and end_time`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'only', end_time = '1'`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and resolved`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH resolved, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and diff`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH diff, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and mvcc_timestamp`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH mvcc_timestamp, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan_only and updated`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, initial_scan = 'only'`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `unknown initial_scan: foo`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'foo'`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and no_initial_scan`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'yes', no_initial_scan`, `kafka://nope`,
	)
	sqlDB.ExpectErr(
		t, `cannot specify both initial_scan and initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH initial_scan = 'no', initial_scan_only`, `kafka://nope`,
	)

	sqlDB.ExpectErr(
		t, `format=csv is only usable with initial_scan_only`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format = csv`, `kafka://nope`,
	)

	var tsCurrent string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsCurrent)

	sqlDB.ExpectErr(
		t,
		fmt.Sprintf(`specified end time 1.0000000000 cannot be less than statement time %s`, tsCurrent),
		`CREATE CHANGEFEED FOR foo INTO $1 WITH cursor = $2, end_time = '1.0000000000'`, `kafka://nope`, tsCurrent,
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

	// Sanity check webhook sink options.
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
		t, `this sink is incompatible with option confluent_schema_registry`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH format='avro', confluent_schema_registry=$2`,
		`webhook-https://fake-host`, schemaReg.URL(),
	)
	sqlDB.ExpectErr(
		t, `problem parsing option webhook_client_timeout: time: invalid duration "not_an_integer"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='not_an_integer'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `option webhook_client_timeout must be a duration greater than 0`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_client_timeout='0s'`, `webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `negative durations are not accepted: webhook_client_timeout='-500s'`,
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
	sqlDB.ExpectErr(
		t, `invalid option value webhook_sink_config, all config values must be non-negative`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": -100, "Frequency": "1s"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `invalid option value webhook_sink_config, all config values must be non-negative`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": 100, "Frequency": "-1s"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `invalid option value webhook_sink_config, flush frequency is not set, messages may never be sent`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Messages": 100}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `error unmarshalling json: time: invalid duration "Zm9v"`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Flush": {"Frequency": "Zm9v"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `error unmarshalling json: invalid character`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='not json'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `this sink is incompatible with option compression`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH compression='gzip'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `max retries must be either a positive int or 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": "not valid"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `max retry count must be a positive integer. use 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": 0}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `max retry count must be a positive integer. use 'inf' for infinite retries.`,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH webhook_sink_config='{"Retry": {"Max": -1}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, ``,
		`CREATE CHANGEFEED FOR foo INTO $1 WITH updated, webhook_sink_config='{"Retry":{"Max":"inf"}}'`,
		`webhook-https://fake-host`,
	)
	sqlDB.ExpectErr(
		t, `client_cert requires client_key to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`,
		`webhook-https://fake-host?client_cert=Zm9v`,
	)
	sqlDB.ExpectErr(
		t, `client_key requires client_cert to be set`,
		`CREATE CHANGEFEED FOR foo INTO $1`,
		`webhook-https://fake-host?client_key=Zm9v`,
	)

	// Sanity check on_error option
	sqlDB.ExpectErr(
		t, `option "on_error" requires a value`,
		`CREATE CHANGEFEED FOR foo into $1 WITH on_error`,
		`kafka://nope`)
	sqlDB.ExpectErr(
		t, `unknown on_error: not_valid, valid values are 'pause' and 'fail'`,
		`CREATE CHANGEFEED FOR foo into $1 WITH on_error='not_valid'`,
		`kafka://nope`)
}

func TestChangefeedDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intentionally don't use the TestFeedFactory because we want to
	// control the placeholders.
	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	// Create enum to ensure enum values displayed correctly in the summary.
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, status status)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

	sink, cleanup := sqlutils.PGUrl(t, s.Server.SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	sink.Scheme = changefeedbase.SinkSchemeExperimentalSQL
	sink.Path = `d`

	redactedSink := strings.Replace(sink.String(), username.RootUser, `redacted`, 1)
	for _, tc := range []struct {
		create string
		descr  string
	}{
		{
			create: "CREATE CHANGEFEED FOR foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE foo INTO '` + redactedSink + `' WITH envelope = 'wrapped', updated`,
		},
		{
			create: "CREATE CHANGEFEED FOR public.foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE public.foo INTO '` + redactedSink + `' WITH envelope = 'wrapped', updated`,
		},
		{
			create: "CREATE CHANGEFEED FOR d.public.foo INTO $1 WITH updated, envelope = $2",
			descr:  `CREATE CHANGEFEED FOR TABLE d.public.foo INTO '` + redactedSink + `' WITH envelope = 'wrapped', updated`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM foo WHERE a % 2 = 0",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH envelope = 'wrapped', schema_change_policy = 'stop', updated AS SELECT a FROM foo WHERE (a % 2) = 0`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM public.foo AS bar WHERE a % 2 = 0",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH envelope = 'wrapped', schema_change_policy = 'stop', updated AS SELECT a FROM public.foo AS bar WHERE (a % 2) = 0`,
		},
		{
			// TODO(#85143): remove schema_change_policy='stop' from this test.
			create: "CREATE CHANGEFEED INTO $1 WITH updated, envelope = $2, schema_change_policy='stop' AS SELECT a FROM foo WHERE status IN ('open', 'closed')",
			descr:  `CREATE CHANGEFEED INTO '` + redactedSink + `' WITH envelope = 'wrapped', schema_change_policy = 'stop', updated AS SELECT a FROM foo WHERE status IN ('open', 'closed')`,
		},
	} {
		t.Run(tc.create, func(t *testing.T) {
			var jobID jobspb.JobID
			sqlDB.QueryRow(t, tc.create, sink.String(), `wrapped`).Scan(&jobID)

			var description string
			sqlDB.QueryRow(t,
				`SELECT description FROM [SHOW JOBS] WHERE job_id = $1`, jobID,
			).Scan(&description)

			require.Equal(t, tc.descr, description)
		})
	}

}

func TestChangefeedPanicRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Panics can mess with the test setup so run these each in their own test.

	prep := func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
		cdceval.TestingEnableVolatileFunction(`crdb_internal.force_panic`)
		sqlDB.Exec(t, `CREATE TABLE foo(id int primary key, s string)`)
		sqlDB.Exec(t, `INSERT INTO foo(id, s) VALUES (0, 'hello'), (1, null)`)
	}

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		// Check that disallowed expressions have a good error message.
		// Also regression test for https://github.com/cockroachdb/cockroach/issues/90416
		sqlDB.ExpectErr(t, "sub-query expressions not supported by CDC",
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT 1 FROM foo WHERE EXISTS (SELECT true)`)
	})

	// Check that all panics while evaluating the WHERE clause in an expression are recovered from.
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		foo := feed(t, f,
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT 1 FROM foo WHERE crdb_internal.force_panic('wat') IS NULL`)
		defer closeFeed(t, foo)
		var err error
		for err == nil {
			_, err = foo.Next()
		}
		require.Error(t, err, "error while evaluating WHERE clause")
	})

	// Check that all panics while evaluating the SELECT clause in an expression are recovered from.
	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		prep(t, sqlDB)
		foo := feed(t, f,
			`CREATE CHANGEFEED WITH schema_change_policy='stop' AS SELECT crdb_internal.force_panic('wat') FROM foo`)
		defer closeFeed(t, foo)
		var err error
		for err == nil {
			_, err = foo.Next()
		}
		require.Error(t, err, "error while evaluating SELECT clause")
	})
}

func TestChangefeedPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 83946)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedPauseUnpauseCursorAndInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 67565)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedUpdateProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		ptsInterval := 50 * time.Millisecond
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '20ms'`)
		defer closeFeed(t, foo)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")

		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		store, err := s.SystemServer.GetStores().(*kvserver.Stores).GetStore(s.SystemServer.GetFirstStoreID())
		require.NoError(t, err)
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader

		// Wait and return the next resolved timestamp after the wait time
		waitAndDrainResolved := func(ts time.Duration) hlc.Timestamp {
			targetTs := timeutil.Now().Add(ts)
			for {
				resolvedTs, _ := expectResolvedTimestamp(t, foo)
				if resolvedTs.GoTime().UnixNano() > targetTs.UnixNano() {
					return resolvedTs
				}
			}
		}

		mkGetProtections := func(t *testing.T, ptp protectedts.Provider,
			srv serverutils.TestTenantInterface, ptsReader spanconfig.ProtectedTSReader,
			span roachpb.Span) func() []hlc.Timestamp {
			return func() (r []hlc.Timestamp) {
				require.NoError(t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, srv.Clock().Now()))
				protections, _, err := ptsReader.GetProtectionTimestamps(ctx, span)
				require.NoError(t, err)
				return protections
			}
		}

		mkWaitForProtectionCond := func(t *testing.T, getProtection func() []hlc.Timestamp,
			check func(protection []hlc.Timestamp) error) func() {
			return func() {
				t.Helper()
				testutils.SucceedsSoon(t, func() error { return check(getProtection()) })
			}
		}

		// Setup helpers on the system.descriptors table.
		descriptorTableKey := s.Codec.TablePrefix(keys.DescriptorTableID)
		descriptorTableSpan := roachpb.Span{
			Key: descriptorTableKey, EndKey: descriptorTableKey.PrefixEnd(),
		}
		getDescriptorTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader,
			descriptorTableSpan)

		// Setup helpers on the user table.
		tableKey := s.Codec.TablePrefix(uint32(fooDesc.GetID()))
		tableSpan := roachpb.Span{
			Key: tableKey, EndKey: tableKey.PrefixEnd(),
		}
		getTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader, tableSpan)
		waitForProtectionAdvanced := func(ts hlc.Timestamp, getProtection func() []hlc.Timestamp) {
			check := func(protections []hlc.Timestamp) error {
				if len(protections) == 0 {
					return errors.New("expected protection but found none")
				}
				for _, p := range protections {
					if p.LessEq(ts) {
						return errors.Errorf("expected protected timestamp to exceed %v, found %v", ts, p)
					}
				}
				return nil
			}

			mkWaitForProtectionCond(t, getProtection, check)()
		}

		// Observe the protected timestamp advancing along with resolved timestamps
		for i := 0; i < 5; i++ {
			// Progress the changefeed and allow time for a pts record to be laid down
			nextResolved := waitAndDrainResolved(100 * time.Millisecond)
			waitForProtectionAdvanced(nextResolved, getTableProtection)
			waitForProtectionAdvanced(nextResolved, getDescriptorTableProtection)
		}
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
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
		foo := feed(t, f, `CREATE CHANGEFEED WITH envelope='row', schema_change_policy='stop' AS SELECT cdc_prev.b AS old FROM foo`)
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

func TestChangefeedProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx      = context.Background()
		userSpan = roachpb.Span{
			Key:    bootstrap.TestingUserTableDataMin(),
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
			ctx context.Context, ba *roachpb.BatchRequest,
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
				case <-ctx.Done():
				}
			default:
			}
			return nil
		})
		mkGetProtections = func(t *testing.T, ptp protectedts.Provider,
			srv serverutils.TestTenantInterface, ptsReader spanconfig.ProtectedTSReader,
			span roachpb.Span) func() []hlc.Timestamp {
			return func() (r []hlc.Timestamp) {
				require.NoError(t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, srv.Clock().Now()))
				protections, _, err := ptsReader.GetProtectionTimestamps(ctx, span)
				require.NoError(t, err)
				return protections
			}
		}
		checkProtection = func(protections []hlc.Timestamp) error {
			if len(protections) == 0 {
				return errors.New("expected protected timestamp to exist")
			}
			return nil
		}
		checkNoProtection = func(protections []hlc.Timestamp) error {
			if len(protections) != 0 {
				return errors.Errorf("expected protected timestamp to not exist, found %v", protections)
			}
			return nil
		}
		mkWaitForProtectionCond = func(t *testing.T, getProtection func() []hlc.Timestamp,
			check func(protection []hlc.Timestamp) error) func() {
			return func() {
				t.Helper()
				testutils.SucceedsSoon(t, func() error { return check(getProtection()) })
			}
		}
	)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms';`)
		sqlDB.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 100`)
		sqlDB.Exec(t, `ALTER RANGE system CONFIGURE ZONE USING gc.ttlseconds = 100`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		var tableID int
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables `+
			`WHERE name = 'foo' AND database_name = current_database()`).
			Scan(&tableID)

		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 100*time.Millisecond)

		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		store, err := s.SystemServer.GetStores().(*kvserver.Stores).GetStore(s.SystemServer.GetFirstStoreID())
		require.NoError(t, err)
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader

		// Setup helpers on the system.descriptors table.
		descriptorTableKey := s.Codec.TablePrefix(keys.DescriptorTableID)
		descriptorTableSpan := roachpb.Span{
			Key: descriptorTableKey, EndKey: descriptorTableKey.PrefixEnd(),
		}
		getDescriptorTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader,
			descriptorTableSpan)
		waitForDescriptorTableProtection := mkWaitForProtectionCond(t, getDescriptorTableProtection,
			checkProtection)
		waitForNoDescriptorTableProtection := mkWaitForProtectionCond(t, getDescriptorTableProtection,
			checkNoProtection)

		// Setup helpers on the user table.
		tableKey := s.Codec.TablePrefix(uint32(tableID))
		tableSpan := roachpb.Span{
			Key: tableKey, EndKey: tableKey.PrefixEnd(),
		}
		getTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader, tableSpan)
		waitForTableProtection := mkWaitForProtectionCond(t, getTableProtection, checkProtection)
		waitForNoTableProtection := mkWaitForProtectionCond(t, getTableProtection, checkNoProtection)
		waitForBlocked := requestBlockedScan()
		waitForProtectionAdvanced := func(ts hlc.Timestamp, getProtection func() []hlc.Timestamp) {
			check := func(protections []hlc.Timestamp) error {
				if len(protections) != 0 {
					for _, p := range protections {
						if p.LessEq(ts) {
							return errors.Errorf("expected protected timestamp to exceed %v, found %v", ts, p)
						}
					}
				}
				return nil
			}

			mkWaitForProtectionCond(t, getProtection, check)()
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`)
		defer closeFeed(t, foo)
		{
			// Ensure that there's a protected timestamp on startup that goes
			// away after the initial scan.
			unblock := waitForBlocked()
			waitForTableProtection()
			unblock()
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
				`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
				`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
				`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
				`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
			})
			resolved, _ := expectResolvedTimestamp(t, foo)
			waitForProtectionAdvanced(resolved, getTableProtection)
		}

		{
			// Ensure that a protected timestamp is created for a backfill due
			// to a schema change and removed after.
			waitForBlocked = requestBlockedScan()
			sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN c INT NOT NULL DEFAULT 1`)
			unblock := waitForBlocked()
			waitForTableProtection()
			waitForDescriptorTableProtection()
			unblock()
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a", "c": 1}}`,
				`foo: [2]->{"after": {"a": 2, "b": "b", "c": 1}}`,
				`foo: [4]->{"after": {"a": 4, "b": "c", "c": 1}}`,
				`foo: [7]->{"after": {"a": 7, "b": "d", "c": 1}}`,
				`foo: [8]->{"after": {"a": 8, "b": "e", "c": 1}}`,
			})
			resolved, _ := expectResolvedTimestamp(t, foo)
			waitForProtectionAdvanced(resolved, getTableProtection)
			waitForProtectionAdvanced(resolved, getDescriptorTableProtection)
		}

		{
			// Ensure that the protected timestamp is removed when the job is
			// canceled.
			waitForBlocked = requestBlockedScan()
			sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d INT NOT NULL DEFAULT 2`)
			_ = waitForBlocked()
			waitForTableProtection()
			waitForDescriptorTableProtection()
			sqlDB.Exec(t, `CANCEL JOB $1`, foo.(cdctest.EnterpriseTestFeed).JobID())
			waitForNoTableProtection()
			waitForNoDescriptorTableProtection()
		}
	}

	cdcTestWithSystem(t, testFn, feedTestNoTenants, feedTestEnterpriseSinks, withArgsFn(func(args *base.TestServerArgs) {
		storeKnobs := &kvserver.StoreTestingKnobs{}
		storeKnobs.TestingRequestFilter = requestFilter
		args.Knobs.Store = storeKnobs
	}))
}

func TestChangefeedProtectedTimestampOnPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(shouldPause bool) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
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
			serverCfg := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig
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
					require.True(t, r.Timestamp.LessEq(*progress.GetHighWater()))
				} else {
					require.Equal(t, uuid.Nil, details.ProtectedTimestampRecord)
				}
			}

			// Resume the job and ensure that the protected timestamp is removed once
			// the changefeed has caught up.
			require.NoError(t, feedJob.Resume())
			testutils.SucceedsSoon(t, func() error {
				resolvedTs, _ := expectResolvedTimestamp(t, foo)
				j, err := jr.LoadJob(ctx, feedJob.JobID())
				require.NoError(t, err)
				details := j.Progress().Details.(*jobspb.Progress_Changefeed).Changefeed

				err = serverCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					r, err := pts.GetRecord(ctx, txn, details.ProtectedTimestampRecord)
					if err != nil || r.Timestamp.Less(resolvedTs) {
						return fmt.Errorf("expected protected timestamp record %v to have timestamp greater than %v", r, resolvedTs)
					}
					return nil
				})
				return err
			})
		}
	}

	testutils.RunTrueAndFalse(t, "protect_on_pause", func(t *testing.T, shouldPause bool) {
		cdcTest(t, testFn(shouldPause), feedTestEnterpriseSinks)
	})

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
	serverutils.SetClusterSetting(t, tc, "changefeed.experimental_poll_interval", time.Millisecond)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

	// Create a factory which uses server 1 as the output of the Sink, but
	// executes the CREATE CHANGEFEED statement on server 0.
	sink, cleanup := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User(username.RootUser))
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

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
	cdcTest(t, testFn, feedTestForceSink("enterprise"))
}

// Regression test for #41694.
func TestChangefeedRestartDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 75080, "flaky test")
	defer log.Scope(t).Close(t)

	// TODO(yevgeniy): Rework this test.  It's too brittle.

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
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

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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
		beforeEmitRowCh <- errors.New(`nope don't write it`)

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

	useSysCfgInKV := withKnobsFn(func(knobs *base.TestingKnobs) {
		// TODO(irfansharif): This test is "skipped" under span configs;
		// #75080.
		if knobs.Store == nil {
			knobs.Store = &kvserver.StoreTestingKnobs{}
		}
		knobs.Store.(*kvserver.StoreTestingKnobs).UseSystemConfigSpanForQueues = true
	})

	cdcTest(t, testFn, feedTestForceSink("kafka"), useSysCfgInKV)
}

func TestChangefeedHandlesDrainingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "Takes too long with race enabled")

	shouldDrain := true
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
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
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}

	sinkDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	tc := serverutils.StartNewTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test uses SPLIT AT, which isn't currently supported for
			// secondary tenants. Tracked with #76378.
			DisableDefaultTestTenant: true,
			UseDatabase:              "test",
			Knobs:                    knobs,
			ExternalIODir:            sinkDir,
		}})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(1)
	sqlDB := sqlutils.MakeSQLRunner(db)
	serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)
	serverutils.SetClusterSetting(t, tc, "kv.closed_timestamp.target_duration", time.Second)
	serverutils.SetClusterSetting(t, tc, "changefeed.experimental_poll_interval", 10*time.Millisecond)

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
	f, closeSink := makeFeedFactory(t, randomSinkType(feedTestEnterpriseSinks), tc.Server(1), tc.ServerConn(0))
	defer closeSink()

	feed := feed(t, f, "CREATE CHANGEFEED FOR foo")
	defer closeFeed(t, feed)

	// At this point, the job created by feed will fail to start running on node 0 due to draining
	// registry.  However, this job will be retried, and it should succeed.
	// Note: This test is a bit unrealistic in that if the registry is draining, that
	// means that the server is draining (i.e. being shut down).  We don't do a full shutdown
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

func TestChangefeedPropagatesTerminalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	opts := makeOptions()
	defer addCloudStorageOptions(t, &opts)()
	defer changefeedbase.TestingSetDefaultMinCheckpointFrequency(testSinkFlushFrequency)()
	defer testingUseFastRetry()()
	const numNodes = 3

	perServerKnobs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		perServerKnobs[i] = base.TestServerArgs{
			// Test uses SPLIT AT, which isn't currently supported for
			// secondary tenants. Tracked with #76378.
			DisableDefaultTestTenant: true,
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					DrainFast:  true,
					Changefeed: &TestingKnobs{},
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			ExternalIODir: opts.externalIODir,
			UseDatabase:   "d",
		}
	}

	tc := serverutils.StartNewTestCluster(t, numNodes,
		base.TestClusterArgs{
			ServerArgsPerNode: perServerKnobs,
			ReplicationMode:   base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())

	{
		db := tc.ServerConn(1)
		sqlDB := sqlutils.MakeSQLRunner(db)
		serverutils.SetClusterSetting(t, tc, "kv.rangefeed.enabled", true)

		sqlDB.ExecMultiple(t,
			`CREATE DATABASE d;`,
			`CREATE TABLE foo (k INT PRIMARY KEY);`,
			`INSERT INTO foo (k) SELECT * FROM generate_series(1, 1000);`,
			`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 1000, 50));`,
		)
		for i := 1; i <= 1000; i += 50 {
			sqlDB.ExecSucceedsSoon(t, "ALTER TABLE foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[$1], $2)", 1+(i%numNodes), i)
		}
	}
	// changefeed coordinator will run on this node.
	const coordinatorID = 0

	testFn := func(t *testing.T, nodesToFail []int, opts feedTestOptions) {
		for _, n := range nodesToFail {
			// Configure changefeed to emit fatal error on the specified nodes.
			distSQLKnobs := perServerKnobs[n].Knobs.DistSQL.(*execinfra.TestingKnobs)
			var numEmitted int32
			nodeToFail := n
			distSQLKnobs.Changefeed.(*TestingKnobs).BeforeEmitRow = func(ctx context.Context) error {
				// Emit few rows before returning an error.
				if atomic.AddInt32(&numEmitted, 1) > 10 {
					// Mark error as terminal, but make it a bit more
					// interesting by wrapping it few times.
					err := errors.Wrap(
						changefeedbase.WithTerminalError(
							pgerror.Wrapf(
								errors.Newf("synthetic fatal error from node %d", nodeToFail),
								pgcode.Io, "something happened with IO")),
						"while doing something")
					log.Errorf(ctx, "BeforeEmitRow returning error %s", err)
					return err
				}
				return nil
			}
		}

		defer func() {
			// Reset all changefeed knobs.
			for i := 0; i < numNodes; i++ {
				perServerKnobs[i].Knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed = &TestingKnobs{}
			}
		}()

		sinkType := randomSinkTypeWithOptions(opts)
		f, closeSink := makeFeedFactoryWithOptions(t, sinkType, tc, tc.ServerConn(coordinatorID), opts)
		defer closeSink()
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo")
		defer closeFeed(t, feed)

		// We don't know if we picked enterprise or core feed; regardless, consuming
		// from feed should eventually return an error.
		var feedErr error
		for feedErr == nil {
			_, feedErr = feed.Next()
		}
		log.Errorf(context.Background(), "feedErr=%s", feedErr)
		require.Regexp(t, "synthetic fatal error", feedErr)

		// enterprise feeds should also have the job marked failed.
		if jobFeed, ok := feed.(cdctest.EnterpriseTestFeed); ok {
			require.NoError(t, jobFeed.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusFailed }))
		}
	}

	for _, tc := range []struct {
		name        string
		nodesToFail []int
		opts        feedTestOptions
	}{
		{
			name:        "coordinator",
			nodesToFail: []int{coordinatorID},
			opts:        opts,
		},
		{
			name:        "aggregator",
			nodesToFail: []int{2},
			opts:        opts.omitSinks("sinkless"), // Sinkless run on coordinator only.
		},
		{
			name:        "many aggregators",
			nodesToFail: []int{0, 2},
			opts:        opts.omitSinks("sinkless"), // Sinkless run on coordinator only.
		},
	} {
		t.Run(tc.name, func(t *testing.T) { testFn(t, tc.nodesToFail, tc.opts) })
	}
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
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING NOT NULL)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		const baseStmt = `CREATE CHANGEFEED FOR foo WITH resolved = '100ms'`
		foo := feed(t, f, baseStmt)
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

		sqlDB.Exec(t, `ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS (b)`)
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
		assertPayloads(t, foo, []string{
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

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedBackfillCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	rnd, _ := randutil.NewTestRand()
	var maxCheckpointSize int64

	drainUntilTimestamp := func(f cdctest.TestFeed, ts hlc.Timestamp) (err error) {
		var msg *cdctest.TestFeedMessage
		for msg, err = f.Next(); msg != nil; msg, err = f.Next() {
			if msg.Resolved != nil {
				resolvedTs := extractResolvedTimestamp(t, msg)
				if ts.LessEq(resolvedTs) {
					break
				}
			}
		}
		return err
	}

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		valRange := []int{1, 1000}
		sqlDB.Exec(t, `CREATE TABLE foo(a INT PRIMARY KEY)`)
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo (a) SELECT * FROM generate_series(%d, %d)`, valRange[0], valRange[1]))

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")
		tableSpan := fooDesc.PrimaryIndexSpan(s.Codec)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(100)
			return nil
		}

		// Emit resolved events for majority of spans.  Be extra paranoid and ensure that
		// we have at least 1 span for which we don't emit resolved timestamp (to force checkpointing).
		haveGaps := false
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) bool {
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
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.FrontierCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved='100ms'`)
		// Some test feeds (kafka) are not buffered, so we have to consume messages.
		var shouldDrain int32 = 1
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				if shouldDrain == 0 {
					return nil
				}
				m, err := foo.Next()
				if err != nil {
					return err
				}

				if m.Resolved != nil {
					ts := extractResolvedTimestamp(t, m)
					if ts.IsEmpty() {
						return errors.New("unexpected epoch resolved event")
					}
				}
			}
		})

		defer func() {
			closeFeed(t, foo)
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
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) bool {
			if !r.Span.Equal(tableSpan) {
				resolved = append(resolved, r.Span)
			}
			return false
		}

		// Resume job.
		require.NoError(t, jobFeed.Resume())

		// Wait for the high water mark to be non-zero.
		testutils.SucceedsSoon(t, func() error {
			prog := loadProgress()
			if p := prog.GetHighWater(); p != nil && !p.IsEmpty() {
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

		// Consume all potentially buffered kv events
		atomic.StoreInt32(&shouldDrain, 0)
		if err := g.Wait(); err != nil {
			require.NotRegexp(t, "unexpected epoch resolved event", err)
		}
		err := drainUntilTimestamp(foo, *progress.GetHighWater())
		require.NoError(t, err)

		// Verify that the checkpoint does not affect future scans
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'd'`)
		var expected []string
		for i := valRange[0]; i <= valRange[1]; i++ {
			expected = append(expected, fmt.Sprintf(
				`foo: [%d]->{"after": {"a": %d, "b": "d"}}`, i, i,
			))
		}
		assertPayloads(t, foo, expected)
	}

	// TODO(ssd): Tenant testing disabled because of use of DB()
	for _, sz := range []int64{100 << 20, 100} {
		maxCheckpointSize = sz
		cdcTestNamedWithSystem(t, fmt.Sprintf("limit=%s", humanize.Bytes(uint64(sz))), testFn, feedTestForceSink("webhook"))
	}
}

// TestCoreChangefeedBackfillScanCheckpoint tests that a core changefeed
// successfully completes the initial scan of a table when transient errors occur.
// This test only succeeds if checkpoints are taken.
func TestCoreChangefeedBackfillScanCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	skip.UnderShort(t)

	rnd, _ := randutil.NewPseudoRand()

	rowCount := 10000

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo(a INT PRIMARY KEY)`)
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo (a) SELECT * FROM generate_series(%d, %d)`, 0, rowCount))

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill. Also ensure that checkpoint frequency
		// and size are large enough to induce several checkpoints when
		// writing `rowCount` rows.
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 1 + rnd.Int63n(25)
			return nil
		}
		changefeedbase.FrontierCheckpointFrequency.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.FrontierCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 100<<20)

		emittedCount := 0
		knobs.RaiseRetryableError = func() error {
			emittedCount++
			if emittedCount%200 == 0 {
				return errors.New("test transient error")
			}
			return nil
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR TABLE foo`)
		defer closeFeed(t, foo)

		payloads := make([]string, rowCount+1)
		for i := 0; i < rowCount+1; i++ {
			payloads[i] = fmt.Sprintf(`foo: [%d]->{"after": {"a": %d}}`, i, i)
		}
		assertPayloads(t, foo, payloads)
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

func TestCheckpointFrequency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const frontierAdvanced = true
	const frontierDidNotAdvance = false

	// Test the logic around throttling of job progress updates.
	// It's pretty difficult to set up a fast end-to-end test since we need to simulate slow
	// job table update.  Instead, we just test canCheckpointHighWatermark directly.
	ts := timeutil.NewManualTime(timeutil.Now())
	js := newJobState(
		nil, /* job */
		nil, /* core progress */
		cluster.MakeTestingClusterSettings(),
		MakeMetrics(time.Second).(*Metrics), ts,
	)

	ctx := context.Background()

	require.False(t, js.canCheckpointHighWatermark(frontierDidNotAdvance))
	require.True(t, js.canCheckpointHighWatermark(frontierAdvanced))

	// Pretend our mean time to update progress is 1 minute, and we just updated progress.
	require.EqualValues(t, 0, js.checkpointDuration)
	js.checkpointCompleted(ctx, 12*time.Second)
	require.Less(t, int64(0), js.checkpointDuration.Nanoseconds())

	// Even though frontier advanced, we shouldn't checkpoint.
	require.False(t, js.canCheckpointHighWatermark(frontierAdvanced))
	require.True(t, js.progressUpdatesSkipped)

	// Once enough time elapsed, we allow progress update, even if frontier did not advance.
	ts.Advance(js.checkpointDuration)
	require.True(t, js.canCheckpointHighWatermark(frontierDidNotAdvance))

	// If we also specify minimum amount of time between updates, we would skip updates
	// until enough time has elapsed.
	minAdvance := 10 * time.Minute
	changefeedbase.MinHighWaterMarkCheckpointAdvance.Override(ctx, &js.settings.SV, minAdvance)

	require.False(t, js.canCheckpointHighWatermark(frontierAdvanced))
	ts.Advance(minAdvance)
	require.True(t, js.canCheckpointHighWatermark(frontierAdvanced))

	// When we mark checkpoint completion, job state updated to reflect that.
	completionTime := timeutil.Now().Add(time.Hour)
	ts.AdvanceTo(completionTime)
	js.checkpointCompleted(ctx, 42*time.Second)
	require.Equal(t, completionTime, js.lastProgressUpdate)
	require.False(t, js.progressUpdatesSkipped)
}

func TestChangefeedOrderingWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH updated`)
		webhookFoo := foo.(*webhookFeed)
		// retry, then fail, then restart changefeed and successfully send messages
		webhookFoo.mockSink.SetStatusCodes(append(repeatStatusCode(
			http.StatusInternalServerError,
			defaultRetryConfig().MaxRetries+1),
			[]int{http.StatusOK, http.StatusOK, http.StatusOK}...))
		defer closeFeed(t, foo)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'b')`)
		sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
		assertPayloadsPerKeyOrderedStripTs(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "b"}}`,
			`foo: [1]->{"after": null}`,
		})

		webhookFoo.mockSink.SetStatusCodes([]int{http.StatusInternalServerError})
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'c')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (1, 'd')`)
		feedJob := foo.(cdctest.EnterpriseTestFeed)

		// check that running status correctly updates with retryable error
		testutils.SucceedsSoon(t, func() error {
			status, err := feedJob.FetchRunningStatus()
			if err != nil {
				return err
			}
			require.Regexp(t, "500 Internal Server Error", status)
			return nil
		})

		webhookFoo.mockSink.SetStatusCodes([]int{http.StatusOK})
		// retryable error should disappear after request becomes successful
		assertPayloadsPerKeyOrderedStripTs(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "c"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "d"}}`,
		})
	}

	// only used for webhook sink for now since it's the only testfeed where
	// we can control the ordering of errors
	cdcTest(t, testFn, feedTestForceSink("webhook"))
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
			require.NoError(t, feedJob.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusPaused }))

			// Verify job progress contains paused on error status.
			jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
			registry := s.Server.JobRegistry().(*jobs.Registry)
			job, err := registry.LoadJob(context.Background(), jobID)
			require.NoError(t, err)
			require.Contains(t, job.Progress().RunningStatus, "job failed (should fail with custom error) but is being paused because of on_error=pause")
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
			require.NoError(t, feedJob.WaitForStatus(func(s jobs.Status) bool {
				return s == jobs.StatusCancelRequested ||
					s == jobs.StatusReverting ||
					s == jobs.StatusCanceled
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

			require.NoError(t, feedJob.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusFailed }))
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
			require.NoError(t, feedJob.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusFailed }))
			require.EqualError(t, feedJob.FetchTerminalJobErr(), "should fail with custom error")
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestDistSenderRangeFeedPopulatesVirtualTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, cleanup := makeServer(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled='true';`)
	sqlDB.Exec(t, `CREATE TABLE tbl (a INT, b STRING);`)
	sqlDB.Exec(t, `INSERT INTO tbl VALUES (1, 'one'), (2, 'two'), (3, 'three');`)
	sqlDB.Exec(t, `CREATE CHANGEFEED FOR tbl INTO 'null://';`)

	var tableID int
	sqlDB.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name='tbl'").Scan(&tableID)
	tableKey := s.Codec.TablePrefix(uint32(tableID))

	numRangesQuery := fmt.Sprintf(
		"SELECT count(*) FROM crdb_internal.active_range_feeds WHERE range_start LIKE '%s/%%'",
		tableKey)
	sqlDB.CheckQueryResultsRetry(t, numRangesQuery, [][]string{{"1"}})
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

func TestChangefeedEndTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		endTimeReached := make(chan struct{})
		knobs.FeedKnobs.EndTimeReached = func() bool {
			select {
			case <-endTimeReached:
				return true
			default:
				return false
			}
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
		sqlDB.Exec(t, "INSERT INTO foo VALUES (1), (2), (3)")

		fakeEndTime := s.Server.Clock().Now().Add(int64(time.Hour), 0).AsOfSystemTime()
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH end_time = $1", fakeEndTime)
		defer closeFeed(t, feed)

		assertPayloads(t, feed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
		})

		close(endTimeReached)

		testFeed := feed.(cdctest.EnterpriseTestFeed)
		require.NoError(t, testFeed.WaitForStatus(func(s jobs.Status) bool {
			return s == jobs.StatusSucceeded
		}))
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedEndTimeWithCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		endTimeReached := make(chan struct{})
		knobs.FeedKnobs.EndTimeReached = func() bool {
			select {
			case <-endTimeReached:
				return true
			default:
				return false
			}
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
		sqlDB.Exec(t, "INSERT INTO foo VALUES (1), (2), (3)")

		var tsCursor string
		sqlDB.QueryRow(t, "SELECT (cluster_logical_timestamp())").Scan(&tsCursor)
		sqlDB.Exec(t, "INSERT INTO foo VALUES (4), (5), (6)")

		fakeEndTime := s.Server.Clock().Now().Add(int64(time.Hour), 0).AsOfSystemTime()
		feed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH cursor = $1, end_time = $2, no_initial_scan", tsCursor, fakeEndTime)
		defer closeFeed(t, feed)

		assertPayloads(t, feed, []string{
			`foo: [4]->{"after": {"a": 4}}`,
			`foo: [5]->{"after": {"a": 5}}`,
			`foo: [6]->{"after": {"a": 6}}`,
		})
		close(endTimeReached)

		testFeed := feed.(cdctest.EnterpriseTestFeed)
		require.NoError(t, testFeed.WaitForStatus(func(s jobs.Status) bool {
			return s == jobs.StatusSucceeded
		}))
	}

	// TODO: Fix sinkless feeds not providing pre-close events if Next is called
	// after the feed was closed
	cdcTest(t, testFn, feedTestEnterpriseSinks)
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

				// Most changefeed tests can afford to have a race condition between the initial
				// inserts and starting the feed because the output looks the same for an initial
				// scan and an insert. For tests with initial_scan=only, though, we can't start the feed
				// until it's going to see all the initial inserts in the initial scan.
				sqlDB.CheckQueryResultsRetry(t, `SELECT count(*) FROM foo`, [][]string{{`5000`}})

				feed := feed(t, f, changefeedStmt)

				sqlDB.Exec(t, "INSERT INTO foo VALUES (5005), (5007), (5009)")

				g := ctxgroup.WithContext(context.Background())
				var expectedMessages []string
				for i := 1; i <= 5000; i++ {
					expectedMessages = append(expectedMessages, fmt.Sprintf(
						`foo: [%d]->{"after": {"a": %d}}`, i, i,
					))
				}
				var seenMessages []string
				g.Go(func() error {
					for {
						m, err := feed.Next()
						if err != nil {
							return err
						}
						seenMessages = append(seenMessages, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, m.Value))
					}
				})

				jobFeed := feed.(cdctest.EnterpriseTestFeed)
				require.NoError(t, jobFeed.WaitForStatus(func(s jobs.Status) bool {
					return s == jobs.StatusSucceeded
				}))

				closeFeed(t, feed)
				sqlDB.Exec(t, `DROP TABLE foo`)
				_ = g.Wait()
				require.Equal(t, len(expectedMessages), len(seenMessages))
				sort.Strings(expectedMessages)
				sort.Strings(seenMessages)
				for i := range expectedMessages {
					require.Equal(t, expectedMessages[i], seenMessages[i])
				}
			})
		}
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
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
				require.NoError(t, jobFeed.WaitForStatus(func(s jobs.Status) bool {
					return s == jobs.StatusSucceeded
				}))
			})
		}
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
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

func TestChangefeedPredicates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(alias string) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
			sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL, 
  e status DEFAULT 'inactive',
  PRIMARY KEY (a, b)
)`)

			// TODO(#85143): remove schema_change_policy='stop' from this test.
			sqlDB.Exec(t, `
INSERT INTO foo (a, b) VALUES (0, 'zero'), (1, 'one');
INSERT INTO foo (a, b, e) VALUES (2, 'two', 'closed');
`)
			topic, fromClause := "foo", "foo"
			if alias != "" {
				topic, fromClause = "foo", "foo AS "+alias
			}
			feed := feed(t, f, `
CREATE CHANGEFEED 
WITH schema_change_policy='stop'
AS SELECT * FROM `+fromClause+` 
WHERE e IN ('open', 'closed') AND NOT cdc_is_delete()`)
			defer closeFeed(t, feed)

			assertPayloads(t, feed, []string{
				topic + `: [2, "two"]->{"a": 2, "b": "two", "c": null, "e": "closed"}`,
			})

			sqlDB.Exec(t, `
UPDATE foo SET e = 'open', c = 'really open' WHERE a=0;  -- should be emitted
DELETE FROM foo WHERE a=2; -- should be skipped
INSERT INTO foo (a, b, e) VALUES (3, 'tres', 'closed'); -- should be emitted
`)

			assertPayloads(t, feed, []string{
				topic + `: [0, "zero"]->{"a": 0, "b": "zero", "c": "really open", "e": "open"}`,
				topic + `: [3, "tres"]->{"a": 3, "b": "tres", "c": null, "e": "closed"}`,
			})
		}
	}

	testutils.RunTrueAndFalse(t, "alias", func(t *testing.T, useAlias bool) {
		alias := ""
		if useAlias {
			alias = "bar"
		}
		cdcTest(t, testFn(alias))
	})
}

// Some predicates and projections can be verified when creating changefeed.
// The types of errors that can be detected early on is restricted to simple checks
// (such as type checking, non-existent columns, etc).  More complex errors detected
// during execution.
// Verify that's the case.
func TestChangefeedInvalidPredicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, db, stopServer := startTestFullServer(t, feedTestOptions{})
	defer stopServer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL, 
  e status DEFAULT 'inactive',
  PRIMARY KEY (a, b)
)`)

	for _, tc := range []struct {
		name   string
		create string
		err    string
	}{
		{
			name:   "no such column",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT no_such_column FROM foo`,
			err:    `column "foo.no_such_column" does not exist`,
		},
		{
			name:   "wrong type",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE a = 'wrong type'`,
			err:    `could not parse "wrong type" as type int`,
		},
		{
			name:   "invalid enum value",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE e = 'bad'`,
			err:    `invalid input value for enum status: "bad"`,
		},
		{
			name:   "contradiction: a > 1 && a < 1",
			create: `CREATE CHANGEFEED INTO 'null://'  AS SELECT * FROM foo WHERE a > 1 AND a < 1`,
			err:    `does not match any rows`,
		},
		{
			name:   "contradiction: a IS null",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo WHERE a IS NULL`,
			err:    `does not match any rows`,
		},
		{
			name:   "wrong table name",
			create: `CREATE CHANGEFEED INTO 'null://' AS SELECT * FROM foo AS bar WHERE foo.a > 0`,
			err:    `no data source matches prefix: foo in this context`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErr(t, tc.err, tc.create)
		})
	}
}

func TestChangefeedPredicateWithSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes too long under race")
	// TODO(#85143): remove this skip.
	skip.WithIssue(t, 85143)

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
		`foo: [1, "one"]->{"after": {"a": 1, "b": "one", "c": null, "e": "inactive"}}`,
		`foo: [2, "two"]->{"after": {"a": 2, "b": "two", "c": "c string", "e": "open"}}`,
	}

	type testCase struct {
		name           string
		createFeedStmt string   // Create changefeed statement.
		initialPayload []string // Expected payload after create.
		alterStmt      string   // Alter statement to execute.
		afterAlterStmt string   // Execute after alter statement.
		expectErr      string   // Alter may result in changefeed terminating with error.
		payload        []string // Expect the following payload after executing afterAlterStmt.
	}

	testFn := func(tc testCase) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)

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
				require.NoError(t, feedJob.WaitForStatus(
					func(s jobs.Status) bool { return s == jobs.StatusFailed }))
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
			expectErr:      `column "foo.c" does not exist`,
		},
		{
			name:           "drop referenced column filter",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo WHERE c IS NOT NULL",
			initialPayload: []string{
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
			},
			alterStmt: "ALTER TABLE foo DROP COLUMN c",
			expectErr: `column "foo.c" does not exist`,
		},
		{
			name:           "rename referenced column projection",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT a, b, c, e FROM foo",
			initialPayload: initialPayload,
			alterStmt:      "ALTER TABLE foo RENAME COLUMN c TO c_new",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			expectErr:      `column "foo.c" does not exist`,
		},
		{
			name:           "rename referenced column filter",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT * FROM foo WHERE c IS NOT NULL",
			initialPayload: []string{
				`foo: [2, "two"]->{"a": 2, "b": "two", "c": "c string", "e": "open"}`,
			},
			alterStmt:      "ALTER TABLE foo RENAME COLUMN c TO c_new",
			afterAlterStmt: "INSERT INTO foo (a, b) VALUES (3, 'tres')",
			expectErr:      `column "foo.c" does not exist`,
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
				`foo: [2, "two"]->{"after": {"a": 2, "b": "two", "c": "c string", "e": "open"}}`,
			},
			alterStmt:      "ALTER TYPE status RENAME VALUE 'open' TO 'active'",
			afterAlterStmt: "INSERT INTO foo (a, b, e) VALUES (3, 'tres', 'active')",
			expectErr:      `invalid input value for enum status: "open"`,
		},
		{
			name:           "alter enum use correct enum version",
			createFeedStmt: "CREATE CHANGEFEED AS SELECT e, cdc_prev.e AS prev_e FROM foo",
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			cdcTest(t, testFn(tc), feedTestEnterpriseSinks)
		})
	}
}

func startMonitorWithBudget(budget int64) *mon.BytesMonitor {
	mm := mon.NewMonitorWithLimit(
		"test-mm", mon.MemoryResource, budget,
		nil, nil,
		128 /* small allocation increment */, 100,
		cluster.MakeTestingClusterSettings())
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(budget))
	return mm
}

type memoryHoggingSink struct {
	allEmitted chan struct{}
	mu         struct {
		syncutil.Mutex
		expectedRows int
		seenRows     map[string]struct{}
		numFlushes   int
		alloc        kvevent.Alloc
	}
}

var _ Sink = (*memoryHoggingSink)(nil)

func (s *memoryHoggingSink) expectRows(n int) chan struct{} {
	if n <= 0 {
		panic("n<=0")
	}
	s.allEmitted = make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.expectedRows = n
	s.mu.numFlushes = 0
	s.mu.seenRows = make(map[string]struct{})
	s.mu.alloc.Release(context.Background()) // Release leftover alloc
	return s.allEmitted
}

func (s *memoryHoggingSink) Dial() error {
	return nil
}

func (s *memoryHoggingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.seenRows[string(key)] = struct{}{}
	s.mu.alloc.Merge(&alloc)
	if s.mu.expectedRows == len(s.mu.seenRows) && s.allEmitted != nil {
		close(s.allEmitted)
		s.allEmitted = nil
	}
	return nil
}

func (s *memoryHoggingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	panic("should not be called")
}

func (s *memoryHoggingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.numFlushes++
	s.mu.alloc.Release(ctx)
	return nil
}

func (s *memoryHoggingSink) numFlushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numFlushes
}
func (s *memoryHoggingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.alloc.Release(context.Background())
	return nil
}

func TestChangefeedFlushesSinkToReleaseMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	knobs := s.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)

	// Arrange for a small memory budget.
	knobs.MemMonitor = startMonitorWithBudget(4096)

	// Ignore resolved events delivered to this changefeed.  This has
	// an effect of never advancing the frontier, and thus never flushing
	// the sink due to frontier advancement.  The only time we flush the sink
	// is if the memory pressure causes flush request to be delivered.
	knobs.FilterSpanWithMutation = func(_ *jobspb.ResolvedSpan) bool {
		return true
	}

	// Arrange for custom sink to be used -- a sink that does not
	// release its resources.
	sink := &memoryHoggingSink{}
	knobs.WrapSink = func(_ Sink, _ jobspb.JobID) Sink {
		return sink
	}

	// Create table, and insert 123 rows in it -- this fills up
	// our tiny memory buffer (~26 rows do)
	sqlDB.Exec(t, `CREATE TABLE foo(key INT PRIMARY KEY DEFAULT unique_rowid(), val INT)`)
	sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 123)`)

	// Expect 123 rows from backfill.
	allEmitted := sink.expectRows(123)

	sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO 'http://host/does/not/matter'`)

	<-allEmitted
	require.Greater(t, sink.numFlushes(), 0)

	// Insert another set of rows.  This now uses rangefeeds.
	allEmitted = sink.expectRows(123)
	sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 123)`)
	<-allEmitted
	require.Greater(t, sink.numFlushes(), 0)
}

func TestChangefeedMultiPodTenantPlanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "may time out due to multiple servers")

	// Record the number of aggregators in planning
	aggregatorCount := 0

	// Create 2 connections of the same tenant on a cluster to have 2 pods
	tc, _, cleanupDB := startTestCluster(t)
	defer cleanupDB()

	tenantKnobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
			OnDistflowSpec: func(aggregatorSpecs []*execinfrapb.ChangeAggregatorSpec, _ *execinfrapb.ChangeFrontierSpec) {
				aggregatorCount = len(aggregatorSpecs)
			},
		}},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		Server:           &server.TestingKnobs{},
	}
	tenant1Args := base.TestTenantArgs{
		TenantID:     serverutils.TestTenantID(),
		TestingKnobs: tenantKnobs,
	}
	tenant1Server, tenant1DB := serverutils.StartTenant(t, tc.Server(0), tenant1Args)
	tenantRunner := sqlutils.MakeSQLRunner(tenant1DB)
	tenantRunner.ExecMultiple(t, strings.Split(serverSetupStatements, ";")...)
	sql1 := sqlutils.MakeSQLRunner(tenant1DB)
	defer tenant1DB.Close()

	tenant2Args := tenant1Args
	tenant2Args.DisableCreateTenant = true
	_, db2 := serverutils.StartTenant(t, tc.Server(1), tenant2Args)
	defer db2.Close()

	// Ensure both pods can be assigned work
	waitForTenantPodsActive(t, tenant1Server, 2)

	feedFactory, cleanupSink := makeFeedFactory(t, randomSinkType(feedTestEnterpriseSinks), tenant1Server, tenant1DB)
	defer cleanupSink()

	// Run a changefeed across two tables to guarantee multiple spans that can be spread across the aggregators
	sql1.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
	sql1.Exec(t, "INSERT INTO foo VALUES (1), (2)")
	sql1.Exec(t, "CREATE TABLE bar (b INT PRIMARY KEY)")
	sql1.Exec(t, "INSERT INTO bar VALUES (1), (2)")

	foo := feed(t, feedFactory, "CREATE CHANGEFEED FOR foo, bar")
	defer closeFeed(t, foo)

	assertPayloads(t, foo, []string{
		`foo: [1]->{"after": {"a": 1}}`,
		`foo: [2]->{"after": {"a": 2}}`,
		`bar: [1]->{"after": {"b": 1}}`,
		`bar: [2]->{"after": {"b": 2}}`,
	})

	require.Equal(t, 2, aggregatorCount)
}

func TestChangefeedCreateTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)

	t.Run(`core_sink_type`, func(t *testing.T) {
		coreSink, cleanup := sqlutils.PGUrl(t, s.Server.SQLAddr(), t.Name(), url.User(username.RootUser))
		defer cleanup()
		coreFeedFactory := makeSinklessFeedFactory(s.Server, coreSink, nil)

		beforeCreateSinkless := timeutil.Now()
		coreFeed := feed(t, coreFeedFactory, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, coreFeed)

		createLogs := checkCreateChangefeedLogs(t, beforeCreateSinkless.UnixNano())
		require.Equal(t, 1, len(createLogs))
		require.Equal(t, createLogs[0].SinkType, "core")
	})

	t.Run(`gcpubsub_sink_type with options`, func(t *testing.T) {
		pubsubFeedFactory := makePubsubFeedFactory(s.Server, s.DB)
		beforeCreatePubsub := timeutil.Now()
		pubsubFeed := feed(t, pubsubFeedFactory, `CREATE CHANGEFEED FOR foo, bar WITH resolved="10s", no_initial_scan`)
		defer closeFeed(t, pubsubFeed)

		createLogs := checkCreateChangefeedLogs(t, beforeCreatePubsub.UnixNano())
		require.Equal(t, 1, len(createLogs))
		require.Equal(t, createLogs[0].SinkType, `gcpubsub`)
		require.Equal(t, createLogs[0].NumTables, int32(2))
		require.Equal(t, createLogs[0].Resolved, `10s`)
		require.Equal(t, createLogs[0].InitialScan, `no`)
	})
}

// Note that closeFeed needs to be called in order for the logs to be detected
func TestChangefeedFailedTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	waitForLogs := func(t *testing.T, startTime time.Time) []eventpb.ChangefeedFailed {
		var logs []eventpb.ChangefeedFailed
		testutils.SucceedsSoon(t, func() error {
			logs = checkChangefeedFailedLogs(t, startTime.UnixNano())
			if len(logs) < 1 {
				return fmt.Errorf("no logs found")
			}
			return nil
		})
		return logs
	}

	t.Run(`connection_closed`, func(t *testing.T) {
		s, stopServer := makeServer(t)
		defer stopServer()

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		coreFactory, sinkCleanup := makeFeedFactory(t, "sinkless", s.Server, s.DB)
		coreFeed := feed(t, coreFactory, `CREATE CHANGEFEED FOR foo`)
		assertPayloads(t, coreFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})
		beforeCoreSinkClose := timeutil.Now()

		sinkCleanup()
		closeFeed(t, coreFeed)

		failLogs := waitForLogs(t, beforeCoreSinkClose)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.ConnectionClosed)
	})

	cdcTestNamed(t, "user_input", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		beforeCreate := timeutil.Now()
		_, err := f.Feed(`CREATE CHANGEFEED FOR foo, invalid_table`)
		require.Error(t, err)

		failLogs := waitForLogs(t, beforeCreate)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.UserInput)
	}, feedTestEnterpriseSinks)

	cdcTestNamed(t, "unknown_error", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			return changefeedbase.WithTerminalError(errors.New("should fail"))
		}

		beforeCreate := timeutil.Now()
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH on_error=FAIL`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'next')`)
		feedJob := foo.(cdctest.EnterpriseTestFeed)
		require.NoError(t, feedJob.WaitForStatus(func(s jobs.Status) bool { return s == jobs.StatusFailed }))

		closeFeed(t, foo)
		failLogs := waitForLogs(t, beforeCreate)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.UnknownError)
		require.Equal(t, failLogs[0].SinkType, `gcpubsub`)
		require.Equal(t, failLogs[0].NumTables, int32(1))
	}, feedTestForceSink("pubsub"))
}

func TestChangefeedTestTimesOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		nada := feed(t, f, "CREATE CHANGEFEED FOR foo WITH resolved='100ms'")
		defer closeFeed(t, nada)

		expectResolvedTimestamp(t, nada) // Make sure feed is running.

		const expectTimeout = 500 * time.Millisecond
		var observedError error
		require.NoError(t,
			testutils.SucceedsWithinError(func() error {
				observedError = withTimeout(
					nada, expectTimeout,
					func(ctx context.Context) error {
						return assertPayloadsBaseErr(
							ctx, nada, []string{`nada: [2]->{"after": {}}`}, false, false)
					})
				return nil
			}, 20*expectTimeout))

		require.Error(t, observedError)
	}

	cdcTest(t, testFn)
}

// Regression for #85008.
func TestSchemachangeDoesNotBreakSinklessFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
}

func TestChangefeedKafkaMessageTooLarge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		changefeedbase.BatchReductionRetryEnabled.Override(
			context.Background(), &s.Server.ClusterSettings().SV, true)

		knobs := f.(*kafkaFeedFactory).knobs
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)

		t.Run(`succeed eventually if batches are rejected by the server for being too large`, func(t *testing.T) {
			// MaxMessages of 0 means unlimited
			foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH kafka_sink_config='{"Flush": {"MaxMessages": 0}}'`)
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1}}`,
				`foo: [2]->{"after": {"a": 2}}`,
			})

			// Messages should be sent by a smaller and smaller MaxMessages config
			// only until ErrMessageSizeTooLarge is no longer returned.
			knobs.kafkaInterceptor = func(m *sarama.ProducerMessage, client kafkaClient) error {
				maxMessages := client.Config().Producer.Flush.MaxMessages
				if maxMessages == 0 || maxMessages >= 250 {
					return sarama.ErrMessageSizeTooLarge
				}
				require.Greater(t, maxMessages, 100)
				return nil
			}

			sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)
			assertPayloads(t, foo, []string{
				`foo: [3]->{"after": {"a": 3}}`,
				`foo: [4]->{"after": {"a": 4}}`,
			})
			sqlDB.Exec(t, `INSERT INTO foo VALUES (5)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (6)`)
			assertPayloads(t, foo, []string{
				`foo: [5]->{"after": {"a": 5}}`,
				`foo: [6]->{"after": {"a": 6}}`,
			})
		})

		t.Run(`succeed against a large backfill`, func(t *testing.T) {
			sqlDB.Exec(t, `CREATE TABLE large (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO large (a) SELECT * FROM generate_series(1, 2000);`)

			foo := feed(t, f, `CREATE CHANGEFEED FOR large WITH kafka_sink_config='{"Flush": {"MaxMessages": 1000}}'`)
			defer closeFeed(t, foo)

			rnd, _ := randutil.NewPseudoRand()

			knobs.kafkaInterceptor = func(m *sarama.ProducerMessage, client kafkaClient) error {
				if client.Config().Producer.Flush.MaxMessages > 1 && rnd.Int()%5 == 0 {
					return sarama.ErrMessageSizeTooLarge
				}
				return nil
			}

			var expected []string
			for i := 1; i <= 2000; i++ {
				expected = append(expected, fmt.Sprintf(
					`large: [%d]->{"after": {"a": %d}}`, i, i,
				))
			}
			assertPayloads(t, foo, expected)
		})

		// Validate that different failure scenarios result in a full changefeed retry
		sqlDB.Exec(t, `CREATE TABLE errors (a INT PRIMARY KEY);`)
		sqlDB.Exec(t, `INSERT INTO errors (a) SELECT * FROM generate_series(1, 1000);`)
		for _, failTest := range []struct {
			failInterceptor func(m *sarama.ProducerMessage, client kafkaClient) error
			errMsg          string
		}{
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					return sarama.ErrMessageSizeTooLarge
				},
				"kafka server: Message was too large, server rejected it to avoid allocation error",
			},
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					return errors.Errorf("unrelated error")
				},
				"unrelated error",
			},
			{
				func(m *sarama.ProducerMessage, client kafkaClient) error {
					maxMessages := client.Config().Producer.Flush.MaxMessages
					if maxMessages == 0 || maxMessages > 250 {
						return sarama.ErrMessageSizeTooLarge
					}
					return errors.Errorf("unrelated error mid-retry")
				},
				"unrelated error mid-retry",
			},
			{
				func() func(m *sarama.ProducerMessage, client kafkaClient) error {
					// Trigger an internal retry for the first message but have successive
					// messages throw a non-retryable error. This can happen in practice
					// when the second message is on a different topic to the first.
					startedBuffering := false
					return func(m *sarama.ProducerMessage, client kafkaClient) error {
						if !startedBuffering {
							startedBuffering = true
							return sarama.ErrMessageSizeTooLarge
						}
						return errors.Errorf("unrelated error mid-buffering")
					}
				}(),
				"unrelated error mid-buffering",
			},
		} {
			t.Run(fmt.Sprintf(`eventually surface error for retry: %s`, failTest.errMsg), func(t *testing.T) {
				knobs.kafkaInterceptor = failTest.failInterceptor
				foo := feed(t, f, `CREATE CHANGEFEED FOR errors WITH kafka_sink_config='{"Flush": {"MaxMessages": 0}}'`)
				defer closeFeed(t, foo)

				feedJob := foo.(cdctest.EnterpriseTestFeed)

				// check that running status correctly updates with retryable error
				testutils.SucceedsSoon(t, func() error {
					status, err := feedJob.FetchRunningStatus()
					if err != nil {
						return err
					}

					if !strings.Contains(status, failTest.errMsg) {
						return errors.Errorf("expected error to contain '%s', got: %v", failTest.errMsg, status)
					}
					return nil
				})
			})
		}
	}

	cdcTest(t, testFn, feedTestForceSink(`kafka`))
}

// Regression for #85902.
func TestRedactedSchemaRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE test_table (id INT PRIMARY KEY, i int, j int)`)

		userInfoToRedact := "7JHKUXMWYD374NV:secret-key"
		registryURI := fmt.Sprintf("https://%s@psrc-x77pq.us-central1.gcp.confluent.cloud:443", userInfoToRedact)

		changefeedDesc := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE test_table WITH updated,
					confluent_schema_registry =
					"%s";`, registryURI)
		registryURIWithRedaction := strings.Replace(registryURI, userInfoToRedact, "redacted", 1)
		cf := feed(t, f, changefeedDesc)
		defer closeFeed(t, cf)

		var description string
		sqlDB.QueryRow(t, "SELECT description from [SHOW CHANGEFEED JOBS]").Scan(&description)

		assert.Contains(t, description, registryURIWithRedaction)
	}

	// kafka supports the confluent_schema_registry option.
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

type echoResolver struct {
	result []roachpb.Spans
	pos    int
}

func (r *echoResolver) getRangesForSpans(
	_ context.Context, _ []roachpb.Span,
) (spans []roachpb.Span, _ error) {
	spans = r.result[r.pos]
	r.pos++
	return spans, nil
}

func TestPartitionSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	partitions := func(p ...sql.SpanPartition) []sql.SpanPartition {
		return p
	}
	mkPart := func(n base.SQLInstanceID, spans ...roachpb.Span) sql.SpanPartition {
		return sql.SpanPartition{SQLInstanceID: n, Spans: spans}
	}
	mkSpan := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: []byte(start), EndKey: []byte(end)}
	}
	spans := func(s ...roachpb.Span) roachpb.Spans {
		return s
	}
	const sensitivity = 0.01

	for i, tc := range []struct {
		input   []sql.SpanPartition
		resolve []roachpb.Spans
		expect  []sql.SpanPartition
	}{
		{
			input: partitions(
				mkPart(1, mkSpan("a", "j")),
				mkPart(2, mkSpan("j", "q")),
				mkPart(3, mkSpan("q", "z")),
			),
			// 6 total ranges, 2 per node.
			resolve: []roachpb.Spans{
				spans(mkSpan("a", "c"), mkSpan("c", "e"), mkSpan("e", "j")),
				spans(mkSpan("j", "q")),
				spans(mkSpan("q", "y"), mkSpan("y", "z")),
			},
			expect: partitions(
				mkPart(1, mkSpan("a", "e")),
				mkPart(2, mkSpan("e", "q")),
				mkPart(3, mkSpan("q", "z")),
			),
		},
		{
			input: partitions(
				mkPart(1, mkSpan("a", "c"), mkSpan("e", "p"), mkSpan("r", "z")),
				mkPart(2),
				mkPart(3, mkSpan("c", "e"), mkSpan("p", "r")),
			),
			// 5 total ranges -- on 2 nodes; target should be 1 per node.
			resolve: []roachpb.Spans{
				spans(mkSpan("a", "c"), mkSpan("e", "p"), mkSpan("r", "z")),
				spans(),
				spans(mkSpan("c", "e"), mkSpan("p", "r")),
			},
			expect: partitions(
				mkPart(1, mkSpan("a", "c"), mkSpan("e", "p")),
				mkPart(2, mkSpan("r", "z")),
				mkPart(3, mkSpan("c", "e"), mkSpan("p", "r")),
			),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			sp, err := rebalanceSpanPartitions(context.Background(),
				&echoResolver{result: tc.resolve}, sensitivity, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expect, sp)
		})
	}
}

// TestPubsubValidationErrors tests error messages during pubsub sink URI validations.
func TestPubsubValidationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()

	for _, tc := range []struct {
		name          string
		uri           string
		expectedError string
	}{
		{
			name:          "project name",
			expectedError: "missing project name",
			uri:           "gcpubsub://?region={region}",
		},
		{
			name:          "region",
			expectedError: "region query parameter not found",
			uri:           "gcpubsub://myproject",
		},
		{
			name:          "credentials for default auth specified",
			expectedError: "missing credentials parameter",
			uri:           "gcpubsub://myproject?region={region}&AUTH=specified",
		},
		{
			name:          "base64",
			expectedError: "illegal base64 data",
			uri:           "gcpubsub://myproject?region={region}&CREDENTIALS={credentials}",
		},
		{
			name:          "invalid json",
			expectedError: "creating credentials from json: invalid character",
			uri: fmt.Sprintf("gcpubsub://myproject?region={region}&CREDENTIALS=%s",
				base64.StdEncoding.EncodeToString([]byte("invalid json"))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErr(t, tc.expectedError, fmt.Sprintf("CREATE CHANGEFEED FOR foo INTO '%s'", tc.uri))
		})
	}
}
