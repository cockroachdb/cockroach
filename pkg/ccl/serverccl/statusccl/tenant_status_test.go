// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statusccl

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/serverccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This value is arbitrary and needs to be enough in case of slow tests.
const LastReadThresholdSeconds = 30

func TestTenantStatusAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)
	defer s.SetupSingleFileLogging()()

	// The liveness session might expire before the stress race can finish.
	skip.UnderRace(t, "expensive tests")

	ctx := context.Background()

	var knobs base.TestingKnobs
	knobs.SQLStatsKnobs = sqlstats.CreateTestingKnobs()
	knobs.SpanConfig = &spanconfig.TestingKnobs{
		// Some of these subtests expect multiple (uncoalesced) tenant ranges.
		StoreDisableCoalesceAdjacent: true,
	}

	testHelper := serverccl.NewTestTenantHelper(t, 3 /* tenantClusterSize */, knobs)
	defer testHelper.Cleanup(ctx, t)

	// Speed up propagation of tenant capability changes.
	db := testHelper.HostCluster().ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10 ms'")

	t.Run("reset_sql_stats", func(t *testing.T) {
		skip.UnderDeadlockWithIssue(t, 99559)
		testResetSQLStatsRPCForTenant(ctx, t, testHelper)
	})

	t.Run("reset_index_usage_stats", func(t *testing.T) {
		testResetIndexUsageStatsRPCForTenant(ctx, t, testHelper)
	})

	t.Run("table_index_stats", func(t *testing.T) {
		testTableIndexStats(ctx, t, testHelper)
	})

	t.Run("tenant_contention_event", func(t *testing.T) {
		testContentionEventsForTenant(ctx, t, testHelper)
	})

	t.Run("tenant_cancel_session", func(t *testing.T) {
		testTenantStatusCancelSession(t, testHelper)
	})

	t.Run("tenant_cancel_session_error_messages", func(t *testing.T) {
		testTenantStatusCancelSessionErrorMessages(t, testHelper)
	})

	t.Run("tenant_cancel_query", func(t *testing.T) {
		testTenantStatusCancelQuery(ctx, t, testHelper)
	})

	t.Run("tenant_cancel_query_error_messages", func(t *testing.T) {
		testTenantStatusCancelQueryErrorMessages(t, testHelper)
	})

	t.Run("index_usage_stats", func(t *testing.T) {
		testIndexUsageForTenants(t, testHelper)
	})

	t.Run("txn_id_resolution", func(t *testing.T) {
		skip.UnderDeadlockWithIssue(t, 99770)
		testTxnIDResolutionRPC(ctx, t, testHelper)
	})

	t.Run("tenant_ranges", func(t *testing.T) {
		testTenantRangesRPC(ctx, t, testHelper)
	})

	t.Run("ranges", func(t *testing.T) {
		testRangesRPC(ctx, t, testHelper)
	})

	t.Run("tenant_auth_statement", func(t *testing.T) {
		testTenantAuthOnStatements(ctx, t, testHelper)
	})

	t.Run("tenant_logs", func(t *testing.T) {
		testTenantLogs(ctx, t, testHelper)
	})

	t.Run("tenant_hot_ranges", func(t *testing.T) {
		testTenantHotRanges(ctx, t, testHelper)
	})

	t.Run("tenant_span_stats", func(t *testing.T) {
		testTenantSpanStats(ctx, t, testHelper)
	})

	t.Run("tenant_nodes_capability", func(t *testing.T) {
		testTenantNodesCapability(ctx, t, testHelper)
	})
}

func testTenantSpanStats(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	tenantA := helper.TestCluster().Tenant(0)
	tenantB := helper.ControlCluster().Tenant(0)

	aSpan := tenantA.GetTenant().Codec().TenantSpan()
	bSpan := tenantB.GetTenant().Codec().TenantSpan()

	t.Run("test tenant permissioning", func(t *testing.T) {
		req := roachpb.SpanStatsRequest{
			NodeID: "0",
			Spans:  []roachpb.Span{aSpan},
		}
		resp := roachpb.SpanStatsResponse{}

		client := helper.TestCluster().TenantHTTPClient(t, 1, false)
		err := client.PostJSONChecked("/_status/span", &req, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Forbidden")

		// VIEWCLUSTERMETADATA should allow the user to see the span stats.
		grantStmt := `GRANT SYSTEM VIEWCLUSTERMETADATA TO authentic_user_noadmin;`
		helper.TestCluster().TenantConn(0).Exec(t, grantStmt)

		err = client.PostJSONChecked("/_status/span", &req, &resp)
		require.NoError(t, err)
		require.NotEmpty(t, resp.SpanToStats)

		revokeStmt := `REVOKE SYSTEM VIEWCLUSTERMETADATA FROM authentic_user_noadmin;`
		helper.TestCluster().TenantConn(0).Exec(t, revokeStmt)

		adminClient := helper.TestCluster().TenantHTTPClient(t, 1, true)
		adminClient.PostJSON("/_status/span", &req, &resp)
		require.Greaterf(t, resp.SpanToStats[aSpan.String()].RangeCount, int32(0), "positive range count")
	})

	t.Run("test tenant isolation", func(t *testing.T) {
		_, err := tenantA.TenantStatusSrv().(serverpb.TenantStatusServer).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: "0", // 0 indicates we want stats from all nodes.
				Spans:  []roachpb.Span{bSpan},
			})
		require.Error(t, err)
	})

	t.Run("test invalid request payload", func(t *testing.T) {
		_, err := tenantA.TenantStatusSrv().(serverpb.TenantStatusServer).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID:   "0", // 0 indicates we want stats from all nodes.
				StartKey: roachpb.RKey(aSpan.Key),
				EndKey:   roachpb.RKey(aSpan.EndKey),
			})
		require.ErrorContains(t, err, `span stats request - unexpected populated legacy fields (StartKey, EndKey)`)
	})

	t.Run("test exceed span request limit", func(t *testing.T) {
		// Set the span batch limit to 1.
		_, err := helper.HostCluster().ServerConn(0).Exec(`SET CLUSTER SETTING server.span_stats.span_batch_limit = 1`)
		require.NoError(t, err)
		res, err := tenantA.TenantStatusSrv().(serverpb.TenantStatusServer).
			SpanStats(ctx,
				&roachpb.SpanStatsRequest{
					NodeID: "0", // 0 indicates we want stats from all nodes.
					Spans:  []roachpb.Span{aSpan, aSpan},
				})
		require.NoError(t, err)
		require.Contains(t, res.SpanToStats, aSpan.String())
		// Reset the span batch limit to default.
		_, err = helper.HostCluster().ServerConn(0).Exec(`RESET CLUSTER SETTING server.span_stats.span_batch_limit`)
		require.NoError(t, err)
	})

	t.Run("test KV node fan-out", func(t *testing.T) {
		_, tID, err := keys.DecodeTenantPrefix(aSpan.Key)
		require.NoError(t, err)
		tPrefix := keys.MakeTenantPrefix(tID)

		makeKey := func(keys ...[]byte) roachpb.Key {
			return bytes.Join(keys, nil)

		}

		// Create a new range in this tenant.
		newRangeKey := makeKey(tPrefix, roachpb.Key("c"))
		_, newDesc, err := helper.HostCluster().Server(0).SplitRange(newRangeKey)
		require.NoError(t, err)

		// Wait until the range split occurs.
		testutils.SucceedsSoon(t, func() error {
			desc, err := helper.HostCluster().LookupRange(newRangeKey)
			require.NoError(t, err)
			if !desc.StartKey.Equal(newDesc.StartKey) {
				return errors.New("range has not split")
			}
			return nil
		})

		// Wait for the new range to replicate.
		err = helper.HostCluster().WaitForFullReplication()
		require.NoError(t, err)

		// Create 6 new keys across the tenant's ranges.
		incKeys := []string{"a", "b", "bb", "d", "e", "f"}
		for _, incKey := range incKeys {
			// Prefix each key appropriately for this tenant.
			k := makeKey(keys.MakeTenantPrefix(tID), []byte(incKey))
			if _, err := helper.HostCluster().Server(0).DB().Inc(ctx, k, 5); err != nil {
				t.Fatal(err)
			}
		}

		// Make a multi-span call
		type spanCase struct {
			span               roachpb.Span
			expectedRangeCount int32
			expectedLiveCount  int64
		}
		spanCases := []spanCase{
			{
				// "a", "b" - single range, single key
				span: roachpb.Span{
					Key:    makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[0])),
					EndKey: makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[1])),
				},
				expectedRangeCount: 1,
				expectedLiveCount:  1,
			},
			{
				// "d", "f" - single range, multiple keys
				span: roachpb.Span{
					Key:    makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[3])),
					EndKey: makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[5])),
				},
				expectedRangeCount: 1,
				expectedLiveCount:  2,
			},
			{
				// "bb", "e" - multiple ranges, multiple keys
				span: roachpb.Span{
					Key:    makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[2])),
					EndKey: makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[4])),
				},
				expectedRangeCount: 2,
				expectedLiveCount:  2,
			},

			{
				// "a", "d" - multiple ranges, multiple keys
				span: roachpb.Span{
					Key:    makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[0])),
					EndKey: makeKey(keys.MakeTenantPrefix(tID), []byte(incKeys[3])),
				},
				expectedRangeCount: 2,
				expectedLiveCount:  3,
			},
		}

		var spans []roachpb.Span
		for _, sc := range spanCases {
			spans = append(spans, sc.span)
		}

		testutils.SucceedsSoon(t, func() error {
			stats, err := tenantA.TenantStatusSrv().(serverpb.TenantStatusServer).SpanStats(ctx,
				&roachpb.SpanStatsRequest{
					NodeID: "0", // 0 indicates we want stats from all nodes.
					Spans:  spans,
				})

			require.NoError(t, err)

			for _, sc := range spanCases {
				spanStats := stats.SpanToStats[sc.span.String()]
				if sc.expectedRangeCount != spanStats.RangeCount {
					return errors.Newf("mismatch on expected range count for span case with span %v", sc.span.String())
				}

				if sc.expectedLiveCount != spanStats.TotalStats.LiveCount {
					return errors.Newf("mismatch on expected live count for span case with span %v", sc.span.String())
				}
			}

			return nil
		})

	})

}

func testTenantNodesCapability(
	ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper,
) {
	tenant := helper.TestCluster().TenantStatusSrv(0)

	_, err := tenant.NodesUI(ctx, &serverpb.NodesRequest{})
	require.Error(t, err)

	db := helper.HostCluster().ServerConn(0)
	_, err = db.Exec("ALTER TENANT [10] GRANT CAPABILITY can_view_node_info=true\n")
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		resp, err := tenant.NodesUI(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}
		if len(resp.Nodes) == 0 || len(resp.LivenessByNodeID) == 0 {
			return errors.New("missing nodes or liveness data")
		}
		return nil
	})
}

func testTenantLogs(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	tenantA := helper.TestCluster().TenantStatusSrv(0)

	logsResp, err := tenantA.Logs(ctx, &serverpb.LogsRequest{
		NodeId: helper.TestCluster().Tenant(0).GetTenant().SQLInstanceID().String(),
		Redact: false,
	})
	require.NoError(t, err)
	require.NotEmpty(t, logsResp.Entries)

	logsFilesListResp, err := tenantA.LogFilesList(ctx, &serverpb.LogFilesListRequest{
		NodeId: helper.TestCluster().Tenant(0).GetTenant().SQLInstanceID().String(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, logsFilesListResp.Files)

	logsFileResp, err := tenantA.LogFile(ctx, &serverpb.LogFileRequest{
		NodeId: helper.TestCluster().Tenant(0).GetTenant().SQLInstanceID().String(),
		File:   logsFilesListResp.Files[0].Name,
	})
	require.NoError(t, err)
	require.NotEmpty(t, logsFileResp.Entries)
	systemTenantIDStr := fmt.Sprintf("%d", roachpb.SystemTenantID.InternalValue)
	for _, resp := range []*serverpb.LogEntriesResponse{logsResp, logsFileResp} {
		for _, entry := range resp.Entries {
			// Logs belonging to the system tenant ID should never show up in a response
			// provided by an app tenant server. Verify this filtering.
			require.NotEqual(t, entry.TenantID, systemTenantIDStr)
		}
	}
}

func TestTenantCannotSeeNonTenantStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressWithIssue(t, 113984)

	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
				}},
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0 /* idx */)

	tenant, sqlDB := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10 /* id */),
		TestingKnobs: base.TestingKnobs{
			SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
		},
	})

	systemLayer := testCluster.Server(1 /* idx */).SystemLayer()

	tenantStatusServer := tenant.StatusServer().(serverpb.SQLStatusServer)

	type testCase struct {
		stmt        string
		fingerprint string
	}

	testCaseTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_t`},
		{stmt: `SET database = roachblog_t`},
		{stmt: `CREATE TABLE posts_t (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_t VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_t VALUES (_, __more__)`,
		},
		{stmt: `SELECT * FROM posts_t`},
	}

	for _, stmt := range testCaseTenant {
		_, err := sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}

	err := sqlDB.Close()
	require.NoError(t, err)

	testCaseNonTenant := []testCase{
		{stmt: `CREATE DATABASE roachblog_nt`},
		{stmt: `SET database = roachblog_nt`},
		{stmt: `CREATE TABLE posts_nt (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:        `INSERT INTO posts_nt VALUES (1, 'foo')`,
			fingerprint: `INSERT INTO posts_nt VALUES (_, __more__)`,
		},
		{stmt: `SELECT * FROM posts_nt`},
	}

	sqlDB = systemLayer.SQLConn(t)

	for _, stmt := range testCaseNonTenant {
		_, err = sqlDB.Exec(stmt.stmt)
		require.NoError(t, err)
	}
	err = sqlDB.Close()
	require.NoError(t, err)

	request := &serverpb.StatementsRequest{}
	var tenantStats *serverpb.StatementsResponse

	// Populate `tenantStats`. The tenant server
	// `Statements` and `CombinedStatements` methods are backed by the
	// sqlinstance system which uses a cache populated through rangefeed
	// for keeping track of SQL pod data. We use `SucceedsSoon` to eliminate
	// race condition with the sqlinstance cache population such as during
	// a stress test.
	testutils.SucceedsSoon(t, func() error {
		tenantStats, err = tenantStatusServer.Statements(ctx, request)
		if err != nil {
			return err
		}
		if tenantStats == nil || len(tenantStats.Statements) == 0 {
			return errors.New("tenant statements are unexpectedly empty")
		}

		return nil
	})

	path := "/_status/statements"
	var systemLayerStats serverpb.StatementsResponse
	err = serverutils.GetJSONProto(systemLayer, path, &systemLayerStats)
	require.NoError(t, err)

	checkStatements := func(t *testing.T, tc []testCase, actual *serverpb.StatementsResponse) {
		t.Helper()
		var expectedStatements []string
		for _, stmt := range tc {
			var expectedStmt = stmt.stmt
			if stmt.fingerprint != "" {
				expectedStmt = stmt.fingerprint
			}
			expectedStatements = append(expectedStatements, expectedStmt)
		}

		var actualStatements []string
		for _, respStatement := range actual.Statements {
			if respStatement.Stats.FailureCount > 0 {
				// We ignore failed statements here as the INSERT statement can fail and
				// be automatically retried, confusing the test success check.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
				// We ignore internal queries, these are not relevant for the
				// validity of this test.
				continue
			}
			actualStatements = append(actualStatements, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStatements)
		sort.Strings(actualStatements)

		require.Equal(t, expectedStatements, actualStatements)
	}

	// First we verify that we have expected stats from tenants.
	t.Run("tenant-stats", func(t *testing.T) {
		checkStatements(t, testCaseTenant, tenantStats)
	})

	// Now we verify the non tenant stats are what we expected.
	t.Run("non-tenant-stats", func(t *testing.T) {
		checkStatements(t, testCaseNonTenant, &systemLayerStats)
	})

	// Now we verify that tenant and non-tenant have no visibility into each other's stats.
	t.Run("overlap", func(t *testing.T) {
		for _, tenantStmt := range tenantStats.Statements {
			for _, systemLayerStmt := range systemLayerStats.Statements {
				require.NotEqual(t, tenantStmt, systemLayerStmt, "expected tenant to have no visibility to system layer's statement stats, but found:", systemLayerStmt)
			}
		}

		for _, tenantTxn := range tenantStats.Transactions {
			for _, systemLayerTxn := range systemLayerStats.Transactions {
				require.NotEqual(t, tenantTxn, systemLayerTxn, "expected tenant to have no visibility to system layer's transaction stats, but found:", systemLayerTxn)
			}
		}

	})
}

func testResetSQLStatsRPCForTenant(
	ctx context.Context, t *testing.T, testHelper serverccl.TenantTestHelper,
) {
	stmts := []string{
		"SELECT 1",
		"SELECT 1, 1",
		"SELECT 1, 1, 1",
	}

	testCluster := testHelper.TestCluster()
	controlCluster := testHelper.ControlCluster()

	// Set automatic flush to some long duration we'll never hit to
	// ensure tests are deterministic.
	testCluster.TenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '24h'")
	controlCluster.TenantConn(0 /* idx */).
		Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '24h'")

	defer func() {
		// Cleanup
		testCluster.TenantConn(0 /* idx */).
			Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '10m'")
		controlCluster.TenantConn(0 /* idx */).
			Exec(t, "SET CLUSTER SETTING sql.stats.flush.interval = '10m'")

	}()

	for _, flushed := range []bool{false, true} {
		testTenant := testCluster.Tenant(serverccl.RandomServer)
		testTenantConn := testTenant.GetTenantConn()
		t.Run(fmt.Sprintf("flushed=%t", flushed), func(t *testing.T) {
			// Clears the SQL Stats at the end of each test via builtin.
			defer func() {
				testTenantConn.Exec(t, "SELECT crdb_internal.reset_sql_stats()")
				controlCluster.TenantConn(serverccl.RandomServer).Exec(t, "SELECT crdb_internal.reset_sql_stats()")
			}()

			for _, stmt := range stmts {
				testTenantConn.Exec(t, stmt)
				controlCluster.TenantConn(serverccl.RandomServer).Exec(t, stmt)
			}

			if flushed {
				testTenantServer := testTenant.TenantSQLServer()
				testTenantServer.GetSQLStatsProvider().MaybeFlush(
					ctx,
					testTenant.GetTenant().AppStopper(),
				)
				randomTenantServer := controlCluster.TenantSQLServer(serverccl.RandomServer)
				randomTenantServer.GetSQLStatsProvider().MaybeFlush(
					ctx,
					controlCluster.Tenant(0).GetTenant().AppStopper(),
				)
			}

			status := testTenant.TenantStatusSrv()

			statsPreReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: flushed,
			})
			require.NoError(t, err)

			require.NotEqual(t, 0, len(statsPreReset.Statements),
				"expected to find stats for at least one statement, but found: %d", len(statsPreReset.Statements))
			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsPreReset, "test")

			_, err = status.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{
				ResetPersistedStats: true,
			})
			require.NoError(t, err)

			statsPostReset, err := status.Statements(ctx, &serverpb.StatementsRequest{
				Combined: flushed,
			})
			require.NoError(t, err)

			if !statsPostReset.LastReset.After(statsPreReset.LastReset) {
				t.Fatal("expected to find stats last reset value changed, but didn't")
			}

			for _, txnStatsPostReset := range statsPostReset.Transactions {
				for _, txnStatsPreReset := range statsPreReset.Transactions {
					require.NotEqual(t, txnStatsPostReset, txnStatsPreReset,
						"expected to have reset SQL stats, but still found transaction %+v", txnStatsPostReset)
				}
			}

			for _, stmtStatsPostReset := range statsPostReset.Statements {
				for _, stmtStatsPreReset := range statsPreReset.Statements {
					require.NotEqual(t, stmtStatsPostReset, stmtStatsPreReset,
						"expected to have reset SQL stats, but still found statement %+v", stmtStatsPostReset)
				}
			}

			// Ensures that sql stats reset is isolated by tenant boundary.
			statsFromControlCluster, err :=
				controlCluster.TenantStatusSrv(serverccl.RandomServer).Statements(ctx, &serverpb.StatementsRequest{
					Combined: flushed,
				})
			require.NoError(t, err)

			ensureExpectedStmtFingerprintExistsInRPCResponse(t, stmts, statsFromControlCluster, "control")
		})
	}
}

func testResetIndexUsageStatsRPCForTenant(
	ctx context.Context, t *testing.T, testHelper serverccl.TenantTestHelper,
) {
	testCases := []struct {
		name    string
		resetFn func(helper serverccl.TenantTestHelper)
	}{
		{
			name: "sql-cli",
			resetFn: func(helper serverccl.TenantTestHelper) {
				// Reset index usage stats using SQL shell built-in.
				testingCluster := helper.TestCluster()
				testingCluster.TenantConn(0).Exec(t, "SELECT crdb_internal.reset_index_usage_stats()")
			},
		},
		{
			name: "http",
			resetFn: func(helper serverccl.TenantTestHelper) {
				// Reset index usage stats over HTTP on tenant SQL pod 1.
				httpPod1 := helper.TestCluster().TenantAdminHTTPClient(t, 1)
				defer httpPod1.Close()
				httpPod1.PostJSON("/_status/resetindexusagestats", &serverpb.ResetIndexUsageStatsRequest{}, &serverpb.ResetIndexUsageStatsResponse{})
			},
		},
	}

	for _, testCase := range testCases {
		testingCluster := testHelper.TestCluster()
		controlCluster := testHelper.ControlCluster()

		t.Run(testCase.name, func(t *testing.T) {
			var testingTableID, controlTableID string
			for i, cluster := range []serverccl.TenantClusterHelper{testingCluster, controlCluster} {
				// Create tables and insert data.
				cluster.TenantConn(0).Exec(t, `
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
)
`)

				cluster.TenantConn(0).Exec(t, `
INSERT INTO test
VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)
`)

				// Record scan on primary index.
				cluster.TenantConn(serverccl.RandomServer).
					Exec(t, "SELECT * FROM test")

				// Record scan on secondary index.
				// Note that this is an index join and will also read from the primary index.
				cluster.TenantConn(serverccl.RandomServer).
					Exec(t, "SELECT * FROM test@test_a_idx")
				testTableIDStr := cluster.TenantConn(serverccl.RandomServer).
					QueryStr(t, "SELECT 'test'::regclass::oid")[0][0]

				// Set table ID outside of loop.
				if i == 0 {
					testingTableID = testTableIDStr
				} else {
					controlTableID = testTableIDStr
				}

				query := `
SELECT
  table_id,
  index_id,
  total_reads,
  extract_duration('second', now() - last_read)
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = ` + testTableIDStr
				// Assert index usage data was inserted.
				expected := []struct {
					tableID    string
					indexID    string
					totalReads string
				}{
					{tableID: testTableIDStr, indexID: "1", totalReads: "2"},
					{tableID: testTableIDStr, indexID: "2", totalReads: "1"},
				}
				rows := cluster.TenantConn(serverccl.RandomServer).QueryStr(t, query)
				for idx, e := range expected {
					require.Equal(t, e.tableID, rows[idx][0])
					require.Equal(t, e.indexID, rows[idx][1])
					require.Equal(t, e.totalReads, rows[idx][2])
					lastReadDurationSec, err := strconv.Atoi(rows[idx][3])
					require.NoError(t, err)
					require.LessOrEqualf(t, lastReadDurationSec, LastReadThresholdSeconds, "Last Read was %ss ago, expected less than 30s", lastReadDurationSec)
				}
			}

			// Reset index usage stats.
			timePreReset := timeutil.Now()
			status := testingCluster.TenantStatusSrv(serverccl.RandomServer)

			// Reset index usage stats.
			testCase.resetFn(testHelper)

			// Check that last reset time was updated for test cluster.
			resp, err := status.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
			require.NoError(t, err)
			require.True(t, resp.LastReset.After(timePreReset))

			// IndexUsageStatistics fan-out returns cluster-wide last reset time.
			// Compare the cluster-wide last reset time from two different node
			// responses, ensure they are equal.
			first := testingCluster.TenantStatusSrv(0)
			firstResp, err := first.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
			require.NoError(t, err)
			require.Equal(t, firstResp.LastReset, resp.LastReset)

			// Ensure tenant data isolation.
			// Check that last reset time was not updated for control cluster.
			status = controlCluster.TenantStatusSrv(serverccl.RandomServer)
			resp, err = status.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
			require.NoError(t, err)
			require.Equal(t, resp.LastReset, time.Time{})

			// Query to fetch index usage stats. We do this instead of sending
			// an RPC request so that we can filter by table id.
			query := `
SELECT
  table_id,
  total_reads,
  last_read
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = $1
`

			// Check that index usage stats were reset.
			rows := testingCluster.TenantConn(2).QueryStr(t, query, testingTableID)
			require.NotNil(t, rows)
			for _, row := range rows {
				require.Equal(t, row[1], "0", "expected total reads for table %s to be reset, but got %s",
					row[0], row[1])
				require.Equal(t, row[2], "NULL", "expected last read time for table %s to be reset, but got %s",
					row[0], row[2])
			}

			// Ensure tenant data isolation.
			rows = controlCluster.TenantConn(0).QueryStr(t, query, controlTableID)
			require.NotNil(t, rows)
			for _, row := range rows {
				require.NotEqual(t, row[1], "0", "expected total reads for table %s to not be reset, but got %s", row[0], row[1])
				require.NotEqual(t, row[2], "NULL", "expected last read time for table %s to not be reset, but got %s", row[0], row[2])
			}

			// Cleanup.
			testingCluster.TenantConn(0).Exec(t, "DROP TABLE IF EXISTS test")
			controlCluster.TenantConn(0).Exec(t, "DROP TABLE IF EXISTS test")
		})
	}
}

func testTableIndexStats(ctx context.Context, t *testing.T, testHelper serverccl.TenantTestHelper) {
	getTableIndexStats := func(t *testing.T, helper serverccl.TenantTestHelper, db string) *serverpb.TableIndexStatsResponse {
		// Get index usage stats using function call.
		cluster := helper.TestCluster()
		status := cluster.TenantStatusSrv(serverccl.RandomServer)
		req := &serverpb.TableIndexStatsRequest{Table: "test", Database: db}
		resp, err := status.TableIndexStats(ctx, req)
		require.NoError(t, err)
		return resp
	}

	cluster := testHelper.TestCluster()

	timePreCreate := timeutil.Now()

	// Create table on a database.
	cluster.TenantConn(0).Exec(t, `
CREATE DATABASE test_db1;
SET DATABASE=test_db1;
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
);`)

	// Create second table on different database.
	cluster.TenantConn(0).Exec(t, `
CREATE DATABASE test_db2;
SET DATABASE=test_db2;
CREATE TABLE test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a, b)
);`)

	// Record scan on primary index.
	timePreRead := timeutil.Now()
	cluster.TenantConn(0).Exec(t, `
SET DATABASE=test_db1;
SELECT * FROM test;
`)

	getCreateStmtQuery := `
SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'test' AND indexname = $1`

	// Get index usage stats and assert expected results.
	requireAfter := func(t *testing.T, a, b *time.Time) {
		t.Helper()
		require.NotNil(t, a)
		require.NotNil(t, b)
		require.Truef(t, a.After(*b), "%v is not after %v", a, b)
	}
	requireBetween := func(t *testing.T, before time.Time, ts *time.Time, after time.Time) {
		t.Helper()
		requireAfter(t, ts, &before)
		requireAfter(t, &after, ts)
	}

	t.Run("validate read index", func(t *testing.T) {
		resp := getTableIndexStats(t, testHelper, "test_db1")
		require.Equal(t, uint64(1), resp.Statistics[0].Statistics.Stats.TotalReadCount)
		requireAfter(t, &resp.Statistics[0].Statistics.Stats.LastRead, &timePreRead)
		indexName := resp.Statistics[0].IndexName
		createStmt := cluster.TenantConn(0).QueryStr(t, getCreateStmtQuery, indexName)[0][0]
		print(createStmt)
		require.Equal(t, resp.Statistics[0].CreateStatement, createStmt)
		requireBetween(t, timePreCreate, resp.Statistics[0].CreatedAt, timePreRead)
	})

	t.Run("validate unread index", func(t *testing.T) {
		resp := getTableIndexStats(t, testHelper, "test_db2")
		require.Equal(t, uint64(0), resp.Statistics[0].Statistics.Stats.TotalReadCount)
		require.Equal(t, resp.Statistics[0].Statistics.Stats.LastRead, time.Time{})
		indexName := resp.Statistics[0].IndexName
		cluster.TenantConn(0).Exec(t, `SET DATABASE=test_db2`)
		createStmt := cluster.TenantConn(0).QueryStr(t, getCreateStmtQuery, indexName)[0][0]
		require.Equal(t, resp.Statistics[0].CreateStatement, createStmt)
		requireBetween(t, timePreCreate, resp.Statistics[0].CreatedAt, timePreRead)
	})

	// Test that a subsequent index creation has an appropriate timestamp.
	t.Run("validate CreatedAt for new index", func(t *testing.T) {
		timeBeforeCreateNewIndex := timeutil.Now()
		cluster.TenantConn(0).Exec(t, `
SET DATABASE=test_db2;
CREATE INDEX idx2 ON test (b, a)`)
		timeAfterCreateNewIndex := timeutil.Now()

		resp := getTableIndexStats(t, testHelper, "test_db2")
		var stat serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics
		var found bool
		for _, idx := range resp.Statistics {
			if found = idx.IndexName == "idx2"; found {
				stat = *idx
				break
			}
		}
		require.True(t, found)
		requireBetween(t,
			timeBeforeCreateNewIndex, stat.CreatedAt, timeAfterCreateNewIndex)
	})
}

func ensureExpectedStmtFingerprintExistsInRPCResponse(
	t *testing.T, expectedStmts []string, resp *serverpb.StatementsResponse, clusterType string,
) {
	t.Helper()

	for _, stmt := range expectedStmts {
		fingerprint := strings.Replace(stmt, "1", "_", -1)
		found := false
		for _, foundStmt := range resp.Statements {
			if !strings.Contains(foundStmt.Key.KeyData.App, resp.InternalAppNamePrefix) {
				if fingerprint == foundStmt.Key.KeyData.Query {
					found = true
					break
				}
			}
		}
		require.True(t, found, "expected %s to be found in "+
			"%s tenant cluster, but it was not found", fingerprint, clusterType)
	}
}

func testContentionEventsForTenant(
	ctx context.Context, t *testing.T, testHelper serverccl.TenantTestHelper,
) {
	testingCluster := testHelper.TestCluster()
	controlledCluster := testHelper.ControlCluster()

	sqlutils.CreateTable(
		t,
		testingCluster.TenantDB(0),
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	testTableID, err :=
		strconv.Atoi(testingCluster.TenantConn(0).QueryStr(t, "SELECT 'test.test'::regclass::oid")[0][0])
	require.NoError(t, err)

	testingCluster.TenantConn(0).Exec(t, "USE test")
	testingCluster.TenantConn(1).Exec(t, "USE test")

	testingCluster.TenantConn(0).Exec(t, `
BEGIN;
UPDATE test SET x = 100 WHERE x = 1;
`)
	testingCluster.TenantConn(1).Exec(t, `
SET TRACING=on;
BEGIN PRIORITY HIGH;
UPDATE test SET x = 1000 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
	testingCluster.TenantConn(0).ExpectErr(
		t,
		"^pq: restart transaction.+",
		"COMMIT;",
	)

	resp, err :=
		testingCluster.TenantStatusSrv(2).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)

	require.GreaterOrEqualf(t, len(resp.Events.IndexContentionEvents), 1,
		"expecting at least 1 contention event, but found none")

	found := false
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			found = true
			break
		}
	}

	require.True(t, found,
		"expect to find contention event for table %d, but found %+v", testTableID, resp)

	resp, err = controlledCluster.TenantStatusSrv(0).ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
	require.NoError(t, err)
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			t.Errorf("did not expect contention event in controlled cluster, but it was found")
		}
	}

	testutils.SucceedsWithin(t, func() error {
		err = testHelper.TestCluster().TenantContentionRegistry(1).FlushEventsForTest(ctx)
		if err != nil {
			return err
		}

		resp := &serverpb.TransactionContentionEventsResponse{}
		testHelper.
			TestCluster().
			TenantAdminHTTPClient(t, 1).
			GetJSON("/_status/transactioncontentionevents", resp)

		if len(resp.Events) == 0 {
			return errors.New("expected transaction contention events being populated, " +
				"but it is not")
		}

		return nil
	}, 5*time.Second)
}

func testIndexUsageForTenants(t *testing.T, testHelper serverccl.TenantTestHelper) {
	testingCluster := testHelper.TestCluster()
	controlledCluster := testHelper.ControlCluster()

	testingCluster.TenantConn(0).Exec(t, "USE defaultdb")
	testingCluster.TenantConn(1).Exec(t, "USE defaultdb")
	testingCluster.TenantConn(2).Exec(t, "USE defaultdb")
	testingCluster.TenantConn(0).Exec(t, `CREATE SCHEMA idx_test`)

	testingCluster.TenantConn(0).Exec(t, `
CREATE TABLE idx_test.test (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX(a)
)
`)

	defer func() {
		testingCluster.TenantConn(0).Exec(t, "DROP TABLE idx_test.test")
	}()

	testingCluster.TenantConn(0).Exec(t, `
INSERT INTO idx_test.test
VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)
`)

	// Record scan on primary index.
	testingCluster.TenantConn(0).Exec(t, "SELECT * FROM idx_test.test")

	// Record scan on secondary index.
	// Note that this is an index join and will also read from the primary index.
	testingCluster.TenantConn(1).Exec(t, "SELECT * FROM idx_test.test@test_a_idx")
	testTableIDStr := testingCluster.TenantConn(2).QueryStr(t, "SELECT 'idx_test.test'::regclass::oid")[0][0]
	testTableID, err := strconv.Atoi(testTableIDStr)
	require.NoError(t, err)

	query := `
SELECT
  table_id,
  index_id,
  total_reads,
  extract_duration('second', now() - last_read)
FROM
  crdb_internal.index_usage_statistics
WHERE
  table_id = $1
`
	expected := []struct {
		tableID    string
		indexID    string
		totalReads string
	}{
		{tableID: testTableIDStr, indexID: "1", totalReads: "2"},
		{tableID: testTableIDStr, indexID: "2", totalReads: "1"},
	}
	rows := testingCluster.TenantConn(2).QueryStr(t, query, testTableID)
	for idx, e := range expected {
		require.Equal(t, e.tableID, rows[idx][0])
		require.Equal(t, e.indexID, rows[idx][1])
		require.Equal(t, e.totalReads, rows[idx][2])
		lastReadDurationSec, err := strconv.Atoi(rows[idx][3])
		require.NoError(t, err)
		require.LessOrEqualf(t, lastReadDurationSec, LastReadThresholdSeconds, "Last Read was %ss ago, expected less than 30s", lastReadDurationSec)
	}

	// Ensure tenant data isolation.
	actual := controlledCluster.TenantConn(0).QueryStr(t, query, testTableID)
	require.Equal(t, [][]string{}, actual)
}

func selectClusterSessionIDs(t *testing.T, conn *sqlutils.SQLRunner) []string {
	var sessionIDs []string
	rows := conn.QueryStr(t,
		"SELECT session_id FROM crdb_internal.cluster_sessions WHERE status = 'ACTIVE' OR status = 'IDLE'")
	for _, row := range rows {
		sessionIDs = append(sessionIDs, row[0])
	}
	return sessionIDs
}

func testTenantStatusCancelSession(t *testing.T, helper serverccl.TenantTestHelper) {
	// Open a SQL session on tenant SQL pod 0.
	ctx := context.Background()
	// Open two different SQL sessions on tenant SQL pod 0.
	sqlPod0 := helper.TestCluster().TenantDB(0)
	sqlPod0SessionToCancel, err := sqlPod0.Conn(ctx)
	require.NoError(t, err)
	sqlPod0SessionForIntrospection, err := sqlPod0.Conn(ctx)
	require.NoError(t, err)
	_, err = sqlPod0SessionToCancel.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
	introspectionRunner := sqlutils.MakeSQLRunner(sqlPod0SessionForIntrospection)

	// See the session over HTTP on tenant SQL pod 1.
	httpPod1 := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer httpPod1.Close()
	listSessionsResp := serverpb.ListSessionsResponse{}
	httpPod1.GetJSON("/_status/sessions?exclude_closed_sessions=true", &listSessionsResp)
	var session serverpb.Session
	for _, s := range listSessionsResp.Sessions {
		if s.LastActiveQuery == "SELECT 1" {
			session = s
			break
		}
	}
	require.NotNil(t, session.ID, "session not found")

	// See the session over SQL on tenant SQL pod 0.
	sessionID := hex.EncodeToString(session.ID)
	require.Eventually(t, func() bool {
		return strings.Contains(strings.Join(selectClusterSessionIDs(t, introspectionRunner), ","), sessionID)
	}, 5*time.Second, 100*time.Millisecond)

	// Cancel the session over HTTP from tenant SQL pod 1.
	cancelSessionReq := serverpb.CancelSessionRequest{SessionID: session.ID}
	cancelSessionResp := serverpb.CancelSessionResponse{}
	httpPod1.PostJSON("/_status/cancel_session/"+session.NodeID.String(), &cancelSessionReq, &cancelSessionResp)
	require.Equal(t, true, cancelSessionResp.Canceled, cancelSessionResp.Error)

	// No longer see the session over SQL from tenant SQL pod 0.
	// (The SQL client maintains an internal connection pool and automatically reconnects.)
	require.Eventually(t, func() bool {
		return !strings.Contains(strings.Join(selectClusterSessionIDs(t, introspectionRunner), ","), sessionID)
	}, 5*time.Second, 100*time.Millisecond)

	// Attempt to cancel the session again over HTTP from tenant SQL pod 1, so that we can see the error message.
	httpPod1.PostJSON("/_status/cancel_session/"+session.NodeID.String(), &cancelSessionReq, &cancelSessionResp)
	require.Equal(t, false, cancelSessionResp.Canceled)
	require.Equal(t, fmt.Sprintf("session ID %s not found", sessionID), cancelSessionResp.Error)
}

func testTenantStatusCancelSessionErrorMessages(t *testing.T, helper serverccl.TenantTestHelper) {
	testCases := []struct {
		sessionID     string
		expectedError string
	}{
		{
			sessionID:     "",
			expectedError: "session ID 00000000000000000000000000000000 not found",
		},
		{
			sessionID:     "01", // This query ID claims to have SQL instance ID 1, different from the one we're talking to.
			expectedError: "session ID 00000000000000000000000000000001 not found",
		},
		{
			sessionID:     "02", // This query ID claims to have SQL instance ID 2, the instance we're talking to.
			expectedError: "session ID 00000000000000000000000000000002 not found",
		},
		{
			sessionID:     "42", // This query ID claims to have SQL instance ID 42, which does not exist.
			expectedError: "session ID 00000000000000000000000000000042 not found",
		},
	}

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		client := helper.TestCluster().TenantHTTPClient(t, 1, isAdmin)
		defer client.Close()

		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("sessionID-%s", testCase.sessionID), func(t *testing.T) {
				sessionID, err := clusterunique.IDFromString(testCase.sessionID)
				require.NoError(t, err)
				resp := serverpb.CancelSessionResponse{}
				err = client.PostJSONChecked("/_status/cancel_session/0", &serverpb.CancelSessionRequest{
					SessionID: sessionID.GetBytes(),
				}, &resp)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedError, resp.Error)
			})
		}
	})
}

func selectClusterQueryIDs(t *testing.T, conn *sqlutils.SQLRunner) []string {
	var queryIDs []string
	rows := conn.QueryStr(t, "SELECT query_id FROM crdb_internal.cluster_queries")
	for _, row := range rows {
		queryIDs = append(queryIDs, row[0])
	}
	return queryIDs
}

func testTenantStatusCancelQuery(
	ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper,
) {
	// Open a SQL session on tenant SQL pod 0 and start a long-running query.
	sqlPod0 := helper.TestCluster().TenantConn(0)
	resultCh := make(chan struct{})
	errorCh := make(chan error)
	defer close(resultCh)
	defer close(errorCh)
	go func() {
		if _, err := sqlPod0.DB.ExecContext(ctx, "SELECT pg_sleep(60)"); err != nil {
			errorCh <- err
		} else {
			resultCh <- struct{}{}
		}
	}()

	// See the query over HTTP on tenant SQL pod 1.
	httpPod1 := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer httpPod1.Close()
	var listSessionsResp serverpb.ListSessionsResponse
	var query serverpb.ActiveQuery
	require.Eventually(t, func() bool {
		httpPod1.GetJSON("/_status/sessions", &listSessionsResp)
		for _, s := range listSessionsResp.Sessions {
			for _, q := range s.ActiveQueries {
				if q.Sql == "SELECT pg_sleep(60)" {
					query = q
					break
				}
			}
		}
		return query.ID != ""
	}, 10*time.Second, 100*time.Millisecond, "query not found")

	// See the query over SQL on tenant SQL pod 0.
	require.Eventually(t, func() bool {
		return strings.Contains(strings.Join(selectClusterQueryIDs(t, sqlPod0), ","), query.ID)
	}, 10*time.Second, 100*time.Millisecond)

	// Cancel the query over HTTP on tenant SQL pod 1.
	cancelQueryReq := serverpb.CancelQueryRequest{QueryID: query.ID}
	cancelQueryResp := serverpb.CancelQueryResponse{}
	httpPod1.PostJSON("/_status/cancel_query/0", &cancelQueryReq, &cancelQueryResp)
	require.Equal(t, true, cancelQueryResp.Canceled,
		"expected query to be canceled, but encountered unexpected error %s", cancelQueryResp.Error)

	// No longer see the query over SQL on tenant SQL pod 0.
	require.Eventually(t, func() bool {
		return !strings.Contains(strings.Join(selectClusterQueryIDs(t, sqlPod0), ","), query.ID)
	}, 10*time.Second, 100*time.Millisecond,
		"expected query %s to no longer be visible in crdb_internal.cluster_queries", query.ID)

	select {
	case <-resultCh:
		t.Fatalf("Expected long-running query to have been canceled with error.")
	case err := <-errorCh:
		require.Equal(t, "pq: query execution canceled", err.Error())
	}

	// Attempt to cancel the query again over HTTP from tenant SQL pod 1, so that we can see the error message.
	httpPod1.PostJSON("/_status/cancel_query/0", &cancelQueryReq, &cancelQueryResp)
	require.Equal(t, false, cancelQueryResp.Canceled)
	require.Equal(t, fmt.Sprintf("query ID %s not found", query.ID), cancelQueryResp.Error)
}

func testTenantStatusCancelQueryErrorMessages(t *testing.T, helper serverccl.TenantTestHelper) {
	testCases := []struct {
		queryID       string
		expectedError string
	}{
		{
			queryID: "BOGUS_QUERY_ID",
			expectedError: "query ID 00000000000000000000000000000000 malformed: " +
				"could not decode BOGUS_QUERY_ID as hex: encoding/hex: invalid byte: U+004F 'O'",
		},
		{
			queryID:       "",
			expectedError: "query ID 00000000000000000000000000000000 not found",
		},
		{
			queryID:       "01", // This query ID claims to have SQL instance ID 1, different from the one we're talking to.
			expectedError: "query ID 00000000000000000000000000000001 not found",
		},
		{
			queryID:       "02", // This query ID claims to have SQL instance ID 2, the instance we're talking to.
			expectedError: "query ID 00000000000000000000000000000002 not found",
		},
		{
			queryID:       "42", // This query ID claims to have SQL instance ID 42, which does not exist.
			expectedError: "query ID 00000000000000000000000000000042 not found",
		},
	}

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		client := helper.TestCluster().TenantHTTPClient(t, 1, isAdmin)
		defer client.Close()

		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("queryID-%s", testCase.queryID), func(t *testing.T) {
				resp := serverpb.CancelQueryResponse{}
				err := client.PostJSONChecked("/_status/cancel_query/0", &serverpb.CancelQueryRequest{
					QueryID: testCase.queryID,
				}, &resp)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedError, resp.Error)
			})
		}
	})
}

// testTxnIDResolutionRPC tests the reachability of TxnIDResolution RPC. The
// underlying implementation correctness is tested within
// pkg/sql/contention/txnidcache.
func testTxnIDResolutionRPC(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	run := func(sqlConn *sqlutils.SQLRunner, status serverpb.SQLStatusServer, coordinatorNodeID int32) {
		sqlConn.Exec(t, "SET application_name='test1'")

		sqlConn.Exec(t, "BEGIN")
		result := sqlConn.QueryStr(t, `
		SELECT
			id
		FROM
			crdb_internal.node_transactions
		WHERE
			application_name = 'test1'`)
		require.Equal(t, 1 /* expected */, len(result),
			"expected only one active txn, but there are %d active txns found", len(result))
		txnID := uuid.FromStringOrNil(result[0][0])
		require.False(t, uuid.Nil.Equal(txnID),
			"expected a valid txnID, but %+v is found", result)
		sqlConn.Exec(t, "COMMIT")

		testutils.SucceedsWithin(t, func() error {
			resp, err := status.TxnIDResolution(ctx, &serverpb.TxnIDResolutionRequest{
				CoordinatorID: strconv.Itoa(int(coordinatorNodeID)),
				TxnIDs:        []uuid.UUID{txnID},
			})
			require.NoError(t, err)
			if len(resp.ResolvedTxnIDs) != 1 {
				return errors.Newf("expected RPC response to have length of 1, but "+
					"it is %d", len(resp.ResolvedTxnIDs))
			}
			require.Equal(t, txnID, resp.ResolvedTxnIDs[0].TxnID,
				"expected to find txn %s on coordinator node %d, but it "+
					"was not", txnID.String(), coordinatorNodeID)

			// It's possible that adding the transaction id to the cache and
			// updating the transaction id with a valid fingerprint are done in
			// 2 separate batches. This allows retries to wait for a valid fingerprint
			if appstatspb.InvalidTransactionFingerprintID == resp.ResolvedTxnIDs[0].TxnFingerprintID {
				return fmt.Errorf("transaction fingerprint id not updated yet. TxnFingerprintID: %d", resp.ResolvedTxnIDs[0].TxnFingerprintID)
			}
			return nil
		}, 1*time.Minute)
	}

	t.Run("regular_cluster", func(t *testing.T) {
		status :=
			helper.HostCluster().Server(0 /* idx */).StatusServer().(serverpb.SQLStatusServer)
		sqlConn := helper.HostCluster().ServerConn(0 /* idx */)
		run(sqlutils.MakeSQLRunner(sqlConn), status, 1 /* coordinatorNodeID */)
	})

	t.Run("tenant_cluster", func(t *testing.T) {
		// Select a different tenant status server here so a pod-to-pod RPC will
		// happen.
		status := helper.TestCluster().TenantStatusSrv(2 /* idx */)
		sqlConn := helper.TestCluster().TenantConn(0 /* idx */)
		run(sqlConn, status, 1 /* coordinatorNodeID */)
	})
}

func testTenantRangesRPC(_ context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	tenantA := helper.TestCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)
	tenantB := helper.ControlCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)

	// Wait for range splits to occur so we get more than just a single range during our tests.
	waitForRangeSplit(t, tenantA)
	waitForRangeSplit(t, tenantB)

	t.Run("test TenantRanges respects tenant isolation", func(t *testing.T) {
		tenIDA := helper.TestCluster().Tenant(0).GetRPCContext().TenantID
		tenIDB := helper.ControlCluster().Tenant(0).GetRPCContext().TenantID
		keySpanForA := keys.MakeTenantSpan(tenIDA)
		keySpanForB := keys.MakeTenantSpan(tenIDB)

		resp, err := tenantA.TenantRanges(context.Background(), &serverpb.TenantRangesRequest{})
		require.NoError(t, err)
		require.NotEmpty(t, resp.RangesByLocality)
		for localityKey, rangeList := range resp.RangesByLocality {
			require.NotEmpty(t, localityKey)
			for _, r := range rangeList.Ranges {
				assertStartKeyInRange(t, r.Span.StartKey, keySpanForA.Key)
				assertEndKeyInRange(t, r.Span.EndKey, keySpanForA.Key, keySpanForA.EndKey)
			}
		}

		resp, err = tenantB.TenantRanges(context.Background(), &serverpb.TenantRangesRequest{})
		require.NoError(t, err)
		require.NotEmpty(t, resp.RangesByLocality)
		for localityKey, rangeList := range resp.RangesByLocality {
			require.NotEmpty(t, localityKey)
			for _, r := range rangeList.Ranges {
				assertStartKeyInRange(t, r.Span.StartKey, keySpanForB.Key)
				assertEndKeyInRange(t, r.Span.EndKey, keySpanForB.Key, keySpanForB.EndKey)
			}
		}
	})

	t.Run("test TenantRanges pagination", func(t *testing.T) {
		ctx := context.Background()
		resp1, err := tenantA.TenantRanges(ctx, &serverpb.TenantRangesRequest{
			Limit: 1,
		})
		require.NoError(t, err)
		require.Equal(t, 1, int(resp1.Next))
		for _, ranges := range resp1.RangesByLocality {
			require.Len(t, ranges.Ranges, 1)
		}

		sql := helper.TestCluster().TenantConn(0)
		// Wait for the split queue to process some before requesting the 2nd range.
		// We expect an offset for the 3rd range, so wait until at least 3 ranges exist.
		testutils.SucceedsSoon(t, func() error {
			res := sql.QueryStr(t, "SELECT count(*) FROM crdb_internal.ranges")
			require.Equal(t, len(res), 1)
			require.Equal(t, len(res[0]), 1)
			rangeCount, err := strconv.Atoi(res[0][0])
			require.NoError(t, err)
			if rangeCount < 3 {
				return errors.Newf("expected >= 3 ranges, got %d", rangeCount)
			}

			resp2, err := tenantA.TenantRanges(ctx, &serverpb.TenantRangesRequest{
				Limit:  1,
				Offset: resp1.Next,
			})
			require.NoError(t, err)
			require.Equal(t, 2, int(resp2.Next))
			for locality, ranges := range resp2.RangesByLocality {
				require.Len(t, ranges.Ranges, 1)
				// Verify pagination functions based on ascending RangeID order.
				require.True(t,
					resp1.RangesByLocality[locality].Ranges[0].RangeID < ranges.Ranges[0].RangeID)
			}
			return nil
		})

	})
}

func testRangesRPC(_ context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	tenantA := helper.TestCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)
	tenantB := helper.ControlCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)

	req := &serverpb.RangesRequest{NodeId: "1"}

	// Wait for range splits to occur so we get more than just a single range during our tests.
	waitForRangeSplit(t, tenantA)
	waitForRangeSplit(t, tenantB)

	t.Run("test Ranges respects tenant isolation", func(t *testing.T) {
		tenIDA := helper.TestCluster().Tenant(0).GetRPCContext().TenantID
		tenIDB := helper.ControlCluster().Tenant(0).GetRPCContext().TenantID
		keySpanForA := keys.MakeTenantSpan(tenIDA)
		keySpanForB := keys.MakeTenantSpan(tenIDB)

		resp, err := tenantA.Ranges(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Ranges)
		for _, r := range resp.Ranges {
			assertStartKeyInRange(t, r.Span.StartKey, keySpanForA.Key)
			assertEndKeyInRange(t, r.Span.EndKey, keySpanForA.Key, keySpanForA.EndKey)
		}

		resp, err = tenantB.Ranges(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Ranges)
		for _, r := range resp.Ranges {
			assertStartKeyInRange(t, r.Span.StartKey, keySpanForB.Key)
			assertEndKeyInRange(t, r.Span.EndKey, keySpanForB.Key, keySpanForB.EndKey)
		}
	})
}

func waitForRangeSplit(t *testing.T, tenant serverpb.TenantStatusServer) {
	req := &serverpb.RangesRequest{NodeId: "1"}
	testutils.SucceedsSoon(t, func() error {
		resp, err := tenant.Ranges(context.Background(), req)
		if err != nil {
			return err
		}
		if len(resp.Ranges) <= 1 {
			return errors.New("waiting for tenant range split")
		}
		return nil
	})
}

func testTenantAuthOnStatements(
	ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper,
) {
	client := helper.TestCluster().TenantHTTPClient(t, 1, false)
	defer client.Close()
	err := client.GetJSONChecked("/_status/statements", &serverpb.StatementsResponse{})
	// Should return an error because the user is not admin and doesn't have any system
	// privileges.
	require.Error(t, err)

	// Once user has been granted the required system privilege there should be no error.
	grantStmt := `GRANT SYSTEM VIEWACTIVITY TO authentic_user_noadmin;`
	helper.TestCluster().TenantConn(0).Exec(t, grantStmt)
	err = client.GetJSONChecked("/_status/statements", &serverpb.StatementsResponse{})
	require.NoError(t, err)
}

// assertStartKeyInRange compares the pretty printed startKey with the provided
// tenantPrefix key, ensuring that the startKey starts with the tenantPrefix.
func assertStartKeyInRange(t *testing.T, startKey string, tenantPrefix roachpb.Key) {
	require.Truef(t, strings.Index(startKey, tenantPrefix.String()) == 0,
		fmt.Sprintf("start key %s is outside of the tenant's keyspace (prefix: %v)",
			startKey, tenantPrefix.String()))
}

// assertEndKeyInRange compares the pretty printed endKey with the provided
// tenantPrefix and tenantEnd keys. Ensures that the key starts with
// either the tenantPrefix, or the tenantEnd (valid as end keys are
// exclusive).
func assertEndKeyInRange(
	t *testing.T, endKey string, tenantPrefix roachpb.Key, tenantEnd roachpb.Key,
) {
	require.Truef(t,
		strings.Index(endKey, tenantPrefix.String()) == 0 ||
			strings.Index(endKey, tenantEnd.String()) == 0 ||
			// Possible if the tenant's ranges fall at the end of the entire keyspace
			// range within the cluster.
			endKey == "/Max",
		fmt.Sprintf("end key %s is outside of the tenant's keyspace (start: %v, end: %v)",
			endKey, tenantPrefix.String(), tenantEnd.String()))
}

func testTenantHotRanges(_ context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	tenantA := helper.TestCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)
	tenantB := helper.ControlCluster().TenantStatusSrv(0).(serverpb.TenantStatusServer)

	tIDa := securitytest.EmbeddedTenantIDs()[0]
	tIDb := securitytest.EmbeddedTenantIDs()[1]

	testutils.SucceedsSoon(t, func() error {
		resp, err := tenantA.HotRangesV2(context.Background(), &serverpb.HotRangesRequest{})
		if err != nil {
			return err
		}
		if len(resp.Ranges) > 1 {
			return nil
		}
		return errors.New("waiting for hot ranges")
	})

	rangeIDs := make(map[roachpb.RangeID]roachpb.TenantID)
	stores := helper.HostCluster().Server(0).GetStores().(*kvserver.Stores)
	_ = stores.VisitStores(func(s *kvserver.Store) error {
		_, _ = s.Capacity(context.Background(), false)
		s.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
			rangeIDs[replica.RangeID], _ = replica.TenantID()
			return true
		})
		return nil
	})

	t.Run("test http request for hot ranges", func(t *testing.T) {
		client := helper.TestCluster().TenantHTTPClient(t, 1, false)
		defer client.Close()

		req := serverpb.HotRangesRequest{}
		resp := serverpb.HotRangesResponseV2{}
		err := client.PostJSONChecked("/_status/v2/hotranges", &req, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Forbidden")

		grantStmt := `GRANT SYSTEM VIEWCLUSTERMETADATA TO authentic_user_noadmin;`
		helper.TestCluster().TenantConn(0).Exec(t, grantStmt)

		client.PostJSON("/_status/v2/hotranges", &req, &resp)
		require.NotEmpty(t, resp.Ranges)

		revokeStmt := `REVOKE SYSTEM VIEWCLUSTERMETADATA FROM authentic_user_noadmin;`
		helper.TestCluster().TenantConn(0).Exec(t, revokeStmt)
	})

	t.Run("test tenant hot ranges respects tenant isolation", func(t *testing.T) {
		resp, err := tenantA.HotRangesV2(context.Background(), &serverpb.HotRangesRequest{})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Ranges)

		for _, r := range resp.Ranges {
			if tID, ok := rangeIDs[r.RangeID]; ok {
				if tID.ToUint64() != tIDa {
					require.Equal(t, tIDa, tID)
				}
			}
		}

		resp, err = tenantB.HotRangesV2(context.Background(), &serverpb.HotRangesRequest{})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Ranges)
		for _, r := range resp.Ranges {
			if tID, ok := rangeIDs[r.RangeID]; ok {
				if tID.ToUint64() != tIDb {
					require.Equal(t, tIDb, tID)
				}
			}
		}
	})

	t.Run("forbids requesting hot ranges for another tenant", func(t *testing.T) {
		// TenantA requests hot ranges for Tenant B.
		resp, err := tenantA.HotRangesV2(context.Background(), &serverpb.HotRangesRequest{
			TenantID: roachpb.MustMakeTenantID(tIDb).String(),
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})
}
