// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/rangetestutils"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminAPINonTableStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(110012),
		},
	})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0).ApplicationLayer()

	// Skip TableStatsResponse.Stats comparison, since it includes data which
	// aren't consistent (time, bytes).
	expectedResponse := serverpb.NonTableStatsResponse{
		TimeSeriesStats: &serverpb.TableStatsResponse{
			RangeCount:   1,
			ReplicaCount: 3,
			NodeCount:    3,
		},
		InternalUseStats: &serverpb.TableStatsResponse{
			RangeCount:   11,
			ReplicaCount: 15,
			NodeCount:    3,
		},
	}

	var resp serverpb.NonTableStatsResponse
	if err := srvtestutils.GetAdminJSONProto(s, "nontablestats", &resp); err != nil {
		t.Fatal(err)
	}

	assertExpectedStatsResponse := func(expected, actual *serverpb.TableStatsResponse) {
		assert.Equal(t, expected.RangeCount, actual.RangeCount)
		assert.Equal(t, expected.ReplicaCount, actual.ReplicaCount)
		assert.Equal(t, expected.NodeCount, actual.NodeCount)
	}

	assertExpectedStatsResponse(expectedResponse.TimeSeriesStats, resp.TimeSeriesStats)
	assertExpectedStatsResponse(expectedResponse.InternalUseStats, resp.InternalUseStats)
}

// Verify that for a cluster with no user data, all the ranges on the Databases
// page consist of:
// 1) the total ranges listed for the system database
// 2) the total ranges listed for the Non-Table data
func TestRangeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(76378),
		},
	})
	require.NoError(t, testCluster.WaitForFullReplication())
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	// Sum up ranges for non-table parts of the system returned
	// from the "nontablestats" enpoint.
	getNonTableRangeCount := func() (ts, internal int64) {
		var resp serverpb.NonTableStatsResponse
		if err := srvtestutils.GetAdminJSONProto(s, "nontablestats", &resp); err != nil {
			t.Fatal(err)
		}
		return resp.TimeSeriesStats.RangeCount, resp.InternalUseStats.RangeCount
	}

	// Return map tablename=>count obtained from the
	// "databases/system/tables/{table}" endpoints.
	getSystemTableRangeCount := func() map[string]int64 {
		m := map[string]int64{}
		var dbResp serverpb.DatabaseDetailsResponse
		if err := srvtestutils.GetAdminJSONProto(s, "databases/system", &dbResp); err != nil {
			t.Fatal(err)
		}
		for _, tableName := range dbResp.TableNames {
			var tblResp serverpb.TableStatsResponse
			path := "databases/system/tables/" + tableName + "/stats"
			if err := srvtestutils.GetAdminJSONProto(s, path, &tblResp); err != nil {
				t.Fatal(err)
			}
			m[tableName] = tblResp.RangeCount
		}
		// Hardcode the single range used by each system sequence, the above
		// request does not return sequences.
		// TODO(richardjcai): Maybe update the request to return
		// sequences as well?
		m[fmt.Sprintf("public.%s", catconstants.DescIDSequenceTableName)] = 1
		m[fmt.Sprintf("public.%s", catconstants.RoleIDSequenceName)] = 1
		m[fmt.Sprintf("public.%s", catconstants.TenantIDSequenceTableName)] = 1
		return m
	}

	getRangeCountFromFullSpan := func() int64 {
		stats, err := s.StatsForSpan(context.Background(), roachpb.Span{
			Key:    keys.LocalMax,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		return stats.RangeCount
	}

	exp := getRangeCountFromFullSpan()

	var systemTableRangeCount int64
	sysDBMap := getSystemTableRangeCount()
	for _, n := range sysDBMap {
		systemTableRangeCount += n
	}

	tsCount, internalCount := getNonTableRangeCount()

	act := tsCount + internalCount + systemTableRangeCount

	if !assert.Equal(t,
		exp,
		act,
	) {
		t.Log("did nonTableDescriptorRangeCount() change?")
		t.Logf(
			"claimed numbers:\ntime series = %d\ninternal = %d\nsystemdb = %d (%v)",
			tsCount, internalCount, systemTableRangeCount, sysDBMap,
		)
		db := testCluster.ServerConn(0)
		defer db.Close()

		runner := sqlutils.MakeSQLRunner(db)
		s := sqlutils.MatrixToStr(runner.QueryStr(t, `SHOW CLUSTER RANGES`))
		t.Logf("actual ranges:\n%s", s)
	}
}

func TestStatsforSpanOnLocalMax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// We want to look at all the ranges in the cluster.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(context.Background())
	firstServer := tc.Server(0)

	underTest := roachpb.Span{
		Key:    keys.LocalMax,
		EndKey: keys.SystemPrefix,
	}

	_, err := firstServer.StatsForSpan(context.Background(), underTest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAdminAPIDataDistribution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	firstServer := tc.Server(0).ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(firstServer.SQLConn(t))

	{
		// TODO(irfansharif): The data-distribution page and underyling APIs don't
		// know how to deal with coalesced ranges. See #97942.
		sysDB := sqlutils.MakeSQLRunner(tc.Server(0).SystemLayer().SQLConn(t))
		sysDB.Exec(t, `SET CLUSTER SETTING spanconfig.range_coalescing.system.enabled = false`)
		sysDB.Exec(t, `SET CLUSTER SETTING spanconfig.range_coalescing.application.enabled = false`)
		// Make sure extra secondary tenants don't cause the endpoint to error.
		sysDB.Exec(t, "CREATE TENANT 'app'")
	}

	// Create some tables.
	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE roachblog.comments (
		id INT PRIMARY KEY,
		post_id INT REFERENCES roachblog.posts,
		body text
	)`)

	// Test for null raw sql config column in crdb_internal.zones,
	// see: https://github.com/cockroachdb/cockroach/issues/140044
	sqlDB.Exec(t, `ALTER TABLE roachblog.posts CONFIGURE ZONE = ''`)

	sqlDB.Exec(t, `CREATE SCHEMA roachblog."foo bar"`)
	sqlDB.Exec(t, `CREATE TABLE roachblog."foo bar".other_stuff(id INT PRIMARY KEY, body TEXT)`)
	// Test special characters in DB and table names.
	sqlDB.Exec(t, `CREATE DATABASE "sp'ec\ch""ars"`)
	sqlDB.Exec(t, `CREATE TABLE "sp'ec\ch""ars"."more\spec'chars" (id INT PRIMARY KEY)`)

	// Verify that we see their replicas in the DataDistribution response, evenly spread
	// across the test cluster's three nodes.

	expectedDatabaseInfo := map[string]serverpb.DataDistributionResponse_DatabaseInfo{
		"roachblog": {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				"public.posts": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
				"public.comments": {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
				`"foo bar".other_stuff`: {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
		`sp'ec\ch"ars`: {
			TableInfo: map[string]serverpb.DataDistributionResponse_TableInfo{
				`public."more\spec'chars"`: {
					ReplicaCountByNodeId: map[roachpb.NodeID]int64{
						1: 1,
						2: 1,
						3: 1,
					},
				},
			},
		},
	}

	require.NoError(t, tc.WaitForFullReplication())

	// Wait for the new tables' ranges to be created and replicated.
	var resp serverpb.DataDistributionResponse
	if err := srvtestutils.GetAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	delete(resp.DatabaseInfo, "system") // delete results for system database.
	if !reflect.DeepEqual(resp.DatabaseInfo, expectedDatabaseInfo) {
		t.Fatalf("expected %v; got %v", expectedDatabaseInfo, resp.DatabaseInfo)
	}

	// Don't test anything about the zone configs for now; just verify that something is there.
	require.NotEmpty(t, resp.ZoneConfigs)

	// Verify that the request still works after a table has been dropped,
	// and that dropped_at is set on the dropped table.
	sqlDB.Exec(t, `DROP TABLE roachblog.comments`)

	//var resp serverpb.DataDistributionResponse
	if err := srvtestutils.GetAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	if resp.DatabaseInfo["roachblog"].TableInfo["public.comments"].DroppedAt == nil {
		t.Fatal("expected roachblog.comments to have dropped_at set but it's nil")
	}

	// Verify that the request still works after a database has been dropped.
	sqlDB.Exec(t, `DROP DATABASE roachblog CASCADE`)

	if err := srvtestutils.GetAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdminAPIDataDistribution(b *testing.B) {
	skip.UnderShort(b, "TODO: fix benchmark")
	tc := serverutils.StartCluster(b, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	firstServer := tc.Server(0).ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	sqlDB.Exec(b, `CREATE DATABASE roachblog`)

	// Create a bunch of tables.
	for i := 0; i < 200; i++ {
		sqlDB.Exec(
			b,
			fmt.Sprintf(`CREATE TABLE roachblog.t%d (id INT PRIMARY KEY, title text, body text)`, i),
		)
		// TODO(vilterp): split to increase the number of ranges for each table
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var resp serverpb.DataDistributionResponse
		if err := srvtestutils.GetAdminJSONProto(firstServer, "data_distribution", &resp); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func TestHotRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer serverutils.TestingSetDefaultTenantSelectionOverride(
		// bug: HotRanges not available with secondary tenants yet.
		base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109499),
	)()

	srv := rangetestutils.StartServer(t)
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	var hotRangesResp serverpb.HotRangesResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "hotranges", &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.HotRangesByNodeID) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}

	for nodeID, nodeResp := range hotRangesResp.HotRangesByNodeID {
		if len(nodeResp.Stores) == 0 {
			t.Errorf("didn't get any stores in hot range response from n%d: %v",
				nodeID, nodeResp.ErrorMessage)
		}
		for _, storeResp := range nodeResp.Stores {
			// Only the first store will actually have any ranges on it.
			if storeResp.StoreID != roachpb.StoreID(1) {
				continue
			}
			lastQPS := math.MaxFloat64
			if len(storeResp.HotRanges) == 0 {
				t.Errorf("didn't get any hot ranges in response from n%d,s%d: %v",
					nodeID, storeResp.StoreID, nodeResp.ErrorMessage)
			}
			for _, r := range storeResp.HotRanges {
				if r.Desc.RangeID == 0 || (len(r.Desc.StartKey) == 0 && len(r.Desc.EndKey) == 0) {
					t.Errorf("unexpected empty/unpopulated range descriptor: %+v", r.Desc)
				}
				if r.QueriesPerSecond > 0 {
					if r.ReadsPerSecond == 0 && r.WritesPerSecond == 0 && r.ReadBytesPerSecond == 0 && r.WriteBytesPerSecond == 0 {
						t.Errorf("qps %.2f > 0, expected either reads=%.2f, writes=%.2f, readBytes=%.2f or writeBytes=%.2f to be non-zero",
							r.QueriesPerSecond, r.ReadsPerSecond, r.WritesPerSecond, r.ReadBytesPerSecond, r.WriteBytesPerSecond)
					}
					// If the architecture doesn't support sampling CPU, it
					// will also be zero.
					if grunning.Supported() && r.CPUTimePerSecond == 0 {
						t.Errorf("qps %.2f > 0, expected cpu=%.2f to be non-zero",
							r.QueriesPerSecond, r.CPUTimePerSecond)
					}
				}
				if r.QueriesPerSecond > lastQPS {
					t.Errorf("unexpected increase in qps between ranges; prev=%.2f, current=%.2f, desc=%v",
						lastQPS, r.QueriesPerSecond, r.Desc)
				}
				lastQPS = r.QueriesPerSecond
			}
		}

	}
}

func TestHotRanges2Response(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv := rangetestutils.StartServer(t)
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	var hotRangesResp serverpb.HotRangesResponseV2
	if err := srvtestutils.PostStatusJSONProto(ts, "v2/hotranges", &serverpb.HotRangesRequest{}, &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.Ranges) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}
	lastQPS := math.MaxFloat64
	for _, r := range hotRangesResp.Ranges {
		if r.RangeID == 0 {
			t.Errorf("unexpected empty range id: %d", r.RangeID)
		}
		if r.QPS > 0 {
			if r.ReadsPerSecond == 0 && r.WritesPerSecond == 0 && r.ReadBytesPerSecond == 0 && r.WriteBytesPerSecond == 0 {
				t.Errorf("qps %.2f > 0, expected either reads=%.2f, writes=%.2f, readBytes=%.2f or writeBytes=%.2f to be non-zero",
					r.QPS, r.ReadsPerSecond, r.WritesPerSecond, r.ReadBytesPerSecond, r.WriteBytesPerSecond)
			}
			// If the architecture doesn't support sampling CPU, it
			// will also be zero.
			if grunning.Supported() && r.CPUTimePerSecond == 0 {
				t.Errorf("qps %.2f > 0, expected cpu=%.2f to be non-zero", r.QPS, r.CPUTimePerSecond)
			}
		}
		if r.QPS > lastQPS {
			t.Errorf("unexpected increase in qps between ranges; prev=%.2f, current=%.2f", lastQPS, r.QPS)
		}
		lastQPS = r.QPS
	}
}

func TestHotRanges2ResponseWithViewActivityOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)

	req := &serverpb.HotRangesRequest{}
	var hotRangesResp serverpb.HotRangesResponseV2
	if err := srvtestutils.PostStatusJSONProtoWithAdminOption(s, "v2/hotranges", req, &hotRangesResp, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}

	// Grant VIEWCLUSTERMETADATA and all test should work.
	db.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWCLUSTERMETADATA TO %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	if err := srvtestutils.PostStatusJSONProtoWithAdminOption(s, "v2/hotranges", req, &hotRangesResp, false); err != nil {
		t.Fatal(err)
	}

	// Grant VIEWACTIVITYREDACTED and all test should get permission errors.
	db.Exec(t, fmt.Sprintf("REVOKE SYSTEM  VIEWCLUSTERMETADATA FROM %s", apiconstants.TestingUserNameNoAdmin().Normalized()))
	if err := srvtestutils.PostStatusJSONProtoWithAdminOption(s, "v2/hotranges", req, &hotRangesResp, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}
}

func TestSpanStatsGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// We want to look at all the ranges in the cluster.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(ctx)

	span := roachpb.Span{
		Key:    roachpb.RKeyMin.AsRawKey(),
		EndKey: roachpb.RKeyMax.AsRawKey(),
	}
	request := roachpb.SpanStatsRequest{
		NodeID: "1",
		Spans:  []roachpb.Span{span},
	}

	client := s.GetStatusClient(t)

	response, err := client.SpanStats(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}
	initialRanges, err := s.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	responseSpanStats := response.SpanToStats[span.String()]
	if a, e := int(responseSpanStats.RangeCount), initialRanges; a != e {
		t.Fatalf("expected %d ranges, found %d", e, a)
	}
}
