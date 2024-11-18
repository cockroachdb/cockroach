// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSpanStatsMetaScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())
	s := testCluster.Server(0)

	testSpans := []roachpb.Span{
		{
			Key:    keys.Meta1Prefix,
			EndKey: keys.Meta1KeyMax,
		},
		{
			Key:    keys.LocalMax,
			EndKey: keys.Meta2KeyMax,
		},
		{
			Key:    keys.Meta2Prefix,
			EndKey: keys.Meta2KeyMax,
		},
	}

	// SpanStats should have no problem finding descriptors for
	// spans up to and including Meta2KeyMax.
	for _, span := range testSpans {
		res, err := s.StatusServer().(serverpb.StatusServer).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: "0",
				Spans: []roachpb.Span{
					span,
				},
			},
		)
		require.NoError(t, err)
		require.Equal(t, int32(1), res.SpanToStats[span.String()].RangeCount)
	}
}

func TestSpanStatsFanOut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	const numNodes = 3
	serverArgs := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec},
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			1: serverArgs,
			2: serverArgs,
			3: serverArgs,
		},
	})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	// Create a number of ranges using splits.
	splitKeys := []string{"a", "c", "e", "g", "i"}
	for _, k := range splitKeys {
		_, _, err := s.SplitRange(roachpb.Key(k))
		require.NoError(t, err)
	}

	// Wait for splits to finish.
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey("z"))
		if actualRSpan := repl.Desc().RSpan(); !actualRSpan.Key.Equal(roachpb.RKey("i")) {
			return errors.Errorf("expected range %s to begin at key 'i'", repl)
		}
		return nil
	})

	// Create some keys across the ranges.
	incKeys := []string{"b", "bb", "bbb", "d", "dd", "h"}
	for _, k := range incKeys {
		if _, err := store.DB().Inc(context.Background(), []byte(k), 5); err != nil {
			t.Fatal(err)
		}
	}

	// Create spans encompassing different ranges.
	spans := []roachpb.Span{
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("i"),
		},
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("e"),
		},
		{
			Key:    roachpb.Key("e"),
			EndKey: roachpb.Key("i"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("d"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("bbb"),
		},
	}

	type testCase struct {
		span           roachpb.Span
		expectedRanges int32
		expectedKeys   int64
	}

	testCases := []testCase{
		{spans[0], 4, int64(6)},
		{spans[1], 1, int64(3)},
		{spans[2], 2, int64(5)},
		{spans[3], 2, int64(1)},
		{spans[4], 2, int64(3)},
		{spans[5], 1, int64(2)},
	}

	testutils.SucceedsSoon(t, func() error {
		// Multi-span request
		multiResult, err := s.StatusServer().(serverpb.StatusServer).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: "0",
				Spans:  spans,
			},
		)
		require.NoError(t, err)

		// Verify stats across different spans.
		for _, tcase := range testCases {
			rSpan, err := keys.SpanAddr(tcase.span)
			require.NoError(t, err)

			// Assert expected values from multi-span request
			spanStats := multiResult.SpanToStats[tcase.span.String()]
			if len(spanStats.StoreIDs) < 3 {
				if tcase.expectedRanges == 1 && len(spanStats.StoreIDs) > 3 {
					return errors.Newf("Multi-span: expected exactly 3 storeIDs in span [%s - %s], found %v",
						rSpan.Key.String(),
						rSpan.EndKey.String(),
						spanStats.StoreIDs,
					)
				}
				return errors.Newf("Multi-span: expected at least 3 storeIDs in span [%s - %s], found %v",
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.StoreIDs,
				)
			}
			if tcase.expectedRanges != spanStats.RangeCount {
				return errors.Newf("Multi-span: expected %d ranges in span [%s - %s], found %d",
					tcase.expectedRanges,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.RangeCount,
				)
			}
			if tcase.expectedKeys != spanStats.TotalStats.LiveCount {
				return errors.Newf(
					"Multi-span: expected %d keys in span [%s - %s], found %d",
					tcase.expectedKeys,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.TotalStats.LiveCount,
				)
			}

			approxKeys := numNodes * tcase.expectedKeys
			if approxKeys != spanStats.ApproximateTotalStats.LiveCount {
				return errors.Newf(
					"Multi-span: expected %d post-replicated keys in span [%s - %s], found %d",
					approxKeys,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.TotalStats.LiveCount,
				)
			}

			expectedReplicas := tcase.expectedRanges * 3
			if spanStats.ReplicaCount != expectedReplicas {
				return errors.Newf(
					"Multi-span: expected %d replica in span [%s - %s], found %d",
					expectedReplicas,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.ReplicaCount,
				)
			}
		}

		return nil
	})
}

// With replication off, we should only get responses from the
// first store and the replica count should be 1 per range.
func TestSpanStatsMultiStoreReplicationOff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	const numNodes = 3
	serverArgs := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec},
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			1: serverArgs,
			2: serverArgs,
			3: serverArgs,
		},
	})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	// Create a number of ranges using splits.
	splitKeys := []string{"a", "c", "e", "g", "i"}
	for _, k := range splitKeys {
		_, _, err := s.SplitRange(roachpb.Key(k))
		require.NoError(t, err)
	}

	// Create some keys across the ranges.
	incKeys := []string{"b", "bb", "bbb", "d", "dd", "h"}
	for _, k := range incKeys {
		if _, err := store.DB().Inc(context.Background(), []byte(k), 5); err != nil {
			t.Fatal(err)
		}
	}

	// Create spans encompassing different ranges.
	spans := []roachpb.Span{
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("i"),
		},
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("e"),
		},
		{
			Key:    roachpb.Key("e"),
			EndKey: roachpb.Key("i"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("d"),
		},
		{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("bbb"),
		},
	}

	type testCase struct {
		span           roachpb.Span
		expectedRanges int32
		expectedKeys   int64
	}

	testCases := []testCase{
		{spans[0], 4, int64(6)},
		{spans[1], 1, int64(3)},
		{spans[2], 2, int64(5)},
		{spans[3], 2, int64(1)},
		{spans[4], 2, int64(3)},
		{spans[5], 1, int64(2)},
	}

	testutils.SucceedsSoon(t, func() error {
		// Multi-span request
		multiResult, err := s.StatusServer().(serverpb.StatusServer).SpanStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: "0",
				Spans:  spans,
			},
		)
		require.NoError(t, err)

		equalStoreIDs := func(a, b []roachpb.StoreID) bool {
			if len(a) != len(b) {
				return false
			}
			for i := range a {
				if a[i] != b[i] {
					return false
				}
			}
			return true
		}

		// Verify stats across different spans.
		for _, tcase := range testCases {
			rSpan, err := keys.SpanAddr(tcase.span)
			require.NoError(t, err)

			// Assert expected values from multi-span request
			spanStats := multiResult.SpanToStats[tcase.span.String()]
			if !equalStoreIDs([]roachpb.StoreID{1}, spanStats.StoreIDs) {
				return errors.Newf("Multi-span: expected  storeIDs %v in span [%s - %s], found %v",
					[]roachpb.StoreID{1},
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.StoreIDs,
				)
			}
			if tcase.expectedRanges != spanStats.RangeCount {
				return errors.Newf("Multi-span: expected %d ranges in span [%s - %s], found %d",
					tcase.expectedRanges,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.RangeCount,
				)
			}
			if tcase.expectedKeys != spanStats.TotalStats.LiveCount {
				return errors.Newf(
					"Multi-span: expected %d keys in span [%s - %s], found %d",
					tcase.expectedKeys,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.TotalStats.LiveCount,
				)
			}

			approxKeys := tcase.expectedKeys
			if approxKeys != spanStats.ApproximateTotalStats.LiveCount {
				return errors.Newf(
					"Multi-span: expected %d post-replicated keys in span [%s - %s], found %d",
					approxKeys,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.TotalStats.LiveCount,
				)
			}

			if spanStats.ReplicaCount != tcase.expectedRanges {
				return errors.Newf(
					"Multi-span: expected %d replica in span [%s - %s], found %d",
					tcase.expectedRanges,
					rSpan.Key.String(),
					rSpan.EndKey.String(),
					spanStats.ReplicaCount,
				)
			}
		}

		return nil
	})

}

func TestSpanStatsFanOutFaultTolerance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test simulates a specific set of failures and verifies that
	// the response collects them appropriately. It is flaky under stress
	// and race.
	skip.UnderStress(t)
	skip.UnderRace(t)

	ctx := context.Background()
	const numNodes = 5

	type testCase struct {
		name         string
		dialCallback func(nodeID roachpb.NodeID) error
		nodeCallback func(ctx context.Context, nodeID roachpb.NodeID) error
		assertions   func(t *testing.T, res *roachpb.SpanStatsResponse)
	}

	containsError := func(errors []string, testString string) bool {
		for _, e := range errors {
			if strings.Contains(e, testString) {
				return true
			}
		}
		return false
	}

	testCases := []testCase{
		{
			// In a complete failure, no node is able to service requests successfully.
			name: "complete-fanout-failure",
			dialCallback: func(nodeID roachpb.NodeID) error {
				// On the 1st and 2nd node, simulate a connection error.
				if nodeID == 1 || nodeID == 2 {
					return errors.Newf("error dialing node %d", nodeID)
				}
				return nil
			},
			nodeCallback: func(ctx context.Context, nodeID roachpb.NodeID) error {
				// On the 3rd node, simulate some sort of KV error.
				if nodeID == 3 {
					return errors.Newf("kv error on node %d", nodeID)
				}

				// On the 4th and 5th node, simulate a request that takes a very long time.
				// In this case, nodeFn will block until the context is cancelled
				// i.e. if iterateNodes respects the timeout cluster setting.
				if nodeID == 4 || nodeID == 5 {
					<-ctx.Done()
					// Return an error that mimics the error returned
					// when a rpc's context is cancelled:
					return errors.Newf("node %d timed out", nodeID)
				}
				return nil
			},
			assertions: func(t *testing.T, res *roachpb.SpanStatsResponse) {
				// Expect to still be able to access SpanToStats for keys.EverythingSpan
				// without panicking, even though there was a failure on every node.
				require.Equal(t, int64(0), res.SpanToStats[keys.EverythingSpan.String()].TotalStats.LiveCount)
				require.Equal(t, 5, len(res.Errors))

				require.Equal(t, true, containsError(res.Errors, "error dialing node 1"))
				require.Equal(t, true, containsError(res.Errors, "error dialing node 2"))
				require.Equal(t, true, containsError(res.Errors, "kv error on node 3"))
				require.Equal(t, true, containsError(res.Errors, "node 4 timed out"))
				require.Equal(t, true, containsError(res.Errors, "node 5 timed out"))
			},
		},
		{
			// In a partial failure, nodes 1, 3, and 4 fail, and nodes 2 and 5 succeed.
			name: "partial-fanout-failure",
			dialCallback: func(nodeID roachpb.NodeID) error {
				if nodeID == 1 {
					return errors.Newf("error dialing node %d", nodeID)
				}
				return nil
			},
			nodeCallback: func(ctx context.Context, nodeID roachpb.NodeID) error {
				if nodeID == 3 {
					return errors.Newf("kv error on node %d", nodeID)
				}

				if nodeID == 4 {
					<-ctx.Done()
					// Return an error that mimics the error returned
					// when a rpc's context is cancelled:
					return errors.Newf("node %d timed out", nodeID)
				}
				return nil
			},
			assertions: func(t *testing.T, res *roachpb.SpanStatsResponse) {
				require.Greater(t, res.SpanToStats[keys.EverythingSpan.String()].TotalStats.LiveCount, int64(0),
					"response contains no stats: %v", res)
				// 3 nodes could not service their requests.
				require.Equal(t, 3, len(res.Errors),
					"response contains incorrect number of errors: %v", res,
				)

				require.Equal(t, true, containsError(res.Errors, "error dialing node 1"))
				require.Equal(t, true, containsError(res.Errors, "kv error on node 3"))
				require.Equal(t, true, containsError(res.Errors, "node 4 timed out"))

				// There should not be any errors for node 2 or node 5.
				require.Equal(t, false, containsError(res.Errors, "error dialing node 2"))
				require.Equal(t, false, containsError(res.Errors, "node 5 timed out"))
			},
		},
	}

	for _, tCase := range testCases {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			serverArgs := base.TestServerArgs{}
			serverArgs.Knobs.Server = &server.TestingKnobs{
				IterateNodesDialCallback: tCase.dialCallback,
				IterateNodesNodeCallback: tCase.nodeCallback,
			}

			tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{ServerArgs: serverArgs})
			defer tc.Stopper().Stop(ctx)

			sqlDB := tc.Server(0).SQLConn(t, serverutils.DBName("defaultdb"))
			_, err := sqlDB.Exec("SET CLUSTER SETTING server.span_stats.node.timeout = '3s'")
			require.NoError(t, err)

			res, err := tc.GetStatusClient(t, 0).SpanStats(ctx, &roachpb.SpanStatsRequest{
				NodeID: "0", // Indicates we want a fan-out.
				Spans:  []roachpb.Span{keys.EverythingSpan},
			})

			require.NoError(t, err)
			tCase.assertions(t, res)
		})
	}
}

// BenchmarkSpanStats measures the cost of collecting span statistics.
func BenchmarkSpanStats(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	createCluster := func(numNodes int) serverutils.TestClusterInterface {
		return serverutils.StartCluster(b, numNodes,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
			})
	}

	clusterSpecs := []struct {
		name     string
		numNodes int
	}{
		{
			name:     "3node",
			numNodes: 3,
		},
	}

	type testSpec struct {
		numSpans  int
		numRanges int
	}

	var testSpecs []testSpec
	numSpanCases := []int{10, 100, 200}
	numRangeCases := []int{25, 50, 100}
	for _, numSpans := range numSpanCases {
		for _, numRanges := range numRangeCases {
			testSpecs = append(testSpecs, testSpec{numSpans: numSpans, numRanges: numRanges})
		}
	}

	for _, cluster := range clusterSpecs {
		b.Run(cluster.name, func(b *testing.B) {
			tc := createCluster(cluster.numNodes)
			ctx := context.Background()
			defer tc.Stopper().Stop(ctx)
			for _, ts := range testSpecs {
				b.Run(fmt.Sprintf("BenchmarkSpanStats - span stats for %d node cluster, collecting %d spans with %d ranges each",
					cluster.numNodes,
					ts.numSpans,
					ts.numRanges,
				), func(b *testing.B) {

					tenant := tc.Server(0).ApplicationLayer()
					tenantCodec := keys.MakeSQLCodec(serverutils.TestTenantID())
					tenantPrefix := tenantCodec.TenantPrefix()

					makeKey := func(keys ...[]byte) roachpb.Key {
						return bytes.Join(keys, nil)
					}

					var spans []roachpb.Span

					// Create a table spans
					for i := 0; i < ts.numSpans; i++ {
						spanStartKey := makeKey(
							tenantPrefix,
							[]byte{byte(i)},
						)
						spanEndKey := makeKey(
							tenantPrefix,
							[]byte{byte(i + 1)},
						)
						// Create ranges.
						for j := 0; j < ts.numRanges; j++ {
							splitKey := makeKey(spanStartKey, []byte{byte(j)})
							_, _, err := tc.Server(0).SplitRange(splitKey)
							require.NoError(b, err)
						}
						spans = append(spans, roachpb.Span{
							Key:    spanStartKey,
							EndKey: spanEndKey,
						})
					}

					_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING server.span_stats.span_batch_limit = $1`, ts.numSpans)
					require.NoError(b, err)
					_, err = tc.ServerConn(0).Exec(`SET CLUSTER SETTING server.span_stats.range_batch_limit = $1`, ts.numRanges)
					require.NoError(b, err)

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, err := tenant.TenantStatusServer().(serverpb.TenantStatusServer).SpanStats(ctx,
							&roachpb.SpanStatsRequest{
								NodeID: "0", // 0 indicates we want stats from all nodes.
								Spans:  spans,
							})
						require.NoError(b, err)
					}
					b.StopTimer()
				})
			}
		})
	}
}
