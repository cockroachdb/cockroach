// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestLocalSpanStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{DisableCanAckBeforeApplication: true},
		},
	})
	s := serv.(*TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
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

	// Verify stats across different spans.
	for _, tcase := range []struct {
		startKey       string
		endKey         string
		expectedRanges int32
		expectedKeys   int64
	}{
		{"a", "i", 4, 6},
		{"a", "c", 1, 3},
		{"b", "e", 2, 5},
		{"e", "i", 2, 1},
		{"b", "d", 2, 3},
		{"b", "bbb", 1, 2},
	} {
		start, end := tcase.startKey, tcase.endKey
		span := roachpb.Span{
			Key:    roachpb.Key(start),
			EndKey: roachpb.Key(end),
		}

		result, err := s.status.getLocalStats(ctx,
			&roachpb.SpanStatsRequest{
				NodeID: "0",
				Spans:  []roachpb.Span{span},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		resultSpanStats := result.SpanToStats[span.String()]
		if a, e := resultSpanStats.RangeCount, tcase.expectedRanges; a != e {
			t.Errorf("Expected %d ranges in span [%s - %s], found %d", e, start, end, a)
		}
		if a, e := resultSpanStats.TotalStats.LiveCount, tcase.expectedKeys; a != e {
			t.Errorf("Expected %d keys in span [%s - %s], found %d", e, start, end, a)
		}
	}
}

// BenchmarkSpanStats measures the cost of collecting span statistics.
func BenchmarkSpanStats(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	createCluster := func(numNodes int) serverutils.TestClusterInterface {
		return serverutils.StartNewTestCluster(b, numNodes,
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

					tenant := tc.Server(0).TenantOrServer()
					tenantCodec := keys.MakeSQLCodec(serverutils.TestTenantID())
					tenantPrefix := tenantCodec.TenantPrefix()

					makeKey := func(keys ...[]byte) roachpb.Key {
						return bytes.Join(keys, nil)
					}

					var spans []roachpb.Span

					// Create a table spans
					var spanStartKey roachpb.Key
					for i := 0; i < ts.numSpans; i++ {
						spanStartKey = nil
						// Create ranges.
						var key roachpb.Key
						for j := 0; j < ts.numRanges; j++ {
							key = makeKey(tenantPrefix, []byte(strconv.Itoa(i*j)))
							if spanStartKey == nil {
								spanStartKey = key
							}
							_, _, err := tc.Server(0).SplitRange(key)
							require.NoError(b, err)
						}
						spans = append(spans, roachpb.Span{
							Key:    spanStartKey,
							EndKey: key,
						})
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for _, span := range spans {
							_, err := tenant.TenantStatusServer().(serverpb.TenantStatusServer).SpanStats(ctx,
								&roachpb.SpanStatsRequest{
									NodeID: "0", // 0 indicates we want stats from all nodes.
									Spans:  []roachpb.Span{span},
								})
							require.NoError(b, err)
						}
					}
					b.StopTimer()
				})
			}
		})
	}
}
