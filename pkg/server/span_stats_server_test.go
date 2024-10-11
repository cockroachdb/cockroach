// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func makeSpans(n int, offset int) roachpb.Spans {
	spans := make([]roachpb.Span, 0, n)

	for i := offset; i < n+offset; i++ {
		startKey := make(roachpb.Key, 8)
		endKey := make(roachpb.Key, 8)
		binary.LittleEndian.PutUint64(startKey, uint64(i))
		binary.LittleEndian.PutUint64(endKey, uint64(i+1))
		spans = append(spans, roachpb.Span{Key: startKey, EndKey: endKey})
	}

	return spans
}

func TestSpanStatsBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	type testCase struct {
		nSpans          int
		limit           int
		expectedBatches [][]roachpb.Span
	}

	testCases := []testCase{
		{nSpans: 2, limit: 5, expectedBatches: [][]roachpb.Span{
			makeSpans(2, 0), // Expect 1 batch of length 2.
		}},
		{nSpans: 5, limit: 5, expectedBatches: [][]roachpb.Span{
			makeSpans(5, 0), // Expect 1 batch of length 5.
		}},
		{nSpans: 4, limit: 3, expectedBatches: [][]roachpb.Span{
			makeSpans(3, 0), // Expect 1st batch of length 3.
			makeSpans(1, 3), // Expect 2nd batch of length 1.
		}},
		{nSpans: 5, limit: 3, expectedBatches: [][]roachpb.Span{
			makeSpans(3, 0), // Expect 1st batch of length 3.
			makeSpans(2, 3), // Expect 2nd batch of length 2.
		}},
		{nSpans: 6, limit: 3, expectedBatches: [][]roachpb.Span{
			makeSpans(3, 0), // Expect 1st batch of length 3.
			makeSpans(3, 3), // Expect 2nd batch of length 3.
		}},
	}

	for _, tcase := range testCases {
		numBatches := 0
		mockSpanStatsEndpoint := func(
			ctx context.Context,
			req *roachpb.SpanStatsRequest,
		) (*roachpb.SpanStatsResponse, error) {

			require.Equal(t, req.Spans, tcase.expectedBatches[numBatches])
			numBatches++

			res := &roachpb.SpanStatsResponse{}
			res.SpanToStats = make(map[string]*roachpb.SpanStats)

			// Provide a response for all spans, and an error per
			// invocation. This lets us confirm that all batched
			// responses make their way back to the caller.
			for _, sp := range req.Spans {
				res.SpanToStats[sp.String()] = &roachpb.SpanStats{}
			}
			res.Errors = append(res.Errors, "some error")
			return res, nil
		}

		req := &roachpb.SpanStatsRequest{}
		req.Spans = makeSpans(tcase.nSpans, 0 /* offset */)
		res, err := batchedSpanStats(ctx, req, mockSpanStatsEndpoint, tcase.limit)
		require.NoError(t, err)

		// Assert that the mocked span stats function is invoked the correct
		// number of times.
		require.Equal(t, len(tcase.expectedBatches), numBatches)

		// Assert that the responses from each batch are returned to the caller.
		require.Equal(t, tcase.nSpans, len(res.SpanToStats))

		// Assert that the errors from each batch are returned to the caller.
		require.Equal(t, len(tcase.expectedBatches), len(res.Errors))
	}

}

func TestCollectSpanStatsResponses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *roachpb.SpanStatsResponse
	}
	type testCase struct {
		actualRes     *roachpb.SpanStatsResponse
		nodeResponses []nodeResponse
		expectedRes   *roachpb.SpanStatsResponse
	}

	var testCases []testCase

	// test case 1
	tc1 := testCase{
		actualRes: createSpanStatsResponse("span1", "span2"),
		nodeResponses: []nodeResponse{
			{nodeID: 1, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span1": {RangeCount: 1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{1},
						ReplicaCount:         1,
					},
				},
			}},
			{nodeID: 2, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span2": {RangeCount: 1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{2},
						ReplicaCount:         1,
					},
				},
			}},
		},
		expectedRes: &roachpb.SpanStatsResponse{
			SpanToStats: map[string]*roachpb.SpanStats{
				"span1": {RangeCount: 1,
					ApproximateDiskBytes: 1,
					RemoteFileBytes:      1,
					ExternalFileBytes:    1,
					StoreIDs:             []roachpb.StoreID{1},
					ReplicaCount:         1,
				},
				"span2": {RangeCount: 1,
					ApproximateDiskBytes: 1,
					RemoteFileBytes:      1,
					ExternalFileBytes:    1,
					StoreIDs:             []roachpb.StoreID{2},
					ReplicaCount:         1,
				},
			},
		},
	}
	testCases = append(testCases, tc1)

	// test case 2 - span is replicated in node 1 and 2
	totalStats1 := enginepb.MVCCStats{LiveBytes: 1}
	totalStats2 := enginepb.MVCCStats{LiveBytes: 2}
	expectedApproxTotalStats := enginepb.MVCCStats{}
	expectedApproxTotalStats.Add(totalStats1)
	expectedApproxTotalStats.Add(totalStats2)
	tc2 := testCase{
		actualRes: createSpanStatsResponse("span1", "span2"),
		nodeResponses: []nodeResponse{
			{nodeID: 1, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span1": {
						TotalStats:           totalStats1,
						RangeCount:           1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{1, 2},
						ReplicaCount:         1,
					},
				},
			}},
			{nodeID: 2, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span1": {
						TotalStats:           totalStats2,
						RangeCount:           1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{2},
						ReplicaCount:         1,
					},
				},
			}},
		},
		expectedRes: &roachpb.SpanStatsResponse{
			SpanToStats: map[string]*roachpb.SpanStats{
				"span1": {
					TotalStats:            totalStats1,
					ApproximateTotalStats: expectedApproxTotalStats,
					RangeCount:            1,
					ApproximateDiskBytes:  2,
					RemoteFileBytes:       2,
					ExternalFileBytes:     2,
					StoreIDs:              []roachpb.StoreID{1, 2},
					ReplicaCount:          1,
				},
			},
		},
	}
	testCases = append(testCases, tc2)

	// test case 3 - node response spanToStats value is nil for span2
	tc3 := testCase{
		actualRes: createSpanStatsResponse("span1", "span2"),
		nodeResponses: []nodeResponse{
			{nodeID: 1, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span1": {
						RangeCount:           1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{1},
						ReplicaCount:         1,
					},
				},
			}},
			{nodeID: 2, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span2": nil,
				},
			}},
		},
		expectedRes: &roachpb.SpanStatsResponse{
			SpanToStats: map[string]*roachpb.SpanStats{
				"span1": {
					RangeCount:           1,
					ApproximateDiskBytes: 1,
					RemoteFileBytes:      1,
					ExternalFileBytes:    1,
					StoreIDs:             []roachpb.StoreID{1},
					ReplicaCount:         1,
				},
				"span2": {},
			},
		},
	}
	testCases = append(testCases, tc3)

	// test case 4 - node response contains span not in original response struct
	tc4 := testCase{
		actualRes: createSpanStatsResponse("span1"),
		nodeResponses: []nodeResponse{
			{nodeID: 1, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span1": {
						RangeCount:           1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{1},
						ReplicaCount:         1,
					},
				},
			}},
			{nodeID: 2, resp: &roachpb.SpanStatsResponse{
				SpanToStats: map[string]*roachpb.SpanStats{
					"span2": {
						RangeCount:           1,
						ApproximateDiskBytes: 1,
						RemoteFileBytes:      1,
						ExternalFileBytes:    1,
						StoreIDs:             []roachpb.StoreID{2},
						ReplicaCount:         1,
					},
				},
			}},
		},
		expectedRes: &roachpb.SpanStatsResponse{
			SpanToStats: map[string]*roachpb.SpanStats{
				"span1": {
					RangeCount:           1,
					ApproximateDiskBytes: 1,
					RemoteFileBytes:      1,
					ExternalFileBytes:    1,
					StoreIDs:             []roachpb.StoreID{1},
					ReplicaCount:         1,
				},
				"span2": {
					RangeCount:           1,
					ApproximateDiskBytes: 1,
					RemoteFileBytes:      1,
					ExternalFileBytes:    1,
					StoreIDs:             []roachpb.StoreID{2},
					ReplicaCount:         1,
				},
			},
		},
	}
	testCases = append(testCases, tc4)

	ctx := context.Background()
	for _, tc := range testCases {
		cb := collectSpanStatsResponses(ctx, tc.actualRes)
		for _, nodeResp := range tc.nodeResponses {
			cb(nodeResp.nodeID, nodeResp.resp)
		}

		for spanStr, spanStats := range tc.expectedRes.SpanToStats {
			actualSpanStat, ok := tc.actualRes.SpanToStats[spanStr]
			require.True(t, ok)
			require.NotNil(t, actualSpanStat)
			require.Equal(t, spanStats.TotalStats, actualSpanStat.TotalStats)
			require.Equal(t, spanStats.RangeCount, actualSpanStat.RangeCount)
			require.Equal(t, spanStats.ApproximateDiskBytes, actualSpanStat.ApproximateDiskBytes)
			require.Equal(t, spanStats.RemoteFileBytes, actualSpanStat.RemoteFileBytes)
			require.Equal(t, spanStats.ExternalFileBytes, actualSpanStat.ExternalFileBytes)
			require.Equal(t, spanStats.StoreIDs, actualSpanStat.StoreIDs)
			require.Equal(t, spanStats.ReplicaCount, actualSpanStat.ReplicaCount)
		}
	}
}

func createSpanStatsResponse(spanStrs ...string) *roachpb.SpanStatsResponse {
	resp := &roachpb.SpanStatsResponse{}
	resp.SpanToStats = make(map[string]*roachpb.SpanStats)
	for _, str := range spanStrs {
		resp.SpanToStats[str] = &roachpb.SpanStats{}
	}
	return resp
}
