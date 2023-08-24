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
	"context"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func makeSpans(n int) roachpb.Spans {
	spans := make([]roachpb.Span, 0, n)

	for i := 0; i < n; i++ {
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
		expectedBatches int
	}

	testCases := []testCase{
		{nSpans: 0, limit: 100, expectedBatches: 0},
		{nSpans: 10, limit: 100, expectedBatches: 1},
		{nSpans: 100, limit: 100, expectedBatches: 1},
		{nSpans: 101, limit: 100, expectedBatches: 2},
		{nSpans: 200, limit: 100, expectedBatches: 2},
	}

	for _, tcase := range testCases {
		numBatches := 0
		mockSpanStatsEndpoint := func(
			ctx context.Context,
			req *roachpb.SpanStatsRequest,
		) (*roachpb.SpanStatsResponse, error) {
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
		req.Spans = makeSpans(tcase.nSpans)
		res, err := batchedSpanStats(ctx, req, mockSpanStatsEndpoint, tcase.limit)
		require.NoError(t, err)

		// Assert that the mocked span stats function is invoked the correct
		// number of times.
		require.Equal(t, tcase.expectedBatches, numBatches)

		// Assert that the responses from each batch are returned to the caller.
		require.Equal(t, tcase.nSpans, len(res.SpanToStats))

		// Assert that the errors from each batch are returned to the caller.
		require.Equal(t, tcase.expectedBatches, len(res.Errors))
	}

}
