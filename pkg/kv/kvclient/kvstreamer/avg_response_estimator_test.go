// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func withMultiple(s int64) int64 {
	return int64(float64(s) * defaultAvgResponseSizeMultiple)
}

func TestAvgResponseEstimator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	e := avgResponseEstimator{avgResponseSizeMultiple: defaultAvgResponseSizeMultiple}

	// Before receiving any responses, we should be using the initial estimate.
	require.Equal(t, int64(InitialAvgResponseSize), e.getAvgResponseSize())

	// Simulate receiving a single response.
	firstResponseSize := int64(42)
	e.update(firstResponseSize, 1 /* numRequestsStarted */)
	// The estimate should now be the size of that single response times
	// defaultAvgResponseSizeMultiple.
	require.Equal(t, withMultiple(firstResponseSize), e.getAvgResponseSize())

	// Simulate receiving 100 small BatchResponses.
	smallResponseSize := int64(63)
	for i := 0; i < 100; i++ {
		e.update(smallResponseSize*5, 5 /* numRequestsStarted */)
	}
	// The estimate should now be pretty close to the size of a single response
	// in the small BatchResponse (after adjusting with the multiple).
	diff := withMultiple(smallResponseSize) - e.getAvgResponseSize()
	require.True(t, math.Abs(float64(diff))/float64(smallResponseSize) < 0.05)

	// Now simulate receiving 10 large BatchResponses.
	largeResponseSize := int64(17)
	for i := 0; i < 10; i++ {
		e.update(largeResponseSize*1000, 1000 /* numRequestsStarted */)
	}
	// The estimate should now be pretty close to the size of a single response
	// in the large BatchResponse (after adjusting with the multiple).
	diff = withMultiple(largeResponseSize) - e.getAvgResponseSize()
	require.True(t, math.Abs(float64(diff))/float64(smallResponseSize) < 0.15)
}

func TestAvgResponseSizeForPartialResponses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	e := avgResponseEstimator{avgResponseSizeMultiple: defaultAvgResponseSizeMultiple}
	// Simulate a ScanRequest that needs to fetch 100 rows, 1KiB each in size.
	const totalRows, rowSize = 100, 1 << 10
	rowsLeft := int64(totalRows)
	var batchRequestsCount int
	for ; rowsLeft > 0; batchRequestsCount++ {
		targetBytes := e.getAvgResponseSize()
		rowsReceived := targetBytes / rowSize
		if rowsReceived > rowsLeft {
			rowsReceived = rowsLeft
		}
		rowsLeft -= rowsReceived
		// Only the first BatchRequest starts evaluation of the ScanRequest.
		var numRequestsStarted int
		if batchRequestsCount == 0 {
			numRequestsStarted = 1
		}
		e.update(rowsReceived*rowSize, numRequestsStarted)
	}
	// We started with the TargetBytes equal to the initial estimate of 1KiB,
	// and then with each update the estimate should have grown. In particular,
	// we expect 7 BatchRequests total that fetch 1, 1, 3, 7, 18, 45, 25 rows
	// respectively (note that the growth is faster than 2x because we use 1.5
	// multiple on top of the average).
	require.Equal(t, 7, batchRequestsCount)
	// From the perspective of the response estimator, we received only one
	// response (that happened to be paginated across BatchRequests), so our
	// estimate should match exactly the total size.
	require.Equal(t, withMultiple(totalRows*rowSize), e.getAvgResponseSize())
}
