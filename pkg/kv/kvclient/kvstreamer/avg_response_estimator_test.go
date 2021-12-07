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

func TestAvgResponseEstimator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var e avgResponseEstimator

	// Before receiving any responses, we should be using the initial estimate.
	require.Equal(t, int64(initialAvgResponseSize), e.getAvgResponseSize())

	// Simulate receiving a single response.
	firstResponseSize := int64(42)
	e.update(firstResponseSize, 1)
	// The estimate should now be exactly the size of that single response.
	require.Equal(t, firstResponseSize, e.getAvgResponseSize())

	// Simulate receiving 100 small BatchResponses.
	smallResponseSize := int64(63)
	for i := 0; i < 100; i++ {
		e.update(smallResponseSize*5, 5)
	}
	// The estimate should now be pretty close to the size of a single response
	// in the small BatchResponse.
	diff := smallResponseSize - e.getAvgResponseSize()
	require.True(t, math.Abs(float64(diff))/float64(smallResponseSize) < 0.05)

	// Now simulate receiving 10 large BatchResponses.
	largeResponseSize := int64(17)
	for i := 0; i < 10; i++ {
		e.update(largeResponseSize*1000, 1000)
	}
	// The estimate should now be pretty close to the size of a single response
	// in the large BatchResponse.
	diff = largeResponseSize - e.getAvgResponseSize()
	require.True(t, math.Abs(float64(diff))/float64(smallResponseSize) < 0.15)
}
