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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestInOrderRequestsProvider verifies that the inOrderRequestsProvider returns
// the requests with the highest priority (i.e. lower 'priority' value) first.
func TestInOrderRequestsProvider(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()

	// Simulate creating a random number of requests.
	const maxNumRequests = 50
	requests := make([]singleRangeBatch, rng.Intn(maxNumRequests)+1)
	priorities := make([]int, len(requests))
	for i := range requests {
		requests[i].priority = rng.Intn(maxNumRequests)
		priorities[i] = requests[i].priority
	}
	sort.Ints(priorities)

	p := newInOrderRequestsProvider()
	p.enqueue(requests)

	for len(priorities) > 0 {
		// Simulate issuing a request.
		p.Lock()
		first := p.firstLocked()
		p.removeFirstLocked()
		p.Unlock()
		require.Equal(t, priorities[0], first.priority)
		priorities = priorities[1:]
		// With 50% probability simulate that a resume request with random
		// priority is added.
		if rng.Float64() < 0.5 {
			// Note that in reality the priority of the resume request cannot
			// have lower value than of the original request, but it's ok for
			// the test.
			first.priority = rng.Intn(maxNumRequests)
			p.add(first)
			priorities = append(priorities, first.priority)
			sort.Ints(priorities)
		}
	}
}
