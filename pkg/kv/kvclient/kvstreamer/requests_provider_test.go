// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		requests[i].positions = []int{rng.Intn(maxNumRequests)}
		priorities[i] = requests[i].priority()
	}
	sort.Ints(priorities)

	p := newInOrderRequestsProvider()
	p.enqueue(requests)

	for len(priorities) > 0 {
		// Simulate issuing a request.
		p.Lock()
		next := p.nextLocked()
		p.removeNextLocked()
		p.Unlock()
		require.Equal(t, priorities[0], next.priority())
		priorities = priorities[1:]
		// With 50% probability simulate that a resume request with random
		// priority is added.
		if rng.Float64() < 0.5 {
			// Note that in reality the position of the resume request cannot
			// have lower value than of the original request, but it's ok for
			// the test.
			next.positions[0] = rng.Intn(maxNumRequests)
			p.add(next)
			priorities = append(priorities, next.priority())
			sort.Ints(priorities)
		}
	}
}
