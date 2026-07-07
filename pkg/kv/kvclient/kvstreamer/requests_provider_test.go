// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import (
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// TestDeepCopyRequestsPreservesStreamerFields verifies that deepCopyRequests
// re-applies the Streamer-level request fields (key locking and raw MVCC
// values) onto the fresh request copies. This guards against a regression
// where ReturnRawMVCCValues was dropped when a request was reconstructed.
func TestDeepCopyRequestsPreservesStreamerFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	getUnion := func(key string) kvpb.RequestUnion {
		var u kvpb.RequestUnion_Get
		u.Get = &kvpb.GetRequest{}
		u.Get.SetSpan(roachpb.Span{Key: roachpb.Key(key)})
		return kvpb.RequestUnion{Value: &u}
	}
	scanUnion := func(start, end string) kvpb.RequestUnion {
		var u kvpb.RequestUnion_Scan
		u.Scan = &kvpb.ScanRequest{}
		u.Scan.SetSpan(roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)})
		return kvpb.RequestUnion{Value: &u}
	}
	revScanUnion := func(start, end string) kvpb.RequestUnion {
		var u kvpb.RequestUnion_ReverseScan
		u.ReverseScan = &kvpb.ReverseScanRequest{}
		u.ReverseScan.SetSpan(roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)})
		return kvpb.RequestUnion{Value: &u}
	}

	for _, reverse := range []bool{false, true} {
		t.Run("reverse="+strconv.FormatBool(reverse), func(t *testing.T) {
			s := &Streamer{
				lockStrength:        lock.Exclusive,
				lockDurability:      lock.Replicated,
				reverse:             reverse,
				returnRawMVCCValues: true,
			}
			r := singleRangeBatch{numGetsInReqs: 1}
			if reverse {
				r.reqs = []kvpb.RequestUnion{getUnion("a"), revScanUnion("b", "c")}
			} else {
				r.reqs = []kvpb.RequestUnion{getUnion("a"), scanUnion("b", "c")}
			}

			r.deepCopyRequests(s)

			for i := range r.reqs {
				req := r.reqs[i].GetInner()
				switch req := req.(type) {
				case *kvpb.GetRequest:
					require.True(t, req.ReturnRawMVCCValues)
					require.Equal(t, lock.Exclusive, req.KeyLockingStrength)
					require.Equal(t, lock.Replicated, req.KeyLockingDurability)
				case *kvpb.ScanRequest:
					require.True(t, req.ReturnRawMVCCValues)
					require.Equal(t, lock.Exclusive, req.KeyLockingStrength)
					require.Equal(t, lock.Replicated, req.KeyLockingDurability)
				case *kvpb.ReverseScanRequest:
					require.True(t, req.ReturnRawMVCCValues)
					require.Equal(t, lock.Exclusive, req.KeyLockingStrength)
					require.Equal(t, lock.Replicated, req.KeyLockingDurability)
				default:
					t.Fatalf("unexpected request type %T", req)
				}
			}
		})
	}
}
