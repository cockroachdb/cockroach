// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestReplicaRankings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rr := newReplicaRankings()

	testCases := []struct {
		replicasByQPS []float64
	}{
		{replicasByQPS: []float64{}},
		{replicasByQPS: []float64{0}},
		{replicasByQPS: []float64{1, 0}},
		{replicasByQPS: []float64{3, 2, 1, 0}},
		{replicasByQPS: []float64{3, 3, 2, 2, 1, 1, 0, 0}},
		{replicasByQPS: []float64{1.1, 1.0, 0.9, -0.9, -1.0, -1.1}},
	}

	for _, tc := range testCases {
		acc := rr.newAccumulator()

		// Randomize the order of the inputs each time the test is run.
		want := make([]float64, len(tc.replicasByQPS))
		copy(want, tc.replicasByQPS)
		rand.Shuffle(len(tc.replicasByQPS), func(i, j int) {
			tc.replicasByQPS[i], tc.replicasByQPS[j] = tc.replicasByQPS[j], tc.replicasByQPS[i]
		})

		for i, replQPS := range tc.replicasByQPS {
			acc.addReplica(replicaWithStats{
				repl: &Replica{RangeID: roachpb.RangeID(i)},
				qps:  replQPS,
			})
		}
		rr.update(acc)

		// Make sure we can read off all expected replicas in the correct order.
		repls := rr.topQPS()
		if len(repls) != len(want) {
			t.Errorf("wrong number of replicas in output; got: %v; want: %v", repls, tc.replicasByQPS)
			continue
		}
		for i := range want {
			if repls[i].qps != want[i] {
				t.Errorf("got %f for %d'th element; want %f (input: %v)", repls[i].qps, i, want, tc.replicasByQPS)
				break
			}
		}
		replsCopy := rr.topQPS()
		if !reflect.DeepEqual(repls, replsCopy) {
			t.Errorf("got different replicas on second call to topQPS; first call: %v, second call: %v", repls, replsCopy)
		}
	}
}
