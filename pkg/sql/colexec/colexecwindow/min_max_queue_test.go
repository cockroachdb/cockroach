// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecwindow

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMinMaxQueue(t *testing.T) {
	const (
		chanceToRemove = 0.1
		numIterations  = 10
		maxIncrement   = 100
		maxValuesToAdd = 1000
	)

	rng, _ := randutil.NewTestRand()

	var queue minMaxQueue
	var oracle []uint32

	for i := 0; i < numIterations; i++ {
		oracle = oracle[:0]
		queue.reset()
		queue.maxLength = rng.Intn(maxValuesToAdd)
		if cap(queue.buffer) > queue.maxLength {
			// We have to nil out the buffer to ensure that the queue does not grow
			// too large, since it will fill out all available capacity.
			queue.buffer = nil
		}
		valuesToAdd := rng.Intn(maxValuesToAdd)
		var num uint32
		for j := 0; j < valuesToAdd; j++ {
			num += uint32(rng.Intn(maxIncrement)) + 1 // Ensure no duplicates.
			if len(oracle) < queue.maxLength {
				oracle = append(oracle, num)
			}
			queue.addLast(num)
			if len(oracle) > 0 && rng.Float64() < chanceToRemove {
				idx := rng.Intn(len(oracle))
				val := oracle[idx]
				oracle = append(oracle[:0], oracle[idx:]...)
				queue.removeAllBefore(val)
				for k := 0; k < rng.Intn(len(oracle)); k++ {
					oracle = oracle[:len(oracle)-1]
					queue.removeLast()
				}
			}
		}
		require.LessOrEqual(t, queue.len(), queue.maxLength, "queue length exceeds maximum")
		require.Equal(t, len(oracle), queue.len(), "expected equal lengths")
		for j := 0; j < len(oracle); j++ {
			require.Equalf(t, oracle[j], queue.get(j), "wrong value at index: %d", j)
		}
	}
}
