// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadimpl_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestPrecomputedRand(t *testing.T) {
	const precomputeLen = 100

	seed0 := workloadimpl.PrecomputedRandInit(rand.NewSource(0), precomputeLen, alphabet)()
	seed1 := workloadimpl.PrecomputedRandInit(rand.NewSource(1), precomputeLen, alphabet)()
	numbers := workloadimpl.PrecomputedRandInit(rand.NewSource(0), precomputeLen, `0123456789`)()

	const shorterThanPrecomputed, longerThanPrecomputed = precomputeLen / 10, precomputeLen + 7

	offset := 0
	fillBytes := func(pr workloadimpl.PrecomputedRand, length int) []byte {
		buf := make([]byte, length)
		offset = pr.FillBytes(offset, buf)
		return buf
	}

	short0 := fillBytes(seed0, shorterThanPrecomputed)
	long0 := fillBytes(seed0, longerThanPrecomputed)

	// Offset has advanced, we should get a different result.
	short0Different := fillBytes(seed0, shorterThanPrecomputed)
	require.NotEqual(t, short0, short0Different)

	// Reset the offset and verify that the results are repeatable
	offset = 0
	short0B := fillBytes(seed0, shorterThanPrecomputed)
	long0B := fillBytes(seed0, longerThanPrecomputed)
	require.Equal(t, short0, short0B)
	require.Equal(t, long0, long0B)

	// Reset the offset and verify that a different seed gets different results.
	offset = 0
	short1 := fillBytes(seed1, shorterThanPrecomputed)
	long1 := fillBytes(seed1, longerThanPrecomputed)
	require.NotEqual(t, short0, short1)
	require.NotEqual(t, long0, long1)

	// Reset the offset and verify that a different alphabet gets different
	// results.
	offset = 0
	shortNumbers := fillBytes(numbers, shorterThanPrecomputed)
	longNumbers := fillBytes(numbers, longerThanPrecomputed)
	require.NotEqual(t, short0, shortNumbers)
	require.NotEqual(t, long0, longNumbers)
}

func BenchmarkPrecomputedRand(b *testing.B) {
	const precomputeLen = 10000
	pr := workloadimpl.PrecomputedRandInit(
		rand.NewSource(uint64(timeutil.Now().UnixNano())), precomputeLen, alphabet)()

	const shortLen, mediumLen, longLen = 2, 100, 100000
	scratch := make([]byte, longLen)
	var randOffset int

	for _, l := range []int{shortLen, mediumLen, longLen} {
		b.Run(fmt.Sprintf(`len=%d`, l), func(b *testing.B) {
			randOffset = 0
			buf := scratch[:l]
			for i := 0; i < b.N; i++ {
				randOffset = pr.FillBytes(randOffset, buf)
			}
			b.SetBytes(int64(len(buf)))
		})
	}
}
