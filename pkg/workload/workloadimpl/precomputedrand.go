// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadimpl

import (
	"sync"

	"golang.org/x/exp/rand"
)

// PrecomputedRand is a precomputed sequence of random data in some alphabet.
type PrecomputedRand []byte

// PrecomputedRandInit returns a init function that lazily initializes and
// returns a PrecomputedRand. This initialization work is done once and the
// result is shared, subsequent calls to return this shared one. The init
// function is concurrency safe.
func PrecomputedRandInit(rng rand.Source, length int, alphabet string) func() PrecomputedRand {
	var prOnce sync.Once
	var pr PrecomputedRand
	return func() PrecomputedRand {
		prOnce.Do(func() {
			pr = make(PrecomputedRand, length)
			RandStringFast(rng, pr, alphabet)
		})
		return pr
	}
}

// FillBytes fills the given buffer with precomputed random data, starting at
// the given offset (which is like a seed) and returning a new offset to be used
// on the next call. FillBytes is concurrency safe.
func (pr PrecomputedRand) FillBytes(offset int, buf []byte) int {
	if len(pr) == 0 {
		panic(`cannot fill from empty precomputed rand`)
	}
	prIdx := offset
	for bufIdx := 0; bufIdx < len(buf); {
		if prIdx == len(pr) {
			prIdx = 0
		}
		need, remaining := len(buf)-bufIdx, len(pr)-prIdx
		copyLen := need
		if copyLen > remaining {
			copyLen = remaining
		}
		newBufIdx, newPRIdx := bufIdx+copyLen, prIdx+copyLen
		copy(buf[bufIdx:newBufIdx], pr[prIdx:newPRIdx])
		bufIdx = newBufIdx
		prIdx = newPRIdx
	}
	return prIdx
}
