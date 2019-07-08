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
	"math"

	"golang.org/x/exp/rand"
)

// RandStringFast is a non-specialized random string generator with an even
// distribution of alphabet in the output.
func RandStringFast(rng rand.Source, buf []byte, alphabet string) {
	// We could pull these computations out to be done once per alphabet, but at
	// that point, you likely should consider PrecomputedRand.
	alphabetLen := uint64(len(alphabet))
	// floor(log(math.MaxUint64)/log(alphabetLen))
	lettersCharsPerRand := uint64(math.Log(float64(math.MaxUint64)) / math.Log(float64(alphabetLen)))

	var r, charsLeft uint64
	for i := 0; i < len(buf); i++ {
		if charsLeft == 0 {
			r = rng.Uint64()
			charsLeft = lettersCharsPerRand
		}
		buf[i] = alphabet[r%alphabetLen]
		r = r / alphabetLen
		charsLeft--
	}
}
