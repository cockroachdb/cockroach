// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/rand"
)

func BenchmarkRandStringFast(b *testing.B) {
	const strLen = 26
	rng := rand.NewSource(uint64(timeutil.Now().UnixNano()))
	buf := make([]byte, strLen)

	b.Run(`letters`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringLetters(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`numbers`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringNumbers(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`aChars`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringAChars(rng, buf)
		}
		b.SetBytes(strLen)
	})
}
