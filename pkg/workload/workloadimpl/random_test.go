// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadimpl_test

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
)

func BenchmarkRandStringFast(b *testing.B) {
	const strLen = 26
	rng := rand.NewPCG(rand.Uint64(), rand.Uint64())
	buf := make([]byte, strLen)

	for i := 0; i < b.N; i++ {
		workloadimpl.RandStringFast(rng, buf, `0123456789abcdefghijklmnopqrstuvwxyz`)
	}
	b.SetBytes(strLen)
}
