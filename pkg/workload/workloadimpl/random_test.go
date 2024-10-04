// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadimpl_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"golang.org/x/exp/rand"
)

func BenchmarkRandStringFast(b *testing.B) {
	const strLen = 26
	rng := rand.NewSource(uint64(timeutil.Now().UnixNano()))
	buf := make([]byte, strLen)

	for i := 0; i < b.N; i++ {
		workloadimpl.RandStringFast(rng, buf, `0123456789abcdefghijklmnopqrstuvwxyz`)
	}
	b.SetBytes(strLen)
}
