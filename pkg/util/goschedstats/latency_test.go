// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goschedstats

import "testing"

func BenchmarkSampleSchedulerLatency(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sampleSchedulerLatencies()
	}
}

func BenchmarkComputeSchedulerP99Latency(b *testing.B) {
	s := sampleSchedulerLatencies()
	for i := 0; i < b.N; i++ {
		percentile(s, 0.99)
	}
}

func BenchmarkCloneLatencyHistogram(b *testing.B) {
	s := sampleSchedulerLatencies()
	for i := 0; i < b.N; i++ {
		clone(s)
	}
}

func BenchmarkSubtractLatencyHistograms(b *testing.B) {
	a, z := sampleSchedulerLatencies(), sampleSchedulerLatencies()
	for i := 0; i < b.N; i++ {
		sub(a, z)
	}
}
