// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func BenchmarkMVCCValueMerger(b *testing.B) {
	d1 := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 12345,
		SampleDurationNanos: 100,
		Offset:              make([]int32, 100),
		Last:                make([]float64, 100),
		Count:               make([]uint32, 100),
		Sum:                 make([]float64, 100),
		Max:                 make([]float64, 100),
		Min:                 make([]float64, 100),
		First:               make([]float64, 100),
	}
	d2 := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 12345,
		SampleDurationNanos: 100,
		Offset:              make([]int32, 200),
		Last:                make([]float64, 200),
		Count:               make([]uint32, 200),
		Sum:                 make([]float64, 200),
		Max:                 make([]float64, 200),
		Min:                 make([]float64, 200),
		First:               make([]float64, 200),
	}
	// Make the offsets increasing so we don't have to resort the columns (which
	// is the common case).
	for i := range d1.Offset {
		d1.Offset[i] = int32(i)
	}
	for i := range d2.Offset {
		d2.Offset[i] = int32(len(d1.Offset) + i)
	}
	vals, err := serializeMergeInputs(d1, d2)
	require.NoError(b, err)
	v1, v2 := vals[0], vals[1]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm, err := MVCCMerger.Merge(nil, v1)
		if err != nil {
			b.Fatal(err)
		}
		if err := vm.MergeNewer(v2); err != nil {
			b.Fatal(err)
		}
		_, closer, err := vm.Finish(true /* includesBase */)
		if err != nil {
			b.Fatal(err)
		}
		if closer != nil {
			_ = closer.Close()
		}
	}
}
