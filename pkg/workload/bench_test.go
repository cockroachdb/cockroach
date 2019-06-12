// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package workload_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

func columnByteSize(col coldata.Vec) int64 {
	switch col.Type() {
	case types.Int64:
		return int64(len(col.Int64()) * 8)
	case types.Float64:
		return int64(len(col.Float64()) * 8)
	case types.Bytes:
		var bytes int64
		for _, b := range col.Bytes() {
			bytes += int64(len(b))
		}
		return bytes
	default:
		panic(fmt.Sprintf(`unhandled type %s`, col.Type().GoTypeName()))
	}
}

var benchmarkSeedAtomic uint64 = 1

func benchmarkInitialData(b *testing.B, gen workload.Generator) {
	var bytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-seed the generator so we don't get to reuse things like
		// PrecomputedRand. This makes the benchmark results an upper bound on
		// performance/allocations. TPCC, for example, special cases the default
		// seed to do some initialization only once which means it will be faster
		// than this benchmark gives it credit for.
		if f, ok := gen.(workload.Flagser); ok {
			seedFlag := fmt.Sprintf(`--seed=%d`, atomic.AddUint64(&benchmarkSeedAtomic, 1))
			if err := f.Flags().Parse([]string{seedFlag}); err != nil {
				b.Fatalf(`could not reset seed: %v`, err)
			}
		}

		// Share the Batch and ByteAllocator across tables but not across benchmark
		// iterations.
		cb := coldata.NewMemBatch(nil)
		var a bufalloc.ByteAllocator
		for _, table := range gen.Tables() {
			for rowIdx := 0; rowIdx < table.InitialRows.NumBatches; rowIdx++ {
				a = a[:0]
				table.InitialRows.FillBatch(rowIdx, cb, &a)
				for _, col := range cb.ColVecs() {
					bytes += columnByteSize(col)
				}
			}
		}
	}
	b.StopTimer()
	b.SetBytes(bytes / int64(b.N))
}

func BenchmarkInitialData(b *testing.B) {
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkInitialData(b, tpcc.FromWarehouses(1))
	})
	b.Run(`bank/rows=1000`, func(b *testing.B) {
		benchmarkInitialData(b, bank.FromRows(1000))
	})
}
