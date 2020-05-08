// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestNumberedRowContainerDeDuping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	const numRows = 20
	const numCols = 2
	const smallMemoryBudget = 40
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	memoryBudget := math.MaxInt64
	if rng.Intn(2) == 0 {
		fmt.Printf("using smallMemoryBudget to spill to disk\n")
		memoryBudget = smallMemoryBudget
	}

	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(int64(memoryBudget)))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	// Use random types and random rows.
	types := sqlbase.RandSortingTypes(rng, numCols)
	ordering := sqlbase.ColumnOrdering{
		sqlbase.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		sqlbase.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		true /*deDup*/, types, &evalCtx, tempEngine, &memoryMonitor, &diskMonitor,
		0 /*rowCapacity*/)
	defer rc.Close(ctx)

	// Each pass does an UnsafeReset at the end.
	for passWithReset := 0; passWithReset < 2; passWithReset++ {
		// Insert rows.
		for insertPass := 0; insertPass < 2; insertPass++ {
			for i := 0; i < len(rows); i++ {
				idx, err := rc.AddRow(ctx, rows[i])
				require.NoError(t, err)
				require.Equal(t, i, int(idx))
			}
		}
		// Random access of the inserted rows.
		var accesses [][]int
		accesses = append(accesses, nil)
		for i := 0; i < 2*len(rows); i++ {
			accesses[0] = append(accesses[0], rng.Intn(len(rows)))
		}
		rc.SetupForRead(ctx, accesses)
		for i := 0; i < len(accesses[0]); i++ {
			skip := rng.Intn(10) == 0
			row, err := rc.GetRow(ctx, uint64(accesses[0][i]), skip)
			require.NoError(t, err)
			if skip {
				continue
			}
			require.Equal(t, rows[accesses[0][i]].String(types), row.String(types))
		}
		// Reset and reorder the rows for the next pass.
		rand.Shuffle(len(rows), func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		require.NoError(t, rc.UnsafeReset(ctx))
	}
}

func TestNumberedRowContainerIteratorCaching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numRows = 200
	const numCols = 2
	// This memory budget allows for some caching, but typically cannot
	// cache all the rows.
	const memoryBudget = 12000

	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(memoryBudget))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	// Use random types and random rows.
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	types := sqlbase.RandSortingTypes(rng, numCols)
	ordering := sqlbase.ColumnOrdering{
		sqlbase.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		sqlbase.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		false /*deDup*/, types, &evalCtx, tempEngine, &memoryMonitor, &diskMonitor,
		0 /*rowCapacity*/)
	defer rc.Close(ctx)

	// Each pass does an UnsafeReset at the end.
	for passWithReset := 0; passWithReset < 2; passWithReset++ {
		// Insert rows.
		for i := 0; i < len(rows); i++ {
			idx, err := rc.AddRow(ctx, rows[i])
			require.NoError(t, err)
			require.Equal(t, i, int(idx))
		}
		// We want all the memory to be usable by the cache, so spill to disk.
		rc.testingSpillToDisk(ctx)
		// Random access of the inserted rows.
		var accesses [][]int
		for i := 0; i < 2*len(rows); i++ {
			var access []int
			for j := 0; j < 4; j++ {
				access = append(access, rng.Intn(len(rows)))
			}
			accesses = append(accesses, access)
		}
		rc.SetupForRead(ctx, accesses)
		for _, access := range accesses {
			for _, index := range access {
				skip := rng.Intn(10) == 0
				row, err := rc.GetRow(ctx, uint64(index), skip)
				require.NoError(t, err)
				if skip {
					continue
				}
				require.Equal(t, rows[index].String(types), row.String(types))
			}
		}
		fmt.Printf("hits: %d, misses: %d, maxCacheSize: %d\n",
			rc.rowIter.hitCount, rc.rowIter.missCount, rc.rowIter.maxCacheSize)
		// Reset and reorder the rows for the next pass.
		rand.Shuffle(len(rows), func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		require.NoError(t, rc.UnsafeReset(ctx))
	}
}

// TODO(sumeer): Benchmarks:
// - de-duping with and without spilling.
// - read throughput with and without cache under various read access patterns.
