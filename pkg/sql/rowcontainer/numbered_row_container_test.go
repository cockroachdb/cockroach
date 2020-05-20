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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// Tests the de-duping functionality of DiskBackedNumberedRowContainer.
func TestNumberedRowContainerDeDuping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	numRows := 20
	const numCols = 2
	const smallMemoryBudget = 40
	rng, _ := randutil.NewPseudoRand()

	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)

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
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		true /*deDup*/, types, &evalCtx, tempEngine, &memoryMonitor, diskMonitor,
		0 /*rowCapacity*/)
	defer rc.Close(ctx)

	// Each pass does an UnsafeReset at the end.
	for passWithReset := 0; passWithReset < 2; passWithReset++ {
		// Insert rows.
		for insertPass := 0; insertPass < 2; insertPass++ {
			for i := 0; i < numRows; i++ {
				idx, err := rc.AddRow(ctx, rows[i])
				require.NoError(t, err)
				require.Equal(t, i, idx)
			}
		}
		// Random access of the inserted rows.
		var accesses []int
		for i := 0; i < 2*numRows; i++ {
			accesses = append(accesses, rng.Intn(numRows))
		}
		rc.SetupForRead(ctx, [][]int{accesses})
		for i := 0; i < len(accesses); i++ {
			skip := rng.Intn(10) == 0
			row, err := rc.GetRow(ctx, accesses[i], skip)
			require.NoError(t, err)
			if skip {
				continue
			}
			require.Equal(t, rows[accesses[i]].String(types), row.String(types))
		}
		// Reset and reorder the rows for the next pass.
		rand.Shuffle(numRows, func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		require.NoError(t, rc.UnsafeReset(ctx))
	}
}

// Tests the iterator and iterator caching of DiskBackedNumberedRowContainer.
// Does not utilize the de-duping functionality since that is tested
// elsewhere.
func TestNumberedRowContainerIteratorCaching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)

	numRows := 200
	const numCols = 2
	// This memory budget allows for some caching, but typically cannot
	// cache all the rows.
	const memoryBudget = 12000

	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(memoryBudget))
	defer memoryMonitor.Stop(ctx)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	// Use random types and random rows.
	rng, _ := randutil.NewPseudoRand()

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
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		false /*deDup*/, types, &evalCtx, tempEngine, &memoryMonitor, diskMonitor,
		0 /*rowCapacity*/)
	defer rc.Close(ctx)

	// Each pass does an UnsafeReset at the end.
	for passWithReset := 0; passWithReset < 2; passWithReset++ {
		// Insert rows.
		for i := 0; i < numRows; i++ {
			idx, err := rc.AddRow(ctx, rows[i])
			require.NoError(t, err)
			require.Equal(t, i, idx)
		}
		// We want all the memory to be usable by the cache, so spill to disk.
		require.NoError(t, rc.testingSpillToDisk(ctx))
		require.True(t, rc.UsingDisk())
		// Random access of the inserted rows.
		var accesses [][]int
		for i := 0; i < 2*numRows; i++ {
			var access []int
			for j := 0; j < 4; j++ {
				access = append(access, rng.Intn(numRows))
			}
			accesses = append(accesses, access)
		}
		rc.SetupForRead(ctx, accesses)
		for _, access := range accesses {
			for _, index := range access {
				skip := rng.Intn(10) == 0
				row, err := rc.GetRow(ctx, index, skip)
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
		rand.Shuffle(numRows, func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		require.NoError(t, rc.UnsafeReset(ctx))
	}
}

// Adapter interface that can be implemented using both DiskBackedNumberedRowContainer
// and DiskBackedIndexedRowContainer.
type numberedContainer interface {
	addRow(context.Context, sqlbase.EncDatumRow) error
	setupForRead(ctx context.Context, accesses [][]int)
	getRow(ctx context.Context, idx int) (sqlbase.EncDatumRow, error)
	close(context.Context)
}

type numberedContainerUsingNRC struct {
	rc            *DiskBackedNumberedRowContainer
	memoryMonitor *mon.BytesMonitor
}

func (d numberedContainerUsingNRC) addRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	_, err := d.rc.AddRow(ctx, row)
	return err
}
func (d numberedContainerUsingNRC) setupForRead(ctx context.Context, accesses [][]int) {
	d.rc.SetupForRead(ctx, accesses)
}
func (d numberedContainerUsingNRC) getRow(
	ctx context.Context, idx int,
) (sqlbase.EncDatumRow, error) {
	return d.rc.GetRow(ctx, idx, false)
}
func (d numberedContainerUsingNRC) close(ctx context.Context) {
	d.rc.Close(ctx)
	d.memoryMonitor.Stop(ctx)
}
func makeNumberedContainerUsingNRC(
	b *testing.B,
	ctx context.Context,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	st *cluster.Settings,
	memoryBudget int64,
	diskMonitor *mon.BytesMonitor,
) numberedContainerUsingNRC {
	memoryMonitor := makeMemMonitorAndStart(ctx, st, memoryBudget)
	rc := NewDiskBackedNumberedRowContainer(
		false, types, evalCtx, engine, memoryMonitor, diskMonitor, 0 /* rowCapacity */)
	require.NoError(b, rc.testingSpillToDisk(ctx))
	return numberedContainerUsingNRC{rc: rc, memoryMonitor: memoryMonitor}
}

type numberedContainerUsingIRC struct {
	rc            *DiskBackedIndexedRowContainer
	memoryMonitor *mon.BytesMonitor
}

func (d numberedContainerUsingIRC) addRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	return d.rc.AddRow(ctx, row)
}
func (d numberedContainerUsingIRC) setupForRead(context.Context, [][]int) {}
func (d numberedContainerUsingIRC) getRow(
	ctx context.Context, idx int,
) (sqlbase.EncDatumRow, error) {
	row, err := d.rc.GetRow(ctx, idx)
	if err != nil {
		return nil, err
	}
	return row.(IndexedRow).Row, nil
}
func (d numberedContainerUsingIRC) close(ctx context.Context) {
	d.rc.Close(ctx)
	d.memoryMonitor.Stop(ctx)
}
func makeNumberedContainerUsingIRC(
	b *testing.B,
	ctx context.Context,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	st *cluster.Settings,
	memoryBudget int64,
	diskMonitor *mon.BytesMonitor,
) numberedContainerUsingIRC {
	memoryMonitor := makeMemMonitorAndStart(ctx, st, memoryBudget)
	rc := NewDiskBackedIndexedRowContainer(
		nil, types, evalCtx, engine, memoryMonitor, diskMonitor, 0 /* rowCapacity */)
	require.NoError(b, rc.SpillToDisk(ctx))
	return numberedContainerUsingIRC{rc: rc, memoryMonitor: memoryMonitor}
}

func makeMemMonitorAndStart(
	ctx context.Context, st *cluster.Settings, budget int64,
) *mon.BytesMonitor {
	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(budget))
	return &memoryMonitor
}

// numRightRows is the number of rows in the container, each of which is on
// average accessed repeatCount times.
func generateLookupJoinAccessPattern(rng *rand.Rand, numRightRows int, repeatCount int) [][]int {
	// Assume that join is using a batch of 100 left rows.
	const leftRowsBatch = 100
	accessesPerRow := (numRightRows * repeatCount) / leftRowsBatch
	out := make([][]int, leftRowsBatch)
	for i := 0; i < len(out); i++ {
		// Each left row sees a contiguous sequence of rows on the right assuming
		// rows are being retrieved and stored in the container in index order.
		// TODO(sumeer): confirm that the right rows will be ordered by the lookup
		// columns.
		start := rng.Intn(numRightRows - accessesPerRow)
		out[i] = make([]int, accessesPerRow)
		for j := start; j < start+accessesPerRow; j++ {
			out[i][j-start] = j
		}
	}
	return out
}

// numRightRows is the number of rows in the container, of which a certain
// fraction of rows are accessed (when using an inverted index for
// intersection the result set can be sparse). accessesPerLeftRow is the
// number of right rows accessed by each left row. Don't pass a
// fractionAccessed value equal to or close to 1.
func generateInvertedJoinAccessPattern(
	b *testing.B, rng *rand.Rand, numRightRows int, fractionAccessed float64, accessesPerLeftRow int,
) [][]int {
	// Construct the rows that will be accessed.
	accessedIndexes := make(map[int]struct{})
	numRightRowsAccessed := int(float64(numRightRows) * fractionAccessed)
	// Don't want each left row to access most of the right rows.
	require.True(b, accessesPerLeftRow < numRightRowsAccessed/2)
	for len(accessedIndexes) < numRightRowsAccessed {
		accessedIndexes[rng.Intn(numRightRows)] = struct{}{}
	}
	accessedRightRows := make([]int, 0, numRightRowsAccessed)
	for k, _ := range accessedIndexes {
		accessedRightRows = append(accessedRightRows, k)
	}
	// Assume that join is using a batch of 100 left rows.
	const leftRowsBatch = 100
	out := make([][]int, leftRowsBatch)
	for i := 0; i < len(out); i++ {
		out[i] = make([]int, 0, accessesPerLeftRow)
		uniqueRows := make(map[int]struct{})
		for len(uniqueRows) < accessesPerLeftRow {
			idx := rng.Intn(len(accessedRightRows))
			_, ok := uniqueRows[idx]
			if ok {
				continue
			} else {
				uniqueRows[idx] = struct{}{}
				out[i] = append(out[i], accessedRightRows[idx])
			}
		}
		// Sort since accesses by a left row are in ascending order.
		sort.Slice(out[i], func(a, b int) bool {
			return out[i][a] < out[i][b]
		})
	}
	return out
}

func accessPatternForBenchmarkIterations(totalAccesses int, accessPattern [][]int) [][]int {
	var out [][]int
	var i, j int
	for count := 0; count < totalAccesses; {
		if i >= len(accessPattern) {
			i = 0
			continue
		}
		if j >= len(accessPattern[i]) {
			j = 0
			i++
			continue
		}
		if j == 0 {
			out = append(out, []int(nil))
		}
		last := len(out) - 1
		out[last] = append(out[last], accessPattern[i][j])
		count++
		j++
	}
	return out
}

func BenchmarkNumberedContainerIteratorCaching(b *testing.B) {
	const numRows = 10000

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	// Each row is 10 string columns. Each string has a mean length of 5, and the
	// row encoded into bytes is ~64 bytes. So we approximate ~512 rows per ssblock.
	// The in-memory decoded footprint in the cache is ~780 bytes.
	var typs []*types.T
	for i := 0; i < 10; i++ {
		typs = append(typs, types.String)
	}
	rng, _ := randutil.NewPseudoRand()
	rows := make([]sqlbase.EncDatumRow, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = make([]sqlbase.EncDatum, len(typs))
		for j := range typs {
			rows[i][j] = sqlbase.DatumToEncDatum(typs[j], sqlbase.RandDatum(rng, typs[j], false))
		}
	}

	// The access pattern generation parameters are made up and need adjusting
	// to make them more representative.

	// repeatCount of 2 is a total of numRows * 2 accesses of right rows, so
	// 20000 accesses. With a left batch of 100 rows, each left row is accessing
	// 200 right rows. For each left row the starting point of the access is
	// random and then 200 consecutive rows are read.
	ljAccessPattern := generateLookupJoinAccessPattern(rng, numRows, 2)
	// 0.25 * 10000 = 2500 right rows are accessed. The remaining 7500 rows are
	// in the store but never accessed. With a left batch of 100 rows, and 100
	// accesses per left row, there are a total of 10000 accesses, so each
	// accessed is on average accessed 4 times.
	ijAccessPattern := generateInvertedJoinAccessPattern(
		b, rng, numRows, 0.25, 50)
	// Access pattern and observed cache behavior:
	// - The inverted join pattern has poor locality and the IndexedRowContainer
	//   does poorly. The NumberedRowContainer is able to use the knowledge that
	//   many rows will never be accessed.
	//                         11000   100KB   500KB   2.5MB
	//   IndexedRowContainer   0.00    0.00    0.00    0.00
	//   NumberedRowContainer  0.22    0.68    0.88    1.00
	// - The lookup join access pattern and observed hit rates. The better
	//   locality improves the behavior of the IndexedRowContainer, but it
	//   is still significantly worse than the NumberedRowContainer.
	//                         11000   100KB   500KB   2.5MB
	//   IndexedRowContainer   0.00    0.00    0.10    0.35
	//   NumberedRowContainer  0.01    0.09    0.28    0.63

	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	for _, pattern := range []string{"lookup-join", "inverted-join"} {
		// Approx cache capacity in rows with these settings: 13, 132, 666, 3300.
		for _, memoryBudget := range []int64{11000, 100 << 10, 500 << 10, 2500 << 10} {
			for _, containerKind := range []string{"indexed", "numbered"} {
				b.Run(fmt.Sprintf("%s/%d/%s", pattern, memoryBudget, containerKind), func(b *testing.B) {
					var nc numberedContainer
					switch containerKind {
					case "indexed":
						nc = makeNumberedContainerUsingIRC(
							b, ctx, typs, &evalCtx, tempEngine, st, memoryBudget, &diskMonitor)
					case "numbered":
						nc = makeNumberedContainerUsingNRC(
							b, ctx, typs, &evalCtx, tempEngine, st, memoryBudget, &diskMonitor)
					}
					defer nc.close(ctx)
					var accesses [][]int
					switch pattern {
					case "lookup-join":
						accesses = accessPatternForBenchmarkIterations(b.N, ljAccessPattern)
					case "inverted-join":
						accesses = accessPatternForBenchmarkIterations(b.N, ijAccessPattern)
					}
					for i := 0; i < len(rows); i++ {
						require.NoError(b, nc.addRow(ctx, rows[i]))
					}
					b.ResetTimer()
					nc.setupForRead(ctx, accesses)
					for i := 0; i < len(accesses); i++ {
						for j := 0; j < len(accesses[i]); j++ {
							if _, err := nc.getRow(ctx, accesses[i][j]); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					if false {
						// Print statements for understanding the performance differences.
						fmt.Printf("\n**%s/%d/%s: iters: %d\n", pattern, memoryBudget, containerKind, b.N)
						switch rc := nc.(type) {
						case numberedContainerUsingNRC:
							fmt.Printf("hit rate: %.2f, maxCacheSize: %d\n",
								float64(rc.rc.rowIter.hitCount)/float64(rc.rc.rowIter.missCount+rc.rc.rowIter.hitCount),
								rc.rc.rowIter.maxCacheSize)
						case numberedContainerUsingIRC:
							fmt.Printf("hit rate: %.2f, maxCacheSize: %d\n",
								float64(rc.rc.hitCount)/float64(rc.rc.missCount+rc.rc.hitCount),
								rc.rc.maxCacheSize)
						}
					}
				})
			}
		}
	}
}

// TODO(sumeer): Benchmarks:
// - de-duping with and without spilling.
