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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	numRows := 20
	const numCols = 2
	const smallMemoryBudget = 40
	rng, _ := randutil.NewPseudoRand()

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	memoryBudget := math.MaxInt64
	if rng.Intn(2) == 0 {
		fmt.Printf("using smallMemoryBudget to spill to disk\n")
		memoryBudget = smallMemoryBudget
	}
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(int64(memoryBudget)))
	defer memoryMonitor.Stop(ctx)

	// Use random types and random rows.
	types := randgen.RandSortingTypes(rng, numCols)
	ordering := colinfo.ColumnOrdering{
		colinfo.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		colinfo.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		true /*deDup*/, types, &evalCtx, tempEngine, memoryMonitor, diskMonitor,
	)
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
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	numRows := 200
	const numCols = 2
	// This memory budget allows for some caching, but typically cannot
	// cache all the rows.
	const memoryBudget = 12000
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(memoryBudget))
	defer memoryMonitor.Stop(ctx)

	// Use random types and random rows.
	rng, _ := randutil.NewPseudoRand()

	types := randgen.RandSortingTypes(rng, numCols)
	ordering := colinfo.ColumnOrdering{
		colinfo.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		colinfo.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)
	rc := NewDiskBackedNumberedRowContainer(
		false /*deDup*/, types, &evalCtx, tempEngine, memoryMonitor, diskMonitor,
	)
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

// Tests that the DiskBackedNumberedRowContainer and
// DiskBackedIndexedRowContainer return the same results.
func TestCompareNumberedAndIndexedRowContainers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	numRows := 200
	const numCols = 2
	// This memory budget allows for some caching, but typically cannot
	// cache all the rows.
	var memoryBudget int64 = 12000
	if rng.Intn(2) == 0 {
		memoryBudget = math.MaxInt64
	}

	// Use random types and random rows.
	types := randgen.RandSortingTypes(rng, numCols)
	ordering := colinfo.ColumnOrdering{
		colinfo.ColumnOrderInfo{
			ColIdx:    0,
			Direction: encoding.Ascending,
		},
		colinfo.ColumnOrderInfo{
			ColIdx:    1,
			Direction: encoding.Descending,
		},
	}
	numRows, rows := makeUniqueRows(t, &evalCtx, rng, numRows, types, ordering)

	var containers [2]numberedContainer
	containers[0] = makeNumberedContainerUsingIRC(
		ctx, t, types, &evalCtx, tempEngine, st, memoryBudget, diskMonitor)
	containers[1] = makeNumberedContainerUsingNRC(
		ctx, t, types, &evalCtx, tempEngine, st, memoryBudget, diskMonitor)
	defer func() {
		for _, rc := range containers {
			rc.close(ctx)
		}
	}()

	// Each pass does an UnsafeReset at the end.
	for passWithReset := 0; passWithReset < 2; passWithReset++ {
		// Insert rows.
		for i := 0; i < numRows; i++ {
			for _, rc := range containers {
				err := rc.addRow(ctx, rows[i])
				require.NoError(t, err)
			}
		}
		// We want all the memory to be usable by the cache, so spill to disk.
		if memoryBudget != math.MaxInt64 {
			for _, rc := range containers {
				require.NoError(t, rc.spillToDisk(ctx))
			}
		}

		// Random access of the inserted rows.
		var accesses [][]int
		for i := 0; i < 2*numRows; i++ {
			var access []int
			for j := 0; j < 4; j++ {
				access = append(access, rng.Intn(numRows))
			}
			accesses = append(accesses, access)
		}
		for _, rc := range containers {
			rc.setupForRead(ctx, accesses)
		}
		for _, access := range accesses {
			for _, index := range access {
				skip := rng.Intn(10) == 0
				var rows [2]rowenc.EncDatumRow
				for i, rc := range containers {
					row, err := rc.getRow(ctx, index, skip)
					require.NoError(t, err)
					rows[i] = row
				}
				if skip {
					continue
				}
				require.Equal(t, rows[0].String(types), rows[1].String(types))
			}
		}
		// Reset and reorder the rows for the next pass.
		rand.Shuffle(numRows, func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})
		for _, rc := range containers {
			require.NoError(t, rc.unsafeReset(ctx))
		}
	}
}

// Adapter interface that can be implemented using both DiskBackedNumberedRowContainer
// and DiskBackedIndexedRowContainer.
type numberedContainer interface {
	addRow(context.Context, rowenc.EncDatumRow) error
	setupForRead(ctx context.Context, accesses [][]int)
	getRow(ctx context.Context, idx int, skip bool) (rowenc.EncDatumRow, error)
	spillToDisk(context.Context) error
	unsafeReset(context.Context) error
	close(context.Context)
}

type numberedContainerUsingNRC struct {
	rc            *DiskBackedNumberedRowContainer
	memoryMonitor *mon.BytesMonitor
}

func (d numberedContainerUsingNRC) addRow(ctx context.Context, row rowenc.EncDatumRow) error {
	_, err := d.rc.AddRow(ctx, row)
	return err
}
func (d numberedContainerUsingNRC) setupForRead(ctx context.Context, accesses [][]int) {
	d.rc.SetupForRead(ctx, accesses)
}
func (d numberedContainerUsingNRC) getRow(
	ctx context.Context, idx int, skip bool,
) (rowenc.EncDatumRow, error) {
	return d.rc.GetRow(ctx, idx, false)
}
func (d numberedContainerUsingNRC) spillToDisk(ctx context.Context) error {
	return d.rc.testingSpillToDisk(ctx)
}
func (d numberedContainerUsingNRC) unsafeReset(ctx context.Context) error {
	return d.rc.UnsafeReset(ctx)
}
func (d numberedContainerUsingNRC) close(ctx context.Context) {
	d.rc.Close(ctx)
	d.memoryMonitor.Stop(ctx)
}
func makeNumberedContainerUsingNRC(
	ctx context.Context,
	t testing.TB,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	st *cluster.Settings,
	memoryBudget int64,
	diskMonitor *mon.BytesMonitor,
) numberedContainerUsingNRC {
	memoryMonitor := makeMemMonitorAndStart(ctx, st, memoryBudget)
	rc := NewDiskBackedNumberedRowContainer(
		false /* deDup */, types, evalCtx, engine, memoryMonitor, diskMonitor)
	require.NoError(t, rc.testingSpillToDisk(ctx))
	return numberedContainerUsingNRC{rc: rc, memoryMonitor: memoryMonitor}
}

type numberedContainerUsingIRC struct {
	rc            *DiskBackedIndexedRowContainer
	memoryMonitor *mon.BytesMonitor
}

func (d numberedContainerUsingIRC) addRow(ctx context.Context, row rowenc.EncDatumRow) error {
	return d.rc.AddRow(ctx, row)
}
func (d numberedContainerUsingIRC) setupForRead(context.Context, [][]int) {}
func (d numberedContainerUsingIRC) getRow(
	ctx context.Context, idx int, skip bool,
) (rowenc.EncDatumRow, error) {
	if skip {
		return nil, nil
	}
	row, err := d.rc.GetRow(ctx, idx)
	if err != nil {
		return nil, err
	}
	return row.(IndexedRow).Row, nil
}
func (d numberedContainerUsingIRC) spillToDisk(ctx context.Context) error {
	if d.rc.UsingDisk() {
		return nil
	}
	return d.rc.SpillToDisk(ctx)
}
func (d numberedContainerUsingIRC) unsafeReset(ctx context.Context) error {
	return d.rc.UnsafeReset(ctx)
}
func (d numberedContainerUsingIRC) close(ctx context.Context) {
	d.rc.Close(ctx)
	d.memoryMonitor.Stop(ctx)
}
func makeNumberedContainerUsingIRC(
	ctx context.Context,
	t require.TestingT,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	st *cluster.Settings,
	memoryBudget int64,
	diskMonitor *mon.BytesMonitor,
) numberedContainerUsingIRC {
	memoryMonitor := makeMemMonitorAndStart(ctx, st, memoryBudget)
	rc := NewDiskBackedIndexedRowContainer(
		nil /* ordering */, types, evalCtx, engine, memoryMonitor, diskMonitor)
	require.NoError(t, rc.SpillToDisk(ctx))
	return numberedContainerUsingIRC{rc: rc, memoryMonitor: memoryMonitor}
}

func makeMemMonitorAndStart(
	ctx context.Context, st *cluster.Settings, budget int64,
) *mon.BytesMonitor {
	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(budget))
	return memoryMonitor
}

// Assume that join is using a batch of 100 left rows.
const leftRowsBatch = 100

// repeatAccesses is the number of times on average that each right row is accessed.
func generateLookupJoinAccessPattern(
	rng *rand.Rand, rightRowsReadPerLeftRow int, repeatAccesses int,
) [][]int {
	// Unique rows accessed.
	numRowsAccessed := (leftRowsBatch * rightRowsReadPerLeftRow) / repeatAccesses
	out := make([][]int, leftRowsBatch)
	for i := 0; i < len(out); i++ {
		// Each left row sees a contiguous sequence of rows on the right since the
		// rows are being retrieved and stored in the container in index order.
		start := rng.Intn(numRowsAccessed - rightRowsReadPerLeftRow)
		out[i] = make([]int, rightRowsReadPerLeftRow)
		for j := start; j < start+rightRowsReadPerLeftRow; j++ {
			out[i][j-start] = j
		}
	}
	return out
}

// numRightRows is the number of rows in the container, of which a certain
// fraction of rows are accessed randomly (when using an inverted index for
// intersection the result set can be sparse).
// repeatAccesses is the number of times on average that each right row is accessed.
func generateInvertedJoinAccessPattern(
	b *testing.B, rng *rand.Rand, numRightRows int, rightRowsReadPerLeftRow int, repeatAccesses int,
) [][]int {
	// Unique rows accessed.
	numRowsAccessed := (leftRowsBatch * rightRowsReadPerLeftRow) / repeatAccesses
	// Don't want each left row to access most of the right rows.
	require.True(b, rightRowsReadPerLeftRow < numRowsAccessed/2)
	accessedIndexes := make(map[int]struct{})
	for len(accessedIndexes) < numRowsAccessed {
		accessedIndexes[rng.Intn(numRightRows)] = struct{}{}
	}
	accessedRightRows := make([]int, 0, numRowsAccessed)
	for k := range accessedIndexes {
		accessedRightRows = append(accessedRightRows, k)
	}
	out := make([][]int, leftRowsBatch)
	for i := 0; i < len(out); i++ {
		out[i] = make([]int, 0, rightRowsReadPerLeftRow)
		uniqueRows := make(map[int]struct{})
		for len(uniqueRows) < rightRowsReadPerLeftRow {
			idx := rng.Intn(len(accessedRightRows))
			if _, notUnique := uniqueRows[idx]; notUnique {
				continue
			}
			uniqueRows[idx] = struct{}{}
			out[i] = append(out[i], accessedRightRows[idx])
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
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{InMemory: true}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	// Each row is 10 string columns. Each string has a mean length of 5, and the
	// row encoded into bytes is ~64 bytes. So we approximate ~512 rows per ssblock.
	// The in-memory decoded footprint in the cache is ~780 bytes.
	var typs []*types.T
	for i := 0; i < 10; i++ {
		typs = append(typs, types.String)
	}
	rng, _ := randutil.NewPseudoRand()
	rows := make([]rowenc.EncDatumRow, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = make([]rowenc.EncDatum, len(typs))
		for j := range typs {
			rows[i][j] = rowenc.DatumToEncDatum(typs[j], randgen.RandDatum(rng, typs[j], false))
		}
	}

	type accessPattern struct {
		joinType string
		paramStr string
		accesses [][]int
	}
	var accessPatterns []accessPattern
	// Lookup join access patterns. The highest number of unique rows accessed is
	// when rightRowsReadPerLeftRow = 64 and repeatAccesses = 1, which with a left
	// batch of 100 is 100 * 64 / 1 = 6400 rows accessed. The container has
	// 10000 rows. If N unique rows are accessed these form a prefix of the rows
	// in the container.
	for _, rightRowsReadPerLeftRow := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, repeatAccesses := range []int{1, 2} {
			accessPatterns = append(accessPatterns, accessPattern{
				joinType: "lookup-join",
				paramStr: fmt.Sprintf("matchRatio=%d/repeatAccesses=%d",
					rightRowsReadPerLeftRow, repeatAccesses),
				accesses: generateLookupJoinAccessPattern(rng, rightRowsReadPerLeftRow, repeatAccesses),
			})
		}
	}
	// Inverted join access patterns.
	// With a left batch of 100 rows, and rightRowsReadPerLeftRow = (25, 50, 100), the
	// total accesses are (2500, 5000, 10000). Consider repeatAccesses = 2: the unique
	// rows accessed are (1250, 2500, 5000), which will be randomly distributed over the
	// 10000 rows.
	for _, rightRowsReadPerLeftRow := range []int{1, 25, 50, 100} {
		for _, repeatAccesses := range []int{1, 2, 4, 8} {
			accessPatterns = append(accessPatterns, accessPattern{
				joinType: "inverted-join",
				paramStr: fmt.Sprintf("matchRatio=%d/repeatAccesses=%d",
					rightRowsReadPerLeftRow, repeatAccesses),
				accesses: generateInvertedJoinAccessPattern(
					b, rng, numRows, rightRowsReadPerLeftRow, repeatAccesses),
			})
		}
	}

	// Observed cache behavior for a particular access pattern for each kind of
	// join, to give some insight into performance.
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

	for _, pattern := range accessPatterns {
		// Approx cache capacity in rows with these settings: 13, 132, 666, 3300.
		for _, memoryBudget := range []int64{11000, 100 << 10, 500 << 10, 2500 << 10} {
			for _, containerKind := range []string{"indexed", "numbered"} {
				b.Run(fmt.Sprintf("%s/%s/mem=%d/%s", pattern.joinType, pattern.paramStr, memoryBudget,
					containerKind), func(b *testing.B) {
					var nc numberedContainer
					switch containerKind {
					case "indexed":
						nc = makeNumberedContainerUsingIRC(
							ctx, b, typs, &evalCtx, tempEngine, st, memoryBudget, diskMonitor)
					case "numbered":
						nc = makeNumberedContainerUsingNRC(
							ctx, b, typs, &evalCtx, tempEngine, st, memoryBudget, diskMonitor)
					}
					defer nc.close(ctx)
					for i := 0; i < len(rows); i++ {
						require.NoError(b, nc.addRow(ctx, rows[i]))
					}
					accesses := accessPatternForBenchmarkIterations(b.N, pattern.accesses)
					b.ResetTimer()
					nc.setupForRead(ctx, accesses)
					for i := 0; i < len(accesses); i++ {
						for j := 0; j < len(accesses[i]); j++ {
							if _, err := nc.getRow(ctx, accesses[i][j], false /* skip */); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					// Disabled code block. Change to true to look at hit ratio and cache sizes
					// for these benchmarks.
					if false {
						// Print statements for understanding the performance differences.
						fmt.Printf("\n**%s/%s/%d/%s: iters: %d\n", pattern.joinType, pattern.paramStr, memoryBudget, containerKind, b.N)
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

// TODO(sumeer):
// - Benchmarks:
//   - de-duping with and without spilling.
