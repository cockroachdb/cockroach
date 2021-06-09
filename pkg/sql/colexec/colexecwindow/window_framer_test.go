// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const probabilityOfNewNumber = 0.5
const orderColIdx = 0
const peersColIdx = 1

func TestWindowFramer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewPseudoRand()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(evalCtx.Ctx())

	spillingQueueUnlimitedAllocator := colmem.NewAllocator(evalCtx.Ctx(), &memAcc, testColumnFactory)
	queueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
	queueCfg.SetDefaultBufferSizeBytesForCacheMode()

	partition := colexecutils.NewSpillingBuffer(
		spillingQueueUnlimitedAllocator, math.MaxInt64, queueCfg,
		colexecop.NewTestingSemaphore(2), []*types.T{types.Int, types.Bool}, testDiskAcc,
	)

	testWindowFramer := func(count int, ordered, asc bool,
		mode tree.WindowFrameMode, startBound, endBound tree.WindowFrameBoundType,
	) {
		colWindowFramer, rowWindowFramer := initWindowFramers(
			t, rng, evalCtx, partition, count, ordered, asc, mode, startBound, endBound,
		)

		for i := 0; i < count; i++ {
			// Advance the columnar framer.
			colWindowFramer.next(evalCtx.Ctx())

			// Advance the row-wise framer.
			if ordered {
				vec, vecIdx, _ := partition.GetVecWithTuple(evalCtx.Ctx(), peersColIdx, i)
				if i > 0 && vec.Bool()[vecIdx] {
					rowWindowFramer.CurRowPeerGroupNum++
					require.NoError(t, rowWindowFramer.PeerHelper.Update(rowWindowFramer))
				}
			}
			rowWindowFramer.RowIdx = i
			rowStartIdx, err := rowWindowFramer.FrameStartIdx(evalCtx.Ctx(), evalCtx)
			require.NoError(t, err)
			rowEndIdx, err := rowWindowFramer.FrameEndIdx(evalCtx.Ctx(), evalCtx)
			require.NoError(t, err)

			// Validate that the columnar window framer describes the same window
			// frame as the row-wise one.
			var colFrameIdx int
			frameIsEmpty := true
			for j := rowStartIdx; j < rowEndIdx; j++ {
				skipped, err := rowWindowFramer.IsRowSkipped(evalCtx.Ctx(), j)
				require.NoError(t, err)
				if skipped {
					continue
				}
				colFrameIdx++
				frameIsEmpty = false
				require.Equal(t, j, colWindowFramer.frameNthIdx(colFrameIdx))
			}
			if frameIsEmpty {
				// The columnar window framer functions return -1 to signify an empty
				// window frame.
				require.Equal(t, -1, colWindowFramer.frameFirstIdx())
				require.Equal(t, -1, colWindowFramer.frameLastIdx())
				continue
			}
			require.Equal(t, rowStartIdx, colWindowFramer.frameFirstIdx())
			require.Equal(t, rowEndIdx, colWindowFramer.frameLastIdx()+1)
		}
	}

	for _, mode := range []tree.WindowFrameMode{tree.ROWS, tree.GROUPS} {
		t.Run(fmt.Sprintf("mode=%v", mode.Name()), func(t *testing.T) {
			for _, bound := range []tree.WindowFrameBoundType{
				tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow,
				tree.OffsetFollowing, tree.UnboundedFollowing,
			} {
				for _, bounds := range [][2]tree.WindowFrameBoundType{
					{bound, getEndBound(rng, bound)},
					{getStartBound(rng, bound), bound},
				} {
					start, end := bounds[0], bounds[1]
					if !validForStart(start) || !validForEnd(end) {
						// Skip syntactically invalid bounds.
						continue
					}
					t.Run(fmt.Sprintf("start=%v/end=%v", start.Name(), end.Name()), func(t *testing.T) {
						for _, count := range []int{1, 17, 42, 91} {
							t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
								t.Run(fmt.Sprintf("ordered"), func(t *testing.T) {
									for _, asc := range []bool{true, false} {
										makeIntSortedPartition(evalCtx.Ctx(), testAllocator, count, asc, partition)
										t.Run(fmt.Sprintf("asc=%v", asc), func(t *testing.T) {
											testWindowFramer(count, true /* ordered */, asc, mode, start, end)
										})
									}
								})
								if mode == tree.ROWS {
									// An ORDER BY clause is required for GROUPS and RANGE modes.
									makeIntSortedPartition(evalCtx.Ctx(), testAllocator, count, false /* asc */, partition)
									t.Run(fmt.Sprintf("unordered"), func(t *testing.T) {
										testWindowFramer(count, false /* ordered */, false /* asc */, mode, start, end)
									})
								}
							})
						}
					})
				}
			}
		})
	}
}

func validForStart(bound tree.WindowFrameBoundType) bool {
	return bound != tree.UnboundedFollowing
}

func validForEnd(bound tree.WindowFrameBoundType) bool {
	return bound != tree.UnboundedPreceding
}

func getStartBound(rng *rand.Rand, endBound tree.WindowFrameBoundType) tree.WindowFrameBoundType {
	startBoundTypes := []tree.WindowFrameBoundType{
		tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing,
	}
	for {
		startBound := startBoundTypes[rng.Intn(len(startBoundTypes))]
		if startBound <= endBound {
			return startBound
		}
	}
}

func getEndBound(rng *rand.Rand, startBound tree.WindowFrameBoundType) tree.WindowFrameBoundType {
	endBoundTypes := []tree.WindowFrameBoundType{
		tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing, tree.UnboundedFollowing,
	}
	for {
		endBound := endBoundTypes[rng.Intn(len(endBoundTypes))]
		if endBound >= startBound {
			return endBound
		}
	}
}

func genRandomOffset(rng *rand.Rand, count int) uint64 {
	return rng.Uint64() % uint64(count+1)
}

// makeIntSortedPartition fills out the given buffer with sorted int values
// (representing an ordering column for a window framer), as well as a boolean
// peer groups column.
func makeIntSortedPartition(
	ctx context.Context,
	allocator *colmem.Allocator,
	count int,
	asc bool,
	partition *colexecutils.SpillingBuffer,
) {
	const minStart, maxStart = -100, 100
	partition.Reset(ctx)
	insertBatch := allocator.NewMemBatchWithFixedCapacity(
		[]*types.T{types.Int, types.Bool},
		1, /* capacity */
	)
	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	number := int64(r.Intn(maxStart-minStart) + minStart)
	for idx := 0; idx < count; idx++ {
		insertBatch.ColVec(peersColIdx).Bool()[0] = idx == 0
		if r.Float64() < probabilityOfNewNumber {
			if asc {
				number += int64(r.Intn(10)) + 1 // Ensure that the number changes.
			} else {
				number -= int64(r.Intn(10)) + 1
			}
			insertBatch.ColVec(peersColIdx).Bool()[0] = true
		}
		insertBatch.ColVec(orderColIdx).Int64()[0] = number
		insertBatch.SetLength(1)
		partition.AppendTuples(ctx, insertBatch, 0 /* startIdx */, insertBatch.Length())
	}
}

func initWindowFramers(
	t *testing.T,
	rng *rand.Rand,
	evalCtx *tree.EvalContext,
	partition *colexecutils.SpillingBuffer,
	count int,
	ordered, asc bool,
	mode tree.WindowFrameMode,
	startBound, endBound tree.WindowFrameBoundType,
) (windowFramer, *tree.WindowFrameRun) {
	startOffset, endOffset := genRandomOffset(rng, count), genRandomOffset(rng, count)

	frame := &execinfrapb.WindowerSpec_Frame{
		Mode: modeToExecinfrapb(mode),
		Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
			Start: execinfrapb.WindowerSpec_Frame_Bound{
				BoundType: boundToExecinfrapb(startBound),
				IntOffset: startOffset,
			},
			End: &execinfrapb.WindowerSpec_Frame_Bound{
				BoundType: boundToExecinfrapb(endBound),
				IntOffset: endOffset,
			},
		},
	}
	peersCol := tree.NoColumnIdx
	orderCol := tree.NoColumnIdx
	if ordered {
		peersCol = peersColIdx
		orderCol = orderColIdx
	}
	colWindowFramer := newWindowFramer(frame, peersCol)
	colWindowFramer.startPartition(evalCtx.Ctx(), partition.Length(), partition)

	rowDir := encoding.Ascending
	if !asc {
		rowDir = encoding.Descending
	}
	rowWindowFramer := &tree.WindowFrameRun{
		Rows:         &indexedRows{partition: partition},
		ArgsIdxs:     []uint32{0},
		FilterColIdx: tree.NoColumnIdx,
		Frame: &tree.WindowFrame{
			Mode: mode,
			Bounds: tree.WindowFrameBounds{
				StartBound: &tree.WindowFrameBound{
					BoundType:  startBound,
					OffsetExpr: tree.NewDInt(tree.DInt(startOffset)),
				},
				EndBound: &tree.WindowFrameBound{
					BoundType:  endBound,
					OffsetExpr: tree.NewDInt(tree.DInt(endOffset)),
				},
			},
		},
		StartBoundOffset: tree.NewDInt(tree.DInt(startOffset)),
		EndBoundOffset:   tree.NewDInt(tree.DInt(endOffset)),
		OrdColIdx:        orderCol,
		OrdDirection:     rowDir,
	}
	rowWindowFramer.PlusOp, rowWindowFramer.MinusOp, _ = tree.WindowFrameRangeOps{}.LookupImpl(types.Int, types.Int)
	require.NoError(t, rowWindowFramer.PeerHelper.Init(rowWindowFramer, &peerGroupChecker{partition: partition}))

	return colWindowFramer, rowWindowFramer
}

type indexedRows struct {
	partition *colexecutils.SpillingBuffer
}

var _ tree.IndexedRows = &indexedRows{}

func (ir indexedRows) Len() int {
	return ir.partition.Length()
}

func (ir indexedRows) GetRow(ctx context.Context, idx int) (tree.IndexedRow, error) {
	orderCol, vecIdx, _ := ir.partition.GetVecWithTuple(ctx, orderColIdx, idx)
	row := make(tree.Datums, 1)
	if orderCol.Nulls().NullAt(vecIdx) {
		row[orderColIdx] = tree.DNull
	} else {
		val := orderCol.Int64()[vecIdx]
		row[orderColIdx] = tree.Datum(tree.NewDInt(tree.DInt(val)))
	}
	return &indexedRow{row: row, idx: idx}, nil
}

type indexedRow struct {
	idx int
	row tree.Datums
}

var _ tree.IndexedRow = &indexedRow{}

func (ir indexedRow) GetIdx() int {
	return ir.idx
}

func (ir indexedRow) GetDatum(colIdx int) (tree.Datum, error) {
	return ir.row[colIdx], nil
}

func (ir indexedRow) GetDatums(firstColIdx, lastColIdx int) (tree.Datums, error) {
	return ir.row[firstColIdx:lastColIdx], nil
}

type peerGroupChecker struct {
	evalCtx   tree.EvalContext
	partition *colexecutils.SpillingBuffer
}

var _ tree.PeerGroupChecker = &peerGroupChecker{}

func (c *peerGroupChecker) InSameGroup(i, j int) (bool, error) {
	if i == j {
		return true, nil
	}
	if j < i {
		i, j = j, i
	}
	for idx := i + 1; idx <= j; idx++ {
		vec, vecIdx, _ := c.partition.GetVecWithTuple(c.evalCtx.Ctx(), peersColIdx, idx)
		if vec.Bool()[vecIdx] {
			// A new peer group has started, so the two given indices are not in the
			// same group.
			return false, nil
		}
	}
	return true, nil
}

func modeToExecinfrapb(mode tree.WindowFrameMode) execinfrapb.WindowerSpec_Frame_Mode {
	switch mode {
	case tree.RANGE:
		return execinfrapb.WindowerSpec_Frame_RANGE
	case tree.ROWS:
		return execinfrapb.WindowerSpec_Frame_ROWS
	case tree.GROUPS:
		return execinfrapb.WindowerSpec_Frame_GROUPS
	}
	return 0
}

func boundToExecinfrapb(bound tree.WindowFrameBoundType) execinfrapb.WindowerSpec_Frame_BoundType {
	switch bound {
	case tree.UnboundedPreceding:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.OffsetPreceding:
		return execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING
	case tree.CurrentRow:
		return execinfrapb.WindowerSpec_Frame_CURRENT_ROW
	case tree.OffsetFollowing:
		return execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	case tree.UnboundedFollowing:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	}
	return 0
}
