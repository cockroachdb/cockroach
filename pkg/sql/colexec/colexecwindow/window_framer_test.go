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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
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
	"github.com/cockroachdb/errors"
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

	testWindowFramer := func(
		count int, asc bool, mode tree.WindowFrameMode, startBound, endBound tree.WindowFrameBoundType,
	) {
		errorMsg := fmt.Sprintf("Count: %d\nMode: %s\nStart Bound: %s\nEnd Bound: %s",
			count, mode, startBound, endBound)

		colWindowFramer, rowWindowFramer := initWindowFramers(
			t, rng, evalCtx, partition, count, asc, mode, startBound, endBound,
		)

		for i := 0; i < count; i++ {
			// Advance the columnar framer.
			colWindowFramer.next(evalCtx.Ctx())

			// Advance the row-wise framer.
			batch, batchIdx := partition.GetBatchWithTuple(evalCtx.Ctx(), i)
			if i > 0 && batch.ColVec(peersColIdx).Bool()[batchIdx] {
				rowWindowFramer.CurRowPeerGroupNum++
				err := rowWindowFramer.PeerHelper.Update(rowWindowFramer)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
			rowWindowFramer.RowIdx = i
			rowStartIdx, err := rowWindowFramer.FrameStartIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			rowEndIdx, err := rowWindowFramer.FrameEndIdx(evalCtx.Ctx(), evalCtx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Validate that the columnar window framer describes the same window
			// frame as the row-wise one.
			var colFrameIdx int
			frameIsEmpty := true
			for j := rowStartIdx; j < rowEndIdx; j++ {
				skipped, err := rowWindowFramer.IsRowSkipped(evalCtx.Ctx(), j)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
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
				require.Equal(t, -1, colWindowFramer.frameFirstIdx(), errorMsg)
				require.Equal(t, -1, colWindowFramer.frameLastIdx(), errorMsg)
				continue
			}
			require.Equal(t, rowStartIdx, colWindowFramer.frameFirstIdx(), errorMsg)
			require.Equal(t, rowEndIdx, colWindowFramer.frameLastIdx()+1, errorMsg)
		}
	}

	// TODO(drewk): remember to add RANGE here once it is supported.
	for _, count := range []int{1, 17, 42, 91} {
		for _, asc := range []bool{true, false} {
			makeIntSortedPartition(evalCtx.Ctx(), count, asc, partition)
			for _, mode := range []tree.WindowFrameMode{tree.ROWS, tree.GROUPS} {
				for _, bound := range []tree.WindowFrameBoundType{
					tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow,
					tree.OffsetFollowing, tree.UnboundedFollowing,
				} {
					if validForStart(bound) {
						testWindowFramer(count, asc, mode, bound, getEndBound(rng, bound))
					}
					if validForEnd(bound) {
						testWindowFramer(count, asc, mode, getStartBound(rng, bound), bound)
					}
				}
			}
		}
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
	ctx context.Context, count int, asc bool, partition *colexecutils.SpillingBuffer,
) {
	const minStart, maxStart = -100, 100
	partition.Reset(ctx)
	insertBatch := coldata.NewMemBatchWithCapacity(
		[]*types.T{types.Int, types.Bool},
		1, /* capacity */
		coldata.StandardColumnFactory,
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
	asc bool,
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
	colWindowFramer := newWindowFramer(frame, peersColIdx)
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
		OrdColIdx:        orderColIdx,
		OrdDirection:     rowDir,
	}
	rowWindowFramer.PlusOp, rowWindowFramer.MinusOp, _ = tree.WindowFrameRangeOps{}.LookupImpl(types.Int, types.Int)
	err := rowWindowFramer.PeerHelper.Init(rowWindowFramer, &peerGroupChecker{partition: partition})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

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
	batch, batchIdx := ir.partition.GetBatchWithTuple(ctx, idx)
	orderCol := batch.ColVec(orderColIdx)
	row := make(tree.Datums, 1)
	if orderCol.Nulls().NullAt(batchIdx) {
		row[orderColIdx] = tree.DNull
	} else {
		val := orderCol.Int64()[batchIdx]
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
		batch, batchIdx := c.partition.GetBatchWithTuple(c.evalCtx.Ctx(), idx)
		if batch.ColVec(peersColIdx).Bool()[batchIdx] {
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
	panic(errors.AssertionFailedf("boundType out of range"))
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
	panic(errors.AssertionFailedf("boundType out of range"))
}
