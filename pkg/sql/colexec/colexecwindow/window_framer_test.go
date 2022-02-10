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
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const probabilityOfNewNumber = 0.5
const orderColIdx = 0
const peersColIdx = 1

// TestWindowFramer runs the various windowFramer operators against the
// WindowFrameRunner used by the row engine.
func TestWindowFramer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	// The spilling buffer used by the window framers always uses
	// colcontainer.DiskQueueCacheModeClearAndReuseCache mode.
	queueCfg.SetCacheMode(colcontainer.DiskQueueCacheModeClearAndReuseCache)
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(evalCtx.Ctx())

	factory := coldataext.NewExtendedColumnFactory(evalCtx)
	allocator := colmem.NewAllocator(evalCtx.Ctx(), &memAcc, factory)

	var memLimits = []int64{1, 1 << 10, 1 << 20}

	testCfg := &testConfig{
		rng:       rng,
		evalCtx:   evalCtx,
		factory:   factory,
		allocator: allocator,
		queueCfg:  queueCfg,
		memLimit:  memLimits[rng.Intn(len(memLimits))],
	}

	const randTypeProbability = 0.5
	var randTypes = []*types.T{
		types.Float, types.Decimal, types.Interval, types.TimestampTZ, types.Date, types.TimeTZ,
	}
	const randExcludeProbability = 0.5
	var randExclusions = []treewindow.WindowFrameExclusion{
		treewindow.ExcludeCurrentRow, treewindow.ExcludeGroup, treewindow.ExcludeTies,
	}
	for _, mode := range []treewindow.WindowFrameMode{treewindow.ROWS, treewindow.GROUPS, treewindow.RANGE} {
		testCfg.mode = mode
		t.Run(fmt.Sprintf("mode=%v", mode.Name()), func(t *testing.T) {
			for _, bound := range []treewindow.WindowFrameBoundType{
				treewindow.UnboundedPreceding, treewindow.OffsetPreceding, treewindow.CurrentRow,
				treewindow.OffsetFollowing, treewindow.UnboundedFollowing,
			} {
				for _, bounds := range [][2]treewindow.WindowFrameBoundType{
					{bound, getEndBound(rng, bound)},
					{getStartBound(rng, bound), bound},
				} {
					start, end := bounds[0], bounds[1]
					if !validForStart(start) || !validForEnd(end) {
						// These bounds would entail a syntactic error.
						continue
					}
					testCfg.startBound, testCfg.endBound = start, end
					t.Run(fmt.Sprintf("start=%v/end=%v", start.Name(), end.Name()), func(t *testing.T) {
						for _, count := range []int{1, 17, 42, 91} {
							testCfg.count = count
							testCfg.ordered = true
							typ := types.Int
							if rng.Float64() < randTypeProbability {
								typ = randTypes[rng.Intn(len(randTypes))]
							}
							testCfg.typ = typ
							exclusion := treewindow.NoExclusion
							if rng.Float64() < randExcludeProbability {
								exclusion = randExclusions[rng.Intn(len(randExclusions))]
							}
							testCfg.exclusion = exclusion
							t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
								for _, asc := range []bool{true, false} {
									testCfg.asc = asc
									t.Run(
										fmt.Sprintf("ordered/asc=%v/typ=%v/exclusion=%v", asc, typ, exclusion.Name()),
										func(t *testing.T) {
											testWindowFramer(t, testCfg)
										})
								}
								if mode == treewindow.ROWS {
									// An ORDER BY clause is required for RANGE and GROUPS modes.
									testCfg.ordered = false
									t.Run(
										fmt.Sprintf("unordered/exclusion=%v", exclusion.Name()),
										func(t *testing.T) {
											testWindowFramer(t, testCfg)
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

type testConfig struct {
	rng        *rand.Rand
	evalCtx    *tree.EvalContext
	factory    coldata.ColumnFactory
	allocator  *colmem.Allocator
	queueCfg   colcontainer.DiskQueueCfg
	typ        *types.T
	count      int
	ordered    bool
	asc        bool
	mode       treewindow.WindowFrameMode
	startBound treewindow.WindowFrameBoundType
	endBound   treewindow.WindowFrameBoundType
	exclusion  treewindow.WindowFrameExclusion
	memLimit   int64
}

func testWindowFramer(t *testing.T, testCfg *testConfig) {
	colWindowFramer, rowWindowFramer, partition := initWindowFramers(t, testCfg)

	for i := 0; i < testCfg.count; i++ {
		// Advance the columnar framer.
		colWindowFramer.next(testCfg.evalCtx.Ctx())

		// Advance the row-wise framer.
		if testCfg.ordered {
			vec, vecIdx, _ := partition.GetVecWithTuple(testCfg.evalCtx.Ctx(), peersColIdx, i)
			if i > 0 && vec.Bool()[vecIdx] {
				rowWindowFramer.CurRowPeerGroupNum++
				require.NoError(t, rowWindowFramer.PeerHelper.Update(rowWindowFramer))
			}
		}
		rowWindowFramer.RowIdx = i
		rowStartIdx, err := rowWindowFramer.FrameStartIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		require.NoError(t, err)
		rowEndIdx, err := rowWindowFramer.FrameEndIdx(testCfg.evalCtx.Ctx(), testCfg.evalCtx)
		require.NoError(t, err)

		// Validate that the columnar window framer describes the same window
		// frame as the row-wise one.
		var colFrameIdx, firstIdx, lastIdx int
		var foundRow bool
		for j := rowStartIdx; j < rowEndIdx; j++ {
			skipped, err := rowWindowFramer.IsRowSkipped(testCfg.evalCtx.Ctx(), j)
			require.NoError(t, err)
			if skipped {
				continue
			}
			if !foundRow {
				firstIdx = j
				foundRow = true
			}
			lastIdx = j
			colFrameIdx++
			require.Equal(t, j, colWindowFramer.frameNthIdx(colFrameIdx), "NthIdx")
		}
		if !foundRow {
			// The columnar window framer functions return -1 to signify an empty
			// window frame.
			require.Equal(t, -1, colWindowFramer.frameFirstIdx(), "No FirstIdx")
			require.Equal(t, -1, colWindowFramer.frameLastIdx(), "No LastIdx")
			require.Equal(t, -1, colWindowFramer.frameNthIdx(1 /* n */), "No NthIdx")
			continue
		}
		require.Equal(t, firstIdx, colWindowFramer.frameFirstIdx(), "FirstIdx")
		require.Equal(t, lastIdx, colWindowFramer.frameLastIdx(), "LastIdx")
	}

	partition.Close(testCfg.evalCtx.Ctx())
}

func validForStart(bound treewindow.WindowFrameBoundType) bool {
	return bound != treewindow.UnboundedFollowing
}

func validForEnd(bound treewindow.WindowFrameBoundType) bool {
	return bound != treewindow.UnboundedPreceding
}

func getStartBound(
	rng *rand.Rand, endBound treewindow.WindowFrameBoundType,
) treewindow.WindowFrameBoundType {
	startBoundTypes := []treewindow.WindowFrameBoundType{
		treewindow.UnboundedPreceding, treewindow.OffsetPreceding, treewindow.CurrentRow, treewindow.OffsetFollowing,
	}
	for {
		startBound := startBoundTypes[rng.Intn(len(startBoundTypes))]
		if startBound <= endBound {
			return startBound
		}
	}
}

func getEndBound(
	rng *rand.Rand, startBound treewindow.WindowFrameBoundType,
) treewindow.WindowFrameBoundType {
	endBoundTypes := []treewindow.WindowFrameBoundType{
		treewindow.OffsetPreceding, treewindow.CurrentRow, treewindow.OffsetFollowing, treewindow.UnboundedFollowing,
	}
	for {
		endBound := endBoundTypes[rng.Intn(len(endBoundTypes))]
		if endBound >= startBound {
			return endBound
		}
	}
}

type datumRows struct {
	rows tree.Datums
	asc  bool
	ctx  *tree.EvalContext
}

func (r *datumRows) Len() int {
	return len(r.rows)
}

func (r *datumRows) Less(i, j int) bool {
	cmp := r.rows[i].Compare(r.ctx, r.rows[j])
	if r.asc {
		return cmp < 0
	}
	return cmp > 0
}

func (r *datumRows) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
}

func makeSortedPartition(testCfg *testConfig) (tree.Datums, *colexecutils.SpillingBuffer) {
	datums := &datumRows{
		rows: make(tree.Datums, testCfg.count),
		asc:  testCfg.asc,
		ctx:  testCfg.evalCtx,
	}
	const nullChance = 0.2
	for i := range datums.rows {
		if testCfg.rng.Float64() < nullChance {
			datums.rows[i] = tree.DNull
			continue
		}
		if i == 0 || testCfg.rng.Float64() < probabilityOfNewNumber {
			datums.rows[i] = randgen.RandDatumSimple(testCfg.rng, testCfg.typ)
			continue
		}
		datums.rows[i] = datums.rows[i-1]
	}
	sort.Sort(datums)

	partition := colexecutils.NewSpillingBuffer(
		testCfg.allocator, testCfg.memLimit, testCfg.queueCfg,
		colexecop.NewTestingSemaphore(2), []*types.T{testCfg.typ, types.Bool}, testDiskAcc,
	)
	insertBatch := testCfg.allocator.NewMemBatchWithFixedCapacity(
		[]*types.T{testCfg.typ, types.Bool},
		1, /* capacity */
	)
	var last tree.Datum
	for i, val := range datums.rows {
		insertBatch.ColVec(orderColIdx).Nulls().UnsetNulls()
		insertBatch.ColVec(peersColIdx).Bool()[0] = false
		if i == 0 || val.Compare(testCfg.evalCtx, last) != 0 {
			insertBatch.ColVec(peersColIdx).Bool()[0] = true
		}
		last = val
		vec := insertBatch.ColVec(orderColIdx)
		if val == tree.DNull {
			vec.Nulls().SetNull(0)
		} else {
			switch t := val.(type) {
			case *tree.DInt:
				vec.Int64()[0] = int64(*t)
			case *tree.DFloat:
				vec.Float64()[0] = float64(*t)
			case *tree.DDecimal:
				vec.Decimal()[0] = t.Decimal
			case *tree.DInterval:
				vec.Interval()[0] = t.Duration
			case *tree.DTimestampTZ:
				vec.Timestamp()[0] = t.Time
			case *tree.DDate:
				vec.Int64()[0] = t.UnixEpochDaysWithOrig()
			case *tree.DTimeTZ:
				vec.Datum().Set(0, t)
			default:
				colexecerror.InternalError(errors.AssertionFailedf("unsupported datum: %v", t))
			}
		}
		insertBatch.SetLength(1)
		partition.AppendTuples(
			testCfg.evalCtx.Ctx(), insertBatch, 0 /* startIdx */, insertBatch.Length())
	}
	return datums.rows, partition
}

func initWindowFramers(
	t *testing.T, testCfg *testConfig,
) (windowFramer, *tree.WindowFrameRun, *colexecutils.SpillingBuffer) {
	offsetType := types.Int
	if testCfg.mode == treewindow.RANGE {
		offsetType = GetOffsetTypeFromOrderColType(t, testCfg.typ)
	}
	startOffset := colexectestutils.MakeRandWindowFrameRangeOffset(t, testCfg.rng, offsetType)
	endOffset := colexectestutils.MakeRandWindowFrameRangeOffset(t, testCfg.rng, offsetType)

	peersCol, orderCol := tree.NoColumnIdx, tree.NoColumnIdx
	if testCfg.ordered {
		peersCol, orderCol = peersColIdx, orderColIdx
	}

	datums, colBuffer := makeSortedPartition(testCfg)

	datumEncoding := descpb.DatumEncoding_ASCENDING_KEY
	if !testCfg.asc {
		datumEncoding = descpb.DatumEncoding_DESCENDING_KEY
	}
	frame := &execinfrapb.WindowerSpec_Frame{
		Mode: modeToExecinfrapb(testCfg.mode),
		Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
			Start: execinfrapb.WindowerSpec_Frame_Bound{
				BoundType:   boundToExecinfrapb(testCfg.startBound),
				TypedOffset: colexectestutils.EncodeWindowFrameOffset(t, startOffset),
				OffsetType: execinfrapb.DatumInfo{
					Type:     testCfg.typ,
					Encoding: datumEncoding,
				},
			},
			End: &execinfrapb.WindowerSpec_Frame_Bound{
				BoundType:   boundToExecinfrapb(testCfg.endBound),
				TypedOffset: colexectestutils.EncodeWindowFrameOffset(t, endOffset),
				OffsetType: execinfrapb.DatumInfo{
					Type:     testCfg.typ,
					Encoding: datumEncoding,
				},
			},
		},
		Exclusion: exclusionToExecinfrapb(testCfg.exclusion),
	}
	if testCfg.mode != treewindow.RANGE {
		frame.Bounds.Start.IntOffset = uint64(*(startOffset.(*tree.DInt)))
		frame.Bounds.End.IntOffset = uint64(*(endOffset.(*tree.DInt)))
	}
	ordering := &execinfrapb.Ordering{}
	if testCfg.ordered {
		dir := execinfrapb.Ordering_Column_ASC
		if !testCfg.asc {
			dir = execinfrapb.Ordering_Column_DESC
		}
		ordering.Columns = []execinfrapb.Ordering_Column{{
			ColIdx:    uint32(orderCol),
			Direction: dir,
		}}
	}
	colWindowFramer := newWindowFramer(
		testCfg.evalCtx, frame, ordering, []*types.T{testCfg.typ, types.Bool}, peersCol)
	colWindowFramer.startPartition(testCfg.evalCtx.Ctx(), colBuffer.Length(), colBuffer)

	rowDir := encoding.Ascending
	if !testCfg.asc {
		rowDir = encoding.Descending
	}
	rowWindowFramer := &tree.WindowFrameRun{
		Rows:         &indexedRows{partition: datums, orderColType: testCfg.typ},
		ArgsIdxs:     []uint32{0},
		FilterColIdx: tree.NoColumnIdx,
		Frame: &tree.WindowFrame{
			Mode: testCfg.mode,
			Bounds: tree.WindowFrameBounds{
				StartBound: &tree.WindowFrameBound{
					BoundType:  testCfg.startBound,
					OffsetExpr: startOffset,
				},
				EndBound: &tree.WindowFrameBound{
					BoundType:  testCfg.endBound,
					OffsetExpr: endOffset,
				},
			},
			Exclusion: testCfg.exclusion,
		},
		StartBoundOffset: startOffset,
		EndBoundOffset:   endOffset,
		OrdColIdx:        orderCol,
		OrdDirection:     rowDir,
	}
	rowWindowFramer.PlusOp, rowWindowFramer.MinusOp, _ = tree.WindowFrameRangeOps{}.LookupImpl(testCfg.typ, offsetType)
	require.NoError(t, rowWindowFramer.PeerHelper.Init(
		rowWindowFramer,
		&peerGroupChecker{partition: datums, ordered: testCfg.ordered}),
	)

	return colWindowFramer, rowWindowFramer, colBuffer
}

type indexedRows struct {
	partition    tree.Datums
	orderColType *types.T
}

var _ tree.IndexedRows = &indexedRows{}

func (ir indexedRows) Len() int {
	return len(ir.partition)
}

func (ir indexedRows) GetRow(ctx context.Context, idx int) (tree.IndexedRow, error) {
	return indexedRow{row: tree.Datums{ir.partition[idx]}}, nil
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
	partition tree.Datums
	ordered   bool
}

var _ tree.PeerGroupChecker = &peerGroupChecker{}

func (c *peerGroupChecker) InSameGroup(i, j int) (bool, error) {
	if !c.ordered {
		// All rows are in the same peer group.
		return true, nil
	}
	return c.partition[i].Compare(&c.evalCtx, c.partition[j]) == 0, nil
}

func modeToExecinfrapb(mode treewindow.WindowFrameMode) execinfrapb.WindowerSpec_Frame_Mode {
	switch mode {
	case treewindow.RANGE:
		return execinfrapb.WindowerSpec_Frame_RANGE
	case treewindow.ROWS:
		return execinfrapb.WindowerSpec_Frame_ROWS
	case treewindow.GROUPS:
		return execinfrapb.WindowerSpec_Frame_GROUPS
	}
	return 0
}

func boundToExecinfrapb(
	bound treewindow.WindowFrameBoundType,
) execinfrapb.WindowerSpec_Frame_BoundType {
	switch bound {
	case treewindow.UnboundedPreceding:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case treewindow.OffsetPreceding:
		return execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING
	case treewindow.CurrentRow:
		return execinfrapb.WindowerSpec_Frame_CURRENT_ROW
	case treewindow.OffsetFollowing:
		return execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	case treewindow.UnboundedFollowing:
		return execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	}
	return 0
}

func exclusionToExecinfrapb(
	exclusion treewindow.WindowFrameExclusion,
) execinfrapb.WindowerSpec_Frame_Exclusion {
	switch exclusion {
	case treewindow.NoExclusion:
		return execinfrapb.WindowerSpec_Frame_NO_EXCLUSION
	case treewindow.ExcludeCurrentRow:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
	case treewindow.ExcludeGroup:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP
	case treewindow.ExcludeTies:
		return execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES
	}
	return 0
}

func TestGetSlidingWindowIntervals(t *testing.T) {
	testCases := []struct {
		prevIntervals []windowInterval
		currIntervals []windowInterval
		expectedToAdd []windowInterval
		expectedToRem []windowInterval
	}{
		{
			prevIntervals: []windowInterval{
				{0, 2},
				{4, 5},
			},
			currIntervals: []windowInterval{
				{0, 6},
			},
			expectedToAdd: []windowInterval{
				{2, 4},
				{5, 6},
			},
			expectedToRem: nil,
		},
		{
			prevIntervals: []windowInterval{
				{1, 2},
			},
			currIntervals: []windowInterval{
				{0, 4},
			},
			expectedToAdd: []windowInterval{
				{0, 1},
				{2, 4},
			},
			expectedToRem: nil,
		},
		{
			prevIntervals: []windowInterval{
				{0, 6},
			},
			currIntervals: []windowInterval{
				{1, 2},
				{5, 7},
			},
			expectedToAdd: []windowInterval{
				{6, 7},
			},
			expectedToRem: []windowInterval{
				{0, 1},
				{2, 5},
			},
		},
		{
			prevIntervals: []windowInterval{
				{0, 2},
				{4, 5},
				{6, 8},
			},
			currIntervals: []windowInterval{
				{0, 2},
				{4, 5},
				{6, 8},
			},
			expectedToAdd: nil,
			expectedToRem: nil,
		},
		{
			prevIntervals: []windowInterval{
				{0, 2},
				{4, 5},
				{6, 8},
			},
			currIntervals: []windowInterval{
				{0, 2},
				{4, 5},
				{6, 8},
				{9, 10},
			},
			expectedToAdd: []windowInterval{
				{9, 10},
			},
			expectedToRem: nil,
		},
	}

	for i := range testCases {
		tc := &testCases[i]
		toAdd, toRemove := getSlidingWindowIntervals(tc.currIntervals, tc.prevIntervals, nil, nil)
		require.Equalf(t, tc.expectedToAdd, toAdd, "toAdd")
		require.Equalf(t, tc.expectedToRem, toRemove, "toRemove")
	}
}
